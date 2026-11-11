//! The daemon. One engine task owns everything mutable: every space's Loro docs, its
//! applied.json, its connection state, and every call to Television. Sockets, the store-events
//! watcher and the timer only send it messages (the actor design), so syncs never interleave.
//!
//! Each sync of a space is the same three-way compare, at start-up and every two seconds:
//!
//! 1. read Television and the files                    → `local`
//! 2. `local` vs `applied`: edits made on this machine  → write them into Loro
//!    (updates from the relay wait until this is done)
//! 3. import them; Loro, now merged, vs `local`         → write the difference into Television
//! 4. what Television holds now                         → becomes `applied`
//!
//! Our own writes come back in step 1 equal to `applied`, so they're never taken for new edits
//! (echo suppression). Edits made while the daemon was off show up as `local ≠ applied`
//! (reconcile on start). Nothing replays events: `tv resource events` only says which stores to
//! read again.

use std::{
    collections::{BTreeMap, HashMap, HashSet},
    fs,
    path::{Path, PathBuf},
    process::Stdio,
    time::Duration,
};

use anyhow::{Context, Result, bail};
use loro::LoroDoc;
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value, json};
use tokio::{
    io::{AsyncBufReadExt, BufReader},
    signal::unix::{SignalKind, signal},
    sync::mpsc,
    time::{Instant, sleep},
};

use crate::{
    daemon,
    files::{self, Hasher},
    links,
    model::{self, Art, Kind},
    relay::Relay,
    rooms::{Outcome, Rooms, SPACE_ROOM, artifact_room},
    state::{Home, Op, Request, SpaceEntry, write_atomic},
    tv::{self, Tv},
    wire::{self, Outbox, WireEvent},
};

const TICK: Duration = Duration::from_secs(2);

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
struct ChanState {
    local_id: String,
    name: String,
    /// Shared IDs, left to right.
    order: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
struct ArtState {
    local_id: String,
    path: Option<String>,
    art: Art,
    store: Value,
    markdown: Option<String>,
}

/// A space as this machine's Television shows it, keyed by shared IDs.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
struct State {
    channels: BTreeMap<String, ChanState>,
    artifacts: BTreeMap<String, ArtState>,
}

/// applied.json: the state we last left Television in. The ID map (shared ID → local ID) lives
/// inside it, so the two can't disagree after a crash.
#[derive(Default, Serialize, Deserialize)]
struct Applied {
    state: State,
    /// The last `share`/`unshare` request handled.
    last_request: u64,
}

/// For `status`: the names of the channels in a space, from its saved doc, and the
/// `share`/`unshare` requests the daemon hasn't handled yet.
pub fn overview(home: &Home, id: &str, entry: &SpaceEntry) -> Result<(Vec<String>, Vec<Request>)> {
    let dir = home.space_dir(id);
    let mut names: Vec<_> = match fs::read(dir.join(format!("{SPACE_ROOM}.loro"))) {
        Ok(bytes) => {
            model::read_space(&LoroDoc::from_snapshot(&bytes)?).channels.into_values().map(|c| c.name).collect()
        }
        Err(_) => vec![], // the daemon hasn't saved the space yet
    };
    names.sort();
    let applied: Applied = match fs::read_to_string(dir.join("applied.json")) {
        Ok(json) => serde_json::from_str(&json)?,
        Err(_) => Applied::default(),
    };
    let pending = entry.requests.iter().filter(|r| r.id > applied.last_request).cloned().collect();
    Ok((names, pending))
}

struct Remote {
    space: model::Space,
    /// Store and markdown of each artifact whose doc is ready.
    docs: BTreeMap<String, (Value, Option<String>)>,
}

enum Link {
    Offline { retry_at: Instant },
    Connecting,
    Online,
    Revoked,
}

struct Space {
    id: String,
    entry: SpaceEntry,
    relay: Relay,
    rooms: Rooms,
    applied: Applied,
    applied_saved: String,
    link: Link,
    /// Bumped on every connection attempt and every loss; events from older sockets are ignored.
    epoch: u64,
    backoff: Duration,
    dirty: bool,
    /// Blobs known to be on the relay.
    uploaded: HashSet<String>,
}

enum Cmd {
    Tick,
    Connected(String, u64, Result<Outbox, String>),
    Wire(String, u64, WireEvent),
    StoreChanged(String),
    StoresUnknown,
}

struct TvNow {
    channels: Vec<tv::Channel>,
    artifacts: HashMap<String, tv::Artifact>,
}

struct Engine {
    home: Home,
    tv: Tv,
    tx: mpsc::UnboundedSender<Cmd>,
    spaces: BTreeMap<String, Space>,
    hasher: Hasher,
    /// Each local artifact's store as last read. Dropped when `tv resource events` reports a change.
    stores: HashMap<String, Value>,
    /// The last warning printed per subject, so a failure that repeats every tick prints once.
    warned: HashMap<String, String>,
    /// This machine's Television port, for links between artifacts (see `links`).
    tv_port: Option<u16>,
}

pub async fn run(home: Home, tv: Tv) -> Result<()> {
    // Two daemons on one data dir would both write the same docs and applied.json.
    let _lock = daemon::lock(&home)?;
    // `tv-sync stop` sends SIGTERM; Ctrl-C sends SIGINT when this runs in a terminal.
    let (mut term, mut interrupt) = (signal(SignalKind::terminate())?, signal(SignalKind::interrupt())?);

    let (tx, mut rx) = mpsc::unbounded_channel();
    spawn_ticker(tx.clone());
    spawn_store_events(tv.clone(), tx.clone());
    eprintln!("tv-sync running for {} (pid {})", home.dir.display(), std::process::id());
    let tv_port = match tv.port().await {
        Ok(port) if port != 0 => Some(port),
        _ => {
            eprintln!("can't tell which port Television uses; links between artifacts sync unchanged");
            None
        }
    };
    let mut engine = Engine {
        tv_port,
        home,
        tv,
        tx,
        spaces: BTreeMap::new(),
        hasher: Hasher::default(),
        stores: HashMap::new(),
        warned: HashMap::new(),
    };
    loop {
        // A signal is only noticed here, between syncs, so stopping never cuts one in half.
        // Anything still in the inbox is safe to drop: the relay keeps it, and the next start
        // backfills it and rereads Television.
        let cmd = tokio::select! {
            Some(cmd) = rx.recv() => cmd,
            _ = term.recv() => break,
            _ = interrupt.recv() => break,
            else => break,
        };
        engine.handle(cmd);
        // Sync once the inbox is empty, so a burst of messages becomes one sync.
        if rx.is_empty() {
            engine.sync_dirty().await;
        }
    }
    eprintln!("tv-sync stopped");
    Ok(())
}

fn spawn_ticker(tx: mpsc::UnboundedSender<Cmd>) {
    tokio::spawn(async move {
        let mut every = tokio::time::interval(TICK);
        while tx.send(Cmd::Tick).is_ok() {
            every.tick().await;
        }
    });
}

/// Follow `tv resource events` to learn which stores changed. The events only mark stores to
/// re-read; the sync still compares whole snapshots.
fn spawn_store_events(tv: Tv, tx: mpsc::UnboundedSender<Cmd>) {
    tokio::spawn(async move {
        loop {
            let child = tv
                .command()
                .args(["resource", "events"])
                .stdin(Stdio::null())
                .stdout(Stdio::piped())
                .stderr(Stdio::null())
                .kill_on_drop(true)
                .spawn();
            if let Ok(mut child) = child {
                if let Some(out) = child.stdout.take() {
                    // Changes made before this listener started are unknown, so once it's
                    // listening, reread every store. Events meanwhile wait in the pipe.
                    sleep(Duration::from_secs(1)).await;
                    if tx.send(Cmd::StoresUnknown).is_err() {
                        return;
                    }
                    let mut lines = BufReader::new(out).lines();
                    while let Ok(Some(line)) = lines.next_line().await {
                        let event: Value = serde_json::from_str(&line).unwrap_or_default();
                        if let Some(id) = event["artifactID"].as_str() {
                            let _ = tx.send(Cmd::StoreChanged(id.to_string()));
                        }
                    }
                }
                let _ = child.wait().await;
            }
            if tx.is_closed() {
                return;
            }
            sleep(TICK).await;
        }
    });
}

fn say(sp: &Space, msg: impl AsRef<str>) {
    eprintln!("[{}] {}", sp.entry.name, msg.as_ref());
}

impl Space {
    fn open(home: &Home, id: String, entry: SpaceEntry) -> Result<Self> {
        let dir = home.space_dir(&id);
        let mut rooms = Rooms::open(dir.clone(), entry.token.clone())?;
        rooms.doc(SPACE_ROOM);
        let saved = fs::read_to_string(dir.join("applied.json")).unwrap_or_default();
        let applied = if saved.is_empty() { Applied::default() } else { serde_json::from_str(&saved)? };
        Ok(Self {
            relay: Relay::new(&entry.relay),
            id,
            entry,
            rooms,
            applied,
            applied_saved: saved,
            link: Link::Offline { retry_at: Instant::now() },
            epoch: 0,
            backoff: Duration::from_secs(1),
            dirty: true,
            uploaded: HashSet::new(),
        })
    }

    fn save_applied(&mut self, home: &Home) -> Result<()> {
        let json = serde_json::to_string_pretty(&self.applied)?;
        if json != self.applied_saved {
            write_atomic(&home.space_dir(&self.id).join("applied.json"), json.as_bytes())?;
            self.applied_saved = json;
        }
        Ok(())
    }

    fn revoked(&mut self, why: &str) {
        self.rooms.disconnected();
        self.epoch += 1;
        self.link = Link::Revoked;
        say(self, format!("this device was removed from the space ({why}). It no longer syncs; local copies stay."));
    }
}

impl Engine {
    fn handle(&mut self, cmd: Cmd) {
        match cmd {
            Cmd::Tick => {
                if let Err(e) = self.load_spaces() {
                    self.warn("spaces", &format!("can't read spaces.json: {e:#}"));
                }
                self.connect_due();
                self.spaces.values_mut().for_each(|sp| sp.dirty = true);
            }
            Cmd::Connected(id, epoch, result) => {
                let Some(sp) = self.spaces.get_mut(&id).filter(|sp| sp.epoch == epoch) else { return };
                match result {
                    Ok(outbox) => {
                        say(sp, "connected to the relay");
                        sp.link = Link::Online;
                        sp.backoff = Duration::from_secs(1);
                        sp.rooms.connected(outbox);
                    }
                    Err(e) => {
                        if sp.backoff == Duration::from_secs(1) {
                            say(sp, format!("can't reach the relay ({e}); working offline, retrying"));
                        }
                        sp.link = Link::Offline { retry_at: Instant::now() + sp.backoff };
                        sp.backoff = (sp.backoff * 2).min(Duration::from_secs(60));
                    }
                }
            }
            Cmd::Wire(id, epoch, event) => {
                let Some(sp) = self.spaces.get_mut(&id).filter(|sp| sp.epoch == epoch) else { return };
                match event {
                    WireEvent::Message(msg) => {
                        sp.dirty = true;
                        match sp.rooms.handle(msg) {
                            Ok(Outcome::Refused(why)) => sp.revoked(&why),
                            Ok(_) => {}
                            Err(e) => say(sp, format!("{e:#}")),
                        }
                    }
                    WireEvent::Closed { revoked: true } => sp.revoked("closed by the relay"),
                    WireEvent::Closed { revoked: false } => {
                        say(sp, "lost the relay; working offline, reconnecting");
                        sp.rooms.disconnected();
                        sp.epoch += 1;
                        sp.link = Link::Offline { retry_at: Instant::now() + Duration::from_secs(1) };
                    }
                }
            }
            Cmd::StoreChanged(artifact) => {
                if self.stores.remove(&artifact).is_some() {
                    self.spaces.values_mut().for_each(|sp| sp.dirty = true);
                }
            }
            Cmd::StoresUnknown => {
                self.stores.clear();
                self.spaces.values_mut().for_each(|sp| sp.dirty = true);
            }
        }
    }

    /// Pick up spaces added by `space create` / `join`, and new requests from `share`/`unshare`.
    fn load_spaces(&mut self) -> Result<()> {
        for (id, entry) in self.home.load()?.spaces {
            match self.spaces.get_mut(&id) {
                Some(sp) => sp.entry = entry,
                None => {
                    let sp = Space::open(&self.home, id.clone(), entry)?;
                    self.spaces.insert(id, sp);
                }
            }
        }
        Ok(())
    }

    fn connect_due(&mut self) {
        for sp in self.spaces.values_mut() {
            let Link::Offline { retry_at } = sp.link else { continue };
            if Instant::now() < retry_at {
                continue;
            }
            sp.epoch += 1;
            sp.link = Link::Connecting;
            let (tx, id, epoch, url) = (self.tx.clone(), sp.id.clone(), sp.epoch, sp.relay.ws_url(&sp.id));
            tokio::spawn(async move {
                let (events, space) = (tx.clone(), id.clone());
                let result = wire::connect(&url, move |e| {
                    let _ = events.send(Cmd::Wire(space.clone(), epoch, e));
                })
                .await;
                let _ = tx.send(Cmd::Connected(id, epoch, result.map_err(|e| format!("{e:#}"))));
            });
        }
    }

    fn warn(&mut self, key: &str, msg: &str) {
        if self.warned.insert(key.to_string(), msg.to_string()).as_deref() != Some(msg) {
            eprintln!("{msg}");
        }
    }

    async fn sync_dirty(&mut self) {
        let due: Vec<String> = self
            .spaces
            .iter()
            .filter(|(_, sp)| sp.dirty && !matches!(sp.link, Link::Revoked))
            .map(|(id, _)| id.clone())
            .collect();
        if due.is_empty() {
            return;
        }
        let now = match self.read_tv().await {
            Ok(now) => now,
            Err(e) => return self.warn("tv", &format!("can't read Television ({e:#}); is `tv serve` running?")),
        };
        self.warned.remove("tv");
        for id in due {
            // Take the space out of the map while syncing it, so `self` and the space can be
            // borrowed mutably at the same time.
            let Some(mut sp) = self.spaces.remove(&id) else { continue };
            sp.dirty = false;
            match self.sync(&mut sp, &now).await {
                Ok(()) => drop(self.warned.remove(&id)),
                Err(e) => self.warn(&id, &format!("[{}] sync failed, will retry: {e:#}", sp.entry.name)),
            }
            self.spaces.insert(id, sp);
        }
    }

    async fn read_tv(&self) -> Result<TvNow> {
        let channels = self.tv.channels().await?;
        let artifacts = self.tv.artifacts().await?.into_iter().map(|a| (a.id.clone(), a)).collect();
        Ok(TvNow { channels, artifacts })
    }

    async fn sync(&mut self, sp: &mut Space, now: &TvNow) -> Result<()> {
        // A space doc that hasn't arrived from the relay yet would read as "empty space". Take in
        // what has arrived; the next tick syncs once it's complete.
        if !sp.rooms.is_ready(SPACE_ROOM) {
            sp.rooms.import_inbox();
            return Ok(());
        }
        self.handle_requests(sp, now)?;
        // A shared channel deleted here is missing from `local`, so step 3 recreates it: deleting
        // a channel here doesn't delete it for everyone (use `unshare` for that).
        let local = self.read_local(sp, now).await?;
        let pending = self.record(sp, &local).await?;
        sp.applied.state = recorded(&local, &sp.applied.state, &pending);
        sp.save_applied(&self.home)?;
        sp.rooms.commit();
        // Only now take in what the relay sent: `record` had to see the docs as Television
        // last showed them (see `Rooms::import_inbox`).
        sp.rooms.import_inbox();
        let remote = read_remote(sp);
        sp.applied.state = self.project(sp, &local, &remote, &pending, now).await?;
        sp.save_applied(&self.home)?;
        sp.rooms.flush()
    }

    fn handle_requests(&mut self, sp: &mut Space, now: &TvNow) -> Result<()> {
        let doc = sp.rooms.doc(SPACE_ROOM);
        let todo: Vec<_> = sp.entry.requests.iter().filter(|r| r.id > sp.applied.last_request).cloned().collect();
        for req in todo {
            let state = &mut sp.applied.state;
            let shared = state.channels.iter().find(|(_, c)| c.local_id == req.channel).map(|(id, _)| id.clone());
            match (req.op, shared) {
                (Op::Share, None) => match now.channels.iter().find(|c| c.id == req.channel) {
                    Some(ch) => {
                        // The channel's shared ID is its ID here, on the machine that shared it.
                        model::add_channel(&doc, &ch.id, &ch.name)?;
                        let st = ChanState { local_id: ch.id.clone(), name: ch.name.clone(), order: vec![] };
                        state.channels.insert(ch.id.clone(), st);
                        say(sp, format!("sharing channel {:?}", ch.name));
                    }
                    None => say(sp, format!("can't share channel {}: Television doesn't have it", req.channel)),
                },
                (Op::Unshare, Some(cid)) => {
                    model::remove_channel(&doc, &cid)?;
                    state.channels.remove(&cid);
                    state.artifacts.retain(|_, a| a.art.channel_id != cid);
                    say(sp, format!("channel {cid} left the space; it stays here, unsynced"));
                }
                _ => {} // already shared, or not shared
            }
            sp.applied.last_request = req.id;
        }
        Ok(())
    }

    // ── Step 1: read Television ─────────────────────────────────────────────────────────────

    async fn read_local(&mut self, sp: &Space, now: &TvNow) -> Result<State> {
        let applied = &sp.applied.state;
        let shared_of: HashMap<&str, &str> =
            applied.artifacts.iter().map(|(sid, a)| (a.local_id.as_str(), sid.as_str())).collect();
        let synced = format!("{}/", self.home.dir.join("files").display());
        let mut local = State::default();
        for (cid, ch) in &applied.channels {
            let Some(tch) = now.channels.iter().find(|c| c.id == ch.local_id) else { continue };
            let mut order = Vec::new();
            for slot in &tch.layout {
                for aid in &slot.artifact_ids {
                    let Some(t) = now.artifacts.get(aid) else { continue };
                    let sid = match shared_of.get(aid.as_str()) {
                        Some(sid) => sid.to_string(),
                        // Not in the ID map. If its files are in our synced folder, we created it
                        // and crashed before saving the map: the folder name is its shared ID.
                        // Otherwise it's new here, and its shared ID is its local ID.
                        None => t
                            .path
                            .as_deref()
                            .and_then(|p| p.strip_prefix(synced.as_str()))
                            .and_then(|rest| rest.split('/').next())
                            .map_or_else(|| aid.clone(), String::from),
                    };
                    order.push(sid.clone());
                    if let Some(st) = self.read_artifact(t, slot, cid, applied.artifacts.get(&sid)).await? {
                        local.artifacts.insert(sid, st);
                    }
                }
            }
            let st = ChanState { local_id: ch.local_id.clone(), name: tch.name.clone(), order };
            local.channels.insert(cid.clone(), st);
        }
        if let Some(port) = self.tv_port {
            let shared: HashMap<String, String> =
                local.artifacts.iter().map(|(sid, a)| (a.local_id.clone(), sid.clone())).collect();
            for a in local.artifacts.values_mut() {
                if let Some(url) = &a.art.url {
                    a.art.url = Some(links::to_shared(url, port, |id| shared.get(id).cloned()));
                }
            }
        }
        Ok(local)
    }

    async fn read_artifact(
        &mut self,
        t: &tv::Artifact,
        slot: &tv::Slot,
        channel: &str,
        prev: Option<&ArtState>,
    ) -> Result<Option<ArtState>> {
        let mut art = Art {
            channel_id: channel.to_string(),
            title: t.title.clone(),
            kind: Kind::Url,
            url: t.url.clone(),
            entry: None,
            files: BTreeMap::new(),
            size: slot.size,
            full_screen: slot.geometry.full_screen,
        };
        let mut markdown = None;
        match (t.kind.as_str(), &t.path) {
            ("url", _) => {}
            ("path", Some(path)) => {
                let p = Path::new(path);
                let read: Result<()> = if path.ends_with('/') || p.is_dir() {
                    art.kind = Kind::Html;
                    self.hasher.scan(p).map(|found| art.files = found)
                } else {
                    let name = p.file_name().map(|n| n.to_string_lossy().into_owned()).unwrap_or_default();
                    art.entry = Some(name.clone());
                    if files::is_markdown(p) {
                        art.kind = Kind::Markdown;
                        fs::read_to_string(p).map(|text| markdown = Some(text)).map_err(Into::into)
                    } else {
                        art.kind = Kind::Html;
                        self.hasher.hash(p).map(|sha| {
                            art.files.insert(name, sha);
                        })
                    }
                };
                match (read, prev) {
                    (Ok(()), _) => {
                        self.warned.remove(&t.id);
                    }
                    // The source is gone (say, a cleaned-up worktree). That isn't a delete: keep
                    // the last synced copy, here and everywhere else.
                    (Err(e), Some(prev)) => {
                        self.warn(&t.id, &format!("can't read {path} ({e}); keeping the last synced copy"));
                        art.kind = prev.art.kind;
                        art.entry = prev.art.entry.clone();
                        art.files = prev.art.files.clone();
                        markdown = prev.markdown.clone();
                    }
                    (Err(e), None) => {
                        self.warn(&t.id, &format!("not syncing {:?}: can't read {path} ({e})", t.title));
                        return Ok(None);
                    }
                }
            }
            _ => return Ok(None), // other kinds (proxies) don't sync
        }
        // Only artifacts served from files have a store; URL artifacts have none.
        let store = match self.stores.get(&t.id) {
            _ if art.kind == Kind::Url => Value::Object(Map::new()),
            Some(store) => store.clone(),
            None => {
                let store = self.tv.store(&t.id).await?;
                self.stores.insert(t.id.clone(), store.clone());
                store
            }
        };
        Ok(Some(ArtState { local_id: t.id.clone(), path: t.path.clone(), art, store, markdown }))
    }

    // ── Step 2: local edits → Loro ──────────────────────────────────────────────────────────

    /// Record what changed here since `applied`. Returns the artifacts that couldn't be recorded
    /// yet (their files aren't on the relay); they're retried next tick.
    async fn record(&mut self, sp: &mut Space, local: &State) -> Result<HashSet<String>> {
        let applied = sp.applied.state.clone();
        let doc = sp.rooms.doc(SPACE_ROOM);
        let mut pending = HashSet::new();

        // Files first: a hash goes into the space doc only once its contents are on the relay,
        // or other machines would see a file they can't download.
        for (sid, l) in &local.artifacts {
            let old = applied.artifacts.get(sid).map(|a| &a.art.files);
            let new_files: Vec<_> =
                l.art.files.iter().filter(|(p, sha)| old.and_then(|o| o.get(*p)) != Some(*sha)).collect();
            if new_files.is_empty() {
                continue;
            }
            if let Err(e) = upload(sp, l, &new_files).await {
                if matches!(sp.link, Link::Online) {
                    self.warn(&format!("upload {sid}"), &format!("{:?}: can't upload files yet ({e:#})", l.art.title));
                }
                pending.insert(sid.clone());
            }
        }

        for (cid, l) in &local.channels {
            let Some(old) = applied.channels.get(cid) else { continue };
            if l.name != old.name {
                model::rename_channel(&doc, cid, &l.name)?;
                say(sp, format!("↑ channel renamed to {:?}", l.name));
            }
            if l.order != old.order {
                model::reorder(&doc, cid, &old.order, &l.order)?;
                say(sp, format!("↑ order of {:?}", l.name));
            }
        }

        for (sid, l) in &local.artifacts {
            if pending.contains(sid) {
                continue;
            }
            let Some(old) = applied.artifacts.get(sid) else {
                model::put_artifact(&doc, sid, &l.art, &sp.entry.device_id)?;
                let art_doc = sp.rooms.create(&artifact_room(sid));
                model::write_store(&art_doc, &json!({}), &l.store)?;
                if let Some(text) = &l.markdown {
                    model::write_markdown(&art_doc, text)?;
                }
                say(sp, format!("↑ new artifact {:?}", l.art.title));
                continue;
            };
            let mut what = changed_fields(&old.art, &l.art);
            if l.art != old.art {
                model::update_artifact(&doc, sid, &old.art, &l.art)?;
            }
            if l.store != old.store || l.markdown != old.markdown {
                let art_doc = sp.rooms.doc(&artifact_room(sid));
                if l.store != old.store {
                    model::write_store(&art_doc, &old.store, &l.store)?;
                    what.push("store");
                }
                if let (Some(text), true) = (&l.markdown, l.markdown != old.markdown) {
                    model::write_markdown(&art_doc, text)?;
                    what.push("text");
                }
            }
            if !what.is_empty() {
                say(sp, format!("↑ {:?}: {}", l.art.title, what.join(", ")));
            }
        }

        for (sid, old) in &applied.artifacts {
            // Gone from a channel that still exists here: deleted (or moved out of the space).
            // Gone because its whole channel vanished: that channel gets recreated instead.
            if !local.artifacts.contains_key(sid) && local.channels.contains_key(&old.art.channel_id) {
                model::delete_artifact(&doc, sid)?;
                say(sp, format!("↑ deleted {:?}", old.art.title));
            }
        }
        Ok(pending)
    }

    // ── Step 3: Loro → Television ───────────────────────────────────────────────────────────

    async fn project(
        &mut self,
        sp: &mut Space,
        local: &State,
        remote: &Remote,
        pending: &HashSet<String>,
        now: &TvNow,
    ) -> Result<State> {
        let applied = sp.applied.state.clone();
        let mut next = State::default();
        let mut relayout = false;

        for (cid, r) in &remote.space.channels {
            let local_id = match local.channels.get(cid) {
                Some(l) => {
                    if l.name != r.name {
                        self.tv.rename_channel(&l.local_id, &r.name).await?;
                        say(sp, format!("↓ channel renamed to {:?}", r.name));
                    }
                    l.local_id.clone()
                }
                None => {
                    let id = self.tv.create_channel(&r.name).await?;
                    say(sp, format!("↓ new channel {:?}", r.name));
                    relayout = true;
                    id
                }
            };
            next.channels.insert(cid.clone(), ChanState { local_id, name: r.name.clone(), order: vec![] });
        }
        for (cid, l) in &local.channels {
            if !remote.space.channels.contains_key(cid) {
                say(sp, format!("channel {:?} left the space; it stays here, unsynced", l.name));
            }
        }

        // Artifacts that link to another artifact go last, so the one they link to exists here.
        let mut order: Vec<(&String, &Art)> = remote.space.artifacts.iter().collect();
        order.sort_by_key(|(_, r)| r.url.as_deref().is_some_and(links::is_shared));
        for (sid, r) in order {
            let channel = next.channels[&r.channel_id].local_id.clone();
            let l = local.artifacts.get(sid);
            let (Some((store, markdown)), false) = (remote.docs.get(sid), pending.contains(sid)) else {
                // Not settled yet (files still uploading, or its doc still arriving): leave
                // Television as it is and remember what it holds.
                if let Some(a) = applied.artifacts.get(sid) {
                    next.artifacts.insert(sid.clone(), a.clone());
                }
                continue;
            };
            let want = ArtState {
                local_id: String::new(),
                path: None,
                art: r.clone(),
                store: store.clone(),
                markdown: markdown.clone(),
            };
            let local_ids: HashMap<&str, &str> =
                next.artifacts.iter().map(|(s, a)| (s.as_str(), a.local_id.as_str())).collect();
            let url = r.url.as_deref().map(|u| match self.tv_port {
                Some(port) => links::to_local(u, port, |s| local_ids.get(s).map(|id| id.to_string())),
                None => Ok(u.to_string()),
            });
            let result = match url.transpose() {
                Err(e) => Err(e),
                Ok(url) => match l {
                    Some(l) => self.update_here(sp, l, &want, url.as_deref(), &channel, &mut relayout).await,
                    None => self.create_here(sp, sid, &want, url.as_deref(), &channel, &mut relayout).await,
                },
            };
            match result {
                Ok(st) => {
                    next.artifacts.insert(sid.clone(), st);
                }
                Err(e) => {
                    say(sp, format!("can't apply {:?} here yet, will retry: {e:#}", r.title));
                    // Remember what Television really holds, so the gap reads as "Loro has a
                    // change to apply", never as "someone edited this here".
                    if let Some(l) = l {
                        next.artifacts.insert(sid.clone(), l.clone());
                    }
                }
            }
        }

        for (sid, l) in &local.artifacts {
            if remote.space.deleted.contains(sid) {
                match self.tv.delete_artifact(&l.local_id).await {
                    Ok(()) => {
                        say(sp, format!("↓ deleted {:?}", l.art.title));
                        relayout = true;
                    }
                    Err(e) => {
                        say(sp, format!("can't delete {:?} here yet: {e:#}", l.art.title));
                        next.artifacts.insert(sid.clone(), l.clone());
                    }
                }
            }
        }

        let fresh;
        let channels = if relayout {
            fresh = self.tv.channels().await?;
            &fresh
        } else {
            &now.channels
        };
        self.project_order(sp, remote, channels, &mut next).await;
        Ok(next)
    }

    /// Put each channel's artifacts in the space's order, then record the order Television
    /// ends up with (exactly what the next read will see).
    async fn project_order(&self, sp: &Space, remote: &Remote, channels: &[tv::Channel], next: &mut State) {
        let State { channels: next_channels, artifacts } = next;
        let shared_of: HashMap<&str, &str> =
            artifacts.iter().map(|(sid, a)| (a.local_id.as_str(), sid.as_str())).collect();
        for (cid, ch) in next_channels.iter_mut() {
            let Some(tch) = channels.iter().find(|c| c.id == ch.local_id) else { continue };
            let mut current: Vec<String> =
                tch.layout.iter().flat_map(|slot| slot.artifact_ids.iter().cloned()).collect();
            let on_channel = |sid: &String| artifacts.get(sid).is_some_and(|a| a.art.channel_id == *cid);
            let mut want: Vec<String> =
                remote.space.channels[cid].order.iter().filter(|s| on_channel(s)).cloned().collect();
            let missing: Vec<String> =
                artifacts.keys().filter(|s| on_channel(s) && !want.contains(s)).cloned().collect();
            want.extend(missing);
            let mut want_local: Vec<String> = want
                .iter()
                .filter_map(|sid| artifacts.get(sid).map(|a| a.local_id.clone()))
                .filter(|id| current.contains(id))
                .collect();
            let rest: Vec<String> = current.iter().filter(|id| !want_local.contains(id)).cloned().collect();
            want_local.extend(rest);

            let mut moved = false;
            for i in 0..want_local.len() {
                if current[i] == want_local[i] {
                    continue;
                }
                if let Err(e) = self.tv.set_position(&want_local[i], i + 1).await {
                    say(sp, format!("can't reorder {:?}: {e:#}", ch.name));
                    break;
                }
                let from = current.iter().position(|id| *id == want_local[i]).unwrap_or(i);
                let id = current.remove(from);
                current.insert(i, id);
                moved = true;
            }
            if moved {
                say(sp, format!("↓ order of {:?}", ch.name));
            }
            ch.order = current
                .iter()
                .map(|id| shared_of.get(id.as_str()).map_or_else(|| id.clone(), |s| s.to_string()))
                .collect();
        }
    }

    async fn update_here(
        &mut self,
        sp: &Space,
        l: &ArtState,
        want: &ArtState,
        url: Option<&str>,
        channel: &str,
        relayout: &mut bool,
    ) -> Result<ArtState> {
        let (id, old, new) = (&l.local_id, &l.art, &want.art);
        // Download first: if the network fails, nothing here has changed yet.
        let blobs = if new.files != old.files { fetch(sp, &old.files, &new.files).await? } else { vec![] };
        let mut what = Vec::new();
        if new.title != old.title {
            self.tv.update_artifact(id, "title", &new.title).await?;
            what.push("title");
        }
        if let (Some(url), true) = (url, new.url != old.url) {
            self.tv.update_artifact(id, "url", url).await?;
            what.push("url");
        }
        if new.channel_id != old.channel_id {
            self.tv.move_artifact(id, channel).await?;
            *relayout = true;
            what.push("channel");
        }
        if new.files != old.files {
            write_files(l.path.as_deref().context("artifact has no path")?, new, &old.files, &blobs)?;
            what.push("files");
        }
        if let (Some(text), Some(path), true) = (&want.markdown, &l.path, want.markdown != l.markdown) {
            files::write_in_place(Path::new(path), text.as_bytes())?;
            what.push("text");
        }
        if (new.size, new.full_screen) != (old.size, old.full_screen) {
            self.tv.resize(id, new.size, new.full_screen).await?;
            what.push("size");
        }
        if want.store != l.store {
            store_into_tv(&self.tv, id, &l.store, &want.store).await?;
            self.stores.remove(id);
            what.push("store");
        }
        if !what.is_empty() {
            say(sp, format!("↓ {:?}: {}", new.title, what.join(", ")));
        }
        Ok(ArtState { local_id: id.clone(), path: l.path.clone(), ..want.clone() })
    }

    /// All or nothing: if any step fails, the half-made artifact is removed and the whole
    /// creation is retried next tick, so it can't be mistaken for a new artifact made here.
    async fn create_here(
        &mut self,
        sp: &Space,
        sid: &str,
        want: &ArtState,
        url: Option<&str>,
        channel: &str,
        relayout: &mut bool,
    ) -> Result<ArtState> {
        let art = &want.art;
        let dir = self.home.dir.join("files").join(sid);
        let path: Option<String> = match (art.kind, &art.entry) {
            (Kind::Url, _) => None,
            (_, Some(entry)) => Some(files::safe_join(&dir, entry)?.display().to_string()),
            (_, None) => Some(format!("{}/", dir.display())),
        };
        // Files go in place first: Television only registers paths that exist.
        if let Some(p) = &path {
            let blobs = fetch(sp, &BTreeMap::new(), &art.files).await?;
            fs::create_dir_all(&dir)?;
            write_files(p, art, &BTreeMap::new(), &blobs)?;
            if let Some(text) = &want.markdown {
                files::write_in_place(Path::new(p), text.as_bytes())?;
            }
        }
        let id = match (&path, url) {
            (Some(p), _) => self.tv.create_path_artifact(channel, &art.title, p).await?,
            (None, Some(url)) => self.tv.create_url_artifact(channel, &art.title, url).await?,
            (None, None) => bail!("URL artifact without a URL"),
        };
        *relayout = true;
        let finish = async {
            self.tv.resize(&id, art.size, art.full_screen).await?;
            if want.store.as_object().is_some_and(|o| !o.is_empty()) {
                self.tv.store_set_root(&id, &want.store).await?;
            }
            anyhow::Ok(())
        };
        if let Err(e) = finish.await {
            let _ = self.tv.delete_artifact(&id).await;
            return Err(e);
        }
        say(sp, format!("↓ new artifact {:?}", art.title));
        Ok(ArtState { local_id: id, path, ..want.clone() })
    }
}

/// Read the merged Loro state. Joins rooms for artifacts seen for the first time.
fn read_remote(sp: &mut Space) -> Remote {
    let space = model::read_space(&sp.rooms.doc(SPACE_ROOM));
    let mut docs = BTreeMap::new();
    for (sid, art) in &space.artifacts {
        let room = artifact_room(sid);
        let doc = sp.rooms.doc(&room);
        if sp.rooms.is_ready(&room) {
            let markdown = (art.kind == Kind::Markdown).then(|| model::read_markdown(&doc));
            docs.insert(sid.clone(), (model::read_store(&doc), markdown));
        }
    }
    Remote { space, docs }
}

/// `applied` after recording: what's in Television now, except artifacts we couldn't record
/// (those keep their old applied entry, so they're tried again).
fn recorded(local: &State, applied: &State, pending: &HashSet<String>) -> State {
    let mut state = local.clone();
    for sid in pending {
        match applied.artifacts.get(sid) {
            Some(old) => state.artifacts.insert(sid.clone(), old.clone()),
            None => state.artifacts.remove(sid),
        };
    }
    state
}

fn changed_fields(old: &Art, new: &Art) -> Vec<&'static str> {
    let mut what = Vec::new();
    if old.title != new.title {
        what.push("title");
    }
    if old.url != new.url {
        what.push("url");
    }
    if old.channel_id != new.channel_id {
        what.push("channel");
    }
    if old.files != new.files {
        what.push("files");
    }
    if (old.size, old.full_screen) != (new.size, new.full_screen) {
        what.push("size");
    }
    what
}

/// Where file `rel` of an artifact lives, given its local path.
fn file_path(base: &str, art: &Art, rel: &str) -> Result<PathBuf> {
    match &art.entry {
        Some(_) => Ok(PathBuf::from(base)),
        None => files::safe_join(Path::new(base), rel),
    }
}

async fn upload(sp: &mut Space, st: &ArtState, new_files: &[(&String, &String)]) -> Result<()> {
    if !matches!(sp.link, Link::Online) {
        bail!("offline");
    }
    let base = st.path.as_deref().context("artifact has no path")?;
    for (rel, sha) in new_files {
        if sp.uploaded.contains(*sha) {
            continue;
        }
        if !sp.relay.has_blob(&sp.id, &sp.entry.token, sha).await? {
            let path = file_path(base, &st.art, rel)?;
            let bytes = fs::read(&path)?;
            if files::sha256_hex(&bytes) != **sha {
                bail!("{} changed while uploading", path.display());
            }
            sp.relay.put_blob(&sp.id, &sp.entry.token, sha, bytes).await?;
        }
        sp.uploaded.insert((*sha).clone());
    }
    Ok(())
}

/// Download the files that differ, each checked against its hash.
async fn fetch(
    sp: &Space,
    old: &BTreeMap<String, String>,
    new: &BTreeMap<String, String>,
) -> Result<Vec<(String, Vec<u8>)>> {
    let mut blobs = Vec::new();
    for (rel, sha) in new {
        if old.get(rel) == Some(sha) {
            continue;
        }
        let bytes = sp.relay.get_blob(&sp.id, &sp.entry.token, sha).await?;
        if files::sha256_hex(&bytes) != *sha {
            bail!("blob for {rel} doesn't match its hash");
        }
        blobs.push((rel.clone(), bytes));
    }
    Ok(blobs)
}

fn write_files(base: &str, art: &Art, old: &BTreeMap<String, String>, blobs: &[(String, Vec<u8>)]) -> Result<()> {
    for (rel, bytes) in blobs {
        files::write_in_place(&file_path(base, art, rel)?, bytes)?;
    }
    if art.entry.is_none() {
        for rel in old.keys().filter(|r| !art.files.contains_key(*r)) {
            let _ = fs::remove_file(files::safe_join(Path::new(base), rel)?);
        }
    }
    Ok(())
}

/// Write the difference between two store snapshots into Television, path by path, so a key
/// written meanwhile by the page itself isn't overwritten.
async fn store_into_tv(tv: &Tv, id: &str, old: &Value, new: &Value) -> Result<()> {
    let mut sets = Map::new();
    let mut removes = Vec::new();
    json_diff("", old, new, &mut sets, &mut removes);
    if sets.contains_key("") {
        return tv.store_set_root(id, new).await;
    }
    if !sets.is_empty() {
        tv.store_update(id, &sets).await?;
    }
    for path in removes {
        tv.store_remove(id, &path).await?;
    }
    Ok(())
}

fn json_diff(at: &str, old: &Value, new: &Value, sets: &mut Map<String, Value>, removes: &mut Vec<String>) {
    let join = |k: &str| if at.is_empty() { k.to_string() } else { format!("{at}/{k}") };
    match (old, new) {
        (Value::Object(o), Value::Object(n)) => {
            for (k, v) in n {
                match o.get(k) {
                    Some(ov) if ov == v => {}
                    Some(ov) => json_diff(&join(k), ov, v, sets, removes),
                    None => {
                        sets.insert(join(k), v.clone());
                    }
                }
            }
            removes.extend(o.keys().filter(|k| !n.contains_key(*k)).map(|k| join(k)));
        }
        _ => {
            sets.insert(at.to_string(), new.clone());
        }
    }
}
