//! The Loro docs of one space and the Loro Protocol handshake that keeps them in step with the
//! relay. One room per doc: `space` for the space doc, `art-<sharedId>` per artifact.
//!
//! Joining is symmetric. We send our version; the relay replies with its version and backfills
//! what we lack; we send `export(updates from relay's version)`, i.e. what the relay lacks. That
//! second half is what the published Rust client skipped, so offline edits never reached the relay.

use std::{
    collections::BTreeMap,
    fs,
    path::PathBuf,
    sync::{Arc, Mutex},
};

use anyhow::{Context, Result};
use loro::{ExportMode, LoroDoc, Subscription, VersionVector};
use loro_protocol::{CrdtType, JoinErrorCode, ProtocolMessage, RoomErrorCode, UpdateStatusCode};

use crate::{
    state::write_atomic,
    wire::{Outbox, batch_id},
};

pub const SPACE_ROOM: &str = "space";

pub fn artifact_room(shared_id: &str) -> String {
    format!("art-{shared_id}")
}

struct Room {
    doc: LoroDoc,
    joined: bool,
    /// Holds everything the relay had when we joined (or we made it). Until then its contents
    /// mustn't be written into Television: an empty doc would read as "the store is empty".
    ready: bool,
    /// The relay's version from the join reply, while the backfill is still on its way.
    waiting_for: Option<VersionVector>,
    /// Updates from the relay, not imported yet. See `import_inbox`.
    inbox: Vec<Vec<u8>>,
    /// Local commits not sent yet. Filled by Loro's local-update callback, drained by `flush`.
    unsent: Arc<Mutex<Vec<Vec<u8>>>>,
    _sub: Subscription,
    saved: VersionVector,
}

pub struct Rooms {
    dir: PathBuf,
    token: String,
    rooms: BTreeMap<String, Room>,
    outbox: Option<Outbox>,
}

#[derive(Debug, PartialEq)]
pub enum Outcome {
    Nothing,
    /// Remote changes were imported.
    Changed,
    /// The relay refused this device's token: it was revoked.
    Refused(String),
}

impl Rooms {
    pub fn open(dir: PathBuf, token: String) -> Result<Self> {
        fs::create_dir_all(&dir)?;
        let mut rooms = Self { dir, token, rooms: BTreeMap::new(), outbox: None };
        for entry in fs::read_dir(&rooms.dir)? {
            let path = entry?.path();
            if path.extension().is_some_and(|e| e == "loro") {
                let name = path.file_stem().unwrap_or_default().to_string_lossy().into_owned();
                let doc =
                    LoroDoc::from_snapshot(&fs::read(&path)?).with_context(|| format!("loading {}", path.display()))?;
                rooms.insert(name, doc, true);
            }
        }
        Ok(rooms)
    }

    fn insert(&mut self, name: String, doc: LoroDoc, ready: bool) {
        let unsent = Arc::new(Mutex::new(Vec::new()));
        let sink = unsent.clone();
        let sub = doc.subscribe_local_update(Box::new(move |bytes| {
            sink.lock().unwrap().push(bytes.clone());
            true
        }));
        let saved = doc.oplog_vv();
        self.rooms.insert(
            name,
            Room { doc, joined: false, ready, waiting_for: None, inbox: Vec::new(), unsent, _sub: sub, saved },
        );
    }

    /// The room's doc. A room seen for the first time starts empty and is joined right away,
    /// so the relay backfills it.
    pub fn doc(&mut self, room: &str) -> LoroDoc {
        if !self.rooms.contains_key(room) {
            self.insert(room.to_string(), LoroDoc::new(), false);
            self.join(room);
        }
        self.rooms[room].doc.clone()
    }

    /// A room this machine creates (a newly shared artifact): ready at once, nothing to wait for.
    pub fn create(&mut self, room: &str) -> LoroDoc {
        if !self.rooms.contains_key(room) {
            self.insert(room.to_string(), LoroDoc::new(), true);
            self.join(room);
        }
        self.rooms[room].doc.clone()
    }

    pub fn is_ready(&self, room: &str) -> bool {
        self.rooms.get(room).is_some_and(|r| r.ready)
    }

    pub fn commit(&self) {
        for r in self.rooms.values() {
            r.doc.commit();
        }
    }

    pub fn connected(&mut self, outbox: Outbox) {
        self.outbox = Some(outbox);
        let names: Vec<String> = self.rooms.keys().cloned().collect();
        for name in names {
            self.join(&name);
        }
    }

    pub fn disconnected(&mut self) {
        self.outbox = None;
        for room in self.rooms.values_mut() {
            room.joined = false;
        }
    }

    fn join(&mut self, room: &str) {
        let (Some(outbox), Some(r)) = (&self.outbox, self.rooms.get_mut(room)) else { return };
        r.joined = false;
        let _ = outbox.send(ProtocolMessage::JoinRequest {
            crdt: CrdtType::Loro,
            room_id: room.to_string(),
            auth: self.token.as_bytes().to_vec(),
            version: r.doc.oplog_vv().encode(),
        });
    }

    fn send_update(&self, room: &str, bytes: Vec<u8>) {
        if let Some(outbox) = &self.outbox {
            let _ = outbox.send(ProtocolMessage::DocUpdate {
                crdt: CrdtType::Loro,
                room_id: room.to_string(),
                updates: vec![bytes],
                batch_id: batch_id(),
            });
        }
    }

    pub fn handle(&mut self, msg: ProtocolMessage) -> Result<Outcome> {
        match msg {
            ProtocolMessage::JoinResponseOk { room_id, version, .. } => {
                let Some(r) = self.rooms.get_mut(&room_id) else { return Ok(Outcome::Nothing) };
                r.joined = true;
                // Anything committed before now is covered by this export.
                r.unsent.lock().unwrap().clear();
                let relay_has = VersionVector::decode(&version).unwrap_or_default();
                if !r.ready {
                    if r.doc.oplog_vv().includes_vv(&relay_has) {
                        r.ready = true;
                    } else {
                        r.waiting_for = Some(relay_has.clone());
                    }
                }
                if !relay_has.includes_vv(&r.doc.oplog_vv()) {
                    let missing = r.doc.export(ExportMode::updates(&relay_has))?;
                    self.send_update(&room_id, missing);
                }
            }
            ProtocolMessage::JoinError { room_id, code, message, .. } => {
                if code == JoinErrorCode::AuthFailed {
                    return Ok(Outcome::Refused(message));
                }
                eprintln!("relay refused room {room_id}: {message}");
            }
            ProtocolMessage::DocUpdate { room_id, updates, .. } => {
                let Some(r) = self.rooms.get_mut(&room_id) else { return Ok(Outcome::Nothing) };
                r.inbox.extend(updates);
                return Ok(Outcome::Changed);
            }
            ProtocolMessage::Ack { room_id, status, .. } => match status {
                UpdateStatusCode::Ok => {}
                // The handshake resends whatever the relay is missing.
                UpdateStatusCode::FragmentTimeout | UpdateStatusCode::PermissionDenied => self.join(&room_id),
                other => eprintln!("relay rejected an update in {room_id}: {other:?}"),
            },
            ProtocolMessage::RoomError { room_id, code, message, .. } => {
                if code == RoomErrorCode::RejoinSuggested {
                    self.join(&room_id);
                } else {
                    eprintln!("relay closed room {room_id}: {message}");
                    if let Some(r) = self.rooms.get_mut(&room_id) {
                        r.joined = false;
                    }
                }
            }
            _ => {}
        }
        Ok(Outcome::Nothing)
    }

    /// Import what the relay sent since the last call.
    ///
    /// The engine calls this only after recording local edits. A local edit is recorded as the
    /// difference between what Television showed after the last sync and what it shows now, so
    /// the doc must still hold exactly what Television showed. A remote change imported earlier
    /// would read as part of the edit's starting point, and recording would undo it: Television's
    /// file doesn't have the remote line yet, so the text diff deletes it.
    pub fn import_inbox(&mut self) {
        let mut rejoin = Vec::new();
        for (name, r) in &mut self.rooms {
            if r.inbox.is_empty() {
                continue;
            }
            let updates = std::mem::take(&mut r.inbox);
            match r.doc.import_batch(&updates) {
                // These build on ops we never got. Rejoining makes the relay send them.
                Ok(status) if status.pending.is_some() => rejoin.push(name.clone()),
                Ok(_) => {}
                Err(e) => eprintln!("can't import an update into {name}: {e}"),
            }
            if r.waiting_for.as_ref().is_some_and(|w| r.doc.oplog_vv().includes_vv(w)) {
                r.ready = true;
                r.waiting_for = None;
            }
        }
        for name in rejoin {
            self.join(&name);
        }
    }

    /// Commit pending edits, send local commits to joined rooms, and save docs that changed.
    /// Commits made while a room isn't joined are dropped here: the next join handshake sends them.
    pub fn flush(&mut self) -> Result<()> {
        let mut out = Vec::new();
        for (name, r) in &mut self.rooms {
            r.doc.commit();
            let unsent = std::mem::take(&mut *r.unsent.lock().unwrap());
            if r.joined {
                out.extend(unsent.into_iter().map(|u| (name.clone(), u)));
            }
            let vv = r.doc.oplog_vv();
            if vv != r.saved {
                let snapshot = r.doc.export(ExportMode::Snapshot)?;
                write_atomic(&self.dir.join(format!("{name}.loro")), &snapshot)?;
                r.saved = vv;
            }
        }
        for (room, bytes) in out {
            self.send_update(&room, bytes);
        }
        Ok(())
    }
}
