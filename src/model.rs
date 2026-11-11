//! The shared data model, and the only code that writes into Loro docs.
//!
//! Space doc (room `space`):
//!   channels:     Map<channelId, Map { name, order: MovableList<sharedId> }>
//!   channelOrder: MovableList<channelId>
//!   artifacts:    Map<sharedId, Map { channelId, title, kind, url?, entry?,
//!                                     files: Map<path, sha256>, size, fullScreen, origin, deleted? }>
//! Artifact doc (room `art-<sharedId>`):
//!   store:    Map. Objects become nested *mergeable* Map containers, so two machines writing
//!             different keys (two comments) both survive, even when both created the parent
//!             map. Arrays are plain values: replaced whole, like Television.
//!
//! Every child container is created with `ensure_mergeable_*`: its ID comes from (parent, key,
//! type), so two machines creating it offline get the same container and their edits merge.
//! `insert_container` would give each its own container, and the key's last writer would hide
//! the other one's contents.
//!   markdown: Text (markdown artifacts only), merged character by character.

use std::collections::{BTreeMap, BTreeSet, HashMap};

use anyhow::{Result, anyhow};
use loro::{LoroDoc, LoroMap, LoroMovableList, LoroValue, ToJson, UpdateOptions};
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};

use crate::tv::Size;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Kind {
    Html,
    Markdown,
    Url,
}

/// An artifact's metadata, as the space doc holds it. Its store and markdown live in its own doc.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Art {
    pub channel_id: String,
    pub title: String,
    pub kind: Kind,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub url: Option<String>,
    /// The file name for single-file artifacts; `None` for a folder with an index.html.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub entry: Option<String>,
    /// Path inside the artifact → sha256 of its contents. The contents live in R2.
    #[serde(default)]
    pub files: BTreeMap<String, String>,
    pub size: Size,
    #[serde(default)]
    pub full_screen: bool,
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct Chan {
    pub name: String,
    #[serde(default)]
    pub order: Vec<String>,
}

/// The space doc read as plain values: the channels in the space, and the live artifacts on them.
#[derive(Debug, Default)]
pub struct Space {
    pub channels: BTreeMap<String, Chan>,
    pub artifacts: BTreeMap<String, Art>,
    pub deleted: BTreeSet<String>,
}

pub fn read_space(doc: &LoroDoc) -> Space {
    let root = doc.get_deep_value().to_json_value();
    let mut space = Space::default();
    for (id, v) in entries(&root["channels"]) {
        if let Ok(mut ch) = serde_json::from_value::<Chan>(v.clone()) {
            // Two machines inserting the same ID concurrently leaves it in the list twice.
            let mut seen = BTreeSet::new();
            ch.order.retain(|a| seen.insert(a.clone()));
            space.channels.insert(id.clone(), ch);
        }
    }
    for (id, v) in entries(&root["artifacts"]) {
        if v["deleted"] == Value::Bool(true) {
            space.deleted.insert(id.clone());
            continue;
        }
        match serde_json::from_value::<Art>(v.clone()) {
            Ok(art) if space.channels.contains_key(&art.channel_id) => {
                space.artifacts.insert(id.clone(), art);
            }
            Ok(_) => {} // its channel left the space
            Err(e) => eprintln!("skipping artifact {id}: {e}"),
        }
    }
    space
}

fn entries(v: &Value) -> impl Iterator<Item = (&String, &Value)> {
    v.as_object().into_iter().flatten()
}

// ── Space doc writes ────────────────────────────────────────────────────────────────────────

pub fn add_channel(doc: &LoroDoc, id: &str, name: &str) -> Result<()> {
    let ch = doc.get_map("channels").ensure_mergeable_map(id)?;
    ch.insert("name", name)?;
    ch.ensure_mergeable_movable_list("order")?;
    let order = doc.get_movable_list("channelOrder");
    if !strings(&order).iter().any(|c| c == id) {
        order.push(id)?;
    }
    Ok(())
}

pub fn remove_channel(doc: &LoroDoc, id: &str) -> Result<()> {
    doc.get_map("channels").delete(id)?;
    let order = doc.get_movable_list("channelOrder");
    while let Some(i) = strings(&order).iter().position(|c| c == id) {
        order.delete(i, 1)?;
    }
    Ok(())
}

pub fn rename_channel(doc: &LoroDoc, id: &str, name: &str) -> Result<()> {
    if let Some(ch) = child_map(&doc.get_map("channels"), id) {
        ch.insert("name", name)?;
    }
    Ok(())
}

/// Record a local reorder as the smallest change: `old` and `new` are this machine's order
/// before and after. Items that kept their relative order (the longest such run) aren't touched;
/// only the others move, each to just after its new left neighbour. Writing the whole order
/// instead would also re-assert the position of everything else, undoing another machine's
/// concurrent move. Items in the list that this machine doesn't know stay where they are.
pub fn reorder(doc: &LoroDoc, channel: &str, old: &[String], new: &[String]) -> Result<()> {
    let Some(ch) = child_map(&doc.get_map("channels"), channel) else { return Ok(()) };
    let list = ch.ensure_mergeable_movable_list("order")?;
    for id in old.iter().filter(|id| !new.contains(id)) {
        while let Some(i) = strings(&list).iter().position(|a| a == id) {
            list.delete(i, 1)?;
        }
    }
    let kept = kept_in_place(old, new);
    for (i, id) in new.iter().enumerate() {
        if kept.contains(id) {
            continue;
        }
        let items = strings(&list);
        let target = new[..i].iter().rev().find_map(|n| items.iter().position(|a| a == n)).map_or(0, |p| p + 1);
        match items.iter().position(|a| a == id) {
            // Taking it out first shifts everything after it left by one.
            Some(from) => {
                let to = if from < target { target - 1 } else { target };
                if to != from {
                    list.mov(from, to)?;
                }
            }
            None => list.insert(target, id.as_str())?,
        }
    }
    Ok(())
}

/// The items of `new` that kept the relative order they had in `old`: a longest increasing run
/// of their old positions. O(n²), fine for the artifacts on one channel.
fn kept_in_place(old: &[String], new: &[String]) -> BTreeSet<String> {
    let at: Vec<(usize, &String)> =
        new.iter().filter_map(|id| old.iter().position(|o| o == id).map(|p| (p, id))).collect();
    let mut len = vec![1; at.len()];
    let mut prev = vec![None; at.len()];
    for i in 0..at.len() {
        for j in 0..i {
            if at[j].0 < at[i].0 && len[j] + 1 > len[i] {
                len[i] = len[j] + 1;
                prev[i] = Some(j);
            }
        }
    }
    let mut kept = BTreeSet::new();
    let mut i = (0..at.len()).max_by_key(|&i| len[i]);
    while let Some(k) = i {
        kept.insert(at[k].1.clone());
        i = prev[k];
    }
    kept
}

pub fn put_artifact(doc: &LoroDoc, id: &str, art: &Art, origin: &str) -> Result<()> {
    let m = doc.get_map("artifacts").ensure_mergeable_map(id)?;
    m.insert("origin", origin)?;
    write_art(&m, None, art)
}

pub fn update_artifact(doc: &LoroDoc, id: &str, old: &Art, new: &Art) -> Result<()> {
    match child_map(&doc.get_map("artifacts"), id) {
        Some(m) => write_art(&m, Some(old), new),
        None => Ok(()),
    }
}

/// A tombstone, not a removal: a concurrent edit to the artifact can't bring it back, and the
/// record stays so the delete could be undone.
pub fn delete_artifact(doc: &LoroDoc, id: &str) -> Result<()> {
    if let Some(m) = child_map(&doc.get_map("artifacts"), id) {
        m.insert("deleted", true)?;
    }
    Ok(())
}

/// Write only the fields that changed since `old`, so a concurrent change to another field on
/// another machine survives the merge.
fn write_art(m: &LoroMap, old: Option<&Art>, new: &Art) -> Result<()> {
    let changed = |f: &dyn Fn(&Art) -> Value| old.map(f) != Some(f(new));
    if changed(&|a| a.channel_id.as_str().into()) {
        m.insert("channelId", new.channel_id.as_str())?;
    }
    if changed(&|a| a.title.as_str().into()) {
        m.insert("title", new.title.as_str())?;
    }
    if changed(&|a| serde_json::to_value(a.kind).unwrap_or_default()) {
        m.insert("kind", to_loro(&serde_json::to_value(new.kind)?))?;
    }
    if changed(&|a| a.url.clone().into()) {
        match &new.url {
            Some(url) => m.insert("url", url.as_str())?,
            None => m.delete("url")?,
        }
    }
    if changed(&|a| a.entry.clone().into()) {
        match &new.entry {
            Some(entry) => m.insert("entry", entry.as_str())?,
            None => m.delete("entry")?,
        }
    }
    if changed(&|a| serde_json::to_value(a.size).unwrap_or_default()) {
        m.insert("size", to_loro(&serde_json::to_value(new.size)?))?;
    }
    if changed(&|a| a.full_screen.into()) {
        m.insert("fullScreen", new.full_screen)?;
    }
    let files = m.ensure_mergeable_map("files")?;
    let none = BTreeMap::new();
    let old_files = old.map_or(&none, |o| &o.files);
    for (path, sha) in &new.files {
        if old_files.get(path) != Some(sha) {
            files.insert(path, sha.as_str())?;
        }
    }
    for path in old_files.keys().filter(|p| !new.files.contains_key(*p)) {
        files.delete(path)?;
    }
    Ok(())
}

// ── Artifact doc ────────────────────────────────────────────────────────────────────────────

pub fn read_store(doc: &LoroDoc) -> Value {
    match doc.get_map("store").get_deep_value().to_json_value() {
        v @ Value::Object(_) => v,
        _ => Value::Object(Map::new()),
    }
}

pub fn read_markdown(doc: &LoroDoc) -> String {
    doc.get_text("markdown").to_string()
}

/// Apply the change from `old` to `new` (two snapshots of the store) as the smallest set of key
/// writes and deletes.
pub fn write_store(doc: &LoroDoc, old: &Value, new: &Value) -> Result<()> {
    let empty = Map::new();
    let as_obj = |v: &'_ Value| -> Map<String, Value> { v.as_object().unwrap_or(&empty).clone() };
    merge_object(&doc.get_map("store"), &as_obj(old), &as_obj(new))
}

fn merge_object(map: &LoroMap, old: &Map<String, Value>, new: &Map<String, Value>) -> Result<()> {
    for (key, value) in new {
        if old.get(key) == Some(value) {
            continue;
        }
        match value {
            Value::Object(fields) => {
                let child = match map.ensure_mergeable_map(key) {
                    Ok(child) => child,
                    // The key holds a plain value (it was a string, say): replace it.
                    Err(_) => {
                        map.delete(key)?;
                        map.ensure_mergeable_map(key)?
                    }
                };
                let before = match old.get(key) {
                    Some(Value::Object(o)) => o.clone(),
                    // It wasn't an object before. A mergeable map may still hold fields from an
                    // earlier life at this key, so diff against what it holds.
                    _ => child.get_deep_value().to_json_value().as_object().cloned().unwrap_or_default(),
                };
                merge_object(&child, &before, fields)?;
            }
            v => map.insert(key, to_loro(v))?,
        }
    }
    for key in old.keys().filter(|k| !new.contains_key(*k)) {
        map.delete(key)?;
    }
    Ok(())
}

pub fn write_markdown(doc: &LoroDoc, text: &str) -> Result<()> {
    // Turns "old file → new file" into minimal inserts and deletes, so edits elsewhere merge.
    doc.get_text("markdown")
        .update(text, UpdateOptions::default())
        .map_err(|e| anyhow!("markdown diff timed out: {e:?}"))
}

// ── Helpers ─────────────────────────────────────────────────────────────────────────────────

/// The map at `key`, if there is one (never creates it).
fn child_map(map: &LoroMap, key: &str) -> Option<LoroMap> {
    map.get(key)?;
    map.ensure_mergeable_map(key).ok()
}

fn strings(list: &LoroMovableList) -> Vec<String> {
    match list.get_value().to_json_value() {
        Value::Array(items) => items.into_iter().map(|v| v.as_str().unwrap_or_default().to_string()).collect(),
        _ => Vec::new(),
    }
}

fn to_loro(v: &Value) -> LoroValue {
    match v {
        Value::Null => LoroValue::Null,
        Value::Bool(b) => (*b).into(),
        Value::Number(n) => match n.as_i64() {
            Some(i) => i.into(),
            None => n.as_f64().unwrap_or_default().into(),
        },
        Value::String(s) => s.as_str().into(),
        Value::Array(items) => items.iter().map(to_loro).collect::<Vec<_>>().into(),
        Value::Object(fields) => {
            fields.iter().map(|(k, v)| (k.clone(), to_loro(v))).collect::<HashMap<String, LoroValue>>().into()
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use loro::ExportMode;
    use serde_json::json;

    fn fork(doc: &LoroDoc, peer: u64) -> LoroDoc {
        let copy = LoroDoc::from_snapshot(&doc.export(ExportMode::Snapshot).unwrap()).unwrap();
        copy.set_peer_id(peer).unwrap();
        copy
    }

    fn sync(a: &LoroDoc, b: &LoroDoc) {
        b.import(&a.export(ExportMode::updates(&b.oplog_vv())).unwrap()).unwrap();
        a.import(&b.export(ExportMode::updates(&a.oplog_vv())).unwrap()).unwrap();
    }

    /// Two machines each add the first comment to a store offline, so both create `comments`.
    /// Writing the store as one value, or creating `comments` with `insert_container`, would keep
    /// only one side.
    #[test]
    fn concurrent_comments_both_survive() {
        let base = LoroDoc::new();
        let v0 = json!({ "title": "Icons", "notes": { "n0": "first" } });
        write_store(&base, &json!({}), &v0).unwrap();
        base.commit();

        let (mac, orb) = (fork(&base, 1), fork(&base, 2));
        let mut on_mac = v0.clone();
        on_mac["comments"] = json!({ "c1": { "text": "from mac" } });
        write_store(&mac, &v0, &on_mac).unwrap();
        let mut on_orb = v0.clone();
        on_orb["comments"] = json!({ "c2": { "text": "from orb" } });
        on_orb["notes"]["n0"] = json!("first, edited");
        write_store(&orb, &v0, &on_orb).unwrap();
        mac.commit();
        orb.commit();
        sync(&mac, &orb);

        let want = json!({
            "title": "Icons",
            "notes": { "n0": "first, edited" },
            "comments": { "c1": { "text": "from mac" }, "c2": { "text": "from orb" } },
        });
        assert_eq!(read_store(&mac), want);
        assert_eq!(read_store(&orb), want);
    }

    fn ids(v: &[&str]) -> Vec<String> {
        v.iter().map(|s| s.to_string()).collect()
    }

    fn order(doc: &LoroDoc) -> Vec<String> {
        read_space(doc).channels["ch"].order.clone()
    }

    /// Mac moves O to the front while the orb deletes D and moves P to the end. Each side wrote
    /// only its own change, so the merge keeps both intents.
    #[test]
    fn concurrent_reorders_keep_both_intents() {
        let base = LoroDoc::new();
        add_channel(&base, "ch", "Channel").unwrap();
        reorder(&base, "ch", &[], &ids(&["P", "L", "D", "O"])).unwrap();
        base.commit();

        let (mac, orb) = (fork(&base, 1), fork(&base, 2));
        reorder(&mac, "ch", &ids(&["P", "L", "D", "O"]), &ids(&["O", "P", "L", "D"])).unwrap();
        reorder(&orb, "ch", &ids(&["P", "L", "D", "O"]), &ids(&["L", "O", "P"])).unwrap();
        mac.commit();
        orb.commit();
        sync(&mac, &orb);

        assert_eq!(order(&mac), ids(&["O", "L", "P"]));
        assert_eq!(order(&orb), ids(&["O", "L", "P"]));
    }

    /// An artifact another machine added (X), not here yet, keeps its place.
    #[test]
    fn reorder_keeps_unknown_ids() {
        let doc = LoroDoc::new();
        add_channel(&doc, "ch", "Channel").unwrap();
        reorder(&doc, "ch", &[], &ids(&["a", "b", "c", "X"])).unwrap();
        reorder(&doc, "ch", &ids(&["a", "b", "c"]), &ids(&["c", "a", "b"])).unwrap();
        assert_eq!(order(&doc), ids(&["c", "a", "b", "X"]));
    }
}
