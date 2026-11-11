//! What tv-sync keeps on disk, under `~/.tv-sync` (or `--data-dir`):
//!
//!   spaces.json              every space this machine belongs to, with its device token (mode 600)
//!   spaces/<id>/<room>.loro  a snapshot of each room's Loro doc

use std::{
    collections::BTreeMap,
    fs,
    path::{Path, PathBuf},
};

use anyhow::{Context, Result, bail};
use serde::{Deserialize, Serialize};

pub struct Home {
    pub dir: PathBuf,
}

#[derive(Default, Serialize, Deserialize)]
pub struct Spaces {
    pub spaces: BTreeMap<String, SpaceEntry>,
}

#[derive(Clone, Serialize, Deserialize)]
pub struct SpaceEntry {
    pub name: String,
    pub relay: String,
    pub device_id: String,
    pub device_name: String,
    pub token: String,
    /// `share`/`unshare` requests, oldest first. The CLI only appends; the daemon handles each
    /// one once (it remembers the last ID it handled), so the CLI never touches Loro docs.
    #[serde(default)]
    pub requests: Vec<Request>,
}

#[derive(Clone, Serialize, Deserialize)]
pub struct Request {
    pub id: u64,
    pub op: Op,
    /// This machine's Television channel ID.
    pub channel: String,
}

#[derive(Clone, Copy, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Op {
    Share,
    Unshare,
}

impl SpaceEntry {
    pub fn request(&mut self, op: Op, channel: String) {
        let now = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap_or_default();
        let last = self.requests.last().map_or(0, |r| r.id);
        self.requests.push(Request { id: (now.as_nanos() as u64).max(last + 1), op, channel });
    }
}

impl Home {
    pub fn new(dir: Option<PathBuf>) -> Result<Self> {
        let dir = match dir {
            Some(d) => d,
            None => std::env::home_dir().context("no home directory")?.join(".tv-sync"),
        };
        fs::create_dir_all(&dir)?;
        Ok(Self { dir })
    }

    pub fn space_dir(&self, id: &str) -> PathBuf {
        self.dir.join("spaces").join(id)
    }

    pub fn load(&self) -> Result<Spaces> {
        let path = self.dir.join("spaces.json");
        if !path.exists() {
            return Ok(Spaces::default());
        }
        serde_json::from_slice(&fs::read(&path)?).with_context(|| format!("reading {}", path.display()))
    }

    pub fn save(&self, spaces: &Spaces) -> Result<()> {
        let path = self.dir.join("spaces.json");
        write_atomic(&path, &serde_json::to_vec_pretty(spaces)?)?;
        // It holds device tokens: readable by this user only.
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            fs::set_permissions(&path, fs::Permissions::from_mode(0o600))?;
        }
        Ok(())
    }
}

impl Spaces {
    /// Look a space up by name or ID. Without one, the space is this machine's only one.
    pub fn find(&self, key: Option<&str>) -> Result<(String, SpaceEntry)> {
        let hits: Vec<_> = self.spaces.iter().filter(|(id, e)| key.is_none_or(|k| *id == k || e.name == k)).collect();
        match (hits.as_slice(), key) {
            ([(id, e)], _) => Ok(((*id).clone(), (*e).clone())),
            ([], None) => {
                bail!("this machine isn't in a space yet: tv-sync space create <name>, or tv-sync join <invite>")
            }
            ([], Some(k)) => bail!("no space named {k:?} on this machine (see `tv-sync status`)"),
            (_, None) => {
                let names: Vec<_> = hits.iter().map(|(_, e)| e.name.as_str()).collect();
                bail!("this machine is in several spaces ({}); pick one with --space", names.join(", "))
            }
            (_, Some(k)) => bail!("several spaces are named {k:?}; use the space ID (see `tv-sync status`)"),
        }
    }
}

/// Write to a temp file, then rename: a crash leaves the old file or the new one, never half of one.
pub fn write_atomic(path: &Path, bytes: &[u8]) -> Result<()> {
    if let Some(dir) = path.parent() {
        fs::create_dir_all(dir)?;
    }
    let tmp = path.with_extension("tmp");
    fs::write(&tmp, bytes)?;
    fs::rename(&tmp, path)?;
    Ok(())
}
