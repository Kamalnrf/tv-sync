//! Artifact files on disk: hashing what's there and writing what the space says should be there.

use std::{
    collections::{BTreeMap, HashMap},
    fs,
    path::{Component, Path, PathBuf},
    time::SystemTime,
};

use anyhow::{Result, bail};
use sha2::{Digest, Sha256};

/// Files above this aren't synced (listed in the log instead).
const MAX_FILE: u64 = 50 * 1024 * 1024;

pub fn sha256_hex(bytes: &[u8]) -> String {
    Sha256::digest(bytes).iter().map(|b| format!("{b:02x}")).collect()
}

/// Hashes files, remembering each file's hash until its size or modified time changes, so a scan
/// every two seconds doesn't reread unchanged files.
#[derive(Default)]
pub struct Hasher {
    cache: HashMap<PathBuf, (SystemTime, u64, String)>,
}

impl Hasher {
    pub fn hash(&mut self, path: &Path) -> Result<String> {
        let meta = fs::metadata(path)?;
        let stamp = (meta.modified()?, meta.len());
        if let Some((m, l, sha)) = self.cache.get(path)
            && (*m, *l) == stamp
        {
            return Ok(sha.clone());
        }
        let sha = sha256_hex(&fs::read(path)?);
        self.cache.insert(path.to_path_buf(), (stamp.0, stamp.1, sha.clone()));
        Ok(sha)
    }

    /// Every file under `root` (skipping dotfiles and oversized files): relative path → sha256.
    pub fn scan(&mut self, root: &Path) -> Result<BTreeMap<String, String>> {
        if !root.is_dir() {
            bail!("{} is not a folder", root.display());
        }
        let mut files = BTreeMap::new();
        let mut stack = vec![root.to_path_buf()];
        while let Some(dir) = stack.pop() {
            for entry in fs::read_dir(&dir)? {
                let entry = entry?;
                if entry.file_name().to_string_lossy().starts_with('.') {
                    continue;
                }
                let path = entry.path();
                let kind = entry.file_type()?;
                if kind.is_dir() {
                    stack.push(path);
                } else if path.is_file() {
                    if fs::metadata(&path)?.len() > MAX_FILE {
                        eprintln!("not syncing {} (over 50 MB)", path.display());
                        continue;
                    }
                    let rel = path.strip_prefix(root)?.to_string_lossy().replace('\\', "/");
                    files.insert(rel, self.hash(&path)?);
                }
            }
        }
        Ok(files)
    }
}

/// `dir/rel`, refusing paths that would land outside `dir` (`..`, absolute paths).
pub fn safe_join(dir: &Path, rel: &str) -> Result<PathBuf> {
    let rel_path = Path::new(rel);
    if rel.is_empty() || !rel_path.components().all(|c| matches!(c, Component::Normal(_))) {
        bail!("refusing file path {rel:?}");
    }
    Ok(dir.join(rel_path))
}

/// Write in place rather than rename-over: Television watches these paths, and replacing the
/// file can leave a watcher on the old inode.
pub fn write_in_place(path: &Path, bytes: &[u8]) -> Result<()> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }
    fs::write(path, bytes)?;
    Ok(())
}

pub fn is_markdown(path: &Path) -> bool {
    path.extension().is_some_and(|e| e.eq_ignore_ascii_case("md") || e.eq_ignore_ascii_case("markdown"))
}
