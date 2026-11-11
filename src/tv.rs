//! Television, reached only through the public `tv` CLI.

use std::{path::PathBuf, process::Stdio, time::Duration};

use anyhow::{Context, Result, bail};
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use serde_json::Value;
use tokio::{io::AsyncWriteExt, process::Command, time::timeout};

/// Every call has a deadline: the engine handles one thing at a time, so a hung `tv` would
/// stall all syncing.
const TV_TIMEOUT: Duration = Duration::from_secs(20);

#[derive(Clone)]
pub struct Tv {
    pub bin: PathBuf,
    pub home: Option<PathBuf>,
}

#[derive(Debug, Deserialize)]
pub struct Channel {
    pub id: String,
    pub name: String,
    #[serde(default)]
    pub layout: Vec<Slot>,
}

#[derive(Debug, Deserialize)]
pub struct Slot {
    #[serde(rename = "artifactIds")]
    pub artifact_ids: Vec<String>,
    pub geometry: Geometry,
    pub size: Size,
}

#[derive(Debug, Deserialize)]
pub struct Geometry {
    #[serde(default)]
    pub full_screen: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub struct Size {
    pub width: f64,
    pub height: f64,
}

#[derive(Debug, Clone, Deserialize)]
pub struct Artifact {
    pub id: String,
    pub kind: String,
    pub title: String,
    pub path: Option<String>,
    pub url: Option<String>,
}

impl Tv {
    pub fn command(&self) -> Command {
        let mut cmd = Command::new(&self.bin);
        if let Some(home) = &self.home {
            cmd.arg("--home").arg(home);
        }
        // Its own process group, so a terminal's Ctrl-C reaches only the daemon, which then
        // finishes the call in progress instead of seeing it killed.
        cmd.process_group(0);
        cmd
    }

    async fn run(&self, args: &[&str], stdin: Option<&str>) -> Result<String> {
        let mut cmd = self.command();
        cmd.args(args)
            .stdin(if stdin.is_some() { Stdio::piped() } else { Stdio::null() })
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .kill_on_drop(true);
        let mut child = cmd.spawn().with_context(|| format!("can't run {}", self.bin.display()))?;
        if let (Some(text), Some(mut pipe)) = (stdin, child.stdin.take()) {
            pipe.write_all(text.as_bytes()).await?;
        }
        let out = timeout(TV_TIMEOUT, child.wait_with_output())
            .await
            .with_context(|| format!("`tv {}` timed out", args.join(" ")))??;
        if !out.status.success() {
            let err = String::from_utf8_lossy(&out.stderr);
            bail!("`tv {}` failed: {}", args.join(" "), err.lines().next().unwrap_or("").trim());
        }
        Ok(String::from_utf8_lossy(&out.stdout).into_owned())
    }

    async fn json<T: DeserializeOwned>(&self, args: &[&str]) -> Result<T> {
        let out = self.run(args, None).await?;
        serde_json::from_str(&out).with_context(|| format!("unexpected output from `tv {}`", args.join(" ")))
    }

    /// The port this home's Television listens on (0: picked at start).
    pub async fn port(&self) -> Result<u16> {
        let v: Value = self.json(&["config", "show"]).await?;
        v["settings"]["port"].as_u64().and_then(|p| u16::try_from(p).ok()).context("no port in `tv config show`")
    }

    pub async fn channels(&self) -> Result<Vec<Channel>> {
        #[derive(Deserialize)]
        struct Out {
            channels: Vec<Channel>,
        }
        Ok(self.json::<Out>(&["list-channels"]).await?.channels)
    }

    pub async fn artifacts(&self) -> Result<Vec<Artifact>> {
        #[derive(Deserialize)]
        struct Out {
            artifacts: Vec<Artifact>,
        }
        Ok(self.json::<Out>(&["list-artifacts"]).await?.artifacts)
    }

    pub async fn create_channel(&self, name: &str) -> Result<String> {
        created_id(&self.run(&["create-channel", "--name", name, "--no-focus"], None).await?)
    }

    pub async fn rename_channel(&self, id: &str, name: &str) -> Result<()> {
        self.run(&["update-channel", "--channel", id, "--name", name], None).await.map(drop)
    }

    pub async fn create_path_artifact(&self, channel: &str, title: &str, path: &str) -> Result<String> {
        let args = ["create-path-artifact", "--channel", channel, "--title", title, "--path", path, "--no-focus"];
        created_id(&self.run(&args, None).await?)
    }

    pub async fn create_url_artifact(&self, channel: &str, title: &str, url: &str) -> Result<String> {
        let args = ["create-url-artifact", "--channel", channel, "--title", title, "--url", url, "--no-focus"];
        created_id(&self.run(&args, None).await?)
    }

    pub async fn update_artifact(&self, id: &str, field: &str, value: &str) -> Result<()> {
        self.run(&["update-artifact", "--id", id, &format!("--{field}"), value], None).await.map(drop)
    }

    pub async fn delete_artifact(&self, id: &str) -> Result<()> {
        self.run(&["delete-artifact", "--id", id], None).await.map(drop)
    }

    pub async fn move_artifact(&self, id: &str, channel: &str) -> Result<()> {
        self.run(&["move-artifact", "--id", id, "--channel", channel, "--no-focus"], None).await.map(drop)
    }

    pub async fn resize(&self, id: &str, size: Size, full_screen: bool) -> Result<()> {
        let (w, h) = (size.width.to_string(), size.height.to_string());
        let fs = if full_screen { "--full-screen" } else { "--no-full-screen" };
        self.run(&["reposition-artifact", "--id", id, "--width", &w, "--height", &h, fs], None).await.map(drop)
    }

    pub async fn set_position(&self, id: &str, position: usize) -> Result<()> {
        self.run(&["reposition-artifact", "--id", id, "--position", &position.to_string()], None).await.map(drop)
    }

    /// The artifact's JSON store; `{}` when it has none yet.
    pub async fn store(&self, id: &str) -> Result<Value> {
        #[derive(Deserialize)]
        struct Out {
            exists: bool,
            value: Option<Value>,
        }
        let out: Out = self.json(&["resource", "json", "get", "--artifact", id]).await?;
        Ok(if out.exists { out.value.unwrap_or_default() } else { Value::Object(Default::default()) })
    }

    /// Write several store paths at once (`{"comments/c1": {...}}`).
    pub async fn store_update(&self, id: &str, paths: &serde_json::Map<String, Value>) -> Result<()> {
        let body = serde_json::to_string(paths)?;
        self.run(&["resource", "json", "update", "--artifact", id, "--file", "-"], Some(&body)).await.map(drop)
    }

    pub async fn store_remove(&self, id: &str, path: &str) -> Result<()> {
        self.run(&["resource", "json", "remove", "--artifact", id, path], None).await.map(drop)
    }

    pub async fn store_set_root(&self, id: &str, value: &Value) -> Result<()> {
        let body = serde_json::to_string(value)?;
        self.run(&["resource", "json", "set", "--artifact", id, "--file", "-"], Some(&body)).await.map(drop)
    }
}

/// `tv create-*` prints e.g. "Path artifact 01M741SDA4QBGWTN32JXN5Q2W2 created." Pick the ULID.
fn created_id(out: &str) -> Result<String> {
    out.split(|c: char| !c.is_ascii_alphanumeric())
        .find(|w| w.len() == 26 && w.chars().all(|c| c.is_ascii_digit() || c.is_ascii_uppercase()))
        .map(String::from)
        .with_context(|| format!("no ID in tv output: {out}"))
}
