//! HTTP calls to the relay Worker: spaces, invites, devices.

use std::time::Duration;

use anyhow::{Context, Result, bail};
use reqwest::{Client, RequestBuilder};
use serde::{Deserialize, de::DeserializeOwned};
use serde_json::json;

pub struct Relay {
    base: String,
    http: Client,
}

/// What the relay returns when a device is created: by `space create` or by redeeming an invite.
#[derive(Debug, Deserialize)]
pub struct Credentials {
    pub space_id: String,
    pub name: String,
    pub device_id: String,
    pub token: String,
}

#[derive(Debug, Deserialize)]
pub struct Invite {
    pub code: String,
}

#[derive(Debug, Deserialize)]
pub struct Device {
    pub id: String,
    pub name: String,
    pub last_seen: Option<u64>,
    pub revoked_at: Option<u64>,
    pub online: bool,
    pub you: bool,
}

impl Relay {
    pub fn new(base: &str) -> Self {
        // Without a timeout a dead network would stall the engine (it handles one thing at a time).
        let http = Client::builder().timeout(Duration::from_secs(30)).build().expect("HTTP client");
        Self { base: base.trim_end_matches('/').to_string(), http }
    }

    pub fn base(&self) -> &str {
        &self.base
    }

    pub fn ws_url(&self, space: &str) -> String {
        let base = self.base.replacen("https://", "wss://", 1).replacen("http://", "ws://", 1);
        format!("{base}/spaces/{space}/ws")
    }

    pub async fn create_space(&self, name: &str, device: &str) -> Result<Credentials> {
        let req = self.http.post(format!("{}/spaces", self.base));
        send(req.json(&json!({ "name": name, "device": device }))).await
    }

    pub async fn invite(&self, space: &str, token: &str) -> Result<Invite> {
        send(self.http.post(format!("{}/spaces/{space}/invites", self.base)).bearer_auth(token)).await
    }

    pub async fn join(&self, space: &str, code: &str, name: &str) -> Result<Credentials> {
        let req = self.http.post(format!("{}/spaces/{space}/join", self.base));
        send(req.json(&json!({ "code": code, "name": name }))).await
    }

    pub async fn devices(&self, space: &str, token: &str) -> Result<Vec<Device>> {
        send(self.http.get(format!("{}/spaces/{space}/devices", self.base)).bearer_auth(token)).await
    }

    pub async fn revoke(&self, space: &str, token: &str, device_id: &str) -> Result<()> {
        let url = format!("{}/spaces/{space}/devices/{device_id}/revoke", self.base);
        send::<serde_json::Value>(self.http.post(url).bearer_auth(token)).await.map(drop)
    }
}

impl Relay {
    fn blob_url(&self, space: &str, sha: &str) -> String {
        format!("{}/spaces/{space}/blobs/{sha}", self.base)
    }

    pub async fn has_blob(&self, space: &str, token: &str, sha: &str) -> Result<bool> {
        let res = self.http.head(self.blob_url(space, sha)).bearer_auth(token).send().await?;
        match res.status().as_u16() {
            200 => Ok(true),
            404 => Ok(false),
            s => bail!("relay said {s} for blob {sha}"),
        }
    }

    pub async fn put_blob(&self, space: &str, token: &str, sha: &str, bytes: Vec<u8>) -> Result<()> {
        let res = self.http.put(self.blob_url(space, sha)).bearer_auth(token).body(bytes).send().await?;
        if !res.status().is_success() {
            bail!("relay refused blob {sha}: {}", res.status());
        }
        Ok(())
    }

    pub async fn get_blob(&self, space: &str, token: &str, sha: &str) -> Result<Vec<u8>> {
        let res = self.http.get(self.blob_url(space, sha)).bearer_auth(token).send().await?;
        if !res.status().is_success() {
            bail!("relay has no blob {sha}: {}", res.status());
        }
        Ok(res.bytes().await?.to_vec())
    }
}

async fn send<T: DeserializeOwned>(req: RequestBuilder) -> Result<T> {
    let res = req.send().await.context("can't reach the relay")?;
    let status = res.status();
    let body = res.text().await?;
    if !status.is_success() {
        let msg = serde_json::from_str::<serde_json::Value>(&body)
            .ok()
            .and_then(|v| v["error"].as_str().map(String::from))
            .unwrap_or(body);
        bail!("relay said {status}: {msg}");
    }
    serde_json::from_str(&body).with_context(|| format!("unexpected relay response: {body}"))
}

/// An invite is everything a new machine needs: `tvs1:<relay>/<space id>/<code>`.
pub fn format_invite(relay: &str, space: &str, code: &str) -> String {
    format!("tvs1:{relay}/{space}/{code}")
}

pub fn parse_invite(invite: &str) -> Result<(String, String, String)> {
    let rest = invite.trim().strip_prefix("tvs1:").context("not a tv-sync invite (expected tvs1:…)")?;
    let mut parts = rest.rsplitn(3, '/');
    match (parts.next(), parts.next(), parts.next()) {
        (Some(code), Some(space), Some(relay)) if code.starts_with("inv_") && !space.is_empty() => {
            Ok((relay.to_string(), space.to_string(), code.to_string()))
        }
        _ => bail!("malformed invite"),
    }
}
