//! URL artifacts that link to another artifact on the same Television, such as
//! `http://127.0.0.1:32848/artifact/01M45933…/atlas.html`.
//!
//! Each machine knows that artifact by its own ID and serves it on its own port, so the space doc
//! holds the link as `tv-artifact:<shared id><rest>`, and each machine turns it back into its own
//! URL. Links to other hosts or ports, or to artifacts outside the space, are kept as they are.

use anyhow::{Context, Result};

const SHARED: &str = "tv-artifact:";

pub fn is_shared(url: &str) -> bool {
    url.starts_with(SHARED)
}

/// This machine's URL → the space's form. `shared_id` maps a local artifact ID to its shared ID
/// when the artifact is in the space.
pub fn to_shared(url: &str, port: u16, shared_id: impl Fn(&str) -> Option<String>) -> String {
    let local =
        ["127.0.0.1", "localhost"].iter().find_map(|host| url.strip_prefix(&format!("http://{host}:{port}/artifact/")));
    let Some(after) = local else { return url.to_string() };
    let end = after.find(['/', '?', '#']).unwrap_or(after.len());
    match shared_id(&after[..end]) {
        Some(sid) => format!("{SHARED}{sid}{}", &after[end..]),
        None => url.to_string(),
    }
}

/// The space's form → this machine's URL. Fails while the linked artifact isn't here yet.
pub fn to_local(url: &str, port: u16, local_id: impl Fn(&str) -> Option<String>) -> Result<String> {
    let Some(after) = url.strip_prefix(SHARED) else { return Ok(url.to_string()) };
    let end = after.find(['/', '?', '#']).unwrap_or(after.len());
    let id = local_id(&after[..end])
        .with_context(|| format!("the artifact it links to ({}) isn't here yet", &after[..end]))?;
    Ok(format!("http://127.0.0.1:{port}/artifact/{id}{}", &after[end..]))
}

#[cfg(test)]
mod tests {
    use super::*;

    const STORY: &str = "/iframe.html?id=icons--inventory&viewMode=story#top";

    #[test]
    fn a_link_opens_each_machines_own_copy() {
        // On the Mac (port 32848) the artifact is ID M and shared as S; on the orb (port 41000) it's O.
        let mac = format!("http://127.0.0.1:32848/artifact/M{STORY}");
        let shared = to_shared(&mac, 32848, |id| (id == "M").then(|| "S".to_string()));
        assert_eq!(shared, format!("tv-artifact:S{STORY}"));
        let orb = to_local(&shared, 41000, |s| (s == "S").then(|| "O".to_string())).unwrap();
        assert_eq!(orb, format!("http://127.0.0.1:41000/artifact/O{STORY}"));
        // ...and reading the orb's URL back gives the same shared form, so it isn't an edit.
        assert_eq!(to_shared(&orb, 41000, |id| (id == "O").then(|| "S".to_string())), shared);
        assert_eq!(to_shared("http://localhost:32848/artifact/M", 32848, |_| Some("S".into())), "tv-artifact:S");
    }

    #[test]
    fn other_links_are_kept() {
        let in_space = |_: &str| Some("S".to_string());
        for url in [
            "http://127.0.0.1:6006/?path=/story/icons",         // a local service
            "http://127.0.0.1:32849/artifact/M/a.html",         // another Television
            "http://100.69.184.98:32848/artifact/M/facts.html", // another machine
            "https://quire.sh/artifact/M",
        ] {
            assert_eq!(to_shared(url, 32848, in_space), url);
        }
        // An artifact that isn't in the space.
        assert_eq!(
            to_shared("http://127.0.0.1:32848/artifact/X/a.html", 32848, |_| None),
            "http://127.0.0.1:32848/artifact/X/a.html"
        );
        assert!(to_local("tv-artifact:S/a.html", 41000, |_| None).is_err());
    }
}
