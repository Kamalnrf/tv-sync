//! tv-sync: carry Television channels across machines, through a relay, with Loro CRDTs.

mod daemon;
mod files;
mod links;
mod model;
mod relay;
mod rooms;
mod state;
mod sync;
mod tv;
mod wire;

use std::{
    path::PathBuf,
    time::{SystemTime, UNIX_EPOCH},
};

use anyhow::{Result, bail};
use clap::{Parser, Subcommand};

use crate::{
    relay::{Credentials, Relay, format_invite, parse_invite},
    state::{Home, Op, SpaceEntry},
    tv::Tv,
};

/// The relay `space create` uses unless told otherwise. Anyone may create a space on it.
const DEFAULT_RELAY: &str = "https://tv-sync-relay.kamalnrf.workers.dev";

#[derive(Parser)]
#[command(name = "tv-sync", version, about = "Sync Television channels across machines")]
struct Cli {
    /// Where tv-sync keeps its state [default: ~/.tv-sync]
    #[arg(long, env = "TV_SYNC_DIR", global = true)]
    data_dir: Option<PathBuf>,
    /// The Television home to sync (passed to `tv --home`)
    #[arg(long, env = "TV_SYNC_TV_HOME", global = true)]
    tv_home: Option<PathBuf>,
    /// The `tv` executable
    #[arg(long, env = "TV_SYNC_TV_BIN", default_value = "tv", global = true)]
    tv_bin: PathBuf,
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Create a space: a group of machines and people that sync channels together
    #[command(subcommand)]
    Space(SpaceCommand),
    /// Sync channels with everyone in the space (starts syncing if it isn't running)
    Share {
        /// Channel names or IDs
        #[arg(required = true)]
        channels: Vec<String>,
        /// The space, if this machine is in several
        #[arg(long, short)]
        space: Option<String>,
    },
    /// Print a one-time invite, valid 15 minutes, for another machine or a friend
    Invite {
        /// The space, if this machine is in several
        #[arg(long, short)]
        space: Option<String>,
    },
    /// Join a space with an invite (starts syncing if it isn't running)
    Join {
        invite: String,
        /// This machine's name in the space [default: hostname]
        #[arg(long)]
        device: Option<String>,
    },
    /// Whether sync is running, and the spaces and channels this machine syncs
    Status,
    /// Stop syncing channels; every machine keeps its copy, unsynced
    Unshare {
        #[arg(required = true)]
        channels: Vec<String>,
        /// The space, if this machine is in several
        #[arg(long, short)]
        space: Option<String>,
    },
    /// List the devices in the space
    Devices {
        /// The space, if this machine is in several
        #[arg(long, short)]
        space: Option<String>,
    },
    /// Cut a device off from the space
    Revoke {
        /// Device name or ID
        device: String,
        /// The space, if this machine is in several
        #[arg(long, short)]
        space: Option<String>,
    },
    /// Start syncing in the background (after a reboot), and show the log; Ctrl-C stops showing it
    Start,
    /// Stop syncing
    Stop,
    /// The sync itself, in this terminal until Ctrl-C. `start` runs it in the background.
    #[command(hide = true)]
    Run,
}

#[derive(Subcommand)]
enum SpaceCommand {
    /// Create a space on a relay; this machine becomes its first device (starts syncing)
    Create {
        name: String,
        /// The relay that holds the space
        #[arg(long, env = "TV_SYNC_RELAY", default_value = DEFAULT_RELAY)]
        relay: String,
        /// This machine's name in the space [default: hostname]
        #[arg(long)]
        device: Option<String>,
    },
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();
    let home = Home::new(cli.data_dir)?;
    let tv = Tv { bin: cli.tv_bin, home: cli.tv_home };

    match cli.command {
        Command::Space(SpaceCommand::Create { name, relay, device }) => {
            let relay = Relay::new(&relay);
            let device = device.unwrap_or_else(hostname);
            let creds = relay.create_space(&name, &device).await?;
            add_space(&home, &relay, creds, &device)?;
            println!("Created space {name:?} as {device:?}.");
            daemon::ensure(&home, &tv).await?;
            eprintln!("Next: tv-sync share <channel>{}", space_flag(&home, &name)?);
        }
        Command::Join { invite, device } => {
            let (relay, space, code) = parse_invite(&invite)?;
            if home.load()?.spaces.contains_key(&space) {
                bail!("this machine is already in that space");
            }
            let relay = Relay::new(&relay);
            let device = device.unwrap_or_else(hostname);
            let creds = relay.join(&space, &code, &device).await?;
            let space_name = creds.name.clone();
            add_space(&home, &relay, creds, &device)?;
            println!("Joined {space_name:?} as {device:?}. Its channels appear in Television as they download.");
            daemon::ensure(&home, &tv).await?;
        }
        Command::Share { channels, space } => {
            let name = request(&home, &tv, space.as_deref(), &channels, Op::Share).await?;
            daemon::ensure(&home, &tv).await?;
            eprintln!("To add another machine or a friend: tv-sync invite{}", space_flag(&home, &name)?);
        }
        Command::Unshare { channels, space } => {
            request(&home, &tv, space.as_deref(), &channels, Op::Unshare).await?;
            daemon::ensure(&home, &tv).await?;
        }
        Command::Invite { space } => {
            let (id, s) = home.load()?.find(space.as_deref())?;
            let relay = Relay::new(&s.relay);
            let invite = relay.invite(&id, &s.token).await?;
            println!("{}", format_invite(relay.base(), &id, &invite.code));
            eprintln!("One use, valid 15 minutes. On the other machine: tv-sync join <invite>");
        }
        Command::Status => status(&home, &tv).await?,
        Command::Devices { space } => {
            let (id, s) = home.load()?.find(space.as_deref())?;
            for d in Relay::new(&s.relay).devices(&id, &s.token).await? {
                let state = match (d.revoked_at, d.online) {
                    (Some(_), _) => "revoked".to_string(),
                    (None, true) => "online".to_string(),
                    (None, false) => format!("seen {}", ago(d.last_seen)),
                };
                let you = if d.you { "  (this device)" } else { "" };
                println!("{:<20} {:<16} {}{you}", d.name, state, d.id);
            }
        }
        Command::Revoke { device, space } => {
            let (id, s) = home.load()?.find(space.as_deref())?;
            let relay = Relay::new(&s.relay);
            let devices = relay.devices(&id, &s.token).await?;
            let Some(d) = devices.iter().find(|d| d.revoked_at.is_none() && (d.name == device || d.id == device))
            else {
                bail!("no active device {device:?} in {:?} (see `tv-sync devices`)", s.name);
            };
            if d.you {
                bail!("{:?} is this device", d.name);
            }
            relay.revoke(&id, &s.token, &d.id).await?;
            println!("Revoked {:?}. It can no longer read or write {:?}; its local copy stays.", d.name, s.name);
        }
        Command::Start => daemon::start(&home, &tv).await?,
        Command::Stop => daemon::stop(&home).await?,
        Command::Run => sync::run(home, tv).await?,
    }
    Ok(())
}

fn add_space(home: &Home, relay: &Relay, creds: Credentials, device: &str) -> Result<()> {
    let mut spaces = home.load()?;
    let entry = SpaceEntry {
        name: creds.name,
        relay: relay.base().to_string(),
        device_id: creds.device_id,
        device_name: device.to_string(),
        token: creds.token,
        requests: vec![],
    };
    spaces.spaces.insert(creds.space_id, entry);
    home.save(&spaces)
}

/// `share` and `unshare` only queue a request in spaces.json. The daemon owns the Loro docs,
/// so it's the one that applies it. Returns the space's name.
async fn request(home: &Home, tv: &Tv, space: Option<&str>, wanted: &[String], op: Op) -> Result<String> {
    let mut spaces = home.load()?;
    let (id, s) = spaces.find(space)?;
    let channels = tv.channels().await?;
    let entry = spaces.spaces.get_mut(&id).expect("found above");
    for want in wanted {
        let hits: Vec<_> = channels.iter().filter(|c| c.id == *want || c.name == *want).collect();
        let ch = match hits.as_slice() {
            [ch] => ch,
            [] => bail!("Television has no channel {want:?}"),
            _ => bail!("several channels are named {want:?}; use its ID"),
        };
        entry.request(op, ch.id.clone());
        let verb = if op == Op::Share { "Sharing" } else { "Unsharing" };
        println!("{verb} {:?} in {:?}.", ch.name, s.name);
    }
    home.save(&spaces)?;
    Ok(s.name)
}

/// What to add to a suggested command so it names `space`: nothing when it's this machine's only one.
fn space_flag(home: &Home, space: &str) -> Result<String> {
    Ok(if home.load()?.spaces.len() > 1 { format!(" --space {space:?}") } else { String::new() })
}

/// `tv-sync status`: it doesn't ask the relay anything, so it works offline and while the sync runs.
async fn status(home: &Home, tv: &Tv) -> Result<()> {
    match daemon::running(home) {
        Ok(Some(pid)) => println!("Syncing (pid {pid}). `tv-sync start` shows its log; `tv-sync stop` stops it."),
        Ok(None) => println!("Not syncing. `tv-sync start` starts it."),
        Err(e) => println!("Can't tell whether it's syncing: {e:#}"),
    }
    let spaces = home.load()?;
    if spaces.spaces.is_empty() {
        println!("\nThis machine isn't in a space yet: tv-sync space create <name>, or tv-sync join <invite>.");
    }
    // Pending requests name this machine's Television channels; look their names up only if needed.
    let mut local = None;
    for (id, s) in &spaces.spaces {
        // The ID only tells apart spaces with the same name; `--space` takes either.
        let twin = spaces.spaces.values().filter(|o| o.name == s.name).count() > 1;
        let tag = if twin { format!(" · {id}") } else { String::new() };
        println!("\n{} · as {} · {}{tag}", s.name, s.device_name, s.relay);
        let (channels, pending) = sync::overview(home, id, s)?;
        if channels.is_empty() && pending.is_empty() {
            println!("  no channels yet: tv-sync share <channel>");
        }
        for name in channels {
            println!("  {name}");
        }
        for req in pending {
            if local.is_none() {
                local = Some(tv.channels().await.unwrap_or_default());
            }
            let ch = local.iter().flatten().find(|c| c.id == req.channel);
            let verb = if req.op == Op::Share { "sharing" } else { "unsharing" };
            println!("  {verb} {:?} (waiting for the sync)", ch.map_or(&req.channel, |c| &c.name));
        }
    }
    Ok(())
}

/// The default device name: the hostname up to its first dot, so "mac.lan" and "mac.local" are "mac".
fn hostname() -> String {
    std::process::Command::new("hostname")
        .output()
        .ok()
        .map(|o| String::from_utf8_lossy(&o.stdout).split('.').next().unwrap_or_default().trim().to_string())
        .filter(|h| !h.is_empty())
        .unwrap_or_else(|| "device".to_string())
}

fn ago(ms: Option<u64>) -> String {
    let Some(ms) = ms else { return "never".to_string() };
    let now = SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default().as_millis() as u64;
    match now.saturating_sub(ms) / 1000 {
        s @ 0..60 => format!("{s}s ago"),
        s @ 60..3600 => format!("{}m ago", s / 60),
        s @ 3600..86400 => format!("{}h ago", s / 3600),
        s => format!("{}d ago", s / 86400),
    }
}
