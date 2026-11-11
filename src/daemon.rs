//! The daemon's lifecycle. `tv-sync run` is the daemon itself, in the foreground. `start` runs
//! it in the background and shows its log, and `stop` ends it. The commands that change what
//! syncs (`space create`, `join`, `share`, `unshare`) start it too, if it isn't running.
//!
//!   daemon.lock   held by the running daemon (a `flock`), and holds its pid. The kernel releases
//!                 the lock when the process ends, however it ends, so a held lock is the proof
//!                 that a daemon is running and a stale pid can't fool `start` or `stop`.
//!   daemon.log    what the background daemon prints; the previous one is kept as daemon.log.1

use std::{
    fs::{self, File, OpenOptions, TryLockError},
    io::{self, IsTerminal, Read, Seek, SeekFrom, Write},
    os::unix::{fs::OpenOptionsExt, process::CommandExt},
    path::Path,
    process::{Command, Stdio},
    time::{Duration, Instant},
};

use anyhow::{Context, Result, anyhow, bail};
use tokio::{
    signal::unix::{SignalKind, signal},
    time::sleep,
};

use crate::{state::Home, tv::Tv};

/// How long `start` waits for the new daemon to take the lock, and `stop` for it to let go.
/// Stopping waits for the sync in progress, which can include an upload.
const START_WAIT: Duration = Duration::from_secs(10);
const STOP_WAIT: Duration = Duration::from_secs(30);

/// The running daemon's hold on `daemon.lock`.
pub struct Lock(File);

/// Taken by `run`: refuses a second daemon on the same data dir, and records this process's pid.
pub fn lock(home: &Home) -> Result<Lock> {
    // Not truncated on open: a refused second daemon mustn't erase the running one's pid.
    let path = home.dir.join("daemon.lock");
    let mut file = OpenOptions::new()
        .create(true)
        .read(true)
        .write(true)
        .truncate(false)
        .open(&path)
        .with_context(|| format!("can't open {}", path.display()))?;
    // `running` takes the lock for an instant to look; a moment of retrying keeps that from
    // being mistaken for another daemon.
    let deadline = Instant::now() + Duration::from_millis(300);
    while let Err(e) = file.try_lock() {
        if matches!(e, TryLockError::Error(_)) || Instant::now() > deadline {
            return Err(anyhow!("another tv-sync daemon is already running for {}", home.dir.display()));
        }
        std::thread::sleep(Duration::from_millis(10));
    }
    file.set_len(0)?;
    write!(file, "{}", std::process::id())?;
    Ok(Lock(file))
}

impl Drop for Lock {
    /// A clean exit leaves no pid behind. (After a crash the pid stays, but the lock is free.)
    fn drop(&mut self) {
        let _ = self.0.set_len(0);
    }
}

/// The pid of the daemon running for this data dir, if one is.
pub fn running(home: &Home) -> Result<Option<u32>> {
    let Ok(mut file) = File::open(home.dir.join("daemon.lock")) else { return Ok(None) };
    // Taking the lock is how to ask whether anyone holds it; it's released when `file` drops.
    // Shared, so two terminals showing the log don't get in each other's way.
    match file.try_lock_shared() {
        Ok(()) => return Ok(None),
        Err(TryLockError::WouldBlock) => {}
        Err(TryLockError::Error(e)) => return Err(e.into()),
    }
    // A daemon that has just taken the lock writes its pid a moment later.
    let deadline = Instant::now() + Duration::from_secs(1);
    loop {
        let mut text = String::new();
        file.seek(SeekFrom::Start(0))?;
        file.read_to_string(&mut text)?;
        if let Ok(pid) = text.trim().parse() {
            return Ok(Some(pid));
        }
        if Instant::now() > deadline {
            // Builds before `start` existed lock the file without writing a pid.
            bail!(
                "a tv-sync daemon holds {} but hasn't written its pid; if it's a `tv-sync run` from an older \
                 build, stop it with Ctrl-C in its terminal and try again",
                home.dir.join("daemon.lock").display()
            );
        }
        std::thread::sleep(Duration::from_millis(20));
    }
}

/// `tv-sync start`: run the daemon in the background unless it already runs, then show its log
/// until Ctrl-C. The daemon runs in its own session, so neither Ctrl-C nor closing the terminal
/// reaches it.
pub async fn start(home: &Home, tv: &Tv) -> Result<()> {
    let log = home.dir.join("daemon.log");
    let (pid, from) = match running(home)? {
        Some(pid) => {
            println!("tv-sync is already running (pid {pid}).");
            (pid, tail_start(&fs::read(&log).unwrap_or_default(), 20) as u64)
        }
        None => {
            let pid = spawn(home, tv, &log).await?;
            println!("Started tv-sync (pid {pid}).");
            (pid, 0)
        }
    };
    if !io::stdout().is_terminal() {
        eprintln!("Its log: {}. `tv-sync stop` stops it.", log.display());
        return Ok(());
    }
    eprintln!("Showing its log. Ctrl-C stops showing it; tv-sync keeps running until `tv-sync stop`.\n");
    follow(home, pid, &log, from).await
}

/// After `space create`, `join`, `share` or `unshare`: a running daemon picks the change up on its
/// next tick, so start one only if none runs.
pub async fn ensure(home: &Home, tv: &Tv) -> Result<()> {
    match running(home)? {
        Some(pid) => println!("The running sync (pid {pid}) applies this within a few seconds."),
        None => {
            let pid = spawn(home, tv, &home.dir.join("daemon.log")).await?;
            println!("Started syncing in the background (pid {pid}); `tv-sync stop` stops it.");
        }
    }
    Ok(())
}

/// Start `tv-sync run` detached, with its output in `log`, and wait until it holds the lock.
async fn spawn(home: &Home, tv: &Tv, log: &Path) -> Result<u32> {
    if log.exists() {
        fs::rename(log, log.with_extension("log.1"))?;
    }
    let out = OpenOptions::new().create(true).append(true).mode(0o600).open(log)?;
    let mut cmd = Command::new(std::env::current_exe()?);
    cmd.arg("--data-dir").arg(&home.dir).arg("--tv-bin").arg(&tv.bin);
    if let Some(tv_home) = &tv.home {
        cmd.arg("--tv-home").arg(tv_home);
    }
    cmd.arg("run").stdin(Stdio::null()).stdout(out.try_clone()?).stderr(out);
    // SAFETY: pre_exec runs in the child between fork and exec, where only async-signal-safe
    // calls are allowed; setsid is one. A new session has no controlling terminal, so the
    // terminal's Ctrl-C and hang-up never reach the daemon.
    unsafe {
        cmd.pre_exec(|| if libc::setsid() == -1 { Err(io::Error::last_os_error()) } else { Ok(()) });
    }
    let mut child = cmd.spawn().context("can't start the daemon")?;

    let deadline = Instant::now() + START_WAIT;
    loop {
        if let Some(status) = child.try_wait()? {
            bail!("the daemon exited right away ({status}):\n{}", tail(log, 20));
        }
        if running(home)? == Some(child.id()) {
            return Ok(child.id());
        }
        if Instant::now() > deadline {
            bail!("the daemon (pid {}) didn't start within {START_WAIT:?}; see {}", child.id(), log.display());
        }
        sleep(Duration::from_millis(50)).await;
    }
}

/// Print the log from byte `pos` as it grows, until Ctrl-C or until the daemon stops.
async fn follow(home: &Home, pid: u32, log: &Path, mut pos: u64) -> Result<()> {
    let mut interrupt = signal(SignalKind::interrupt())?;
    let mut out = io::stdout();
    loop {
        // Check first, then print: a daemon that stops in between has already written its last lines.
        let alive = running(home)? == Some(pid);
        if let Ok(mut file) = File::open(log) {
            let len = file.metadata()?.len();
            if len > pos {
                file.seek(SeekFrom::Start(pos))?;
                io::copy(&mut file.take(len - pos), &mut out)?;
                out.flush()?;
                pos = len;
            }
        }
        if !alive {
            eprintln!("\ntv-sync (pid {pid}) has stopped.");
            return Ok(());
        }
        tokio::select! {
            _ = interrupt.recv() => {
                eprintln!("\nStopped showing the log. tv-sync keeps running; `tv-sync stop` stops it.");
                return Ok(());
            }
            () = sleep(Duration::from_millis(200)) => {}
        }
    }
}

/// `tv-sync stop`: ask the daemon to stop (SIGTERM) and wait until it has. It finishes the sync
/// in progress first, so nothing is left half written.
pub async fn stop(home: &Home) -> Result<()> {
    let Some(pid) = running(home)? else {
        println!("tv-sync isn't running.");
        return Ok(());
    };
    // SAFETY: kill has no memory-safety preconditions; it only sends a signal.
    if unsafe { libc::kill(pid as libc::pid_t, libc::SIGTERM) } == -1 {
        let e = io::Error::last_os_error();
        if e.raw_os_error() != Some(libc::ESRCH) {
            return Err(e).context(format!("can't signal pid {pid}"));
        }
    }
    eprintln!("Stopping tv-sync (pid {pid}); it finishes the sync in progress first…");
    let deadline = Instant::now() + STOP_WAIT;
    while running(home)? == Some(pid) {
        if Instant::now() > deadline {
            bail!("tv-sync (pid {pid}) is still running after {STOP_WAIT:?}; `kill -9 {pid}` ends it at once");
        }
        sleep(Duration::from_millis(100)).await;
    }
    println!("Stopped.");
    Ok(())
}

/// Where the last `lines` lines of `bytes` start.
fn tail_start(bytes: &[u8], lines: usize) -> usize {
    let body = bytes.strip_suffix(b"\n").unwrap_or(bytes);
    body.iter().enumerate().rev().filter(|(_, b)| **b == b'\n').nth(lines - 1).map_or(0, |(i, _)| i + 1)
}

fn tail(path: &Path, lines: usize) -> String {
    let bytes = fs::read(path).unwrap_or_default();
    String::from_utf8_lossy(&bytes[tail_start(&bytes, lines)..]).trim_end().to_string()
}
