---
name: tv-sync
description: "Shares Television channels between machines and people with the tv-sync CLI: create a space, share channels, invite and join, check sync status, start or stop the background sync, revoke a device. Use when the user wants a Television channel on another machine, an Amp orb or with a friend, pastes a tv-sync invite (tvs1:...), asks whether a channel is synced, or works on artifacts in a channel tv-sync shares. Not for Television's own share links (tv share-artifact)."
---

# tv-sync

tv-sync keeps Television channels the same on several machines. Each machine runs its own Television and a background sync; the syncs exchange changes through a relay, so no machine has to stay on and none needs a network path to another. It syncs artifacts (HTML files and folders, markdown, URLs), their JSON stores (comments included), titles, sizes and order.

tv-sync has no command for editing. Work on channels and artifacts with the `tv` CLI and the `television` skill as usual: anything written to a shared channel reaches the other machines within a few seconds.

## Spaces decide who sees what

A space is a group of devices, the user's own machines and people they trust, that sync a set of channels. Everyone in a space sees and can edit every channel shared into it, including channels shared later. There are no roles.

- Before sharing a channel, run `tv-sync devices` to see who is in the space, because sharing shows the channel to all of them. To share only some channels with someone, create a separate space for them.
- An invite (`tvs1:...`) lets whoever holds it join the space, once, within 15 minutes. Give it to the user to pass along; keep it out of files, commits and public messages.

## Commands

- `tv-sync status`: whether the sync is running, each space with its channels, and shares still waiting to be applied. Start here.
- `tv-sync space create <name>`: on the public relay, unless `--relay <url>` (or `TV_SYNC_RELAY`) names another. Anyone can create a space.
- `tv-sync share <channel>...`: channel names or IDs from `tv list-channels`.
- `tv-sync invite`, then `tv-sync join <invite>` on the other machine.
- `tv-sync unshare <channel>...`: every machine keeps its copy, no longer synced.
- `tv-sync devices` and `tv-sync revoke <device>`: a revoked device keeps its local copy but gets nothing new.
- `tv-sync start` and `tv-sync stop`.

`space create`, `join`, `share` and `unshare` start the background sync if it isn't running, so `tv-sync start` is needed only after a reboot or a `stop`. In a shell without a terminal, `start` returns at once and prints the log's path; in a terminal it follows the log until Ctrl-C, which leaves the sync running. Don't use `tv-sync run`: it is the same sync in the foreground, and it blocks the shell until it is interrupted.

`share`, `unshare`, `invite`, `devices` and `revoke` take `--space <name>` only when the machine is in more than one space. Without it they refuse and list the spaces; ask the user which one rather than guessing.

A device is named after the hostname up to its first dot unless `--device <name>` is given to `space create` or `join`. When the name is taken in the space, `join` fails with 409 without using up the invite, so retry the same invite with another `--device`.

## Working in a shared channel

- Write to the shared channel by its exact name, as `tv-sync status` lists it. A new channel with a similar name isn't shared, so work put there stays on this machine.
- Edit an artifact's files at the path `tv get-artifact` reports. On the machine that shared it, that's the original file or folder; elsewhere it's a copy under `~/.tv-sync/files/`. Markdown edited on two machines at once merges; for any other file, the last saved version wins.
- JSON stores merge key by key. Keep data that several people may write at once in objects keyed by ID (`tv resource json push` makes the keys). An array is replaced whole, so concurrent edits to one array keep only one side.
- Deleting an artifact deletes it on every machine, and so does moving it to a channel that isn't shared. To take a channel out of sync, `tv-sync unshare` it; deleting a shared channel only makes tv-sync recreate it.
- Renames of channels and artifacts sync. tv-sync never renames anything by itself.

## What doesn't sync

- Proxy artifacts.
- What a URL points at. A URL artifact syncs as is, so `http://localhost:3000` opens each machine's own localhost. Links to another artifact in the same space are the exception: each machine opens its own copy.
- Files over 50 MB, and files or folders whose names start with a dot.
- Per-page UI state, such as a comment panel's "Show resolved".
- Rendering. A WebGL artifact can arrive intact and still be blank on a machine whose browser has no GPU, such as an Amp orb's desktop.

## When a change doesn't show up

1. Run `tv-sync status`. "Not syncing" means run `tv-sync start`; a share listed as waiting hasn't been applied yet.
2. Read the log at `~/.tv-sync/daemon.log` (the previous run's is `daemon.log.1`). `↑` lines are this machine's changes going out and `↓` lines are changes coming in. "lost the relay" means offline: it reconnects and catches up by itself. "removed from the space" means this device was revoked.
3. tv-sync works through the `tv` CLI, so Television must be running on this machine (`tv status`).

Changes made while a machine was off or offline merge when it reconnects; nothing has to be shared again.

`--data-dir` (or `TV_SYNC_DIR`, default `~/.tv-sync`) and `--tv-home` (or `TV_SYNC_TV_HOME`) select another tv-sync state folder or Television home.
