#!/bin/sh
# Installs tv-sync and its agent skill. Run it again to update both.
#
#   curl -fsSL https://raw.githubusercontent.com/Kamalnrf/tv-sync/main/install.sh | sh
set -eu
repo=Kamalnrf/tv-sync

if ! command -v cargo >/dev/null 2>&1; then
  echo "tv-sync is built with Rust's cargo, which isn't installed. Get it from https://rustup.rs, then run this again." >&2
  exit 1
fi
cargo install --locked --git "https://github.com/$repo" tv-sync

if command -v npx >/dev/null 2>&1; then
  # Into each coding agent the skills CLI finds on this machine. Its stdin is the terminal-less
  # pipe this script came from, so nothing may prompt.
  npx -y skills add "$repo" --skill tv-sync --global --yes </dev/null
else
  echo "Installed tv-sync, but not its agent skill, which needs Node's npx: npx skills add $repo --skill tv-sync --global" >&2
fi
