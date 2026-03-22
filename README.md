# Mesh IRC

> ⚠️ **Warning: This project is Vibe Coded** — written quickly, with love, and a healthy dose of "it works on my machine" energy. Don't judge the code too harshly. 🫠

## What is this?

Mesh IRC is a MeshDash plugin that provides a **standalone IRC server** bridging all Meshtastic mesh channels and nodes bidirectionally.

Every Meshtastic node on your mesh appears as a **virtual IRC user** — so you can interact with the mesh using any IRC client!

## Features

- **Full mesh bridging**: All Meshtastic nodes become IRC users
- **BNC (Bouncer)**: Messages are buffered when offline and auto-replayed when you connect
- **Auto-join channels**: IRC clients automatically join all mesh channels
- **Voice status**: Active nodes (heard in last hour) get +v (voice) in channels
- **Password protection**: Optional password authentication
- **Dashboard**: Built-in web UI for monitoring and configuration

## Quick Start

1. Install the plugin in MeshDash
2. Connect your IRC client to `mesh-dash-hostname:6667`
3. If password is enabled, send: `/PASS <your-node-shortname>`
4. Your nick will be auto-set to `LongName-ShortName`
5. Join mesh channels and chat!

## IRC Commands

- `/JOIN #channelname` — Join a mesh channel
- `/WHOIS nodename` — See node info (last heard, hardware)
- `/REPLAYBUFFER` — Replay all buffered messages
- `/CLEARBUFFER` — Clear the message buffer

## Requirements

- MeshDash with plugin support
- An IRC client (weechat, hexchat, irssi, etc.)

## Configuration

Access the dashboard at `/plugins/mesh_irc` to configure:

- IRC server port
- Password (optional)
- BNC buffer size
- Keep messages after replay

## Technical Notes

This is a hobby project, not production software. It works but was written quickly (vibe coded 💅). Expect some jank.

Features include:
- SSE listener for real-time mesh messages
- Periodic mesh state refresh
- SQLite-based message buffering
- Per-channel voice tracking

## License

GNU General Public License v3 (GPL 3). See the LICENSE file for details.
