"""
Mesh IRC — MeshDash Plugin
===========================
A standalone IRC server that bridges all Meshtastic mesh channels and nodes
bidirectionally. Every Meshtastic node appears as a virtual IRC user.
Includes BNC (bouncer) feature to buffer messages when offline.
"""

import asyncio
import logging
import re
import time
import sqlite3
from pathlib import Path
from typing import Optional, Dict, Set, List, Any

from fastapi import APIRouter, HTTPException, Query
from fastapi.responses import JSONResponse

# Plugin boilerplate
core_context: dict = {}
plugin_router = APIRouter()

logger: logging.Logger = None

# Configuration
IRC_SERVER_NAME = "mesh.local"
IRC_PORT = 6667
IRC_PASSWORD = None  # Password for connecting (set to local node's short_name)
IRC_PASSWORD_ENABLED = True  # Require password for connections
BNC_BUFFER_LIMIT = 50  # Max messages per channel to buffer
BNC_ENABLED = True  # BNC buffering enabled by default
BNC_KEEP_AFTER_REPLAY = False  # Keep messages after replay instead of deleting

# Settings file path
_SETTINGS_FILE = "/tmp/mesh_irc_settings.json"

# ===========================================================================
# Settings Persistence
# ===========================================================================

def load_settings():
    """Load settings from file."""
    global IRC_PORT, IRC_PASSWORD, IRC_PASSWORD_ENABLED, BNC_BUFFER_LIMIT, BNC_ENABLED, BNC_KEEP_AFTER_REPLAY
    try:
        import json
        with open(_SETTINGS_FILE, 'r') as f:
            data = json.load(f)
        IRC_PORT = data.get('irc_port', 6667)
        IRC_PASSWORD = data.get('irc_password')
        IRC_PASSWORD_ENABLED = data.get('irc_password_enabled', True)
        BNC_BUFFER_LIMIT = data.get('buffer_size', 50)
        BNC_ENABLED = data.get('bnc_enabled', True)
        BNC_KEEP_AFTER_REPLAY = data.get('keep_after_replay', False)
        logger.info(f"Settings loaded from {_SETTINGS_FILE}")
    except Exception as e:
        logger.info(f"No saved settings found, using defaults")

def save_settings_to_file():
    """Save settings to file."""
    global IRC_PORT, IRC_PASSWORD, IRC_PASSWORD_ENABLED, BNC_BUFFER_LIMIT, BNC_ENABLED, BNC_KEEP_AFTER_REPLAY
    try:
        import json
        data = {
            'irc_port': IRC_PORT,
            'irc_password': IRC_PASSWORD,
            'irc_password_enabled': IRC_PASSWORD_ENABLED,
            'buffer_size': BNC_BUFFER_LIMIT,
            'bnc_enabled': BNC_ENABLED,
            'keep_after_replay': BNC_KEEP_AFTER_REPLAY
        }
        with open(_SETTINGS_FILE, 'w') as f:
            json.dump(data, f)
        logger.info(f"Settings saved to {_SETTINGS_FILE}")
    except Exception as e:
        logger.warning(f"Failed to save settings: {e}")

# IRC state
_irc_server_task: Optional[asyncio.Task] = None
_irc_server: Optional['IRCServer'] = None
_sse_task: Optional[asyncio.Task] = None
_db_path: str = None
_settings: Dict[str, Any] = {}
_local_node_id: str = None  # Track local node ID for op status


# ===========================================================================
# Database Functions
# ===========================================================================

def get_db_path() -> str:
    """Get the database path for the IRC buffer."""
    global _db_path
    if _db_path is None:
        # Use a temp directory or current directory
        import tempfile
        _db_path = "/tmp/mesh_irc_buffer.db"
    return _db_path


def init_db():
    """Initialize the database for message buffering."""
    db_path = get_db_path()
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS irc_buffer (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            channel_name TEXT NOT NULL,
            from_nick TEXT NOT NULL,
            from_id TEXT,
            message TEXT NOT NULL,
            timestamp REAL NOT NULL
        )
    """)
    conn.commit()
    conn.close()
    logger.info(f"IRC buffer database initialized at {db_path}")


def buffer_message(channel_name: str, from_nick: str, from_id: str, message: str):
    """Buffer a message to the database."""
    try:
        db_path = get_db_path()
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        
        # Insert the message
        cursor.execute(
            "INSERT INTO irc_buffer (channel_name, from_nick, from_id, message, timestamp) VALUES (?, ?, ?, ?, ?)",
            (channel_name, from_nick, from_id, message, time.time())
        )
        
        # Delete old messages beyond the limit (per channel)
        cursor.execute("""
            DELETE FROM irc_buffer 
            WHERE channel_name = ? AND id NOT IN (
                SELECT id FROM irc_buffer 
                WHERE channel_name = ? 
                ORDER BY timestamp DESC 
                LIMIT ?
            )
        """, (channel_name, channel_name, BNC_BUFFER_LIMIT))
        
        conn.commit()
        conn.close()
    except Exception as e:
        logger.warning(f"Failed to buffer message: {e}")


def get_buffered_messages(channel_name: str = None) -> List[Dict]:
    """Get buffered messages, optionally filtered by channel."""
    try:
        db_path = get_db_path()
        conn = sqlite3.connect(db_path)
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()
        
        if channel_name:
            cursor.execute(
                "SELECT * FROM irc_buffer WHERE channel_name = ? ORDER BY timestamp ASC LIMIT ?",
                (channel_name, BNC_BUFFER_LIMIT)
            )
        else:
            cursor.execute(
                "SELECT * FROM irc_buffer ORDER BY timestamp ASC LIMIT ?",
                (BNC_BUFFER_LIMIT * 10,)
            )
        
        rows = cursor.fetchall()
        conn.close()
        
        return [dict(row) for row in rows]
    except Exception as e:
        logger.warning(f"Failed to get buffered messages: {e}")
        return []


def clear_buffered_messages(channel_name: str = None):
    """Clear buffered messages, optionally for a specific channel."""
    try:
        db_path = get_db_path()
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        
        if channel_name:
            cursor.execute("DELETE FROM irc_buffer WHERE channel_name = ?", (channel_name,))
        else:
            cursor.execute("DELETE FROM irc_buffer")
        
        conn.commit()
        conn.close()
        logger.info(f"Cleared IRC buffer" + (f" for {channel_name}" if channel_name else ""))
    except Exception as e:
        logger.warning(f"Failed to clear buffer: {e}")


# ===========================================================================
# IRC Server Classes
# ===========================================================================

class IRCClient:
    """Represents a connected IRC client."""
    
    def __init__(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
        self.reader = reader
        self.writer = writer
        self.nickname: str = ""
        self.username: str = ""
        self.realname: str = ""
        self.host: str = ""
        self.channels: Dict[str, 'IRCChannel'] = {}
        self.registered: bool = False
        self._buffer: str = ""
        self.password_ok: bool = False  # Password validated
    
    @property
    def nick(self) -> str:
        return self.nickname or "*"
    
    async def send(self, message: str):
        """Send a raw IRC message to the client."""
        try:
            self.writer.write(f"{message}\r\n".encode())
            await self.writer.drain()
        except Exception as e:
            logger.warning(f"Failed to send to {self.nick}: {e}")
    
    async def send_numeric(self, numeric: int, *params: str):
        """Send a numeric reply."""
        if numeric >= 400:
            msg = f":{IRC_SERVER_NAME} {numeric} {self.nickname} {' '.join(params)}"
        else:
            msg = f":{IRC_SERVER_NAME} {numeric} {self.nickname} {' '.join(params)}"
        await self.send(msg)
    
    async def privmsg(self, target: str, message: str):
        await self.send(f":{self.nickname}!{self.username}@{IRC_SERVER_NAME} PRIVMSG {target} :{message}")
    
    async def notice(self, target: str, message: str):
        await self.send(f":{IRC_SERVER_NAME} NOTICE {target} :{message}")


class IRCChannel:
    def __init__(self, name: str, mesh_channel_index: int = None):
        self.name = name
        self.mesh_channel_index = mesh_channel_index
        self.clients: Dict[str, IRCClient] = {}
        self.topic: str = ""
        self.modes: str = ""
    
    def add_client(self, client: IRCClient):
        self.clients[client.nickname] = client
    
    def remove_client(self, client: IRCClient):
        if client.nickname in self.clients:
            del self.clients[client.nickname]
    
    async def broadcast(self, sender: IRCClient, message: str, include_sender: bool = True):
        full_msg = f":{sender.nickname}!{sender.username}@{IRC_SERVER_NAME} PRIVMSG {self.name} :{message}"
        for client in list(self.clients.values()):
            if client != sender or include_sender:
                try:
                    await client.send(full_msg)
                except Exception:
                    pass


class VirtualMeshClient:
    def __init__(self, node_id: str, long_name: str = None, short_name: str = None):
        self.node_id = node_id
        self.long_name = long_name or node_id
        self.short_name = short_name or node_id
        # Format: long_name-short_name (e.g., "GergosPhone-grgMob")
        self.irc_nickname = self._create_irc_nickname(long_name or node_id, short_name)
        self.username = node_id
        self.realname = long_name or node_id
        self.last_seen = None  # Will be updated from mesh data
        self.voice_by_channel: Dict[str, bool] = {}  # Track voice status per channel
    
    @staticmethod
    def _sanitize_part(nick: str) -> str:
        """Remove special characters from a nick part."""
        if not nick:
            return ""
        sanitized = re.sub(r'[^a-zA-Z0-9_\-\[\]\\`]', '', nick)
        return sanitized[:20]  # Limit each part to 20 chars
    
    @classmethod
    def _create_irc_nickname(cls, long_name: str, short_name: str = None) -> str:
        """Create IRC nick in format: long_name-short_name"""
        long_sanitized = cls._sanitize_part(long_name)
        short_sanitized = cls._sanitize_part(short_name) if short_name else ""
        
        if short_sanitized:
            nick = f"{long_sanitized}-{short_sanitized}"
        else:
            nick = long_sanitized
        
        if not nick:
            return "meshnode"
        
        return nick[:50]  # IRC nicks max 50 chars
    
    def get_channel_prefix(self) -> str:
        """Get the channel prefix based on last activity.
        @ = op (local node only)
        + = voice (active in last hour)
        (nothing) = regular
        """
        if self.node_id == _local_node_id:
            return "@"  # Local node is always op
        if self.last_seen and (time.time() - self.last_seen) < 3600:
            return "+"
        return ""


class IRCServer:
    def __init__(self):
        self.clients: Dict[str, IRCClient] = {}
        self.channels: Dict[str, IRCChannel] = {}
        self.virtual_clients: Dict[str, VirtualMeshClient] = {}
        self.pending_clients: List[IRCClient] = []
        self._running: bool = False
        self.mesh_channels: Dict[int, str] = {}
    
    def _sanitize_nick(self, nick: str) -> str:
        base = VirtualMeshClient._create_irc_nickname(nick, None)
        if base not in self.clients and base not in [v.irc_nickname for v in self.virtual_clients.values()]:
            return base
        for i in range(1, 1000):
            test = f"{base}{i}"
            if test not in self.clients and test not in [v.irc_nickname for v in self.virtual_clients.values()]:
                return test
        return f"meshnode_{int(time.time()) % 10000}"
    
    async def handle_client(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
        addr = writer.get_extra_info('peername')
        logger.info(f"New IRC connection from {addr}")
        
        client = IRCClient(reader, writer)
        self.pending_clients.append(client)
        
        try:
            while self._running:
                try:
                    data = await asyncio.wait_for(reader.read(1024), timeout=60)
                except asyncio.TimeoutError:
                    await client.send(f"PING :{IRC_SERVER_NAME}")
                    continue
                
                if not data:
                    break
                
                client._buffer += data.decode('utf-8', errors='ignore')
                
                while '\n' in client._buffer:
                    line, client._buffer = client._buffer.split('\n', 1)
                    line = line.strip()
                    if line:
                        await self.handle_command(client, line)
        
        except Exception as e:
            logger.warning(f"Client error: {e}")
        finally:
            await self.disconnect_client(client)
            writer.close()
            await writer.wait_closed()
            logger.info(f"IRC client disconnected: {addr}")
    
    async def disconnect_client(self, client: IRCClient):
        for channel in list(client.channels.values()):
            channel.remove_client(client)
            if not channel.clients:
                del self.channels[channel.name]
        
        if client.nickname in self.clients:
            del self.clients[client.nickname]
        
        if client in self.pending_clients:
            self.pending_clients.remove(client)
    
    async def handle_command(self, client: IRCClient, line: str):
        parts = line.split()
        if not parts:
            return
        
        command = parts[0].upper()
        
        if command == "NICK":
            await self.cmd_nick(client, parts)
        elif command == "USER":
            await self.cmd_user(client, parts)
        elif command == "JOIN":
            await self.cmd_join(client, parts)
        elif command == "PART":
            await self.cmd_part(client, parts)
        elif command == "PRIVMSG":
            await self.cmd_privmsg(client, parts)
        elif command == "NOTICE":
            await self.cmd_notice(client, parts)
        elif command == "QUIT":
            await self.cmd_quit(client, parts)
        elif command == "PING":
            await self.cmd_ping(client, parts)
        elif command == "NAMES":
            await self.cmd_names(client, parts)
        elif command == "LIST":
            await self.cmd_list(client, parts)
        elif command == "WHO":
            await self.cmd_who(client, parts)
        elif command == "WHOIS":
            await self.cmd_whois(client, parts)
        elif command == "MODE":
            await self.cmd_mode(client, parts)
        elif command == "TOPIC":
            await self.cmd_topic(client, parts)
        elif command == "REPLAYBUFFER":
            await self.cmd_replay_buffer(client, parts)
        elif command == "CLEARBUFFER":
            await self.cmd_clear_buffer(client, parts)
        elif command == "PASS":
            await self.cmd_pass(client, parts)
    
    async def cmd_pass(self, client: IRCClient, parts: List[str]):
        """Handle PASS command for authentication."""
        global IRC_PASSWORD, IRC_PASSWORD_ENABLED
        
        # Skip password check if not required
        if not IRC_PASSWORD_ENABLED:
            client.password_ok = True
            return
        
        if IRC_PASSWORD is None:
            # Get password from local node's short_name
            long_name, short_name = self.get_local_node_info()
            if short_name:
                IRC_PASSWORD = short_name
            else:
                IRC_PASSWORD = long_name.replace(' ', '')[:20]  # Use first 20 chars of long_name
            logger.info(f"IRC password set to: {IRC_PASSWORD}")
        
        if len(parts) < 2:
            return
        
        password = parts[1].lstrip(':')
        if password == IRC_PASSWORD:
            client.password_ok = True
            logger.info(f"IRC: Password accepted for connection from {client.host}")
            # If we already have NICK and USER, try to complete registration
            if client.nickname and client.username:
                await self.complete_registration(client)
        else:
            await client.send(f":{IRC_SERVER_NAME} ERROR :Invalid password")
            logger.warning(f"IRC: Invalid password attempt from {client.host}")
    
    async def cmd_nick(self, client: IRCClient, parts: List[str]):
        if len(parts) < 2:
            return
        new_nick = parts[1].split('!')[0]
        if client.registered:
            # Cannot change nickname - auto-set to local node
            await client.notice(client.nickname, "Cannot change nickname - it is auto-set to your node's name")
            return
        else:
            client.nickname = self._sanitize_nick(new_nick)
    
    async def cmd_user(self, client: IRCClient, parts: List[str]):
        if len(parts) < 5:
            return
        client.username = parts[1]
        client.realname = ' '.join(parts[4:])[1:] if parts[4].startswith(':') else parts[4]
        if client.nickname and client.username:
            await self.complete_registration(client)
    
    def get_local_node_info(self) -> tuple:
        """Get local node's long_name and short_name."""
        meshtastic_data = core_context.get("meshtastic_data")
        if meshtastic_data:
            nodes = getattr(meshtastic_data, 'nodes', {})
            for node_id, node in nodes.items():
                if node.get('isLocal', False):
                    return (
                        node.get('long_name', 'MeshUser'),
                        node.get('short_name', None)
                    )
        return ('MeshUser', None)
    
    def get_channel_nicklist(self, channel: IRCChannel) -> str:
        """Get channel nicklist with prefixes."""
        nicks = []
        # Add connected IRC client
        for nick in channel.clients.keys():
            nicks.append(nick)
        # Add virtual mesh clients with prefixes
        for node_id, virtual in self.virtual_clients.items():
            prefix = virtual.get_channel_prefix()
            nicks.append(f"{prefix}{virtual.irc_nickname}")
        return ' '.join(nicks)
    
    async def refresh_all_nicklists(self):
        """Refresh nicklist for all channels - called when voice status may have changed."""
        for channel_name, channel in self.channels.items():
            nicklist = self.get_channel_nicklist(channel)
            # Broadcast updated nicklist to all clients in this channel
            for client in list(channel.clients.values()):
                await client.send(f":{IRC_SERVER_NAME} 353 {client.nickname} = {channel_name} :{nicklist}")
                await client.send(f":{IRC_SERVER_NAME} 366 {client.nickname} {channel_name} :End of NAMES list")
    
    async def update_voice_status(self, virtual: VirtualMeshClient, channel_name: str):
        """Send MODE commands to update voice status for a node in a channel."""
        should_have_voice = virtual.get_channel_prefix() == "+"
        current_voice = virtual.voice_by_channel.get(channel_name, False)
        
        # Only send MODE if status actually changed
        if should_have_voice and not current_voice:
            # Give voice +v - send to ALL connected clients
            for client in self.clients.values():
                await client.send(f":{IRC_SERVER_NAME} MODE {channel_name} +v {virtual.irc_nickname}")
            virtual.voice_by_channel[channel_name] = True
            logger.info(f"VOICE: Gave +v to {virtual.irc_nickname} in {channel_name}")
        elif not should_have_voice and current_voice:
            # Remove voice -v - send to ALL connected clients
            for client in self.clients.values():
                await client.send(f":{IRC_SERVER_NAME} MODE {channel_name} -v {virtual.irc_nickname}")
            virtual.voice_by_channel[channel_name] = False
            logger.info(f"VOICE: Removed +v from {virtual.irc_nickname} in {channel_name}")
    
    async def check_all_voice_status(self):
        """Check and update voice status for all virtual clients in ALL mesh channels."""
        # Get channels from all connected IRC clients
        all_mesh_channels = set()
        for client in self.clients.values():
            all_mesh_channels.update(client.channels.keys())
        
        # Also add mesh_channels from API
        for channel_index, channel_name in self.mesh_channels.items():
            irc_name = f"#{channel_name}"
            all_mesh_channels.add(irc_name)
        
        for channel_name in all_mesh_channels:
            # Make sure channel exists in our tracking
            if channel_name not in self.channels:
                chan_short = channel_name.lstrip('#')
                mesh_idx = None
                for idx, name in self.mesh_channels.items():
                    if name == chan_short:
                        mesh_idx = idx
                        break
                self.channels[channel_name] = IRCChannel(channel_name, mesh_idx)
            
            for virtual in self.virtual_clients.values():
                await self.update_voice_status(virtual, channel_name)
    
    async def complete_registration(self, client: IRCClient):
        if client.registered:
            return
        
        # Check password if required
        global IRC_PASSWORD_ENABLED
        if IRC_PASSWORD_ENABLED and not client.password_ok:
            await client.send(f":{IRC_SERVER_NAME} ERROR :Password required")
            await client.send(f":{IRC_SERVER_NAME} NOTICE {client.nickname} :Use /PASS <password> to connect")
            return
        
        # Get local node info and create nick in same format as virtual clients
        long_name, short_name = self.get_local_node_info()
        local_nick = VirtualMeshClient._create_irc_nickname(long_name, short_name)
        
        # Check if the nick is already taken
        if local_nick in self.clients:
            await client.send_numeric(433, f"{local_nick} :Nickname is already in use")
            return
        
        client.nickname = local_nick
        
        await refresh_mesh_state()
        
        client.registered = True
        self.clients[client.nickname] = client
        if client in self.pending_clients:
            self.pending_clients.remove(client)
        
        await client.send(f":{IRC_SERVER_NAME} 001 {client.nickname} :Welcome to Mesh IRC {client.nickname}!{client.username}@{IRC_SERVER_NAME}")
        await client.send(f":{IRC_SERVER_NAME} 002 {client.nickname} :Your host is {IRC_SERVER_NAME}")
        await client.send(f":{IRC_SERVER_NAME} 003 {client.nickname} :This server was created for Meshtastic mesh bridging")
        await client.send(f":{IRC_SERVER_NAME} 004 {client.nickname} {IRC_SERVER_NAME} i :")
        
        # Auto-join all mesh channels and replay buffered messages
        channels_joined = False
        for channel_index, channel_name in self.mesh_channels.items():
            if channel_name:
                full_name = f"#{channel_name}"
                if full_name not in self.channels:
                    self.channels[full_name] = IRCChannel(full_name, channel_index)
                channel = self.channels[full_name]
                channel.add_client(client)
                client.channels[full_name] = channel
                await client.send(f":{client.nickname}!{client.username}@{IRC_SERVER_NAME} JOIN {full_name}")
                if channel.topic:
                    await client.send(f":{IRC_SERVER_NAME} 332 {client.nickname} {full_name} :{channel.topic}")
                # Send nicklist with prefixes
                nicklist = self.get_channel_nicklist(channel)
                if nicklist:
                    await client.send(f":{IRC_SERVER_NAME} 353 {client.nickname} = {full_name} :{nicklist}")
                await client.send(f":{IRC_SERVER_NAME} 366 {client.nickname} {full_name} :End of NAMES list")
                
                # Replay buffered messages for this channel (using index-based key)
                buffer_key = f"ch{channel_index}"
                await self.replay_buffer(client, full_name, buffer_key)
                
                logger.info(f"Auto-joined {client.nickname} to {full_name}")
                channels_joined = True
        
        
        if self.virtual_clients:
            # Send virtual clients as separate list
            virt_list = []
            for v in self.virtual_clients.values():
                prefix = v.get_channel_prefix()
                virt_list.append(f"{prefix}{v.irc_nickname}")
            await client.send(f":{IRC_SERVER_NAME} 353 {client.nickname} = * :{' '.join(virt_list)}")
            await client.send(f":{IRC_SERVER_NAME} 366 {client.nickname} * :End of NAMES list")
        
        logger.info(f"IRC client registered: {client.nickname}")
        
        # Check for any DM buffers for this user
        dm_buffer_keys = [f"dm_{client.nickname}"]
        for key in dm_buffer_keys:
            dm_msgs = get_buffered_messages(key)
            if dm_msgs:
                await client.notice(client.nickname, f"--- You have {len(dm_msgs)} offline DMs ---")
                for msg in dm_msgs:
                    import datetime
                    ts = datetime.datetime.fromtimestamp(msg['timestamp'])
                    time_str = ts.strftime('%Y-%m-%d %H:%M')
                    from_nick = msg.get('from_nick', 'unknown')
                    from_id = msg.get('from_id', '')
                    formatted = f"[{time_str}] {msg['message']}"
                    await client.send(f":{from_nick}!{from_id}@{IRC_SERVER_NAME} PRIVMSG {client.nickname} :{formatted}")
                await client.notice(client.nickname, f"--- End of offline DMs ---")
                # Only clear if keep_after_replay is disabled
                if not BNC_KEEP_AFTER_REPLAY:
                    clear_buffered_messages(key)
                else:
                    await client.notice(client.nickname, "Messages kept in buffer (keep_after_replay is ON)")
    
    async def replay_buffer(self, client: IRCClient, channel_name: str, buffer_key: str = None):
        """Replay buffered messages for a channel."""
        if buffer_key is None:
            buffer_key = channel_name
        
        # Check both new format (ch0, ch1) and old format (#channelName)
        all_keys = [buffer_key]
        if buffer_key.startswith('ch') and channel_name.startswith('#'):
            all_keys.append(channel_name)  # Also check old format
        
        logger.info(f"BNC: Checking buffer for channel: {channel_name} (keys: {all_keys})")
        
        messages = []
        for key in all_keys:
            msgs = get_buffered_messages(key)
            if msgs:
                messages.extend(msgs)
                logger.info(f"BNC: Found {len(msgs)} messages with key: {key}")
        
        logger.info(f"BNC: Total found {len(messages)} buffered messages")
        
        if messages:
            await client.notice(client.nickname, f"--- Offline messages for {channel_name} ---")
            for msg in messages:
                # Format: [YYYY-MM-DD HH:MM] message (from actual node nick)
                import datetime
                ts = datetime.datetime.fromtimestamp(msg['timestamp'])
                time_str = ts.strftime('%Y-%m-%d %H:%M')
                from_nick = msg.get('from_nick', 'unknown')
                from_id = msg.get('from_id', '')
                formatted = f"[{time_str}] {msg['message']}"
                # Use the actual node's nick as sender, not mesh.local
                await client.send(f":{from_nick}!{from_id}@{IRC_SERVER_NAME} PRIVMSG {channel_name} :{formatted}")
            await client.notice(client.nickname, f"--- End of offline messages ({len(messages)} messages) ---")
            # Clear buffers unless keep_after_replay is enabled
            if not BNC_KEEP_AFTER_REPLAY:
                for key in all_keys:
                    clear_buffered_messages(key)
            else:
                await client.notice(client.nickname, "Messages kept in buffer (keep_after_replay is ON)")
    
    async def cmd_join(self, client: IRCClient, parts: List[str]):
        if not client.registered:
            await client.send_numeric(451, ":You have not registered")
            return
        if len(parts) < 2:
            return
        
        for chan_name in parts[1].split(','):
            chan_name = chan_name.lstrip('#')
            full_name = f"#{chan_name}"
            
            mesh_index = None
            for idx, name in self.mesh_channels.items():
                if name == chan_name:
                    mesh_index = idx
                    break
            
            if full_name not in self.channels:
                self.channels[full_name] = IRCChannel(full_name, mesh_index)
            
            channel = self.channels[full_name]
            channel.add_client(client)
            client.channels[full_name] = channel
            
            await client.send(f":{client.nickname}!{client.username}@{IRC_SERVER_NAME} JOIN {full_name}")
            if channel.topic:
                await client.send(f":{IRC_SERVER_NAME} 332 {client.nickname} {full_name} :{channel.topic}")
            # Send nicklist with prefixes
            nicklist = self.get_channel_nicklist(channel)
            if nicklist:
                await client.send(f":{IRC_SERVER_NAME} 353 {client.nickname} = {full_name} :{nicklist}")
            await client.send(f":{IRC_SERVER_NAME} 366 {client.nickname} {full_name} :End of NAMES list")
            
            # Replay buffered messages using index-based key
            if mesh_index is not None:
                buffer_key = f"ch{mesh_index}"
                await self.replay_buffer(client, full_name, buffer_key)
            else:
                await self.replay_buffer(client, full_name)
    
    async def cmd_part(self, client: IRCClient, parts: List[str]):
        # Users cannot part from channels - auto-joined only
        await client.notice(client.nickname, "Cannot part from channels - you are auto-joined to mesh channels")
    
    async def cmd_privmsg(self, client: IRCClient, parts: List[str]):
        if not client.registered or len(parts) < 3:
            return
        
        target = parts[1]
        message = (' '.join(parts[2:])[1:] if parts[2].startswith(':') else ' '.join(parts[2:]))
        
        if target.startswith('#'):
            await self.handle_channel_message(client, target, message)
        else:
            await self.handle_private_message(client, target, message)
    
    async def handle_channel_message(self, client: IRCClient, channel_name: str, message: str):
        if channel_name not in client.channels:
            await client.send_numeric(442, f"{channel_name} :You're not on that channel")
            return
        
        channel = client.channels[channel_name]
        await channel.broadcast(client, message, include_sender=False)
        await self.forward_to_mesh_channel(channel, message)
    
    async def handle_private_message(self, client: IRCClient, target: str, message: str):
        # First check if target is a virtual client by irc_nickname
        for node_id, virtual in self.virtual_clients.items():
            if virtual.irc_nickname == target:
                await self.forward_to_mesh_node(node_id, message)
                return
        
        # Check by node_id directly
        if target in self.virtual_clients:
            virtual = self.virtual_clients[target]
            await self.forward_to_mesh_node(virtual.node_id, message)
            return
        
        # Check if target is another connected IRC client
        if target in self.clients:
            await self.clients[target].privmsg(client.nickname, message)
            return
        
        # If target starts with !, treat it as a node_id
        if target.startswith('!'):
            await self.forward_to_mesh_node(target, message)
            return
        
        await client.send_numeric(401, f"{target} :No such nick/channel")
    
    async def cmd_notice(self, client: IRCClient, parts: List[str]):
        if len(parts) < 3:
            return
        target = parts[1]
        message = (' '.join(parts[2:]))[1:] if parts[2].startswith(':') else ' '.join(parts[2:])
        if target in self.clients:
            await self.clients[target].notice(client.nickname, message)
    
    async def cmd_quit(self, client: IRCClient, parts: List[str]):
        await self.disconnect_client(client)
    
    async def cmd_ping(self, client: IRCClient, parts: List[str]):
        server = parts[1][1:] if len(parts) > 1 and parts[1].startswith(':') else (parts[1] if len(parts) > 1 else IRC_SERVER_NAME)
        await client.send(f"PONG :{server}")
    
    async def cmd_names(self, client: IRCClient, parts: List[str]):
        if not client.registered:
            return
        if len(parts) < 2:
            for channel in self.channels.values():
                nicklist = self.get_channel_nicklist(channel)
                await client.send(f":{IRC_SERVER_NAME} 353 {client.nickname} = {channel.name} :{nicklist}")
            await client.send(f":{IRC_SERVER_NAME} 366 {client.nickname} :End of NAMES list")
        else:
            channel_name = parts[1].split(',')[0]
            if channel_name in self.channels:
                channel = self.channels[channel_name]
                nicklist = self.get_channel_nicklist(channel)
                await client.send(f":{IRC_SERVER_NAME} 353 {client.nickname} = {channel_name} :{nicklist}")
            await client.send(f":{IRC_SERVER_NAME} 366 {client.nickname} {channel_name} :End of NAMES list")
    
    async def cmd_list(self, client: IRCClient, parts: List[str]):
        if not client.registered:
            return
        for channel in self.channels.values():
            await client.send(f":{IRC_SERVER_NAME} 322 {client.nickname} {channel.name} {len(channel.clients)} :{channel.topic or 'No topic'}")
        await client.send(f":{IRC_SERVER_NAME} 323 {client.nickname} :End of LIST")
    
    async def cmd_who(self, client: IRCClient, parts: List[str]):
        if len(parts) < 2:
            return
        target = parts[1]
        if target.startswith('#') and target in self.channels:
            for nick, c in self.channels[target].clients.items():
                await client.send(f":{IRC_SERVER_NAME} 352 {client.nickname} * {c.username} {IRC_SERVER_NAME} {c.nickname} H :0 {c.realname}")
        elif target in self.clients:
            c = self.clients[target]
            await client.send(f":{IRC_SERVER_NAME} 352 {client.nickname} * {c.username} {IRC_SERVER_NAME} {c.nickname} H :0 {c.realname}")
        elif target in self.virtual_clients:
            v = self.virtual_clients[target]
            await client.send(f":{IRC_SERVER_NAME} 352 {client.nickname} * {v.username} {IRC_SERVER_NAME} {v.irc_nickname} H :0 {v.realname}")
        await client.send(f":{IRC_SERVER_NAME} 315 {client.nickname} {target} :End of WHO list")
    
    async def cmd_whois(self, client: IRCClient, parts: List[str]):
        if len(parts) < 2:
            return
        target = parts[1]
        
        # Check if target is a connected IRC client
        if target in self.clients:
            c = self.clients[target]
            await client.send(f":{IRC_SERVER_NAME} 311 {client.nickname} {c.nickname} {c.username} {IRC_SERVER_NAME} * :{c.realname}")
            await client.send(f":{IRC_SERVER_NAME} 318 {client.nickname} {target} :End of WHOIS list")
            return
        
        # Check if target is a virtual mesh client (by irc_nickname or node_id)
        virtual = None
        for node_id, v in self.virtual_clients.items():
            if v.irc_nickname == target or node_id == target:
                virtual = v
                break
        
        if virtual:
            # Get node info from the virtual client (which is updated via periodic refresh)
            node_info = {}
            # Try to get fresh data from virtual client's stored node data
            if hasattr(virtual, '_node_data'):
                node_info = virtual._node_data
            
            # Build rich WHOIS response
            await client.send(f":{IRC_SERVER_NAME} 311 {client.nickname} {virtual.irc_nickname} {virtual.username} {IRC_SERVER_NAME} * :{virtual.long_name}")
            
            # Additional info lines (using 307 for custom info)
            if virtual.node_id == _local_node_id:
                await client.send(f":{IRC_SERVER_NAME} 307 {client.nickname} {virtual.irc_nickname} :isLocalNode")
            
            # Use last_seen from virtual client (updated periodically)
            last_heard = virtual.last_seen if virtual.last_seen and virtual.last_seen > 1000000000 else None
            if last_heard:
                import datetime
                try:
                    ts = datetime.datetime.fromtimestamp(last_heard)
                    ago = datetime.datetime.now() - ts
                    if ago.days > 0:
                        last_heard_str = f"{ago.days}d ago"
                    elif ago.seconds >= 3600:
                        last_heard_str = f"{ago.seconds // 3600}h ago"
                    elif ago.seconds >= 60:
                        last_heard_str = f"{ago.seconds // 60}m ago"
                    else:
                        last_heard_str = f"{ago.seconds}s ago"
                    await client.send(f":{IRC_SERVER_NAME} 307 {client.nickname} {virtual.irc_nickname} :lastHeard={last_heard_str}")
                except:
                    pass
            
            hw_model = node_info.get('hw_model') or node_info.get('hardware')
            if hw_model:
                await client.send(f":{IRC_SERVER_NAME} 307 {client.nickname} {virtual.irc_nickname} :hwModel={hw_model}")
            
            await client.send(f":{IRC_SERVER_NAME} 312 {client.nickname} {virtual.irc_nickname} {IRC_SERVER_NAME} :Meshtastic Node ({virtual.node_id})")
            await client.send(f":{IRC_SERVER_NAME} 318 {client.nickname} {virtual.irc_nickname} :End of WHOIS list")
            return
        
        await client.send(f":{IRC_SERVER_NAME} 401 {client.nickname} {target} :No such nick/channel")
    
    async def cmd_mode(self, client: IRCClient, parts: List[str]):
        await client.send(f":{IRC_SERVER_NAME} 221 {client.nickname} +i")
    
    async def cmd_topic(self, client: IRCClient, parts: List[str]):
        if len(parts) < 2:
            return
        channel_name = parts[1]
        if channel_name not in self.channels:
            await client.send_numeric(442, f"{channel_name} :You're not on that channel")
            return
        channel = self.channels[channel_name]
        if len(parts) == 2:
            if channel.topic:
                await client.send(f":{IRC_SERVER_NAME} 332 {client.nickname} {channel_name} :{channel.topic}")
            else:
                await client.send(f":{IRC_SERVER_NAME} 331 {client.nickname} {channel_name} :No topic is set")
        else:
            if channel_name in client.channels:
                channel.topic = ' '.join(parts[2:])[1:] if parts[2].startswith(':') else parts[2]
                await client.send(f":{client.nickname}!{client.username}@{IRC_SERVER_NAME} TOPIC {channel_name} :{channel.topic}")
    
    async def cmd_replay_buffer(self, client: IRCClient, parts: List[str]):
        """Replay buffered messages manually as PRIVMSG to channels or DMs."""
        if not client.registered:
            await client.send_numeric(451, ":You have not registered")
            return
        
        # Get all buffered messages
        messages = get_buffered_messages()
        
        if not messages:
            await client.notice(client.nickname, "No buffered messages found.")
            return
        
        await client.notice(client.nickname, f"--- Replaying {len(messages)} buffered messages ---")
        
        # Group by channel
        by_channel = {}
        dm_messages = []
        
        for msg in messages:
            ch = msg.get('channel_name', 'unknown')
            # Check if this is a DM (starts with dm_)
            if ch.startswith('dm_'):
                dm_messages.append(msg)
            else:
                if ch not in by_channel:
                    by_channel[ch] = []
                by_channel[ch].append(msg)
        
        # Replay channel messages
        for channel_key, msgs in by_channel.items():
            # Convert buffer key (ch0, ch1) to IRC channel name if needed
            if channel_key.startswith('ch'):
                idx = int(channel_key[2:])
                channel_name = f"#{self.mesh_channels.get(idx, channel_key)}"
            else:
                channel_name = channel_key
            
            # Send to the channel if user is in it, otherwise as notice
            if channel_name in client.channels:
                for msg in msgs:
                    import datetime
                    ts = datetime.datetime.fromtimestamp(msg['timestamp'])
                    time_str = ts.strftime('%Y-%m-%d %H:%M')
                    from_nick = msg.get('from_nick', 'unknown')
                    from_id = msg.get('from_id', '')
                    formatted = f"[{time_str}] {msg['message']}"
                    await client.send(f":{from_nick}!{from_id}@{IRC_SERVER_NAME} PRIVMSG {channel_name} :{formatted}")
            else:
                await client.notice(client.nickname, f"--- Channel: {channel_name} ---")
                for msg in msgs:
                    import datetime
                    ts = datetime.datetime.fromtimestamp(msg['timestamp'])
                    time_str = ts.strftime('%Y-%m-%d %H:%M')
                    from_nick = msg.get('from_nick', 'unknown')
                    formatted = f"[{time_str}] <{from_nick}> {msg['message']}"
                    await client.notice(client.nickname, formatted)
        
        # Replay DM messages directly to the client
        for msg in dm_messages:
            import datetime
            ts = datetime.datetime.fromtimestamp(msg['timestamp'])
            time_str = ts.strftime('%Y-%m-%d %H:%M')
            from_nick = msg.get('from_nick', 'unknown')
            from_id = msg.get('from_id', '')
            formatted = f"[{time_str}] {msg['message']}"
            # Send as PRIVMSG to the client (DM to self)
            await client.send(f":{from_nick}!{from_id}@{IRC_SERVER_NAME} PRIVMSG {client.nickname} :{formatted}")
        
        await client.notice(client.nickname, f"--- End of {len(messages)} messages ---")
        
        if not BNC_KEEP_AFTER_REPLAY:
            clear_buffered_messages()
            await client.notice(client.nickname, "Buffer cleared.")
    
    async def cmd_clear_buffer(self, client: IRCClient, parts: List[str]):
        """Clear buffered messages manually."""
        if not client.registered:
            await client.send_numeric(451, ":You have not registered")
            return
        
        clear_buffered_messages()
        await client.notice(client.nickname, "Buffer cleared.")
    
    async def cmd_tracepath(self, client: IRCClient, parts: List[str]):
        """Send a tracepath request to a mesh node and return results as PRIVMSG."""
        if not client.registered:
            await client.send_numeric(451, ":You have not registered")
            return
        
        if len(parts) < 2:
            await client.send(f":{IRC_SERVER_NAME} PRIVMSG {client.nickname} :Usage: /TRACE <nodename>")
            return
        
        target = parts[1]
        
        # Try to find the node ID from the nickname
        target_node_id = None
        
        # Check if it's a virtual client
        for node_id, virtual in self.virtual_clients.items():
            if virtual.irc_nickname == target or node_id == target:
                target_node_id = node_id
                break
        
        # If not found, check if target starts with ! (node ID)
        if not target_node_id and target.startswith('!'):
            target_node_id = target
        
        if not target_node_id:
            await client.send(f":{IRC_SERVER_NAME} PRIVMSG {client.nickname} :Node not found: {target}")
            return
        
        # Send tracepath request via connection manager
        import httpx
        try:
            async with httpx.AsyncClient(timeout=10.0) as http_client:
                # Use MeshDash API to send trace request
                response = await http_client.post(
                    "http://127.0.0.1:8000/api/meshtastic/send",
                    json={
                        "payload": {
                            "to": target_node_id,
                            "channel": 0,
                            "text": "__trace__"
                        }
                    }
                )
                
                # For now, show a message that the trace was sent
                await client.send(f":{IRC_SERVER_NAME} PRIVMSG {client.nickname} :Trace request sent to {target}. Results will appear as they arrive.")
                logger.info(f"TRACE: Trace request sent to {target_node_id}")
                
        except Exception as e:
            logger.warning(f"TRACE: Failed to send trace request: {e}")
            await client.send(f":{IRC_SERVER_NAME} PRIVMSG {client.nickname} :Error sending trace request: {e}")
    
    async def forward_to_mesh_channel(self, channel: IRCChannel, message: str):
        cm = core_context.get("connection_manager")
        if not cm or not cm.is_ready.is_set():
            logger.warning("Cannot forward to mesh: not connected")
            return
        channel_index = channel.mesh_channel_index if channel.mesh_channel_index is not None else 0
        try:
            await cm.sendText(message, destinationId="^all", channelIndex=channel_index)
            logger.info(f"IRC->Mesh: to channel {channel_index}: {message}")
        except Exception as e:
            logger.error(f"Failed to send to mesh: {e}")
    
    async def forward_to_mesh_node(self, node_id: str, message: str):
        cm = core_context.get("connection_manager")
        if not cm or not cm.is_ready.is_set():
            logger.warning("Cannot forward to mesh: not connected")
            return
        try:
            await cm.sendText(message, destinationId=node_id, channelIndex=0)
            logger.info(f"IRC->Mesh DM: -> {node_id}: {message}")
        except Exception as e:
            logger.error(f"Failed to send DM to mesh: {e}")
    
    async def broadcast_mesh_message(self, from_node_id: str, from_name: str, channel_index: int, message: str, to_id: str = None):
        # Use channel INDEX for buffering (consistent across restarts)
        buffer_key = f"ch{channel_index}"
        
        channel_name = self.mesh_channels.get(channel_index, str(channel_index))
        channel_irc_name = f"#{channel_name}"
        
        # Check if this is a private message (to a specific node, not broadcast)
        is_private = to_id and to_id != "^all" and to_id != 4294967295
        
        # Get or create virtual client for this node to get the proper nick
        if from_node_id in self.virtual_clients:
            virtual = self.virtual_clients[from_node_id]
            nick = virtual.irc_nickname
        else:
            # Fallback: create a temp virtual client for this message
            from_name = get_node_name(from_node_id) or from_name
            virtual = VirtualMeshClient(from_node_id, from_name, None)
            nick = virtual.irc_nickname
        
        if is_private:
            # This is a private message - send only to the target IRC client if they're connected
            target_nick = None
            if to_id in self.virtual_clients:
                target_virtual = self.virtual_clients[to_id]
                target_nick = target_virtual.irc_nickname
            
            # Also check if there's an IRC client with this nick
            if target_nick and target_nick in self.clients:
                target_client = self.clients[target_nick]
                full_msg = f":{nick}!{from_node_id}@{IRC_SERVER_NAME} PRIVMSG {target_nick} :{message}"
                try:
                    await target_client.send(full_msg)
                except Exception:
                    pass
                logger.info(f"Mesh->IRC DM: {from_name} -> {target_nick}: {message}")
            else:
                # Buffer the private message for the RECIPIENT (target), not sender
                if BNC_ENABLED and target_nick:
                    buffer_key = f"dm_{target_nick}"
                    logger.info(f"BNC: Buffering DM for recipient {target_nick}: {message}")
                    buffer_message(buffer_key, nick, from_node_id, message)
            return
        
        # Regular channel message - broadcast to the channel
        if channel_irc_name not in self.channels:
            self.channels[channel_irc_name] = IRCChannel(channel_irc_name, channel_index)
        
        channel = self.channels[channel_irc_name]
        
        full_msg = f":{nick}!{from_node_id}@{IRC_SERVER_NAME} PRIVMSG {channel_irc_name} :{message}"
        
        has_clients = len(channel.clients) > 0
        
        for client in list(channel.clients.values()):
            try:
                await client.send(full_msg)
            except Exception:
                pass
        
        # Buffer message if no clients are in the channel (BNC feature)
        # Use channel INDEX (ch0, ch1, etc) for consistent buffering
        if not has_clients and BNC_ENABLED:
            logger.info(f"BNC: Buffering message for {buffer_key} (irc: {channel_irc_name}) from {nick}")
            buffer_message(buffer_key, nick, from_node_id, message)
        
        logger.info(f"Mesh->IRC: {from_name} on {channel_name}: {message}")
    
    async def update_mesh_nodes(self, nodes: Dict[str, Any]):
        global _local_node_id
        current_node_ids = set(self.virtual_clients.keys())
        new_node_ids = set(nodes.keys())
        
        for node_id, node in nodes.items():
            # Track local node
            if node.get('isLocal', False):
                _local_node_id = node_id
            
            if node_id not in self.virtual_clients:
                long_name = node.get('long_name', node_id)
                short_name = node.get('short_name')
                virtual = VirtualMeshClient(node_id, long_name, short_name)
                self.virtual_clients[node_id] = virtual
                logger.info(f"Added virtual client for mesh node: {long_name} ({node_id})")
            else:
                # Update last_seen if we have activity info
                # Use lastHeard (newer field) instead of last_heard
                last_heard = node.get('lastHeard') or node.get('last_heard')
                if last_heard and last_heard > 0:
                    self.virtual_clients[node_id].last_seen = last_heard
        
        for node_id in current_node_ids - new_node_ids:
            del self.virtual_clients[node_id]
    
    async def update_mesh_channels(self, channels: List[Dict[str, Any]]):
        old_mesh_channels = self.mesh_channels.copy()
        self.mesh_channels = {}
        
        for i, channel in enumerate(channels):
            name = channel.get('name', '').strip()
            if name:
                self.mesh_channels[i] = name
                irc_name = f"#{name}"
                if irc_name not in self.channels:
                    self.channels[irc_name] = IRCChannel(irc_name, i)
                ch = self.channels[irc_name]
                ch.topic = f"Mesh Channel: {name}"


# ===========================================================================
# SSE-based Mesh Message Reception
# ===========================================================================

def get_node_name(node_id: str) -> str:
    if not node_id or node_id == 'unknown':
        return 'Local'
    meshtastic_data = core_context.get("meshtastic_data")
    if meshtastic_data:
        nodes = getattr(meshtastic_data, 'nodes', {})
        if node_id in nodes:
            node = nodes[node_id]
            return node.get('short_name') or node.get('long_name') or node_id
    return node_id


async def listen_sse_messages():
    """Listen to MeshDash SSE stream for instant packet delivery."""
    import httpx
    
    while True:
        try:
            async with httpx.AsyncClient(timeout=30.0) as client:
                async with client.stream('GET', 'http://127.0.0.1:8000/sse') as response:
                    logger.info("SSE connection established")
                    event_type = None
                    async for line in response.aiter_lines():
                        if not _irc_server or not _irc_server._running:
                            break
                        
                        if line.startswith('event: '):
                            event_type = line[7:]
                        elif line.startswith('data: ') and event_type == 'packet':
                            try:
                                data = line[6:]
                                import json
                                packet = json.loads(data)
                                
                                packet_type = packet.get('packet_type', packet.get('app_packet_type', ''))
                                if packet_type == 'Message':
                                    from_id = packet.get('fromId') or str(packet.get('from', ''))
                                    if not from_id:
                                        continue
                                    
                                    from_name = get_node_name(from_id)
                                    if not from_name:
                                        from_name = from_id
                                    
                                    channel = packet.get('channel', 0)
                                    to_id = packet.get('toId') or packet.get('to')
                                    text = packet.get('decoded', {}).get('text', '')
                                    
                                    if text:
                                        logger.info(f"SSE Mesh message: from={from_id} name={from_name} ch={channel} to={to_id} text={text}")
                                        # Update last_seen when we receive a message from this node
                                        if from_id in _irc_server.virtual_clients:
                                            _irc_server.virtual_clients[from_id].last_seen = time.time()
                                            logger.info(f"SSE: Updated last_seen for {from_id} to now")
                                            # Refresh nicklists to update voice prefixes
                                            await _irc_server.refresh_all_nicklists()
                                        await _irc_server.broadcast_mesh_message(from_id, from_name, channel, text, to_id)
                            except Exception as e:
                                logger.warning(f"SSE parse error: {e}")
        
        except asyncio.CancelledError:
            logger.info("🛑 SSE listener stopped")
            return
        except Exception as e:
            logger.warning(f"SSE connection error: {e}, reconnecting in 5s...")
            await asyncio.sleep(5)


# ===========================================================================
# IRC Server Lifecycle
# ===========================================================================

async def start_irc_server():
    global _irc_server, _sse_task
    
    if _irc_server and _irc_server._running:
        logger.warning("IRC server already running")
        return
    
    _irc_server = IRCServer()
    _irc_server._running = True
    
    server = await asyncio.start_server(
        _irc_server.handle_client,
        '0.0.0.0',
        IRC_PORT
    )
    
    addr = server.sockets[0].getsockname()
    logger.info(f"IRC server listening on {addr[0]}:{addr[1]}")
    
    await refresh_mesh_state()
    
    # Start SSE listener
    loop = core_context.get("event_loop")
    if loop:
        _sse_task = asyncio.run_coroutine_threadsafe(listen_sse_messages(), loop)
    
    async with server:
        await server.serve_forever()


async def stop_irc_server():
    global _irc_server, _sse_task
    
    if _sse_task:
        _sse_task.cancel()
        _sse_task = None
    
    if _irc_server:
        _irc_server._running = False
        _irc_server = None
        logger.info("IRC server stopped")


async def refresh_mesh_state():
    if not _irc_server:
        return

    try:
        import httpx
        async with httpx.AsyncClient(timeout=5.0) as client:
            # Fetch fresh node data via API
            try:
                r = await client.get("http://127.0.0.1:8000/api/nodes")
                if r.status_code == 200:
                    nodes_data = r.json()
                    logger.info(f"REFRESH: Got nodes API response: {type(nodes_data)}")
                    # The API returns a dict like {'!nodeid': {...}, '!nodeid2': {...}}
                    if isinstance(nodes_data, dict):
                        # Check if it's already in nodeid: nodedata format
                        first_key = next(iter(nodes_data.keys())) if nodes_data else ""
                        if first_key and isinstance(nodes_data[first_key], dict):
                            # Already in correct format
                            nodes_dict = nodes_data
                            logger.info(f"REFRESH: Using {len(nodes_dict)} nodes (dict format)")
                        elif 'nodes' in nodes_data:
                            # Format is {nodes: [...]} 
                            nodes = nodes_data['nodes']
                            nodes_dict = {}
                            for node in nodes:
                                node_id = node.get('id') or node.get('user', {}).get('id')
                                if node_id:
                                    nodes_dict[node_id] = node
                            logger.info(f"REFRESH: Converted {len(nodes_dict)} nodes from list")
                        else:
                            nodes_dict = {}
                            logger.warning(f"REFRESH: Unknown dict format")
                    elif isinstance(nodes_data, list):
                        # Format is [...]
                        nodes_dict = {}
                        for node in nodes_data:
                            node_id = node.get('id') or node.get('user', {}).get('id')
                            if node_id:
                                nodes_dict[node_id] = node
                        logger.info(f"REFRESH: Converted {len(nodes_dict)} nodes from list")
                    else:
                        nodes_dict = {}
                        logger.warning(f"REFRESH: Unknown nodes format")
                    await _irc_server.update_mesh_nodes(nodes_dict)
            except Exception as e:
                logger.warning(f"Could not fetch nodes: {e}")
                # Fallback to core_context
                meshtastic_data = core_context.get("meshtastic_data")
                if meshtastic_data:
                    nodes = getattr(meshtastic_data, 'nodes', {})
                    await _irc_server.update_mesh_nodes(nodes)
            
            # Fetch channels via API
            try:
                r = await client.get("http://127.0.0.1:8000/api/channels")
                if r.status_code == 200:
                    channels = r.json()
                    await _irc_server.update_mesh_channels(channels)
            except Exception as e:
                logger.warning(f"Could not fetch channels: {e}")
            
            # Now check and update voice status (after nodes AND channels are updated)
            await _irc_server.refresh_all_nicklists()
            await _irc_server.check_all_voice_status()

    except Exception as e:
        logger.error(f"Error refreshing mesh state: {e}")


# ===========================================================================
# Watchdog Heartbeat
# ===========================================================================

async def _watchdog_heartbeat():
    while True:
        await asyncio.sleep(30)
        wd = core_context.get("plugin_watchdog")
        pid = core_context.get("plugin_id")
        if wd is not None and pid:
            wd[pid] = time.time()


# ===========================================================================
# Plugin Lifecycle
# ===========================================================================

def init_plugin(context: dict):
    global logger, _irc_server_task
    
    core_context.update(context)
    logger = core_context.get("logger") or logging.getLogger("mesh_irc")
    logger.info("✅ Mesh IRC plugin initializing…")
    
    # Load saved settings
    load_settings()
    
    # Initialize the database
    init_db()
    
    loop = core_context.get("event_loop")
    if loop is None:
        logger.warning("⚠️  event_loop not in context")
        return
    
    asyncio.run_coroutine_threadsafe(_watchdog_heartbeat(), loop)
    asyncio.run_coroutine_threadsafe(start_irc_server(), loop)
    logger.info("🚀 Mesh IRC server starting on port 6667")


def deinit_plugin():
    """Called when the plugin is stopped/deactivated."""
    global _irc_server
    
    logger.info("🛑 DEINIT: Mesh IRC plugin shutting down...")
    
    # Cancel all tasks
    if _irc_server_task:
        _irc_server_task.cancel()
    
    # Directly set running to False - the server loop will exit on next iteration
    if _irc_server:
        _irc_server._running = False
    
    # Cancel SSE task
    if _sse_task:
        _sse_task.cancel()
    
    logger.info("🛑 DEINIT: Done")


# ===========================================================================
# REST API Endpoints
# ===========================================================================

@plugin_router.get("/status")
async def get_status():
    buffer_count = len(get_buffered_messages())
    if _irc_server and _irc_server._running:
        return {
            "running": True,
            "clients": len(_irc_server.clients),
            "channels": len(_irc_server.channels),
            "virtual_nodes": len(_irc_server.virtual_clients),
            "port": IRC_PORT,
            "buffered_messages": buffer_count
        }
    return {
        "running": False,
        "clients": 0,
        "channels": 0,
        "virtual_nodes": 0,
        "port": IRC_PORT,
        "buffered_messages": buffer_count
    }


@plugin_router.get("/clients")
async def get_clients():
    if not _irc_server:
        return {"clients": []}
    clients = []
    for nick, client in _irc_server.clients.items():
        clients.append({
            "nickname": nick,
            "username": client.username,
            "channels": list(client.channels.keys())
        })
    return {"clients": clients}


@plugin_router.get("/channels")
async def get_channels():
    if not _irc_server:
        return {"channels": []}
    channels = []
    for name, channel in _irc_server.channels.items():
        channels.append({
            "name": name,
            "topic": channel.topic,
            "members": len(channel.clients),
            "mesh_channel_index": channel.mesh_channel_index
        })
    return {"channels": channels}


@plugin_router.get("/nodes")
async def get_virtual_nodes():
    if not _irc_server:
        return {"nodes": []}
    nodes = []
    for node_id, virtual in _irc_server.virtual_clients.items():
        nodes.append({
            "node_id": node_id,
            "irc_nickname": virtual.irc_nickname,
            "long_name": virtual.long_name,
            "short_name": virtual.short_name
        })
    return {"nodes": nodes}


@plugin_router.get("/buffer")
async def get_buffer():
    """Get buffered messages."""
    messages = get_buffered_messages()
    return {"messages": messages, "count": len(messages)}


@plugin_router.post("/buffer/clear")
async def clear_buffer():
    """Clear buffered messages."""
    clear_buffered_messages()
    return {"status": "ok"}


@plugin_router.post("/refresh")
async def refresh_mesh():
    await refresh_mesh_state()
    return {"status": "ok"}


@plugin_router.post("/restart")
async def restart_server():
    await stop_irc_server()
    await asyncio.sleep(1)
    await start_irc_server()
    return {"status": "restarting"}


@plugin_router.post("/stop")
async def stop_server_endpoint():
    await stop_irc_server()
    return {"status": "stopped"}


@plugin_router.get("/log_test")
async def log_test():
    log = core_context.get("logger") or logging.getLogger("mesh_irc")
    log.debug("🔵 DEBUG — testing log")
    log.info("🟢 INFO — testing log")
    log.warning("🟡 WARNING — testing log")
    log.error("🔴 ERROR — testing log")
    return {"status": "ok", "message": "Log lines emitted"}


@plugin_router.get("/settings")
async def get_settings():
    """Get plugin settings."""
    global BNC_BUFFER_LIMIT, BNC_ENABLED, IRC_PORT, BNC_KEEP_AFTER_REPLAY, IRC_PASSWORD, IRC_PASSWORD_ENABLED
    return {
        "bnc_enabled": BNC_ENABLED,
        "buffer_size": BNC_BUFFER_LIMIT,
        "keep_after_replay": BNC_KEEP_AFTER_REPLAY,
        "irc_port": IRC_PORT,
        "irc_password_enabled": IRC_PASSWORD_ENABLED,
        "irc_password": IRC_PASSWORD if IRC_PASSWORD else ""
    }


@plugin_router.post("/settings")
async def save_settings_endpoint(settings: Dict[str, Any]):
    """Save plugin settings."""
    global BNC_BUFFER_LIMIT, BNC_ENABLED, IRC_PORT, BNC_KEEP_AFTER_REPLAY, IRC_PASSWORD, IRC_PASSWORD_ENABLED
    
    if "bnc_enabled" in settings:
        BNC_ENABLED = bool(settings["bnc_enabled"])
    if "buffer_size" in settings:
        BNC_BUFFER_LIMIT = max(1, min(500, int(settings["buffer_size"])))
    if "keep_after_replay" in settings:
        BNC_KEEP_AFTER_REPLAY = bool(settings["keep_after_replay"])
    if "irc_port" in settings:
        new_port = max(1024, min(65535, int(settings["irc_port"])))
        if new_port != IRC_PORT:
            IRC_PORT = new_port
            # Restart server on port change
            await stop_irc_server()
            await asyncio.sleep(0.5)
            await start_irc_server()
    if "irc_password_enabled" in settings:
        IRC_PASSWORD_ENABLED = bool(settings["irc_password_enabled"])
    if "irc_password" in settings:
        pwd = settings["irc_password"]
        if pwd and pwd.strip():
            IRC_PASSWORD = pwd.strip()
            logger.info(f"IRC password set to: {IRC_PASSWORD}")
        else:
            IRC_PASSWORD = None  # Reset to auto (short_name)
            logger.info("IRC password reset to auto (short_name)")
    
    # Save to file for persistence
    save_settings_to_file()
    
    logger.info(f"Settings saved: bnc_enabled={BNC_ENABLED}, buffer_size={BNC_BUFFER_LIMIT}, keep_after_replay={BNC_KEEP_AFTER_REPLAY}, irc_port={IRC_PORT}, irc_password_enabled={IRC_PASSWORD_ENABLED}, irc_password={'set' if IRC_PASSWORD else 'auto'}")
    return {"status": "ok"}

# Alias for backward compatibility
save_settings = save_settings_endpoint
