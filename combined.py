#!/usr/bin/env python3
from __future__ import annotations

"""
combined_forwarder.py — Rebuilt Telegram Forwarder (Sections 1-3 merged)

Features:
 - Account/session manager with 2FA support
 - DB persistence for accounts, handlers, history, deletions, last_sent
 - ForwardEngine with slowmode detection, permission checks, default topic attempts,
   delete-before-forward, batching and cooldowns
 - Tkinter GUI supporting one source selection and multiple targets
 - Proper sign-in (code + 2FA) flow and simple deletion/restore flows

Note: This script will automatically install Telethon if not present.
"""
# Check and install Telethon if not present
try:
    import telethon
except ImportError:
    import subprocess
    import sys
    print("Telethon not found. Installing...")
    try:
        subprocess.check_call([sys.executable, "-m", "pip", "install", "telethon"])
        print("Telethon installed successfully.")
    except subprocess.CalledProcessError as e:
        print(f"Failed to install Telethon: {e}")
        sys.exit(1)

try:
    import requests
except ImportError:
    import subprocess
    import sys
    print("requests not found. Installing...")
    subprocess.check_call([sys.executable, "-m", "pip", "install", "requests"])
    import requests

import asyncio
import json
import logging
import sqlite3
import threading
import time
from collections import defaultdict, deque
from datetime import datetime
from pathlib import Path
from queue import Queue
from typing import Dict, Optional, Tuple, Any, List

import tkinter as tk
from tkinter import ttk, messagebox, simpledialog, filedialog

VERSION = "1.0.0"
UPDATE_URL = "https://raw.githubusercontent.com/jayenzofresh/TGBot/main/combined.py"

def check_for_updates():
    try:
        response = requests.get(UPDATE_URL, timeout=10)
        if response.status_code == 200:
            content = response.text
            latest_version_line = None
            for line in content.split('\n'):
                if line.startswith('VERSION = '):
                    latest_version_line = line.strip()
                    break
            current_version_line = f'VERSION = "{VERSION}"'
            if latest_version_line and latest_version_line != current_version_line:
                if messagebox.askyesno('Update Available', 'A new version is available. Update now?'):
                    with open(__file__, 'w', encoding='utf-8') as f:
                        f.write(content)
                    messagebox.showinfo('Updated', 'Script updated. Please restart the application.')
                    sys.exit(0)
    except Exception as e:
        print(f"Update check failed: {e}")

from telethon import TelegramClient, errors, events
from telethon.errors import FloodWaitError, SessionPasswordNeededError
from telethon.sessions import StringSession
from telethon.tl.types import Channel, Chat

# -------------------------
# Basic configuration
# -------------------------
API_ID = 20037186
API_HASH = "f8970da5123d2c2e827b03e96b351f36"

BASE_DIR = Path(".")
DATA_DIR = BASE_DIR / "data"
SESSIONS_DIR = BASE_DIR / "sessions"
DB_PATH = DATA_DIR / "forwarder.db"
LOG_MAX_LINES = 500
MSG_QUEUE_MAXSIZE = 6

DATA_DIR.mkdir(parents=True, exist_ok=True)
SESSIONS_DIR.mkdir(parents=True, exist_ok=True)

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

# -------------------------
# Simple DB manager wrapper
# -------------------------
class DBManager:
    def __init__(self, path: Path):
        self.path = path
        self._init_db()

    def _get_conn(self):
        return sqlite3.connect(str(self.path))

    def _init_db(self):
        conn = self._get_conn()
        with conn:
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS accounts (
                    phone TEXT PRIMARY KEY,
                    session TEXT,
                    created_at TEXT
                )"""
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS handlers (
                    handler_key TEXT PRIMARY KEY,
                    source_title TEXT,
                    target_titles TEXT,
                    options TEXT,
                    last_seen_id INTEGER,
                    active INTEGER,
                    created_at TEXT
                )"""
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS history (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    ts TEXT,
                    source_title TEXT,
                    target_title TEXT,
                    message_id INTEGER
                )"""
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS deletions (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    ts TEXT,
                    group_title TEXT,
                    deleted_message_id INTEGER,
                    original_message_id INTEGER
                )"""
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS last_sent (
                    group_title TEXT PRIMARY KEY,
                    last_message_id INTEGER,
                    ts TEXT
                )"""
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS queued_messages (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    source_id INTEGER,
                    message_id INTEGER,
                    target_ids TEXT,
                    options TEXT,
                    enqueued_at TEXT
                )"""
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS settings (
                    key TEXT PRIMARY KEY,
                    value TEXT
                )"""
            )
        conn.close()

    def execute(self, sql: str, params: Tuple = ()):
        conn = self._get_conn()
        cur = conn.cursor()
        cur.execute(sql, params)
        conn.commit()
        cur.close()
        conn.close()

    def query(self, sql: str, params: Tuple = ()):
        conn = self._get_conn()
        cur = conn.cursor()
        cur.execute(sql, params)
        rows = cur.fetchall()
        cur.close()
        conn.close()
        return rows

    def get_setting(self, key: str, default: str = "") -> str:
        rows = self.query("SELECT value FROM settings WHERE key=? LIMIT 1", (key,))
        return rows[0][0] if rows else default

    def set_setting(self, key: str, value: str):
        self.execute("INSERT OR REPLACE INTO settings (key, value) VALUES (?, ?)", (key, value))

# create DB manager instance
db = DBManager(DB_PATH)

# -------------------------
# Forwarding history/statistics helpers
# -------------------------
class ForwardingHistory:
    def add_forward(self, src, tgt, msg_id):
        db.execute("INSERT INTO history (ts, source_title, target_title, message_id) VALUES (?, ?, ?, ?)",
                   (datetime.now().isoformat(), src, tgt, msg_id))

    def add_deleted(self, group, deleted_msg_id, orig_msg_id=None):
        db.execute("INSERT INTO deletions (ts, group_title, deleted_message_id, original_message_id) VALUES (?, ?, ?, ?)",
                   (datetime.now().isoformat(), group, deleted_msg_id, orig_msg_id))

    def get_forward_history(self):
        rows = db.query("SELECT ts, source_title, target_title, message_id FROM history ORDER BY id DESC LIMIT 1000")
        return [{"timestamp": r[0], "source": r[1], "target": r[2], "message_id": r[3]} for r in rows]

    def get_deleted_history(self):
        rows = db.query("SELECT ts, group_title, deleted_message_id, original_message_id FROM deletions ORDER BY id DESC LIMIT 1000")
        return [{"timestamp": r[0], "group": r[1], "deleted_message_id": r[2], "original_message_id": r[3]} for r in rows]

class ForwardingStatistics:
    def __init__(self):
        self._forwards = defaultdict(int)
        self._deletes = defaultdict(int)

    def incr_forward(self, group_id):
        self._forwards[group_id] += 1

    def incr_delete(self, group_id):
        self._deletes[group_id] += 1

    def snapshot(self):
        return {"forwarded": dict(self._forwards), "deleted": dict(self._deletes)}

# -------------------------
# Account / Session manager
# -------------------------
class AccountManager:
    def __init__(self, api_id: int, api_hash: str, dbm: DBManager, sessions_dir: Path):
        self.api_id = api_id
        self.api_hash = api_hash
        self.db = dbm
        self.sessions_dir = sessions_dir
        self._accounts: Dict[str, Dict[str, Any]] = {}
        self._load_sessions()

    def _load_sessions(self):
        rows = self.db.query("SELECT phone, session FROM accounts")
        for phone, sess in rows:
            try:
                client = TelegramClient(StringSession(sess), self.api_id, self.api_hash)
                self._accounts[phone] = {"client": client, "session": sess}
            except Exception:
                logger.exception("Failed to restore session for %s", phone)
        # fallback: load any .session files that aren't in DB
        for f in self.sessions_dir.glob("*.session"):
            phone = f.stem
            if phone in self._accounts:
                continue
            try:
                sess = f.read_text(encoding="utf-8")
                client = TelegramClient(StringSession(sess), self.api_id, self.api_hash)
                self._accounts[phone] = {"client": client, "session": sess}
                self.db.execute("INSERT OR REPLACE INTO accounts (phone, session, created_at) VALUES (?, ?, ?)", (phone, sess, datetime.now().isoformat()))
            except Exception:
                logger.exception("Failed to load session file %s", f)

    def add(self, phone: str, client: TelegramClient, session_str: Optional[str] = None) -> None:
        self._accounts[phone] = {"client": client, "session": session_str}
        if session_str:
            try:
                self.db.execute("INSERT OR REPLACE INTO accounts (phone, session, created_at) VALUES (?, ?, ?)", (phone, session_str, datetime.now().isoformat()))
                (self.sessions_dir / f"{phone}.session").write_text(session_str, encoding="utf-8")
            except Exception:
                logger.exception("Failed to persist session for %s", phone)

    def get_client(self, phone: str) -> Optional[TelegramClient]:
        rec = self._accounts.get(phone)
        return rec.get("client") if rec else None

    def get_session(self, phone: str) -> Optional[str]:
        rec = self._accounts.get(phone)
        return rec.get("session") if rec else None

    def all_phones(self) -> List[str]:
        return list(self._accounts.keys())

    def remove(self, phone: str) -> None:
        if phone in self._accounts:
            self._accounts.pop(phone, None)
        try:
            self.db.execute("DELETE FROM accounts WHERE phone=?", (phone,))
        except Exception:
            pass
        try:
            f = self.sessions_dir / f"{phone}.session"
            if f.exists():
                f.unlink()
        except Exception:
            pass

    # -------------------------
    # Async helpers for sign-in
    # -------------------------
    async def create_client_and_send_code(self, phone: str) -> Dict[str, Any]:
        client = TelegramClient(StringSession(), self.api_id, self.api_hash)
        await client.connect()

        if await client.is_user_authorized():
            sess = client.session.save()
            self.add(phone, client, sess)
            return {"status": "already", "client": client, "session": sess}

        try:
            await client.send_code_request(phone)
        except errors.PhoneCodeFloodError:
            raise RuntimeError("Telegram is temporarily blocking code sends. Try again in a few minutes.")
        except errors.PhoneNumberBannedError:
            raise RuntimeError("This phone number is banned by Telegram.")
        except Exception as e:
            raise RuntimeError(f"Failed to send login code: {e}")

        return {"status": "sent", "client": client, "session": client.session.save()}

    async def sign_in_with_code(self, phone: str, code: str, client: Optional[TelegramClient] = None) -> Dict[str, Any]:
        client = client or self.get_client(phone)
        if client is None:
            raise RuntimeError("Client not found; call create_client_and_send_code first")
        if await client.is_user_authorized():
            sess = client.session.save()
            self.add(phone, client, sess)
            return {"status": "already", "session": sess}
        try:
            await client.sign_in(phone, code)
            sess = client.session.save()
            self.add(phone, client, sess)
            return {"status": "ok", "session": sess}
        except SessionPasswordNeededError:
            raise

    async def sign_in_with_password(self, phone: str, password: str) -> Dict[str, Any]:
        client = self.get_client(phone)
        if client is None:
            raise RuntimeError("Client missing; cannot complete password flow")
        await client.sign_in(password=password)
        sess = client.session.save()
        self.add(phone, client, sess)
        return {"status": "ok", "session": sess}

# create account manager
account_mgr = AccountManager(API_ID, API_HASH, db, SESSIONS_DIR)

# -------------------------
# Module-level deletion and restore helpers (re-usable)
# -------------------------
async def _async_delete_forwarded_in_groups(client: TelegramClient, group_list: List[Tuple[Any, str]], groups_titles: List[str], current_user_id: Optional[int]):
    total_deleted = 0
    for title in groups_titles:
        ent = next((e for e, t in group_list if t == title), None)
        if not ent:
            logger.warning("Delete: group '%s' not found; skipping.", title)
            continue
        async for msg in client.iter_messages(ent):
            if getattr(msg, "forward", None) is None:
                continue
            if current_user_id is None or getattr(msg, "sender_id", None) != current_user_id:
                continue
            orig_id = None
            try:
                fwd = getattr(msg, "forward", None)
                orig_id = getattr(fwd, "channel_post", None) or getattr(fwd, "msg_id", None)
            except Exception:
                orig_id = None
            try:
                allow_delete = False
                if orig_id is not None:
                    rows = db.query("SELECT 1 FROM history WHERE message_id=? AND target_title=? LIMIT 1", (orig_id, title))
                    if rows:
                        allow_delete = True
                if not allow_delete:
                    rows2 = db.query("SELECT 1 FROM deletions WHERE group_title=? AND deleted_message_id=? LIMIT 1", (title, msg.id))
                    if rows2:
                        allow_delete = True
                if not allow_delete:
                    continue
                await client.delete_messages(ent, msg.id)
                db.execute("INSERT INTO deletions (ts, group_title, deleted_message_id, original_message_id) VALUES (?, ?, ?, ?)", (datetime.now().isoformat(), title, msg.id, orig_id))
                total_deleted += 1
            except Exception as e:
                logger.exception("Error deleting %s in %s: %s", msg.id, title, e)
    return total_deleted

# -------------------------
# Section 2: ForwardEngine
# -------------------------
class ForwardEngine:
    def __init__(self, client: TelegramClient, dbm: DBManager, history_obj=None, defaults=None, source_entity=None, target_entities=None):
        self.client = client
        self.db = dbm
        self.history = history_obj
        self.source_entity = source_entity
        self.target_entities = target_entities or []
        if defaults is None:
            self.defaults = {
                "batch_size": int(self.db.get_setting("batch_size", "10")),
                "per_forward_delay": float(self.db.get_setting("per_forward_delay", "1.0")),
                "cooldown_minutes": int(self.db.get_setting("cooldown_minutes", "1"))
            }
        else:
            self.defaults = defaults

        # in-memory tracking: target_title -> (last_msg_id, last_sent_ts)
        self._last_sent: Dict[str, Tuple[Optional[int], float]] = {}
        self._load_last_sent_from_db()

        # queue for messages: each item is (source_entity, message_obj, target_entities, options)
        self.queue: "Queue[Tuple]" = Queue(maxsize=MSG_QUEUE_MAXSIZE)
        self._worker_task: Optional[asyncio.Future] = None
        self._running = False
        self._handler = None

    def _load_last_sent_from_db(self):
        try:
            rows = self.db.query("SELECT group_title, last_message_id, ts FROM last_sent")
            for title, mid, ts in rows:
                try:
                    ts_f = float(datetime.fromisoformat(ts).timestamp()) if ts else 0.0
                except Exception:
                    ts_f = 0.0
                self._last_sent[title] = (mid, ts_f)
        except Exception:
            logger.exception("Failed to load last_sent from DB")

    def _persist_last_sent(self, group_title: str, message_id: Optional[int]):
        try:
            ts = datetime.now().isoformat()
            self.db.execute("INSERT OR REPLACE INTO last_sent (group_title, last_message_id, ts) VALUES (?, ?, ?)", (group_title, message_id, ts))
            self._last_sent[group_title] = (message_id, datetime.now().timestamp())
        except Exception:
            logger.exception("Failed to persist last_sent for %s", group_title)

    async def _can_post_to(self, entity) -> bool:
        try:
            perms = await self.client.get_permissions(entity, 'me')
            send_msg = getattr(perms, 'send_messages', True)
            send_media = getattr(perms, 'send_media', True)
            return bool(send_msg) and bool(send_media)
        except Exception as e:
            logger.debug("Could not verify permissions for %s: %s", getattr(entity, 'title', str(entity)), e)
            return False

    async def _get_slowmode_seconds(self, entity) -> int:
        try:
            if hasattr(entity, 'slowmode_seconds') and getattr(entity, 'slowmode_seconds') is not None:
                return int(getattr(entity, 'slowmode_seconds') or 0)
            try:
                full = await self.client.get_entity(entity)
                if hasattr(full, 'slowmode_seconds') and full.slowmode_seconds is not None:
                    return int(full.slowmode_seconds or 0)
            except Exception:
                pass
        except Exception:
            logger.exception("Error checking slowmode for %s", getattr(entity, 'title', str(entity)))
        return 0

    async def _get_default_topic_id(self, entity) -> Optional[int]:
        try:
            if hasattr(entity, 'default_forum_topic_id'):
                return getattr(entity, 'default_forum_topic_id')
            try:
                full = await self.client.get_entity(entity)
                if hasattr(full, 'default_forum_topic_id'):
                    return getattr(full, 'default_forum_topic_id')
            except Exception:
                pass
        except Exception:
            logger.exception("Error checking default topic for %s", getattr(entity, 'title', str(entity)))
        return None

    async def _delete_last_forwarded_if_exists(self, target_entity):
        title = getattr(target_entity, 'title', str(getattr(target_entity, 'id', target_entity)))
        rec = self._last_sent.get(title)
        if not rec:
            return
        last_msg_id, ts = rec
        if not last_msg_id:
            return
        try:
            await self.client.delete_messages(target_entity, last_msg_id)
            if self.history:
                self.history.add_deleted(title, last_msg_id, None)
            logger.info("Deleted previous forwarded message %s in %s", last_msg_id, title)
        except Exception as e:
            logger.warning("Could not delete previous msg %s in %s: %s", last_msg_id, title, e)

    async def _forward_to_target(self, source_entity, message_obj, target_entity, options: Dict):
        title = getattr(target_entity, 'title', str(getattr(target_entity, 'id', target_entity)))
        can_post = await self._can_post_to(target_entity)
        if not can_post:
            logger.info("Skipping %s because posting permission not available", title)
            return False, None

        slow_secs = await self._get_slowmode_seconds(target_entity)
        last_rec = self._last_sent.get(title)
        last_ts = last_rec[1] if last_rec else 0.0
        now_ts = time.time()
        since = now_ts - last_ts
        if slow_secs and since < slow_secs:
            wait = slow_secs - since
            logger.info("Respecting slowmode for %s: waiting %.1fs", title, wait)
            await asyncio.sleep(wait)

        if options.get('delete_prev', True):
            await self._delete_last_forwarded_if_exists(target_entity)

        topic_id = None
        if options.get('use_default_topic', True):
            topic_id = await self._get_default_topic_id(target_entity)

        try:
            try:
                res = await self.client.forward_messages(getattr(target_entity, 'id', target_entity), message_obj)
            except Exception as e:
                logger.debug("forward_messages failed for %s: %s; trying copy/send", title, e)
                try:
                    if getattr(message_obj, 'media', None):
                        sent = await self.client.send_file(target_entity, message_obj.media, caption=getattr(message_obj, 'text', None))
                    else:
                        if topic_id is not None:
                            sent = await self.client.send_message(target_entity, getattr(message_obj, 'text', '') or '', topic_id=topic_id)
                        else:
                            sent = await self.client.send_message(target_entity, getattr(message_obj, 'text', '') or '')
                    res = sent
                except Exception as e2:
                    logger.exception("Failed to copy/send message to %s: %s", title, e2)
                    return False, None

            sent_id = None
            if isinstance(res, list) and res:
                sent_id = getattr(res[-1], 'id', None)
            else:
                sent_id = getattr(res, 'id', None)

            try:
                if self.history:
                    self.history.add_forward(getattr(source_entity, 'title', str(getattr(source_entity, 'id', ''))), title, getattr(message_obj, 'id', None))
            except Exception:
                logger.exception("Failed to record forward to history for %s", title)

            if sent_id:
                self._persist_last_sent(title, sent_id)
                db.execute("INSERT INTO history (ts, source_title, target_title, message_id) VALUES (?, ?, ?, ?)", (datetime.now().isoformat(), getattr(source_entity, 'title', ''), title, getattr(message_obj, 'id', None)))

            logger.info("Forwarded message %s -> %s (sent id=%s)", getattr(message_obj, 'id', None), title, sent_id)
            return True, sent_id
        except FloodWaitError as e:
            wait = getattr(e, 'seconds', None) or 30
            logger.warning("Flood wait encountered: sleeping %ss", wait)
            await asyncio.sleep(wait + 1)
            return False, None
        except Exception as e:
            logger.exception("Unexpected error forwarding to %s: %s", title, e)
            return False, None

    def enqueue(self, source_entity, message_obj, target_entities: List, options: Dict = None, persist: bool = True):
        if options is None:
            options = {}
        source_id = getattr(source_entity, 'id', None)
        message_id = getattr(message_obj, 'id', None)
        target_ids = [getattr(t, 'id', None) for t in target_entities]
        if persist:
            # Check if already queued
            rows = self.db.query("SELECT 1 FROM queued_messages WHERE source_id=? AND message_id=? LIMIT 1", (source_id, message_id))
            if rows:
                logger.debug("Message %s already queued", message_id)
                return False
            try:
                self.db.execute("INSERT INTO queued_messages (source_id, message_id, target_ids, options, enqueued_at) VALUES (?, ?, ?, ?, ?)",
                                (source_id, message_id, json.dumps(target_ids), json.dumps(options), datetime.now().isoformat()))
            except Exception:
                logger.exception("Failed to persist queued message")
                return False
        try:
            self.queue.put_nowait((source_entity, message_obj, target_entities, options))
            logger.debug("Enqueued message %s for %d targets", message_id, len(target_entities))
            return True
        except Exception:
            logger.exception("Queue full or unavailable")
            return False

    async def load_queued(self):
        rows = self.db.query("SELECT source_id, message_id, target_ids, options FROM queued_messages")
        for row in rows:
            source_id, message_id, target_ids_str, options_str = row
            try:
                source_ent = await self.client.get_entity(source_id)
                message_obj = await self.client.get_messages(source_ent, ids=message_id)
                if not message_obj:
                    continue
                target_ids = json.loads(target_ids_str)
                target_ents = []
                for tid in target_ids:
                    try:
                        tent = await self.client.get_entity(tid)
                        target_ents.append(tent)
                    except Exception:
                        logger.exception("Failed to get target entity %s", tid)
                options = json.loads(options_str) if options_str else {}
                self.enqueue(source_ent, message_obj, target_ents, options, persist=False)
            except Exception as e:
                logger.exception("Error loading queued message %s", message_id)
        self.db.execute("DELETE FROM queued_messages")

    async def _on_new_message(self, event):
        message = event.message
        # Enqueue the new message for forwarding
        self.enqueue(self.source_entity, message, self.target_entities, {})

    async def _worker(self):
        logger.info("ForwardEngine worker started")
        while self._running:
            try:
                item = await asyncio.get_event_loop().run_in_executor(None, lambda: self.queue.get(True, 0.5))
            except Exception:
                await asyncio.sleep(0.1)
                continue
            if not item:
                continue
            source_entity, message_obj, target_entities, options = item
            batch = options.get('batch_size', self.defaults.get('batch_size', 6))
            cooldown = options.get('cooldown_minutes', self.defaults.get('cooldown_minutes', 1))
            sent_count = 0
            try:
                for tgt in target_entities:
                    if not self._running:
                        break
                    ok, sent_id = await self._forward_to_target(source_entity, message_obj, tgt, options)
                    if ok:
                        sent_count += 1
                    await asyncio.sleep(self.defaults.get('per_forward_delay', 0.8))
                    if sent_count >= batch:
                        if cooldown:
                            logger.info("Batch complete — cooling down for %d minutes", cooldown)
                            await asyncio.sleep(cooldown * 60)
                        sent_count = 0
            except Exception:
                logger.exception("Error in worker batch processing")
            finally:
                try:
                    self.queue.task_done()
                except Exception:
                    pass
        logger.info("ForwardEngine worker stopped")

    def start(self, loop=None):
        if self._running:
            return
        self._running = True
        self._handler = self.client.add_event_handler(events.NewMessage, self._on_new_message)
        loop = loop or asyncio.get_event_loop()
        self._worker_task = loop.create_task(self._worker())

    def stop(self):
        if self._handler:
            self.client.remove_event_handler(self._handler)
            self._handler = None
        self._running = False
        if self._worker_task:
            try:
                self._worker_task.cancel()
            except Exception:
                pass
            self._worker_task = None

# -------------------------
# Section 3: GUI (TelegramForwarderApp)
# -------------------------
class TelegramForwarderApp:
    def __init__(self):
        self.account_mgr = account_mgr
        self.db = db
        self.forward_engines: Dict[str, ForwardEngine] = {}

        self.current_phone: Optional[str] = None
        self.current_client: Optional[TelegramClient] = None
        self.current_user_id: Optional[int] = None
        self.group_list: List[Tuple[Any, str]] = []
        self.forward_queued_on_start = False

        self.root = tk.Tk()
        self.root.title("Telegram Forwarder — Rebuilt")
        self._build_ui()
        self._tick_animation()

        # Check for queued messages on startup
        rows = self.db.query("SELECT COUNT(*) FROM queued_messages")
        count = rows[0][0] if rows else 0
        if count > 0:
            if messagebox.askyesno('Queued Messages', f'There are {count} queued messages from previous session. Do you want to forward them?'):
                self.forward_queued_on_start = True
            else:
                self.db.execute("DELETE FROM queued_messages")
                self.forward_queued_on_start = False
        else:
            self.forward_queued_on_start = False

        # Check for updates on startup
        check_for_updates()

        # Start asyncio loop in a separate thread
        self.loop = asyncio.new_event_loop()
        self.loop_thread = threading.Thread(target=self._run_loop, daemon=True)
        self.loop_thread.start()

    def _run_loop(self):
        asyncio.set_event_loop(self.loop)
        self.loop.run_forever()

    def schedule_coro(self, coro, ui_callback=None):
        fut = asyncio.run_coroutine_threadsafe(coro, self.loop)
        if ui_callback:
            def cb(f):
                try:
                    r = f.result()
                    self.root.after(0, lambda r=r: ui_callback(r, None))
                except Exception as e:
                    self.root.after(0, lambda e=e: ui_callback(None, e))
            fut.add_done_callback(cb)
        return fut

    def _tick_animation(self):
        self.root.after(200, self._tick_animation)

    def _build_ui(self):
        frm = ttk.Frame(self.root, padding=8)
        frm.grid(row=0, column=0, sticky='nsew')

        ttk.Label(frm, text='Phone (+country):').grid(row=0, column=0, sticky='w')
        self.phone_var = tk.StringVar()
        ttk.Entry(frm, textvariable=self.phone_var, width=28).grid(row=0, column=1, sticky='w')
        ttk.Button(frm, text='Send Code', command=self.on_send_code).grid(row=0, column=2)
        ttk.Label(frm, text='Code:').grid(row=1, column=0, sticky='w')
        self.code_var = tk.StringVar()
        ttk.Entry(frm, textvariable=self.code_var, width=28).grid(row=1, column=1, sticky='w')
        ttk.Button(frm, text='Login', command=self.on_login).grid(row=1, column=2)

        ttk.Label(frm, text='Switch Account:').grid(row=2, column=0, sticky='w')
        self.account_combo = ttk.Combobox(frm, values=self.account_mgr.all_phones(), width=30)
        self.account_combo.grid(row=2, column=1, sticky='w')
        ttk.Button(frm, text='Switch', command=self.on_switch_account).grid(row=2, column=2)
        ttk.Button(frm, text='Remove', command=self.on_remove_account).grid(row=2, column=3)

        ttk.Label(frm, text='Source Group (single):').grid(row=3, column=0, sticky='w')
        self.source_listbox = tk.Listbox(frm, height=8, selectmode=tk.SINGLE, exportselection=False)
        self.source_listbox.grid(row=4, column=0, sticky='nsew')
        ttk.Label(frm, text='Target Groups (multi):').grid(row=3, column=1, sticky='w')
        self.target_listbox = tk.Listbox(frm, height=8, selectmode=tk.MULTIPLE, exportselection=False)
        self.target_listbox.grid(row=4, column=1, sticky='nsew')

        ttk.Button(frm, text='Refresh Groups', command=self.on_refresh_groups).grid(row=5, column=0)
        ttk.Button(frm, text='Start Forwarding', command=self.on_start_forwarding).grid(row=5, column=1)
        ttk.Button(frm, text='Stop Forwarding', command=self.on_stop_forwarding).grid(row=5, column=2)

        ttk.Button(frm, text='Forward Now (one-shot)', command=self.on_forward_once).grid(row=6, column=0)
        ttk.Button(frm, text='Delete Forwarded', command=self.on_delete_forwarded).grid(row=6, column=1)
        ttk.Button(frm, text='Undo Deleted', command=self.on_undo_deleted).grid(row=6, column=2)
        ttk.Button(frm, text='Settings', command=self.on_settings).grid(row=6, column=3)

        ttk.Label(frm, text='Log:').grid(row=7, column=0, sticky='w')
        self.log_text = tk.Text(frm, height=12, width=100, state=tk.DISABLED)
        self.log_text.grid(row=8, column=0, columnspan=4, pady=(4,0))

    def log(self, msg: str, level=logging.INFO):
        ts = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        line = f"{ts}: {msg}"
        try:
            self.log_text.config(state=tk.NORMAL)
            self.log_text.insert(tk.END, line + '\n')
            self.log_text.see(tk.END)
            self.log_text.config(state=tk.DISABLED)
        except Exception:
            pass
        logger.log(level, msg)

    def on_send_code(self):
        phone = self.phone_var.get().strip()
        if not phone or not phone.startswith('+'):
            messagebox.showerror('Invalid phone', 'Enter phone starting with + and country code')
            return
        self.log(f"Sending code to {phone}...")
        def cb(res, exc):
            if exc:
                self.log(f"Send code failed: {exc}")
                messagebox.showerror('Error', str(exc))
                return
            status = res.get('status')
            client = res.get('client')
            session = res.get('session')
            self.account_mgr.add(phone, client, session)
            self.current_phone = phone
            self.current_client = client
            try:
                self.account_combo['values'] = self.account_mgr.all_phones()
            except Exception:
                pass
            self.log(f"Code request {status} for {phone}")
            messagebox.showinfo('Sent', f'Verification code sent to {phone}')
        self.schedule_coro(self.account_mgr.create_client_and_send_code(phone), ui_callback=cb)

    def on_login(self):
        phone = self.phone_var.get().strip()
        code = self.code_var.get().strip()
        if not phone or not code:
            messagebox.showerror('Missing', 'Provide phone and code')
            return
        self.log(f"Logging in {phone}...")
        def cb(res, exc):
            if exc:
                if isinstance(exc, SessionPasswordNeededError) or isinstance(exc, errors.SessionPasswordNeededError):
                    pwd = simpledialog.askstring('2FA Password', 'Enter your 2-step verification password:', show='*', parent=self.root)
                    if not pwd:
                        self.log('2FA cancelled')
                        return
                    def cb2(r2, e2):
                        if e2:
                            self.log(f"Password sign-in failed: {e2}")
                            messagebox.showerror('Error', str(e2))
                            return
                        self._after_successful_login(phone)
                    self.schedule_coro(self.account_mgr.sign_in_with_password(phone, pwd), ui_callback=cb2)
                    return
                self.log(f"Login failed: {exc}")
                messagebox.showerror('Login failed', str(exc))
                return
            status = res.get('status')
            self.log(f"Login result: {status}")
            self._after_successful_login(phone)
        self.schedule_coro(self.account_mgr.sign_in_with_code(phone, code), ui_callback=cb)

    def _after_successful_login(self, phone: str):
        client = self.account_mgr.get_client(phone)
        if not client:
            messagebox.showerror('Error', 'Client not found after login')
            return
        self.current_phone = phone
        self.current_client = client
        async def set_me():
            try:
                if not client.is_connected():
                    await client.connect()
                me = await client.get_me()
                self.current_user_id = getattr(me, 'id', None)
            except Exception:
                self.current_user_id = None
        self.schedule_coro(set_me())
        try:
            self.account_combo['values'] = self.account_mgr.all_phones()
            self.account_combo.set(phone)
        except Exception:
            pass
        self.log(f"Logged in {phone}")

    def on_switch_account(self):
        phone = self.account_combo.get().strip()
        if not phone:
            messagebox.showerror('Error', 'Pick an account')
            return
        client = self.account_mgr.get_client(phone)
        if not client:
            messagebox.showerror('Error', 'Client not present for that phone')
            return
        self.current_phone = phone
        self.current_client = client
        async def set_me():
            try:
                if not client.is_connected():
                    await client.connect()
                me = await client.get_me()
                self.current_user_id = getattr(me, 'id', None)
            except Exception:
                self.current_user_id = None
        self.schedule_coro(set_me())
        self.log(f"Switched to {phone}")
        self.on_refresh_groups()

    def on_remove_account(self):
        phone = self.account_combo.get().strip()
        if not phone:
            messagebox.showerror('Error', 'Pick an account to remove')
            return
        if messagebox.askyesno('Confirm', f'Remove account {phone}?'):
            self.account_mgr.remove(phone)
            try:
                vals = self.account_mgr.all_phones()
                self.account_combo['values'] = vals
            except Exception:
                pass
            self.log(f"Removed account {phone}")

    def on_refresh_groups(self):
        if not self.current_client:
            messagebox.showerror('Not connected', 'Switch to an account first')
            return
        self.log('Refreshing groups...')
        async def do_refresh():
            groups = []
            try:
                async for dialog in self.current_client.iter_dialogs():
                    entity = getattr(dialog, 'entity', None)
                    if isinstance(entity, (Channel, Chat)):
                        title = getattr(dialog, 'title', None) or getattr(entity, 'title', None) or str(getattr(entity, 'id', ''))
                        groups.append((entity, title))
            except Exception as e:
                logger.exception('Error iterating dialogs: %s', e)
            return groups
        def cb(res, exc):
            if exc:
                self.log(f"Refresh failed: {exc}")
                messagebox.showerror('Error', str(exc))
                return
            self.group_list = res
            self.source_listbox.delete(0, tk.END)
            self.target_listbox.delete(0, tk.END)
            for e, t in self.group_list:
                self.source_listbox.insert(tk.END, t)
                self.target_listbox.insert(tk.END, t)
            # Load and select saved source and targets
            saved_source = self.db.get_setting("last_source_title", "")
            saved_targets = self.db.get_setting("last_target_titles", "")
            if saved_targets:
                try:
                    target_titles = json.loads(saved_targets)
                except:
                    target_titles = []
            else:
                target_titles = []
            if saved_source:
                for i, (e, t) in enumerate(self.group_list):
                    if t == saved_source:
                        self.source_listbox.selection_set(i)
                        break
            for title in target_titles:
                for i, (e, t) in enumerate(self.group_list):
                    if t == title:
                        self.target_listbox.selection_set(i)
                        break
            self.log(f"Found {len(self.group_list)} groups")
            messagebox.showinfo('Refreshed', f'Found {len(self.group_list)} groups')
        self.schedule_coro(do_refresh(), ui_callback=cb)

    def on_start_forwarding(self):
        if not self.current_client:
            messagebox.showerror('Not connected', 'Switch to an account and login first')
            return
        if self.current_phone in self.forward_engines and self.forward_engines[self.current_phone]._running:
            messagebox.showinfo('Already running', 'Forwarding already running')
            return
        sel = self.source_listbox.curselection()
        tsel = self.target_listbox.curselection()
        if not sel or not tsel:
            messagebox.showerror('Select', 'Select a source and at least one target')
            return
        # Save selections to DB
        source_title = self.group_list[sel[0]][1]
        target_titles = [self.group_list[i][1] for i in tsel]
        self.db.set_setting("last_source_title", source_title)
        self.db.set_setting("last_target_titles", json.dumps(target_titles))
        source_ent = self.group_list[sel[0]][0]
        targets = [self.group_list[i][0] for i in tsel]
        async def start_eng():
            if not self.current_client.is_connected():
                await self.current_client.connect()
            eng = ForwardEngine(self.current_client, self.db, history_obj=ForwardingHistory(), defaults=None, source_entity=source_ent, target_entities=targets)
            eng.start(self.loop)
            if self.forward_queued_on_start:
                await eng.load_queued()
                self.forward_queued_on_start = False
            return eng
        def cb(res, exc):
            if exc:
                self.log(f"Failed to start engine: {exc}")
                messagebox.showerror('Error', str(exc))
                return
            self.forward_engines[self.current_phone] = res
            self.log('Forwarding started')
            messagebox.showinfo('Started', 'Forwarding engine started')
        self.schedule_coro(start_eng(), ui_callback=cb)

    def on_stop_forwarding(self):
        if self.current_phone not in self.forward_engines or not self.forward_engines[self.current_phone]._running:
            messagebox.showinfo('Not running', 'Forwarding engine is not running')
            return
        self.forward_engines[self.current_phone].stop()
        del self.forward_engines[self.current_phone]
        self.log('Forwarding stopped')
        messagebox.showinfo('Stopped', 'Forwarding engine stopped')

    def on_forward_once(self):
        if not self.current_client:
            messagebox.showerror('Not connected', 'Switch to an account first')
            return
        sel = self.source_listbox.curselection()
        tsel = self.target_listbox.curselection()
        if not sel or not tsel:
            messagebox.showerror('Select', 'Select a source and at least one target')
            return
        # Save selections to DB
        source_title = self.group_list[sel[0]][1]
        target_titles = [self.group_list[i][1] for i in tsel]
        self.db.set_setting("last_source_title", source_title)
        self.db.set_setting("last_target_titles", json.dumps(target_titles))
        source_ent = self.group_list[sel[0]][0]
        targets = [self.group_list[i][0] for i in tsel]
        kw = simpledialog.askstring('Keyword', 'Keyword filter (optional):', parent=self.root)
        attach_only = messagebox.askyesno('Attachments only', 'Forward only media/attachments?')

        async def gather_and_enqueue():
            try:
                count = 0
                async for m in self.current_client.iter_messages(source_ent, limit=100):
                    if not isinstance(source_ent, Channel) and self.current_user_id is not None and getattr(m, 'sender_id', None) != self.current_user_id:
                        continue
                    if kw and (not getattr(m, 'text', '') or kw.lower() not in getattr(m, 'text', '').lower()):
                        continue
                    if attach_only and not getattr(m, 'media', None):
                        continue
                    if self.current_phone not in self.forward_engines or not self.forward_engines[self.current_phone]._running:
                        eng = ForwardEngine(self.current_client, self.db, history_obj=ForwardingHistory(), defaults=None)
                        eng.start(self.loop)
                        self.forward_engines[self.current_phone] = eng
                    ok = self.forward_engines[self.current_phone].enqueue(source_ent, m, targets, { 'batch_size': self.forward_engines[self.current_phone].defaults.get('batch_size', 6), 'cooldown_minutes': self.forward_engines[self.current_phone].defaults.get('cooldown_minutes', 1) })
                    if ok:
                        count += 1
                    if count >= MSG_QUEUE_MAXSIZE:
                        break
                self.log(f'Queued {count} messages for forwarding')
                messagebox.showinfo('Queued', f'Queued {count} messages for forwarding')
            except Exception as e:
                logger.exception('Error collecting messages: %s', e)
                messagebox.showerror('Error', str(e))
        self.schedule_coro(gather_and_enqueue())

    def on_delete_forwarded(self):
        if not self.group_list:
            messagebox.showerror('No groups', 'Refresh groups first')
            return
        titles = [t for e, t in self.group_list]
        popup = tk.Toplevel(self.root)
        popup.title('Select groups to delete forwarded messages from')
        lb = tk.Listbox(popup, selectmode=tk.MULTIPLE, height=12, exportselection=False)
        lb.pack(fill=tk.BOTH, expand=True)
        for t in titles:
            lb.insert(tk.END, t)
        def go():
            picks = lb.curselection()
            sel_titles = [titles[i] for i in picks]
            popup.destroy()
            def cb(res, exc):
                if exc:
                    self.log(f'Delete run failed: {exc}')
                    messagebox.showerror('Error', str(exc))
                    return
                self.log(f'Deleted {res} forwarded messages')
                messagebox.showinfo('Deleted', f'Deleted {res} forwarded messages')
            self.schedule_coro(_async_delete_forwarded_in_groups(self.current_client, self.group_list, sel_titles, self.current_user_id), ui_callback=cb)
        tk.Button(popup, text='Delete', command=go).pack(pady=6)

    def on_undo_deleted(self):
        rows = db.query('SELECT DISTINCT group_title FROM deletions ORDER BY group_title')
        if not rows:
            messagebox.showinfo('No Deleted Messages', 'No deleted messages to restore')
            return
        popup = tk.Toplevel(self.root)
        popup.title('Select group to restore deleted messages')
        lb = tk.Listbox(popup, selectmode=tk.SINGLE, height=12, exportselection=False)
        lb.pack(fill=tk.BOTH, expand=True)
        groups = [r[0] for r in rows]
        for g in groups:
            lb.insert(tk.END, g)
        def go():
            idx = lb.curselection()
            if not idx:
                return
            group_title = groups[idx[0]]
            popup.destroy()
            self._restore_deleted_messages_slow(group_title)
        tk.Button(popup, text='Restore', command=go).pack(pady=6)

    def on_settings(self):
        batch_size = simpledialog.askstring('Settings', 'Batch size (messages per batch):', initialvalue=self.db.get_setting("batch_size", "10"), parent=self.root)
        if batch_size is None:
            return
        try:
            batch_size_int = int(batch_size)
            if batch_size_int <= 0:
                raise ValueError
        except ValueError:
            messagebox.showerror('Invalid', 'Batch size must be a positive integer')
            return

        per_forward_delay = simpledialog.askstring('Settings', 'Delay between messages (seconds):', initialvalue=self.db.get_setting("per_forward_delay", "1.0"), parent=self.root)
        if per_forward_delay is None:
            return
        try:
            per_forward_delay_float = float(per_forward_delay)
            if per_forward_delay_float < 0:
                raise ValueError
        except ValueError:
            messagebox.showerror('Invalid', 'Delay must be a non-negative number')
            return

        cooldown_minutes = simpledialog.askstring('Settings', 'Cooldown after batch (minutes):', initialvalue=self.db.get_setting("cooldown_minutes", "1"), parent=self.root)
        if cooldown_minutes is None:
            return
        try:
            cooldown_minutes_int = int(cooldown_minutes)
            if cooldown_minutes_int < 0:
                raise ValueError
        except ValueError:
            messagebox.showerror('Invalid', 'Cooldown must be a non-negative integer')
            return

        self.db.set_setting("batch_size", str(batch_size_int))
        self.db.set_setting("per_forward_delay", str(per_forward_delay_float))
        self.db.set_setting("cooldown_minutes", str(cooldown_minutes_int))
        self.log(f'Settings updated: batch_size={batch_size_int}, per_forward_delay={per_forward_delay_float}, cooldown_minutes={cooldown_minutes_int}')
        messagebox.showinfo('Settings', 'Settings saved successfully')

    def _restore_deleted_messages_slow(self, group_title):
        async def do_restore():
            rows = db.query('SELECT id, deleted_message_id, original_message_id FROM deletions WHERE group_title=? ORDER BY id ASC', (group_title,))
            if not rows:
                self.log(f'No deleted messages to restore for {group_title}')
                return
            ent = next((e for e, t in self.group_list if t == group_title), None)
            if not ent:
                self.log(f'Group not present in refreshed groups: {group_title}. Refresh groups and try again.')
                return
            for rec in rows:
                rec_id, deleted_msg_id, orig_msg_id = rec
                if not orig_msg_id:
                    continue
                hist = db.query('SELECT source_title FROM history WHERE message_id=? LIMIT 1', (orig_msg_id,))
                if not hist:
                    continue
                source_title = hist[0][0]
                source_ent = next((e for e, t in self.group_list if t == source_title), None)
                if not source_ent:
                    continue
                try:
                    orig_msg = await self.current_client.get_messages(source_ent, ids=orig_msg_id)
                    if not orig_msg:
                        continue
                    if getattr(orig_msg, 'media', None):
                        await self.current_client.send_file(ent, orig_msg.media, caption=getattr(orig_msg, 'text', '') or None)
                    else:
                        await self.current_client.send_message(ent, orig_msg.text or '')
                    db.execute('DELETE FROM deletions WHERE id=?', (rec_id,))
                except Exception as e:
                    self.log(f'Error restoring {orig_msg_id}: {e}')
                    continue
                await asyncio.sleep(1.0)
            self.log(f'Completed restore job for {group_title}')
        self.schedule_coro(do_restore())

    def run(self):
        try:
            self.root.mainloop()
        finally:
            for eng in self.forward_engines.values():
                eng.stop()
            self.log('Shutdown complete')

# -------------------------
# Entrypoint when executed as a script
# -------------------------
if __name__ == '__main__':
    app = TelegramForwarderApp()
    app.run()
