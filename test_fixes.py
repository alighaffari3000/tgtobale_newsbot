"""Offline checks for the bug fixes. Run: python test_fixes.py"""

import asyncio
import json
import logging
import os
import sys
import tempfile
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
os.chdir(tempfile.mkdtemp())  # main.py creates tg2bale.log / pending_media in cwd

import httpx
from telethon.tl.types import MessageMediaPhoto

import bale as bale_mod
import forwarder as fw
from bale import BaleClient
from db import Database
from main import RedactFilter


def test_redact():
    rec = logging.LogRecord("httpx", logging.INFO, "", 0,
                            "POST %s", ("https://tapi.bale.ai/botSECRET123/sendMessage",), None)
    RedactFilter("SECRET123").filter(rec)
    assert "SECRET123" not in rec.getMessage() and "bot***" in rec.getMessage()


def test_rate_limit_retry():
    bodies = []

    def handler(req):
        bodies.append(len(req.read()))
        if len(bodies) == 1:
            return httpx.Response(429, json={"ok": False, "parameters": {"retry_after": 2}})
        return httpx.Response(200, json={"ok": True, "result": {"message_id": 7}})

    bale_mod.time = SimpleNamespace(sleep=lambda s: None)  # don't patch the global time module
    c = BaleClient("TOKEN", "123")
    c._client = httpx.Client(transport=httpx.MockTransport(handler))
    Path("p.jpg").write_bytes(b"x" * 1000)
    assert c.send_photo("p.jpg", "cap") == 7
    assert len(bodies) == 2 and bodies[0] == bodies[1] > 1000  # file re-sent in full
    assert bale_mod._retry_after(httpx.Response(429, json={})) == 5


class FakeBale:
    def __init__(self, ok):
        self.ok, self.deleted, self.edits = ok, [], []

    def send_media_group(self, items, caption, reply_to=None):
        return [100 + i * 2 for i in range(len(items))] if self.ok else []

    def edit_message_caption(self, mid, caption):
        self.edits.append((mid, caption))
        return True

    def send_text(self, text, reply_to=None):
        return 101 if self.ok else None

    def delete_message(self, mid):
        self.deleted.append(mid)
        return True


class FakeTg:
    def __init__(self, msgs=()):
        self.msgs = {m.id: m for m in msgs}

    async def download_media(self, msg, file):
        Path(file).write_bytes(b"img")
        return file

    async def get_messages(self, chat_id, ids=None, **kw):
        return [self.msgs.get(i) for i in ids] if ids else list(self.msgs.values())

    async def get_entity(self, _):
        return SimpleNamespace(username="src")

    def on(self, event):  # capture handlers so tests can call them directly
        def deco(fn):
            self.handlers = getattr(self, "handlers", {})
            self.handlers[fn.__name__] = fn
            return fn
        return deco


def msg(mid, text="", gid=None):
    return SimpleNamespace(id=mid, grouped_id=gid, message=text, entities=None, reply_to=None,
                           date=datetime.now(timezone.utc), media=MessageMediaPhoto())


def make(bale, tg=None, blocked=()):
    db = Database("t.db")
    return fw.Forwarder(tg or FakeTg(), bale, db, ["-1"], list(blocked)), db


async def test_album_survives_retry():
    f, db = make(FakeBale(ok=False))
    row_id = db.insert_message(1, "-1", "cap", None, "album")
    await f._send_media_group([msg(1), msg(2)], "cap", row_id)
    row = db.get_row_by_telegram_id(1, "-1")
    items = json.loads(row["media_paths"])
    assert row["status"] == "pending" and len(items) == 2
    assert all(Path(i["path"]).exists() for i in items)

    f.bale = FakeBale(ok=True)
    assert await f._send_pending_row(row) == 100
    assert not any(Path(i["path"]).exists() for i in items)
    assert json.loads(db.get_row_by_telegram_id(1, "-1")["bale_message_ids"]) == [100, 102]


async def test_give_up():
    f, db = make(FakeBale(ok=False))
    row_id = db.insert_message(2, "-1", "cap", None, "photo")
    Path("pending_media/g.jpg").write_bytes(b"x")
    db.update_media(row_id, "pending_media/g.jpg", "photo")
    row = db.get_row_by_telegram_id(2, "-1")
    assert not f._give_up_if_expired(row)  # fresh post is kept
    row = dict(row, created_at=(datetime.now(timezone.utc) - fw.MAX_PENDING_AGE).isoformat())
    assert f._give_up_if_expired(row)
    assert db.get_row_by_telegram_id(2, "-1")["status"] == "failed"
    assert not Path("pending_media/g.jpg").exists()


async def test_expire_during_outage():
    bale = FakeBale(ok=True)
    bale.check_connectivity = lambda: False
    f, db = make(bale)
    old = db.insert_message(3, "-1", "cap", None, None)
    db.mark_failed(old, 1)  # queued (retry_count=2)
    with db._connect() as c:
        c.execute("UPDATE messages SET created_at=? WHERE id=?",
                  ((datetime.now(timezone.utc) - fw.MAX_PENDING_AGE).isoformat(), old))
    fresh = db.insert_message(4, "-1", "cap", None, None)
    db.mark_failed(fresh, 1)
    fw.CONNECTIVITY_POLL = 0
    task = asyncio.create_task(f.connectivity_loop())
    await asyncio.sleep(0.1)
    task.cancel()
    assert db.get_row_by_telegram_id(3, "-1")["status"] == "failed"  # expired though Bale is down
    assert db.get_row_by_telegram_id(4, "-1")["status"] == "pending"


async def test_interrupted_send_requeued():
    f, db = make(FakeBale(ok=True))
    crashed = db.insert_message(5, "-1", "cap", None, None)  # left at retry_count=0 by a crash
    sent = db.insert_message(6, "-1", "cap", None, None)
    db.mark_sent(sent, 1)
    assert db.requeue_new() == 1
    assert [r["id"] for r in db.get_immediate_retries()] == [crashed]

    async def boom():
        raise ValueError("unexpected")
    row_id = db.insert_message(7, "-1", "cap", None, None)
    await f._first_send(row_id, boom())
    assert db.get_row_by_telegram_id(7, "-1")["retry_count"] == 1


def test_bold_link_markdown():
    from telethon.tl.types import MessageEntityBold, MessageEntityTextUrl
    ents = [MessageEntityBold(0, 5), MessageEntityTextUrl(0, 5, "http://x")]
    assert fw._entities_to_markdown("hello world", ents) == "*[hello](http://x)* world"


def test_400_drops_markdown_then_reply():
    sent = []

    def handler(req):
        body = json.loads(req.read())
        sent.append(body)
        if "parse_mode" in body or "reply_to_message_id" in body:
            return httpx.Response(400, json={"ok": False, "description": "bad request"})
        return httpx.Response(200, json={"ok": True, "result": {"message_id": 8}})

    c = BaleClient("TOKEN", "123")
    c._client = httpx.Client(transport=httpx.MockTransport(handler))
    assert c.send_text("*broken", reply_to_message_id=5) == 8
    assert len(sent) == 3 and sent[-1]["text"] == "*broken"


async def test_blocked_album():
    fw.MEDIA_GROUP_WAIT = 0
    m1, m2 = msg(10, gid=5), msg(11, "BAD news", gid=5)
    f, db = make(FakeBale(ok=True), FakeTg([m1, m2]), blocked=["bad"])
    await f._buffer_media_group(m1, "-1")
    await f._buffer_media_group(m2, "-1")
    await asyncio.sleep(0.05)
    assert not db.is_known(10, "-1")


async def test_blocked_edit():
    bale = FakeBale(ok=True)
    f, db = make(bale, blocked=["bad"])
    row_id = db.insert_message(20, "-1", "cap", None, None)
    db.mark_sent(row_id, 555)
    await f._withdraw_post(20, "-1", "blocked")
    assert bale.deleted == [555]
    assert db.get_row_by_telegram_id(20, "-1")["status"] == "blocked"


async def test_source_delete():
    bale = FakeBale(ok=True)
    tg = FakeTg()
    f, db = make(bale, tg)
    f.register_handlers()
    on_delete = tg.handlers["on_message_deleted"]
    album = db.insert_message(40, "-1", "cap", None, "album", None, 88)
    db.mark_sent(album, 800, [800, 801])
    db.insert_message(42, "-1", "cap", None, None)  # still queued, never reached Bale

    # whole album removed (ids 40, 41), a queued post (42) and an unknown id (99)
    await on_delete(SimpleNamespace(deleted_ids=[40, 41, 42, 99], chat_id=-1))
    assert bale.deleted == [800, 801]
    assert db.get_row_by_telegram_id(40, "-1")["status"] == "deleted"
    assert db.get_row_by_telegram_id(42, "-1")["status"] == "deleted"  # never sent anymore
    assert db.is_known(40, "-1")  # a replayed update must not re-forward it
    assert db.get_row_by_telegram_id(99, "-1") is None

    # a second delete event for the same post does nothing
    await on_delete(SimpleNamespace(deleted_ids=[40], chat_id=-1))
    assert bale.deleted == [800, 801]


def test_media_group_returns_all_ids():
    calls = []

    def handler(req):
        calls.append(req.url.path.rsplit("/", 1)[-1])
        if calls[-1] == "sendMediaGroup":
            return httpx.Response(200, json={"ok": True, "result": [{"message_id": 1}, {"message_id": 2}]})
        return httpx.Response(200, json={"ok": True, "result": {"message_id": 9}})

    c = BaleClient("TOKEN", "123")
    c._client = httpx.Client(transport=httpx.MockTransport(handler))
    for n in ("a.jpg", "b.jpg", "c.mp4"):
        Path(n).write_bytes(b"x")
    photos = [{"type": "photo", "path": "a.jpg"}, {"type": "photo", "path": "b.jpg"}]
    assert c.send_media_group(photos, "cap") == [1, 2]
    # mixed: photo sub-group (1, 2) + single video (9)
    assert c.send_media_group(photos + [{"type": "video", "path": "c.mp4"}], "cap") == [1, 2, 9]


def test_migration_adds_columns():
    import sqlite3
    conn = sqlite3.connect("old.db")
    conn.execute("""CREATE TABLE messages (id INTEGER PRIMARY KEY AUTOINCREMENT,
        telegram_message_id INTEGER NOT NULL, channel_id TEXT NOT NULL, caption TEXT,
        media_paths TEXT, status TEXT NOT NULL DEFAULT 'pending',
        retry_count INTEGER NOT NULL DEFAULT 0, created_at TEXT NOT NULL,
        UNIQUE(telegram_message_id, channel_id))""")
    conn.commit()
    conn.close()
    db = Database("old.db")
    with db._connect() as c:
        cols = {r[1] for r in c.execute("PRAGMA table_info(messages)")}
    assert {"grouped_id", "bale_message_ids"} <= cols


async def test_album_edit():
    fw._format_date = lambda dt: "date"  # real one uses %-d, which only works on Linux
    bale = FakeBale(ok=True)
    tg = FakeTg()
    f, db = make(bale, tg, blocked=["bad"])
    f.register_handlers()
    on_edit = tg.handlers["on_message_edited"]
    row_id = db.insert_message(30, "-1", "old", None, "album", None, 77)
    db.mark_sent(row_id, 700, [700, 701, 702])

    # caption edit arrives on a later album item → edits the caption-carrying Bale message
    await on_edit(SimpleNamespace(message=msg(32, "new text", gid=77), chat_id=-1))
    assert bale.edits and bale.edits[0][0] == 700 and "new text" in bale.edits[0][1]
    assert "new text" in db.get_row_by_telegram_id(30, "-1")["caption"]

    # edit on an item without text is ignored (would otherwise wipe the caption)
    await on_edit(SimpleNamespace(message=msg(31, "", gid=77), chat_id=-1))
    assert len(bale.edits) == 1

    # blocked word → every message of the album is deleted
    await on_edit(SimpleNamespace(message=msg(32, "BAD text", gid=77), chat_id=-1))
    assert bale.deleted == [700, 701, 702]
    assert db.get_row_by_telegram_id(30, "-1")["status"] == "blocked"


if __name__ == "__main__":
    test_redact()
    test_rate_limit_retry()
    test_media_group_returns_all_ids()
    test_migration_adds_columns()
    test_bold_link_markdown()
    test_400_drops_markdown_then_reply()
    for t in (test_album_survives_retry, test_give_up, test_blocked_album, test_blocked_edit,
              test_source_delete, test_album_edit, test_expire_during_outage,
              test_interrupted_send_requeued):
        asyncio.run(t())
    print("all checks passed")
