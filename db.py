"""
db.py — SQLite state management for message forwarding and retry scheduling.

Retry logic:
- First failure  → retry_count=1, stays pending (retried once more immediately next cycle)
- Second failure → retry_count=2, stays pending in queue — no timer
  Connectivity loop flushes all queued messages in order once Bale is reachable again.
"""

import sqlite3
import logging
from datetime import datetime, timezone
from typing import Optional

logger = logging.getLogger(__name__)


def _utcnow() -> str:
    return datetime.now(timezone.utc).isoformat()


class Database:
    def __init__(self, db_path: str = "tg2bale.db"):
        self.db_path = db_path
        self._init_db()

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA journal_mode=WAL")
        return conn

    def _init_db(self):
        with self._connect() as conn:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS messages (
                    id                      INTEGER PRIMARY KEY AUTOINCREMENT,
                    telegram_message_id     INTEGER NOT NULL,
                    channel_id              TEXT NOT NULL,
                    caption                 TEXT,
                    media_paths             TEXT,
                    media_type              TEXT,
                    status                  TEXT NOT NULL DEFAULT 'pending',
                    retry_count             INTEGER NOT NULL DEFAULT 0,
                    bale_message_id         INTEGER,
                    reply_to_tg_message_id  INTEGER,
                    created_at              TEXT NOT NULL,
                    UNIQUE(telegram_message_id, channel_id)
                )
            """)
            # Migrations for existing databases
            existing = {row[1] for row in conn.execute("PRAGMA table_info(messages)")}
            if "bale_message_id" not in existing:
                conn.execute("ALTER TABLE messages ADD COLUMN bale_message_id INTEGER")
                logger.info("DB migration: added bale_message_id column")
            if "media_type" not in existing:
                conn.execute("ALTER TABLE messages ADD COLUMN media_type TEXT")
                logger.info("DB migration: added media_type column")
            if "reply_to_tg_message_id" not in existing:
                conn.execute("ALTER TABLE messages ADD COLUMN reply_to_tg_message_id INTEGER")
                logger.info("DB migration: added reply_to_tg_message_id column")
            conn.commit()
        logger.debug("Database initialized: %s", self.db_path)

    def insert_message(
        self,
        telegram_message_id: int,
        channel_id: str,
        caption: Optional[str],
        media_paths: Optional[str],
        media_type: Optional[str] = None,
        reply_to_tg_message_id: Optional[int] = None,
    ) -> Optional[int]:
        """Insert a new pending message. Returns row id or None if duplicate."""
        try:
            with self._connect() as conn:
                cur = conn.execute(
                    """
                    INSERT OR IGNORE INTO messages
                        (telegram_message_id, channel_id, caption, media_paths, media_type,
                         reply_to_tg_message_id, status, created_at)
                    VALUES (?, ?, ?, ?, ?, ?, 'pending', ?)
                    """,
                    (telegram_message_id, channel_id, caption, media_paths, media_type,
                     reply_to_tg_message_id, _utcnow()),
                )
                conn.commit()
                return cur.lastrowid if cur.rowcount else None
        except sqlite3.Error as e:
            logger.error("DB insert error: %s", e)
            return None

    def mark_sent(self, row_id: int, bale_message_id: Optional[int] = None):
        with self._connect() as conn:
            conn.execute(
                "UPDATE messages SET status='sent', bale_message_id=? WHERE id=?",
                (bale_message_id, row_id),
            )
            conn.commit()

    def mark_failed(self, row_id: int, retry_count: int):
        """
        Increment retry_count and keep status='pending'.
        No timer is set — connectivity loop handles flushing when Bale is reachable.
        """
        with self._connect() as conn:
            conn.execute(
                "UPDATE messages SET status='pending', retry_count=? WHERE id=?",
                (retry_count + 1, row_id),
            )
            conn.commit()

    def get_immediate_retries(self) -> list[sqlite3.Row]:
        """Pending messages tried exactly once — eligible for one immediate retry."""
        with self._connect() as conn:
            return conn.execute(
                """
                SELECT * FROM messages
                WHERE status = 'pending' AND retry_count = 1
                ORDER BY created_at
                """,
            ).fetchall()

    def get_queued_pending(self) -> list[sqlite3.Row]:
        """All pending messages with retry_count >= 2 (waiting for connectivity restore)."""
        with self._connect() as conn:
            return conn.execute(
                """
                SELECT * FROM messages
                WHERE status = 'pending' AND retry_count >= 2
                ORDER BY created_at
                """,
            ).fetchall()

    def get_bale_message_id(self, telegram_message_id: int, channel_id: str) -> Optional[int]:
        with self._connect() as conn:
            row = conn.execute(
                """SELECT bale_message_id FROM messages
                   WHERE telegram_message_id=? AND channel_id=? AND status='sent'""",
                (telegram_message_id, channel_id),
            ).fetchone()
        return row["bale_message_id"] if row else None

    def get_row_by_telegram_id(self, telegram_message_id: int, channel_id: str) -> Optional[sqlite3.Row]:
        """Return the full DB row for a message (any status), or None if not found."""
        with self._connect() as conn:
            return conn.execute(
                "SELECT * FROM messages WHERE telegram_message_id=? AND channel_id=?",
                (telegram_message_id, channel_id),
            ).fetchone()

    def reset_for_resend(self, row_id: int):
        """
        Reset a sent row back to pending so it can be re-sent with new media.
        Used when a Telegram post is edited to add media after being sent as text.
        """
        with self._connect() as conn:
            conn.execute(
                """UPDATE messages
                   SET status='pending', bale_message_id=NULL,
                       media_paths=NULL, media_type=NULL, retry_count=0
                   WHERE id=?""",
                (row_id,),
            )
            conn.commit()
        logger.debug("DB row %s reset for re-send", row_id)

    def get_row_by_telegram_id(self, telegram_message_id: int, channel_id: str) -> Optional[sqlite3.Row]:
        """Return the full DB row for a message (any status), or None if not found."""
        with self._connect() as conn:
            return conn.execute(
                "SELECT * FROM messages WHERE telegram_message_id=? AND channel_id=?",
                (telegram_message_id, channel_id),
            ).fetchone()

    def reset_for_resend(self, row_id: int):
        """
        Reset a sent row back to pending so it can be re-sent with new media.
        Used when a Telegram post is edited to add media after being sent as text.
        """
        with self._connect() as conn:
            conn.execute(
                """UPDATE messages
                   SET status='pending', bale_message_id=NULL,
                       media_paths=NULL, media_type=NULL, retry_count=0
                   WHERE id=?""",
                (row_id,),
            )
            conn.commit()
        logger.debug("DB row %s reset for re-send", row_id)

    def update_caption(self, telegram_message_id: int, channel_id: str, caption: str):
        """Update stored caption after a Telegram edit."""
        with self._connect() as conn:
            conn.execute(
                "UPDATE messages SET caption=? WHERE telegram_message_id=? AND channel_id=?",
                (caption, telegram_message_id, channel_id),
            )
            conn.commit()

    def update_media(self, row_id: int, media_path: str, media_type: str):
        """Store downloaded media path and type for a pending message."""
        with self._connect() as conn:
            conn.execute(
                "UPDATE messages SET media_paths=?, media_type=? WHERE id=?",
                (media_path, media_type, row_id),
            )
            conn.commit()

    def is_already_processed(self, telegram_message_id: int, channel_id: str) -> bool:
        with self._connect() as conn:
            row = conn.execute(
                "SELECT status FROM messages WHERE telegram_message_id=? AND channel_id=?",
                (telegram_message_id, channel_id),
            ).fetchone()
        return row is not None and row["status"] == "sent"

    def is_known(self, telegram_message_id: int, channel_id: str) -> bool:
        """Return True if this message exists in DB with ANY status (sent or pending).
        Used to prevent duplicate processing after Telethon replays missed updates."""
        with self._connect() as conn:
            row = conn.execute(
                "SELECT id FROM messages WHERE telegram_message_id=? AND channel_id=?",
                (telegram_message_id, channel_id),
            ).fetchone()
        return row is not None

    def is_pending(self, telegram_message_id: int, channel_id: str) -> bool:
        """Return True if this message is still pending (not yet sent to Bale)."""
        with self._connect() as conn:
            row = conn.execute(
                "SELECT status FROM messages WHERE telegram_message_id=? AND channel_id=?",
                (telegram_message_id, channel_id),
            ).fetchone()
        return row is not None and row["status"] == "pending"
