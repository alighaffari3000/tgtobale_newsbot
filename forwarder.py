"""
forwarder.py — Core forwarding logic.
Listens to Telegram events, downloads media, sends to Bale, manages retries.
"""

import asyncio
import json
import logging
import tempfile
from collections import defaultdict
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Optional
from zoneinfo import ZoneInfo

from telethon import TelegramClient, events
from telethon.tl.types import (
    MessageEntityBold,
    MessageEntityCode,
    MessageEntityItalic,
    MessageEntityPre,
    MessageEntityStrike,
    MessageEntityTextUrl,
    MessageEntityUnderline,
    MessageEntityUrl,
    MessageMediaDocument,
    MessageMediaPhoto,
    MessageMediaWebPage,
)

from bale import BaleClient
from db import Database

logger = logging.getLogger(__name__)

MEDIA_GROUP_WAIT = 3.5        # seconds to collect media group items
EDIT_WATCH_MINUTES = 10       # how long to watch for edits after a post
CONNECTIVITY_POLL = 900       # 15 minutes between connectivity checks
IMMEDIATE_RETRY_DELAY = 60    # 1 minute before the one immediate retry
MEDIA_STORE_DIR = "pending_media"  # persistent storage for pending message media
BALE_CAPTION_LIMIT = 4096     # Bale max caption length
BALE_MAX_MEDIA_BYTES = 20 * 1024 * 1024  # Bale 20 MB upload limit
MAX_SEND_ATTEMPTS = 10       # give up (status='failed') after this many failed sends

TEHRAN_TZ = ZoneInfo("Asia/Tehran")


class Forwarder:
    def __init__(self, tg_client: TelegramClient, bale: BaleClient, db: Database,
                 source_channels: list[str], blocked_words: list[str] | None = None):
        self.tg = tg_client
        self.bale = bale
        self.db = db
        self.source_channels = source_channels
        self.blocked_words = [w.lower() for w in (blocked_words or [])]

        # media group buffering: group_key → list of messages
        self._media_groups: dict[str, list] = defaultdict(list)
        self._media_group_tasks: dict[str, asyncio.Task] = {}

        # cache channel usernames: channel_id → username string
        self._username_cache: dict[str, Optional[str]] = {}

        # ensure persistent media storage directory exists
        Path(MEDIA_STORE_DIR).mkdir(exist_ok=True)

    # ── Channel username ──────────────────────────────────────────────────────

    async def _get_channel_username(self, channel_id: str) -> Optional[str]:
        if channel_id not in self._username_cache:
            try:
                entity = await self.tg.get_entity(int(channel_id))
                username = getattr(entity, "username", None)
                self._username_cache[channel_id] = f"@{username}" if username else None
            except Exception:
                self._username_cache[channel_id] = None
        return self._username_cache[channel_id]

    def _is_blocked(self, text: Optional[str]) -> bool:
        text = (text or "").lower()
        return any(w in text for w in self.blocked_words)

    # ── Event handlers ────────────────────────────────────────────────────────

    def register_handlers(self):
        @self.tg.on(events.NewMessage(chats=self.source_channels))
        async def on_new_message(event):
            msg = event.message
            channel_id = str(event.chat_id)

            # Skip if already in DB (sent or pending) — prevents duplicate sends
            # after Telethon reconnects and replays missed updates
            if self.db.is_known(msg.id, channel_id):
                return

            # فیلتر کلمات مسدود — آلبوم‌ها بعد از جمع شدن کامل در _flush_media_group چک می‌شوند
            if not msg.grouped_id and self._is_blocked(msg.message):
                logger.info("Post blocked (contains blocked word) from channel: %s (msg_id=%s)",
                            channel_id, msg.id)
                return

            logger.info("New post detected from channel: %s (msg_id=%s)", channel_id, msg.id)

            if msg.grouped_id:
                await self._buffer_media_group(msg, channel_id)
            else:
                await self._process_single(msg, channel_id)

        @self.tg.on(events.MessageEdited(chats=self.source_channels))
        async def on_message_edited(event):
            msg = event.message
            channel_id = str(event.chat_id)

            # Only handle edits within the watch window
            if not msg.date:
                return
            msg_date = msg.date if msg.date.tzinfo else msg.date.replace(tzinfo=timezone.utc)
            age = datetime.now(timezone.utc) - msg_date
            if age > timedelta(minutes=EDIT_WATCH_MINUTES):
                return

            # آلبوم با شناسه اولین آیتمش ذخیره شده؛ ویرایش ممکن است روی هر آیتمی برسد
            tg_id = msg.id
            if msg.grouped_id:
                # ponytail: ویرایش آیتم بدون متن نادیده گرفته می‌شود، پس حذف کامل کپشن آلبوم به بله نمی‌رسد
                if not msg.message:
                    return
                album_row = self.db.get_row_by_grouped_id(msg.grouped_id, channel_id)
                if album_row:  # rows from before the grouped_id migration fall back to msg.id
                    tg_id = album_row["telegram_message_id"]

            # ویرایشی که کلمه مسدود اضافه کرده: از بله حذف کن / از صف خارج کن
            if self._is_blocked(msg.message):
                await self._withdraw_post(tg_id, channel_id, "blocked")
                return

            new_text = _entities_to_markdown(msg.message, msg.entities) if msg.message is not None else None
            source = await self._get_channel_username(channel_id)
            date_str = _format_date(msg_date)
            new_caption = _build_footer(new_text, source, date_str)

            # Case 1: already sent to Bale → edit the Bale message
            bale_msg_id = self.db.get_bale_message_id(tg_id, channel_id)
            if bale_msg_id:
                if not new_caption:
                    return

                tg_has_media = bool(msg.media and not isinstance(msg.media, MessageMediaWebPage))
                db_row = self.db.get_row_by_telegram_id(tg_id, channel_id)
                db_media_type = db_row["media_type"] if db_row else None

                # ── FIX: پست اول بدون عکس ارسال شده، بعد در تلگرام عکس اضافه شده ──
                # در این حالت پیام بله یه text message هست ولی تلگرام الان media داره.
                # editMessageCaption روی text message کار نمیکنه (404 میده).
                # راه‌حل: پیام text قدیمی رو حذف کن، پیام جدید با media بفرست.
                if tg_has_media and not db_media_type:
                    logger.info(
                        "Post was sent as text but now has media in Telegram (msg_id=%s, channel=%s). "
                        "Deleting old Bale message and re-sending with media.",
                        msg.id, channel_id,
                    )
                    # تشخیص نوع media جدید
                    new_media_type: Optional[str] = None
                    if isinstance(msg.media, MessageMediaPhoto):
                        new_media_type = "photo"
                    elif isinstance(msg.media, MessageMediaDocument):
                        mime = msg.media.document.mime_type or ""
                        new_media_type = "video" if mime.startswith("video/") else "document"

                    # حذف پیام قدیمی از بله
                    deleted = await asyncio.to_thread(self.bale.delete_message, bale_msg_id)
                    if not deleted:
                        logger.warning(
                            "Could not delete old Bale message %s, will try re-send anyway", bale_msg_id
                        )

                    # ری‌ست DB تا بتوان دوباره insert کرد
                    if db_row:
                        self.db.reset_for_resend(db_row["id"])

                    # ارسال مجدد با media
                    row_id = db_row["id"] if db_row else self.db.insert_message(
                        msg.id, channel_id, new_caption, None, new_media_type
                    )
                    if row_id:
                        await self._send_single(msg, new_caption, row_id, new_media_type)
                    return

                logger.info("Post edited in channel %s (msg_id=%s), updating Bale msg %s",
                            channel_id, msg.id, bale_msg_id)

                # حالت عادی: پیام بله media داشته یا هنوز هم text هست
                if tg_has_media:
                    success = await asyncio.to_thread(self.bale.edit_message_caption, bale_msg_id, new_caption)
                else:
                    success = await asyncio.to_thread(self.bale.edit_message_text, bale_msg_id, new_caption)

                if success:
                    self.db.update_caption(tg_id, channel_id, new_caption)
                    logger.info("Bale message %s updated successfully", bale_msg_id)
                else:
                    logger.warning("Failed to update Bale message %s", bale_msg_id)
                return

            # Case 2: still pending (not yet sent) → update the stored caption so
            # the correct text is used when it eventually gets sent
            if self.db.is_pending(tg_id, channel_id):
                self.db.update_caption(tg_id, channel_id, new_caption or "")
                logger.info("Pending post caption updated before send (msg_id=%s, channel=%s)",
                            tg_id, channel_id)

        @self.tg.on(events.MessageDeleted(chats=self.source_channels))
        async def on_message_deleted(event):
            # ponytail: آلبوم با شناسه‌ی اولین آیتمش ذخیره شده؛ حذف فقط یک آیتم دیگرِ آلبوم نادیده گرفته می‌شود
            channel_id = str(event.chat_id)
            for tg_id in event.deleted_ids:
                await self._withdraw_post(tg_id, channel_id, "deleted")

    # ── Media group buffering ─────────────────────────────────────────────────

    async def _buffer_media_group(self, msg, channel_id: str):
        group_key = f"{channel_id}:{msg.grouped_id}"
        self._media_groups[group_key].append(msg)

        if group_key in self._media_group_tasks:
            self._media_group_tasks[group_key].cancel()

        task = asyncio.create_task(self._flush_media_group(group_key, channel_id))
        self._media_group_tasks[group_key] = task

    async def _flush_media_group(self, group_key: str, channel_id: str):
        await asyncio.sleep(MEDIA_GROUP_WAIT)
        buffered = self._media_groups.pop(group_key, [])
        self._media_group_tasks.pop(group_key, None)

        if not buffered:
            return

        buffered.sort(key=lambda m: m.id)

        grouped_id = buffered[0].grouped_id
        chat_id = int(channel_id)

        # ── Step 1: re-fetch buffered IDs directly to get their latest state ──
        # This ensures we have the freshest version of each message (caption may
        # have been set on the last item after it was first received).
        try:
            buffered_ids = [m.id for m in buffered]
            fetched_by_ids = await self.tg.get_messages(chat_id, ids=buffered_ids)
            # get_messages(ids=...) may return None entries for missing messages
            refreshed = [m for m in fetched_by_ids if m is not None
                         and getattr(m, "grouped_id", None) == grouped_id]
            if len(refreshed) >= len(buffered):
                messages = sorted(refreshed, key=lambda m: m.id)
                logger.debug("Re-fetched %d buffered group messages by ID", len(messages))
            else:
                messages = buffered
        except Exception as e:
            logger.warning("Could not re-fetch buffered group messages: %s — using buffer", e)
            messages = buffered

        # ── Step 2: if caption still missing, scan a wider ID window ──
        # Telegram places the caption on the *last* item of the group, which may
        # have an ID higher than anything we buffered (arrived after buffer closed).
        if not _extract_caption(messages):
            logger.info(
                "Caption not found after ID-fetch for group %s (channel %s), "
                "scanning wider ID window…",
                grouped_id, channel_id,
            )
            try:
                min_id = messages[0].id
                max_id = messages[-1].id
                fetched_range = await self.tg.get_messages(
                    chat_id,
                    min_id=min_id - 1,
                    max_id=max_id + 15,
                    limit=30,
                )
                group_msgs = [m for m in fetched_range
                              if getattr(m, "grouped_id", None) == grouped_id]
                if group_msgs:
                    messages = sorted(group_msgs, key=lambda m: m.id)
                    logger.debug("Wide-window fetch got %d group messages", len(messages))
            except Exception as e:
                logger.warning("Wide-window fetch failed: %s", e)

        # ── Step 3: last resort — wait a moment and re-fetch known IDs once more ──
        # Covers the rare case where Telegram delivers caption with a short delay.
        if not _extract_caption(messages):
            logger.info(
                "Caption still missing for group %s, waiting 2 s and re-fetching…",
                grouped_id,
            )
            await asyncio.sleep(2.0)
            try:
                all_ids = [m.id for m in messages]
                refetched = await self.tg.get_messages(chat_id, ids=all_ids)
                refreshed2 = [m for m in refetched if m is not None
                              and getattr(m, "grouped_id", None) == grouped_id]
                if refreshed2:
                    messages = sorted(refreshed2, key=lambda m: m.id)
                    logger.debug("Final re-fetch got %d group messages", len(messages))
                    if _extract_caption(messages):
                        logger.info("Caption found after final re-fetch for group %s", grouped_id)
                    else:
                        logger.warning(
                            "Caption still not found after all retries for group %s "
                            "(channel %s) — proceeding without caption",
                            grouped_id, channel_id,
                        )
            except Exception as e:
                logger.warning("Final re-fetch failed: %s", e)

        if any(self._is_blocked(m.message) for m in messages):
            logger.info("Media group blocked (contains blocked word) from channel: %s (group=%s)",
                        channel_id, grouped_id)
            return

        first_msg = messages[0]
        raw_caption = _extract_caption(messages)
        source = await self._get_channel_username(channel_id)
        msg_date = first_msg.date if first_msg.date.tzinfo else first_msg.date.replace(tzinfo=timezone.utc)
        date_str = _format_date(msg_date)
        caption = _build_footer(raw_caption, source, date_str)

        # Detect if this media group is a reply to another post in the same channel
        reply_to_tg_id: Optional[int] = None
        reply_to_bale_id: Optional[int] = None
        if first_msg.reply_to and getattr(first_msg.reply_to, "reply_to_msg_id", None):
            reply_to_tg_id = first_msg.reply_to.reply_to_msg_id
            reply_to_bale_id = self.db.get_bale_message_id(reply_to_tg_id, channel_id)
            if reply_to_bale_id:
                logger.info(
                    "Media group (first_msg=%s) is a reply to tg_msg_id=%s → bale_msg_id=%s",
                    first_msg.id, reply_to_tg_id, reply_to_bale_id,
                )
            else:
                logger.info(
                    "Media group (first_msg=%s) is a reply to tg_msg_id=%s but no Bale mapping found",
                    first_msg.id, reply_to_tg_id,
                )

        row_id = self.db.insert_message(first_msg.id, channel_id, caption, None, "album",
                                        reply_to_tg_id, grouped_id)
        if row_id is None:
            return

        logger.info("Processing media group (%d items) from channel: %s", len(messages), channel_id)
        await self._send_media_group(messages, caption, row_id, reply_to_bale_id)

    # ── Single message ────────────────────────────────────────────────────────

    async def _process_single(self, msg, channel_id: str):
        raw_caption = _entities_to_markdown(msg.message, msg.entities) if msg.message is not None else None
        source = await self._get_channel_username(channel_id)
        msg_date = msg.date if msg.date.tzinfo else msg.date.replace(tzinfo=timezone.utc)
        date_str = _format_date(msg_date)
        caption = _build_footer(raw_caption, source, date_str)

        # Determine media type upfront so it's stored in DB for retry
        media_type: Optional[str] = None
        if msg.media and not isinstance(msg.media, MessageMediaWebPage):
            if isinstance(msg.media, MessageMediaPhoto):
                media_type = "photo"
            elif isinstance(msg.media, MessageMediaDocument):
                mime = msg.media.document.mime_type or ""
                media_type = "video" if mime.startswith("video/") else "document"

        # Detect if this is a reply to another post in the same channel
        reply_to_tg_id: Optional[int] = None
        reply_to_bale_id: Optional[int] = None
        if msg.reply_to and getattr(msg.reply_to, "reply_to_msg_id", None):
            reply_to_tg_id = msg.reply_to.reply_to_msg_id
            reply_to_bale_id = self.db.get_bale_message_id(reply_to_tg_id, channel_id)
            if reply_to_bale_id:
                logger.info(
                    "Post msg_id=%s is a reply to tg_msg_id=%s → bale_msg_id=%s",
                    msg.id, reply_to_tg_id, reply_to_bale_id,
                )
            else:
                logger.info(
                    "Post msg_id=%s is a reply to tg_msg_id=%s but no Bale mapping found yet",
                    msg.id, reply_to_tg_id,
                )

        row_id = self.db.insert_message(msg.id, channel_id, caption, None, media_type,
                                        reply_to_tg_id)
        if row_id is None:
            return

        await self._send_single(msg, caption, row_id, media_type, reply_to_bale_id)

    async def _send_single(self, msg, caption: Optional[str], row_id: int,
                           media_type: Optional[str] = None,
                           reply_to_bale_id: Optional[int] = None):
        bale_msg_id: Optional[int] = None
        downloaded_path: Optional[str] = None

        if not media_type:
            # Text-only message
            bale_msg_id = await asyncio.to_thread(self.bale.send_text, caption or "",
                                                  reply_to_bale_id)
        else:
            # Download to persistent dir so it survives retries
            downloaded_path = await self._download_to_store(msg, row_id)
            if downloaded_path:
                file_size = Path(downloaded_path).stat().st_size
                if file_size > BALE_MAX_MEDIA_BYTES:
                    logger.warning(
                        "Media file too large for Bale (%.1f MB > 20 MB), row_id=%s"
                        " — sending caption only",
                        file_size / 1024 / 1024,
                        row_id,
                    )
                    _safe_remove(downloaded_path)
                    downloaded_path = None
                    caption = _caption_with_oversize_note(caption, None)
                    bale_msg_id = await asyncio.to_thread(self.bale.send_text, caption or "",
                                                          reply_to_bale_id)
                else:
                    bale_msg_id = await self._send_media_file(downloaded_path, media_type, caption,
                                                              reply_to_bale_id)
            else:
                # Download failed; fall back to text
                logger.warning("Media download failed for row_id=%s, falling back to text", row_id)
                bale_msg_id = await asyncio.to_thread(self.bale.send_text, caption or "",
                                                      reply_to_bale_id)

        if bale_msg_id:
            self.db.mark_sent(row_id, bale_msg_id)
            _safe_remove(downloaded_path or "")
            logger.info("Post sent to Bale successfully (row_id=%s, bale_msg=%s)", row_id, bale_msg_id)
            # Send overflow caption as a reply if needed
            await self._send_caption_overflow(caption, bale_msg_id)
        else:
            # Persist media path so retry loops can use it
            if downloaded_path:
                self.db.update_media(row_id, downloaded_path, media_type or "")
            self.db.mark_failed(row_id, 0)
            logger.warning("Failed to send post to Bale (row_id=%s), will retry", row_id)

    # ── Media group send ──────────────────────────────────────────────────────

    async def _send_media_group(self, messages: list, caption: Optional[str], row_id: int,
                                reply_to_bale_id: Optional[int] = None):
        media_items = []
        has_oversized = False
        for i, msg in enumerate(messages):
            if not msg.media:
                continue
            item = await self._build_media_item(msg, row_id, i)
            if not item:
                continue
            file_size = Path(item["path"]).stat().st_size if item.get("path") else 0
            if file_size > BALE_MAX_MEDIA_BYTES:
                logger.warning(
                    "Media file too large for Bale (%.1f MB > 20 MB) in media group,"
                    " row_id=%s — skipping this file",
                    file_size / 1024 / 1024,
                    row_id,
                )
                _safe_remove(item.get("path", ""))
                has_oversized = True
                continue
            media_items.append(item)

        if has_oversized:
            caption = _caption_with_oversize_note(caption, None)

        bale_ids: list[int] = []
        if media_items:
            # Pass caption separately; send_media_group places it on first item
            bale_ids = await asyncio.to_thread(
                self.bale.send_media_group, media_items, caption, reply_to_bale_id
            )

        if not bale_ids and caption:
            text_id = await asyncio.to_thread(self.bale.send_text, caption, reply_to_bale_id)
            bale_ids = [text_id] if text_id else []

        bale_msg_id = bale_ids[0] if bale_ids else None
        if bale_msg_id:
            self.db.mark_sent(row_id, bale_msg_id, bale_ids)
            for item in media_items:
                _safe_remove(item["path"])
            logger.info("Post sent to Bale successfully (row_id=%s, media_group)", row_id)
            # Send overflow caption as a reply if needed
            await self._send_caption_overflow(caption, bale_msg_id)
        else:
            # Keep downloaded files so retry loops can re-send the whole album
            if media_items:
                self.db.update_media(row_id, json.dumps(media_items), "album")
            self.db.mark_failed(row_id, 0)
            logger.warning("Failed to send post to Bale (row_id=%s), will retry", row_id)

    # ── Media helpers ─────────────────────────────────────────────────────────

    async def _build_media_item(self, msg, row_id: int, index: int) -> Optional[dict]:
        try:
            # Download to persistent dir so it survives retries
            dest = Path(MEDIA_STORE_DIR) / f"row{row_id}_{index}{_guess_suffix(msg)}"
            path = await self.tg.download_media(msg, file=str(dest))
            if not path:
                return None

            media_type = "document"
            if isinstance(msg.media, MessageMediaPhoto):
                media_type = "photo"
            elif isinstance(msg.media, MessageMediaDocument):
                mime = msg.media.document.mime_type or ""
                if mime.startswith("video/"):
                    media_type = "video"
                elif mime.startswith("image/"):
                    media_type = "photo"

            # Bale's sendMediaGroup only supports photo and video
            if media_type == "document":
                logger.warning("Skipping unsupported media type 'document' (msg %s) in media group", msg.id)
                _safe_remove(path)
                return None

            return {"type": media_type, "path": path}
        except Exception as e:
            logger.error("Failed to download media for msg %s: %s", msg.id, e)
            return None

    async def _download_to_store(self, msg, row_id: int) -> Optional[str]:
        """Download media to persistent MEDIA_STORE_DIR. Returns file path or None."""
        try:
            suffix = _guess_suffix(msg)
            dest = Path(MEDIA_STORE_DIR) / f"row{row_id}{suffix}"
            path = await self.tg.download_media(msg, file=str(dest))
            return path if path else None
        except Exception as e:
            logger.error("Failed to download media for row_id=%s: %s", row_id, e)
            return None

    async def _send_media_file(self, file_path: str, media_type: str,
                               caption: Optional[str],
                               reply_to_bale_id: Optional[int] = None) -> Optional[int]:
        """Send an already-downloaded media file to Bale.
        Caption is automatically truncated to BALE_CAPTION_LIMIT; overflow is
        handled separately via _send_caption_overflow."""
        short_caption = caption[:BALE_CAPTION_LIMIT] if caption else None
        if media_type == "photo":
            return await asyncio.to_thread(self.bale.send_photo, file_path, short_caption,
                                           reply_to_bale_id)
        elif media_type == "video":
            return await asyncio.to_thread(self.bale.send_video, file_path, short_caption,
                                           reply_to_bale_id)
        else:
            return await asyncio.to_thread(self.bale.send_document, file_path, short_caption,
                                           reply_to_bale_id)

    async def _send_caption_overflow(self, caption: Optional[str], reply_to_msg_id: int):
        """If caption exceeds BALE_CAPTION_LIMIT, send the remainder as a reply."""
        if not caption or len(caption) <= BALE_CAPTION_LIMIT:
            return
        overflow = caption[BALE_CAPTION_LIMIT:]
        logger.info("Caption overflow (%d chars), sending as reply to msg %s",
                    len(overflow), reply_to_msg_id)
        await asyncio.to_thread(self.bale.send_text, overflow, reply_to_msg_id)

    async def _download_and_send(self, msg, media_type: str, caption: Optional[str]) -> Optional[int]:
        """Download media to a temp file and send immediately (used by media groups)."""
        try:
            suffix = _guess_suffix(msg)
            tmp = tempfile.NamedTemporaryFile(delete=False, suffix=suffix)
            tmp.close()
            path = await self.tg.download_media(msg, file=tmp.name)
            if not path:
                logger.warning("Download returned None for msg %s, sending caption only", msg.id)
                return await asyncio.to_thread(self.bale.send_text, caption or "")

            msg_id = await self._send_media_file(path, media_type, caption)
            _safe_remove(path)

            if not msg_id and caption:
                logger.warning("Media send failed, falling back to text")
                return await asyncio.to_thread(self.bale.send_text, caption)

            return msg_id
        except Exception as e:
            logger.error("Error during download/send: %s", e)
            if caption:
                return await asyncio.to_thread(self.bale.send_text, caption)
            return None

    # ── Pending row send helper ───────────────────────────────────────────────

    async def _send_pending_row(self, row) -> Optional[int]:
        """
        Send a pending DB row to Bale.
        Uses stored media file if available, otherwise falls back to text.
        Cleans up media file on success.
        """
        media_path: Optional[str] = row["media_paths"]
        media_type: Optional[str] = row["media_type"]
        caption: Optional[str] = row["caption"]

        # Resolve reply_to_bale_id from stored reply_to_tg_message_id
        reply_to_bale_id: Optional[int] = None
        reply_to_tg_id = row["reply_to_tg_message_id"] if "reply_to_tg_message_id" in row.keys() else None
        if reply_to_tg_id:
            reply_to_bale_id = self.db.get_bale_message_id(reply_to_tg_id, row["channel_id"])

        if media_type == "album" and media_path:
            items = [i for i in json.loads(media_path) if Path(i["path"]).exists()]
            if items:
                sent_ids = await asyncio.to_thread(self.bale.send_media_group, items, caption,
                                                   reply_to_bale_id)
                if not sent_ids:
                    return None
                # store all album ids now; the caller's mark_sent(id) keeps them
                self.db.mark_sent(row["id"], sent_ids[0], sent_ids)
                for i in items:
                    _safe_remove(i["path"])
                return sent_ids[0]
            # files gone — fall through to text

        elif media_path and media_type and Path(media_path).exists():
            file_size = Path(media_path).stat().st_size
            if file_size > BALE_MAX_MEDIA_BYTES:
                logger.warning(
                    "Pending media too large for Bale (%.1f MB > 20 MB), row_id=%s"
                    " — discarding file and sending caption only",
                    file_size / 1024 / 1024,
                    row["id"],
                )
                _safe_remove(media_path)
                updated_caption = _caption_with_oversize_note(caption, None)
                if updated_caption:
                    return await asyncio.to_thread(self.bale.send_text, updated_caption,
                                                   reply_to_bale_id)
                return None
            sent_id = await self._send_media_file(media_path, media_type, caption, reply_to_bale_id)
            if sent_id:
                _safe_remove(media_path)
            return sent_id

        # No media (or file missing) — send as text
        if caption:
            return await asyncio.to_thread(self.bale.send_text, caption, reply_to_bale_id)
        return None

    def _give_up_if_exhausted(self, row) -> bool:
        """After MAX_SEND_ATTEMPTS failures, mark the row 'failed' and drop its media."""
        if row["retry_count"] + 1 < MAX_SEND_ATTEMPTS:
            return False
        self.db.set_status(row["id"], "failed")
        _remove_row_media(row)
        logger.error("Giving up on row_id=%s after %d attempts", row["id"], MAX_SEND_ATTEMPTS)
        return True

    async def _withdraw_post(self, telegram_message_id: int, channel_id: str, status: str):
        """Take a post off Bale — delete the Bale copy, or drop it from the queue — and record
        why in the row: 'blocked' (edit added a blocked word) or 'deleted' (removed in Telegram)."""
        row = self.db.get_row_by_telegram_id(telegram_message_id, channel_id)
        if not row or row["status"] not in ("sent", "pending"):
            return
        if row["status"] == "sent" and row["bale_message_id"]:
            ids = json.loads(row["bale_message_ids"] or "null") or [row["bale_message_id"]]
            failed = [mid for mid in ids
                      if not await asyncio.to_thread(self.bale.delete_message, mid)]
            if failed:
                # stays 'sent', so the leftover Bale copies remain visible in the DB
                logger.warning("Could not delete Bale messages %s (%s post)", failed, status)
                return
        _remove_row_media(row)
        self.db.set_status(row["id"], status)
        logger.info("Post %s (msg_id=%s, channel=%s, was %s)",
                    status, telegram_message_id, channel_id, row["status"])

    # ── Retry & connectivity loops ────────────────────────────────────────────

    async def immediate_retry_loop(self):
        """
        Every IMMEDIATE_RETRY_DELAY seconds, retry messages that failed once (retry_count=1).
        If they fail again, they move to the pending queue for connectivity_loop.
        """
        while True:
            await asyncio.sleep(IMMEDIATE_RETRY_DELAY)
            try:
                rows = self.db.get_immediate_retries()
                for row in rows:
                    logger.info("Retry attempt #1 for row_id=%s", row["id"])
                    sent_id = await self._send_pending_row(row)
                    if sent_id:
                        self.db.mark_sent(row["id"], sent_id)
                        logger.info("Post sent to Bale successfully on retry (row_id=%s)", row["id"])
                    elif not self._give_up_if_exhausted(row):
                        self.db.mark_failed(row["id"], row["retry_count"])
                        logger.warning("Retry failed for row_id=%s, moved to pending queue", row["id"])
            except Exception as e:
                logger.error("Immediate retry loop error: %s", e)

    async def connectivity_loop(self):
        """
        Every CONNECTIVITY_POLL seconds, test Bale connectivity.
        If reachable, flush all queued pending messages (retry_count >= 2) in order.
        """
        while True:
            await asyncio.sleep(CONNECTIVITY_POLL)
            try:
                reachable = await asyncio.to_thread(self.bale.check_connectivity)
                if not reachable:
                    logger.info("Bale connectivity check: unreachable. Queue preserved.")
                    continue

                logger.info("Bale connectivity check: OK. Flushing pending queue...")
                rows = self.db.get_queued_pending()
                if not rows:
                    logger.info("No queued messages to flush.")
                    continue

                for row in rows:
                    sent_id = await self._send_pending_row(row)
                    if sent_id:
                        self.db.mark_sent(row["id"], sent_id)
                        logger.info("Queued post sent to Bale (row_id=%s)", row["id"])
                    elif not self._give_up_if_exhausted(row):
                        self.db.mark_failed(row["id"], row["retry_count"])
                        logger.warning("Failed to flush queued post (row_id=%s), keeping in queue", row["id"])
            except Exception as e:
                logger.error("Connectivity loop error: %s", e)


# ── Helpers ───────────────────────────────────────────────────────────────────

def _entities_to_markdown(text: str, entities) -> str:
    """
    Convert Telegram message entities to Markdown format supported by Bale.

    Bale uses Markdown (not HTML) for formatting, despite accepting parse_mode=html
    in the API — the HTML tags are not rendered correctly.

    Supported entity types:
        MessageEntityTextUrl  → [text](url)
        MessageEntityUrl      → url  (plain URL, left as-is)
        MessageEntityBold     → *text*
        MessageEntityItalic   → _text_
        MessageEntityCode     → `text`
        MessageEntityPre      → ```text```
        MessageEntityUnderline→ text  (not supported in Bale Markdown, left plain)
        MessageEntityStrike   → text  (not supported in Bale Markdown, left plain)
    """
    if not text:
        return text or ""
    if not entities:
        return _escape_markdown(text)

    # Telethon entity offsets are in UTF-16 code units, not Python str indices.
    utf16 = text.encode("utf-16-le")

    def utf16_offset_to_str_index(offset: int) -> int:
        byte_pos = offset * 2
        chunk = utf16[:byte_pos]
        return len(chunk.decode("utf-16-le"))

    # For Markdown links [text](url), we need to know the URL at open-tag time
    # but the text span comes between open and close. We handle TextUrl specially:
    # store (start, end, url) and build them after collecting all spans.

    # For simple wrap tags: list of (start, end, open_marker, close_marker)
    spans: list[tuple[int, int, str, str]] = []
    # For links: list of (start, end, url)
    link_spans: list[tuple[int, int, str]] = []
    # Ranges to skip escaping (already handled as link text or plain URL)
    skip_escape: list[tuple[int, int]] = []

    for entity in entities:
        start = utf16_offset_to_str_index(entity.offset)
        end = utf16_offset_to_str_index(entity.offset + entity.length)

        if isinstance(entity, MessageEntityTextUrl):
            link_spans.append((start, end, entity.url))
            skip_escape.append((start, end))

        elif isinstance(entity, MessageEntityUrl):
            # Plain URL in text — leave as-is (no wrapping needed)
            skip_escape.append((start, end))

        elif isinstance(entity, MessageEntityBold):
            spans.append((start, end, "*", "*"))
            skip_escape.append((start, end))

        elif isinstance(entity, MessageEntityItalic):
            spans.append((start, end, "_", "_"))
            skip_escape.append((start, end))

        elif isinstance(entity, MessageEntityCode):
            spans.append((start, end, "`", "`"))
            skip_escape.append((start, end))

        elif isinstance(entity, MessageEntityPre):
            spans.append((start, end, "```", "```"))
            skip_escape.append((start, end))

        # Underline and Strike are not supported in Bale Markdown — render as plain text

    # Build a sorted list of insertions: (str_index, priority, text_to_insert)
    # priority 0 = open marker, 1 = close marker
    insertions: list[tuple[int, int, str]] = []

    for start, end, open_m, close_m in spans:
        insertions.append((start, 0, open_m))
        insertions.append((end, 1, close_m))

    # For link spans, we use "replace" entries: a single insertion at `start`
    # that contains the full [text](url) string, and we skip over [start, end)
    # in the build loop so the original text is not appended again.
    # replace_spans: list of (start, end, replacement_string)
    replace_spans: list[tuple[int, int, str]] = []

    for start, end, url in link_spans:
        link_text = text[start:end]
        escaped_text = _escape_markdown_link_text(link_text)
        escaped_url = _escape_url_parens(url)
        full_link = f"[{escaped_text}]({escaped_url})"
        replace_spans.append((start, end, full_link))
        # Add a sentinel insertion at `start` so the build loop processes this position
        insertions.append((start, 0, ""))  # placeholder; handled via replace_spans

    insertions.sort(key=lambda x: (x[0], x[1]))

    # Build result, escaping plain text segments but not entity-covered ranges
    result_parts: list[str] = []
    prev = 0
    _processed_replace_starts: set[int] = set()

    def _get_replace(pos: int) -> Optional[tuple[int, int, str]]:
        """Return the replace_span that starts at pos, if any (and not yet processed)."""
        if pos in _processed_replace_starts:
            return None
        for rs, re, rt in replace_spans:
            if rs == pos:
                return rs, re, rt
        return None

    for str_index, _priority, marker in insertions:
        if str_index > prev:
            segment = text[prev:str_index]
            if _in_skip_range(prev, str_index, skip_escape, link_spans):
                result_parts.append(segment)
            else:
                result_parts.append(_escape_markdown(segment))
        # Check if this position is the start of a link replace span
        rep = _get_replace(str_index)
        if rep is not None:
            rs, re, rt = rep
            result_parts.append(rt)
            _processed_replace_starts.add(rs)
            prev = re  # skip over the original link text in `text`
        else:
            if str_index >= prev:
                if marker:
                    result_parts.append(marker)
                prev = str_index

    if prev < len(text):
        segment = text[prev:]
        if _in_skip_range(prev, len(text), skip_escape, link_spans):
            result_parts.append(segment)
        else:
            result_parts.append(_escape_markdown(segment))

    return "".join(result_parts)


def _in_skip_range(start: int, end: int, skip_escape, link_spans) -> bool:
    """Return True if this segment is inside any skip-escape range."""
    for s, e in skip_escape:
        if start >= s and end <= e:
            return True
    for s, e, _ in link_spans:
        if start >= s and end <= e:
            return True
    return False


def _escape_markdown(text: str) -> str:
    """Escape Markdown special characters in plain text segments."""
    # Only escape chars that Bale Markdown treats as special
    return (
        text
        .replace("\\", "\\\\")
        .replace("*", "\\*")
        .replace("_", "\\_")
        .replace("`", "\\`")
        .replace("[", "\\[")
    )


def _escape_markdown_link_text(text: str) -> str:
    """Escape ] inside link text."""
    return text.replace("]", "\\]")


def _escape_url_parens(url: str) -> str:
    """Escape ) inside URL part of a Markdown link."""
    return url.replace(")", "\\)")


def _extract_caption(messages: list) -> Optional[str]:
    """Return the first non-empty caption found across all messages in the group,
    converted to HTML (preserving entity formatting).
    Telegram typically places the caption on the last item, so we scan all."""
    for msg in messages:
        if msg.message:
            return _entities_to_markdown(msg.message, msg.entities)
    return None


def _format_date(dt: datetime) -> str:
    """Format a datetime as D/M/YYYY - HH:MM in Tehran time."""
    if dt is None:
        return ""
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    dt_tehran = dt.astimezone(TEHRAN_TZ)
    return dt_tehran.strftime("%-d/%-m/%Y - %H:%M")


def _build_footer(caption: Optional[str], source: Optional[str], date_str: str) -> Optional[str]:
    """Append source and date footer to caption."""
    parts = []
    if source:
        username = source.lstrip("@")
        url = _escape_url_parens(f"https://t.me/{username}")
        escaped_source = _escape_markdown_link_text(source)
        parts.append(f'منبع: [{escaped_source}]({url})')
    if date_str:
        parts.append(date_str)
    footer = " \n ".join(parts) if parts else ""

    if caption and footer:
        return f"{caption}\n\n{footer}"
    if footer:
        return footer
    return caption


def _guess_suffix(msg) -> str:
    if isinstance(msg.media, MessageMediaPhoto):
        return ".jpg"
    if isinstance(msg.media, MessageMediaDocument):
        mime = msg.media.document.mime_type or ""
        if mime == "video/mp4":
            return ".mp4"
        if mime == "video/webm":
            return ".webm"
        if mime.startswith("image/"):
            return f".{mime.split('/')[-1]}"
        for attr in msg.media.document.attributes:
            name = getattr(attr, "file_name", None)
            if name:
                return Path(name).suffix or ".bin"
    return ".bin"


def _safe_remove(path: str):
    try:
        if path:
            Path(path).unlink(missing_ok=True)
    except Exception:
        pass


def _remove_row_media(row):
    """Delete media files stored for a DB row (single path or album JSON)."""
    media_path = row["media_paths"]
    if not media_path:
        return
    if row["media_type"] == "album":
        for item in json.loads(media_path):
            _safe_remove(item["path"])
    else:
        _safe_remove(media_path)


def _caption_with_oversize_note(caption: Optional[str], source: Optional[str]) -> str:
    """Append an oversize-media notice just before the source line in caption."""
    note = "[به دلیل محدودیت ۲۰ مگابایتی ارسال فایل در بله، ارسال این ویدئو موفقیت‌آمیز نبود]"
    if not caption:
        return note
    # Insert note before the "منبع:" footer line (which may be plain or HTML anchor)
    # Search for the literal string "منبع:" regardless of what follows
    marker = "منبع:"
    idx = caption.find(marker)
    if idx != -1:
        return caption[:idx] + note + "\n" + caption[idx:]
    return f"{caption}\n{note}"