"""
bale.py — Bale Bot API client.
Docs: https://docs.bale.ai/
"""

import logging
import time
from pathlib import Path
from typing import Optional

import httpx

logger = logging.getLogger(__name__)

BALE_API_BASE = "https://tapi.bale.ai/bot{token}/{method}"
MAX_FILE_SIZE = 20 * 1024 * 1024  # 20 MB
MAX_RATE_LIMIT_RETRIES = 3        # retries after HTTP 429 before giving up


class BaleClient:
    def __init__(self, bot_token: str, channel_id: str):
        self.bot_token = bot_token
        self.channel_id = channel_id
        # connect timeout short, read/write timeout long for heavy media uploads
        self._client = httpx.Client(
            timeout=httpx.Timeout(connect=10, read=300, write=300, pool=10)
        )

    def _url(self, method: str) -> str:
        return BALE_API_BASE.format(token=self.bot_token, method=method)

    def _request(self, method: str, **kwargs) -> httpx.Response:
        """POST to Bale, sleeping and retrying when rate-limited (HTTP 429)."""
        for attempt in range(MAX_RATE_LIMIT_RETRIES + 1):
            # open file handles may have been consumed by a previous upload of this request
            for f in (kwargs.get("files") or {}).values():
                if hasattr(f, "seek"):
                    f.seek(0)
            resp = self._client.post(self._url(method), **kwargs)
            if resp.status_code != 429 or attempt == MAX_RATE_LIMIT_RETRIES:
                return resp
            wait = _retry_after(resp)
            logger.warning("Bale rate limit [%s], retrying in %ss", method, wait)
            time.sleep(wait)
        return resp

    def _post(self, method: str, **kwargs) -> Optional[dict]:
        """Generic POST to Bale API. Returns parsed JSON or None on error."""
        try:
            resp = self._request(method, **kwargs)
            # a 400 never heals on retry; the usual causes are Markdown Bale can't parse or a
            # reply target gone from Bale, so drop those in turn (text then shows raw Markdown).
            # Sends only: an edit's 400 may be "not modified", and retrying it without Markdown
            # would replace a fine post with raw Markdown
            for field in ("parse_mode", "reply_to_message_id"):
                if resp.status_code == 400 and method.startswith("send") and _drop_field(kwargs, field):
                    logger.warning("Bale rejected [%s] with 400, retrying without %s", method, field)
                    resp = self._request(method, **kwargs)
            resp.raise_for_status()
            data = resp.json()
            if not data.get("ok"):
                logger.error("Bale API error [%s]: %s", method, data.get("description"))
                return None
            return data
        except httpx.HTTPError as e:
            logger.error("Bale HTTP error [%s]: %s", method, e)
            return None

    def check_connectivity(self) -> bool:
        """Ping Bale API. Returns True if reachable."""
        try:
            resp = self._client.get(self._url("getMe"), timeout=10)
            data = resp.json()
            return data.get("ok", False)
        except Exception:
            return False

    def send_text(self, text: str, reply_to_message_id: Optional[int] = None) -> Optional[int]:
        """Returns Bale message_id on success, None on failure."""
        if not text or not text.strip():
            return None
        payload: dict = {
            "chat_id": self.channel_id,
            "text": text[:4096],
            "parse_mode": "Markdown",
        }
        if reply_to_message_id:
            payload["reply_to_message_id"] = reply_to_message_id
        result = self._post("sendMessage", json=payload)
        return result["result"]["message_id"] if result else None

    def edit_message_text(self, bale_message_id: int, text: str) -> bool:
        """Edit an existing Bale text message. Returns True on success."""
        result = self._post(
            "editMessageText",
            json={
                "chat_id": self.channel_id,
                "message_id": bale_message_id,
                "text": text[:4096],
                "parse_mode": "Markdown",
            },
        )
        return result is not None

    def edit_message_caption(self, bale_message_id: int, caption: str) -> bool:
        """Edit caption of an existing Bale media message.
        Falls back to editMessageText if editMessageCaption is not supported (404).
        Returns True on success."""
        try:
            resp = self._request(
                "editMessageCaption",
                json={
                    "chat_id": self.channel_id,
                    "message_id": bale_message_id,
                    "caption": caption[:4096],
                    "parse_mode": "Markdown",
                },
            )
            if resp.status_code == 404:
                # Bale may not support editMessageCaption — fall back to editMessageText
                logger.warning("editMessageCaption returned 404, falling back to editMessageText")
                return self.edit_message_text(bale_message_id, caption)
            resp.raise_for_status()
            data = resp.json()
            if not data.get("ok"):
                logger.error("Bale API error [editMessageCaption]: %s", data.get("description"))
                return False
            return True
        except httpx.HTTPError as e:
            logger.error("Bale HTTP error [editMessageCaption]: %s", e)
            return False

    def send_photo(self, file_path: str, caption: Optional[str] = None,
                   reply_to_message_id: Optional[int] = None) -> Optional[int]:
        if not _check_size(file_path):
            logger.warning("Photo too large (>20MB), skipping: %s", file_path)
            return None
        data: dict = {
            "chat_id": self.channel_id,
            "caption": (caption or "")[:4096],
            "parse_mode": "Markdown",
        }
        if reply_to_message_id:
            data["reply_to_message_id"] = reply_to_message_id
        with open(file_path, "rb") as f:
            result = self._post("sendPhoto", data=data, files={"photo": f})
        return result["result"]["message_id"] if result else None

    def send_video(self, file_path: str, caption: Optional[str] = None,
                   reply_to_message_id: Optional[int] = None) -> Optional[int]:
        if not _check_size(file_path):
            logger.warning("Video too large (>20MB), skipping: %s", file_path)
            return None
        data: dict = {
            "chat_id": self.channel_id,
            "caption": (caption or "")[:4096],
            "parse_mode": "Markdown",
        }
        if reply_to_message_id:
            data["reply_to_message_id"] = reply_to_message_id
        with open(file_path, "rb") as f:
            result = self._post("sendVideo", data=data, files={"video": f})
        return result["result"]["message_id"] if result else None

    def send_document(self, file_path: str, caption: Optional[str] = None,
                      reply_to_message_id: Optional[int] = None) -> Optional[int]:
        if not _check_size(file_path):
            logger.warning("Document too large (>20MB), skipping: %s", file_path)
            return None
        data: dict = {
            "chat_id": self.channel_id,
            "caption": (caption or "")[:4096],
            "parse_mode": "Markdown",
        }
        if reply_to_message_id:
            data["reply_to_message_id"] = reply_to_message_id
        with open(file_path, "rb") as f:
            result = self._post("sendDocument", data=data, files={"document": f})
        return result["result"]["message_id"] if result else None

    def send_media_group(self, media_items: list[dict], caption: Optional[str] = None,
                         reply_to_message_id: Optional[int] = None) -> list[int]:
        """
        media_items: list of {"type": "photo"|"video"|"document", "path": str}
        Sends all items as a single gallery (sendMediaGroup API).
        Caption goes on the first item only.
        Returns message_ids of every sent message (first one carries the caption), or [].

        Bale API limitation: sendMediaGroup only supports homogeneous groups
        (all photos OR all videos). Mixed photo+video groups always return
        UNSUPPORTED_FILE (400). We detect this upfront and go straight to
        individual sends to avoid the wasted round-trip and the lost caption.
        """
        if not media_items:
            return []

        # Filter out missing files
        valid_items = [item for item in media_items if item.get("path") and Path(item["path"]).exists()]
        if not valid_items:
            return []

        # If only one item, send individually
        if len(valid_items) == 1:
            item = valid_items[0]
            msg_id = self._send_single_item(item["path"], item.get("type", "photo"), caption,
                                            reply_to_message_id)
            return [msg_id] if msg_id else []

        # ── Bale limitation: sendMediaGroup rejects mixed photo+video groups ──
        # Split into homogeneous sub-groups (photos / videos) and send each as its
        # own sendMediaGroup — both carrying the same caption so it is never lost.
        types_in_group = {item.get("type", "photo") for item in valid_items}
        is_mixed = len(types_in_group) > 1
        if is_mixed:
            logger.info(
                "Media group contains mixed types %s — splitting into homogeneous "
                "sub-groups (each with caption).",
                sorted(types_in_group),
            )
            return self._send_split_by_type(valid_items, caption, reply_to_message_id)

        # Homogeneous group — delegate to shared helper
        return self._send_homogeneous_group(valid_items, caption, reply_to_message_id)

    def _send_split_by_type(self, valid_items: list, caption: Optional[str],
                            reply_to_message_id: Optional[int] = None) -> list[int]:
        """Send a mixed photo+video group by splitting into homogeneous sub-groups.

        Each sub-group is sent as a proper sendMediaGroup call and receives the
        same caption, so no matter which sub-group the user reads first, the
        context is there. The message_id of the first successfully-sent sub-group
        is returned (used as the canonical Bale message_id in the DB).

        Sub-group order: photos first, then videos — preserving original ordering
        within each type.
        """
        from collections import defaultdict as _dd
        buckets: dict[str, list] = _dd(list)
        for item in valid_items:
            buckets[item.get("type", "photo")].append(item)

        sent_ids: list[int] = []

        # Canonical order: photo → video → document
        for media_type in ("photo", "video", "document"):
            group = buckets.get(media_type)
            if not group:
                continue

            # caption rides on the first sub-group only
            sub_caption = caption if not sent_ids else None

            logger.info(
                "Sending sub-group: %d %s(s) (caption present: %s)",
                len(group), media_type, bool(sub_caption),
            )

            if len(group) == 1:
                msg_id = self._send_single_item(
                    group[0]["path"], media_type, sub_caption, reply_to_message_id
                )
                ids = [msg_id] if msg_id else []
            else:
                ids = self._send_homogeneous_group(group, sub_caption, reply_to_message_id)

            if not ids:
                logger.warning("Sub-group send failed for type=%s", media_type)
                return self._rollback(sent_ids)
            sent_ids.extend(ids)

        return sent_ids

    def _rollback(self, sent_ids: list[int]) -> list[int]:
        """Delete the part of an album that did get through and report failure, so the caller
        retries the whole album instead of keeping it half-sent. Always returns []."""
        if sent_ids:
            logger.warning("Album only partly sent; deleting %s so it is retried whole", sent_ids)
            for mid in sent_ids:
                self.delete_message(mid)
        return []

    def _send_homogeneous_group(self, items: list, caption: Optional[str],
                                reply_to_message_id: Optional[int] = None) -> list[int]:
        """Send a list of same-type items via sendMediaGroup. No mixed-type check here."""
        import json as _json

        media_array = []
        files = {}
        mime_map = {
            ".jpg": "image/jpeg", ".jpeg": "image/jpeg", ".png": "image/png",
            ".gif": "image/gif", ".webp": "image/webp",
            ".mp4": "video/mp4", ".webm": "video/webm", ".mov": "video/quicktime",
        }

        for i, item in enumerate(items):
            media_type = item.get("type", "photo")
            path = item["path"]
            suffix = Path(path).suffix.lower()
            attach_name = f"file{i}"
            safe_filename = f"{attach_name}{suffix}"

            media_entry: dict = {
                "type": media_type if media_type in ("photo", "video") else "document",
                "media": f"attach://{attach_name}",
            }
            if i == 0 and caption:
                media_entry["caption"] = caption[:4096]
                media_entry["parse_mode"] = "Markdown"

            media_array.append(media_entry)

            mime = mime_map.get(suffix, "application/octet-stream")
            with open(path, "rb") as fh:
                file_bytes = fh.read()
            files[attach_name] = (safe_filename, file_bytes, mime)

        media_summary = [(item.get("type"), Path(item["path"]).name) for item in items]
        logger.debug("_send_homogeneous_group: %d items: %s", len(items), media_summary)

        try:
            data = {
                "chat_id": self.channel_id,
                "media": _json.dumps(media_array),
            }
            if reply_to_message_id:
                data["reply_to_message_id"] = str(reply_to_message_id)

            resp = self._request("sendMediaGroup", data=data, files=files)
            result = resp.json()

            if result.get("ok"):
                return [m["message_id"] for m in result.get("result", []) if m.get("message_id")]
            else:
                logger.error(
                    "sendMediaGroup (homogeneous) error — status=%s | description=%s | items=%s",
                    resp.status_code, result.get("description"), media_summary,
                )
                if resp.status_code < 500:
                    # Last resort: individual sends for this sub-group
                    return self._fallback_individual(items, caption, reply_to_message_id)
                return []

        except httpx.HTTPStatusError as e:
            logger.error(
                "sendMediaGroup (homogeneous) HTTP error — status=%s | items=%s",
                e.response.status_code, media_summary,
            )
            if e.response.status_code < 500:
                return self._fallback_individual(items, caption, reply_to_message_id)
        except Exception as e:
            logger.error("sendMediaGroup (homogeneous) unexpected error — %s | items=%s",
                         e, media_summary)

        return []

    def _fallback_individual(self, valid_items: list, caption: Optional[str],
                             reply_to_message_id: Optional[int] = None) -> list[int]:
        """Send media group items individually as a fallback.

        Caption goes on the first item. All or nothing: if any item fails, the ones
        already sent are deleted and [] is returned so the album is retried whole.
        """
        logger.warning(
            "Sending %d media items individually (caption present: %s)",
            len(valid_items),
            bool(caption),
        )
        sent_ids: list[int] = []

        for item in valid_items:
            item_caption = caption if not sent_ids else None
            # Only the very first message gets reply_to; keep thread tidy
            reply_id = reply_to_message_id if not sent_ids else None

            msg_id = self._send_single_item(
                item["path"], item.get("type", "photo"), item_caption, reply_id
            )

            if msg_id is None:
                logger.warning(
                    "Individual send failed for item %s (type=%s)",
                    Path(item["path"]).name,
                    item.get("type"),
                )
                return self._rollback(sent_ids)
            sent_ids.append(msg_id)

        return sent_ids

    def _send_single_item(self, path: str, media_type: str, caption: Optional[str],
                          reply_to_message_id: Optional[int] = None) -> Optional[int]:
        """Send a single media file."""
        if media_type == "photo":
            return self.send_photo(path, caption, reply_to_message_id)
        elif media_type == "video":
            return self.send_video(path, caption, reply_to_message_id)
        else:
            return self.send_document(path, caption, reply_to_message_id)

    def delete_message(self, bale_message_id: int) -> bool:
        """Delete a message from the Bale channel. Returns True on success."""
        result = self._post(
            "deleteMessage",
            json={
                "chat_id": self.channel_id,
                "message_id": bale_message_id,
            },
        )
        return result is not None

    def close(self):
        self._client.close()


def _retry_after(resp: httpx.Response) -> int:
    """Seconds to wait from a 429 response (Telegram-style parameters.retry_after), clamped to 1..60."""
    try:
        wait = int(resp.json()["parameters"]["retry_after"])
    except Exception:
        wait = 5
    return max(1, min(wait, 60))


def _drop_field(kwargs: dict, field: str) -> bool:
    """Remove field from the request's json/data payload. True if it was there."""
    payload = kwargs.get("json") or kwargs.get("data") or {}
    return payload.pop(field, None) is not None


def _check_size(file_path: str) -> bool:
    try:
        return Path(file_path).stat().st_size <= MAX_FILE_SIZE
    except OSError:
        return False
