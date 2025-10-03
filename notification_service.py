"""Backend integration for dispatching KPI alerts to mobile devices."""
from __future__ import annotations

import hashlib
import json
import logging
import os
from typing import Any, Mapping, Optional, Sequence

import requests

logger = logging.getLogger(__name__)


class NotificationService:
    """Wrapper for sending push notifications via Firebase Cloud Messaging."""

    def __init__(
        self,
        *,
        server_key: Optional[str] = None,
        default_tokens: Optional[Sequence[str]] = None,
        topic: Optional[str] = None,
        dry_run: bool = False,
        session: Optional[requests.Session] = None,
    ) -> None:
        self.server_key = server_key or os.getenv("FCM_SERVER_KEY")
        tokens = default_tokens if default_tokens is not None else os.getenv("FCM_DEVICE_TOKENS", "")
        if isinstance(tokens, str):
            token_list = [token.strip() for token in tokens.split(",") if token.strip()]
        else:
            token_list = [token for token in tokens if token]
        self.device_tokens = token_list
        self.topic = topic or os.getenv("FCM_TOPIC")
        dry_run_flag = dry_run or os.getenv("FCM_DRY_RUN", "false").lower() in {"1", "true", "yes"}
        self.dry_run = dry_run_flag
        self.session = session or requests.Session()
        self._last_payload_digest: Optional[str] = None

    @classmethod
    def from_settings(cls, settings: Optional[Mapping[str, Any]] = None) -> "NotificationService":
        settings = dict(settings or {})
        server_key = settings.get("server_key") or settings.get("firebase_server_key")
        tokens = settings.get("device_tokens") or settings.get("tokens")
        topic = settings.get("topic")
        dry_run = bool(settings.get("dry_run", False))
        return cls(server_key=server_key, default_tokens=tokens, topic=topic, dry_run=dry_run)

    @property
    def is_configured(self) -> bool:
        return bool(self.server_key and (self.device_tokens or self.topic))

    def compute_digest(self, payload: Mapping[str, Any]) -> str:
        canonical = json.dumps(payload, sort_keys=True, ensure_ascii=False)
        return hashlib.sha256(canonical.encode("utf-8")).hexdigest()

    def _build_payload(
        self,
        *,
        title: str,
        body: str,
        data: Optional[Mapping[str, Any]],
        tokens: Sequence[str],
    ) -> Mapping[str, Any]:
        payload: dict[str, Any] = {
            "notification": {
                "title": title,
                "body": body,
            },
            "data": {key: str(value) for key, value in (data or {}).items()},
        }
        if tokens:
            if len(tokens) == 1:
                payload["to"] = tokens[0]
            else:
                payload["registration_ids"] = list(tokens)
        elif self.topic:
            payload["to"] = f"/topics/{self.topic}"
        if self.dry_run:
            payload["dry_run"] = True
        return payload

    def send_alerts(
        self,
        alerts: Sequence[str],
        *,
        title: str,
        data: Optional[Mapping[str, Any]] = None,
        tokens: Optional[Sequence[str]] = None,
    ) -> bool:
        if not alerts:
            return False
        if not self.is_configured:
            logger.debug("NotificationService is not configured; skipping push notification.")
            return False

        tokens_to_use = list(tokens or self.device_tokens)
        body = "\n".join(alerts)
        payload = self._build_payload(title=title, body=body, data=data, tokens=tokens_to_use)
        digest = self.compute_digest({"alerts": alerts, "title": title, "data": data, "tokens": tokens_to_use})
        if digest == self._last_payload_digest:
            logger.debug("Identical notification payload detected; suppressing duplicate send.")
            return False

        headers = {
            "Authorization": f"key={self.server_key}",
            "Content-Type": "application/json; charset=UTF-8",
        }

        try:
            response = self.session.post(
                "https://fcm.googleapis.com/fcm/send",
                headers=headers,
                json=payload,
                timeout=10,
            )
            response.raise_for_status()
            logger.info("Sent %d alert(s) via Firebase Cloud Messaging.", len(alerts))
            self._last_payload_digest = digest
            return True
        except requests.RequestException:
            logger.exception("Failed to deliver KPI alert notification via FCM.")
            return False
