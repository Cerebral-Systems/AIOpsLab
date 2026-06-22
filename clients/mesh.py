"""Mesh x AIOpsLab adapter - neutral bidirectional link."""

from __future__ import annotations

import asyncio
import json
import os
import re
import time
import urllib.parse
import urllib.request
from datetime import datetime, timezone
from typing import Any


MESH_URL = os.getenv("MESH_ENGINE_URL", "http://localhost:8879").rstrip("/")
SETTLE_SECONDS = float(os.getenv("MESH_SETTLE_SECONDS", "75"))
POLL_TIMEOUT = float(os.getenv("MESH_POLL_TIMEOUT", "420"))
POLL_INTERVAL = float(os.getenv("MESH_POLL_INTERVAL", "6"))
CLOCK_SKEW_SECONDS = float(os.getenv("MESH_CLOCK_SKEW_SECONDS", "10"))


def _get(path: str, timeout: float = 15) -> dict[str, Any]:
    req = urllib.request.Request(
        f"{MESH_URL}{path}",
        headers={"Accept": "application/json"},
        method="GET",
    )
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return json.loads(r.read().decode("utf-8"))


def _ts(value: Any) -> float:
    if not value:
        return 0.0
    try:
        dt = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.timestamp()
    except Exception:
        return 0.0


class MeshAgent:
    def __init__(self) -> None:
        self.namespace = "default"
        self.task_kind = "detection"
        self.started = time.time()
        self.submitted = False

    def init_context(self, problem_desc: str, instructions: str, apis: dict[str, str]) -> None:
        m = re.search(r"Namespace\s*:\s*(\S+)", problem_desc)
        self.namespace = m.group(1).strip() if m else "default"
        self.task_kind = self._task_kind((problem_desc + "\n" + instructions).lower())
        self.started = time.time()

    async def get_action(self, _input: str) -> str:
        if self.submitted:
            return self._fence(self._fallback())

        self.submitted = True
        result = await asyncio.to_thread(self._poll)
        return self._fence(self._submit(result))

    def _poll(self) -> dict[str, Any] | None:
        deadline = time.time() + POLL_TIMEOUT
        floor = self.started - CLOCK_SKEW_SECONDS

        while time.time() < deadline:
            for run in self._runs():
                if run.get("namespace") != self.namespace:
                    continue
                if _ts(run.get("created_at")) < floor:
                    continue
                if not self._terminal(run):
                    continue
                if time.time() - self.started < SETTLE_SECONDS:
                    continue

                return self._read(run.get("run_id")) or run

            time.sleep(POLL_INTERVAL)

        return None

    def _runs(self) -> list[dict[str, Any]]:
        try:
            runs = _get("/api/runs?summary=1").get("runs", [])
            return runs if isinstance(runs, list) else []
        except Exception:
            return []

    def _read(self, run_id: Any) -> dict[str, Any] | None:
        if not isinstance(run_id, str) or not run_id:
            return None
        try:
            return _get(f"/api/runs/{urllib.parse.quote(run_id)}")
        except Exception:
            return None

    @staticmethod
    def _terminal(run: dict[str, Any]) -> bool:
        return str(run.get("status", "")).lower() in {
            "completed",
            "done",
            "failed",
            "no_trigger",
            "no_anomaly",
        }

    def _submit(self, result: dict[str, Any] | None) -> str:
        if not result:
            return self._fallback()

        if self.task_kind == "detection":
            return 'submit("Yes")' if result.get("anomaly") is True else 'submit("No")'

        if self.task_kind == "localization":
            svc = result.get("localization")
            return f"submit([{json.dumps(svc)}])" if isinstance(svc, str) and svc else "submit([])"

        if self.task_kind == "analysis":
            taxonomy = result.get("taxonomy")
            return f"submit({json.dumps(taxonomy)})" if isinstance(taxonomy, dict) and taxonomy else "submit({})"

        # Mitigation must be performed by Mesh itself out-of-band.
        return "submit()"

    def _fallback(self) -> str:
        if self.task_kind == "detection":
            return 'submit("No")'
        if self.task_kind == "localization":
            return "submit([])"
        if self.task_kind == "analysis":
            return "submit({})"
        return "submit()"

    @staticmethod
    def _task_kind(blob: str) -> str:
        if "system_level" in blob or "fault_type" in blob or "fault type" in blob:
            return "analysis"
        if "localiz" in blob or "faulty component" in blob or "which service" in blob:
            return "localization"
        if "mitigat" in blob or "remediat" in blob or "resolve the" in blob or "fix the" in blob:
            return "mitigation"
        return "detection"

    @staticmethod
    def _fence(call: str) -> str:
        return f"```\n{call}\n```"
