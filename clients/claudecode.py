"""Native-mode Claude Code agent for AIOpsLab (SREGym-style).

Instead of a one-AIOpsLab-action-per-turn text picker, this runs Claude Code's
*native agentic loop* with real `Bash` (+ a kubeconfig pointed at the worker
cluster), so Claude investigates and fixes the cluster itself. It ends with a
structured `ANSWER:` line, which we map to a single AIOpsLab submit(...).

One AIOpsLab "turn" == one full native Claude Code run. Reuses AIOpsLab's
deploy/fault-injection/eval; only the agent's brain changes.
"""
from __future__ import annotations

import asyncio
import json
import os
import re
import subprocess
from pathlib import Path

try:
    from dotenv import load_dotenv
    load_dotenv("/root/.env"); load_dotenv()
except Exception:
    pass

CLAUDE_BIN = os.environ.get("CLAUDE_BIN", "/root/.local/bin/claude")
ALLOWED_TOOLS = ["Bash", "Read", "Grep", "Glob", "LS", "Edit", "Write", "TodoWrite"]


class ClaudeCodeNativeAgent:
    _FMT = {
        "detection": 'On the LAST line output exactly:  ANSWER: Yes   (anomaly present)  OR  ANSWER: No',
        "localization": 'On the LAST line output exactly:  ANSWER: <comma-separated faulty service name(s)>  (e.g. ANSWER: product-catalog)',
        "analysis": 'On the LAST line output exactly:  ANSWER: system_level=<level>; fault_type=<type>',
        "mitigation": 'Actually APPLY the fix with kubectl, verify the app recovers, then on the LAST line output exactly:  ANSWER: done',
    }

    def __init__(self):
        self.model = os.environ.get("CLAUDE_CODE_MODEL", "claude-opus-4-8")
        self.timeout = float(os.environ.get("CLAUDE_NATIVE_TIMEOUT", "1200"))
        self.max_turns = os.environ.get("CLAUDE_NATIVE_MAX_TURNS", "60")
        base = Path(os.environ.get("CLAUDE_LOGS_DIR", "/root/bench/cc_native_logs"))
        self.problem_id = os.environ.get("AIOPSLAB_PROBLEM_ID") or f"prob_{os.getpid()}"
        self.logs_dir = base
        self.logs_dir.mkdir(parents=True, exist_ok=True)
        self.namespace = "default"
        self.task_kind = "detection"
        self.submitted = False

    @staticmethod
    def _detect(blob: str) -> str:
        if "system_level" in blob or "fault_type" in blob or "fault type" in blob:
            return "analysis"
        if "localiz" in blob or "faulty component" in blob or "which service" in blob:
            return "localization"
        if "mitigat" in blob or "remediat" in blob or "resolve the" in blob or "fix the" in blob:
            return "mitigation"
        return "detection"

    def init_context(self, problem_desc: str, instructions: str, apis: dict):
        self.problem_desc = problem_desc
        self.instructions = instructions
        m = re.search(r"Namespace\s*:\s*(\S+)", problem_desc)
        self.namespace = m.group(1).strip() if m else "default"
        self.task_kind = self._detect((problem_desc + "\n" + instructions).lower())

    def _prompt(self) -> str:
        return (
            "You are an autonomous SRE agent debugging a Kubernetes microservice app.\n"
            "You have FULL kubectl access via the Bash tool (KUBECONFIG is already set to the "
            f"target cluster). The application under test is in namespace: {self.namespace}.\n\n"
            "TASK CONTEXT:\n" + self.problem_desc.strip() + "\n\n" + self.instructions.strip() + "\n\n"
            "Investigate the cluster directly with kubectl (get pods/events/logs, describe, "
            "check services/endpoints/configmaps, etc.). Reason across what you find.\n"
            f"This is a {self.task_kind} task. {self._FMT[self.task_kind]}\n"
            "Do not ask questions; act autonomously and finish with the single ANSWER line."
        )

    async def get_action(self, _input: str) -> str:
        if self.submitted:
            return self._fence(self._fallback())
        self.submitted = True
        out = await asyncio.to_thread(self._run_native, self._prompt())
        return self._fence(self._answer_to_submit(out))


    def _run_native(self, prompt: str) -> str:
        d = self.logs_dir / self.problem_id
        d.mkdir(parents=True, exist_ok=True)
        (d / "prompt.txt").write_text(prompt)
        env = os.environ.copy()
        env["PATH"] = "/root/.local/bin:" + env.get("PATH", "")
        env["CLAUDE_CONFIG_DIR"] = str(d / "claude_config")
        env["ANTHROPIC_MODEL"] = self.model
        cmd = [CLAUDE_BIN, "-p", prompt, "--model", self.model,
               "--output-format", "stream-json", "--verbose",
               "--max-turns", str(self.max_turns), "--allowedTools"] + ALLOWED_TOOLS
        try:
            proc = subprocess.run(cmd, env=env, text=True, stdout=subprocess.PIPE,
                                  stderr=subprocess.STDOUT, timeout=self.timeout, stdin=subprocess.DEVNULL)
            out = proc.stdout or ""
        except subprocess.TimeoutExpired as e:
            out = (e.stdout or "") if isinstance(e.stdout, str) else ""
        (d / "claude_native.out").write_text(out)
        return out

    def _final_text(self, out: str) -> str:
        texts = []
        for ln in out.splitlines():
            ln = ln.strip()
            if not ln.startswith("{"):
                continue
            try:
                ev = json.loads(ln)
            except Exception:
                continue
            if ev.get("type") == "result" and ev.get("result"):
                texts.append(str(ev["result"]))
            elif ev.get("type") == "assistant":
                msg = ev.get("message", {})
                for c in (msg.get("content") if isinstance(msg.get("content"), list) else []):
                    if isinstance(c, dict) and c.get("type") == "text":
                        texts.append(c.get("text", ""))
        return "\n".join(texts) if texts else out

    def _answer_to_submit(self, out: str) -> str:
        text = self._final_text(out)
        m = list(re.finditer(r"ANSWER:\s*(.+)", text, re.IGNORECASE))
        ans = m[-1].group(1).strip() if m else ""
        tk = self.task_kind
        if tk == "detection":
            yes = ans.lower().startswith("y") or (not ans and "anomal" in text.lower())
            return f'submit("{"Yes" if yes else "No"}")'
        if tk == "localization":
            svcs = [s.strip().strip('"[]') for s in re.split(r"[,\s]+", ans) if s.strip().strip('"[]')]
            return f"submit({json.dumps(svcs)})" if svcs else "submit([])"
        if tk == "analysis":
            sl = re.search(r"system_level\s*=\s*([^;]+)", ans, re.I)
            ft = re.search(r"fault_type\s*=\s*(.+)", ans, re.I)
            dd = {}
            if sl: dd["system_level"] = sl.group(1).strip()
            if ft: dd["fault_type"] = ft.group(1).strip()
            return f"submit({json.dumps(dd)})"
        return "submit()"

    def _fallback(self) -> str:
        return {"detection": 'submit("No")', "localization": "submit([])",
                "analysis": "submit({})", "mitigation": "submit()"}.get(self.task_kind, 'submit("No")')

    @staticmethod
    def _fence(call: str) -> str:
        return f"```\n{call}\n```"
