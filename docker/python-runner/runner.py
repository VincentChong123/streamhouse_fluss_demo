"""Python execution service for the Query Workspace.

Runs one submitted program at a time in a child process with:
  * a wall-clock limit (timeout_seconds) and a CPU-time limit,
  * a cap on captured stdout/stderr (OUTPUT_LIMIT bytes each),
  * a file-size limit and a private temporary directory,
  * a minimal environment (FLUSS_BOOTSTRAP_SERVERS only).

Container-level isolation (compose.yaml): no host mounts, no Docker socket, read-only root
filesystem, tmpfs /tmp, memory and PID limits, all capabilities dropped, non-root user, and a
network that reaches only Fluss and the demo app.

The service is internal to the Compose network; the demo app is its only client.
"""

import json
import os
import resource
import shutil
import signal
import subprocess
import sys
import tempfile
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

OUTPUT_LIMIT = int(os.environ.get("RUNNER_OUTPUT_LIMIT_BYTES", "65536"))
MAX_TIMEOUT = int(os.environ.get("RUNNER_MAX_TIMEOUT_SECONDS", "60"))
MAX_CODE = 20_000
FLUSS = os.environ.get("FLUSS_BOOTSTRAP_SERVERS", "fluss-coordinator:9123")

_lock = threading.Lock()
_current = {"id": None, "proc": None, "canceled": False}


def _pyfluss_version():
    try:
        from importlib.metadata import version
        return version("pyfluss")
    except Exception:  # pragma: no cover
        return "unknown"


def _limits(timeout):
    def apply():
        os.setsid()
        resource.setrlimit(resource.RLIMIT_CPU, (timeout + 5, timeout + 5))
        resource.setrlimit(resource.RLIMIT_FSIZE, (1 << 20, 1 << 20))
        resource.setrlimit(resource.RLIMIT_CORE, (0, 0))
    return apply


def _reader(stream, sink):
    total = 0
    for chunk in iter(lambda: stream.read(4096), b""):
        if total < OUTPUT_LIMIT:
            sink["data"] += chunk[: OUTPUT_LIMIT - total]
        else:
            sink["truncated"] = True
        total += len(chunk)
    stream.close()


def run(execution_id, code, timeout):
    workdir = tempfile.mkdtemp(prefix="exec-", dir="/tmp")
    path = os.path.join(workdir, "main.py")
    with open(path, "w", encoding="utf-8") as f:
        f.write(code)
    env = {"PATH": "/usr/local/bin:/usr/bin:/bin", "HOME": workdir, "PYTHONUNBUFFERED": "1",
           "PYTHONDONTWRITEBYTECODE": "1", "FLUSS_BOOTSTRAP_SERVERS": FLUSS}
    started = time.monotonic()
    proc = subprocess.Popen([sys.executable, "-I", "-B", path], cwd=workdir, env=env, stdin=subprocess.DEVNULL,
                            stdout=subprocess.PIPE, stderr=subprocess.PIPE, preexec_fn=_limits(timeout))
    with _lock:
        _current.update(id=execution_id, proc=proc, canceled=False)
    out, err = {"data": b"", "truncated": False}, {"data": b"", "truncated": False}
    threads = [threading.Thread(target=_reader, args=(proc.stdout, out)),
               threading.Thread(target=_reader, args=(proc.stderr, err))]
    for t in threads:
        t.start()
    timed_out = False
    try:
        proc.wait(timeout=timeout)
    except subprocess.TimeoutExpired:
        timed_out = True
        _kill(proc)
        proc.wait()
    for t in threads:
        t.join(timeout=5)
    duration_ms = int((time.monotonic() - started) * 1000)
    with _lock:
        canceled = _current["canceled"] and _current["id"] == execution_id
        _current.update(id=None, proc=None, canceled=False)
    shutil.rmtree(workdir, ignore_errors=True)
    return {
        "execution_id": execution_id,
        "exit_code": proc.returncode,
        "stdout": out["data"].decode("utf-8", "replace"),
        "stderr": err["data"].decode("utf-8", "replace"),
        "truncated": out["truncated"] or err["truncated"],
        "timed_out": timed_out and not canceled,
        "canceled": canceled,
        "duration_ms": duration_ms,
        "limits": {"timeout_seconds": timeout, "output_bytes": OUTPUT_LIMIT, "file_size_bytes": 1 << 20,
                   "memory": "container limit (compose.yaml)"},
    }


def _kill(proc):
    try:
        os.killpg(proc.pid, signal.SIGKILL)
    except ProcessLookupError:
        pass


class Handler(BaseHTTPRequestHandler):
    server_version = "python-runner/1"

    def _send(self, status, body):
        data = json.dumps(body).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def _body(self):
        n = int(self.headers.get("Content-Length") or 0)
        if n > 256 * 1024:
            raise ValueError("request too large")
        return json.loads(self.rfile.read(n) or b"{}")

    def do_GET(self):
        if self.path == "/health":
            self._send(200, {"ok": True, "python": sys.version.split()[0], "pyfluss": _pyfluss_version(),
                             "busy": _current["id"] is not None})
        else:
            self._send(404, {"error": "not found"})

    def do_POST(self):
        try:
            body = self._body()
        except Exception as e:
            self._send(400, {"error": "invalid request: %s" % e})
            return
        if self.path == "/run":
            code = body.get("code") or ""
            if not code.strip() or len(code) > MAX_CODE:
                self._send(400, {"error": "code must be 1..%d characters" % MAX_CODE})
                return
            timeout = max(1, min(int(body.get("timeout_seconds") or 20), MAX_TIMEOUT))
            with _lock:
                if _current["id"] is not None:
                    self._send(429, {"error": "another execution is running (%s)" % _current["id"]})
                    return
                _current["id"] = body.get("execution_id") or "anonymous"
            try:
                self._send(200, run(_current["id"], code, timeout))
            except Exception as e:
                with _lock:
                    _current.update(id=None, proc=None, canceled=False)
                self._send(500, {"error": "runner failure: %s" % e})
        elif self.path == "/cancel":
            with _lock:
                if _current["id"] == body.get("execution_id") and _current["proc"] is not None:
                    _current["canceled"] = True
                    _kill(_current["proc"])
                    self._send(200, {"canceled": True})
                    return
            self._send(409, {"error": "no such running execution"})
        else:
            self._send(404, {"error": "not found"})

    def log_message(self, fmt, *args):
        sys.stderr.write("python-runner: " + (fmt % args) + "\n")


if __name__ == "__main__":
    print("python-runner listening on :8000 (pyfluss %s)" % _pyfluss_version(), flush=True)
    ThreadingHTTPServer(("0.0.0.0", 8000), Handler).serve_forever()
