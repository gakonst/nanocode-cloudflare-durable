import base64
import http.server
import json
import os
import pwd
import shutil
import signal
import subprocess
import threading
import time

RUNNER = pwd.getpwnam("runner")
LOCK = threading.Lock()
ALLOWED = ("/workspace", "/tmp", "/home/user")
LIMIT = 4 * 1024 * 1024


def allowed(path):
    return any(path == root or path.startswith(root + "/") for root in ALLOWED)


def stop_runner():
    for _ in range(3):
        found = False
        for entry in os.listdir("/proc"):
            if not entry.isdigit():
                continue
            try:
                if os.stat("/proc/" + entry).st_uid == RUNNER.pw_uid:
                    found = True
                    os.kill(int(entry), signal.SIGKILL)
            except (FileNotFoundError, ProcessLookupError, PermissionError):
                pass
        if not found:
            return
        time.sleep(0.05)


def own(path):
    try:
        os.lchown(path, RUNNER.pw_uid, RUNNER.pw_gid)
    except FileNotFoundError:
        pass


def reset(roots, dirs, files):
    for root in roots:
        if os.path.isdir(root) and not os.path.islink(root):
            for name in os.listdir(root):
                target = os.path.join(root, name)
                if os.path.isdir(target) and not os.path.islink(target):
                    shutil.rmtree(target, ignore_errors=True)
                else:
                    os.unlink(target)
        elif os.path.lexists(root):
            os.unlink(root)
    for path in sorted(dirs):
        os.makedirs(path, exist_ok=True)
        own(path)
    for path, data in files.items():
        parent = os.path.dirname(path)
        if not os.path.isdir(parent):
            os.makedirs(parent, exist_ok=True)
            own(parent)
        with open(path, "wb") as handle:
            handle.write(data)
        own(path)


def collect(roots, before):
    dirs, files = [], []
    seen = set()

    def visit(path):
        if path in seen or not os.path.lexists(path):
            return
        seen.add(path)
        if os.path.isdir(path) and not os.path.islink(path):
            dirs.append(path)
            for name in sorted(os.listdir(path)):
                visit(os.path.join(path, name))
            return
        if not os.path.isfile(path):
            return
        with open(path, "rb") as handle:
            data = handle.read()
        files.append([path, None if before.get(path) == data else base64.b64encode(data).decode()])

    for root in roots:
        visit(root)
    return dirs, files


def argv(request):
    kind, target, rest = request["kind"], request["target"], request["argv"][1:]
    if kind == "version":
        return ["python3", "--version"]
    if kind == "code":
        return ["python3", "-c", target, *rest]
    if kind == "file":
        return ["python3", request["filename"], *rest]
    if kind == "module":
        return ["python3", "-m", target, *rest]
    if kind == "stdin":
        return ["python3", "-", *rest]
    return ["python3", "-m", "pip", *request["argv"]]


def text(data):
    return data[:LIMIT].decode("utf-8", "backslashreplace") + ("\n[output truncated]\n" if len(data) > LIMIT else "")


def run(request):
    roots = request["roots"]
    refused = [root for root in roots if not allowed(root)]
    if refused:
        return {"status": 1, "stdout": "", "stderr": "python3 runs only under /workspace, /tmp or /home/user, not " + ", ".join(refused) + "\n", "dirs": [], "files": []}
    before = {path: base64.b64decode(data) for path, data in request["files"]}
    with LOCK:
        stop_runner()
        reset(roots, request["dirs"], before)
        cwd = request["cwd"] if allowed(request["cwd"]) else "/workspace"
        os.makedirs(cwd, exist_ok=True)
        own(cwd)
        env = {**request["env"], "HOME": "/home/user", "PATH": "/home/user/.local/bin:/usr/local/bin:/usr/bin:/bin", "MPLBACKEND": "Agg", "MPLCONFIGDIR": "/opt/mpl", "PYTHONDONTWRITEBYTECODE": "1", "PYTHONUNBUFFERED": "1", "PIP_USER": "1", "PIP_DISABLE_PIP_VERSION_CHECK": "1", "PIP_ROOT_USER_ACTION": "ignore"}
        stdin = (request["target"] if request["kind"] == "stdin" else request["stdin"]).encode()
        process = subprocess.Popen(argv(request), cwd=cwd, env=env, user=RUNNER.pw_uid, group=RUNNER.pw_gid, extra_groups=[], stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE, start_new_session=True)
        try:
            out, err = process.communicate(stdin, timeout=max(1, request.get("timeoutMs", 30000)) / 1000)
            status = process.returncode if process.returncode >= 0 else 128 - process.returncode
        except subprocess.TimeoutExpired:
            os.killpg(process.pid, signal.SIGKILL)
            out, err = process.communicate()
            status = -1
        stop_runner()
        dirs, files = collect(roots, before)
    return {"status": status, "stdout": text(out), "stderr": text(err), "dirs": dirs, "files": files}


class Handler(http.server.BaseHTTPRequestHandler):
    def reply(self, code, body):
        data = json.dumps(body).encode()
        self.send_response(code)
        self.send_header("content-type", "application/json")
        self.send_header("content-length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def do_GET(self):
        self.reply(200, {"ok": True})

    def do_POST(self):
        try:
            request = json.loads(self.rfile.read(int(self.headers["content-length"])))
            self.reply(200, run(request))
        except Exception as error:
            self.reply(500, {"error": type(error).__name__ + ": " + str(error)})

    def log_message(self, *args):
        pass


http.server.ThreadingHTTPServer(("0.0.0.0", 8080), Handler).serve_forever()
