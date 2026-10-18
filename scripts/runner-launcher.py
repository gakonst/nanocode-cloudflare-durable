import ctypes
import os
import subprocess
import sys


def main():
    libc = ctypes.CDLL(None, use_errno=True)
    if libc.prctl(4, 0, 0, 0, 0) != 0:
        raise RuntimeError("runner launcher hardening failed")
    token = os.environ.pop("NANOCODE_RUNNER_TOKEN", None)
    if not token or "\n" in token or "\r" in token:
        raise RuntimeError("runner launcher credential unavailable")
    child = subprocess.Popen(
        ["/usr/local/libexec/nanocode-runner", "--token-stdin", *sys.argv[1:]],
        stdin=subprocess.PIPE,
        env=os.environ.copy(),
    )
    child.stdin.write((token + "\n").encode())
    child.stdin.close()
    del token
    return child.wait()


if __name__ == "__main__":
    try:
        sys.exit(main())
    except Exception:
        print("runner launcher failed", file=sys.stderr)
        sys.exit(1)
