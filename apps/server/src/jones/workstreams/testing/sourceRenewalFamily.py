"""Private Linux subreaper for the source-renewal test consumer only."""
import ctypes
import hashlib
import json
import os
import select
import signal
import stat
import sys
import time


def subreaper():
    libc = ctypes.CDLL(None, use_errno=True)
    value = ctypes.c_int()
    if libc.prctl(36, 1, 0, 0, 0) or libc.prctl(37, ctypes.byref(value), 0, 0, 0) or value.value != 1:
        raise RuntimeError("subreaper unavailable")


def identity(pid):
    with open(f"/proc/{pid}/stat", encoding="ascii") as source:
        fields = source.read().rsplit(")", 1)[1].split()
    return int(fields[1]), [pid, int(fields[19])]


def children():
    with open(f"/proc/self/task/{os.getpid()}/children", encoding="ascii") as source:
        return [int(value) for value in source.read().split()]


def probe():
    subreaper()
    if not hasattr(signal, "pidfd_send_signal"):
        raise RuntimeError("pidfd_send_signal unavailable")
    read_fd, write_fd = os.pipe()
    pid = os.fork()
    if not pid:
        os.close(write_fd)
        signal.alarm(10)
        os.read(read_fd, 1)
        os._exit(0)
    os.close(read_fd)
    handle = None
    try:
        if pid not in children() or identity(pid)[0] != os.getpid():
            raise RuntimeError("owned child observation unavailable")
        handle = os.pidfd_open(pid)
        signal.pidfd_send_signal(handle, 0)
        os.close(write_fd)
        write_fd = None
        if not select.select([handle], [], [], 5)[0]:
            raise RuntimeError("pidfd readiness unavailable")
        if os.waitpid(pid, 0)[1] != 0:
            raise RuntimeError("probe child failed")
        try:
            os.waitpid(-1, os.WNOHANG)
        except ChildProcessError:
            print(json.dumps({"schema": "jones-source-renewal-family-capability/v1", "supported": True}))
        else:
            raise RuntimeError("ECHILD unavailable")
    finally:
        if write_fd is not None:
            os.close(write_fd)
        if handle is not None:
            os.close(handle)
        # This exact fork is ours; wait also owns the failure path.
        try:
            os.waitpid(pid, 0)
        except ChildProcessError:
            pass


def supervise(path):
    subreaper()
    if os.environ.get("NODE_CHANNEL_FD") != "3" or os.environ.get("NODE_CHANNEL_SERIALIZATION_MODE") != "json":
        raise RuntimeError("JSON IPC descriptor binding unavailable")
    with open(path, "rb") as source:
        raw = source.read(65537)
    if len(raw) > 65536:
        raise RuntimeError("request bound exceeded")
    request = json.loads(raw)
    if request["schema"] != "jones-source-renewal-family-request/v1" or request["ipcFd"] != 3 or request["controlFd"] != 4:
        raise RuntimeError("request schema mismatch")
    root = os.lstat(request["custodyRoot"])
    if not stat.S_ISDIR(root.st_mode) or [root.st_dev, root.st_ino, root.st_uid] != request["custodyRootIdentity"]:
        raise RuntimeError("root binding mismatch")
    bounds = request["bounds"]
    if bounds != {"executionDeadlineMs": 600000, "terminateGraceMs": 2000, "reapTimeoutMs": 3000, "maxTracked": 8, "maxReceiptBytes": 65536}:
        raise RuntimeError("unsupported bounds")
    control_stat = os.fstat(4)
    result = {key: request[key] for key in ("nonce", "attempt", "custodyRoot", "custodyRootIdentity")}
    result.update({"schema": "jones-source-renewal-family-receipt/v1", "requestSha256": hashlib.sha256(raw).hexdigest(),
                   "supervisorIdentity": identity(os.getpid())[1], "controlIdentity": [control_stat.st_dev, control_stat.st_ino],
                   "consumerIdentity": None, "consumerExit": None, "identities": [], "reapedUntracked": 0,
                   "signalsSent": [], "familyClosed": False, "reaped": False, "allIdentitiesExited": False,
                   "ipcFdReleased": False, "processOverflow": False, "ownershipUnresolved": False, "outcome": "unknown"})
    pid = os.fork()
    if not pid:
        os.close(4)
        os.set_inheritable(3, True)
        os.chdir(request["cwd"])
        os.execve(request["executable"], [request["executable"], *request["args"]], os.environ)
    started = time.monotonic()
    os.close(3)
    result["ipcFdReleased"] = True
    os.set_blocking(4, False)
    tracked = {}
    reaped = set()
    stop_at = None
    reason = None
    control_data = b""
    control_open = True
    kill_sent = False
    terminated = set()
    killed = set()

    def capture():
        try:
            listed = children()
        except OSError:
            result["ownershipUnresolved"] = True
            return
        for child in listed:
            if child in tracked:
                continue
            try:
                ppid, observed = identity(child)
                if ppid != os.getpid():
                    raise RuntimeError("child ownership changed")
                # No waitpid occurs before this validation and pidfd capture.
                if len(tracked) >= bounds["maxTracked"]:
                    result["processOverflow"] = True
                    continue
                handle = os.pidfd_open(child)
                tracked[child] = (observed, handle)
                result["identities"].append(observed)
                if child == pid:
                    result["consumerIdentity"] = observed
            except (OSError, RuntimeError):
                result["ownershipUnresolved"] = True

    def send_owned(sig, sent):
        for child, (observed, handle) in tracked.items():
            if child in reaped or child in sent:
                continue
            try:
                signal.pidfd_send_signal(handle, sig)
                sent.add(child)
                result["signalsSent"].append({"identity": observed, "signal": sig})
            except ProcessLookupError:
                pass
            except OSError:
                result["ownershipUnresolved"] = True

    try:
        while True:
            capture()
            empty = False
            while True:
                try:
                    child, status = os.waitpid(-1, os.WNOHANG)
                except ChildProcessError:
                    empty = True  # Genuine ECHILD, while this subreaper is alive.
                    break
                if not child:
                    break
                reaped.add(child)
                if child not in tracked:
                    result["reapedUntracked"] += 1
                if child == pid:
                    result["reaped"] = True
                    result["consumerExit"] = {"code": os.waitstatus_to_exitcode(status) if os.WIFEXITED(status) else None,
                                              "signal": signal.Signals(os.WTERMSIG(status)).name if os.WIFSIGNALED(status) else None}
            if empty and result["reaped"]:
                result["familyClosed"] = True
                result["allIdentitiesExited"] = all(child in reaped for child in tracked)
                break
            now = time.monotonic()
            if stop_at is None:
                if result["processOverflow"]:
                    reason = "overflow"
                elif result["ownershipUnresolved"]:
                    reason = "ownership-unresolved"
                elif result["reaped"]:
                    reason = "survivors"
                elif now - started >= bounds["executionDeadlineMs"] / 1000:
                    reason = "execution-deadline"
                if control_open and select.select([4], [], [], 0)[0]:
                    chunk = os.read(4, 64)
                    if not chunk:
                        reason = "parent-loss"
                        control_open = False
                    else:
                        control_data += chunk
                        if b"cancel\n" in control_data:
                            reason = "cancel"
                        elif len(control_data) > 64:
                            reason = "control-invalid"
                if reason:
                    stop_at = now
            if stop_at is not None:
                if now - stop_at >= bounds["terminateGraceMs"] / 1000:
                    kill_sent = True
                send_owned(signal.SIGKILL if kill_sent else signal.SIGTERM, killed if kill_sent else terminated)
                if now - stop_at >= (bounds["terminateGraceMs"] + bounds["reapTimeoutMs"]) / 1000:
                    reason = "cleanup-deadline"
                    break
            time.sleep(0.01)
        result["outcome"] = "success" if (reason is None and result["familyClosed"] and result["reaped"] and
            result["allIdentitiesExited"] and result["consumerExit"] == {"code": 0, "signal": None} and
            not result["signalsSent"] and not result["processOverflow"] and not result["ownershipUnresolved"]) else (reason or "consumer-failure")
        encoded = json.dumps(result, separators=(",", ":")).encode()
        if len(encoded) > bounds["maxReceiptBytes"]:
            raise RuntimeError("receipt bound exceeded")
        fd = os.open(request["receiptPath"], os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600)
        try:
            with os.fdopen(fd, "wb") as destination:
                destination.write(encoded)
                destination.flush()
                os.fsync(destination.fileno())
            directory = os.open(os.path.dirname(request["receiptPath"]), os.O_RDONLY | os.O_DIRECTORY)
            try:
                os.fsync(directory)
            finally:
                os.close(directory)
        finally:
            pass
    finally:
        for _, handle in tracked.values():
            os.close(handle)
        os.close(4)


if __name__ == "__main__":
    if sys.platform != "linux":
        raise RuntimeError("Linux required")
    if sys.argv[1:] == ["--probe"]:
        probe()
    elif len(sys.argv) == 2:
        supervise(sys.argv[1])
    else:
        raise RuntimeError("request path required")
