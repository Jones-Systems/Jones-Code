"""Private installer-family supervisor. Subreaping is ownership, not containment."""
import ctypes
import errno
import json
import os
import selectors
import signal
import stat
import subprocess
import sys
import time


def identity(pid):
    with open(f"/proc/{pid}/stat", encoding="ascii") as stream:
        # comm can contain spaces and parentheses.
        fields = stream.read().rsplit(")", 1)[1].split()
    return (pid, int(fields[19]))


def children(pid):
    try:
        with open(f"/proc/{pid}/task/{pid}/children", encoding="ascii") as stream:
            return [int(value) for value in stream.read().split()]
    except FileNotFoundError:
        return []


def monitor_tree(request):
    root = request["root"]
    info = os.lstat(root)
    expected = request["rootIdentity"]
    if [info.st_dev, info.st_ino, info.st_uid] != expected:
        raise ValueError("root_replaced")
    pending = [root, request["evidenceRoot"]]
    entries = total = cache = 0
    seen = set()
    limits = request["bounds"]
    while pending:
        path = pending.pop()
        info = os.lstat(path)
        entries += 1
        if entries > limits["maxEntries"]:
            raise ValueError("entry_limit")
        active_identity = request["evidenceIdentity"] if path == request["evidenceRoot"] or path.startswith(request["evidenceRoot"] + "/") else expected
        if info.st_dev != active_identity[0] or info.st_uid != active_identity[2]:
            raise ValueError("foreign_device_or_owner")
        if stat.S_ISLNK(info.st_mode):
            continue
        if stat.S_ISDIR(info.st_mode):
            with os.scandir(path) as stream:
                for entry in stream:
                    pending.append(entry.path)
                    if len(pending) + entries > limits["maxEntries"]:
                        raise ValueError("entry_limit")
        elif stat.S_ISREG(info.st_mode):
            if info.st_size > limits["maxFileBytes"]:
                raise ValueError("file_limit")
            key = (info.st_dev, info.st_ino)
            if key not in seen:
                seen.add(key)
                total += info.st_size
            if path.startswith(root + "/cache/"):
                cache += info.st_size
            if total > limits["maxBytes"] or cache > limits["maxCacheBytes"]:
                raise ValueError("storage_limit")
        else:
            raise ValueError("special_file")


def supervise(request):
    libc = ctypes.CDLL(None, use_errno=True)
    if libc.prctl(36, 1, 0, 0, 0) != 0:  # PR_SET_CHILD_SUBREAPER, before spawn
        raise OSError(ctypes.get_errno(), "subreaper unavailable")
    selector = selectors.DefaultSelector()
    os.set_blocking(3, False)
    selector.register(3, selectors.EVENT_READ, "control")
    outcome = None
    interrupted = []
    signal.signal(signal.SIGTERM, lambda *_: interrupted.append("cancelled"))
    signal.signal(signal.SIGINT, lambda *_: interrupted.append("cancelled"))
    started = time.monotonic()
    limits = request["bounds"]
    deadline = started + limits["deadlineMs"] / 1000
    stop_at = None
    output = {"stdout": bytearray(), "stderr": bytearray()}
    output_bytes = 0
    family = {}
    reaped = []
    overflow = False
    installer_status = None
    last_scan = 0
    producer = subprocess.Popen(
        [request["executable"], *request["args"]],
        cwd=request["cwd"], env=request["env"], stdin=subprocess.DEVNULL,
        stdout=subprocess.PIPE, stderr=subprocess.PIPE, close_fds=True,
        start_new_session=True,
    )
    installer_pid = producer.pid
    for name in output:
        stream = getattr(producer, name)
        os.set_blocking(stream.fileno(), False)
        selector.register(stream, selectors.EVENT_READ, name)

    def capture(pid):
        nonlocal overflow
        try:
            key = identity(pid)
            if key in family:
                return
            if len(family) >= limits["maxProcesses"]:
                overflow = True
                return
            fd = os.pidfd_open(pid)
            if identity(pid) != key:
                os.close(fd)
                return
            family[key] = fd
        except ProcessLookupError:
            pass
        except FileNotFoundError:
            pass

    capture(installer_pid)
    installer_identity = next((list(k) for k in family if k[0] == installer_pid), None)
    if installer_identity is None:
        outcome = "identity_unknown"
    try:
        while True:
            now = time.monotonic()
            # Walk only this supervisor's child edges. Orphans are adopted here.
            pending = children(os.getpid())
            observed = set()
            while pending:
                pid = pending.pop()
                if pid in observed:
                    continue
                observed.add(pid)
                capture(pid)
                if len(observed) > limits["maxProcesses"]:
                    overflow = True
                    break
                pending.extend(children(pid))
            while True:
                try:
                    pid, status = os.waitpid(-1, os.WNOHANG)
                except ChildProcessError:
                    break
                if pid == 0:
                    break
                reaped.append(pid)
                if pid == installer_pid:
                    installer_status = os.waitstatus_to_exitcode(status)
                    producer.returncode = installer_status
            direct = children(os.getpid())
            if interrupted and outcome is None:
                outcome = interrupted[0]
            if now >= deadline and outcome is None:
                outcome = "deadline"
            if overflow and outcome is None:
                outcome = "process_limit"
            if now - last_scan >= 0.1:
                last_scan = now
                try:
                    monitor_tree(request)
                except FileNotFoundError:
                    # A producer can remove its own transient file during observation.
                    pass
                except (OSError, ValueError) as error:
                    outcome = outcome or str(error)
            if installer_status is not None and outcome is None:
                if installer_status != 0:
                    outcome = "failed"
                elif direct:
                    outcome = "survivors"
            if outcome is not None:
                if stop_at is None:
                    stop_at = now
                sig = signal.SIGTERM if now < stop_at + limits["terminateGraceMs"] / 1000 else signal.SIGKILL
                for key, fd in family.items():
                    try:
                        if identity(key[0]) == key:
                            signal.pidfd_send_signal(fd, sig)
                    except (ProcessLookupError, FileNotFoundError):
                        pass
            for key, _ in selector.select(0.02):
                try:
                    block = os.read(key.fd, 65536)
                except BlockingIOError:
                    continue
                if key.data == "control":
                    if not block:
                        selector.unregister(key.fileobj)
                        outcome = outcome or "parent_disconnected"
                    else:
                        outcome = outcome or "cancelled"
                elif not block:
                    selector.unregister(key.fileobj)
                    key.fileobj.close()
                else:
                    remaining = max(0, limits["maxOutputBytes"] - output_bytes)
                    output[key.data].extend(block[:remaining])
                    output_bytes += len(block)
                    if output_bytes > limits["maxOutputBytes"]:
                        outcome = outcome or "output_limit"
            streams_closed = all(key.data == "control" for key in selector.get_map().values())
            # ECHILD after reaping, no direct children and EOF are jointly required.
            no_children = False
            if not children(os.getpid()):
                try:
                    pid, status = os.waitpid(-1, os.WNOHANG)
                    if pid:
                        reaped.append(pid)
                        if pid == installer_pid:
                            installer_status = os.waitstatus_to_exitcode(status)
                            producer.returncode = installer_status
                except ChildProcessError:
                    no_children = True
            if no_children and streams_closed:
                break
            if stop_at is not None and now >= stop_at + (limits["terminateGraceMs"] + limits["reapTimeoutMs"]) / 1000:
                break
        closed = no_children and streams_closed and not overflow and installer_identity is not None
        return {
            "schema": "jones-historical-process/v1", "token": request["token"],
            "outcome": outcome or ("success" if installer_status == 0 else "failed"),
            "installerIdentity": installer_identity,
            "identities": [list(key) for key in family], "reapedPids": reaped,
            "installerExitCode": installer_status, "familyClosed": closed,
            "streamsClosed": streams_closed, "reaped": no_children,
            "processOverflow": overflow, "outputBytes": output_bytes,
            **{name: value.decode("utf8", errors="replace") for name, value in output.items()},
        }
    finally:
        for fd in family.values():
            os.close(fd)
        selector.close()


def cleanup(request):
    root = request["root"]
    parent_fd = os.open(request["parent"], os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
    root_fd = None
    handles = []
    try:
        def same(info, expected):
            return [info.st_dev, info.st_ino, info.st_uid] == [expected["dev"], expected["ino"], expected["uid"]]

        if not same(os.fstat(parent_fd), request["parentIdentity"]):
            raise ValueError("parent_replaced")
        name = os.path.basename(root)
        root_fd = os.open(name, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW, dir_fd=parent_fd)
        if not same(os.fstat(root_fd), request["rootIdentity"]):
            raise ValueError("root_replaced")
        marker_fd = os.open(".invocation", os.O_RDONLY | os.O_NOFOLLOW, dir_fd=root_fd)
        try:
            if os.read(marker_fd, 128).decode() != request["marker"]:
                raise ValueError("marker_changed")
        finally:
            os.close(marker_fd)
        entries = []
        links = {}
        total = 0
        pending = [(root_fd, "")]
        while pending:
            directory_fd, relative = pending.pop()
            for child_name in os.listdir(directory_fd):
                info = os.stat(child_name, dir_fd=directory_fd, follow_symlinks=False)
                if not same(info, {**request["rootIdentity"], "ino": info.st_ino}):
                    raise ValueError("foreign_device_or_owner")
                if len(entries) + 1 > request["bounds"]["maxEntries"]:
                    raise ValueError("entry_limit")
                item = (directory_fd, child_name, info)
                entries.append(item)
                if stat.S_ISDIR(info.st_mode):
                    fd = os.open(child_name, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW, dir_fd=directory_fd)
                    handles.append(fd)
                    opened = os.fstat(fd)
                    if (opened.st_dev, opened.st_ino) != (info.st_dev, info.st_ino):
                        raise ValueError("directory_replaced")
                    pending.append((fd, relative + "/" + child_name))
                elif stat.S_ISREG(info.st_mode):
                    if info.st_size > request["bounds"]["maxFileBytes"]:
                        raise ValueError("file_limit")
                    key = (info.st_dev, info.st_ino)
                    if key not in links:
                        total += info.st_size
                        links[key] = [0, info.st_nlink]
                    links[key][0] += 1
                    if total > request["bounds"]["maxBytes"]:
                        raise ValueError("storage_limit")
                elif not stat.S_ISLNK(info.st_mode):
                    raise ValueError("special_file")
        if any(count != nlink for count, nlink in links.values()):
            raise ValueError("external_hardlink")
        removed_links = {}
        for directory_fd, child_name, expected in reversed(entries):
            if not same(os.stat(name, dir_fd=parent_fd, follow_symlinks=False), request["rootIdentity"]):
                raise ValueError("root_replaced")
            current = os.stat(child_name, dir_fd=directory_fd, follow_symlinks=False)
            key = (expected.st_dev, expected.st_ino)
            if (current.st_dev, current.st_ino, current.st_uid, current.st_mode) != (expected.st_dev, expected.st_ino, expected.st_uid, expected.st_mode):
                raise ValueError("entry_replaced")
            if stat.S_ISDIR(current.st_mode):
                os.rmdir(child_name, dir_fd=directory_fd)
            else:
                if stat.S_ISREG(current.st_mode):
                    if current.st_nlink != expected.st_nlink - removed_links.get(key, 0) or current.st_size != expected.st_size or current.st_mtime_ns != expected.st_mtime_ns:
                        raise ValueError("file_changed")
                    removed_links[key] = removed_links.get(key, 0) + 1
                os.unlink(child_name, dir_fd=directory_fd)
        if not same(os.stat(name, dir_fd=parent_fd, follow_symlinks=False), request["rootIdentity"]):
            raise ValueError("root_replaced")
        os.rmdir(name, dir_fd=parent_fd)
        return {"outcome": "complete", "absent": not os.path.lexists(root)}
    finally:
        for fd in handles:
            os.close(fd)
        if root_fd is not None:
            os.close(root_fd)
        os.close(parent_fd)


def main():
    if sys.argv[1] == "--cleanup":
        with open(sys.argv[2], encoding="utf8") as stream:
            print(json.dumps(cleanup(json.load(stream))))
        return
    with open(sys.argv[1], encoding="utf8") as stream:
        request = json.load(stream)
    result = supervise(request)
    # Evidence survives loss of the JS parent; no cleanup is done by this helper.
    with open(request["receiptPath"] + ".writing", "x", encoding="utf8") as stream:
        json.dump(result, stream)
        stream.flush()
        os.fsync(stream.fileno())
    os.link(request["receiptPath"] + ".writing", request["receiptPath"])
    os.unlink(request["receiptPath"] + ".writing")
    try:
        print(json.dumps({"receiptPath": request["receiptPath"]}), flush=True)
    except BrokenPipeError:
        pass


if __name__ == "__main__":
    main()
