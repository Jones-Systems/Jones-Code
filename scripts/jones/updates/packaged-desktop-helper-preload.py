"""External RH-F2 test preload; never imported by product source."""
import errno
import hashlib
import json
import os
import pathlib
import re
import stat
import sys


def _install():
    plan_name = os.environ.get("JONES_PACKAGED_DESKTOP_FAULT_PLAN")
    if not plan_name or not re.fullmatch(r"activate-v1-[0-9a-f]{64}\.py", pathlib.Path(sys.argv[0]).name):
        return
    try:
        plan_path = pathlib.Path(plan_name)
        root = plan_path.parent.parent
        root_info = root.lstat()
        assert root.is_absolute() and root.resolve(strict=True) == root
        assert stat.S_ISDIR(root_info.st_mode) and root_info.st_uid == os.getuid()
        assert root_info.st_mode & 0o077 == 0

        def confined(path):
            path = pathlib.Path(path)
            path.relative_to(root)
            assert path.is_absolute() and path.resolve(strict=True) == path
            for parent in [path.parent, *path.parents]:
                if parent == root:
                    break
                info = parent.lstat()
                assert stat.S_ISDIR(info.st_mode) and info.st_uid == os.getuid()
                assert info.st_mode & 0o077 == 0
            return path

        def read_file(path, limit):
            path = confined(path)
            fd = os.open(path, os.O_RDONLY | os.O_NOFOLLOW)
            with os.fdopen(fd, "rb") as stream:
                info = os.fstat(stream.fileno())
                assert stat.S_ISREG(info.st_mode) and info.st_uid == os.getuid()
                assert info.st_nlink == 1 and info.st_mode & 0o077 == 0
                assert info.st_size <= limit
                raw = stream.read(limit + 1)
                assert len(raw) == info.st_size
                return raw

        plan = json.loads(read_file(plan_path, 16384))
        assert set(plan) == {"protocol", "fixtureRoot", "rootDevice", "rootInode", "helper", "helperSha256",
                             "manifest", "manifestSha256", "intent", "intentSha256", "transactionId", "point", "observeRelaunch"}
        assert type(plan["protocol"]) is int and plan["protocol"] == 1
        assert type(plan["observeRelaunch"]) is bool
        assert plan["fixtureRoot"] == str(root)
        assert (plan["rootDevice"], plan["rootInode"]) == (str(root_info.st_dev), str(root_info.st_ino))
        helper = confined(plan["helper"])
        assert sys.argv == [str(helper), "--manifest", plan["manifest"], "--activate", plan["intent"]]
        assert helper.name == "activate-v1-" + plan["helperSha256"] + ".py"
        assert hashlib.sha256(read_file(helper, 2 * 1024 * 1024)).hexdigest() == plan["helperSha256"]
        manifest_raw = read_file(plan["manifest"], 65536)
        intent_raw = read_file(plan["intent"], 65536)
        assert hashlib.sha256(manifest_raw).hexdigest() == plan["manifestSha256"]
        assert hashlib.sha256(intent_raw).hexdigest() == plan["intentSha256"]
        manifest, intent = json.loads(manifest_raw), json.loads(intent_raw)
        assert re.fullmatch("[0-9a-f]{64}", plan["transactionId"])
        assert manifest["protocol"] == 1 and manifest["owner"] == "desktop"
        assert intent["protocol"] == 1 and intent["transactionId"] == plan["transactionId"]
        assert intent["expected"] == manifest
        home = confined(manifest["home"])
        assert os.environ["T3CODE_HOME"] == str(home)
        expected_manifest = home / "runtime" / "jones-active-install.json"
        transaction = home / "runtime" / "jones-updates" / "transactions" / plan["transactionId"]
        assert str(expected_manifest) == plan["manifest"]
        assert str(transaction / "intent.json") == plan["intent"]
        assert helper.parent == home / "runtime" / "jones-updates"
        point = plan["point"]
        points = {"storage": ("storage_preflight", "intent", "storage"),
                  "reserve": ("reserve_recovery_capacity", "preparing", "reserve"),
                  "exit-wait": ("kernel_process_identity", "preparing", "stop-processes"),
                  "before-pair": ("pair_state", "quiescent", "pair")}
        target, phase, step = points[point]
        receipt_path = plan_path.with_name("fault-receipt.json")
        assert not os.path.lexists(receipt_path)
        assert not os.path.lexists(plan_path.with_name("before-relaunch.json"))
        plan_sha = hashlib.sha256(read_file(plan_path, 16384)).hexdigest()


        def snapshot_state():
            counts = [0, 0]

            def visit(path, depth=0):
                counts[0] += 1
                assert depth <= 16 and counts[0] <= 2048
                path = pathlib.Path(path)
                path.relative_to(root)
                assert path != root and path.is_absolute()
                try:
                    info = path.lstat()
                except FileNotFoundError:
                    assert path.parent.resolve(strict=True) == path.parent
                    return None
                assert not stat.S_ISLNK(info.st_mode) and info.st_uid == os.getuid()
                assert path.resolve(strict=True) == path
                identity = {"device": str(info.st_dev), "inode": str(info.st_ino), "mode": info.st_mode}
                if stat.S_ISDIR(info.st_mode):
                    children = {child.name: visit(child, depth + 1) for child in sorted(path.iterdir())}
                    after = path.lstat()
                    assert (after.st_dev, after.st_ino, after.st_mode, after.st_mtime_ns, after.st_ctime_ns) == (
                        info.st_dev, info.st_ino, info.st_mode, info.st_mtime_ns, info.st_ctime_ns)
                    return dict(identity, children=children)
                assert stat.S_ISREG(info.st_mode) and info.st_nlink == 1
                counts[1] += info.st_size
                assert counts[1] <= 64 * 1024 * 1024
                fd = os.open(path, os.O_RDONLY | os.O_NOFOLLOW)
                with os.fdopen(fd, "rb") as stream:
                    before = os.fstat(stream.fileno())
                    fields = ("st_dev", "st_ino", "st_mode", "st_size", "st_mtime_ns", "st_ctime_ns")
                    assert all(getattr(before, key) == getattr(info, key) for key in fields)
                    digest = hashlib.sha256()
                    for chunk in iter(lambda: stream.read(65536), b""):
                        digest.update(chunk)
                    after = os.fstat(stream.fileno())
                    current = path.lstat()
                    assert all(getattr(after, key) == getattr(before, key) == getattr(current, key) for key in fields)
                return dict(identity, bytes=info.st_size, sha256=digest.hexdigest())

            database = pathlib.Path(manifest["databasePath"])
            profile = pathlib.Path(manifest["profile"])
            database.relative_to(home)
            profile.relative_to(root)
            paths = [pathlib.Path(str(database) + suffix) for suffix in ("", "-wal", "-shm")]
            paths += [database.parent / name for name in ("settings.json", "desktop-settings.json", "client-settings.json", "saved-environments.json", "environment-id")]
            paths += [profile]
            return {str(path): visit(path) for path in paths}

        fault_record = None

        def observe_relaunch(frame, event, arg):
            if (not plan["observeRelaunch"] or fault_record is None or event != "call"
                    or frame.f_code.co_filename != str(helper) or frame.f_code.co_name != "launch"):
                return
            parent = frame.f_back
            activation = parent.f_back if parent is not None else None
            if (parent is None or parent.f_code.co_filename != str(helper) or parent.f_code.co_name != "relaunch_previous"
                    or activation is None or activation.f_code.co_filename != str(helper) or activation.f_code.co_name != "activate"):
                return
            sys.setprofile(None)
            try:
                local = activation.f_locals
                assert local.get("transaction_id") == plan["transactionId"]
                assert frame.f_locals.get("active") == manifest and frame.f_locals.get("descriptor") is None
                journal_raw = read_file(transaction / "journal.json", 1024 * 1024)
                journal = json.loads(journal_raw)
                assert journal == local["journal"] and journal["intent"] == intent and journal["phase"] == "rolled-back"
                evidence = journal["preEffect"]
                assert (evidence["phase"], evidence["step"]) == (phase, step)
                assert evidence["reason"] == "preparation-failed" and evidence["relaunch"] == "skipped"
                assert evidence["diagnostics"]["errno"] == errno.EIO and "relaunch" not in journal
                assert hashlib.sha256(read_file(plan["manifest"], 65536)).hexdigest() == plan["manifestSha256"]
                assert hashlib.sha256(read_file(plan["intent"], 65536)).hexdigest() == plan["intentSha256"]
                assert hashlib.sha256(read_file(helper, 2 * 1024 * 1024)).hexdigest() == plan["helperSha256"]
                fault_raw = read_file(receipt_path, 16384)
                assert json.loads(fault_raw) == fault_record
                receipt = dict(fault_record, kind="packaged-desktop-before-relaunch",
                               faultReceiptSha256=hashlib.sha256(fault_raw).hexdigest(),
                               journalSha256=hashlib.sha256(journal_raw).hexdigest(), state=snapshot_state())
                raw = json.dumps(receipt, sort_keys=True).encode()
                assert len(raw) <= 1024 * 1024
                fd = os.open(plan_path.with_name("before-relaunch.json"), os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600)
                with os.fdopen(fd, "wb") as stream:
                    stream.write(raw)
                    stream.flush()
                    os.fsync(stream.fileno())
                fd = os.open(plan_path.parent, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
                try:
                    os.fsync(fd)
                finally:
                    os.close(fd)
            except BaseException:
                os.write(2, b"RH-F2 pre-relaunch observation failed; owned evidence retained.\n")
                os._exit(125)

        def trace(frame, event, arg):
            nonlocal fault_record
            if event != "call" or frame.f_code.co_filename != str(helper):
                return None
            if frame.f_code.co_name != target:
                return None
            ancestors = []
            current = frame.f_back
            activation = None
            while current:
                if current.f_code.co_filename == str(helper):
                    ancestors.append(current.f_code.co_name)
                    if current.f_code.co_name == "activate":
                        activation = current
                        break
                current = current.f_back
            if activation is None:
                return None
            if point == "exit-wait" and not all(name in ancestors for name in ("alive", "<genexpr>", "stop_exact")):
                return None
            local = activation.f_locals
            if (local.get("transaction_id") != plan["transactionId"] or local.get("preparation_step") != step
                    or local.get("journal", {}).get("phase") != phase):
                return None
            journal_raw = read_file(transaction / "journal.json", 1024 * 1024)
            journal = json.loads(journal_raw)
            assert journal["phase"] == phase and journal["intent"] == intent
            assert hashlib.sha256(read_file(plan["manifest"], 65536)).hexdigest() == plan["manifestSha256"]
            assert hashlib.sha256(read_file(plan["intent"], 65536)).hexdigest() == plan["intentSha256"]
            assert hashlib.sha256(read_file(helper, 2 * 1024 * 1024)).hexdigest() == plan["helperSha256"]
            receipt = {"protocol": 1, "kind": "packaged-desktop-helper-fault", "pid": os.getpid(),
                       "point": point, "function": target, "phase": phase, "step": step,
                       "transactionId": plan["transactionId"], "planSha256": plan_sha,
                       "helperSha256": plan["helperSha256"], "manifestSha256": plan["manifestSha256"],
                       "intentSha256": plan["intentSha256"],
                       "journalSha256": hashlib.sha256(journal_raw).hexdigest()}
            fd = os.open(receipt_path, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600)
            with os.fdopen(fd, "w") as stream:
                json.dump(receipt, stream, sort_keys=True)
                stream.flush()
                os.fsync(stream.fileno())
            fd = os.open(receipt_path.parent, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
            try:
                os.fsync(fd)
            finally:
                os.close(fd)
            fault_record = receipt
            sys.settrace(None)
            raise OSError(errno.EIO, "RH-F2 owned fixture preparation fault: " + point)

        if plan["observeRelaunch"]:
            sys.setprofile(observe_relaunch)
        sys.settrace(trace)
    except BaseException:
        # sitecustomize exceptions alone may be swallowed; an armed invalid plan must stop the helper.
        os.write(2, b"RH-F2 helper preload refused its fixture binding.\n")
        os._exit(125)


_install()
