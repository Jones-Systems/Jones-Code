/** Kept outside the app bundle before handoff so quitting Electron cannot stop activation. */
export const jonesNativeHelperSource = String.raw`#!/usr/bin/env python3
import argparse, fcntl, hashlib, json, os, pathlib, signal, sqlite3, subprocess, sys, time, uuid, shutil, stat, plistlib, struct

PROTOCOL = 1
STARTUP_GATE_PROTOCOL = 1
SETTINGS = ('settings.json', 'desktop-settings.json', 'client-settings.json', 'saved-environments.json')
TRANSIENT_PROFILE = {'SingletonLock', 'SingletonCookie', 'SingletonSocket'}
RECOVERY_HEADROOM = 256 * 1024 * 1024

def read(path):
    with open(path, 'r', encoding='utf8') as stream:
        return json.load(stream)

def durable(path, value, exclusive=False):
    path = pathlib.Path(path)
    path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    scratch = path.with_name(path.name + '.' + uuid.uuid4().hex)
    try:
        with open(scratch, 'x', encoding='utf8') as stream:
            os.chmod(scratch, 0o600)
            json.dump(value, stream, sort_keys=True)
            stream.flush()
            os.fsync(stream.fileno())
        if exclusive:
            os.link(scratch, path)
            scratch.unlink()
        else:
            os.replace(scratch, path)
        descriptor = os.open(path.parent, os.O_RDONLY)
        try: os.fsync(descriptor)
        finally: os.close(descriptor)
    finally:
        if scratch.exists(): scratch.unlink()

def digest(path):
    result = hashlib.sha256()
    with open(path, 'rb') as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b''): result.update(block)
    return result.hexdigest()

def app_digest(root, opaque_profile=False):
    root = pathlib.Path(root).resolve()
    result = hashlib.sha256()
    entries = sorted(root.rglob('*'), key=lambda value: value.relative_to(root).as_posix().encode('utf8'))
    if len(entries) > 200000: raise RuntimeError('Native app exceeds its file-count bound.')
    total_bytes = 0
    for entry in entries:
        relative = entry.relative_to(root).as_posix()
        info = entry.lstat()
        if entry.is_symlink():
            target = os.readlink(entry)
            if not opaque_profile and not entry.resolve().is_relative_to(root): raise RuntimeError('App symlink escapes the staged app.')
            record = ['link', relative, target]
        elif entry.is_file():
            total_bytes += info.st_size
            if total_bytes > 8 * 1024 * 1024 * 1024: raise RuntimeError('Native app exceeds its payload-size bound.')
            record = ['file', relative, info.st_mode & 0o777, digest(entry)]
        elif entry.is_dir(): record = ['directory', relative, info.st_mode & 0o777]
        else: raise RuntimeError('Unexpected app payload file type.')
        result.update((json.dumps(record, separators=(',', ':'), ensure_ascii=False) + '\n').encode())
    return result.hexdigest()

def sync_tree(directory):
    directory = pathlib.Path(directory)
    directories = [directory]
    for entry in directory.rglob('*'):
        if entry.is_symlink(): continue
        if entry.is_dir(): directories.append(entry)
        elif entry.is_file():
            descriptor = os.open(entry, os.O_RDONLY | os.O_NOFOLLOW)
            try: os.fsync(descriptor)
            finally: os.close(descriptor)
    for entry in reversed(directories):
        descriptor = os.open(entry, os.O_RDONLY)
        try: os.fsync(descriptor)
        finally: os.close(descriptor)

def same_binding(left, right):
    return left == right and left.get('protocol') == PROTOCOL and left.get('owner') == 'desktop'

def process_identity(pid):
    result = subprocess.run(['/bin/ps', '-p', str(pid), '-o', 'lstart=', '-o', 'command='], capture_output=True, text=True)
    if result.returncode == 1 and not result.stdout.strip() and not result.stderr.strip(): return None
    if result.returncode != 0 or result.stderr.strip() or len(result.stdout.strip().splitlines()) != 1:
        raise RuntimeError('Process inspection failed; signal withheld.')
    return result.stdout.strip()

def process_start(pid):
    result = subprocess.run(['/bin/ps', '-p', str(pid), '-o', 'lstart=', '-o', 'stat='], capture_output=True, text=True)
    if result.returncode == 1 and not result.stdout.strip() and not result.stderr.strip(): return None
    fields = result.stdout.strip().rsplit(None, 1)
    if result.returncode != 0 or result.stderr.strip() or len(result.stdout.strip().splitlines()) != 1 or len(fields) != 2:
        raise RuntimeError('Process inspection failed; signal withheld.')
    birth, status = fields
    if status[0] not in 'DIRSTUWXYZt' or any(flag not in '<>AELNSTVWXslN+' for flag in status[1:]):
        raise RuntimeError('Process inspection failed; signal withheld.')
    return status, ' '.join(birth.split())

def alive(proof):
    identity = process_identity(proof['pid'])
    if identity is None: return False
    if identity == proof['identity']: return True
    start = process_start(proof['pid'])
    if start is None: return False
    # lstart is locale-owned text. An unreaped exit retains that complete birth
    # field while losing its command; both observations must retain the binding.
    prefix = start[1] + ' '
    if start[0].startswith('Z') and all(' '.join(value.split()).startswith(prefix) for value in (identity, proof['identity'])): return False
    raise RuntimeError('Process identity changed; signal withheld.')

def stop_exact(proofs):
    for proof in proofs:
        if alive(proof): os.kill(proof['pid'], signal.SIGTERM)
    deadline = time.monotonic() + 90
    while any(alive(proof) for proof in proofs):
        if time.monotonic() >= deadline: raise RuntimeError('Owned native writers did not stop; recovery held.')
        time.sleep(0.1)

def prove_quiescence(expected):
    database = pathlib.Path(expected['databasePath'])
    profile = pathlib.Path(expected['profile'])
    files = [database, pathlib.Path(str(database) + '-wal'), pathlib.Path(str(database) + '-shm')]
    files += [database.parent / name for name in SETTINGS]
    existing = [str(path) for path in files if path.exists()]
    if not existing: raise RuntimeError('Native database files are missing; writer inspection held.')
    # Chromium stores are not a fixed filename list. Inspect the copied profile
    # tree as well as SQLite/settings; this observes outsiders without excluding them.
    result = subprocess.run(['/usr/sbin/lsof', '-F', 'pfa', '+D', str(profile), '--'] + existing, capture_output=True, text=True, timeout=30)
    if result.returncode not in (0, 1): raise RuntimeError('Native file-writer inspection failed.')
    if result.stderr.strip() and (any(path in result.stderr for path in existing + [str(profile)]) or not all(line.startswith("lsof: WARNING: can't stat()") or line.strip() == 'Output information may be incomplete.' for line in result.stderr.splitlines() if line.strip())):
        raise RuntimeError('Native file-writer inspection was incomplete.')
    access = None
    descriptor_open = False
    for line in result.stdout.splitlines():
        if line.startswith('f'):
            if descriptor_open and access is None: raise RuntimeError('Native descriptor access is missing; recovery held.')
            access = None
            descriptor_open = True
        elif line.startswith('a'):
            access = line[1:]
            if access in ('w', 'u'): raise RuntimeError('Native state still has open writers; recovery held.')
            if access != 'r': raise RuntimeError('Native descriptor access is unknown; recovery held.')
        elif line.startswith('p'):
            if descriptor_open and access is None: raise RuntimeError('Native descriptor access is missing; recovery held.')
            descriptor_open = False
        elif line: raise RuntimeError('Native file-writer output was invalid.')
    if descriptor_open and access is None: raise RuntimeError('Native descriptor access is missing; recovery held.')
    if result.returncode == 1 and result.stdout.strip(): raise RuntimeError('Native writer inspection returned an inconsistent result.')


def preserve_restart_tunnel(expected, transaction_id):
    marker = pathlib.Path(expected['home']) / 'runtime' / 'desktop-update-restart'
    try:
        with open(marker, 'x', encoding='utf8') as stream:
            os.chmod(marker, 0o600)
            stream.write(transaction_id)
            stream.flush(); os.fsync(stream.fileno())
    except FileExistsError:
        pass  # Preserve an occupied marker; the backend consumes its own protocol marker.

def verify_app(staged, expected):
    if type(staged.get('startupGateProtocol')) is not int or staged['startupGateProtocol'] != STARTUP_GATE_PROTOCOL:
        raise RuntimeError('The staged candidate does not prove the required startup gate.')
    if expected.get('bundleIdentifier') is not None and staged.get('bundleIdentifier') != expected['bundleIdentifier']:
        raise RuntimeError('The staged app has a different bundle identity.')
    receipt = read(staged['receiptPath'])
    if receipt['app'] != staged: raise RuntimeError('Staged handle no longer binds its receipt.')
    candidate = receipt['candidate']
    if candidate['repository'] != 'Jones-Systems/Jones-Code' or candidate['source'] != staged['sourceSha'] or candidate['tree'] != staged['sourceTree']:
        raise RuntimeError('Staged app is not source-qualified.')
    artifact = receipt['artifact']
    if artifact['candidate'] != candidate or candidate['installedSource'] != expected['sourceSha']:
        raise RuntimeError('Staged artifact does not descend the active source.')

def clone_file(source, target):
    if source.is_symlink() or not source.is_file(): raise RuntimeError('Unexpected recovery file type.')
    result = subprocess.run(['/bin/cp', '-c', '-p', str(source), str(target)], capture_output=True, text=True)
    return result.returncode == 0

def clone_tree(source, target, profile=False):
    result = subprocess.run(['/bin/cp', '-cR', str(source), str(target)], capture_output=True, text=True)
    if result.returncode != 0:
        # Preserve partial copy effects inside this new task-owned tree; retained generations are never removed.
        shutil.copytree(source, target, symlinks=True, dirs_exist_ok=True,
                        ignore=lambda root, names: [name for name in names if profile and name in TRANSIENT_PROFILE])
    if profile:
        for name in TRANSIENT_PROFILE:
            copied = pathlib.Path(target) / name
            if copied.exists() or copied.is_symlink(): copied.unlink()

def tree_storage_size(root):
    root = pathlib.Path(root)
    if root.is_symlink() or not root.is_dir(): raise RuntimeError('Native storage root is not a real directory.')
    total, count = 0, 0
    for entry in root.rglob('*'):
        count += 1
        if count > 200000: raise RuntimeError('Native storage exceeds its file-count bound.')
        info = entry.lstat()
        if stat.S_ISREG(info.st_mode): total += info.st_size
        elif not (stat.S_ISDIR(info.st_mode) or stat.S_ISLNK(info.st_mode)):
            raise RuntimeError('Native storage contains an unsupported file type.')
    return total

def recovery_topology(expected, transaction):
    database = pathlib.Path(expected['databasePath'])
    profile = pathlib.Path(expected['profile'])
    roots = [database.parent.resolve(strict=True), profile.resolve(strict=True), pathlib.Path(transaction).resolve(strict=True)]
    state, profile, transaction = roots
    if state == profile or state.is_relative_to(profile) or profile.is_relative_to(state):
        raise RuntimeError('Native database and profile roots overlap; installation held.')
    if any(transaction == source or transaction.is_relative_to(source) for source in (state, profile)):
        raise RuntimeError('Native recovery would copy its own destination; installation held.')
    if len({path.stat().st_dev for path in roots}) != 1:
        raise RuntimeError('Native state and profile require same-volume recovery; installation held.')
    return roots

def native_writer_lease_paths(expected):
    home = pathlib.Path(expected['home']).resolve(strict=True)
    profile = pathlib.Path(expected['profile']).resolve(strict=True)
    profile_hash = hashlib.sha256(str(profile).encode()).hexdigest()
    return sorted(((home / 'runtime' / 'jones-native-writer.sqlite', 'home:' + str(home)),
                   (profile.parent / ('.jones-profile-writer-' + profile_hash + '.sqlite'), 'profile:' + str(profile))))

def native_lease_identity(path, scope):
    info = path.lstat()
    witness = read(str(path) + '.identity.json')
    if not stat.S_ISREG(info.st_mode) or info.st_nlink != 1 or info.st_uid != os.getuid() or info.st_mode & 0o077 or witness != {'protocol': 1, 'scope': scope, 'device': str(info.st_dev), 'inode': str(info.st_ino)}:
        raise RuntimeError('Native lease inode changed; reconciliation required.')
    return info.st_dev, info.st_ino

def initialize_native_lease(path, scope):
    if path.exists() or path.is_symlink(): return
    if pathlib.Path(str(path) + '.identity.json').exists(): raise RuntimeError('Native lease disappeared; reconciliation required.')
    path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    scratch = path.with_name(path.name + '.' + uuid.uuid4().hex + '.pending')
    descriptor = os.open(scratch, os.O_CREAT | os.O_EXCL | os.O_RDWR, 0o600)
    os.close(descriptor)
    try:
        connection = sqlite3.connect(scratch, isolation_level=None)
        try:
            connection.execute('PRAGMA journal_mode = DELETE')
            connection.execute('PRAGMA synchronous = FULL')
            connection.execute('CREATE TABLE jones_native_writer_lease (protocol INTEGER, scope TEXT)')
            connection.execute('INSERT INTO jones_native_writer_lease VALUES (1, ?)', (scope,))
        finally: connection.close()
        descriptor = os.open(scratch, os.O_RDONLY)
        try: os.fsync(descriptor)
        finally: os.close(descriptor)
        try: os.link(scratch, path)
        except FileExistsError: return
        scratch.unlink()
        sync_parent(path)
        info = path.lstat()
        durable(str(path) + '.identity.json', {'protocol': 1, 'scope': scope, 'device': str(info.st_dev), 'inode': str(info.st_ino)}, True)
    finally:
        if scratch.exists(): scratch.unlink()

def release_writer_exclusion(connections):
    while connections:
        connections[-1].close()
        connections.pop()

def acquire_writer_exclusion(expected):
    connections = []
    try:
        for path, scope in native_writer_lease_paths(expected):
            initialize_native_lease(path, scope)
            identity = native_lease_identity(path, scope)
            connection = sqlite3.connect(path.as_uri() + '?mode=rw', uri=True, timeout=0, isolation_level=None)
            connections.append(connection)
            if connection.execute('PRAGMA journal_mode').fetchone() != ('delete',):
                raise RuntimeError('Native leases require rollback journal mode.')
            connection.execute('BEGIN EXCLUSIVE')
            if connection.execute('SELECT protocol, scope FROM jones_native_writer_lease').fetchall() != [(1, scope)] or native_lease_identity(path, scope) != identity:
                raise RuntimeError('Native writer lease identity changed.')
        return connections
    except Exception:
        release_writer_exclusion(connections)
        raise

def recovery_state_bytes(expected):
    database = pathlib.Path(expected['databasePath'])
    state_bytes = tree_storage_size(expected['profile'])
    for path in [pathlib.Path(str(database) + suffix) for suffix in ('', '-wal', '-shm')] + [database.parent / name for name in SETTINGS]:
        if path.exists():
            if path.is_symlink() or not path.is_file(): raise RuntimeError('Native recovery file type is unknown.')
            state_bytes += path.stat().st_size
    return state_bytes

def storage_preflight(expected, staged, transaction):
    state, profile, transaction = recovery_topology(expected, transaction)
    state_bytes = recovery_state_bytes(expected)
    app_parent = pathlib.Path(expected['appPath']).parent.resolve(strict=True)
    candidate_bytes = tree_storage_size(staged['appPath'])
    requirements = {}
    for path, amount in ((transaction, state_bytes * 2), (app_parent, candidate_bytes)):
        device = path.stat().st_dev
        if device not in requirements: requirements[device] = [path, RECOVERY_HEADROOM]
        requirements[device][1] += amount
    # Budget physical copies even when clonefile currently works: fallback and
    # later copy-on-write allocation must not consume rollback's entire budget.
    capacity = []
    for device, (path, required) in requirements.items():
        space = os.statvfs(path)
        available = space.f_bavail * space.f_frsize
        capacity.append({'path': str(path), 'device': str(device), 'requiredBytes': required, 'availableBytes': available})
        if available < required:
            raise RuntimeError('Insufficient free space for native installation and paired recovery: needed ' + str(required) + ' bytes, available ' + str(available) + ' bytes; installation held.')
    return {'stateBytes': state_bytes, 'candidateBytes': candidate_bytes,
            'requiredBytes': sum(required for _, required in requirements.values()), 'headroomBytesPerVolume': RECOVERY_HEADROOM, 'volumes': capacity}

def allocate_recovery_file(path, size):
    descriptor = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600)
    created = os.fstat(descriptor)
    try:
        try:
            if sys.platform == 'darwin':
                # Darwin fcntl(2): F_PREALLOCATE, F_ALLOCATEALL|F_ALLOCATEPERSIST,
                # F_PEOFPOSMODE. Verify physical allocation again after close.
                allocated = fcntl.fcntl(descriptor, 42, struct.pack('=Iiqqq', 12, 3, 0, size, 0))
                if struct.unpack('=Iiqqq', allocated)[4] < size:
                    raise RuntimeError('Native recovery reserve was only partially allocated.')
                os.ftruncate(descriptor, size)
            elif hasattr(os, 'posix_fallocate'):
                os.posix_fallocate(descriptor, 0, size)
            else:
                remaining, chunk = size, bytes(1024 * 1024)
                while remaining:
                    written = os.write(descriptor, chunk[:min(remaining, len(chunk))])
                    if written <= 0: raise RuntimeError('Native reserve allocation did not progress.')
                    remaining -= written
            os.fsync(descriptor)
        finally: os.close(descriptor)
        info = path.lstat()
        if (info.st_dev, info.st_ino) != (created.st_dev, created.st_ino) or info.st_nlink != 1 or info.st_size != size or info.st_blocks * 512 < size:
            raise RuntimeError('Native recovery capacity was not physically reserved.')
        sync_parent(path)
        return {'path': str(path), 'bytes': size, 'device': str(info.st_dev), 'inode': str(info.st_ino)}
    except Exception:
        current = path.lstat()
        if (current.st_dev, current.st_ino) == (created.st_dev, created.st_ino):
            path.unlink()
            sync_parent(path)
        raise

def release_recovery_reserve(reserves, purpose=None):
    for index in reversed(range(len(reserves))):
        reserve = reserves[index]
        if purpose is not None and reserve.get('purpose') != purpose: continue
        path = pathlib.Path(reserve['path'])
        info = path.lstat()
        if not stat.S_ISREG(info.st_mode) or info.st_nlink != 1 or str(info.st_dev) != reserve['device'] or str(info.st_ino) != reserve['inode'] or info.st_size != reserve['bytes']:
            raise RuntimeError('Native reserve ownership changed; capacity release held.')
        path.unlink()
        sync_parent(path)
        reserves.pop(index)

def reserve_recovery_capacity(expected, transaction, storage):
    transaction = pathlib.Path(transaction)
    app_parent = pathlib.Path(expected['appPath']).parent
    reserves = []
    try:
        journal_bytes = min(4 * 1024 * 1024, RECOVERY_HEADROOM // 4)
        reserves.append(dict(allocate_recovery_file(transaction / 'recovery-journal-reserve.bin', journal_bytes), purpose='journal'))
        reserves.append(dict(allocate_recovery_file(transaction / 'recovery-reserve.bin', storage['stateBytes'] + RECOVERY_HEADROOM - journal_bytes), purpose='recovery'))
        if transaction.stat().st_dev != app_parent.stat().st_dev:
            reserves.append(dict(allocate_recovery_file(app_parent / ('.jones-recovery-reserve-' + transaction.name), RECOVERY_HEADROOM), purpose='recovery'))
        return reserves
    except Exception:
        release_recovery_reserve(reserves)
        raise

def recovery_capacity_before_restore(expected, pair):
    state = pathlib.Path(expected['databasePath']).parent
    app_parent = pathlib.Path(expected['appPath']).parent
    requirements = [(state, tree_storage_size(pair) + RECOVERY_HEADROOM // 2)]
    if state.stat().st_dev != app_parent.stat().st_dev:
        requirements.append((app_parent, RECOVERY_HEADROOM // 2))
    for path, required in requirements:
        space = os.statvfs(path)
        available = space.f_bavail * space.f_frsize
        if available < required:
            raise RuntimeError('Insufficient free space after recovery reserve release: needed ' + str(required) + ' bytes, available ' + str(available) + ' bytes; recovery held before state mutation.')

def pair_state(expected, directory):
    directory.mkdir(mode=0o700)
    sync_parent(directory)
    database = pathlib.Path(expected['databasePath'])
    started = time.time()
    copied = clone_file(database, directory / 'state.sqlite')
    for suffix in ('-wal', '-shm'):
        source = pathlib.Path(str(database) + suffix)
        if source.exists(): copied = clone_file(source, directory / ('state.sqlite' + suffix)) and copied
    method = 'clone'
    if not copied:
        # Writers are stopped before this snapshot; preserve the exact DB/WAL/SHM pair.
        shutil.copy2(database, directory / 'state.sqlite')
        for suffix in ('-wal', '-shm'):
            source = pathlib.Path(str(database) + suffix)
            if source.exists(): shutil.copy2(source, directory / ('state.sqlite' + suffix))
        method = 'copy'
    os.chmod(directory / 'state.sqlite', database.stat().st_mode & 0o777)
    presence = {}
    for name in SETTINGS:
        source = database.parent / name
        presence[name] = source.exists()
        if source.exists():
            if not clone_file(source, directory / name): shutil.copy2(source, directory / name)
    clone_tree(expected['profile'], directory / 'profile', profile=True)
    sync_tree(directory)
    durable(directory / 'pair.json', {'expected': expected, 'settings': presence,
            'recovery': {'method': method, 'bytes': sum((directory / ('state.sqlite' + suffix)).stat().st_size for suffix in ('', '-wal', '-shm') if (directory / ('state.sqlite' + suffix)).exists()),
                         'startedAt': started, 'completedAt': time.time()}}, True)

def restore_pair(expected, directory, advanced):
    pair = read(directory / 'pair.json')
    if pair['expected'] != expected or not (directory / 'state.sqlite').is_file() or not (directory / 'profile').is_dir():
        raise RuntimeError('Recovery pair no longer binds the previous app and state.')
    if set(pair['settings']) != set(SETTINGS): raise RuntimeError('Recovery settings presence is unknown.')
    recovery_topology(expected, advanced.parent)
    prove_quiescence(expected)
    advanced.mkdir(mode=0o700)
    sync_parent(advanced)
    database = pathlib.Path(expected['databasePath'])
    for name in (database.name, database.name + '-wal', database.name + '-shm') + SETTINGS:
        source = database.parent / name
        if source.exists():
            os.rename(source, advanced / name)
            sync_parent(source)
            sync_parent(advanced / name)
    os.rename(expected['profile'], advanced / 'profile')
    sync_parent(expected['profile'])
    sync_parent(advanced / 'profile')
    sync_tree(advanced)
    for suffix in ('', '-wal', '-shm'):
        source = directory / ('state.sqlite' + suffix)
        if source.exists() and not clone_file(source, pathlib.Path(str(database) + suffix)):
            shutil.copy2(source, pathlib.Path(str(database) + suffix))
    for name, exists in pair['settings'].items():
        if exists and not clone_file(directory / name, database.parent / name): shutil.copy2(directory / name, database.parent / name)
    clone_tree(directory / 'profile', pathlib.Path(expected['profile']), profile=True)
    sync_tree(expected['profile'])
    sync_parent(expected['profile'])
    restored_files = [pathlib.Path(str(database) + suffix) for suffix in ('', '-wal', '-shm')]
    restored_files += [database.parent / name for name, exists in pair['settings'].items() if exists]
    for restored in restored_files:
        if not restored.exists(): continue
        descriptor = os.open(restored, os.O_RDONLY | os.O_NOFOLLOW)
        try: os.fsync(descriptor)
        finally: os.close(descriptor)
    sync_parent(database)
    durable(advanced / 'retained.json', {'protocol': PROTOCOL, 'previous': expected}, True)

def sync_parent(path):
    descriptor = os.open(pathlib.Path(path).parent, os.O_RDONLY)
    try: os.fsync(descriptor)
    finally: os.close(descriptor)

def asar_metadata(archive):
    descriptor = os.open(archive, os.O_RDONLY | os.O_NOFOLLOW)
    with os.fdopen(descriptor, 'rb') as stream:
        info = os.fstat(stream.fileno())
        if not stat.S_ISREG(info.st_mode) or info.st_size > 2 * 1024 * 1024 * 1024:
            raise RuntimeError('App metadata archive is not a bounded regular file.')
        prefix = stream.read(16)
        if len(prefix) != 16: raise RuntimeError('App metadata archive is truncated.')
        size, header_size, _, json_size = struct.unpack('<IIII', prefix)
        if size != 4 or header_size < 8 or header_size > 16 * 1024 * 1024 or json_size > header_size - 8 or 8 + header_size > info.st_size:
            raise RuntimeError('App metadata archive header is invalid.')
        entry = json.loads(stream.read(json_size))['files']['package.json']
        offset, length = entry.get('offset'), entry.get('size')
        if entry.get('unpacked') is True or 'link' in entry or not isinstance(offset, str) or not offset.isascii() or not offset.isdigit() or type(length) is not int or length < 1 or length > 1024 * 1024:
            raise RuntimeError('App package metadata is not a bounded packed file.')
        position = 8 + header_size + int(offset)
        if position + length > info.st_size: raise RuntimeError('App package metadata exceeds its archive.')
        stream.seek(position)
        return json.loads(stream.read(length))

def bundle_identity(active):
    app = pathlib.Path(active['appPath'])
    if app.is_symlink() or not app.is_dir() or any(part in ('AppTranslocation', 'Volumes', 'tmp', 'jones-updates') for part in app.parts):
        raise RuntimeError('Move Jones Code to a stable writable Applications folder before updating.')
    with open(app / 'Contents/Info.plist', 'rb') as stream: info = plistlib.load(stream)
    executable = app / 'Contents/MacOS' / info['CFBundleExecutable']
    if info['CFBundleShortVersionString'] != active['version'] or str(executable) != active['executablePath'] or (active.get('bundleIdentifier') is not None and info['CFBundleIdentifier'] != active['bundleIdentifier']):
        raise RuntimeError('Native bundle identity changed; launch held.')
    metadata = asar_metadata(app / 'Contents/Resources/app.asar')
    source = metadata.get('jonesSource', {})
    if metadata.get('version') != active['version'] or source.get('repository') != 'Jones-Systems/Jones-Code' or source.get('sha') != active['sourceSha'] or source.get('tree') != active['sourceTree']:
        raise RuntimeError('Native app source or version changed; launch held.')
    if not os.access(app.parent, os.W_OK): raise RuntimeError('The stable app folder is not writable; native authorization is required.')
    return info['CFBundleIdentifier']

def prepare_bundle(expected, staged, transaction_id):
    stable = pathlib.Path(expected['appPath'])
    incoming = stable.with_name(stable.name + '.jones-incoming-' + transaction_id)
    previous = stable.with_name(stable.name + '.jones-previous-' + expected['generation'])
    if incoming.exists() or incoming.is_symlink() or previous.exists() or previous.is_symlink():
        raise RuntimeError('An occupied bundle generation requires reconciliation; it was preserved.')
    clone_tree(staged['appPath'], incoming)
    if app_digest(incoming) != staged['appDigest']: raise RuntimeError('Staged app integrity changed during bundle preparation.')
    sync_tree(incoming)
    sync_parent(incoming)
    return incoming, previous

def swap_bundle(expected, incoming, previous):
    stable = pathlib.Path(expected['appPath'])
    os.rename(stable, previous)
    sync_parent(stable)
    try:
        os.rename(incoming, stable)
        sync_parent(stable)
    except Exception:
        if not stable.exists() and previous.exists():
            os.rename(previous, stable)
            sync_parent(stable)
        raise

def restore_bundle(expected, incoming, previous):
    stable = pathlib.Path(expected['appPath'])
    if not previous.exists(): return
    if incoming.exists(): raise RuntimeError('Occupied trial bundle path; rollback held.')
    os.rename(stable, incoming)
    sync_parent(stable)
    os.rename(previous, stable)
    sync_parent(stable)

def launch(active, descriptor=None):
    env = os.environ.copy()
    # Provider stores and credentials stay on this host. No copied auth is consulted.
    env.pop('ELECTRON_RUN_AS_NODE', None)
    env['T3CODE_HOME'] = active['home']
    env['T3CODE_DESKTOP_USER_DATA_DIR'] = active['profile']
    env['T3CODE_JONES_ACTIVE_MANIFEST'] = str(manifest_path)
    env['T3CODE_JONES_ACTIVE_GENERATION'] = active['generation']
    if descriptor:
        env['T3CODE_JONES_TRIAL_DESCRIPTOR'] = str(descriptor)
        env['T3CODE_PORT'] = str(read(descriptor)['listener'].rsplit(':', 1)[1])
    else: env.pop('T3CODE_JONES_TRIAL_DESCRIPTOR', None)
    child = subprocess.Popen([active['executablePath']], env=env, stdin=subprocess.DEVNULL,
                             stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, start_new_session=True)
    identity = process_identity(child.pid)
    if identity is None: raise RuntimeError('Native app exited before its identity was captured; OS consent may be required.')
    return {'pid': child.pid, 'identity': identity}

class SelectionRefused(Exception):
    def __init__(self, reason): self.reason = reason

def bounded_native_json(path, limit=1024 * 1024):
    descriptor = os.open(path, os.O_RDONLY | os.O_NOFOLLOW)
    with os.fdopen(descriptor, 'rb') as stream:
        info = os.fstat(stream.fileno())
        if not stat.S_ISREG(info.st_mode) or info.st_uid != os.getuid() or info.st_size > limit:
            raise SelectionRefused('unknown-state')
        raw = stream.read(limit + 1)
        if len(raw) > limit: raise SelectionRefused('unknown-state')
    value = json.loads(raw)
    if not isinstance(value, dict): raise SelectionRefused('unknown-state')
    return value, raw, info

def selection_binding(active, handle, selection_path, expected_digest):
    if len(handle) != 64 or any(char not in '0123456789abcdef' for char in handle):
        raise SelectionRefused('selection-mismatch')
    path = pathlib.Path(selection_path)
    staging = manifest_path.parent / 'jones-updates' / 'staging'
    if not path.exists(): raise SelectionRefused('selection-mismatch')
    if path.parent != staging or path.is_symlink() or path.resolve(strict=True).parent != staging.resolve(strict=True):
        raise SelectionRefused('selection-mismatch')
    if not path.name.startswith(active['sourceSha'] + '-') or not path.name.endswith('.json'):
        raise SelectionRefused('selection-mismatch')
    selection, raw, info = bounded_native_json(path, 131072)
    if hashlib.sha256(raw).hexdigest() != expected_digest:
        raise SelectionRefused('selection-mismatch')
    wanted = {'schema': 1, 'source': 'jones-actions', 'home': active['home'], 'profile': active['profile'],
              'currentVersion': active['version'], 'installedSource': active['sourceSha'], 'active': active}
    if type(selection.get('schema')) is not int or any(selection.get(key) != value for key, value in wanted.items()):
        raise SelectionRefused('selection-mismatch')
    if not isinstance(selection.get('app'), dict) or selection['app'].get('handle') != handle:
        raise SelectionRefused('selection-mismatch')
    return path, selection, raw, info

def inspect_selection_transactions(active, transaction_id=None, claiming=False, staged_handle=None):
    root = manifest_path.parent / 'jones-updates' / 'transactions'
    if not root.exists():
        if claiming: raise SelectionRefused('unknown-state')
        return
    if root.is_symlink() or not root.is_dir(): raise SelectionRefused('unknown-state')
    count = 0
    for transaction in root.iterdir():
        count += 1
        if count > 1000 or transaction.is_symlink() or not transaction.is_dir() or len(transaction.name) != 64 or any(char not in '0123456789abcdef' for char in transaction.name):
            raise SelectionRefused('unknown-state')
        intent, journal, preparation = [transaction / name for name in ('intent.json', 'journal.json', 'prepare-intent.json')]
        prepared = any((transaction / name).exists() for name in ('prepare-dispatched.json', 'continuation.json'))
        if transaction.name == transaction_id:
            if intent.exists() or journal.exists() or ((preparation.exists() or prepared) and not claiming):
                raise SelectionRefused('activation-pending')
            if claiming and not preparation.is_file(): raise SelectionRefused('unknown-state')
            continue
        if journal.exists():
            value, _, _ = bounded_native_json(journal)
            prior, _, _ = bounded_native_json(intent)
            if value.get('intent') != prior or prior.get('transactionId') != transaction.name or prior.get('protocol') != PROTOCOL or prior.get('expected', {}).get('home') != active['home']:
                raise SelectionRefused('unknown-state')
            if value.get('phase') not in ('resumed', 'rolled-back'):
                raise SelectionRefused('activation-pending')
            if staged_handle is not None and prior.get('staged', {}).get('handle') == staged_handle:
                if value['phase'] != 'rolled-back' or prior.get('expected') != active:
                    raise SelectionRefused('activation-pending')
        elif intent.exists() or preparation.exists() or prepared:
            raise SelectionRefused('activation-pending')

def preparation_claim(active, staged_handle, transaction_id, selection_path, selection_digest):
    return dict(protocol=PROTOCOL, preparationClaimProtocol=2, stagedHandle=staged_handle, transactionId=transaction_id,
                selectionPath=str(selection_path), selectionSha256=selection_digest,
                **{key: active[key] for key in ('home', 'databasePath', 'profile', 'environmentId')})

def attempt_selection_binding(active, staged_handle, transaction_id, selection_path, selection_digest):
    key = hashlib.sha256((str(selection_path) + '\n' + selection_digest).encode()).hexdigest()
    path = manifest_path.parent / 'jones-updates' / 'attempt-selections' / (key + '.json')
    value, _, _ = bounded_native_json(path)
    expected = {'protocol': PROTOCOL, 'transactionId': transaction_id, 'stagedHandle': staged_handle,
                'selectionPath': str(selection_path), 'selectionSha256': selection_digest, 'expected': active}
    if type(value.get('protocol')) is not int or value != expected: raise SelectionRefused('selection-mismatch')

def selection_command(operation, active, handle, selection_path, expected_digest, request_path=None, transaction_id=None):
    result = {'protocol': PROTOCOL, 'operation': operation, 'handle': handle}
    if operation in ('claim-preparation', 'claim-activation'): result['transactionId'] = transaction_id
    try:
        if operation in ('claim-preparation', 'claim-activation') and (not isinstance(transaction_id, str) or len(transaction_id) != 64 or any(char not in '0123456789abcdef' for char in transaction_id)):
            raise SelectionRefused('unknown-state')
        # A mismatch may clear the caller's provisional hold only when there is
        # no durable preparation/activation already owned by this transaction.
        inspect_selection_transactions(active, transaction_id, operation == 'claim-activation', handle)
        path, selection, raw, info = selection_binding(active, handle, selection_path, expected_digest)
        if read(manifest_path) != active: raise SelectionRefused('selection-mismatch')
        transaction = manifest_path.parent / 'jones-updates' / 'transactions' / transaction_id if transaction_id is not None else None
        if operation in ('claim-preparation', 'claim-activation'):
            attempt_selection_binding(active, handle, transaction_id, path, expected_digest)
            claim = preparation_claim(active, handle, transaction_id, path, expected_digest)
        if operation == 'claim-activation':
            request = pathlib.Path(request_path)
            if request != transaction / 'activation-request.json' or request.resolve(strict=True) != request:
                raise SelectionRefused('selection-mismatch')
            intent, _, _ = bounded_native_json(request)
            if type(intent.get('protocol')) is not int or intent['protocol'] != PROTOCOL or intent.get('transactionId') != transaction_id or intent.get('expected') != active or intent.get('staged') != selection['app'] or intent.get('continuationReceipt') != str(transaction / 'continuation.json'):
                raise SelectionRefused('selection-mismatch')
            verify_app(intent['staged'], active)
            continuation, _, _ = bounded_native_json(transaction / 'continuation.json')
            preparation, _, _ = bounded_native_json(transaction / 'prepare-intent.json')
            dispatched, _, _ = bounded_native_json(transaction / 'prepare-dispatched.json')
            if any(type(value.get('protocol')) is not int or type(value.get('preparationClaimProtocol')) is not int for value in (continuation, preparation, dispatched)) or continuation != dict(claim, prepared=True) or preparation != claim or dispatched != claim:
                raise SelectionRefused('unknown-state')
            proofs = intent.get('processes')
            if not isinstance(proofs, list) or not proofs or len(proofs) > 256 or any(not isinstance(proof, dict) or type(proof.get('pid')) is not int or proof['pid'] <= 0 or not isinstance(proof.get('identity'), str) or not proof['identity'].strip() for proof in proofs):
                raise SelectionRefused('unknown-state')
            if not isinstance(intent.get('listener'), str) or not intent['listener'].strip():
                raise SelectionRefused('unknown-state')
        elif operation == 'claim-preparation': verify_app(selection['app'], active)
        elif operation != 'discard-staged': raise SelectionRefused('unknown-state')
        _, current_raw, current = bounded_native_json(path, 131072)
        if current_raw != raw or (current.st_dev, current.st_ino, current.st_size) != (info.st_dev, info.st_ino, len(raw)):
            raise SelectionRefused('selection-mismatch')
    except SelectionRefused as refused:
        return dict(result, status='refused', reason=refused.reason)
    except Exception:
        return dict(result, status='refused', reason='unknown-state')
    try:
        if operation == 'discard-staged':
            path.unlink()
            sync_parent(path)
            return dict(result, status='discarded')
        if operation == 'claim-preparation':
            transaction.parent.mkdir(exist_ok=True, mode=0o700)
            sync_parent(transaction.parent)
            transaction.mkdir(exist_ok=True, mode=0o700)
            sync_parent(transaction)
            durable(transaction / 'prepare-intent.json', claim, True)
            return dict(result, status='preparation-claimed')
        intent_path = transaction / 'intent.json'
        durable(intent_path, intent, True)
        return dict(result, status='claimed', intentPath=str(intent_path))
    except Exception:
        # A failed final fsync may follow unlink/link. Preserve the caller's hold.
        return dict(result, status='uncertain')

def activate(intent_file):
    intent = read(intent_file)
    expected, staged = intent['expected'], intent['staged']
    tx = pathlib.Path(intent_file).parent
    transaction_id = intent.get('transactionId')
    if not isinstance(transaction_id, str) or len(transaction_id) != 64 or any(char not in '0123456789abcdef' for char in transaction_id) or tx != manifest_path.parent / 'jones-updates' / 'transactions' / transaction_id:
        raise RuntimeError('Native activation transaction path changed.')
    journal_path = tx / 'journal.json'
    if journal_path.exists():
        journal = read(journal_path)
        if journal.get('intent') != intent: raise RuntimeError('Occupied journal belongs to a different install intent.')
        if journal['phase'] in ('resumed', 'rolled-back', 'blocked'): return
        journal.update(phase='blocked', message='Interrupted native activation requires reconciliation; automatic replay held.')
        durable(journal_path, journal)
        return
    if intent['protocol'] != PROTOCOL or not same_binding(read(manifest_path), expected):
        raise RuntimeError('Active binding changed; install refused.')
    verify_app(staged, expected)
    continuation = read(intent['continuationReceipt'])
    for field in ('transactionId', 'home', 'databasePath', 'profile', 'environmentId'):
        wanted = intent['transactionId'] if field == 'transactionId' else expected[field]
        if continuation.get(field) != wanted: raise RuntimeError('Continuation receipt has a stale native binding.')
    if type(continuation.get('protocol')) is not int or continuation['protocol'] != PROTOCOL or type(continuation.get('preparationClaimProtocol')) is not int or continuation['preparationClaimProtocol'] != 2 or continuation.get('stagedHandle') != staged['handle'] or continuation.get('prepared') is not True:
        raise RuntimeError('Native continuations were not prepared.')
    claim = preparation_claim(expected, staged['handle'], intent['transactionId'], continuation['selectionPath'], continuation['selectionSha256'])
    if continuation != dict(claim, prepared=True) or read(tx / 'prepare-intent.json') != claim or read(tx / 'prepare-dispatched.json') != claim:
        raise RuntimeError('Native preparation dispatch did not prove its selection claim.')
    _, selection, _, _ = selection_binding(expected, staged['handle'], claim['selectionPath'], claim['selectionSha256'])
    attempt_selection_binding(expected, staged['handle'], intent['transactionId'], claim['selectionPath'], claim['selectionSha256'])
    if selection['app'] != staged: raise RuntimeError('Native preparation selection changed.')
    journal = {'intent': intent, 'phase': 'intent'}
    def record(phase, **fields):
        journal.update(fields, phase=phase)
        durable(journal_path, journal)
    record('intent')
    candidate = None
    backend_process = None
    incoming = previous = None
    startup_unknown = False
    exclusive_writers = []
    recovery_reserves = []
    try:
        storage = storage_preflight(expected, staged, tx)
        record('preparing', storage=storage)
        recovery_reserves = reserve_recovery_capacity(expected, tx, storage)
        record('preparing', recoveryReserves=list(recovery_reserves))
        incoming, previous = prepare_bundle(expected, staged, intent['transactionId'])
        record('preparing', incomingBundle=str(incoming), previousBundle=str(previous))
        preserve_restart_tunnel(expected, intent['transactionId'])
        stop_exact(intent['processes'])
        prove_quiescence(expected)
        exclusive_writers = acquire_writer_exclusion(expected)
        record('quiescent')
        if recovery_state_bytes(expected) > storage['stateBytes'] + RECOVERY_HEADROOM // 2:
            raise RuntimeError('Native state grew beyond reserved recovery capacity before shutdown.')
        pair_state(expected, tx / 'previous-pair')
        record('paired', pairedState=str(tx / 'previous-pair'), recovery=read(tx / 'previous-pair' / 'pair.json')['recovery'])
        record('swap-intent')
        swap_bundle(expected, incoming, previous)
        record('swapped')
        descriptor = {'protocol': PROTOCOL, 'startupGateProtocol': STARTUP_GATE_PROTOCOL, 'transactionId': intent['transactionId'], 'stagedHandle': staged['handle'],
                      'home': expected['home'], 'databasePath': expected['databasePath'],
                      'profile': expected['profile'], 'environmentId': expected['environmentId'],
                      'sourceSha': staged['sourceSha'], 'sourceTree': staged['sourceTree'],
                      'version': staged['version'], 'listener': intent['listener'],
                      'trialReceiptPath': str(tx / 'trial-receipt.json'), 'commitGrantPath': str(tx / 'commit-grant.json')}
        durable(tx / 'trial-descriptor.json', descriptor, True)
        next_active = dict(expected, generation=intent['transactionId'], transactionId=intent['transactionId'],
                           **{key: staged[key] for key in ('version', 'sourceSha', 'sourceTree', 'appDigest')},
                           executablePath=str(pathlib.Path(expected['appPath']) / pathlib.Path(staged['executablePath']).relative_to(staged['appPath'])))
        record('trial')
        release_writer_exclusion(exclusive_writers)
        candidate = launch(next_active, tx / 'trial-descriptor.json')
        record('trial', candidateWriter=candidate)
        deadline = time.monotonic() + 90
        while not (tx / 'trial-receipt.json').exists():
            if not alive(candidate): raise RuntimeError('Candidate app stopped; native OS consent may be required.')
            if time.monotonic() >= deadline: raise RuntimeError('Candidate did not prove native readiness.')
            time.sleep(0.1)
        startup_unknown = True
        receipt = read(tx / 'trial-receipt.json')
        if not isinstance(receipt, dict) or type(receipt.get('startupGateProtocol')) is not int or receipt['startupGateProtocol'] != STARTUP_GATE_PROTOCOL:
            raise RuntimeError('Candidate startup dispatch has unknown effects; the required gate was not proved.')
        startup_unknown = False
        for key in ('protocol', 'startupGateProtocol', 'transactionId', 'stagedHandle', 'home', 'databasePath', 'profile', 'environmentId', 'version', 'sourceSha', 'sourceTree', 'listener'):
            if receipt.get(key) != descriptor[key]: raise RuntimeError('Candidate trial identity does not match install intent.')
        if type(receipt.get('protocol')) is not int or type(receipt.get('startupGateProtocol')) is not int:
            raise RuntimeError('Candidate did not prove the required startup gate protocol.')
        if receipt.get('resumeHeld') is not True: raise RuntimeError('Candidate did not hold native continuations.')
        proof = receipt.get('backendProcess')
        if not isinstance(proof, dict) or set(proof) != {'pid', 'identity'} or type(proof.get('pid')) is not int or proof['pid'] <= 0 or not isinstance(proof.get('identity'), str) or not proof['identity'].strip():
            raise RuntimeError('Candidate did not prove the exact backend process.')
        backend_process = proof
        record('validated', trialReceipt=receipt)
        if not alive(backend_process): raise RuntimeError('Candidate backend stopped before commit.')
        if not same_binding(read(manifest_path), expected): raise RuntimeError('Active manifest changed before commit.')
        durable(manifest_path, next_active)
        record('committed')
        # Resume intent is durable before dispatch; a second helper cannot replay it.
        record('resume-intent')
        durable(tx / 'commit-grant.json', dict(generation=intent['transactionId'],
                    **{key: descriptor[key] for key in ('protocol', 'startupGateProtocol', 'transactionId', 'stagedHandle', 'home', 'databasePath', 'profile', 'environmentId', 'version', 'sourceSha', 'sourceTree', 'listener')}), True)
        record('resumed')
    except Exception as failure:
        try:
            # Error evidence has its own small reserve. Candidate writers cannot
            # consume the bulk rollback capacity while shutdown is unresolved.
            release_recovery_reserve(recovery_reserves, 'journal')
        except Exception:
            record('blocked', message='Recovery reserve ownership or release is uncertain; state retained.')
            return
        active = read(manifest_path)
        if startup_unknown or (tx / 'resume-dispatched.json').exists() or journal['phase'] in ('committed', 'resume-intent', 'resumed') or active != expected or (journal['phase'] == 'trial' and candidate is None):
            record('blocked', message='Unknown activation, startup, or resume effect; binaries and advanced state retained.')
            return
        if not (tx / 'previous-pair' / 'pair.json').exists():
            if journal['phase'] == 'quiescent':
                # Snapshotting only reads native state. With no trial launched,
                # its unchanged prior app/state pair remains compatible.
                try:
                    prove_quiescence(expected)
                    release_recovery_reserve(recovery_reserves)
                    record('rolled-back', message='Snapshot preparation failed before any candidate launch; prior state was unchanged.')
                    release_writer_exclusion(exclusive_writers)
                    launch(expected)
                    return
                except Exception:
                    pass
            record('blocked', message='Update preparation refused: ' + str(failure) + '; previous binary and state retained.')
            return
        try:
            record('rollback-intent')
            proofs = [candidate] if candidate else []
            # Only a validated startup-gate receipt supplies the backend proof, independently of app ownership.
            if backend_process is not None: proofs.append(backend_process)
            preserve_restart_tunnel(expected, intent['transactionId'])
            stop_exact(proofs)
            prove_quiescence(expected)
            if not exclusive_writers: exclusive_writers = acquire_writer_exclusion(expected)
            release_recovery_reserve(recovery_reserves)
            recovery_capacity_before_restore(expected, tx / 'previous-pair')
            restore_pair(expected, tx / 'previous-pair', tx / 'advanced-state')
            if incoming is not None and previous is not None: restore_bundle(expected, incoming, previous)
            record('rolled-back', message='Update rolled back: ' + str(failure))
            release_writer_exclusion(exclusive_writers)
            launch(expected)
        except Exception as recovery_failure:
            record('blocked', message='Paired recovery could not be proved; retained state requires reconciliation: ' + str(recovery_failure))
    finally:
        release_writer_exclusion(exclusive_writers)

parser = argparse.ArgumentParser()
parser.add_argument('--manifest', required=True)
action = parser.add_mutually_exclusive_group(required=True)
action.add_argument('--activate')
action.add_argument('--launch', action='store_true')
action.add_argument('--cli', action='store_true')
action.add_argument('--discard-staged')
action.add_argument('--claim-activation')
action.add_argument('--claim-preparation')
parser.add_argument('--transaction')
parser.add_argument('--staged-handle')
parser.add_argument('--selection')
parser.add_argument('--selection-sha256')
parser.add_argument('arguments', nargs=argparse.REMAINDER)
args = parser.parse_args()
if args.arguments and args.arguments[0] == '--': args.arguments = args.arguments[1:]
manifest_path = pathlib.Path(args.manifest).resolve(strict=True)
if sys.platform != 'darwin': raise SystemExit('The Jones native helper requires macOS.')
lock_path = manifest_path.with_name('jones-activation.lock')
try:
    descriptor = os.open(lock_path, os.O_CREAT | os.O_EXCL | os.O_RDWR | os.O_NOFOLLOW, 0o600)
    os.write(descriptor, b'jones-activation-lock-v1\n')
    os.fsync(descriptor)
except FileExistsError:
    descriptor = os.open(lock_path, os.O_RDWR | os.O_NOFOLLOW)
    info = os.fstat(descriptor)
    if not stat.S_ISREG(info.st_mode) or info.st_uid != os.getuid() or os.read(descriptor, 256) != b'jones-activation-lock-v1\n':
        os.close(descriptor)
        raise SystemExit('An occupied native activation lock is unknown and was preserved.')
with os.fdopen(descriptor, 'r+', encoding='utf8') as lock:
    operation = 'discard-staged' if args.discard_staged else 'claim-activation' if args.claim_activation else 'claim-preparation' if args.claim_preparation else None
    handle = args.discard_staged or args.claim_preparation or args.staged_handle
    transaction_id = args.transaction if args.claim_preparation else pathlib.Path(args.claim_activation).parent.name if args.claim_activation else None
    try: fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except BlockingIOError:
        if operation:
            result = {'protocol': PROTOCOL, 'operation': operation, 'handle': handle, 'status': 'refused', 'reason': 'busy'}
            if transaction_id is not None: result['transactionId'] = transaction_id
            print(json.dumps(result))
            raise SystemExit(0)
        raise
    active = read(manifest_path)
    if active.get('protocol') != PROTOCOL or active.get('owner') != 'desktop':
        raise SystemExit('Native launcher bootstrap is required.')
    bundle_identity(active)
    if operation:
        print(json.dumps(selection_command(operation, active, handle, args.selection, args.selection_sha256, args.claim_activation, transaction_id)))
    elif args.activate: activate(args.activate)
    elif args.launch:
        transactions = manifest_path.parent / 'jones-updates' / 'transactions'
        if transactions.exists():
            for transaction in transactions.iterdir():
                journal = transaction / 'journal.json'
                if journal.exists():
                    if read(journal).get('phase') not in ('resumed', 'rolled-back'):
                        raise SystemExit('Native activation requires reconciliation before launch.')
                elif (transaction / 'intent.json').exists():
                    raise SystemExit('Unknown activation effects require reconciliation before launch.')
        prove_quiescence(active)
        launch(active)
    elif args.cli:
        if args.arguments not in (['--version'], ['--help']):
            raise SystemExit('This home is owned by desktop; the CLI refuses a parallel server writer.')
        env = dict(os.environ, ELECTRON_RUN_AS_NODE='1', T3CODE_HOME=active['home'])
        child = subprocess.run([active['executablePath'], str(pathlib.Path(active['appPath']) / 'Contents/Resources/app.asar/apps/server/dist/bin.mjs')] + args.arguments, env=env)
        raise SystemExit(child.returncode)
    else: raise SystemExit('Choose --activate, --launch, or --cli.')
`;
