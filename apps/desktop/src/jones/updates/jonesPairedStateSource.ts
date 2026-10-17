/** Python storage definitions share the caller's existing globals and perform no work until called. */
export const jonesPairedStateSource = Object.freeze({
  syncTree: String.raw`def sync_tree(directory):
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
        finally: os.close(descriptor)`,
  copyAndTopology: String.raw`def clone_file(source, target):
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
    return roots`,
  recoveryAndPair: String.raw`def recovery_state_bytes(expected):
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

def restore_pair(expected, directory, advanced, candidate):
    pair = read(directory / 'pair.json')
    if pair['expected'] != expected or not (directory / 'state.sqlite').is_file() or not (directory / 'profile').is_dir():
        raise RuntimeError('Recovery pair no longer binds the previous app and state.')
    if set(pair['settings']) != set(SETTINGS): raise RuntimeError('Recovery settings presence is unknown.')
    recovery_topology(expected, advanced.parent)
    prove_quiescence(expected)
    advanced.mkdir(mode=0o700)
    sync_parent(advanced)
    database = pathlib.Path(expected['databasePath'])
    guard = native_authority_transition(expected, candidate, 'fence')
    durable(advanced.with_name(advanced.name + '-authority-guard.json'), {'protocol': 1, 'guard': guard}, True)
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
    native_authority_transition(expected, candidate, 'advance', guard)`,
  syncParent: String.raw`def sync_parent(path):
    descriptor = os.open(pathlib.Path(path).parent, os.O_RDONLY)
    try: os.fsync(descriptor)
    finally: os.close(descriptor)`,
} as const);
