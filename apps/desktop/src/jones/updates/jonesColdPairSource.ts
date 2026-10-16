import { jonesPairedStateSource } from "./jonesPairedStateSource.ts";

const indentColdStorage = (source: string) =>
  source
    .split("\n")
    .map((line) => "    " + line)
    .join("\n");

/** Private storage mechanics; the installed entry has no cold-owner constructor. */
export const jonesColdPairSource = String.raw`
def _cold_pair_storage():
    import shutil, subprocess, time
    SETTINGS = ('settings.json', 'desktop-settings.json', 'client-settings.json', 'saved-environments.json')
    TRANSIENT_PROFILE = {'SingletonLock', 'SingletonCookie', 'SingletonSocket'}
${indentColdStorage(jonesPairedStateSource.syncTree)}
${indentColdStorage(jonesPairedStateSource.copyAndTopology)}
${indentColdStorage(jonesPairedStateSource.recoveryAndPair)}
${indentColdStorage(jonesPairedStateSource.syncParent)}
    return sync_tree, sync_parent, clone_file, clone_tree, tree_storage_size, recovery_topology, allocate_recovery_file

_COLD_PAIR_SETTINGS = ('settings.json', 'desktop-settings.json', 'client-settings.json', 'saved-environments.json')
_COLD_PAIR_TRANSIENT = {'SingletonLock', 'SingletonCookie', 'SingletonSocket'}
_COLD_PAIR_HEADROOM = 256 * 1024 * 1024
_COLD_PAIR_BOUNDS = {'entries': 200000, 'depth': 64, 'bytes': 32 * 1024 * 1024 * 1024}

def _cold_pair_runtime():
    # A boot observation, fixed lock or serialized receipt is not continuous custody.
    raise Refused('Continuous cold-owner construction is not qualified.')

def _cold_pair_identity(owner, path, kind):
    result = native_identity(owner.native, owner.bundle['identityNativeSha256'], [(kind, path)])
    return stable_native(result['objects'][0])

def _cold_pair_clone(source, parent, name):
    if sys.platform != 'darwin': return False
    import ctypes
    # Darwin sys/clonefile.h exposes the source-FD/destination-dir-FD operation.
    clone = getattr(ctypes.CDLL(None, use_errno=True), 'fclonefileat', None)
    if clone is None: return False
    clone.argtypes = (ctypes.c_int, ctypes.c_int, ctypes.c_char_p, ctypes.c_uint32)
    clone.restype = ctypes.c_int
    if clone(source, parent, os.fsencode(name), 0) == 0: return True
    # A failed native call with a destination is an unknown effect, not copy fallback.
    try: os.stat(name, dir_fd=parent, follow_symlinks=False)
    except FileNotFoundError: return False
    raise Refused('Clone destination exists after a failed native operation.')

def _cold_pair_operation(method):
    def guarded(self, *args):
        try:
            self.check()
            return method(self, *args)
        except Exception as failure:
            # Do not retry an uncertain namespace, sync or close effect in this scope.
            if self.failure is None: self.failure = failure
            raise
    return guarded

class _ColdPairTransaction:
    def __init__(self, maintenance):
        self.runtime = _cold_pair_runtime()
        self.owner, self.failure = maintenance, None
        self.check()
        self.root = maintenance.directory / 'state'
        self.database = absolute(maintenance.original['databasePath'])
        self.profile = absolute(maintenance.original['profile'])
        self.members = [('database', self.database), ('wal', pathlib.Path(str(self.database) + '-wal')),
                        ('shm', pathlib.Path(str(self.database) + '-shm'))]
        self.members += [(name, self.database.parent / name) for name in _COLD_PAIR_SETTINGS]
        self.members += [('profile', self.profile)]
        require(len({str(path) for _, path in self.members}) == len(self.members))
        self.journal = self.journal_raw = self.intent = None
        self.live_root_identities = {}
        fence = maintenance.journal.get('stateTransaction')
        if fence is not None:
            raw = read_owned(self.root / 'intent.json')
            require(sha(raw) == fence['intentSha256'])
            self.intent = decode(raw)
            require(self.intent == self.make_intent(fence['transactionId'], self.intent['roots']))
            self.journal_raw = read_owned(self.root / 'journal.json')
            self.journal = decode(self.journal_raw)
            require(set(self.journal) == {'schema', 'intentSha256', 'phase', 'reserve', 'reserveState', 'pairSha256', 'planSha256', 'evidenceSha256', 'restoreSha256', 'done', 'pending'})
            require(self.journal['schema'] == 1 and self.journal['intentSha256'] == fence['intentSha256'])
            require(self.journal['phase'] in ('snapshot', 'paired', 'preparation-intent', 'prepared', 'restore', 'restored'))
            require(self.journal['reserveState'] in ('held', 'release-intent', 'released'))
            phase = self.journal['phase']
            require(self.journal['pairSha256'] is None if phase == 'snapshot' else hex_value(self.journal['pairSha256'], 64))
            for key in ('planSha256', 'evidenceSha256'):
                require(self.journal[key] is None or hex_value(self.journal[key], 64))
            if phase in ('snapshot', 'paired'): require(self.journal['planSha256'] is None and self.journal['evidenceSha256'] is None)
            if phase in ('preparation-intent', 'prepared'): require(hex_value(self.journal['planSha256'], 64))
            if phase == 'prepared': require(hex_value(self.journal['evidenceSha256'], 64))
            if self.journal['evidenceSha256'] is not None: require(hex_value(self.journal['planSha256'], 64))
            if phase in ('restore', 'restored'): require(hex_value(self.journal['restoreSha256'], 64))
            else: require(self.journal['restoreSha256'] is None and self.journal['done'] == [] and self.journal['pending'] is None)
            self.check_roots()

    def check(self):
        if self.failure is not None: raise Refused('Cold-pair scope has an unresolved prior effect.') from self.failure
        self.runtime.check(self.owner)
        self.owner.current_lock()
        require(self.owner.journal['phase'] == 'armed')
        require(read_owned(self.owner.directory / 'journal.json') == self.owner.journal_raw)

    def root_identity(self, path):
        observed = Observation()
        with observed.directory(path) as fd:
            info = os.fstat(fd)
            result = {'device': str(info.st_dev), 'inode': str(info.st_ino),
                      'native': _cold_pair_identity(self.owner, path, 'directory')}
            observed.current()
            return result

    def check_roots(self):
        self.check()
        paths = (self.database.parent, self.profile.parent, self.owner.directory)
        require(isinstance(self.intent['roots'], list) and len(self.intent['roots']) == len(paths))
        roots = [self.bind_root_identity(path, expected) for path, expected in zip(paths, self.intent['roots'])]
        require(len({item['device'] for item in roots}) == 1)
        state, transaction = self.database.parent, self.owner.directory
        for left, right in ((state, self.profile), (state, transaction), (self.profile, transaction)):
            require(left != right and not left.is_relative_to(right) and not right.is_relative_to(left))
        # The profile itself may be a mount point even when its parent is on the state volume.
        with Observation().directory(self.profile.parent) as parent:
            try: info = os.stat(self.profile.name, dir_fd=parent, follow_symlinks=False)
            except FileNotFoundError:
                require(self.journal is not None and self.journal['phase'] == 'restore')
            else:
                require(stat.S_ISDIR(info.st_mode) and str(info.st_dev) == roots[0]['device'])
                actual = self.root_identity(self.profile)
                require(actual['device'] == roots[0]['device'] and actual['inode'] == str(info.st_ino))

    def bind_root_identity(self, path, expected):
        require(isinstance(expected, dict) and set(expected) == {'device', 'inode', 'native'})
        require(all(isinstance(expected[key], str) and re.fullmatch('[0-9]+', expected[key]) for key in ('device', 'inode')))
        actual = self.root_identity(path)
        # Persisted stat numbers may change after remount; native identity binds epochs.
        # Once observed here, all three fields remain exact for this live transaction.
        require(actual['native'] == expected['native'])
        if path in self.live_root_identities: require(actual == self.live_root_identities[path])
        else: self.live_root_identities[path] = actual
        return actual

    def make_intent(self, transaction_id, roots):
        return {'schema': 1, 'transactionId': transaction_id,
                'maintenanceGeneration': self.owner.plan['maintenanceGeneration'],
                'maintenancePlanSha256': self.owner.journal['planSha256'],
                'originalManifestSha256': self.owner.plan['originalManifestSha256'],
                'installedGeneration': self.owner.original['generation'], 'installedSource': self.owner.original['sourceSha'],
                'candidateBinding': self.owner.plan['candidateBinding'], 'roots': roots,
                'members': [{'name': name, 'path': str(path)} for name, path in self.members], 'bounds': _COLD_PAIR_BOUNDS}

    def write(self, name, value, previous=None):
        self.check()
        raw = encoded(value) + b'\n'
        durable_file(self.root / name, raw, 0o600, previous)
        return raw

    def save(self, **updates):
        value = dict(self.journal, **updates)
        raw = self.write('journal.json', value, self.journal_raw)
        self.journal, self.journal_raw = value, raw

    def mkdir(self, path):
        self.check()
        with Observation().directory(path.parent) as fd:
            os.mkdir(path.name, 0o700, dir_fd=fd)
            sync_fd(fd)
        with Observation().directory(path) as fd: sync_fd(fd)

    def names(self, fd):
        # A fresh open description makes bounded readback independent of directory offsets.
        listing_fd = os.open('.', os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW, dir_fd=fd)
        try:
            require(identity(os.fstat(listing_fd), True) == identity(os.fstat(fd), True))
            names = []
            with os.scandir(listing_fd) as listing:
                for entry in listing:
                    require(len(names) < _COLD_PAIR_BOUNDS['entries'])
                    names.append(entry.name)
            return sorted(names)
        finally: os.close(listing_fd)

    def stat_at(self, parent, name):
        try: return os.stat(name, dir_fd=parent, follow_symlinks=False)
        except FileNotFoundError: return None

    def walk(self, parent, name, expected, relative, depth, profile, device, entries, budget, target=None):
        require(depth <= _COLD_PAIR_BOUNDS['depth'] and len(entries) < _COLD_PAIR_BOUNDS['entries'])
        before = self.stat_at(parent, name)
        require(before is not None and identity(before) == identity(expected))
        require(before.st_uid == os.getuid() and before.st_dev == device)
        mode = stat.S_IMODE(before.st_mode)
        if stat.S_ISLNK(before.st_mode):
            require(profile)
            link = os.readlink(name, dir_fd=parent)
            entries.append([relative, 'link', mode, link])
            if target is not None: os.symlink(link, target[1], dir_fd=target[0])
        else:
            require(not before.st_mode & 0o022)
            directory = stat.S_ISDIR(before.st_mode)
            require(directory or (stat.S_ISREG(before.st_mode) and before.st_nlink == 1))
            flags = os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK
            if directory: flags |= os.O_DIRECTORY
            fd = os.open(name, flags, dir_fd=parent)
            destination = None
            try:
                require(identity(os.fstat(fd)) == identity(before))
                if target is not None:
                    self.check()
                    if directory:
                        os.mkdir(target[1], 0o700, dir_fd=target[0])
                        destination = os.open(target[1], flags, dir_fd=target[0])
                    else:
                        cloned = _cold_pair_clone(fd, target[0], target[1])
                        destination = os.open(target[1], flags if cloned else os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600, dir_fd=target[0])
                    created = os.fstat(destination)
                    require(created.st_dev == device and created.st_uid == os.getuid())
                    require(stat.S_ISDIR(created.st_mode) if directory else stat.S_ISREG(created.st_mode) and created.st_nlink == 1)
                if directory:
                    names = self.names(fd)
                    entries.append([relative, 'directory', mode])
                    for child in names:
                        if profile and child in _COLD_PAIR_TRANSIENT: continue
                        child_info = self.stat_at(fd, child)
                        require(child_info is not None)
                        self.walk(fd, child, child_info, relative + '/' + child, depth + 1, profile,
                                  device, entries, budget, None if destination is None else (destination, child))
                    require(self.names(fd) == names)
                else:
                    budget[0] += before.st_size
                    require(budget[0] <= _COLD_PAIR_BOUNDS['bytes'])
                    digest, read_bytes = hashlib.sha256(), 0
                    while True:
                        chunk = os.read(fd, min(1024 * 1024, before.st_size - read_bytes + 1))
                        if not chunk: break
                        read_bytes += len(chunk)
                        require(read_bytes <= before.st_size)
                        digest.update(chunk)
                        if destination is not None and not cloned:
                            remaining = memoryview(chunk)
                            while remaining:
                                written = os.write(destination, remaining)
                                require(written > 0)
                                remaining = remaining[written:]
                    require(read_bytes == before.st_size)
                    entries.append([relative, 'file', mode, before.st_size, digest.hexdigest()])
                require(identity(os.fstat(fd)) == identity(before))
                if destination is not None:
                    os.fchmod(destination, mode)
                    os.utime(destination, ns=(before.st_atime_ns, before.st_mtime_ns))
                    sync_fd(destination)
                    current = self.stat_at(target[0], target[1])
                    require(current is not None and (current.st_dev, current.st_ino) == (created.st_dev, created.st_ino))
            finally:
                try:
                    if destination is not None: os.close(destination)
                finally: os.close(fd)
        current = self.stat_at(parent, name)
        require(current is not None and identity(current) == identity(before))

    def observe(self, path, profile=False):
        self.check()
        observed = Observation()
        with observed.directory(path.parent) as parent:
            info = self.stat_at(parent, path.name)
            if info is None:
                observed.current()
                return None
            require(stat.S_ISDIR(info.st_mode) if profile else stat.S_ISREG(info.st_mode))
            native = _cold_pair_identity(self.owner, path, 'directory' if profile else 'file')
            entries, budget = [], [0]
            self.walk(parent, path.name, info, '.', 0, profile, os.fstat(parent).st_dev, entries, budget)
            require(_cold_pair_identity(self.owner, path, 'directory' if profile else 'file') == native)
            observed.current()
            return {'identity': native, 'content': entries, 'bytes': budget[0]}

    def observe_members(self, directory=None):
        return {name: self.observe(path if directory is None else directory / name, name == 'profile') for name, path in self.members}

    def sync_directory(self, path):
        observed = Observation()
        with observed.directory(path) as fd:
            sync_fd(fd)
            observed.current()

    def copy(self, source, target, profile):
        self.check()
        before = self.observe(source, profile)
        require(before is not None)
        source_observed, target_observed = Observation(), Observation()
        with source_observed.directory(source.parent) as src, target_observed.directory(target.parent) as dst:
            require(self.stat_at(dst, target.name) is None)
            info = self.stat_at(src, source.name)
            require(info is not None and _cold_pair_identity(self.owner, source, 'directory' if profile else 'file') == before['identity'])
            entries, budget = [], [0]
            self.walk(src, source.name, info, '.', 0, profile, os.fstat(src).st_dev, entries, budget, (dst, target.name))
            sync_fd(dst)
            source_observed.current()
            target_observed.current()
            require(entries == before['content'])
        after = self.observe(target, profile)
        require(self.observe(source, profile) == before and after['content'] == before['content'])
        return after

    def allocate_reserve(self, size):
        # The shared allocator accepts a pathname; cold recovery must retain the parent FD.
        observed = Observation()
        with observed.directory(self.root) as parent:
            fd = os.open('reserve.bin', os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600, dir_fd=parent)
            created = os.fstat(fd)
            try:
                if sys.platform == 'darwin':
                    allocated = fcntl.fcntl(fd, 42, struct.pack('=Iiqqq', 12, 3, 0, size, 0))
                    require(struct.unpack('=Iiqqq', allocated)[4] >= size)
                    os.ftruncate(fd, size)
                elif hasattr(os, 'posix_fallocate'): os.posix_fallocate(fd, 0, size)
                else:
                    remaining, chunk = size, bytes(1024 * 1024)
                    while remaining:
                        written = os.write(fd, chunk[:min(remaining, len(chunk))])
                        require(written > 0)
                        remaining -= written
                sync_fd(fd)
                info = os.fstat(fd)
                require(info.st_size == size and info.st_blocks * 512 >= size and info.st_nlink == 1)
                current = self.stat_at(parent, 'reserve.bin')
                require(current is not None and (current.st_dev, current.st_ino) == (created.st_dev, created.st_ino))
                sync_fd(parent)
                observed.current()
            finally: os.close(fd)

    def present(self, path):
        observed = Observation()
        with observed.directory(path.parent) as parent:
            result = self.stat_at(parent, path.name) is not None
            observed.current()
            return result

    def observe_reserve(self, path, size):
        observed = Observation()
        with observed.directory(path.parent) as parent:
            info = self.stat_at(parent, path.name)
            require(info is not None and stat.S_ISREG(info.st_mode) and info.st_uid == os.getuid() and info.st_nlink == 1 and info.st_size == size and info.st_blocks * 512 >= size)
            result = {'identity': _cold_pair_identity(self.owner, path, 'file'), 'bytes': size}
            current = self.stat_at(parent, path.name)
            require(current is not None and identity(current) == identity(info))
            observed.current()
            return result

    def reserve(self):
        value = self.journal['reserve']
        require(isinstance(value, dict) and set(value) == {'identity', 'bytes'})
        path = self.root / 'reserve.bin'
        if self.journal['reserveState'] == 'held':
            require(self.observe_reserve(path, value['bytes']) == value)
        elif self.journal['reserveState'] == 'released':
            require(not self.present(path))
        else:
            if self.present(path): require(self.observe_reserve(path, value['bytes']) == value)
        return value

    def release_reserve(self):
        self.reserve()
        if self.journal['reserveState'] == 'released': return
        if self.journal['reserveState'] == 'held': self.save(reserveState='release-intent')
        path = self.root / 'reserve.bin'
        with Observation().directory(self.root) as fd:
            self.check()
            if self.present(path):
                require(self.observe_reserve(path, self.journal['reserve']['bytes']) == self.journal['reserve'])
                os.unlink(path.name, dir_fd=fd)
            sync_fd(fd)
        self.save(reserveState='released')

    def pair(self):
        raw = read_owned(self.root / 'pair-descriptor.json')
        require(sha(raw) == self.journal['pairSha256'])
        value = decode(raw)
        require(set(value) == {'schema', 'intentSha256', 'source', 'copied', 'reserve'})
        require(value['schema'] == 1 and value['intentSha256'] == self.journal['intentSha256'])
        require(value['reserve'] == self.reserve())
        require(set(value['source']) == set(value['copied']) == {name for name, _ in self.members})
        require(self.observe_members(self.root / 'previous-pair') == value['copied'])
        for name in value['source']:
            old, copied = value['source'][name], value['copied'][name]
            require((old is None and copied is None) or (old is not None and copied is not None and old['content'] == copied['content']))
        return value

    @_cold_pair_operation
    def snapshot(self):
        if self.journal is not None:
            require(self.journal['phase'] in ('paired', 'preparation-intent', 'prepared', 'restore', 'restored'))
            return self.pair()
        roots = [self.root_identity(path) for path in (self.database.parent, self.profile.parent, self.owner.directory)]
        self.intent = self.make_intent(uuid.uuid4().hex + uuid.uuid4().hex, roots)
        self.check_roots()
        source = self.observe_members()
        require(source['database'] is not None and source['profile'] is not None)
        amount = sum(item['bytes'] for item in source.values() if item is not None)
        require(amount <= _COLD_PAIR_BOUNDS['bytes'])
        with Observation().directory(self.owner.directory) as fd: space = os.fstatvfs(fd)
        require(space.f_bavail * space.f_frsize >= amount * 3 + _COLD_PAIR_HEADROOM * 2)
        intent_raw = encoded(self.intent) + b'\n'
        self.owner.save(stateTransaction={'schema': 1, 'transactionId': self.intent['transactionId'], 'intentSha256': sha(intent_raw)})
        self.mkdir(self.root)
        self.write('intent.json', self.intent)
        self.journal = {'schema': 1, 'intentSha256': sha(intent_raw), 'phase': 'snapshot', 'reserve': None, 'reserveState': 'held',
                        'pairSha256': None, 'planSha256': None, 'evidenceSha256': None, 'restoreSha256': None, 'done': [], 'pending': None}
        self.journal_raw = self.write('journal.json', self.journal)
        reserve_size = amount + _COLD_PAIR_HEADROOM
        self.allocate_reserve(reserve_size)
        self.save(reserve=self.observe_reserve(self.root / 'reserve.bin', reserve_size))
        previous = self.root / 'previous-pair'
        self.mkdir(previous)
        for name, path in self.members:
            if source[name] is not None: self.copy(path, previous / name, name == 'profile')
        self.sync_directory(previous)
        self.check_roots()
        require(self.observe_members() == source)
        descriptor = {'schema': 1, 'intentSha256': sha(intent_raw), 'source': source,
                      'copied': self.observe_members(previous), 'reserve': self.reserve()}
        raw = self.write('pair-descriptor.json', descriptor)
        self.save(phase='paired', pairSha256=sha(raw))
        return self.pair()

    @_cold_pair_operation
    def preparation_intent(self, plan_sha256):
        require(hex_value(plan_sha256, 64) and self.journal['phase'] == 'paired')
        self.pair()
        self.save(phase='preparation-intent', planSha256=plan_sha256)

    @_cold_pair_operation
    def preparation_observed(self, evidence_sha256):
        require(hex_value(evidence_sha256, 64) and self.journal['phase'] == 'preparation-intent')
        self.save(phase='prepared', evidenceSha256=evidence_sha256)

    @_cold_pair_operation
    def begin_restore(self):
        require(self.journal['phase'] in ('paired', 'preparation-intent', 'prepared'))
        pair = self.pair()
        self.check_roots()
        self.release_reserve()
        live = self.observe_members()
        restore_path = self.root / 'restore-intent.json'
        if self.present(restore_path):
            raw = read_owned(restore_path)
            value = decode(raw)
            self.validate_restore_intent(value)
            require(value['live'] == live and self.observe_members(self.root / 'restore-staging') == value['staged'])
            for name in value['staged']:
                staged, previous = value['staged'][name], pair['copied'][name]
                require((staged is None and previous is None) or (staged is not None and previous is not None and staged['content'] == previous['content']))
            with Observation().directory(self.root / 'advanced-state') as fd: require(not self.names(fd))
            self.save(phase='restore', restoreSha256=sha(raw), done=[], pending=None)
            return
        with Observation().directory(self.root) as fd: space = os.fstatvfs(fd)
        require(space.f_bavail * space.f_frsize >= sum(item['bytes'] for item in pair['copied'].values() if item is not None) + _COLD_PAIR_HEADROOM // 2)
        stage = self.root / 'restore-staging'
        self.mkdir(stage)
        for name, _ in self.members:
            if pair['copied'][name] is not None: self.copy(self.root / 'previous-pair' / name, stage / name, name == 'profile')
        self.sync_directory(stage)
        require(self.observe_members() == live)
        advanced = self.root / 'advanced-state'
        self.mkdir(advanced)
        advanced_identity = self.root_identity(advanced)
        require(advanced not in self.live_root_identities)
        self.live_root_identities[advanced] = advanced_identity
        value = {'schema': 1, 'intentSha256': self.journal['intentSha256'], 'pairSha256': self.journal['pairSha256'],
                 'live': live, 'staged': self.observe_members(stage), 'advancedIdentity': advanced_identity}
        raw = self.write('restore-intent.json', value)
        self.save(phase='restore', restoreSha256=sha(raw), done=[], pending=None)

    def validate_restore_intent(self, value):
        require(set(value) == {'schema', 'intentSha256', 'pairSha256', 'live', 'staged', 'advancedIdentity'})
        require(value['schema'] == 1 and value['intentSha256'] == self.journal['intentSha256'] and value['pairSha256'] == self.journal['pairSha256'])
        require(set(value['live']) == set(value['staged']) == {name for name, _ in self.members})
        self.bind_root_identity(self.root / 'advanced-state', value['advancedIdentity'])

    def move(self, source, target, expected, profile):
        self.check()
        left, right = self.observe(source, profile), self.observe(target, profile)
        if left == expected and right is None:
            with Observation().directory(source.parent) as src, Observation().directory(target.parent) as dst:
                self.check()
                require(self.observe(source, profile) == expected and self.observe(target, profile) is None)
                os.rename(source.name, target.name, src_dir_fd=src, dst_dir_fd=dst)
                sync_fd(src)
                sync_fd(dst)
        else:
            require(left is None and right == expected)
            with Observation().directory(source.parent) as src, Observation().directory(target.parent) as dst:
                sync_fd(src)
                sync_fd(dst)
        require(self.observe(source, profile) is None and self.observe(target, profile) == expected)

    @_cold_pair_operation
    def reconcile_restore(self):
        require(self.journal['phase'] in ('restore', 'restored'))
        self.check_roots()
        self.pair()
        raw = read_owned(self.root / 'restore-intent.json')
        require(sha(raw) == self.journal['restoreSha256'])
        value = decode(raw)
        self.validate_restore_intent(value)
        steps = [(name, operation) for name, _ in self.members for operation in ('retain', 'install')]
        done, pending = self.journal['done'], self.journal['pending']
        require(isinstance(done, list) and len(done) <= len(steps) and done == [list(step) for step in steps[:len(done)]])
        require(pending is None or (len(done) < len(steps) and pending == list(steps[len(done)])))
        if self.journal['phase'] == 'restored': require(len(done) == len(steps) and pending is None)
        paths = dict(self.members)
        # Validate completed effects before continuing a later member.
        for name, path in self.members:
            retained, installed = [name, 'retain'] in done, [name, 'install'] in done
            advanced, staged = self.root / 'advanced-state' / name, self.root / 'restore-staging' / name
            if retained: require(self.observe(advanced, name == 'profile') == value['live'][name])
            if installed:
                require(self.observe(path, name == 'profile') == value['staged'][name] and self.observe(staged, name == 'profile') is None)
            else:
                current, replacement = self.observe(path, name == 'profile'), self.observe(staged, name == 'profile')
                if pending == [name, 'install']:
                    require((current is None and replacement == value['staged'][name]) or
                            (replacement is None and current == value['staged'][name]))
                else:
                    require(replacement == value['staged'][name])
                    if retained: require(current is None)
                if not retained:
                    saved = self.observe(advanced, name == 'profile')
                    if pending == [name, 'retain']:
                        require((current == value['live'][name] and saved is None) or
                                (current is None and saved == value['live'][name]))
                    else: require(current == value['live'][name] and saved is None)
        for name, operation in steps[len(done):]:
            step = [name, operation]
            self.save(pending=step)
            path, profile = paths[name], name == 'profile'
            expected = value['live' if operation == 'retain' else 'staged'][name]
            source, target = (path, self.root / 'advanced-state' / name) if operation == 'retain' else (self.root / 'restore-staging' / name, path)
            if expected is None:
                require(self.observe(source, profile) is None and self.observe(target, profile) is None)
            else: self.move(source, target, expected, profile)
            self.save(done=self.journal['done'] + [step], pending=None)
        self.save(phase='restored')
        return {'status': 'pair-restored-gates-retained', 'stateAdmissionAllowed': False, 'activationAllowed': False}
`;
