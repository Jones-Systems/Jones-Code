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
        self.storage = _cold_pair_storage()
        self.root = maintenance.directory / 'state'
        self.database = absolute(maintenance.original['databasePath'])
        self.profile = absolute(maintenance.original['profile'])
        self.members = [('database', self.database), ('wal', pathlib.Path(str(self.database) + '-wal')),
                        ('shm', pathlib.Path(str(self.database) + '-shm'))]
        self.members += [(name, self.database.parent / name) for name in _COLD_PAIR_SETTINGS]
        self.members += [('profile', self.profile)]
        require(len({str(path) for _, path in self.members}) == len(self.members))
        self.journal = self.journal_raw = self.intent = None
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
        with Observation().directory(path) as fd:
            return {'device': str(os.fstat(fd).st_dev), 'inode': str(os.fstat(fd).st_ino),
                    'native': _cold_pair_identity(self.owner, path, 'directory')}

    def check_roots(self):
        self.check()
        require(self.intent['roots'] == [self.root_identity(path) for path in (self.database.parent, self.profile.parent, self.owner.directory)])
        state, profile_parent, transaction = (self.database.parent, self.profile.parent, self.owner.directory)
        require(len({path.stat().st_dev for path in (state, profile_parent, transaction)}) == 1)
        for left, right in ((state, self.profile), (state, transaction), (self.profile, transaction)):
            require(left != right and not left.is_relative_to(right) and not right.is_relative_to(left))

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

    def observe(self, path, profile=False):
        self.check()
        try: info = path.lstat()
        except FileNotFoundError: return None
        require(info.st_uid == os.getuid() and not info.st_mode & 0o022)
        directory = stat.S_ISDIR(info.st_mode)
        require(directory if profile else stat.S_ISREG(info.st_mode) and info.st_nlink == 1)
        native = _cold_pair_identity(self.owner, path, 'directory' if directory else 'file')
        entries, total = [], 0
        def visit(current, relative, depth):
            nonlocal total
            require(depth <= _COLD_PAIR_BOUNDS['depth'] and len(entries) < _COLD_PAIR_BOUNDS['entries'])
            before = current.lstat()
            require(before.st_uid == os.getuid())
            mode = stat.S_IMODE(before.st_mode)
            if stat.S_ISLNK(before.st_mode):
                require(profile)
                entries.append([relative, 'link', mode, os.readlink(current)])
            else:
                require(not before.st_mode & 0o022)
                if stat.S_ISDIR(before.st_mode):
                    names = []
                    with os.scandir(current) as listing:
                        for item in listing:
                            require(len(names) + len(entries) < _COLD_PAIR_BOUNDS['entries'])
                            names.append(item.name)
                    names.sort()
                    entries.append([relative, 'directory', mode])
                    for name in names:
                        if profile and name in _COLD_PAIR_TRANSIENT: continue
                        visit(current / name, relative + '/' + name, depth + 1)
                    require(sorted(os.listdir(current)) == names)
                else:
                    require(stat.S_ISREG(before.st_mode) and before.st_nlink == 1)
                    total += before.st_size
                    require(total <= _COLD_PAIR_BOUNDS['bytes'])
                    digest = hashlib.sha256()
                    fd = os.open(current, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
                    with os.fdopen(fd, 'rb') as stream:
                        require(identity(os.fstat(stream.fileno())) == identity(before))
                        for chunk in iter(lambda: stream.read(1024 * 1024), b''): digest.update(chunk)
                        require(identity(os.fstat(stream.fileno())) == identity(before))
                    entries.append([relative, 'file', mode, before.st_size, digest.hexdigest()])
            require(identity(current.lstat()) == identity(before))
        visit(path, '.', 0)
        require(_cold_pair_identity(self.owner, path, 'directory' if directory else 'file') == native)
        return {'identity': native, 'content': entries, 'bytes': total}

    def observe_members(self, directory=None):
        return {name: self.observe(path if directory is None else directory / name, name == 'profile') for name, path in self.members}

    def copy(self, source, target, profile):
        import shutil
        self.check()
        require(not target.exists() and not target.is_symlink())
        before = self.observe(source, profile)
        require(before is not None)
        if profile:
            self.storage[3](source, target, profile=True)
            # Clone and fallback copies apply the same exclusion at every depth.
            for directory, dirs, files in os.walk(target, followlinks=False):
                for name in dirs + files:
                    if name in _COLD_PAIR_TRANSIENT:
                        entry = pathlib.Path(directory) / name
                        require(entry.is_symlink() or entry.is_file())
                        entry.unlink()
            self.storage[0](target)
        else:
            if not self.storage[2](source, target): shutil.copy2(source, target)
            with Observation().file(target, _COLD_PAIR_BOUNDS['bytes']) as stream: sync_fd(stream.fileno())
        self.storage[1](target)
        after = self.observe(target, profile)
        require(self.observe(source, profile) == before and after['content'] == before['content'])
        return after

    def observe_reserve(self, path, size):
        info = path.lstat()
        require(stat.S_ISREG(info.st_mode) and info.st_uid == os.getuid() and info.st_nlink == 1 and info.st_size == size and info.st_blocks * 512 >= size)
        return {'identity': _cold_pair_identity(self.owner, path, 'file'), 'bytes': size}

    def reserve(self):
        value = self.journal['reserve']
        require(isinstance(value, dict) and set(value) == {'identity', 'bytes'})
        path = self.root / 'reserve.bin'
        if self.journal['reserveState'] == 'held':
            require(self.observe_reserve(path, value['bytes']) == value)
        elif self.journal['reserveState'] == 'released':
            require(not path.exists() and not path.is_symlink())
        else:
            if path.exists() or path.is_symlink(): require(self.observe_reserve(path, value['bytes']) == value)
        return value

    def release_reserve(self):
        self.reserve()
        if self.journal['reserveState'] == 'released': return
        if self.journal['reserveState'] == 'held': self.save(reserveState='release-intent')
        path = self.root / 'reserve.bin'
        with Observation().directory(self.root) as fd:
            self.check()
            if path.exists() or path.is_symlink():
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
        space = os.statvfs(self.owner.directory)
        require(space.f_bavail * space.f_frsize >= amount * 3 + _COLD_PAIR_HEADROOM * 2)
        intent_raw = encoded(self.intent) + b'\n'
        self.owner.save(stateTransaction={'schema': 1, 'transactionId': self.intent['transactionId'], 'intentSha256': sha(intent_raw)})
        self.mkdir(self.root)
        self.write('intent.json', self.intent)
        self.journal = {'schema': 1, 'intentSha256': sha(intent_raw), 'phase': 'snapshot', 'reserve': None, 'reserveState': 'held',
                        'pairSha256': None, 'planSha256': None, 'evidenceSha256': None, 'restoreSha256': None, 'done': [], 'pending': None}
        self.journal_raw = self.write('journal.json', self.journal)
        reserve_size = amount + _COLD_PAIR_HEADROOM
        self.storage[6](self.root / 'reserve.bin', reserve_size)
        self.save(reserve=self.observe_reserve(self.root / 'reserve.bin', reserve_size))
        previous = self.root / 'previous-pair'
        self.mkdir(previous)
        for name, path in self.members:
            if source[name] is not None: self.copy(path, previous / name, name == 'profile')
        self.storage[0](previous)
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
        if restore_path.exists() or restore_path.is_symlink():
            raw = read_owned(restore_path)
            value = decode(raw)
            self.validate_restore_intent(value)
            require(value['live'] == live and self.observe_members(self.root / 'restore-staging') == value['staged'])
            for name in value['staged']:
                staged, previous = value['staged'][name], pair['copied'][name]
                require((staged is None and previous is None) or (staged is not None and previous is not None and staged['content'] == previous['content']))
            require(not os.listdir(self.root / 'advanced-state'))
            self.save(phase='restore', restoreSha256=sha(raw), done=[], pending=None)
            return
        space = os.statvfs(self.root)
        require(space.f_bavail * space.f_frsize >= sum(item['bytes'] for item in pair['copied'].values() if item is not None) + _COLD_PAIR_HEADROOM // 2)
        stage = self.root / 'restore-staging'
        self.mkdir(stage)
        for name, _ in self.members:
            if pair['copied'][name] is not None: self.copy(self.root / 'previous-pair' / name, stage / name, name == 'profile')
        self.storage[0](stage)
        require(self.observe_members() == live)
        self.mkdir(self.root / 'advanced-state')
        value = {'schema': 1, 'intentSha256': self.journal['intentSha256'], 'pairSha256': self.journal['pairSha256'],
                 'live': live, 'staged': self.observe_members(stage), 'advancedIdentity': self.root_identity(self.root / 'advanced-state')}
        raw = self.write('restore-intent.json', value)
        self.save(phase='restore', restoreSha256=sha(raw), done=[], pending=None)

    def validate_restore_intent(self, value):
        require(set(value) == {'schema', 'intentSha256', 'pairSha256', 'live', 'staged', 'advancedIdentity'})
        require(value['schema'] == 1 and value['intentSha256'] == self.journal['intentSha256'] and value['pairSha256'] == self.journal['pairSha256'])
        require(set(value['live']) == set(value['staged']) == {name for name, _ in self.members})
        require(value['advancedIdentity'] == self.root_identity(self.root / 'advanced-state'))

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
