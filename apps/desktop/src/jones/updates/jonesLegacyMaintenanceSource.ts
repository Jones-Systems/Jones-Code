import { jonesColdPairSource } from "./jonesColdPairSource.ts";
import { jonesLegacyBootstrapSource } from "./jonesLegacyBootstrapSource.ts";

const bootstrapEntry = "if __name__ == '__main__':\n    sys.exit(main())\n";
if (!jonesLegacyBootstrapSource.endsWith(bootstrapEntry)) {
  throw new Error("Legacy bootstrap entry changed; maintenance composition needs review.");
}

/** External maintenance with private recovery definitions; no state admission or launch operation. */
export const jonesLegacyMaintenanceSource =
  jonesLegacyBootstrapSource.slice(0, -bootstrapEntry.length) +
  jonesColdPairSource +
  String.raw`
import fcntl, subprocess

MAINTENANCE_SCHEMA = 1
LEGACY_HELPER_SHA256 = '399fc969ab11523ef66f87915b59a68a2eeddf4dbcca9833984a79933764d9cc'
LEGACY_SOURCE = 'bd20d2d2fc0ba89797698192b877a7f1312f8fe3'
LIMITS = {'apps': 32, 'visits': 200000, 'depth': 32, 'entries': 4096,
          'records': 8192, 'metadataBytes': 64 * 1024 * 1024,
          'appBytes': 8 * 1024 * 1024 * 1024, 'totalBytes': 32 * 1024 * 1024 * 1024,
          'gates': 1024}
PHASES = ('prepared', 'owner-published', 'gating', 'armed', 'cancel-intent',
          'cancel-retained-gating', 'cancel-owner-published', 'cancel-restoring', 'cancelled')
BUNDLE_V2_KEYS = {'schema', 'repository', 'sourceSha', 'sourceTree', 'version',
                  'bootstrapSha256', 'executorSha256', 'maintenanceSha256',
                  'identityNativeSha256', 'identityNativeSourceSha256', 'nativeArchitecture'}
LOCK_BYTES = b'jones-activation-lock-v1\n'
BaseObservation = Observation

class NativeUnsupported(Refused):
    pass

class MaintenanceObservation(BaseObservation):
    def __init__(self):
        super().__init__()
        self.visits = 0
        self.record_bytes = 0
        self.record_count = 0
        self.listings = {}

    def names(self, path, optional=False):
        try:
            with self.directory(path) as fd:
                names = []
                with os.scandir(fd) as entries:
                    for entry in entries:
                        self.visits += 1
                        require(len(names) < LIMITS['entries'] and self.visits <= LIMITS['visits'])
                        names.append(entry.name)
                names.sort()
                require(path not in self.listings or self.listings[path] == names)
                self.listings[path] = names
                return names
        except FileNotFoundError:
            require(optional)
            self.listings[path] = None
            return None

    def metadata(self, path, limit=131072):
        raw = self.raw(path, limit)
        self.record_count += 1
        self.record_bytes += len(raw)
        require(self.record_count <= LIMITS['records'] and self.record_bytes <= LIMITS['metadataBytes'])
        return decode(raw), sha(raw)

    def current(self):
        for path in tuple(self.listings): self.names(path, self.listings[path] is None)
        # No unbounded listdir is inherited into the maintenance command.
        for path, names in tuple(self.app_directories.items()): require(self.names(path) == names)
        for path, proof in tuple(self.directories.items()):
            with self.directory(path) as fd: require(identity(os.fstat(fd), True) == proof)
        for path, (proof, target) in tuple(self.links.items()):
            with self.directory(path.parent) as fd:
                require(identity(os.stat(path.name, dir_fd=fd, follow_symlinks=False)) == proof)
                require(os.readlink(path.name, dir_fd=fd) == target)
        for path, proof in tuple(self.files.items()):
            with self.directory(path.parent) as fd:
                require(identity(os.stat(path.name, dir_fd=fd, follow_symlinks=False)) == proof)

Observation = MaintenanceObservation

def maintenance_anchor(manifest, active):
    require(manifest.name == 'jones-active-install.json' and manifest.parent.name == 'runtime')
    home = manifest.parent.parent
    require(active.get('protocol') == 1 and type(active.get('protocol')) is int and active.get('home') == str(home))
    require(hex_value(active.get('generation'), 64) and hex_value(active.get('sourceSha'), 40) and hex_value(active.get('sourceTree'), 40))
    require(all(isinstance(active.get(k), str) and active[k] for k in ('transactionId', 'environmentId', 'version')))
    app, executable = absolute(active['appPath']), absolute(active['executablePath'])
    require(app.name.endswith('.app') and executable.parent == app / 'Contents/MacOS')
    observed = Observation()
    observed.denied = (absolute(active['databasePath']).parent, absolute(active['profile']))
    for path in (home / 'runtime', home / 'bin', app):
        observed.permitted(path)
        require(not any(root.is_relative_to(path) for root in observed.denied))
    return observed

def maintenance_app_entries(observed, root):
    pending, entries, total = [(root, 0)], [], 0
    while pending:
        directory, depth = pending.pop()
        require(depth <= LIMITS['depth'])
        for name in observed.names(directory):
            path = directory / name
            with observed.directory(directory) as fd:
                info = os.stat(name, dir_fd=fd, follow_symlinks=False)
            require(info.st_uid == os.getuid())
            relative = path.relative_to(root).as_posix()
            if stat.S_ISLNK(info.st_mode):
                internal_link(observed, root, path)
                target = observed.links[path][1]
                record = ['link', relative, target]
            elif stat.S_ISDIR(info.st_mode):
                require(info.st_mode & stat.S_IRUSR and not info.st_mode & 0o022)
                record = ['directory', relative, stat.S_IMODE(info.st_mode)]
                pending.append((path, depth + 1))
            else:
                require(stat.S_ISREG(info.st_mode) and info.st_nlink == 1 and info.st_mode & stat.S_IRUSR and not info.st_mode & 0o022)
                require(not info.st_mode & (stat.S_ISUID | stat.S_ISGID | stat.S_ISVTX))
                total += info.st_size
                require(total <= LIMITS['appBytes'])
                record = ['file', relative, stat.S_IMODE(info.st_mode), observed.digest(path, LIMITS['appBytes'])]
            entries.append((path, info, record))
    entries.sort(key=lambda entry: entry[2][1].encode('utf8'))
    return entries, total

def app_digest(observed, root):
    entries, _ = maintenance_app_entries(observed, root)
    return sha(b''.join(encoded(record) + b'\n' for _, _, record in entries))

def transactions(observed, root, home):
    names = observed.names(root, True)
    for name in names or ():
        require(hex_value(name, 64))
        directory = root / name
        observed.denied += (directory / 'previous-pair', directory / 'advanced-state')
        children = observed.names(directory)
        require(not ('retirement-intent.json' in children and 'retirement-receipt.json' not in children))
        intent, _ = observed.metadata(directory / 'intent.json')
        journal, _ = observed.metadata(directory / 'journal.json')
        require(intent.get('protocol') == 1 and type(intent.get('protocol')) is int and intent.get('transactionId') == name)
        require(intent.get('expected', {}).get('home') == home and journal.get('intent') == intent and journal.get('phase') in ('resumed', 'rolled-back'))
    return names

def native_identity(native, digest, requests=()):
    observed = Observation()
    require(observed.digest(native, 16 * 1024 * 1024) == digest)
    require(observed.files[native][3] & 0o777 == 0o500)
    require(len(requests) <= 64 and sum(len(str(path).encode()) + 1 for _, path in requests) <= 32768)
    with contextlib.ExitStack() as stack:
        args, descriptors = [str(native), 'inspect' if requests else 'boot'], []
        for kind, path in requests:
            if kind == 'directory': fd = stack.enter_context(observed.directory(path))
            else: fd = stack.enter_context(observed.file(path, LIMITS['appBytes'])).fileno()
            descriptors.append(fd)
            args.extend(('--' + kind, str(fd), str(path)))
        # Only the retained, hash-bound read-only observer can be executed here.
        result = subprocess.run(args, pass_fds=descriptors, stdin=subprocess.DEVNULL,
                                stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=15,
                                env={'PATH': '/usr/bin:/bin'})
        require(len(result.stdout) <= 131072 and len(result.stderr) <= 8192)
        value = decode(result.stdout)
        if result.returncode == 77 and value.get('status') == 'unsupported':
            raise NativeUnsupported('Required native identity observation is unavailable.')
        require(result.returncode == 0 and value.get('schema') == 1 and value.get('status') == 'observed')
        require(value.get('operation') == ('inspect' if requests else 'boot'))
        require(value.get('observationOnly') is True and value.get('activationAllowed') is False)
        require(isinstance(value.get('bootSessionUuid'), str) and re.fullmatch('[0-9a-fA-F-]{36}', value['bootSessionUuid']))
        require(len(value.get('objects', [])) == len(requests))
        for index, ((kind, path), item) in enumerate(zip(requests, value.get('objects', []))):
            require(item.get('index') == index and item.get('path') == str(path) and item.get('kind') == kind and item.get('current') is True)
            require(item['stat']['uid'] == os.getuid() and item['stat']['flags'] == 0)
            require(item['mount']['filesystem'] == 'apfs' and all(item['mount'].get(key) is True for key in ('local', 'ownershipHonored', 'executable', 'persistentObjectIds', 'objectIds64Bit')))
            require(re.fullmatch('[0-9a-fA-F-]{36}', item['persistent']['volumeUuid']) and re.fullmatch('[0-9]+', item['persistent']['fileId']))
        observed.current()
        return value

def stable_native(item):
    fields = ('uid', 'gid', 'flags', 'birthtimeNs')
    if item['kind'] == 'file': fields += ('nlink', 'size', 'mtimeNs')
    return {'persistent': item['persistent'], 'stat': {key: item['stat'][key] for key in fields}}

def shell_word(value): return "'" + value.replace("'", "'\"'\"'") + "'"

def expected_shim(home, active):
    return ('#!/bin/sh\n# Written by T3 Code: runs the desktop app\'s bundled t3 CLI.\n' +
            'export T3CODE_CLI_PATH=' + shell_word(str(home / 'bin/t3')) + '\n' +
            'home=' + shell_word(str(home)) + '\nexport T3CODE_HOME="' + chr(36) + '{T3CODE_HOME:-$home}"\n' +
            'export ELECTRON_RUN_AS_NODE=1\napp=' + shell_word(active['executablePath']) + '\n' +
            'if [ ! -x "$app" ]; then\n  echo "T3 Code has moved or been removed. Open the app once to update this command." >&2\n  exit 127\nfi\n' +
            'exec "$app" ' + shell_word(active['appPath'] + '/Contents/Resources/app.asar/apps/server/dist/bin.mjs') + ' "$@"\n').encode()

class StrictInventory:
    def __init__(self, manifest, active, native, native_digest, candidate_helper, planned=None, modes=None):
        self.observed = maintenance_anchor(manifest, active)
        self.active, self.home = active, absolute(active['home'])
        self.updater = self.home / 'runtime/jones-updates'
        self.native, self.native_digest = native, native_digest
        self.candidate_helper = candidate_helper
        self.planned, self.modes = planned, modes or {}
        self.records, self.apps, self.objects, self.total = {}, {}, [], 0
        self.indexes = {}
        self.boot = None

    def record(self, path):
        value, digest = self.observed.metadata(path)
        self.records[str(path)] = digest
        return value

    def index(self, path):
        with self.observed.directory(path.parent) as parent:
            fd = os.open(path.name, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK, dir_fd=parent)
            with os.fdopen(fd, 'rb') as stream:
                info = os.fstat(stream.fileno())
                require(stat.S_ISREG(info.st_mode) and info.st_uid == os.getuid() and info.st_nlink in (1, 2) and not info.st_mode & 0o022 and info.st_size <= 4096)
                raw = stream.read(4097)
                require(len(raw) == info.st_size and identity(os.fstat(stream.fileno())) == identity(info))
                require(identity(os.stat(path.name, dir_fd=parent, follow_symlinks=False)) == identity(info))
                self.observed.files[path] = identity(info)
        self.observed.record_count += 1
        self.observed.record_bytes += len(raw)
        require(self.observed.record_count <= LIMITS['records'] and self.observed.record_bytes <= LIMITS['metadataBytes'])
        value = decode(raw)
        require(set(value) == {'schema', 'handle', 'directory'} and type(value['schema']) is int and value['schema'] == 1 and hex_value(value['handle'], 64))
        self.records[str(path)] = sha(raw)
        return value, info

    def stage_dirs(self, root):
        result = []
        for name in self.observed.names(root, True) or ():
            path = root / name
            if hex_value(name, 64): result.append(path)
            elif name == 'attempts':
                for attempt in self.observed.names(path):
                    require(re.fullmatch('stage-[a-zA-Z0-9]+', attempt))
                    children = self.observed.names(path / attempt)
                    handles = [name for name in children if hex_value(name, 64)]
                    require(len(handles) == 1 and set(children) <= {handles[0], 'completion.json'})
                    directory = path / attempt / handles[0]
                    result.append(directory)
                    if 'completion.json' in children:
                        value, info = self.index(path / attempt / 'completion.json')
                        require(value == {'schema': 1, 'handle': handles[0], 'directory': str(directory)})
                        self.indexes[str(directory)] = (value, info)
            elif name == 'completed':
                # Publisher indexes are checked after their attempt companions.
                continue
            else: raise Refused('Unknown staging route.')
        return result

    def app(self, root, expected, role):
        root = absolute(str(root))
        if str(root) in self.apps:
            require(self.apps[str(root)]['expected'] == expected)
            return
        require(len(self.apps) < LIMITS['apps'])
        self.observed.permitted(root)
        require(not any(denied.is_relative_to(root) for denied in self.observed.denied))
        bundle_identity(self.observed, root, expected)
        entries, total = maintenance_app_entries(self.observed, root)
        self.total += total
        require(self.total <= LIMITS['totalBytes'])
        records = []
        pending = [('directory', root, stat.S_IMODE(os.stat(root, follow_symlinks=False).st_mode), None)]
        for path, info, original in entries:
            record = list(original)
            if record[0] == 'file' and str(path) in self.modes:
                require(record[2] in self.modes[str(path)])
                record[2] = self.planned[str(path)]['originalMode']
            records.append(record)
            if record[0] == 'link': continue
            pending.append(('directory' if record[0] == 'directory' else 'file', path, record[2], record[3] if len(record) == 4 else None))
        digest = sha(b''.join(encoded(record) + b'\n' for record in records))
        if 'appDigest' in expected: require(digest == expected['appDigest'])
        self.apps[str(root)] = {'expected': expected, 'role': role, 'digest': digest, 'links': [r for r in records if r[0] == 'link']}
        batch = []
        for entry in pending:
            if batch and (len(batch) == 64 or sum(len(str(item[1]).encode()) + 1 for item in batch + [entry]) > 32768):
                self.identities(root, batch)
                batch = []
            batch.append(entry)
        if batch: self.identities(root, batch)

    def identities(self, root, batch):
        value = native_identity(self.native, self.native_digest, [(kind, path) for kind, path, _, _ in batch])
        require(self.boot is None or self.boot == value['bootSessionUuid'])
        self.boot = value['bootSessionUuid']
        for (kind, path, mode, content), item in zip(batch, value['objects']):
            allowed = self.modes.get(str(path), (mode,))
            require(item['stat']['mode'] in allowed)
            self.objects.append({'path': str(path), 'app': str(root), 'kind': kind, 'identity': stable_native(item),
                                 'originalMode': mode, 'gatedMode': mode & ~0o111 if kind == 'file' else mode,
                                 'sha256': content})

    def fenced_file(self, path, content):
        value = native_identity(self.native, self.native_digest, [('file', path)])
        require(self.boot is None or self.boot == value['bootSessionUuid'])
        self.boot = value['bootSessionUuid']
        item = value['objects'][0]
        mode = item['stat']['mode']
        self.objects.append({'path': str(path), 'app': None, 'kind': 'file', 'identity': stable_native(item),
                             'originalMode': mode, 'gatedMode': mode, 'sha256': content})

    def run(self):
        observed, updater = self.observed, self.updater
        roots = observed.names(updater)
        known = {'apps', 'artifacts', 'transactions', 'staging', 'bootstrap-executors'}
        for name in roots:
            if name in known: continue
            match = re.fullmatch(r'activate-v1-([a-f0-9]{64})\.py', name)
            require(match is not None and match[1] in (LEGACY_HELPER_SHA256, self.candidate_helper))
            path = updater / name
            require(observed.digest(path, 1048576) == match[1])
            self.records[str(path)] = match[1]
            self.fenced_file(path, match[1])
        self.app(absolute(self.active['appPath']), self.active, 'canonical')
        artifacts = {}
        for directory in self.stage_dirs(updater / 'artifacts'):
            initial = self.record(directory / 'jones-actions-stage.json')
            validate_candidate(initial['candidate'])
            artifact_receipt(initial['receipt'], initial['candidate'])
            for payload in ('github-artifact.zip', initial['receipt']['artifact']):
                with observed.file(directory / payload, MAX_FILE) as stream: self.total += os.fstat(stream.fileno()).st_size
            require(self.total <= LIMITS['totalBytes'])
            artifact, _ = validate_artifact(observed, directory)
            self.record(directory / 'jones-actions-stage.json')
            require(set(observed.names(directory)) == {'jones-actions-stage.json', 'github-artifact.zip', artifact['receipt']['artifact']})
            artifacts.setdefault(directory.name, []).append(artifact)
        staged = {}
        for directory in self.stage_dirs(updater / 'apps'):
            receipt = self.record(directory / 'mac-app-receipt.json')
            require(set(receipt) == {'app', 'artifact', 'candidate'})
            expected, candidate = receipt['app'], receipt['candidate']
            require(validate_candidate(candidate) == directory.name and receipt['artifact'] in artifacts.get(directory.name, ()))
            require(receipt['artifact']['candidate'] == candidate and expected['handle'] == directory.name)
            require(expected['receiptPath'] == str(directory / 'mac-app-receipt.json'))
            root = absolute(expected['appPath'])
            require(root.parent == directory and root.name.endswith('.app'))
            require(set(observed.names(directory)) == {root.name, 'mac-app-receipt.json'})
            require(all(expected[key] == candidate[source] for key, source in (('sourceSha', 'source'), ('sourceTree', 'tree'), ('version', 'version'))))
            require(hex_value(expected.get('appDigest'), 64))
            self.app(root, expected, 'staged')
            staged[str(root)] = expected
        for name in transactions(observed, updater / 'transactions', str(self.home)) or ():
            directory = updater / 'transactions' / name
            intent, journal = self.record(directory / 'intent.json'), self.record(directory / 'journal.json')
            prior, candidate = intent['expected'], intent['staged']
            require(candidate == staged.get(candidate.get('appPath')))
            require(all(prior.get(k) == self.active[k] for k in ('home', 'databasePath', 'profile', 'environmentId', 'appPath')))
            require(hex_value(prior.get('generation'), 64) and intent.get('continuationReceipt') == str(directory / 'continuation.json'))
            public = {'intent.json', 'journal.json', 'continuation.json', 'preflight.json', 'retirement-intent.json', 'retirement-receipt.json',
                      'trial-descriptor.json', 'trial-receipt.json', 'commit-grant.json', 'resume-intent.json', 'resume-dispatched.json',
                      'prepare-intent.json', 'prepare-dispatched.json', 'activation-request.json'}
            children = observed.names(directory)
            require(set(children) <= public | {'previous-pair', 'advanced-state'})
            for child in children:
                if child in public: self.record(directory / child)
            stable = absolute(self.active['appPath'])
            for sibling, expected, role in ((stable.with_name(stable.name + '.jones-incoming-' + name), candidate, 'incoming'),
                                           (stable.with_name(stable.name + '.jones-previous-' + prior['generation']), prior, 'retained')):
                if observed.names(sibling, True) is not None:
                    require(hex_value(expected.get('appDigest'), 64))
                    relocated = dict(expected, appPath=str(sibling), executablePath=str(sibling / 'Contents/MacOS' / absolute(expected['executablePath']).name))
                    self.app(sibling, relocated, role)
        for name in observed.names(updater / 'staging', True) or ():
            require(re.fullmatch(r'[a-f0-9]{40}-[a-f0-9]{64}\.json', name))
            selection = self.record(updater / 'staging' / name)
            require(selection.get('schema') == 1 and selection.get('source') == 'jones-actions' and selection.get('home') == str(self.home))
            require(selection.get('app') == staged.get(selection.get('app', {}).get('appPath')))
            prior = selection['active']
            require(all(prior.get(k) == self.active[k] for k in ('home', 'databasePath', 'profile', 'environmentId', 'appPath')))
        for name in observed.names(updater / 'bootstrap-executors', True) or ():
            require(hex_value(name, 64))
            directory = updater / 'bootstrap-executors' / name
            require(set(observed.names(directory)) == {'executor.py', 'receipt.json'})
            receipt = self.record(directory / 'receipt.json')
            require(receipt.get('bindingDigest') == name and receipt.get('status') == 'executor-ready' and receipt.get('activationAllowed') is False)
            require(receipt.get('manifestPath') == str(self.home / 'runtime/jones-active-install.json'))
            require(receipt.get('executorSha256') == self.candidate_helper and observed.digest(directory / 'executor.py', 1048576) == self.candidate_helper)
            self.records[str(directory / 'executor.py')] = self.candidate_helper
            self.fenced_file(directory / 'executor.py', self.candidate_helper)
        # A completion index can select only the independently traversed attempt
        # whose publisher-owned companion has the identical inode and bytes.
        for kind in ('apps', 'artifacts'):
            root = updater / kind / 'completed'
            for name in observed.names(root, True) or ():
                require(re.fullmatch(r'[a-f0-9]{64}\.json', name))
                path = root / name
                value, info = self.index(path)
                require(value['handle'] == name[:-5] and value['directory'] in self.indexes)
                original, prior = self.indexes[value['directory']]
                require(original == value and info.st_dev == prior.st_dev and info.st_ino == prior.st_ino and info.st_nlink == 2)
        bin_names = observed.names(self.home / 'bin', True)
        if bin_names is not None:
            require(set(bin_names) <= {'t3'})
            if 't3' in bin_names:
                path = self.home / 'bin/t3'
                require(observed.raw(path, 16384) == expected_shim(self.home, self.active))
                self.records[str(path)] = sha(expected_shim(self.home, self.active))
                self.fenced_file(path, self.records[str(path)])
        require(sum(item['gatedMode'] != item['originalMode'] for item in self.objects) <= LIMITS['gates'])
        observed.current()
        return {'records': self.records, 'listings': {str(k): v for k, v in observed.listings.items()},
                'apps': self.apps, 'objects': sorted(self.objects, key=lambda item: item['path'])}

def sync_fd(fd):
    os.fsync(fd)
    if sys.platform == 'darwin' and stat.S_ISREG(os.fstat(fd).st_mode):
        fcntl.fcntl(fd, 51)  # Darwin F_FULLFSYNC also requests stable storage.

def read_owned(path, limit=LIMITS['metadataBytes']):
    return Observation().raw(path, limit)

def durable_file(path, raw, mode, previous=None):
    require(len(raw) <= LIMITS['metadataBytes'])
    observed = Observation()
    with observed.directory(path.parent) as parent:
        if previous is None:
            flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW
            fd = os.open(path.name, flags, mode, dir_fd=parent)
            with os.fdopen(fd, 'wb') as stream:
                os.fchmod(stream.fileno(), mode)
                stream.write(raw)
                stream.flush()
                sync_fd(stream.fileno())
        else:
            require(observed.raw(path, LIMITS['metadataBytes']) == previous)
            temporary = '.write-' + uuid.uuid4().hex
            fd = os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, mode, dir_fd=parent)
            try:
                with os.fdopen(fd, 'wb') as stream:
                    os.fchmod(stream.fileno(), mode)
                    stream.write(raw)
                    stream.flush()
                    sync_fd(stream.fileno())
                observed.current()
                os.replace(temporary, path.name, src_dir_fd=parent, dst_dir_fd=parent)
            finally:
                try: os.unlink(temporary, dir_fd=parent)
                except FileNotFoundError: pass
        sync_fd(parent)
    require(read_owned(path) == raw)
    with Observation().file(path, LIMITS['metadataBytes']) as stream: sync_fd(stream.fileno())

@contextlib.contextmanager
def fixed_lock(manifest):
    path = manifest.parent / 'jones-activation.lock'
    observed = Observation()
    with observed.directory(path.parent) as parent:
        created = False
        try:
            fd = os.open(path.name, os.O_RDWR | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600, dir_fd=parent)
            created = True
        except FileExistsError:
            fd = os.open(path.name, os.O_RDWR | os.O_NOFOLLOW | os.O_NONBLOCK, dir_fd=parent)
        try:
            if created:
                require(os.write(fd, LOCK_BYTES) == len(LOCK_BYTES))
                sync_fd(fd)
                sync_fd(parent)
            info = os.fstat(fd)
            require(stat.S_ISREG(info.st_mode) and info.st_uid == os.getuid() and info.st_nlink == 1 and not info.st_mode & 0o022)
            os.lseek(fd, 0, os.SEEK_SET)
            require(info.st_size == len(LOCK_BYTES) and os.read(fd, len(LOCK_BYTES) + 1) == LOCK_BYTES)
            fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
            proof = identity(info)
            def current():
                require(identity(os.fstat(fd)) == proof and identity(os.stat(path.name, dir_fd=parent, follow_symlinks=False)) == proof)
            current()
            yield current
            current()
        finally: os.close(fd)

def maintenance_report(status, **fields):
    return dict(schema=1, status=status, stateAdmissionAllowed=False, activationAllowed=False,
                launchMembershipComplete=False, processCustody='unobserved', blocker='DESK-FIRST-01', **fields)

class Maintenance:
    def __init__(self, manifest, current_lock):
        self.manifest, self.current_lock = manifest, current_lock
        self.root = manifest.parent / 'jones-maintenance'

    def save(self, **updates):
        value = dict(self.journal, **updates)
        raw = encoded(value) + b'\n'
        self.current_lock()
        durable_file(self.directory / 'journal.json', raw, 0o600, self.journal_raw)
        self.journal, self.journal_raw = value, raw

    def load(self):
        pointer = decode(read_owned(self.root / 'current.json', 8192))
        require(set(pointer) == {'schema', 'generation', 'planSha256'} and pointer['schema'] == 1 and hex_value(pointer['generation'], 64))
        self.directory = self.root / pointer['generation']
        raw = read_owned(self.directory / 'plan.json')
        require(sha(raw) == pointer['planSha256'])
        self.plan = decode(raw)
        require(set(self.plan) == {'schema', 'maintenanceGeneration', 'originalManifest', 'originalManifestSha256', 'manifestPath', 'selectionPath', 'selectionSha256', 'candidateBinding', 'bundle', 'inventory', 'lockIdentity', 'toolIdentities'})
        require(self.plan['schema'] == 1 and self.plan['maintenanceGeneration'] == pointer['generation'] and self.plan['manifestPath'] == str(self.manifest))
        require(sha(self.plan['originalManifest'].encode()) == self.plan['originalManifestSha256'])
        self.original = decode(self.plan['originalManifest'].encode())
        maintenance_anchor(self.manifest, self.original)
        require(self.original['owner'] == 'desktop' and self.original['sourceSha'] == LEGACY_SOURCE)
        self.journal_raw = read_owned(self.directory / 'journal.json')
        self.journal = decode(self.journal_raw)
        journal_keys = {'schema', 'maintenanceGeneration', 'planSha256', 'phase', 'done', 'intent', 'armedBootSessionUuid', 'cancel'}
        require(set(self.journal) in (journal_keys, journal_keys | {'stateTransaction'}))
        require(self.journal['schema'] == 1 and self.journal['maintenanceGeneration'] == pointer['generation'] and self.journal['planSha256'] == pointer['planSha256'] and self.journal['phase'] in PHASES)
        self.native = self.directory / 'tools/identity-native'
        self.bundle = self.plan['bundle']
        require(set(self.bundle) == BUNDLE_V2_KEYS and self.bundle['schema'] == 2 and self.bundle['nativeArchitecture'] == 'arm64')
        require(self.bundle['repository'] == REPOSITORY and all(hex_value(self.bundle[key], 64) for key in ('bootstrapSha256', 'executorSha256', 'maintenanceSha256', 'identityNativeSha256', 'identityNativeSourceSha256')))
        binding = self.plan['candidateBinding']
        require(binding['manifestPath'] == str(self.manifest) and binding['manifestSha256'] == self.plan['originalManifestSha256'])
        require(binding['selectionPath'] == self.plan['selectionPath'] and binding['selectionSha256'] == self.plan['selectionSha256'])
        require(binding['installedGeneration'] == self.original['generation'] and binding['installedSource'] == self.original['sourceSha'])
        require(binding['sourceSha'] == self.bundle['sourceSha'] and binding['sourceTree'] == self.bundle['sourceTree'] and binding['executorSha256'] == self.bundle['executorSha256'])
        require(binding['status'] == 'executor-ready' and binding['activationAllowed'] is False)
        require(sha(read_owned(self.directory / 'tools/maintenance.py')) == self.bundle['maintenanceSha256'])
        require(Observation().digest(self.native, 16 * 1024 * 1024) == self.bundle['identityNativeSha256'])
        require(absolute(__file__) == self.directory / 'tools/maintenance.py')
        self.gates = [item for item in self.plan['inventory']['objects'] if item['gatedMode'] != item['originalMode']]
        require(len(self.gates) <= LIMITS['gates'] and self.gates)
        paths = [item['path'] for item in self.gates]
        require(len(paths) == len(set(paths)) and isinstance(self.journal['done'], list) and len(self.journal['done']) == len(set(self.journal['done'])) and set(self.journal['done']) <= set(paths))
        if self.journal['intent'] is not None: require(self.journal['intent'] in paths and self.journal['intent'] not in self.journal['done'])
        self.validate_cursor()

    def validate_cursor(self):
        phase = self.journal['phase']
        if 'stateTransaction' in self.journal:
            fence = self.journal['stateTransaction']
            require(phase == 'armed' and isinstance(fence, dict) and set(fence) == {'schema', 'transactionId', 'intentSha256'})
            require(type(fence['schema']) is int and fence['schema'] == 1 and hex_value(fence['transactionId'], 64) and hex_value(fence['intentSha256'], 64))
        paths = [item['path'] for item in self.gates]
        if phase in ('cancel-owner-published', 'cancel-restoring', 'cancelled'):
            paths = sorted((item['path'] for item in self.gates if item['app'] == self.original['appPath']),
                           key=lambda path: (path == self.original['executablePath'], path))
        done, intent = self.journal['done'], self.journal['intent']
        require(done == paths[:len(done)] and (intent is None or (len(done) < len(paths) and intent == paths[len(done)])))
        if phase in ('prepared', 'owner-published'): require(not done and intent is None)
        if phase in ('armed', 'cancelled'): require(done == paths and intent is None)
        if phase == 'armed': require(isinstance(self.journal['armedBootSessionUuid'], str))
        if not phase.startswith('cancel'): require(self.journal['cancel'] is None)
        else:
            cancellation = self.journal['cancel']
            require(isinstance(cancellation, dict) and set(cancellation) == {'generation', 'manifest', 'manifestSha256', 'armedDone', 'armedIntent'})
            armed_paths = [item['path'] for item in self.gates]
            require(cancellation['armedDone'] == armed_paths[:len(cancellation['armedDone'])])
            require(cancellation['armedIntent'] is None or (len(cancellation['armedDone']) < len(armed_paths) and cancellation['armedIntent'] == armed_paths[len(cancellation['armedDone'])]))
            require(hex_value(cancellation['generation'], 64) and cancellation['generation'] not in (self.original['generation'], self.plan['maintenanceGeneration']))
            raw = cancellation['manifest'].encode()
            require(sha(raw) == cancellation['manifestSha256'] and decode(raw) == dict(self.original, generation=cancellation['generation']))

    def owner(self):
        return dict(self.original, owner='maintenance', generation=self.plan['maintenanceGeneration'],
                    maintenance={'schema': 1, 'planSha256': self.journal['planSha256']})

    def modes(self):
        phase, done, intent = self.journal['phase'], self.journal['done'], self.journal['intent']
        cancelling = phase.startswith('cancel')
        result = {}
        for item in self.gates:
            path, old, gated = item['path'], item['originalMode'], item['gatedMode']
            if phase in ('cancel-owner-published', 'cancel-restoring', 'cancelled'):
                if item['app'] != self.original['appPath']: allowed = (gated,)
                else: allowed = (old,) if path in done else (gated, old) if path == intent else (gated,)
            elif cancelling:
                original_done = self.journal['cancel']['armedDone']
                original_intent = self.journal['cancel']['armedIntent']
                allowed = (gated,) if path in original_done or path in done else (old, gated) if path in (original_intent, intent) else (old,)
            else: allowed = (gated,) if path in done else (old, gated) if path == intent else (old,)
            result[path] = allowed
        return result

    def verify(self):
        self.validate_cursor()
        self.current_lock()
        retained = [self.manifest.parent / 'jones-activation.lock', self.directory / 'tools/maintenance.py', self.native]
        identity_batch = native_identity(self.native, self.bundle['identityNativeSha256'], [('file', path) for path in retained])
        require(stable_native(identity_batch['objects'][0]) == self.plan['lockIdentity'])
        require([stable_native(item) for item in identity_batch['objects'][1:]] == self.plan['toolIdentities'])
        require(all(item['stat']['mode'] == 0o500 for item in identity_batch['objects'][1:]))
        active_raw = read_owned(self.manifest)
        active = decode(active_raw)
        phase = self.journal['phase']
        if phase == 'prepared': require(active_raw.decode() == self.plan['originalManifest'] or active == self.owner())
        elif phase in ('cancel-retained-gating', 'cancel-intent'):
            require(active == self.owner() or (phase == 'cancel-intent' and active_raw.decode() == self.plan['originalManifest']))
        elif phase in ('cancel-owner-published', 'cancel-restoring', 'cancelled'):
            require(active_raw.decode() == self.journal['cancel']['manifest'] or (phase == 'cancel-owner-published' and active == self.owner()))
        else: require(active == self.owner())
        planned = {item['path']: item for item in self.plan['inventory']['objects']}
        inventory = StrictInventory(self.manifest, self.original, self.native, self.bundle['identityNativeSha256'], self.bundle['executorSha256'], planned, self.modes())
        require(inventory.run() == self.plan['inventory'] and inventory.boot == identity_batch['bootSessionUuid'])
        require(read_owned(absolute(self.plan['selectionPath'])) and sha(read_owned(absolute(self.plan['selectionPath']))) == self.plan['selectionSha256'])
        self.current_lock()
        return inventory.boot

    def gate(self, item, mode):
        path = absolute(item['path'])
        before = native_identity(self.native, self.bundle['identityNativeSha256'], [('file', path)])['objects'][0]
        require(stable_native(before) == item['identity'] and before['stat']['mode'] in self.modes()[str(path)])
        with Observation().directory(path.parent) as parent:
            fd = os.open(path.name, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK, dir_fd=parent)
            try:
                proof = os.fstat(fd)
                require(stat.S_ISREG(proof.st_mode) and proof.st_uid == os.getuid() and proof.st_nlink == 1)
                require(str(proof.st_ino) == before['stat']['inode'] and str(proof.st_dev) == before['stat']['device'])
                digest = hashlib.sha256()
                read_bytes = 0
                while True:
                    block = os.read(fd, 1048576)
                    if not block: break
                    read_bytes += len(block)
                    require(read_bytes <= proof.st_size <= LIMITS['appBytes'])
                    digest.update(block)
                require(digest.hexdigest() == item['sha256'] and identity(os.fstat(fd)) == identity(proof))
                require(identity(os.stat(path.name, dir_fd=parent, follow_symlinks=False)) == identity(proof))
                self.current_lock()
                os.fchmod(fd, mode)
                sync_fd(fd)
                require(os.fstat(fd).st_ino == proof.st_ino)
                require(identity(os.stat(path.name, dir_fd=parent, follow_symlinks=False)) == identity(os.fstat(fd)))
            finally: os.close(fd)
        after = native_identity(self.native, self.bundle['identityNativeSha256'], [('file', path)])['objects'][0]
        require(stable_native(after) == item['identity'] and after['stat']['mode'] == mode)

    def apply(self, items, restore=False):
        for item in items:
            path = item['path']
            if path in self.journal['done']: continue
            require(self.journal['intent'] in (None, path))
            self.save(intent=path)
            self.gate(item, item['originalMode'] if restore else item['gatedMode'])
            self.save(done=self.journal['done'] + [path], intent=None)

    def reconcile(self):
        boot = self.verify()
        if self.journal['phase'].startswith('cancel'): return self.cancel()
        if self.journal['phase'] == 'prepared':
            raw = read_owned(self.manifest)
            if decode(raw) != self.owner(): durable_file(self.manifest, encoded(self.owner()) + b'\n', 0o600, raw)
            self.save(phase='owner-published')
        if self.journal['phase'] in ('owner-published', 'gating'):
            self.save(phase='gating')
            self.apply(self.gates)
            boot = self.verify()
            self.save(phase='armed', armedBootSessionUuid=boot)
        require(self.journal['phase'] == 'armed' and isinstance(self.journal['armedBootSessionUuid'], str))
        boot = self.verify()
        self.sync_evidence()
        status = 'armed-awaiting-cold-restart' if boot == self.journal['armedBootSessionUuid'] else 'cold-boundary-observed-admission-held'
        return maintenance_report(status, maintenanceGeneration=self.plan['maintenanceGeneration'], recoveryEntry=str(self.directory / 'tools/maintenance.py'))

    def cancel(self):
        # Missing child evidence cannot turn a possible state effect into PRE-STATE cancellation.
        require('stateTransaction' not in self.journal)
        self.verify()
        if not self.journal['phase'].startswith('cancel'):
            recovered = uuid.uuid4().hex + uuid.uuid4().hex
            require(recovered not in (self.original['generation'], self.plan['maintenanceGeneration']))
            raw = encoded(dict(self.original, generation=recovered)) + b'\n'
            cancellation = {'generation': recovered, 'manifest': raw.decode(), 'manifestSha256': sha(raw),
                            'armedDone': self.journal['done'], 'armedIntent': self.journal['intent']}
            self.save(phase='cancel-intent', done=[], intent=None, cancel=cancellation)
        cancellation = self.journal['cancel']
        require(set(cancellation) == {'generation', 'manifest', 'manifestSha256', 'armedDone', 'armedIntent'})
        require(hex_value(cancellation['generation'], 64) and cancellation['generation'] not in (self.original['generation'], self.plan['maintenanceGeneration']))
        recovered = dict(self.original, generation=cancellation['generation'])
        require(decode(cancellation['manifest'].encode()) == recovered and sha(cancellation['manifest'].encode()) == cancellation['manifestSha256'])
        if self.journal['phase'] in ('cancel-intent', 'cancel-retained-gating'):
            raw = read_owned(self.manifest)
            if raw.decode() == self.plan['originalManifest']:
                durable_file(self.manifest, encoded(self.owner()) + b'\n', 0o600, raw)
            self.save(phase='cancel-retained-gating')
            # Gate all originals first: cancellation can begin halfway through arm.
            self.apply(self.gates)
            self.verify()
            raw = read_owned(self.manifest)
            require(decode(raw) == self.owner())
            # Publish intent before owner replacement so a crash after replace is
            # recognizable without accepting any unrelated desktop epoch.
            self.save(phase='cancel-owner-published', done=[], intent=None)
        if self.journal['phase'] in ('cancel-owner-published', 'cancel-restoring'):
            raw = read_owned(self.manifest)
            if raw.decode() != cancellation['manifest']:
                require(self.journal['phase'] == 'cancel-owner-published' and decode(raw) == self.owner())
                durable_file(self.manifest, cancellation['manifest'].encode(), 0o600, raw)
            self.save(phase='cancel-restoring')
            canonical = [item for item in self.gates if item['app'] == self.original['appPath']]
            canonical.sort(key=lambda item: (item['path'] == self.original['executablePath'], item['path']))
            require(canonical and canonical[-1]['path'] == self.original['executablePath'])
            self.apply(canonical, True)
            self.verify()
            self.save(phase='cancelled')
        self.verify()
        self.sync_evidence()
        return maintenance_report('cancelled-pre-state', recoveredGeneration=cancellation['generation'], canonicalExecutionRestored=True)

    def sync_evidence(self):
        for name in ('plan.json', 'journal.json', 'tools/maintenance.py', 'tools/identity-native'):
            with Observation().file(self.directory / name, LIMITS['metadataBytes']) as stream: sync_fd(stream.fileno())
        with Observation().file(self.manifest, 131072) as stream: sync_fd(stream.fileno())
        with Observation().file(self.root / 'current.json', 8192) as stream: sync_fd(stream.fileno())
        for path in (self.directory / 'tools', self.directory, self.root, self.manifest.parent):
            with Observation().directory(path) as fd: sync_fd(fd)

    def arm(self, selection_path, selection_sha):
        require(self.manifest.name == 'jones-active-install.json' and self.manifest.parent.name == 'runtime')
        original_raw = read_owned(self.manifest, 131072)
        self.original = decode(original_raw)
        maintenance_anchor(self.manifest, self.original)
        require(self.original['owner'] == 'desktop' and self.original['sourceSha'] == LEGACY_SOURCE)
        require(selection_path.parent == self.manifest.parent / 'jones-updates/staging' and selection_path.name.startswith(LEGACY_SOURCE + '-') and selection_path.name.endswith('.json') and hex_value(selection_sha, 64))
        observed = Observation()
        require(observed.names(self.root, True) is None)
        selection = decode(read_owned(selection_path))
        tools = absolute(selection['app']['appPath']) / 'Contents/Resources/jones-bootstrap'
        require(absolute(__file__) == tools / 'maintenance.py')
        checked, active, updater, names, output, executor, binding = inspect_inputs(self.manifest, selection_path, selection_sha, tools / 'bootstrap.py')
        self.bundle, _ = checked.metadata(tools / 'BUNDLE.json', 8192)
        require(set(self.bundle) == BUNDLE_V2_KEYS and self.bundle['schema'] == 2 and self.bundle['nativeArchitecture'] == 'arm64')
        maintenance = checked.raw(tools / 'maintenance.py', 1048576)
        native = checked.raw(tools / 'identity-native', 16 * 1024 * 1024)
        require(sha(maintenance) == self.bundle['maintenanceSha256'] and sha(native) == self.bundle['identityNativeSha256'])
        inventory = StrictInventory(self.manifest, active, tools / 'identity-native', self.bundle['identityNativeSha256'], sha(executor))
        snapshot = inventory.run()
        checked.current()
        require(read_owned(self.manifest) == original_raw)
        generation = uuid.uuid4().hex + uuid.uuid4().hex
        self.plan = {'schema': 1, 'maintenanceGeneration': generation, 'originalManifest': original_raw.decode(),
                     'originalManifestSha256': sha(original_raw), 'manifestPath': str(self.manifest),
                     'selectionPath': str(selection_path), 'selectionSha256': selection_sha,
                     'candidateBinding': binding, 'bundle': self.bundle, 'inventory': snapshot}
        self.current_lock()
        self.root = ensure_directory(Observation(), self.manifest.parent, 'jones-maintenance')
        self.directory = ensure_directory(Observation(), self.root, generation)
        retained = ensure_directory(Observation(), self.directory, 'tools')
        durable_file(retained / 'maintenance.py', maintenance, 0o500)
        durable_file(retained / 'identity-native', native, 0o500)
        identities = native_identity(retained / 'identity-native', self.bundle['identityNativeSha256'],
                                     [('file', path) for path in (self.manifest.parent / 'jones-activation.lock', retained / 'maintenance.py', retained / 'identity-native')])
        self.plan['lockIdentity'] = stable_native(identities['objects'][0])
        self.plan['toolIdentities'] = [stable_native(item) for item in identities['objects'][1:]]
        plan_raw = encoded(self.plan) + b'\n'
        require(len(plan_raw) <= LIMITS['metadataBytes'])
        durable_file(self.directory / 'plan.json', plan_raw, 0o400)
        self.journal = {'schema': 1, 'maintenanceGeneration': generation, 'planSha256': sha(plan_raw),
                        'phase': 'prepared', 'done': [], 'intent': None, 'armedBootSessionUuid': None, 'cancel': None}
        self.journal_raw = encoded(self.journal) + b'\n'
        durable_file(self.directory / 'journal.json', self.journal_raw, 0o600)
        durable_file(self.root / 'current.json', encoded({'schema': 1, 'generation': generation, 'planSha256': sha(plan_raw)}) + b'\n', 0o400)
        self.native = retained / 'identity-native'
        self.gates = [item for item in snapshot['objects'] if item['gatedMode'] != item['originalMode']]
        require(self.gates and any(item['path'] == active['executablePath'] for item in self.gates))
        self.sync_evidence()
        return self.reconcile()

def maintenance_main(argv=None):
    parser = argparse.ArgumentParser(description='Pre-state legacy maintenance; state admission is unavailable.')
    sub = parser.add_subparsers(dest='operation', required=True)
    for operation in ('arm', 'reconcile', 'cancel'):
        command = sub.add_parser(operation)
        command.add_argument('--manifest', required=True)
        if operation == 'arm':
            command.add_argument('--selection', required=True)
            command.add_argument('--selection-sha256', required=True)
    args = parser.parse_args(argv)
    status = 'refused'
    try:
        require(sys.platform == 'darwin')
        manifest = absolute(args.manifest)
        require(manifest.name == 'jones-active-install.json' and manifest.parent.name == 'runtime')
        active = decode(read_owned(manifest, 131072))
        maintenance_anchor(manifest, active)
        with fixed_lock(manifest) as current_lock:
            owner = Maintenance(manifest, current_lock)
            if args.operation == 'arm': result = owner.arm(absolute(args.selection), args.selection_sha256)
            else:
                status = 'held'
                owner.load()
                result = owner.cancel() if args.operation == 'cancel' else owner.reconcile()
        print(json.dumps(result, sort_keys=True))
        return 0
    except NativeUnsupported:
        print(json.dumps(maintenance_report('unsupported', reason='Required native identity observation is unavailable; existing gates are preserved.'), sort_keys=True))
        return 77
    except (Refused, OSError, ValueError, KeyError, TypeError, OverflowError, RuntimeError, subprocess.SubprocessError, zipfile.BadZipFile):
        print(json.dumps(maintenance_report(status, reason='Maintenance evidence is incomplete or changed; retained files and existing gates are preserved.'), sort_keys=True))
        return 1

if __name__ == '__main__':
    sys.exit(maintenance_main())
`;
