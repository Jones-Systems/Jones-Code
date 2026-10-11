/** Standalone observation and staging; neither the application nor the executor is launched. */
export const jonesLegacyBootstrapSource = String.raw`#!/usr/bin/env python3
import sys
sys.dont_write_bytecode = True
import argparse, contextlib, datetime, hashlib, json, os, pathlib, plistlib, re, stat, struct, uuid, zipfile

REPOSITORY = 'Jones-Systems/Jones-Code'
MAX_FILE = 2 * 1024 * 1024 * 1024
CANDIDATE_FIELDS = ('schema', 'repository', 'source', 'tree', 'installedSource', 'workflow', 'workflowId', 'runId', 'runAttempt', 'ciRunId', 'artifactId', 'artifactName', 'artifactDigest', 'artifactBytes', 'expiresAt', 'version', 'platform', 'architecture')

class Refused(Exception):
    pass

def require(condition):
    if not condition: raise Refused('Bootstrap input or retained output requires reconciliation.')

def encoded(value):
    return json.dumps(value, ensure_ascii=False, separators=(',', ':'), allow_nan=False).encode('utf8')

def sha(raw):
    return hashlib.sha256(raw).hexdigest()

def hex_value(value, length):
    return isinstance(value, str) and re.fullmatch('[a-f0-9]{' + str(length) + '}', value) is not None

def integer(value, minimum=1):
    return type(value) is int and minimum <= value <= 9007199254740991

def object_pairs(pairs):
    result = {}
    for key, value in pairs:
        require(key not in result)
        result[key] = value
    return result

def decode(raw):
    value = json.loads(raw, object_pairs_hook=object_pairs,
                       parse_constant=lambda value: (_ for _ in ()).throw(Refused('Non-finite metadata.')))
    require(isinstance(value, dict))
    return value

def absolute(value):
    require(isinstance(value, str) and value.startswith('/') and '\x00' not in value and os.path.normpath(value) == value)
    return pathlib.Path(value)

def identity(info, directory=False):
    common = (info.st_dev, info.st_ino, info.st_uid, info.st_mode)
    return common if directory else common + (info.st_nlink, info.st_size, info.st_mtime_ns, info.st_ctime_ns)

class Observation:
    def __init__(self):
        self.files = {}
        self.directories = {}
        self.app_directories = {}
        self.links = {}
        self.denied = ()

    def permitted(self, path):
        require(not any(path == root or path.is_relative_to(root) for root in self.denied))

    @contextlib.contextmanager
    def directory(self, path):
        path = absolute(str(path))
        self.permitted(path)
        fd = os.open('/', os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
        try:
            for part in path.parts[1:]:
                child = os.open(part, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW, dir_fd=fd)
                os.close(fd)
                fd = child
            info = os.fstat(fd)
            require(stat.S_ISDIR(info.st_mode) and info.st_uid == os.getuid() and info.st_mode & 0o022 == 0)
            proof = identity(info, True)
            require(path not in self.directories or self.directories[path] == proof)
            self.directories[path] = proof
            yield fd
            require(identity(os.fstat(fd), True) == proof)
        finally:
            os.close(fd)

    @contextlib.contextmanager
    def file(self, path, limit):
        path = absolute(str(path))
        self.permitted(path)
        with self.directory(path.parent) as parent:
            fd = os.open(path.name, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK, dir_fd=parent)
            with os.fdopen(fd, 'rb') as stream:
                info = os.fstat(stream.fileno())
                require(stat.S_ISREG(info.st_mode) and info.st_uid == os.getuid() and info.st_nlink == 1 and info.st_mode & 0o022 == 0 and info.st_size <= limit)
                proof = identity(info)
                require(path not in self.files or self.files[path] == proof)
                self.files[path] = proof
                yield stream
                require(identity(os.fstat(stream.fileno())) == proof)
                require(identity(os.stat(path.name, dir_fd=parent, follow_symlinks=False)) == proof)

    def raw(self, path, limit=131072):
        with self.file(path, limit) as stream:
            raw = stream.read(limit + 1)
            require(len(raw) <= limit)
            return raw

    def metadata(self, path, limit=131072):
        raw = self.raw(path, limit)
        return decode(raw), sha(raw)

    def digest(self, path, limit=MAX_FILE):
        digest = hashlib.sha256()
        with self.file(path, limit) as stream:
            for block in iter(lambda: stream.read(1024 * 1024), b''):
                digest.update(block)
        return digest.hexdigest()

    def current(self):
        for path, proof in list(self.directories.items()):
            with self.directory(path) as fd:
                require(identity(os.fstat(fd), True) == proof)
        for path, names in self.app_directories.items():
            with self.directory(path) as fd: require(sorted(os.listdir(fd)) == names)
        for path, (proof, target) in self.links.items():
            with self.directory(path.parent) as parent:
                require(identity(os.stat(path.name, dir_fd=parent, follow_symlinks=False)) == proof and os.readlink(path.name, dir_fd=parent) == target)
        for path, proof in list(self.files.items()):
            with self.directory(path.parent) as parent:
                require(identity(os.stat(path.name, dir_fd=parent, follow_symlinks=False)) == proof)

def internal_link(observed, root, path):
    pending, resolved, links = list(path.relative_to(root).parts), [], 0
    while pending:
        name = pending.pop(0)
        if name in ('', '.'): continue
        if name == '..':
            require(bool(resolved))
            resolved.pop()
            continue
        parent = root.joinpath(*resolved)
        current = parent / name
        observed.permitted(current)
        with observed.directory(parent) as fd:
            info = os.stat(name, dir_fd=fd, follow_symlinks=False)
            if stat.S_ISLNK(info.st_mode):
                links += 1
                require(links <= 40)
                target = os.readlink(name, dir_fd=fd)
                proof = (identity(info), target)
                require(current not in observed.links or observed.links[current] == proof)
                observed.links[current] = proof
                parts = list(pathlib.PurePosixPath(target).parts)
                if target.startswith('/'):
                    require(tuple(parts[:len(root.parts)]) == root.parts)
                    parts, resolved = parts[len(root.parts):], []
                pending = parts + pending
            else:
                require(not pending or stat.S_ISDIR(info.st_mode))
                resolved.append(name)

def app_digest(observed, root):
    root = absolute(str(root))
    entries = []
    pending = [root]
    while pending:
        directory = pending.pop()
        with observed.directory(directory) as fd:
            names = sorted(os.listdir(fd))
            require(directory not in observed.app_directories or observed.app_directories[directory] == names)
            observed.app_directories[directory] = names
            for name in names:
                path = directory / name
                observed.permitted(path)
                info = os.stat(name, dir_fd=fd, follow_symlinks=False)
                require(info.st_uid == os.getuid())
                entries.append((path, info))
                require(len(entries) <= 200000)
                if stat.S_ISDIR(info.st_mode): pending.append(path)
    digest, total = hashlib.sha256(), 0
    for path, info in sorted(entries, key=lambda item: item[0].relative_to(root).as_posix().encode('utf8')):
        relative = path.relative_to(root).as_posix()
        if stat.S_ISLNK(info.st_mode):
            target = os.readlink(path)
            internal_link(observed, root, path)
            require(identity(path.lstat()) == identity(info))
            observed.links[path] = (identity(info), target)
            record = ['link', relative, target]
        elif stat.S_ISREG(info.st_mode):
            total += info.st_size
            require(total <= 8 * 1024 * 1024 * 1024)
            record = ['file', relative, info.st_mode & 0o777, observed.digest(path, 8 * 1024 * 1024 * 1024)]
            require(observed.files[path] == identity(info))
        elif stat.S_ISDIR(info.st_mode):
            record = ['directory', relative, info.st_mode & 0o777]
        else:
            raise Refused('Unsupported app payload entry.')
        digest.update(encoded(record) + b'\n')
    return digest.hexdigest()

def app_metadata(observed, app):
    archive = app / 'Contents/Resources/app.asar'
    with observed.file(archive, MAX_FILE) as stream:
        prefix = stream.read(16)
        require(len(prefix) == 16)
        size, header_size, _, json_size = struct.unpack('<IIII', prefix)
        require(size == 4 and 8 <= header_size <= 16 * 1024 * 1024 and json_size <= header_size - 8)
        header = decode(stream.read(json_size))
        entry = header['files']['package.json']
        offset, length = entry.get('offset'), entry.get('size')
        require(entry.get('unpacked') is not True and 'link' not in entry and isinstance(offset, str) and offset.isascii() and offset.isdigit() and integer(length) and length <= 1048576)
        position = 8 + header_size + int(offset)
        require(position + length <= os.fstat(stream.fileno()).st_size)
        stream.seek(position)
        raw = stream.read(length)
        require(len(raw) == length)
        return decode(raw)

def bundle_identity(observed, app, expected):
    info = plistlib.loads(observed.raw(app / 'Contents/Info.plist', 1048576))
    require(isinstance(info, dict))
    executable = info.get('CFBundleExecutable')
    require(isinstance(executable, str) and re.fullmatch('[^/\\\\]+', executable) is not None and executable not in ('.', '..'))
    require(str(app / 'Contents/MacOS' / executable) == expected['executablePath'])
    require(info.get('CFBundleShortVersionString') == expected['version'])
    require(isinstance(info.get('CFBundleIdentifier'), str) and bool(info['CFBundleIdentifier'].strip()))
    if 'bundleIdentifier' in expected: require(info['CFBundleIdentifier'] == expected['bundleIdentifier'])
    metadata = app_metadata(observed, app)
    require(metadata.get('version') == expected['version'] and metadata.get('jonesSource') == {'repository': REPOSITORY, 'sha': expected['sourceSha'], 'tree': expected['sourceTree']})
    return metadata

def stage_directory(observed, root, handle, value):
    path = absolute(value)
    parts = path.relative_to(root).parts
    require(parts == (handle,) or (len(parts) == 3 and parts[0] == 'attempts' and re.fullmatch('stage-[a-zA-Z0-9]+', parts[1]) is not None and parts[2] == handle))
    with observed.directory(path): pass
    return path

def validate_candidate(candidate):
    require(set(candidate) == set(CANDIDATE_FIELDS))
    require(type(candidate['schema']) is int and candidate['schema'] == 1 and candidate['repository'] == REPOSITORY)
    require(candidate['platform'] == 'darwin' and candidate['architecture'] == 'arm64' and candidate['workflow'] == '.github/workflows/artifact-desktop-mac.yml')
    require(all(hex_value(candidate[key], 40) for key in ('source', 'tree', 'installedSource')) and candidate['source'] != candidate['installedSource'])
    require(hex_value(candidate['artifactDigest'], 64))
    require(all(integer(candidate[key]) for key in ('workflowId', 'runId', 'runAttempt', 'ciRunId', 'artifactId', 'artifactBytes')) and candidate['artifactBytes'] <= MAX_FILE)
    version = candidate['version']
    require(isinstance(version, str) and len(version) <= 128 and re.fullmatch(r'\d+\.\d+\.\d+-preview\.\d{8}\.\d+(?:\.\d+)?', version) is not None)
    require(version.endswith('.' + str(candidate['runId']) + '.' + str(candidate['runAttempt'])))
    require(candidate['artifactName'] == 'desktop-mac-arm64-' + str(candidate['runId']) + '-' + str(candidate['runAttempt']) + '--' + version)
    require(isinstance(candidate['expiresAt'], str))
    datetime.datetime.fromisoformat(candidate['expiresAt'].replace('Z', '+00:00'))
    return sha(encoded([candidate[key] for key in CANDIDATE_FIELDS]))

def artifact_receipt(receipt, candidate):
    wanted = {key: candidate[key] for key in ('repository', 'source', 'tree', 'workflow', 'version', 'platform', 'architecture')}
    wanted.update(schema=1, event='push', ref='refs/heads/main', runId=str(candidate['runId']), runAttempt=str(candidate['runAttempt']), artifact='T3-Code-' + candidate['version'] + '-arm64.dmg')
    require(type(receipt.get('schema')) is int and all(receipt.get(key) == value for key, value in wanted.items()) and hex_value(receipt.get('sha256'), 64))

def validate_artifact(observed, directory):
    artifact, receipt_digest = observed.metadata(directory / 'jones-actions-stage.json')
    candidate, receipt = artifact['candidate'], artifact['receipt']
    handle = validate_candidate(candidate)
    artifact_receipt(receipt, candidate)
    require(type(artifact.get('schema')) is int and artifact['schema'] == 1 and artifact.get('source') == 'jones-actions' and artifact.get('channel') == 'jones-main' and artifact.get('stagedHandle') == handle)
    require(directory.name == handle and artifact.get('payloadPath') == str(directory / receipt['artifact']))
    require(observed.digest(directory / receipt['artifact']) == receipt['sha256'])
    archive = directory / 'github-artifact.zip'
    require(observed.digest(archive) == candidate['artifactDigest'] and observed.files[archive][5] == candidate['artifactBytes'])
    with observed.file(archive, MAX_FILE) as stream, zipfile.ZipFile(stream) as packed:
        entries = packed.infolist()
        require(1 <= len(entries) <= 8 and len({entry.filename for entry in entries}) == len(entries))
        require(sum(entry.file_size for entry in entries) <= MAX_FILE)
        for entry in entries:
            require(re.fullmatch('[A-Za-z0-9][A-Za-z0-9._-]{0,240}', entry.filename) is not None and not entry.flag_bits & 1 and entry.compress_type in (0, 8) and not entry.external_attr & 0x10)
            mode = entry.external_attr >> 16
            require(stat.S_IFMT(mode) in (0, stat.S_IFREG))
        entry = packed.getinfo('ARTIFACT.json')
        require(entry.file_size <= 65536 and entry.compress_size <= 131072)
        with packed.open(entry) as source:
            raw = source.read(65537)
            require(len(raw) == entry.file_size)
        original = decode(raw)
        artifact_receipt(original, candidate)
        require(original['sha256'] == receipt['sha256'])
    return artifact, receipt_digest

def transactions(observed, root, home):
    try:
        with observed.directory(root) as fd: names = sorted(os.listdir(fd))
    except FileNotFoundError:
        return None
    require(len(names) <= 1000)
    for name in names:
        require(hex_value(name, 64))
        directory = root / name
        with observed.directory(directory) as fd: children = set(os.listdir(fd))
        require(not ('retirement-intent.json' in children and 'retirement-receipt.json' not in children))
        if 'journal.json' in children:
            journal, _ = observed.metadata(directory / 'journal.json')
            intent, _ = observed.metadata(directory / 'intent.json')
            require(journal.get('intent') == intent and intent.get('transactionId') == name and type(intent.get('protocol')) is int and intent['protocol'] == 1 and intent.get('expected', {}).get('home') == home and journal.get('phase') in ('resumed', 'rolled-back'))
        else:
            raise Refused('A retained native transaction has no verified terminal journal.')
    return names

# Kept equivalent to the worker's preparation proof contract without importing
# its process or state machinery into this standalone, nonexecuting command.
_BOOTSTRAP_PREPARATION_NAME = 'jones-offline-preparation'
_BOOTSTRAP_RUNTIME_BYTES = 256 * 1024 * 1024
_BOOTSTRAP_BUNDLE_KEYS = {'schema', 'repository', 'sourceSha', 'sourceTree', 'version', 'bootstrapSha256', 'executorSha256'}
_BOOTSTRAP_BUNDLE_V2_KEYS = _BOOTSTRAP_BUNDLE_KEYS | {'maintenanceSha256', 'identityNativeSha256', 'identityNativeSourceSha256', 'nativeArchitecture'}
_BOOTSTRAP_PREPARATION_SOURCE_INPUTS = {'pnpm-lock.yaml', 'pnpm-workspace.yaml', 'package.json',
    'apps/server/package.json', 'apps/desktop/package.json', 'apps/web/package.json',
    'packages/contracts/package.json', 'apps/server/vite.config.ts', 'vite.config.ts',
    'scripts/jones/updates/offline-preparation-build.ts',
    'apps/server/src/jones/updates/offlinePreparationEntry.ts',
    'apps/server/src/jones/updates/offlinePreparation.ts',
    'apps/server/src/jones/updates/offlinePreparationCore.ts',
    'apps/server/src/jones/updates/offlinePreparationPlan.ts',
    'apps/server/src/jones/updates/offlinePreparationWorker.ts'}

def _bootstrap_preparation_bundle(bundle):
    require(set(bundle) == _BOOTSTRAP_BUNDLE_V2_KEYS | {'preparation'} and type(bundle['schema']) is int and bundle['schema'] == 3)
    require(bundle['repository'] == 'Jones-Systems/Jones-Code' and hex_value(bundle['sourceSha'], 40) and hex_value(bundle['sourceTree'], 40))
    require(isinstance(bundle['version'], str) and len(bundle['version']) <= 128 and re.fullmatch('[0-9]+\.[0-9]+\.[0-9]+-preview\.[0-9]{8}\.[0-9]+(?:\.[0-9]+)?', bundle['version']) is not None)
    require(all(hex_value(bundle[key], 64) for key in ('bootstrapSha256', 'executorSha256', 'maintenanceSha256', 'identityNativeSha256', 'identityNativeSourceSha256')))
    value = bundle['preparation']
    require(isinstance(value, dict) and set(value) == {'schema', 'protocol', 'provenanceSha256', 'sourceInputHashes', 'entry', 'seaConfig', 'executable', 'node', 'sqlite'})
    require(type(value['schema']) is int and value['schema'] == 1 and type(value['protocol']) is int and value['protocol'] == 1)
    require(hex_value(value['provenanceSha256'], 64))
    inputs = value['sourceInputHashes']
    require(isinstance(inputs, dict) and set(inputs) == _BOOTSTRAP_PREPARATION_SOURCE_INPUTS)
    require(all(hex_value(digest, 64) for digest in inputs.values()))
    for key, maximum in (('entry', 32 * 1024 * 1024), ('seaConfig', 16384), ('executable', 256 * 1024 * 1024)):
        item = value[key]
        require(isinstance(item, dict) and set(item) == ({'sha256', 'size', 'name'} if key == 'executable' else {'sha256', 'size'}))
        require(hex_value(item['sha256'], 64) and integer(item['size']) and item['size'] <= maximum)
    require(value['executable']['name'] == _BOOTSTRAP_PREPARATION_NAME)
    config = b'{"main":"offlinePreparationEntry.mjs","mainFormat":"module","executable":"node","output":"jones-offline-preparation","disableExperimentalSEAWarning":true,"useSnapshot":false,"useCodeCache":false,"execArgv":[],"execArgvExtension":"none"}\n'
    require(value['seaConfig'] == {'sha256': sha(config), 'size': len(config)})
    node = value['node']
    require(isinstance(node, dict) and set(node) == {'version', 'platform', 'architecture', 'archiveSha256', 'archiveBytes', 'checksumsSha256', 'runtimeSha256', 'runtimeBytes'})
    require(node['version'] == '26.8.2' and node['platform'] == 'darwin' and node['architecture'] == bundle['nativeArchitecture'] == 'arm64')
    require(all(hex_value(node[key], 64) for key in ('archiveSha256', 'checksumsSha256', 'runtimeSha256')))
    require(integer(node['archiveBytes']) and node['archiveBytes'] <= 128 * 1024 * 1024 and integer(node['runtimeBytes']) and node['runtimeBytes'] <= 256 * 1024 * 1024)
    sqlite = value['sqlite']
    require(isinstance(sqlite, dict) and set(sqlite) == {'version', 'sourceId'})
    require(isinstance(sqlite['version'], str) and len(sqlite['version']) <= 11 and re.fullmatch('(?:0|[1-9][0-9]{0,2})\.(?:0|[1-9][0-9]{0,2})\.(?:0|[1-9][0-9]{0,2})', sqlite['version']) is not None)
    require(isinstance(sqlite['sourceId'], str) and len(sqlite['sourceId']) == 84 and re.fullmatch('[0-9]{4}-[0-9]{2}-[0-9]{2} [0-9]{2}:[0-9]{2}:[0-9]{2} [a-f0-9]{64}', sqlite['sourceId']) is not None)
    parts = [int(part) for part in re.split('[- :]', sqlite['sourceId'][:19])]
    # ECMAScript's canonical source timestamp also admits astronomical year zero.
    try: datetime.datetime(parts[0] or 400, *parts[1:])
    except ValueError: raise Refused('Preparation SQLite source timestamp is invalid.')
    return value

def _bootstrap_preparation_runtime(observed, path, proof):
    digest, count = hashlib.sha256(), 0
    with observed.file(path, _BOOTSTRAP_RUNTIME_BYTES) as stream:
        info = os.fstat(stream.fileno())
        require(info.st_size == proof['size'] and stat.S_IMODE(info.st_mode) == 0o500)
        while True:
            block = stream.read(1048576)
            if not block: break
            if count == 0: require(len(block) >= 32 and block[:16] == bytes.fromhex('cffaedfe0c0000010000000002000000'))
            count += len(block)
            require(count <= proof['size'] <= _BOOTSTRAP_RUNTIME_BYTES)
            digest.update(block)
    require(count == proof['size'] and digest.hexdigest() == proof['sha256'])

def _bootstrap_bundle(observed, tools, candidate):
    bundle, bundle_digest = observed.metadata(tools / 'BUNDLE.json', 65536)
    require(type(bundle.get('schema')) is int and bundle['schema'] in (1, 2, 3))
    if bundle['schema'] == 3:
        preparation = _bootstrap_preparation_bundle(bundle)
        require(sha(encoded(bundle) + b'\n') == bundle_digest)
        expected = {'bootstrap.py', 'executor.py', 'BUNDLE.json', 'maintenance.py', 'identity-native', _BOOTSTRAP_PREPARATION_NAME}
        with observed.directory(tools) as fd:
            names = []
            with os.scandir(fd) as entries:
                for entry in entries:
                    require(len(names) < len(expected) and entry.name in expected)
                    names.append(entry.name)
        names.sort()
        require(set(names) == expected and (tools not in observed.app_directories or observed.app_directories[tools] == names))
        observed.app_directories[tools] = names
        require(stat.S_IMODE(observed.files[tools / 'BUNDLE.json'][3]) == 0o400)
        for name in ('bootstrap.py', 'executor.py', 'maintenance.py', 'identity-native'):
            with observed.file(tools / name, 4 * 1024 * 1024 if name == 'identity-native' else 1048576) as stream:
                require(stat.S_IMODE(os.fstat(stream.fileno()).st_mode) == 0o500)
    else:
        require(observed.files[tools / 'BUNDLE.json'][5] <= 8192)
        require(set(bundle) == (_BOOTSTRAP_BUNDLE_KEYS if bundle['schema'] == 1 else _BOOTSTRAP_BUNDLE_V2_KEYS))
    require(bundle['repository'] == REPOSITORY and bundle['sourceSha'] == candidate['source'] and bundle['sourceTree'] == candidate['tree'] and bundle['version'] == candidate['version'])
    if bundle['schema'] in (2, 3):
        require(bundle['nativeArchitecture'] == 'arm64' and all(hex_value(bundle[key], 64) for key in ('maintenanceSha256', 'identityNativeSha256', 'identityNativeSourceSha256')))
        require(sha(observed.raw(tools / 'maintenance.py', 1048576)) == bundle['maintenanceSha256'])
        native = observed.raw(tools / 'identity-native', 4 * 1024 * 1024)
        require(sha(native) == bundle['identityNativeSha256'])
        require(len(native) >= 32 and native[:16] == bytes.fromhex('cffaedfe0c0000010000000002000000'))
        require(observed.files[tools / 'maintenance.py'][3] & 0o777 == 0o500 and observed.files[tools / 'identity-native'][3] & 0o777 == 0o500)
    if bundle['schema'] == 3:
        _bootstrap_preparation_runtime(observed, tools / _BOOTSTRAP_PREPARATION_NAME, preparation['executable'])
    return bundle, bundle_digest

def inspect_inputs(manifest, selection_path, selection_sha, bootstrap_path):
    observed = Observation()
    active, active_digest = observed.metadata(manifest)
    require(type(active.get('protocol')) is int and active['protocol'] == 1 and active.get('owner') == 'desktop')
    require(all(isinstance(active.get(key), str) and bool(active[key]) for key in ('generation', 'transactionId', 'home', 'databasePath', 'profile', 'environmentId', 'appPath', 'executablePath', 'version', 'sourceSha', 'sourceTree')))
    require(hex_value(active['sourceSha'], 40) and hex_value(active['sourceTree'], 40))
    home = absolute(active['home'])
    # State paths are binding strings only; even an existence probe is unnecessary here.
    observed.denied = (absolute(active['databasePath']).parent, absolute(active['profile']))
    require(manifest == home / 'runtime/jones-active-install.json')
    updater = manifest.parent / 'jones-updates'
    require(selection_path.parent == updater / 'staging' and selection_path.name.startswith(active['sourceSha'] + '-') and selection_path.name.endswith('.json') and hex_value(selection_sha, 64))
    selection, selected_digest = observed.metadata(selection_path)
    require(selected_digest == selection_sha and type(selection.get('schema')) is int and selection['schema'] == 1)
    require(all(selection.get(key) == value for key, value in {'source': 'jones-actions', 'home': active['home'], 'profile': active['profile'], 'currentVersion': active['version'], 'installedSource': active['sourceSha'], 'active': active}.items()))
    app = selection['app']
    require(isinstance(app, dict) and hex_value(app.get('handle'), 64))
    candidate_app = absolute(app['appPath'])
    app_directory = stage_directory(observed, updater / 'apps', app['handle'], str(candidate_app.parent))
    artifact_directory = stage_directory(observed, updater / 'artifacts', app['handle'], selection['artifactDirectory'])
    artifact, artifact_digest = validate_artifact(observed, artifact_directory)
    candidate = artifact['candidate']
    require(artifact['stagedHandle'] == app['handle'] and candidate['installedSource'] == active['sourceSha'])
    require(all(app.get(key) == candidate[source] for key, source in (('sourceSha', 'source'), ('sourceTree', 'tree'), ('version', 'version'))))
    require(app.get('receiptPath') == str(app_directory / 'mac-app-receipt.json'))
    mac_receipt, mac_digest = observed.metadata(app_directory / 'mac-app-receipt.json')
    require(mac_receipt == {'app': app, 'artifact': artifact, 'candidate': candidate})
    require(type(app.get('startupGateProtocol')) is int and app['startupGateProtocol'] == 1)
    metadata = bundle_identity(observed, candidate_app, app)
    require(type(metadata.get('startupGateProtocol')) is int and metadata['startupGateProtocol'] == 1)
    require(app_digest(observed, candidate_app) == app['appDigest'])
    require(observed.digest(candidate_app / 'Contents/Resources/app.asar') == app['asarDigest'] and observed.digest(absolute(app['executablePath'])) == app['executableDigest'])
    installed_app = absolute(active['appPath'])
    require(installed_app != candidate_app and installed_app.name.endswith('.app'))
    bundle_identity(observed, installed_app, active)
    installed_digest = app_digest(observed, installed_app)
    if 'appDigest' in active: require(active['appDigest'] == installed_digest)
    tools = candidate_app / 'Contents/Resources/jones-bootstrap'
    require(bootstrap_path == tools / 'bootstrap.py')
    bundle, bundle_digest = _bootstrap_bundle(observed, tools, candidate)
    bootstrap = observed.raw(bootstrap_path, 1048576)
    executor = observed.raw(tools / 'executor.py', 1048576)
    require(sha(bootstrap) == bundle['bootstrapSha256'] and sha(executor) == bundle['executorSha256'])
    transaction_names = transactions(observed, updater / 'transactions', active['home'])
    binding = {'schema': 1, 'manifestPath': str(manifest), 'manifestSha256': active_digest,
               'selectionPath': str(selection_path), 'selectionSha256': selection_sha,
               'artifactReceiptSha256': artifact_digest, 'macAppReceiptSha256': mac_digest,
               'bundleSha256': bundle_digest, 'installedGeneration': active['generation'],
               'installedSource': active['sourceSha'], 'installedAppDigest': installed_digest,
               'stagedHandle': app['handle'], 'sourceSha': candidate['source'], 'sourceTree': candidate['tree'],
               'appDigest': app['appDigest'], 'executorSha256': bundle['executorSha256']}
    binding_digest = sha(encoded(binding))
    output = updater / 'bootstrap-executors' / binding_digest
    receipt = dict(binding, bindingDigest=binding_digest, executorPath=str(output / 'executor.py'),
                   status='executor-ready', activationAllowed=False, observationOnly=True,
                   blocker='DESK-FIRST-01', nextRequirement='qualified-maintenance-admission')
    return observed, active, updater, transaction_names, output, executor, receipt

def verify_and_sync_output(observed, output, executor, receipt):
    with observed.directory(output) as fd:
        require(set(os.listdir(fd)) == {'executor.py', 'receipt.json'})
    require(observed.raw(output / 'executor.py', 1048576) == executor)
    require(observed.files[output / 'executor.py'][3] & 0o777 == 0o500)
    actual, _ = observed.metadata(output / 'receipt.json')
    require(actual == receipt)
    # A prior invocation may have linked the receipt before its directory sync failed.
    for name in ('executor.py', 'receipt.json'):
        with observed.file(output / name, 1048576) as stream:
            os.fsync(stream.fileno())
    with observed.directory(output) as fd:
        require(set(os.listdir(fd)) == {'executor.py', 'receipt.json'})
        os.fsync(fd)
    with observed.directory(output.parent) as fd:
        os.fsync(fd)

def ensure_directory(observed, parent, name):
    with observed.directory(parent) as fd:
        try:
            os.mkdir(name, 0o700, dir_fd=fd)
        except FileExistsError:
            pass
        os.fsync(fd)
    path = parent / name
    with observed.directory(path): pass
    return path

def stage_executor(observed, active, updater, transaction_names, output, executor, receipt):
    parent = ensure_directory(observed, updater, 'bootstrap-executors')
    with observed.directory(parent) as parent_fd:
        try:
            os.mkdir(output.name, 0o700, dir_fd=parent_fd)
            os.fsync(parent_fd)
        except FileExistsError:
            verify_and_sync_output(observed, output, executor, receipt)
            observed.current()
            require(transactions(observed, updater / 'transactions', active['home']) == transaction_names)
            return 'no-change'
    with observed.directory(output) as fd:
        helper = os.open('executor.py', os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o500, dir_fd=fd)
        with os.fdopen(helper, 'wb') as stream:
            os.fchmod(stream.fileno(), 0o500)
            stream.write(executor)
            stream.flush()
            os.fsync(stream.fileno())
        os.fsync(fd)
        observed.current()
        require(transactions(observed, updater / 'transactions', active['home']) == transaction_names)
        # An interrupted directory is retained and refused, never silently adopted.
        temporary = '.receipt-' + uuid.uuid4().hex
        pending = os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600, dir_fd=fd)
        try:
            with os.fdopen(pending, 'wb') as stream:
                stream.write(encoded(receipt) + b'\n')
                stream.flush()
                os.fsync(stream.fileno())
            os.link(temporary, 'receipt.json', src_dir_fd=fd, dst_dir_fd=fd, follow_symlinks=False)
        finally:
            os.unlink(temporary, dir_fd=fd)
        os.fsync(fd)
    verify_and_sync_output(observed, output, executor, receipt)
    observed.current()
    return 'created'

# Inventory is an observation of fixed installer path families, never admission
# evidence. In particular, old helper births and orphan sibling apps are unproved.
INVENTORY_LIMITS = {'directoryEntries': 4096, 'visits': 200000, 'depth': 32,
                    'apps': 32, 'records': 8192, 'readBytes': 64 * 1024 * 1024}

class InventoryLimit(Exception):
    pass

class InventoryReader:
    def __init__(self, stream, inventory):
        self.stream, self.inventory = stream, inventory
    def fileno(self): return self.stream.fileno()
    def seek(self, *args): return self.stream.seek(*args)
    def read(self, size=-1):
        remaining = max(0, os.fstat(self.stream.fileno()).st_size - self.stream.tell())
        requested = remaining if size < 0 else min(size, remaining)
        if requested > INVENTORY_LIMITS['readBytes'] - self.inventory.read_bytes:
            raise InventoryLimit()
        result = self.stream.read(requested)
        self.inventory.read_bytes += len(result)
        return result

class InventoryObservation(Observation):
    def __init__(self, inventory):
        super().__init__()
        self.inventory = inventory
    def permitted(self, path):
        super().permitted(path)
        parts = path.parts
        for index, name in enumerate(parts):
            if name != 'jones-updates': continue
            suffix = parts[index + 1:]
            require(not (len(suffix) >= 3 and suffix[0] == 'transactions' and suffix[2] in ('previous-pair', 'advanced-state')))
            if suffix[:1] != ('apps',): continue
            if len(suffix) >= 3 and hex_value(suffix[1], 64): require(suffix[2] != 'mount')
            if len(suffix) >= 5 and suffix[1] == 'attempts' and re.fullmatch('stage-[a-zA-Z0-9]+', suffix[2]) is not None and hex_value(suffix[3], 64):
                require(suffix[4] != 'mount')
    @contextlib.contextmanager
    def file(self, path, limit):
        with super().file(path, limit) as stream:
            yield InventoryReader(stream, self.inventory)

class RouteInventory:
    def __init__(self, manifest):
        self.report = {'schema': 1, 'operation': 'inventory-routes', 'status': 'observed',
                       'observationOnly': True, 'activationAllowed': False,
                       'launchMembershipComplete': False, 'processCustody': 'unobserved',
                       'blocker': 'DESK-FIRST-01', 'manifestPath': str(manifest),
                       'coverage': {'history': 'partial', 'orphanSiblingApps': 'unobserved',
                                    'manualCopies': 'not-enumerated', 'interpreterIdentity': 'unobserved',
                                    'payloadDigests': 'not-verified'},
                       'observations': [], 'issues': []}
        self.observed = InventoryObservation(self)
        self.visits = self.read_bytes = 0
        self.limited = False
        self.listings, self.nodes = {}, {}
        self.absent, self.apps, self.emitted = set(), set(), set()
        self.manifest = manifest

    def visit(self):
        if self.limited: raise InventoryLimit()
        self.visits += 1
        if self.visits > INVENTORY_LIMITS['visits']: raise InventoryLimit()

    def room(self):
        if self.limited: raise InventoryLimit()
        # Reserve one record for the terminal budget diagnostic.
        if len(self.report['observations']) + len(self.report['issues']) >= INVENTORY_LIMITS['records'] - 1:
            raise InventoryLimit()

    def issue(self, code, path, partial=False):
        self.room()
        self.report['issues'].append({'code': code, 'path': str(path)})
        if partial: self.report['status'] = 'partial'

    def add(self, kind, path, binding='unknown', info=None, **fields):
        key = kind, str(path)
        if key in self.emitted: return
        self.room()
        self.emitted.add(key)
        value = dict(kind=kind, path=str(path), binding=binding, **fields)
        if info is not None:
            value['identity'] = {'device': str(info.st_dev), 'inode': str(info.st_ino),
                                 'uid': info.st_uid, 'mode': stat.S_IMODE(info.st_mode),
                                 'nlink': info.st_nlink, 'size': info.st_size,
                                 'mtimeNs': str(info.st_mtime_ns), 'ctimeNs': str(info.st_ctime_ns)}
        self.report['observations'].append(value)

    def limit(self, path):
        if not self.limited:
            self.limited = True
            self.report['status'] = 'partial'
            self.report['issues'].append({'code': 'inventory-limit', 'path': str(path)})

    def attempt(self, path, operation):
        if self.limited: return None
        try:
            return operation()
        except InventoryLimit:
            self.limit(path)
        except (Refused, OSError, ValueError, KeyError, TypeError, OverflowError, RuntimeError, struct.error):
            try: self.issue('unreadable-or-changed', path, True)
            except InventoryLimit: self.limit(path)
        return None

    def names(self, path, optional=False, checking=False):
        if self.limited: raise InventoryLimit()
        try:
            with self.observed.directory(path) as fd:
                names = []
                with os.scandir(fd) as entries:
                    for entry in entries:
                        self.visit()
                        if len(names) >= INVENTORY_LIMITS['directoryEntries']: raise InventoryLimit()
                        names.append(entry.name)
                names.sort()
        except FileNotFoundError:
            if not optional: raise
            self.absent.add(path)
            self.add('absent', path)
            return None
        if checking:
            require(self.listings[path] == names)
        else:
            require(path not in self.listings or self.listings[path] == names)
            self.listings[path] = names
        return names

    def node(self, path):
        self.visit()
        self.observed.permitted(path)
        with self.observed.directory(path.parent) as fd:
            info = os.stat(path.name, dir_fd=fd, follow_symlinks=False)
            proof = identity(info)
            require(path not in self.nodes or self.nodes[path] == proof)
            self.nodes[path] = proof
            if stat.S_ISLNK(info.st_mode):
                target = os.readlink(path.name, dir_fd=fd)
                self.add('link', path, info=info, target=target)
                self.issue('unresolved-link', path)
                return None
            require(info.st_uid == os.getuid())
            return info

    def record(self, path):
        value, digest = self.observed.metadata(path)
        self.add('record', path, sha256=digest)
        return value

    def active_shape(self, value):
        require(isinstance(value, dict))
        require(type(value.get('protocol')) is int and value['protocol'] == 1 and value.get('owner') == 'desktop')
        for name in ('home', 'databasePath', 'profile', 'appPath', 'executablePath'):
            require(isinstance(value.get(name), str) and len(value[name]) <= 4096)
            absolute(value[name])
        require(isinstance(value.get('environmentId'), str) and 0 < len(value['environmentId']) <= 512)
        require(isinstance(value.get('version'), str) and 0 < len(value['version']) <= 128)
        require(isinstance(value.get('generation'), str) and re.fullmatch('[A-Za-z0-9_-]{1,128}', value['generation']) is not None)
        require(value.get('transactionId') == 'bootstrap' or hex_value(value.get('transactionId'), 64))
        require(hex_value(value.get('sourceSha'), 40) and hex_value(value.get('sourceTree'), 40))
        app, executable = absolute(value['appPath']), absolute(value['executablePath'])
        require(app.name.endswith('.app') and executable.parent == app / 'Contents/MacOS')
        require(executable.name not in ('.', '..'))
        return value

    def stage_path(self, root, handle, directory):
        require(hex_value(handle, 64))
        parts = absolute(str(directory)).relative_to(root).parts
        require(parts == (handle,) or (len(parts) == 3 and parts[0] == 'attempts' and
                re.fullmatch('stage-[a-zA-Z0-9]+', parts[1]) is not None and parts[2] == handle))

    def staged_shape(self, value):
        require(isinstance(value, dict))
        app = absolute(value['appPath'])
        self.stage_path(self.updater / 'apps', value['handle'], app.parent)
        require(app.name.endswith('.app') and all(hex_value(value.get(key), 64) for key in ('appDigest', 'asarDigest', 'executableDigest')))
        if 'startupGateProtocol' in value: require(type(value['startupGateProtocol']) is int and value['startupGateProtocol'] == 1)
        require(value.get('receiptPath') == str(app.parent / 'mac-app-receipt.json'))
        require(absolute(value['executablePath']).parent == app / 'Contents/MacOS')
        require(hex_value(value.get('sourceSha'), 40) and hex_value(value.get('sourceTree'), 40))
        require(isinstance(value.get('version'), str) and 0 < len(value['version']) <= 128)
        return value

    def anchor(self):
        require(self.manifest.name == 'jones-active-install.json' and self.manifest.parent.name == 'runtime')
        active, digest = self.observed.metadata(self.manifest)
        self.active = self.active_shape(active)
        self.home = absolute(active['home'])
        require(self.manifest == self.home / 'runtime/jones-active-install.json')
        self.updater = self.manifest.parent / 'jones-updates'
        self.observed.denied = (absolute(active['databasePath']).parent, absolute(active['profile']))
        for path in (self.manifest, self.updater, absolute(active['appPath']), self.home / 'bin/t3'):
            self.observed.permitted(path)
            require(not any(root.is_relative_to(path) for root in self.observed.denied))
        self.report['manifestSha256'] = digest
        self.add('record', self.manifest, 'manifest', sha256=digest)

    def app(self, path, role, expected=None):
        if path in self.apps: return
        if len(self.apps) >= INVENTORY_LIMITS['apps']: raise InventoryLimit()
        self.apps.add(path)
        names = self.names(path, optional=True)
        if names is None: return
        with self.observed.directory(path) as fd: info = os.fstat(fd)
        binding = 'manifest' if role == 'active' else 'record' if expected is not None else 'unbound'
        self.add('app', path, binding, info=info, role=role)
        def metadata():
            info = plistlib.loads(self.observed.raw(path / 'Contents/Info.plist', 1048576))
            package = app_metadata(self.observed, path)
            source = package.get('jonesSource', {})
            require(isinstance(info, dict) and isinstance(source, dict))
            executable = info.get('CFBundleExecutable')
            require(isinstance(executable, str) and re.fullmatch('[^/\\\\]+', executable) is not None and executable not in ('.', '..'))
            require(source.get('repository') == REPOSITORY and hex_value(source.get('sha'), 40) and hex_value(source.get('tree'), 40))
            require(package.get('version') == info.get('CFBundleShortVersionString'))
            if expected is not None:
                require(source['sha'] == expected['sourceSha'] and source['tree'] == expected['sourceTree'] and package['version'] == expected['version'])
                require(executable == absolute(expected['executablePath']).name)
            self.add('app-metadata', path, binding, sourceSha=source['sha'], sourceTree=source['tree'], version=package['version'])
        self.attempt(path / 'Contents/Resources/app.asar', metadata)
        pending = [(path, 0)]
        while pending and not self.limited:
            directory, depth = pending.pop()
            if depth > INVENTORY_LIMITS['depth']: raise InventoryLimit()
            children = self.names(directory)
            for name in children:
                child = directory / name
                info = self.attempt(child, lambda child=child: self.node(child))
                if info is None: continue
                if stat.S_ISDIR(info.st_mode):
                    pending.append((child, depth + 1))
                elif stat.S_ISREG(info.st_mode):
                    if info.st_mode & 0o111:
                        self.add('executable', child, binding, info=info, appPath=str(path))
                    if info.st_nlink != 1: self.issue('unresolved-hardlink', child)
                else:
                    self.issue('unsupported-entry', child, True)

    def helper(self, path, kind='helper', expected=None):
        info = self.node(path)
        if info is None: return
        require(stat.S_ISREG(info.st_mode))
        raw = self.observed.raw(path, 1048576)
        digest = sha(raw)
        wanted = expected if expected is not None else path.name[len('activate-v1-'):-3]
        binding = 'content-addressed' if digest == wanted else 'unbound'
        self.add(kind, path, binding, info=info, sha256=digest)
        if digest != wanted: self.issue('helper-digest-unbound', path)

    def stage(self, directory, apps):
        names = self.names(directory)
        marker = 'mac-app-receipt.json' if apps else 'jones-actions-stage.json'
        binding, expected = 'unbound', None
        if marker in names:
            def receipt():
                value = self.record(directory / marker)
                if apps:
                    app = self.staged_shape(value['app'])
                    require(absolute(app['appPath']).parent == directory and app['handle'] == directory.name)
                    candidate = value['candidate']
                    require(isinstance(candidate, dict))
                    require(validate_candidate(candidate) == directory.name)
                    require(candidate.get('source') == app['sourceSha'] and candidate.get('tree') == app['sourceTree'] and candidate.get('version') == app['version'])
                    return app
                require(type(value.get('schema')) is int and value['schema'] == 1 and value.get('source') == 'jones-actions' and value.get('channel') == 'jones-main' and value.get('stagedHandle') == directory.name)
                candidate = value['candidate']
                require(isinstance(candidate, dict) and isinstance(value['receipt'], dict))
                require(validate_candidate(candidate) == directory.name)
                artifact_receipt(value['receipt'], candidate)
                require(value.get('payloadPath') == str(directory / value['receipt']['artifact']))
                return value
            expected = self.attempt(directory / marker, receipt)
            if expected is not None: binding = 'record'
        else:
            self.issue('receipt-missing', directory / marker)
        self.add('stage', directory, binding, stageKind='apps' if apps else 'artifacts')
        for name in names:
            if apps and name == 'mount':
                self.add('excluded', directory / name, reason='mount-not-traversed')
            elif apps and name.endswith('.app'):
                target = directory / name
                matching = expected if expected is not None and expected['appPath'] == str(target) else None
                self.attempt(target, lambda target=target, matching=matching: self.app(target, 'staged', matching))
            elif name != marker:
                self.attempt(directory / name, lambda name=name: self.add('stage-entry', directory / name, info=self.node(directory / name)))

    def stages(self, root, apps):
        names = self.names(root, optional=True)
        if names is None: return
        for name in names:
            target = root / name
            if hex_value(name, 64):
                self.attempt(target, lambda target=target: self.stage(target, apps))
            elif name == 'attempts':
                attempts = self.names(target)
                for attempt in attempts:
                    base = target / attempt
                    if re.fullmatch('stage-[a-zA-Z0-9]+', attempt) is None:
                        self.add('unknown-entry', base)
                        continue
                    handles = self.attempt(base, lambda base=base: self.names(base))
                    if handles is None: continue
                    for handle in handles:
                        if hex_value(handle, 64):
                            directory = base / handle
                            self.attempt(directory, lambda directory=directory: self.stage(directory, apps))
                        else:
                            self.add('unknown-entry', base / handle)
            elif name == 'completed':
                # These indexes can have the publisher's second hard link. Enumerate
                # their identities only; actual attempts are scanned independently.
                completed = self.names(target)
                for entry in completed:
                    index = target / entry
                    self.attempt(index, lambda index=index: self.add('completion-index', index, info=self.node(index)))
            else:
                self.add('unknown-entry', target)

    def transaction(self, directory):
        self.observed.denied += (directory / 'previous-pair', directory / 'advanced-state')
        names = self.names(directory)
        if 'intent.json' not in names:
            self.issue('transaction-intent-missing', directory, True)
            return
        intent = self.record(directory / 'intent.json')
        require(type(intent.get('protocol')) is int and intent['protocol'] == 1 and intent.get('transactionId') == directory.name)
        prior, staged = self.active_shape(intent['expected']), self.staged_shape(intent['staged'])
        require(all(prior[key] == self.active[key] for key in ('home', 'databasePath', 'profile', 'environmentId', 'appPath')))
        require(intent.get('continuationReceipt') == str(directory / 'continuation.json'))
        if 'journal.json' in names:
            journal = self.record(directory / 'journal.json')
            require(journal.get('intent') == intent)
        stable = absolute(self.active['appPath'])
        for role, path, expected in (
            ('incoming', stable.with_name(stable.name + '.jones-incoming-' + directory.name), staged),
            ('previous', stable.with_name(stable.name + '.jones-previous-' + prior['generation']), prior)):
            self.attempt(path, lambda path=path, role=role, expected=expected: self.app(path, role, expected))
        for name in ('previous-pair', 'advanced-state'):
            if name in names: self.add('excluded', directory / name, reason='private-state-copy')

    def transactions(self):
        root = self.updater / 'transactions'
        names = self.names(root, optional=True)
        for name in names or ():
            path = root / name
            if not hex_value(name, 64): self.add('unknown-entry', path)
            else: self.attempt(path, lambda path=path: self.transaction(path))

    def selections(self):
        root = self.updater / 'staging'
        names = self.names(root, optional=True)
        for name in names or ():
            path = root / name
            if re.fullmatch('[a-f0-9]{40}-[a-f0-9]{64}\\.json', name) is None:
                self.add('unknown-entry', path)
                continue
            def selection(path=path):
                value = self.record(path)
                require(type(value.get('schema')) is int and value['schema'] == 1 and value.get('source') == 'jones-actions')
                prior = self.active_shape(value['active'])
                require(all(prior[key] == self.active[key] for key in ('home', 'databasePath', 'profile', 'environmentId', 'appPath')))
                require(value.get('home') == prior['home'] and value.get('profile') == prior['profile'])
                require(value.get('installedSource') == prior['sourceSha'] == path.name[:40] and value.get('currentVersion') == prior['version'])
                app = self.staged_shape(value['app'])
                self.stage_path(self.updater / 'artifacts', app['handle'], value['artifactDirectory'])
            self.attempt(path, selection)

    def executors(self):
        root = self.updater / 'bootstrap-executors'
        names = self.names(root, optional=True)
        for name in names or ():
            directory = root / name
            if not hex_value(name, 64):
                self.add('unknown-entry', directory)
                continue
            def executor(directory=directory, name=name):
                children = self.names(directory)
                for child in children:
                    if child not in ('receipt.json', 'executor.py'): self.add('unknown-entry', directory / child)
                if 'receipt.json' not in children:
                    self.issue('executor-receipt-missing', directory, True)
                    return
                receipt = self.record(directory / 'receipt.json')
                require(type(receipt.get('schema')) is int and receipt['schema'] == 1 and receipt.get('bindingDigest') == name and receipt.get('manifestPath') == str(self.manifest))
                require(receipt.get('executorPath') == str(directory / 'executor.py') and hex_value(receipt.get('executorSha256'), 64) and receipt.get('activationAllowed') is False)
                self.helper(directory / 'executor.py', 'bootstrap-executor', receipt['executorSha256'])
            self.attempt(directory, executor)

    def roots(self):
        self.add('interpreter', pathlib.Path('/usr/bin/python3'), identityStatus='unobserved')
        self.attempt(absolute(self.active['appPath']), lambda: self.app(absolute(self.active['appPath']), 'active', self.active))
        names = self.names(self.updater, optional=True)
        for name in names or ():
            path = self.updater / name
            if re.fullmatch('activate-v1-[a-f0-9]{64}\\.py', name) is not None:
                self.attempt(path, lambda path=path: self.helper(path))
            elif name not in ('apps', 'artifacts', 'transactions', 'staging', 'bootstrap-executors'):
                self.add('unknown-entry', path)
        for name, operation in (
            ('transactions', self.transactions), ('staging', self.selections),
            ('apps', lambda: self.stages(self.updater / 'apps', True)),
            ('artifacts', lambda: self.stages(self.updater / 'artifacts', False)),
            ('bootstrap-executors', self.executors)):
            self.attempt(self.updater / name, operation)
        shim = self.home / 'bin/t3'
        def cli():
            names = self.names(shim.parent, optional=True)
            if names is None or shim.name not in names:
                self.add('absent', shim)
                return
            info = self.node(shim)
            if info is not None:
                self.add('cli-shim', shim, 'unbound', info=info, sha256=sha(self.observed.raw(shim)))
        self.attempt(shim, cli)

    def current(self):
        for path in self.listings: self.names(path, checking=True)
        for path, proof in self.nodes.items():
            self.visit()
            with self.observed.directory(path.parent) as fd:
                require(identity(os.stat(path.name, dir_fd=fd, follow_symlinks=False)) == proof)
        for path in self.absent:
            self.visit()
            try:
                with self.observed.directory(path): raise Refused('Previously missing inventory directory appeared.')
            except FileNotFoundError: pass
        for _ in range(len(self.observed.directories) + len(self.observed.files)): self.visit()
        self.observed.current()

    def finish(self):
        self.report['observations'].sort(key=lambda item: (item['path'], item['kind']))
        self.report['issues'].sort(key=lambda item: (item['path'], item['code']))
        self.report['limits'] = INVENTORY_LIMITS
        self.report['usage'] = {'visits': self.visits, 'readBytes': self.read_bytes}
        return self.report

def inventory_routes(argv):
    parser = argparse.ArgumentParser(description='Observe bounded installer path families; this cannot prove launch membership or authorize activation.')
    parser.add_argument('--manifest', required=True)
    args = parser.parse_args(argv)
    inventory = RouteInventory(args.manifest)
    try:
        require(sys.platform == 'darwin')
        require(len(args.manifest) <= 4096)
        inventory.manifest = absolute(args.manifest)
        inventory.anchor()
    except (Refused, OSError, ValueError, KeyError, TypeError, OverflowError, RuntimeError, InventoryLimit):
        inventory.report['status'] = 'refused'
        inventory.issue('manifest-unbound', args.manifest)
    else:
        inventory.attempt(inventory.manifest.parent, inventory.roots)
        inventory.attempt(inventory.manifest, inventory.current)
    report = inventory.finish()
    print(json.dumps(report, sort_keys=True))
    return {'observed': 0, 'partial': 2, 'refused': 1}[report['status']]

def main(argv=None):
    arguments = sys.argv[1:] if argv is None else argv
    if arguments[:1] == ['inventory-routes']: return inventory_routes(arguments[1:])
    parser = argparse.ArgumentParser(description='Inspect or stage the Jones executor. This does not install an update or authorize activation.')
    parser.add_argument('operation', choices=('inspect', 'stage-executor'))
    parser.add_argument('--manifest', required=True)
    parser.add_argument('--selection', required=True)
    parser.add_argument('--selection-sha256', required=True)
    args = parser.parse_args(argv)
    try:
        require(sys.platform == 'darwin')
        values = inspect_inputs(absolute(args.manifest), absolute(args.selection), args.selection_sha256, absolute(os.path.abspath(__file__)))
        observed, active, updater, transaction_names, output, executor, receipt = values
        observed.current()
        require(transactions(observed, updater / 'transactions', active['home']) == transaction_names)
        if args.operation == 'stage-executor':
            result = stage_executor(*values)
            response = dict(receipt, result=result, receiptPath=str(output / 'receipt.json'))
        else:
            response = dict(receipt, status='bootstrap-required', result='inspected')
        print(json.dumps(response, sort_keys=True))
        return 0
    except (Refused, OSError, ValueError, KeyError, TypeError, OverflowError, zipfile.BadZipFile, RuntimeError):
        print(json.dumps({'schema': 1, 'status': 'refused', 'activationAllowed': False, 'blocker': 'DESK-FIRST-01', 'reason': 'Bootstrap binding or retained output could not be verified; any partial executor output is retained for reconciliation.'}))
        return 1

if __name__ == '__main__':
    sys.exit(main())
`;
