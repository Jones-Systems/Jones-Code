/** Standalone, non-activating entry point; the executor is verified and copied only as data. */
export const jonesLegacyBootstrapSource = String.raw`#!/usr/bin/env python3
import argparse, contextlib, datetime, hashlib, json, os, pathlib, plistlib, re, stat, struct, sys, uuid, zipfile

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
    bundle, bundle_digest = observed.metadata(tools / 'BUNDLE.json', 8192)
    require(set(bundle) == {'schema', 'repository', 'sourceSha', 'sourceTree', 'version', 'bootstrapSha256', 'executorSha256'})
    require(type(bundle['schema']) is int and bundle['schema'] == 1 and bundle['repository'] == REPOSITORY and bundle['sourceSha'] == candidate['source'] and bundle['sourceTree'] == candidate['tree'] and bundle['version'] == candidate['version'])
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

def verify_output(observed, output, executor, receipt):
    with observed.directory(output) as fd:
        require(set(os.listdir(fd)) == {'executor.py', 'receipt.json'})
    require(observed.raw(output / 'executor.py', 1048576) == executor)
    require(observed.files[output / 'executor.py'][3] & 0o777 == 0o500)
    actual, _ = observed.metadata(output / 'receipt.json')
    require(actual == receipt)

def ensure_directory(observed, parent, name):
    with observed.directory(parent) as fd:
        try:
            os.mkdir(name, 0o700, dir_fd=fd)
            os.fsync(fd)
        except FileExistsError:
            pass
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
            verify_output(observed, output, executor, receipt)
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
    verify_output(observed, output, executor, receipt)
    observed.current()
    return 'created'

def main(argv=None):
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
