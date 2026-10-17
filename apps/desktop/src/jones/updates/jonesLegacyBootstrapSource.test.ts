// @effect-diagnostics nodeBuiltinImport:off - The synthetic Python fixture invokes only the inert public bootstrap command.
import * as NodeChildProcess from "node:child_process";
import * as NodeCrypto from "node:crypto";
import { describe, expect, it } from "vite-plus/test";
import {
  createJonesLegacyBootstrapBundle,
  createJonesLegacyBootstrapPreparationBundle,
  JONES_PREPARATION_SOURCE_INPUTS,
  type JonesPreparationArtifactEvidence,
} from "../../../../../scripts/jones/updates/legacy-bootstrap-bundle.ts";
import { jonesPreparationWorkerSource } from "./jonesPreparationWorkerSource.ts";

const fixture = String.raw`
import contextlib, hashlib, io, json, os, pathlib, plistlib, signal, stat, struct, sys, tempfile, zipfile

payload = json.load(sys.stdin)
fault = payload['fault']
files = payload['files']
bootstrap_source = files[0]['content']
namespace = {'__name__': 'bootstrap_fixture'}
exec(compile(bootstrap_source, 'bootstrap.py', 'exec'), namespace)
if fault.startswith('v3-'):
    worker = dict(namespace, BUNDLE_V2_KEYS=namespace['_BOOTSTRAP_BUNDLE_V2_KEYS'])
    exec(compile(payload['workerSource'], 'worker-definitions.py', 'exec'), worker)
# The command has no platform-specific mutations. Linux CI covers its Python
# protocol; the separate packaged Mac qualifier invokes the unmodified platform gate.
namespace['sys'].platform = 'darwin'
signal.signal(signal.SIGTERM, lambda number, frame: (_ for _ in ()).throw(SystemExit(128 + number)))

def encode(value):
    return json.dumps(value, separators=(',', ':'), ensure_ascii=False).encode()

def digest(data):
    return hashlib.sha256(data).hexdigest()

def write(path, data, mode=0o600):
    assert path.is_relative_to(root)
    path.parent.mkdir(parents=True, exist_ok=True)
    try: existing = path.lstat()
    except FileNotFoundError: existing = None
    if existing is not None:
        assert stat.S_ISREG(existing.st_mode) and existing.st_uid == os.getuid() and existing.st_nlink == 1
        path.chmod(stat.S_IMODE(existing.st_mode) | stat.S_IWUSR)
    try:
        path.write_bytes(data)
    finally:
        if existing is not None or path.exists(): path.chmod(mode)

def metadata_app(app, source, tree, version, packaged=False):
    metadata = {'version': version, 'jonesSource': {'repository': 'Jones-Systems/Jones-Code', 'sha': source, 'tree': tree}}
    if packaged: metadata['startupGateProtocol'] = 1
    package = encode(metadata)
    header = encode({'files': {'package.json': {'size': len(package), 'offset': '0'}}})
    write(app / 'Contents/Resources/app.asar', struct.pack('<IIII', 4, len(header) + 8, len(header) + 4, len(header)) + header + package)
    write(app / 'Contents/Info.plist', plistlib.dumps({'CFBundleExecutable': 'Jones', 'CFBundleIdentifier': 'test.jones.bootstrap', 'CFBundleShortVersionString': version}))
    write(app / 'Contents/MacOS/Jones', b'fixture-executable-not-runnable', 0o500)
    if packaged:
        for item in files:
            write(app / 'Contents/Resources' / item['relativePath'], bytes.fromhex(item['binaryHex']) if 'binaryHex' in item else item['content'].encode(), item['mode'])

def hash_app(app):
    return namespace['app_digest'](namespace['Observation'](), app)

result = {}
with tempfile.TemporaryDirectory(prefix='jones-legacy-bootstrap-') as temporary:
    root = pathlib.Path(temporary).resolve()
    home, profile = root / 'home', root / 'profile'
    updater = home / 'runtime/jones-updates'
    updater.mkdir(parents=True)
    database = home / 'userdata/statev2.sqlite'
    write(database, b'private-database-must-not-open')
    write(profile / 'profile-sentinel', b'private-profile-must-not-open')
    old = root / 'Applications/Installed.app'
    metadata_app(old, 'a' * 40, 'b' * 40, '0.0.45-preview.20261010.12.1')
    active = {'protocol': 1, 'owner': 'desktop', 'generation': '1' * 64, 'transactionId': 'bootstrap',
              'home': str(home), 'databasePath': str(database), 'profile': str(profile), 'environmentId': 'test-environment',
              'appPath': str(old), 'executablePath': str(old / 'Contents/MacOS/Jones'),
              'version': '0.0.45-preview.20261010.12.1', 'sourceSha': 'a' * 40, 'sourceTree': 'b' * 40,
              'bundleIdentifier': 'test.jones.bootstrap'}
    manifest = home / 'runtime/jones-active-install.json'
    original_manifest = encode(active)
    write(manifest, original_manifest)
    candidate = {'schema': 1, 'repository': 'Jones-Systems/Jones-Code', 'source': 'c' * 40, 'tree': 'd' * 40,
                 'installedSource': active['sourceSha'], 'workflow': '.github/workflows/artifact-desktop-mac.yml',
                 'workflowId': 11, 'runId': 123, 'runAttempt': 1, 'ciRunId': 456, 'artifactId': 789,
                 'artifactName': 'desktop-mac-arm64-123-1--0.0.45-preview.20261011.123.1',
                 'artifactDigest': '', 'artifactBytes': 0, 'expiresAt': '2099-12-01T00:00:00Z',
                 'version': '0.0.45-preview.20261011.123.1', 'platform': 'darwin', 'architecture': 'arm64'}
    dmg = b'fixture-dmg-already-staged'
    artifact_receipt = {key: candidate[key] for key in ('repository', 'source', 'tree', 'workflow', 'version', 'platform', 'architecture')}
    artifact_receipt.update(schema=1, event='push', ref='refs/heads/main', runId='123', runAttempt='1', artifact='T3-Code-' + candidate['version'] + '-arm64.dmg', sha256=digest(dmg))
    archive_buffer = io.BytesIO()
    with zipfile.ZipFile(archive_buffer, 'w') as archive:
        archive.writestr('ARTIFACT.json', encode(artifact_receipt))
        archive.writestr(artifact_receipt['artifact'], dmg)
    archive = archive_buffer.getvalue()
    candidate.update(artifactDigest=digest(archive), artifactBytes=len(archive))
    handle = digest(encode([candidate[key] for key in namespace['CANDIDATE_FIELDS']]))
    if fault == 'immutable-attempt':
        artifact_dir = updater / 'artifacts/attempts/stage-Artifact1' / handle
        app_dir = updater / 'apps/attempts/stage-App1' / handle
    else:
        artifact_dir = updater / 'artifacts' / handle
        app_dir = updater / 'apps' / handle
    candidate_app = app_dir / 'Candidate.app'
    metadata_app(candidate_app, candidate['source'], candidate['tree'], candidate['version'], True)
    if fault == 'internal-links':
        (candidate_app / 'Contents/ResourcesAlias').symlink_to('Resources', target_is_directory=True)
        (candidate_app / 'Contents/Chained.asar').symlink_to('ResourcesAlias/app.asar')
    artifact = {'schema': 1, 'source': 'jones-actions', 'channel': 'jones-main', 'stagedHandle': handle,
                'candidate': candidate, 'receipt': artifact_receipt, 'payloadPath': str(artifact_dir / artifact_receipt['artifact'])}
    write(artifact_dir / artifact_receipt['artifact'], dmg)
    write(artifact_dir / 'github-artifact.zip', archive)
    write(artifact_dir / 'jones-actions-stage.json', encode(artifact))
    app = {'handle': handle, 'receiptPath': str(app_dir / 'mac-app-receipt.json'), 'appPath': str(candidate_app),
           'executablePath': str(candidate_app / 'Contents/MacOS/Jones'), 'version': candidate['version'],
           'sourceSha': candidate['source'], 'sourceTree': candidate['tree'], 'appDigest': hash_app(candidate_app),
           'asarDigest': digest((candidate_app / 'Contents/Resources/app.asar').read_bytes()),
           'executableDigest': digest((candidate_app / 'Contents/MacOS/Jones').read_bytes()),
           'bundleIdentifier': 'test.jones.bootstrap', 'startupGateProtocol': 1}
    write(app_dir / 'mac-app-receipt.json', encode({'app': app, 'artifact': artifact, 'candidate': candidate}))
    selection = {'schema': 1, 'source': 'jones-actions', 'home': str(home), 'profile': str(profile),
                 'currentVersion': active['version'], 'installedSource': active['sourceSha'], 'active': active,
                 'artifactDirectory': str(artifact_dir), 'app': app}
    selection_path = updater / 'staging' / (active['sourceSha'] + '-fixture.json')
    selection_raw = encode(selection)
    write(selection_path, selection_raw)
    entry = candidate_app / 'Contents/Resources/jones-bootstrap/bootstrap.py'
    namespace['__file__'] = str(entry)
    args = ['--manifest', str(manifest), '--selection', str(selection_path), '--selection-sha256', digest(selection_raw)]
    runtime_reads = []
    if fault.startswith('v3-'):
        Observation = namespace['Observation']
        original_file, original_raw = Observation.file, Observation.raw
        @contextlib.contextmanager
        def bounded_file(self, path, limit):
            with original_file(self, path, limit) as stream:
                if path.name != 'jones-offline-preparation': yield stream; return
                class Reader:
                    def fileno(self): return stream.fileno()
                    def read(self, size):
                        assert 0 < size <= 1048576
                        runtime_reads.append((limit, size))
                        return stream.read(size)
                yield Reader()
        def bounded_raw(self, path, limit=131072):
            assert path.name != 'jones-offline-preparation', 'Runtime reached buffered metadata reader'
            return original_raw(self, path, limit)
        Observation.file, Observation.raw = bounded_file, bounded_raw
        proof = namespace['decode']((entry.parent / 'BUNDLE.json').read_bytes())
        assert namespace['_bootstrap_preparation_bundle'](proof) == worker['_preparation_bundle'](proof)
        result['proofAgreement'] = True
    guard = {'active': False, 'forbidden': []}
    def audit(event, arguments):
        if not guard['active']: return
        if event in ('open', 'os.listdir', 'os.scandir') and arguments and isinstance(arguments[0], (str, bytes)):
            path = os.fsdecode(arguments[0])
            if path in ('userdata', 'profile', 'statev2.sqlite', 'profile-sentinel') or path.startswith(str(database.parent)) or path.startswith(str(profile)):
                guard['forbidden'].append(event)
                raise AssertionError('Bootstrap accessed protected fixture state')
        if event in ('subprocess.Popen', 'os.system', 'os.exec', 'os.posix_spawn', 'sqlite3.connect'):
            guard['forbidden'].append(event)
            raise AssertionError('Bootstrap attempted execution or database access')
    sys.addaudithook(audit)
    def invoke(operation):
        output = io.StringIO()
        guard['active'] = True
        try:
            with contextlib.redirect_stdout(output), contextlib.redirect_stderr(io.StringIO()): code = namespace['main']([operation] + args)
        finally:
            guard['active'] = False
        return code, json.loads(output.getvalue())
    code, inspection = invoke('inspect')
    assert code == 0 and inspection['status'] == 'bootstrap-required' and not (updater / 'bootstrap-executors').exists()
    output = pathlib.Path(inspection['executorPath']).parent
    if (fault.startswith('v3-') and fault != 'v3-success') or fault == 'old-proof-oversize':
        bundle_path = entry.parent / 'BUNDLE.json'
        proof = namespace['decode'](bundle_path.read_bytes())
        runtime_path = entry.parent / 'jones-offline-preparation'
        if fault == 'v3-old-inputs': del proof['preparation']['sourceInputHashes']['apps/server/src/jones/updates/offlinePreparationWorker.ts']
        elif fault == 'v3-extra-proof': proof['preparation']['callerApproved'] = True
        elif fault == 'v3-runtime-bytes': write(runtime_path, runtime_path.read_bytes()[:-1] + b'y', 0o500)
        elif fault == 'v3-runtime-mode': runtime_path.chmod(0o400)
        elif fault == 'v3-source': proof['sourceSha'] = 'f' * 40
        elif fault == 'v3-runtime-oversize': proof['preparation']['executable']['size'] = 256 * 1024 * 1024 + 1
        elif fault == 'v3-native-oversize':
            native_path = entry.parent / 'identity-native'
            native = native_path.read_bytes() + bytes(4 * 1024 * 1024 + 1 - native_path.stat().st_size)
            write(native_path, native, 0o500)
            proof['identityNativeSha256'] = digest(native)
        elif fault == 'v3-runtime-header':
            runtime = bytes(16) + runtime_path.read_bytes()[16:]
            write(runtime_path, runtime, 0o500)
            proof['preparation']['executable']['sha256'] = digest(runtime)
        elif fault == 'v3-runtime-size': proof['preparation']['executable']['size'] += 1
        elif fault == 'v3-missing-runtime': runtime_path.unlink()
        elif fault == 'v3-extra-resource': write(entry.parent / 'unexpected', b'preserved')
        elif fault == 'v3-sqlite-date': proof['preparation']['sqlite']['sourceId'] = '1900-02-29 00:00:00 ' + 'a' * 64
        elif fault == 'v3-node-version': proof['preparation']['node']['version'] = '26.8.1'
        elif fault == 'v3-boolean-protocol': proof['preparation']['protocol'] = True
        elif fault == 'v3-executor-mode': (entry.parent / 'executor.py').chmod(0o400)
        elif fault == 'v3-maintenance-bytes': write(entry.parent / 'maintenance.py', b'changed', 0o500)
        raw_bundle = encode(proof) + b'\n'
        if fault == 'v3-canonical-proof': raw_bundle = json.dumps(proof, indent=2).encode() + b'\n'
        elif fault == 'v3-proof-oversize': raw_bundle += b' ' * 65536
        elif fault == 'old-proof-oversize': raw_bundle += b' ' * 8192
        write(bundle_path, raw_bundle, 0o500 if fault == 'v3-bundle-mode' else 0o400)
        if fault.startswith('v3-'):
            def accepts(decoder):
                try: decoder(proof); return True
                except namespace['Refused']: return False
            assert accepts(namespace['_bootstrap_preparation_bundle']) == accepts(worker['_preparation_bundle'])
        # Rebind the fixture receipts so refusal proves bundle validation, not an
        # earlier stale app digest. This creates no production provenance.
        app['appDigest'] = hash_app(candidate_app)
        write(app_dir / 'mac-app-receipt.json', encode({'app': app, 'artifact': artifact, 'candidate': candidate}))
        selection_raw = encode(selection)
        write(selection_path, selection_raw)
        args[-1] = digest(selection_raw)
        refused, rejected = invoke('inspect')
        assert refused == 1 and rejected['status'] == 'refused' and not (updater / 'bootstrap-executors').exists()
        result['inspectionRefused'] = True
    elif fault == 'selection-changed': write(selection_path, selection_raw + b' ')
    elif fault == 'manifest-changed': write(manifest, encode(dict(active, generation='2' * 64)))
    elif fault == 'candidate-changed': write(candidate_app / 'Contents/MacOS/Jones', b'changed', 0o500)
    elif fault == 'protected-link': (candidate_app / 'Contents/Resources/protected-link').symlink_to(database)
    elif fault == 'archive-changed': write(artifact_dir / 'github-artifact.zip', b'changed')
    elif fault == 'bundle-changed': write(entry.parent / 'BUNDLE.json', b'{}', 0o400)
    elif fault == 'pending-transaction': write(updater / 'transactions' / ('f' * 64) / 'prepare-intent.json', b'{}')
    elif fault == 'terminal-transaction':
        tx = updater / 'transactions' / ('f' * 64)
        intent = {'protocol': 1, 'transactionId': 'f' * 64, 'expected': active}
        write(tx / 'intent.json', encode(intent))
        write(tx / 'journal.json', encode({'intent': intent, 'phase': 'rolled-back'}))
    elif fault == 'partial-output': write(output / 'executor.py', b'unfinished')
    elif fault == 'symlink-output':
        foreign = root / 'foreign'
        foreign.mkdir()
        (updater / 'bootstrap-executors').symlink_to(foreign, target_is_directory=True)
    elif fault == 'metadata-symlink':
        selection_path.unlink()
        target = root / 'copied-selection.json'
        write(target, selection_raw)
        selection_path.symlink_to(target)
    elif fault == 'receipt-publication-failed':
        real_link = os.link
        def failed_link(source, target, **kwargs):
            if target == 'receipt.json': raise OSError('injected receipt publication failure')
            return real_link(source, target, **kwargs)
        os.link = failed_link
    elif fault == 'manifest-race':
        real_sync = os.fsync
        def racing_sync(fd):
            real_sync(fd)
            info = os.fstat(fd)
            if stat.S_ISREG(info.st_mode) and info.st_size == len(files[1]['content'].encode()) and info.st_mode & 0o777 == 0o500:
                write(manifest, original_manifest + b' ')
        os.fsync = racing_sync
    elif fault in ('final-output-sync-failed', 'final-output-sync-persistent', 'bootstrap-parent-sync-failed', 'bootstrap-parent-sync-persistent'):
        real_sync = os.fsync
        durability = {'failures': 0, 'retry': False, 'synced': []}
        sync_paths = {'updater': updater, 'bootstrap-root': output.parent, 'output': output,
                      'executor': output / 'executor.py', 'receipt': output / 'receipt.json'}
        def failing_sync(fd):
            info = os.fstat(fd)
            label = None
            for name, path in sync_paths.items():
                try: target = path.stat()
                except FileNotFoundError: continue
                if (info.st_dev, info.st_ino) == (target.st_dev, target.st_ino):
                    label = name
                    break
            boundary = (fault.startswith('final-output-') and label == 'output' and (output / 'receipt.json').exists()) or (fault.startswith('bootstrap-parent-') and label == 'updater')
            if boundary and (fault.endswith('-persistent') or durability['failures'] == 0):
                durability['failures'] += 1
                raise OSError('injected directory durability failure')
            real_sync(fd)
            if durability['retry'] and label is not None: durability['synced'].append(label)
        os.fsync = failing_sync
    elif fault == 'unsupported-command':
        try: invoke('activate')
        except SystemExit as error: result['argumentExit'] = error.code
        else: raise AssertionError('Activation command was accepted')
    if fault != 'unsupported-command':
        code, response = invoke('stage-executor')
        result.update(code=code, response=response)
        if code == 0:
            assert (output / 'executor.py').read_bytes() == files[1]['content'].encode()
            assert sorted(path.name for path in output.iterdir()) == ['executor.py', 'receipt.json']
            assert stat.S_IMODE((output / 'executor.py').stat().st_mode) == 0o500
            assert json.loads((output / 'receipt.json').read_bytes())['activationAllowed'] is False
            again, retry = invoke('stage-executor')
            assert again == 0 and retry['result'] == 'no-change'
            assert manifest.read_bytes() == original_manifest
            result['retry'] = retry['result']
        elif fault in ('partial-output', 'receipt-publication-failed', 'manifest-race'):
            assert output.is_dir() and (output / 'executor.py').is_file() and not (output / 'receipt.json').exists()
            if fault == 'receipt-publication-failed': os.link = real_link
            if fault == 'manifest-race':
                os.fsync = real_sync
                write(manifest, original_manifest)
            again, retry = invoke('stage-executor')
            assert again == 1 and retry['status'] == 'refused'
            result['partialPreserved'] = True
        elif fault in ('final-output-sync-failed', 'final-output-sync-persistent', 'bootstrap-parent-sync-failed', 'bootstrap-parent-sync-persistent'):
            assert code == 1 and response['status'] == 'refused' and durability['failures'] == 1
            root_identity = output.parent.stat().st_ino
            retained = None
            if fault.startswith('final-output-'):
                retained = {name: (output / name).read_bytes() for name in ('executor.py', 'receipt.json')}
            else:
                assert not output.exists()
            durability['retry'] = True
            again, retry = invoke('stage-executor')
            assert output.parent.stat().st_ino == root_identity
            if fault.endswith('-persistent'):
                assert again == 1 and retry['status'] == 'refused' and durability['failures'] == 2
            else:
                expected = 'no-change' if retained is not None else 'created'
                assert again == 0 and retry['result'] == expected
                assert set(sync_paths) <= set(durability['synced'])
            if retained is not None:
                assert all((output / name).read_bytes() == data for name, data in retained.items())
            assert manifest.read_bytes() == original_manifest
            result.update(retryCode=again, retryResult=retry.get('result'), synced=durability['synced'])
    assert database.read_bytes() == b'private-database-must-not-open'
    assert (profile / 'profile-sentinel').read_bytes() == b'private-profile-must-not-open'
    assert guard['forbidden'] == []
    assert not list(updater.rglob('commit-grant.json')) and not list(updater.rglob('trial-descriptor.json'))
    result['protectedAccesses'] = guard['forbidden']
    if fault.startswith('v3-'):
        assert any(limit == 256 * 1024 * 1024 for limit, size in runtime_reads)
        assert len(runtime_reads) > 1 and manifest.read_bytes() == original_manifest
        result['runtimeQualification'] = 'synthetic-nonexecuted-resource'
        result['runtimeExecuted'] = False
        if fault != 'v3-success': assert not (updater / 'bootstrap-executors').exists()
result['fixtureRemoved'] = not root.exists()
print(json.dumps(result))
`;

const files = createJonesLegacyBootstrapBundle({
  repository: "Jones-Systems/Jones-Code",
  sourceSha: "c".repeat(40),
  sourceTree: "d".repeat(40),
  version: "0.0.45-preview.20261011.123.1",
});

function scenario(fault: string, bundle = files) {
  const output = NodeChildProcess.execFileSync("python3", ["-c", fixture], {
    input: JSON.stringify({
      files: bundle,
      fault,
      ...(fault.startsWith("v3-") ? { workerSource: jonesPreparationWorkerSource } : {}),
    }),
    encoding: "utf8",
    timeout: 15000,
    maxBuffer: 1024 * 1024,
  });
  const result = JSON.parse(output);
  expect(result.fixtureRemoved).toBe(true);
  expect(result.protectedAccesses).toEqual([]);
  return result;
}

describe("standalone legacy bootstrap", () => {
  it.each(["success", "immutable-attempt", "terminal-transaction", "internal-links"])(
    "stages the exact executor and makes exact retries inert: %s",
    (fault) => {
      const result = scenario(fault);
      expect(result.code).toBe(0);
      expect(result.retry).toBe("no-change");
      expect(result.response).toMatchObject({
        status: "executor-ready",
        activationAllowed: false,
        observationOnly: true,
        blocker: "DESK-FIRST-01",
        nextRequirement: "qualified-maintenance-admission",
      });
    },
  );

  it.each([
    "selection-changed",
    "manifest-changed",
    "candidate-changed",
    "protected-link",
    "archive-changed",
    "bundle-changed",
    "pending-transaction",
    "symlink-output",
    "metadata-symlink",
  ])("refuses changed or uncertain inputs without state access: %s", (fault) => {
    const result = scenario(fault);
    expect(result.code).toBe(1);
    expect(result.response).toMatchObject({ status: "refused", activationAllowed: false });
  });

  it.each(["partial-output", "receipt-publication-failed", "manifest-race"])(
    "preserves incomplete output and never reports it complete: %s",
    (fault) => {
      const result = scenario(fault);
      expect(result.code).toBe(1);
      expect(result.partialPreserved).toBe(true);
    },
  );

  it.each([
    ["final-output-sync-failed", "no-change"],
    ["bootstrap-parent-sync-failed", "created"],
  ])("re-establishes retained output durability after %s", (fault, expected) => {
    const result = scenario(fault);
    expect(result.code).toBe(1);
    expect(result.retryCode).toBe(0);
    expect(result.retryResult).toBe(expected);
    expect(result.synced).toEqual(
      expect.arrayContaining(["updater", "bootstrap-root", "output", "executor", "receipt"]),
    );
  });

  it.each(["final-output-sync-persistent", "bootstrap-parent-sync-persistent"])(
    "refuses repeated staging while durability remains uncertain: %s",
    (fault) => {
      const result = scenario(fault);
      expect(result.code).toBe(1);
      expect(result.retryCode).toBe(1);
      expect(result.retryResult).toBeNull();
    },
  );

  it("has no activation command", () => {
    expect(scenario("unsupported-command").argumentExit).toBe(2);
  });
});

const inventoryFixture = String.raw`
import builtins, contextlib, hashlib, io, json, os, pathlib, plistlib, signal, stat, struct, sys, tempfile

payload = json.load(sys.stdin)
imports = []
def checked_import(name, *args, **kwargs):
    if name != 'sys':
        assert sys.dont_write_bytecode, 'Bootstrap must disable bytecode writes before importing library modules'
        imports.append(name)
    return builtins.__import__(name, *args, **kwargs)
namespace = {'__name__': 'inventory_fixture', '__builtins__': dict(vars(builtins), __import__=checked_import)}
sys.dont_write_bytecode = False
exec(compile(payload['files'][0]['content'], 'bootstrap.py', 'exec'), namespace)
assert imports
namespace['sys'].platform = 'darwin'
signal.signal(signal.SIGTERM, lambda number, frame: (_ for _ in ()).throw(SystemExit(128 + number)))
fault = payload['fault']
guard = {'active': False, 'violations': [], 'controls': 0, 'fixtureMutations': 0}
real_stat = os.stat

def encode(value): return json.dumps(value, separators=(',', ':')).encode()
def digest(value): return hashlib.sha256(value).hexdigest()
def write(path, value, mode=0o600):
    assert path.is_relative_to(root)
    path.parent.mkdir(parents=True, exist_ok=True)
    if path.exists(): path.chmod(0o600)
    path.write_bytes(value)
    path.chmod(mode)

def app(path):
    package = encode({'version': 'legacy', 'jonesSource': {'repository': 'Jones-Systems/Jones-Code', 'sha': 'a' * 40, 'tree': 'b' * 40}})
    header = encode({'files': {'package.json': {'size': len(package), 'offset': '0'}}})
    write(path / 'Contents/Resources/app.asar', struct.pack('<IIII', 4, len(header) + 8, len(header) + 4, len(header)) + header + package)
    write(path / 'Contents/Info.plist', plistlib.dumps({'CFBundleExecutable': 'Jones', 'CFBundleShortVersionString': 'legacy'}))
    write(path / 'Contents/MacOS/Jones', b'inert fixture executable', 0o500)

def snapshot():
    result = {}
    for directory, directories, files in os.walk(root, followlinks=False):
        for name in directories + files:
            path = pathlib.Path(directory) / name
            info = path.lstat()
            item = [stat.S_IMODE(info.st_mode), str(info.st_ino)]
            if stat.S_ISLNK(info.st_mode): item.append(os.readlink(path))
            elif stat.S_ISREG(info.st_mode): item.append(digest(path.read_bytes()))
            result[str(path.relative_to(root))] = item
    return result

with tempfile.TemporaryDirectory(prefix='jones-inventory-fixture-') as temporary:
    root = pathlib.Path(temporary).resolve()
    home, profile = root / 'home', root / 'profile'
    updater = home / 'runtime/jones-updates'
    database = home / 'userdata/statev2.sqlite'
    write(database, b'private database sentinel')
    write(profile / 'sentinel', b'private profile sentinel')
    installed = root / 'Applications/Installed.app'
    app(installed)
    active = {'protocol': 1, 'owner': 'desktop', 'generation': 'legacy-generation', 'transactionId': 'bootstrap',
              'home': str(home), 'databasePath': str(database), 'profile': str(profile), 'environmentId': 'fixture',
              'appPath': str(installed), 'executablePath': str(installed / 'Contents/MacOS/Jones'),
              'version': 'legacy', 'sourceSha': 'a' * 40, 'sourceTree': 'b' * 40}
    manifest = home / 'runtime/jones-active-install.json'
    write(manifest, encode(active))
    stage = updater / 'apps/attempts/stage-PreReceipt' / ('e' * 64)
    staged_app = stage / 'Candidate.app'
    app(staged_app)
    nested = staged_app / 'Contents/Frameworks/Nested.app/Contents/MacOS/Helper'
    write(nested, b'inert nested executable', 0o500)
    stage_mount = stage / 'mount'
    write(stage_mount / 'private-mounted-file', b'must not inspect retained mount')
    unknown = stage / 'unknown-directory'
    write(unknown / 'private-unknown-file', b'must not enter unknown sibling')
    staged = {'handle': 'e' * 64, 'receiptPath': str(stage / 'mac-app-receipt.json'), 'appPath': str(staged_app),
              'executablePath': str(staged_app / 'Contents/MacOS/Jones'), 'version': 'legacy',
              'sourceSha': 'a' * 40, 'sourceTree': 'b' * 40,
              'appDigest': '1' * 64, 'asarDigest': '2' * 64, 'executableDigest': '3' * 64}
    tx = updater / 'transactions' / ('d' * 64)
    intent = {'protocol': 1, 'transactionId': tx.name, 'expected': active, 'staged': staged,
              'continuationReceipt': str(tx / 'continuation.json')}
    intent_path = tx / 'intent.json'
    write(intent_path, encode(intent))
    write(tx / 'journal.json', encode({'intent': intent, 'phase': 'rolled-back'}))
    for name in ('previous-pair', 'advanced-state'): write(tx / name / 'sentinel', b'private retained state')
    incoming = installed.with_name(installed.name + '.jones-incoming-' + tx.name)
    previous = installed.with_name(installed.name + '.jones-previous-' + active['generation'])
    app(incoming)
    app(previous)
    helper_data = b'# inert legacy helper; never import or launch\n'
    helper = updater / ('activate-v1-' + digest(helper_data) + '.py')
    write(helper, helper_data, 0o700)
    write(home / 'bin/t3', b'#!/bin/sh\n# inert shim\n', 0o700)
    index = updater / 'apps/completed' / ('e' * 64 + '.json')
    write(index, b'index data must not be read')
    os.link(index, index.with_name('second-link.json'))
    denied = [database.parent, profile, tx / 'previous-pair', tx / 'advanced-state', stage_mount]
    forbidden_reads = [index, index.with_name('second-link.json'), unknown / 'private-unknown-file']
    arguments = ['inventory-routes', '--manifest', str(manifest)]
    if fault == 'invalid-anchor': arguments[-1] = str(database)
    elif fault == 'overlap-anchor': write(manifest, encode(dict(active, profile=str(updater))))
    elif fault == 'partial-transaction': write(intent_path, encode({'protocol': 1, 'transactionId': tx.name}))
    elif fault == 'foreign-intent':
        foreign = dict(active, appPath=str(root / 'foreign.app'), executablePath=str(root / 'foreign.app/Contents/MacOS/Jones'))
        write(intent_path, encode(dict(intent, expected=foreign)))
    elif fault == 'partial-executor': write(updater / 'bootstrap-executors' / ('9' * 64) / 'executor.py', b'partial')
    elif fault == 'symlink': (staged_app / 'Contents/LinkedProfile').symlink_to(profile, target_is_directory=True)
    elif fault == 'metadata-symlink':
        target = staged_app / 'Contents/Info.plist'
        target.unlink()
        target.symlink_to(profile / 'sentinel')
    elif fault == 'selection-argument': arguments += ['--selection', str(database)]
    elif fault == 'read-failed':
        real_metadata = namespace['InventoryObservation'].metadata
        def failed_metadata(self, path, limit=131072):
            if path == intent_path: raise OSError('injected owned fixture metadata read failure')
            return real_metadata(self, path, limit)
        namespace['InventoryObservation'].metadata = failed_metadata
    elif fault.startswith('limit-'):
        name = fault[len('limit-'):]
        value = {'directoryEntries': 2, 'visits': 3, 'depth': 0, 'apps': 1, 'records': 3,
                 'readBytes': len(manifest.read_bytes()) + 16}[name]
        namespace['INVENTORY_LIMITS'][name] = value

    def protected(path, metadata=False):
        if not isinstance(path, (str, bytes, os.PathLike)): return False
        value = os.fsdecode(path)
        parsed = pathlib.Path(value)
        if parsed.is_absolute():
            return any(parsed == item or parsed.is_relative_to(item) for item in denied) or (not metadata and parsed in forbidden_reads)
        return value in ('userdata', 'profile', 'previous-pair', 'advanced-state', 'mount', 'private-mounted-file', 'private-unknown-file')

    def reject(event):
        guard['violations'].append(event)
        raise AssertionError('Inventory crossed the synthetic read-only boundary: ' + event)

    def checked_stat(path, *args, **kwargs):
        if guard['active'] and protected(path, True): reject('stat')
        return real_stat(path, *args, **kwargs)

    def audit(event, args):
        if not guard['active']: return
        if event in ('open', 'os.listdir', 'os.scandir') and args and protected(args[0]): reject(event)
        if event == 'open' and len(args) > 2 and isinstance(args[2], int) and args[2] & (os.O_WRONLY | os.O_RDWR | os.O_CREAT | os.O_TRUNC | os.O_APPEND): reject('write-open')
        if event in ('os.mkdir', 'os.remove', 'os.rmdir', 'os.rename', 'os.link', 'os.symlink', 'os.chmod', 'os.truncate', 'os.utime', 'subprocess.Popen', 'os.system', 'os.exec', 'os.posix_spawn', 'sqlite3.connect'): reject(event)

    sys.addaudithook(audit)
    os.stat = checked_stat
    try:
        guard['active'] = True
        for path in denied:
            for operation in (lambda path=path: os.stat(path), lambda path=path: os.listdir(path), lambda path=path: os.open(path, os.O_RDONLY)):
                try: operation()
                except AssertionError: guard['controls'] += 1
                else: raise AssertionError('Protected-path positive control did not reject')
        try: os.chmod(helper, 0o600)
        except AssertionError: guard['controls'] += 1
        else: raise AssertionError('Mutation positive control did not reject')
        guard['active'] = False
        guard['violations'].clear()
        expected = snapshot()
        if fault in ('manifest-race', 'membership-race', 'mode-race'):
            real_current = namespace['RouteInventory'].current
            def raced_current(self):
                global expected
                guard['active'] = False
                try:
                    if fault == 'manifest-race': write(manifest, manifest.read_bytes() + b' ')
                    elif fault == 'membership-race': write(stage / 'appeared', b'owned fixture race')
                    else: nested.chmod(0o400)
                    guard['fixtureMutations'] += 1
                    expected = snapshot()
                finally: guard['active'] = True
                return real_current(self)
            namespace['RouteInventory'].current = raced_current
        output = io.StringIO()
        guard['active'] = True
        try:
            with contextlib.redirect_stdout(output), contextlib.redirect_stderr(io.StringIO()):
                try: code = namespace['main'](arguments)
                except SystemExit as error:
                    assert fault == 'selection-argument'
                    code = error.code
        finally: guard['active'] = False
        report = json.loads(output.getvalue()) if output.getvalue() else None
        assert snapshot() == expected
        assert not guard['violations']
        result = {'code': code, 'report': report, 'violations': guard['violations'], 'controls': guard['controls'],
                  'fixtureMutations': guard['fixtureMutations'], 'unchanged': True,
                  'paths': {'helper': str(helper), 'stage': str(stage), 'nested': str(nested), 'mount': str(stage_mount),
                            'incoming': str(incoming), 'previous': str(previous), 'intent': str(intent_path), 'index': str(index)}}
    finally:
        guard['active'] = False
        os.stat = real_stat
result['fixtureRemoved'] = not root.exists()
print(json.dumps(result))
`;

function inventoryScenario(fault: string) {
  const output = NodeChildProcess.execFileSync("python3", ["-c", inventoryFixture], {
    input: JSON.stringify({ files, fault }),
    encoding: "utf8",
    timeout: 15000,
    maxBuffer: 1024 * 1024,
  });
  const result = JSON.parse(output);
  expect(result.fixtureRemoved).toBe(true);
  expect(result.unchanged).toBe(true);
  expect(result.violations).toEqual([]);
  expect(result.controls).toBe(16);
  if (result.report !== null) {
    expect(result.report).toMatchObject({
      operation: "inventory-routes",
      activationAllowed: false,
      launchMembershipComplete: false,
      processCustody: "unobserved",
      observationOnly: true,
      blocker: "DESK-FIRST-01",
      coverage: {
        history: "partial",
        orphanSiblingApps: "unobserved",
        payloadDigests: "not-verified",
      },
    });
  }
  return result;
}

describe("read-only installer route inventory", () => {
  it("observes helpers, pre-receipt apps, nested executables, and exact retained siblings", () => {
    const result = inventoryScenario("observed");
    expect(result.code).toBe(0);
    expect(result.report.status).toBe("observed");
    expect(result.report.observations).toEqual(
      expect.arrayContaining([
        expect.objectContaining({
          kind: "helper",
          path: result.paths.helper,
          binding: "content-addressed",
        }),
        expect.objectContaining({ kind: "stage", path: result.paths.stage, binding: "unbound" }),
        expect.objectContaining({ kind: "executable", path: result.paths.nested }),
        expect.objectContaining({
          kind: "app",
          path: result.paths.incoming,
          role: "incoming",
          binding: "record",
        }),
        expect.objectContaining({
          kind: "app",
          path: result.paths.previous,
          role: "previous",
          binding: "record",
        }),
        expect.objectContaining({
          kind: "excluded",
          path: result.paths.mount,
          reason: "mount-not-traversed",
        }),
        expect.objectContaining({ kind: "completion-index", path: result.paths.index }),
        expect.objectContaining({ kind: "absent", path: expect.stringContaining("/artifacts") }),
      ]),
    );
    expect(result.report.issues).toContainEqual({
      code: "receipt-missing",
      path: `${result.paths.stage}/mac-app-receipt.json`,
    });
  });

  it.each(["invalid-anchor", "overlap-anchor"])(
    "refuses an unsafe manifest anchor before route traversal: %s",
    (fault) => {
      const result = inventoryScenario(fault);
      expect(result.code).toBe(1);
      expect(result.report.status).toBe("refused");
      expect(result.report.observations).toEqual([]);
    },
  );

  it.each([
    "partial-transaction",
    "foreign-intent",
    "partial-executor",
    "read-failed",
    "metadata-symlink",
  ])("reports partial observation and continues independent routes: %s", (fault) => {
    const result = inventoryScenario(fault);
    expect(result.code).toBe(2);
    expect(result.report.status).toBe("partial");
    expect(result.report.observations).toContainEqual(
      expect.objectContaining({ kind: "helper", path: result.paths.helper }),
    );
    expect(result.report.observations).toContainEqual(
      expect.objectContaining({ kind: "executable", path: result.paths.nested }),
    );
    if (fault === "foreign-intent") {
      expect(result.report.observations).not.toContainEqual(
        expect.objectContaining({ role: "incoming" }),
      );
    }
  });

  it("reports unresolved links without probing their protected targets", () => {
    const result = inventoryScenario("symlink");
    expect(result.code).toBe(0);
    expect(result.report.issues).toContainEqual(
      expect.objectContaining({ code: "unresolved-link" }),
    );
    expect(result.report.observations).toContainEqual(
      expect.objectContaining({ kind: "link", binding: "unknown" }),
    );
  });

  it.each(["manifest-race", "membership-race", "mode-race"])(
    "rechecks metadata and membership before reporting observation: %s",
    (fault) => {
      const result = inventoryScenario(fault);
      expect(result.code).toBe(2);
      expect(result.report.status).toBe("partial");
      expect(result.fixtureMutations).toBe(1);
      expect(result.report.issues).toContainEqual(
        expect.objectContaining({ code: "unreadable-or-changed" }),
      );
    },
  );

  it.each(["directoryEntries", "visits", "depth", "apps", "records", "readBytes"])(
    "bounds inventory work before exhausting %s",
    (bound) => {
      const result = inventoryScenario(`limit-${bound}`);
      expect(result.code).toBe(2);
      expect(result.report.status).toBe("partial");
      expect(result.report.issues).toContainEqual(
        expect.objectContaining({ code: "inventory-limit" }),
      );
      expect(result.report.observations.length + result.report.issues.length).toBeLessThanOrEqual(
        result.report.limits.records,
      );
      expect(result.report.usage.readBytes).toBeLessThanOrEqual(result.report.limits.readBytes);
      expect(result.report.usage.visits).toBeLessThanOrEqual(result.report.limits.visits + 1);
    },
  );

  it("rejects selection flags on the manifest-only command", () => {
    const result = inventoryScenario("selection-argument");
    expect(result.code).toBe(2);
    expect(result.report).toBeNull();
  });
});

it("schema 2 preserves standalone executor staging and never executes its extra tools", () => {
  const binary = Buffer.alloc(160);
  [0xfeedfacf, 0x0100000c, 0, 2, 2, 96, 0, 0].forEach((value, index) =>
    binary.writeUInt32LE(value, index * 4),
  );
  binary.writeUInt32LE(0x19, 32);
  binary.writeUInt32LE(72, 36);
  binary.write("__TEXT", 40, "ascii");
  binary.writeBigUInt64LE(160n, 80);
  binary.writeUInt32LE(0x80000028, 104);
  binary.writeUInt32LE(24, 108);
  binary.writeBigUInt64LE(128n, 112);
  const bundle = createJonesLegacyBootstrapBundle(
    {
      repository: "Jones-Systems/Jones-Code",
      sourceSha: "c".repeat(40),
      sourceTree: "d".repeat(40),
      version: "0.0.45-preview.20261011.123.1",
    },
    {
      identityNative: binary,
      identityNativeSourceSha256: "e".repeat(64),
      maintenanceSource: "raise RuntimeError('must not execute')\n",
    },
  );
  const wire = bundle.map((file) =>
    typeof file.content === "string"
      ? { ...file, content: file.content }
      : { ...file, content: "", binaryHex: Buffer.from(file.content).toString("hex") },
  );
  const result = scenario("success", wire);
  expect(result.code).toBe(0);
  expect(result.retry).toBe("no-change");
  expect(result.response.activationAllowed).toBe(false);
});

function syntheticBootstrapNative() {
  const binary = Buffer.alloc(160);
  [0xfeedfacf, 0x0100000c, 0, 2, 2, 96, 0, 0].forEach((value, index) =>
    binary.writeUInt32LE(value, index * 4),
  );
  binary.writeUInt32LE(0x19, 32);
  binary.writeUInt32LE(72, 36);
  binary.write("__TEXT", 40, "ascii");
  binary.writeBigUInt64LE(160n, 80);
  binary.writeUInt32LE(0x80000028, 104);
  binary.writeUInt32LE(24, 108);
  binary.writeBigUInt64LE(128n, 112);
  return binary;
}

function preparationBootstrapFiles() {
  // These bytes and proof fields are synthetic; the fixture forbids their execution.
  const native = syntheticBootstrapNative();
  const runtime = Buffer.concat([native, Buffer.alloc(1048576 + 17, 0x78)]);
  const hash = (bytes: string | Uint8Array) =>
    NodeCrypto.createHash("sha256").update(bytes).digest("hex");
  const identity = {
    repository: "Jones-Systems/Jones-Code" as const,
    sourceSha: "c".repeat(40),
    sourceTree: "d".repeat(40),
    version: "0.0.45-preview.20261011.123.1",
  };
  const config =
    '{"main":"offlinePreparationEntry.mjs","mainFormat":"module","executable":"node","output":"jones-offline-preparation","disableExperimentalSEAWarning":true,"useSnapshot":false,"useCodeCache":false,"execArgv":[],"execArgvExtension":"none"}\n';
  const evidence: JonesPreparationArtifactEvidence = {
    schema: 1,
    observation: "complete",
    classification: "cold-preparation-worker",
    source: identity,
    preparation: {
      schema: 1,
      protocol: 1,
      provenanceSha256: "1".repeat(64),
      sourceInputHashes: Object.fromEntries(
        JONES_PREPARATION_SOURCE_INPUTS.map((name) => [name, "2".repeat(64)]),
      ) as JonesPreparationArtifactEvidence["preparation"]["sourceInputHashes"],
      entry: { sha256: "3".repeat(64), size: 1 },
      seaConfig: { sha256: hash(config), size: Buffer.byteLength(config) },
      executable: {
        name: "jones-offline-preparation",
        sha256: hash(runtime),
        size: runtime.length,
      },
      node: {
        version: "26.8.2",
        platform: "darwin",
        architecture: "arm64",
        archiveSha256: "4".repeat(64),
        archiveBytes: 1,
        checksumsSha256: "5".repeat(64),
        runtimeSha256: "6".repeat(64),
        runtimeBytes: 1,
      },
      sqlite: { version: "3.50.0", sourceId: "2026-10-11 00:00:00 " + "7".repeat(64) },
    },
  };
  const bundle = createJonesLegacyBootstrapPreparationBundle(
    identity,
    {
      identityNative: native,
      identityNativeSourceSha256: "e".repeat(64),
      maintenanceSource: "raise RuntimeError('must not execute')\n",
    },
    evidence,
  );
  return [
    ...bundle.files.map((file) =>
      typeof file.content === "string"
        ? { ...file, content: file.content }
        : { ...file, content: "", binaryHex: Buffer.from(file.content).toString("hex") },
    ),
    {
      relativePath: bundle.runtime.relativePath,
      mode: bundle.runtime.mode,
      content: "",
      binaryHex: runtime.toString("hex"),
    },
  ];
}

describe("standalone schema 3 executor staging", () => {
  it("inspects and stages only the executor from six exact resources without executing the runtime", () => {
    const result = scenario("v3-success", preparationBootstrapFiles());
    expect(result.code).toBe(0);
    expect(result.retry).toBe("no-change");
    expect(result.proofAgreement).toBe(true);
    expect(result.runtimeQualification).toBe("synthetic-nonexecuted-resource");
    expect(result.runtimeExecuted).toBe(false);
    expect(result.response).toMatchObject({
      activationAllowed: false,
      observationOnly: true,
      blocker: "DESK-FIRST-01",
    });
  });

  it.each([
    "v3-old-inputs",
    "v3-extra-proof",
    "v3-runtime-bytes",
    "v3-runtime-mode",
    "v3-source",
    "v3-runtime-oversize",
    "v3-native-oversize",
    "v3-runtime-header",
    "v3-runtime-size",
    "v3-missing-runtime",
    "v3-extra-resource",
    "v3-sqlite-date",
    "v3-node-version",
    "v3-boolean-protocol",
    "v3-executor-mode",
    "v3-maintenance-bytes",
    "v3-canonical-proof",
    "v3-proof-oversize",
    "v3-bundle-mode",
  ])(
    "refuses malformed schema 3 inspection and staging while retaining the fixtures: %s",
    (fault) => {
      const result = scenario(fault, preparationBootstrapFiles());
      expect(result.code).toBe(1);
      expect(result.inspectionRefused).toBe(true);
      expect(result.proofAgreement).toBe(true);
      expect(result.runtimeExecuted).toBe(false);
      expect(result.response).toMatchObject({ status: "refused", activationAllowed: false });
    },
  );

  it.each([1, 2])("preserves the 8192-byte schema %i proof limit", (schema) => {
    const bundle =
      schema === 1
        ? files
        : createJonesLegacyBootstrapBundle(
            {
              repository: "Jones-Systems/Jones-Code",
              sourceSha: "c".repeat(40),
              sourceTree: "d".repeat(40),
              version: "0.0.45-preview.20261011.123.1",
            },
            {
              identityNative: syntheticBootstrapNative(),
              identityNativeSourceSha256: "e".repeat(64),
              maintenanceSource: "raise RuntimeError('must not execute')\n",
            },
          );
    const wire = bundle.map((file) =>
      typeof file.content === "string"
        ? { ...file, content: file.content }
        : { ...file, content: "", binaryHex: Buffer.from(file.content).toString("hex") },
    );
    const result = scenario("old-proof-oversize", wire);
    expect(result.code).toBe(1);
    expect(result.inspectionRefused).toBe(true);
  });
});
