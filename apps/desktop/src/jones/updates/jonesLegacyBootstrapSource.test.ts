// @effect-diagnostics nodeBuiltinImport:off - The synthetic Python fixture invokes only the inert public bootstrap command.
import * as NodeChildProcess from "node:child_process";
import { describe, expect, it } from "vite-plus/test";
import { createJonesLegacyBootstrapBundle } from "../../../../../scripts/jones/updates/legacy-bootstrap-bundle.ts";

const fixture = String.raw`
import contextlib, hashlib, io, json, os, pathlib, plistlib, signal, struct, sys, tempfile, zipfile

payload = json.load(sys.stdin)
fault = payload['fault']
files = payload['files']
bootstrap_source = files[0]['content']
namespace = {'__name__': 'bootstrap_fixture'}
exec(compile(bootstrap_source, 'bootstrap.py', 'exec'), namespace)
# The command has no platform-specific mutations. Linux CI covers its Python
# protocol; the separate packaged Mac qualifier invokes the unmodified platform gate.
namespace['sys'].platform = 'darwin'
signal.signal(signal.SIGTERM, lambda number, frame: (_ for _ in ()).throw(SystemExit(128 + number)))

def encode(value):
    return json.dumps(value, separators=(',', ':'), ensure_ascii=False).encode()

def digest(data):
    return hashlib.sha256(data).hexdigest()

def write(path, data, mode=0o600):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(data)
    path.chmod(mode)

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
            write(app / 'Contents/Resources' / item['relativePath'], item['content'].encode(), item['mode'])

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
    if fault == 'selection-changed': write(selection_path, selection_raw + b' ')
    elif fault == 'manifest-changed': write(manifest, encode(dict(active, generation='2' * 64)))
    elif fault == 'candidate-changed': write(candidate_app / 'Contents/MacOS/Jones', b'changed', 0o500)
    elif fault == 'protected-link': (candidate_app / 'Contents/Resources/protected-link').symlink_to(database)
    elif fault == 'archive-changed': write(artifact_dir / 'github-artifact.zip', b'changed')
    elif fault == 'bundle-changed': write(entry.parent / 'BUNDLE.json', b'{}')
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
        import stat
        os.fsync = racing_sync
    elif fault == 'unsupported-command':
        try: invoke('activate')
        except SystemExit as error: result['argumentExit'] = error.code
        else: raise AssertionError('Activation command was accepted')
    if fault != 'unsupported-command':
        code, response = invoke('stage-executor')
        result.update(code=code, response=response)
        if code == 0:
            assert (output / 'executor.py').read_bytes() == files[1]['content'].encode()
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
    assert database.read_bytes() == b'private-database-must-not-open'
    assert (profile / 'profile-sentinel').read_bytes() == b'private-profile-must-not-open'
    assert guard['forbidden'] == []
    assert not list(updater.rglob('commit-grant.json')) and not list(updater.rglob('trial-descriptor.json'))
    result['protectedAccesses'] = guard['forbidden']
result['fixtureRemoved'] = not root.exists()
print(json.dumps(result))
`;

const files = createJonesLegacyBootstrapBundle({
  repository: "Jones-Systems/Jones-Code",
  sourceSha: "c".repeat(40),
  sourceTree: "d".repeat(40),
  version: "0.0.45-preview.20261011.123.1",
});

function scenario(fault: string) {
  const output = NodeChildProcess.execFileSync("python3", ["-c", fixture], {
    input: JSON.stringify({ files, fault }),
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

  it("has no activation command", () => {
    expect(scenario("unsupported-command").argumentExit).toBe(2);
  });
});
