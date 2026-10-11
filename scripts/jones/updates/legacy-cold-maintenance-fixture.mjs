import * as NodeAssert from "node:assert/strict";
import * as NodeCrypto from "node:crypto";
import * as NodeFSP from "node:fs/promises";
import * as NodePath from "node:path";
import * as NodeURL from "node:url";
import {
  runLegacyIdentityFixtureCommand,
  withLegacyIdentityScratch,
} from "./legacy-identity-native-fixture.mjs";

const repository = NodeURL.fileURLToPath(new URL("../../../", import.meta.url));
const updateSources = NodePath.join(repository, "apps/desktop/src/jones/updates");
const digest = (bytes) => NodeCrypto.createHash("sha256").update(bytes).digest("hex");

export const coldMaintenanceCases = [
  "success",
  "before-owner",
  "after-owner",
  "after-gate",
  "cancel-partial",
  "before-recovered-owner",
  "after-recovered-owner",
  "after-main-restore",
  "persistent-sync",
  "mode-drift",
  "identity-drift",
  "gate-reopened",
  "lock-replaced",
  "lock-busy",
  "late-copy",
  "unknown-helper",
  "receiptless",
];

function literal(source) {
  const marker = "String.raw`";
  const start = source.indexOf(marker);
  NodeAssert.ok(start >= 0 && source.trimEnd().endsWith("`;"));
  return source.slice(start + marker.length, source.lastIndexOf("`;"));
}

export function materializePairedExecutor(executorSource, pairedSource) {
  const declaration = "export const jonesPairedStateSource = Object.freeze({";
  NodeAssert.equal(pairedSource.split(declaration).length, 2);
  NodeAssert.ok(
    executorSource.includes(
      'import { jonesPairedStateSource } from "./jonesPairedStateSource.ts";',
    ),
  );
  let remaining = pairedSource
    .slice(pairedSource.indexOf(declaration) + declaration.length)
    .trimStart();
  let executor = literal(executorSource);
  // Read only the four declared literals; never evaluate TypeScript or expand
  // arbitrary substitutions in a source-qualified native fixture.
  for (const name of ["syncTree", "copyAndTopology", "recoveryAndPair", "syncParent"]) {
    const prefix = name + ": String.raw`";
    NodeAssert.ok(remaining.startsWith(prefix), "Unexpected paired source fragment.");
    const end = remaining.indexOf("`", prefix.length);
    NodeAssert.ok(end >= prefix.length, "Unterminated paired source fragment.");
    const fragment = remaining.slice(prefix.length, end);
    NodeAssert.ok(!fragment.includes("${"), "Paired source fragments must be literal.");
    remaining = remaining.slice(end + 1).trimStart();
    NodeAssert.ok(remaining.startsWith(","), "Unexpected paired source expression.");
    remaining = remaining.slice(1).trimStart();
    const reference = "${jonesPairedStateSource." + name + "}";
    NodeAssert.equal(
      executor.split(reference).length,
      2,
      "Each paired source fragment must occur exactly once.",
    );
    executor = executor.replace(reference, () => fragment);
  }
  NodeAssert.equal(remaining.trimEnd(), "} as const);");
  NodeAssert.ok(
    !executor.includes("${") && !executor.includes("`"),
    "Unresolved executor substitution.",
  );
  return executor;
}

export async function readColdMaintenanceSources() {
  const [bootstrapText, maintenanceText, executorText, pairedText, nativeSource] =
    await Promise.all([
      NodeFSP.readFile(NodePath.join(updateSources, "jonesLegacyBootstrapSource.ts"), "utf8"),
      NodeFSP.readFile(NodePath.join(updateSources, "jonesLegacyMaintenanceSource.ts"), "utf8"),
      NodeFSP.readFile(NodePath.join(updateSources, "jonesNativeHelperSource.ts"), "utf8"),
      NodeFSP.readFile(NodePath.join(updateSources, "jonesPairedStateSource.ts"), "utf8"),
      NodeFSP.readFile(NodePath.join(updateSources, "jonesLegacyIdentityNative.c")),
    ]);
  const bootstrap = literal(bootstrapText);
  const suffix = "if __name__ == '__main__':\n    sys.exit(main())\n";
  NodeAssert.ok(bootstrap.endsWith(suffix));
  return {
    bootstrap,
    maintenance: bootstrap.slice(0, -suffix.length) + literal(maintenanceText),
    executor: materializePairedExecutor(executorText, pairedText),
    nativeSourceSha256: digest(nativeSource),
  };
}

// This driver exercises the real source and native observer on an owned synthetic
// family. It never fabricates a kernel identity or runs an app or update helper.
const driver = String.raw`
import sys
sys.dont_write_bytecode = True
import contextlib, fcntl, hashlib, io, json, os, pathlib, plistlib, signal, stat, struct, subprocess, traceback, zipfile

payload = json.loads(pathlib.Path(sys.argv[1]).read_text())
root = pathlib.Path(sys.argv[2]).resolve()
case = payload['case']
scope = {'__name__': 'cold_maintenance_fixture'}
exec(compile(payload['maintenance'], 'maintenance.py', 'exec'), scope)
failures = []
production_require = scope['require']
def diagnostic_require(condition):
    if not condition:
        frames = [{'function': frame.name, 'line': frame.lineno} for frame in traceback.extract_stack(limit=8)]
        native = sys._getframe(1).f_locals.get('value')
        if isinstance(native, dict) and native.get('operation') == 'inspect':
            frames.append({key: native.get(key) for key in ('status', 'reason', 'failingIndex', 'errno')})
        failures.append(frames)
    production_require(condition)
scope['require'] = diagnostic_require
signal.signal(signal.SIGTERM, lambda number, frame: (_ for _ in ()).throw(SystemExit(128 + number)))
native_children = set()
real_popen = subprocess.Popen
class CapturedNative(real_popen):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        native_children.add(self)
    def __exit__(self, *args):
        try: return super().__exit__(*args)
        finally:
            if self.returncode is not None: native_children.discard(self)
subprocess.Popen = CapturedNative

def encode(value): return json.dumps(value, separators=(',', ':'), ensure_ascii=False).encode()
def sha(raw): return hashlib.sha256(raw).hexdigest()
def write(path, raw, mode=0o600):
    assert path.is_relative_to(root)
    path.parent.mkdir(parents=True, exist_ok=True)
    if path.exists():
        info = path.lstat()
        assert stat.S_ISREG(info.st_mode) and info.st_uid == os.getuid() and info.st_nlink == 1
        path.chmod(stat.S_IMODE(info.st_mode) | 0o200)
    path.write_bytes(raw)
    path.chmod(mode)
def app(path, source, tree, version, tools=False):
    package = {'version': version, 'jonesSource': {'repository': 'Jones-Systems/Jones-Code', 'sha': source, 'tree': tree}}
    if tools: package['startupGateProtocol'] = 1
    packed = encode(package)
    header = encode({'files': {'package.json': {'size': len(packed), 'offset': '0'}}})
    write(path / 'Contents/Resources/app.asar', struct.pack('<IIII', 4, len(header) + 8, len(header) + 4, len(header)) + header + packed)
    write(path / 'Contents/Info.plist', plistlib.dumps({'CFBundleExecutable': 'Jones', 'CFBundleIdentifier': 'test.jones.cold', 'CFBundleShortVersionString': version}))
    write(path / 'Contents/MacOS/Jones', b'not-an-executable-fixture', 0o500)
    write(path / 'Contents/Frameworks/Nested Helper', b'not-a-helper-fixture', 0o750)
    (path / 'Contents/ResourcesAlias').symlink_to('Resources', target_is_directory=True)
    if tools:
        resources = path / 'Contents/Resources/jones-bootstrap'
        native = pathlib.Path(payload['nativePath']).read_bytes()
        manifest = {'schema': 2, 'repository': 'Jones-Systems/Jones-Code', 'sourceSha': source, 'sourceTree': tree, 'version': version,
                    'bootstrapSha256': sha(payload['bootstrap'].encode()), 'executorSha256': sha(payload['executor'].encode()),
                    'maintenanceSha256': sha(payload['maintenance'].encode()), 'identityNativeSha256': sha(native),
                    'identityNativeSourceSha256': payload['nativeSourceSha256'], 'nativeArchitecture': 'arm64'}
        for name in ('bootstrap', 'executor', 'maintenance'): write(resources / (name + '.py'), payload[name].encode(), 0o500)
        write(resources / 'identity-native', native, 0o500)
        write(resources / 'BUNDLE.json', encode(manifest), 0o400)

home, profile = root / 'home', root / 'profile'
updater = home / 'runtime/jones-updates'
updater.mkdir(parents=True)
database = home / 'userdata/statev2.sqlite'
write(database, b'private fixture database')
write(profile / 'sentinel', b'private fixture profile')
old = root / 'Applications/Installed.app'
old_version = '0.0.45-preview.20261011.38108866317.1'
app(old, scope['LEGACY_SOURCE'], 'b' * 40, old_version)
active = {'protocol': 1, 'owner': 'desktop', 'generation': '1' * 64, 'transactionId': 'bootstrap', 'home': str(home),
          'databasePath': str(database), 'profile': str(profile), 'environmentId': 'synthetic-cold-family',
          'appPath': str(old), 'executablePath': str(old / 'Contents/MacOS/Jones'), 'version': old_version,
          'sourceSha': scope['LEGACY_SOURCE'], 'sourceTree': 'b' * 40, 'bundleIdentifier': 'test.jones.cold'}
manifest = home / 'runtime/jones-active-install.json'
write(manifest, encode(active))
candidate = {'schema': 1, 'repository': 'Jones-Systems/Jones-Code', 'source': 'c' * 40, 'tree': 'd' * 40,
             'installedSource': active['sourceSha'], 'workflow': '.github/workflows/artifact-desktop-mac.yml', 'workflowId': 11,
             'runId': 123, 'runAttempt': 1, 'ciRunId': 456, 'artifactId': 789,
             'artifactName': 'desktop-mac-arm64-123-1--0.0.45-preview.20261011.123.1', 'artifactDigest': '', 'artifactBytes': 0,
             'expiresAt': '2099-12-01T00:00:00Z', 'version': '0.0.45-preview.20261011.123.1', 'platform': 'darwin', 'architecture': 'arm64'}
dmg = b'synthetic-prestaged-payload'
receipt = {key: candidate[key] for key in ('repository', 'source', 'tree', 'workflow', 'version', 'platform', 'architecture')}
receipt.update(schema=1, event='push', ref='refs/heads/main', runId='123', runAttempt='1', artifact='T3-Code-' + candidate['version'] + '-arm64.dmg', sha256=sha(dmg))
archive = io.BytesIO()
with zipfile.ZipFile(archive, 'w') as packed:
    packed.writestr('ARTIFACT.json', encode(receipt))
    packed.writestr(receipt['artifact'], dmg)
raw_archive = archive.getvalue()
candidate.update(artifactDigest=sha(raw_archive), artifactBytes=len(raw_archive))
handle = sha(encode([candidate[key] for key in scope['CANDIDATE_FIELDS']]))
artifact_dir, app_dir = updater / 'artifacts' / handle, updater / 'apps' / handle
candidate_app = app_dir / 'Candidate.app'
app(candidate_app, candidate['source'], candidate['tree'], candidate['version'], True)
artifact = {'schema': 1, 'source': 'jones-actions', 'channel': 'jones-main', 'stagedHandle': handle, 'candidate': candidate,
            'receipt': receipt, 'payloadPath': str(artifact_dir / receipt['artifact'])}
write(artifact_dir / receipt['artifact'], dmg)
write(artifact_dir / 'github-artifact.zip', raw_archive)
write(artifact_dir / 'jones-actions-stage.json', encode(artifact))
staged = {'handle': handle, 'receiptPath': str(app_dir / 'mac-app-receipt.json'), 'appPath': str(candidate_app),
          'executablePath': str(candidate_app / 'Contents/MacOS/Jones'), 'version': candidate['version'], 'sourceSha': candidate['source'], 'sourceTree': candidate['tree'],
          'appDigest': scope['app_digest'](scope['Observation'](), candidate_app),
          'asarDigest': sha((candidate_app / 'Contents/Resources/app.asar').read_bytes()),
          'executableDigest': sha((candidate_app / 'Contents/MacOS/Jones').read_bytes()), 'bundleIdentifier': 'test.jones.cold', 'startupGateProtocol': 1}
write(app_dir / 'mac-app-receipt.json', encode({'app': staged, 'artifact': artifact, 'candidate': candidate}))
transaction = updater / 'transactions' / ('d' * 64)
prior = dict(active, generation='2' * 64, appDigest=scope['app_digest'](scope['Observation'](), old))
intent = {'protocol': 1, 'transactionId': transaction.name, 'expected': prior, 'staged': staged, 'continuationReceipt': str(transaction / 'continuation.json')}
write(transaction / 'intent.json', encode(intent))
write(transaction / 'journal.json', encode({'intent': intent, 'phase': 'rolled-back'}))
write(transaction / 'previous-pair/state.sqlite', b'private retained pair')
write(transaction / 'advanced-state/state.sqlite', b'private advanced state')
retained_app = old.with_name(old.name + '.jones-previous-' + prior['generation'])
app(retained_app, active['sourceSha'], active['sourceTree'], active['version'])
selection = {'schema': 1, 'source': 'jones-actions', 'home': str(home), 'profile': str(profile), 'currentVersion': active['version'],
             'installedSource': active['sourceSha'], 'active': active, 'artifactDirectory': str(artifact_dir), 'app': staged}
selection_raw = encode(selection)
selection_path = updater / 'staging' / (active['sourceSha'] + '-' + 'e' * 64 + '.json')
write(selection_path, selection_raw)
write(home / 'bin/t3', scope['expected_shim'](home, active), 0o750)

guard = {'enabled': False, 'violations': []}
def permitted(value):
    if not guard['enabled'] or not isinstance(value, (str, bytes, os.PathLike)): return
    text = os.fsdecode(value)
    path = pathlib.Path(text)
    private = (database.parent, profile, transaction / 'previous-pair', transaction / 'advanced-state')
    if text in ('userdata', 'profile', 'statev2.sqlite', 'sentinel', 'previous-pair', 'advanced-state') or any(path == denied or path.is_relative_to(denied) for denied in private):
        guard['violations'].append(text)
        raise AssertionError('Private state metadata or contents were probed')
def audit(event, args):
    if not guard['enabled']: return
    if event in ('open', 'os.listdir', 'os.scandir') and args: permitted(args[0])
    if event == 'subprocess.Popen':
        executable = pathlib.Path(args[0])
        assert executable.name == 'identity-native' and executable.is_relative_to(root)
    if event in ('os.system', 'os.exec', 'os.posix_spawn', 'sqlite3.connect'):
        raise AssertionError('Unexpected execution or state access')
sys.addaudithook(audit)
for name in ('stat', 'lstat', 'readlink'):
    original = getattr(os, name)
    def guarded(path, *args, _original=original, **kwargs):
        permitted(path)
        return _original(path, *args, **kwargs)
    setattr(os, name, guarded)

real_durable, real_chmod, real_sync = scope['durable_file'], os.fchmod, scope['sync_fd']
fault = {'enabled': True, 'fired': False}
def fail():
    fault['fired'] = True
    raise OSError('injected durability interruption')
def durable(path, raw, mode, previous=None):
    owner = json.loads(raw).get('owner') if path == manifest else None
    recovered = owner == 'desktop' and json.loads(raw)['generation'] != active['generation']
    if fault['enabled'] and not fault['fired']:
        if case == 'before-owner' and owner == 'maintenance': fail()
        if case == 'before-recovered-owner' and recovered: fail()
    real_durable(path, raw, mode, previous)
    if fault['enabled'] and not fault['fired']:
        if case == 'after-owner' and owner == 'maintenance': fail()
        if case == 'after-recovered-owner' and recovered: fail()
def chmod(fd, mode):
    real_chmod(fd, mode)
    if not fault['enabled'] or fault['fired']: return
    info = os.fstat(fd)
    main = (old / 'Contents/MacOS/Jones').stat()
    if info.st_ino == main.st_ino:
        if case in ('after-gate', 'cancel-partial') and mode == 0o400: fail()
        if case == 'after-main-restore' and mode == 0o500: fail()
def sync(fd):
    if fault['enabled'] and case == 'persistent-sync':
        info, main = os.fstat(fd), (old / 'Contents/MacOS/Jones').stat()
        if info.st_ino == main.st_ino and stat.S_IMODE(info.st_mode) == 0o400: fail()
    return real_sync(fd)
scope['durable_file'], scope['sync_fd'], os.fchmod = durable, sync, chmod

def invoke(operation):
    if operation == 'arm':
        scope['__file__'] = str(candidate_app / 'Contents/Resources/jones-bootstrap/maintenance.py')
        arguments = ['arm', '--manifest', str(manifest), '--selection', str(selection_path), '--selection-sha256', sha(selection_raw)]
    else:
        pointer = json.loads((home / 'runtime/jones-maintenance/current.json').read_text())
        scope['__file__'] = str(home / 'runtime/jones-maintenance' / pointer['generation'] / 'tools/maintenance.py')
        arguments = [operation, '--manifest', str(manifest)]
    output = io.StringIO()
    guard['enabled'] = True
    try:
        with contextlib.redirect_stdout(output): code = scope['maintenance_main'](arguments)
    finally: guard['enabled'] = False
    report = json.loads(output.getvalue())
    assert report['activationAllowed'] is False and report['stateAdmissionAllowed'] is False and report['launchMembershipComplete'] is False
    assert report['processCustody'] == 'unobserved'
    return code, report

report = None
busy_lock = None
try:
    if case == 'unknown-helper': write(updater / ('activate-v1-' + 'f' * 64 + '.py'), b'unknown helper')
    if case == 'receiptless': (updater / 'apps/attempts/stage-Unknown' / ('f' * 64) / 'Unbound.app').mkdir(parents=True)
    if case == 'lock-busy':
        write(manifest.parent / 'jones-activation.lock', scope['LOCK_BYTES'])
        busy_lock = os.open(manifest.parent / 'jones-activation.lock', os.O_RDWR)
        fcntl.flock(busy_lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
    code, report = invoke('arm')
    if case in ('unknown-helper', 'receiptless', 'lock-busy'):
        assert code == 1 and json.loads(manifest.read_text()) == active
        assert stat.S_IMODE((old / 'Contents/MacOS/Jones').stat().st_mode) == 0o500
    else:
        if case in ('before-owner', 'after-owner', 'after-gate', 'cancel-partial', 'persistent-sync'):
            assert code == 1 and fault['fired']
            if case == 'persistent-sync':
                code, report = invoke('reconcile')
                assert code == 1 and report['status'] == 'held'
            fault['enabled'] = False
            code, report = invoke('cancel' if case in ('cancel-partial', 'before-owner') else 'reconcile')
        assert code == 0, (report, failures[-1:])
        if case not in ('cancel-partial', 'before-owner'):
            assert report['status'] == 'armed-awaiting-cold-restart'
            code, repeat = invoke('reconcile')
            assert code == 0 and repeat['status'] == 'armed-awaiting-cold-restart'
        if case in ('mode-drift', 'identity-drift', 'gate-reopened', 'lock-replaced', 'late-copy'):
            main = old / 'Contents/MacOS/Jones'
            if case == 'mode-drift': main.chmod(0)
            if case == 'gate-reopened': main.chmod(0o500)
            if case == 'lock-replaced':
                lock = manifest.parent / 'jones-activation.lock'
                lock.unlink()
                write(lock, scope['LOCK_BYTES'])
            if case == 'identity-drift':
                saved = main.read_bytes()
                main.unlink()
                write(main, saved, 0o400)
            if case == 'late-copy': (updater / 'apps/attempts/stage-Late' / ('f' * 64) / 'Late.app').mkdir(parents=True)
            code, report = invoke('cancel')
            assert code == 1 and report['status'] == 'held'
            assert json.loads(manifest.read_text())['owner'] == 'maintenance'
            assert stat.S_IMODE(main.stat().st_mode) == (0o500 if case == 'gate-reopened' else 0 if case == 'mode-drift' else 0o400)
        else:
            code, report = invoke('cancel')
            if case in ('before-recovered-owner', 'after-recovered-owner', 'after-main-restore'):
                assert code == 1 and fault['fired']
                fault['enabled'] = False
                code, report = invoke('reconcile')
            assert code == 0 and report['status'] == 'cancelled-pre-state', report
            recovered = json.loads(manifest.read_text())
            assert recovered == dict(active, generation=report['recoveredGeneration'])
            assert recovered['generation'] != active['generation']
            assert stat.S_IMODE((old / 'Contents/MacOS/Jones').stat().st_mode) == 0o500
            assert stat.S_IMODE((old / 'Contents/Frameworks/Nested Helper').stat().st_mode) == 0o750
            assert stat.S_IMODE((candidate_app / 'Contents/MacOS/Jones').stat().st_mode) == 0o400
            assert stat.S_IMODE((retained_app / 'Contents/MacOS/Jones').stat().st_mode) == 0o400
            code, repeated = invoke('cancel')
            assert code == 0 and repeated == report
    assert not guard['violations']
    print(json.dumps({'case': case, 'passed': True, 'qualification': 'synthetic-pre-state-same-kernel', 'stateAdmissionAllowed': False}))
finally:
    guard['enabled'] = False
    if busy_lock is not None: os.close(busy_lock)
    for child in tuple(native_children):
        if child.poll() is None: child.terminate()
        try: child.wait(timeout=5)
        except subprocess.TimeoutExpired:
            child.kill()
            child.wait(timeout=5)
        native_children.discard(child)
    print(json.dumps({'nativeChildrenClosed': not native_children}), file=sys.stderr)
    with (root / 'native-children-closed.json').open('x') as closed:
        json.dump({'nativeChildrenClosed': not native_children}, closed)
        closed.flush()
        os.fsync(closed.fileno())
`;

export async function runColdMaintenanceFixture(scenario, { signal } = {}) {
  NodeAssert.ok(coldMaintenanceCases.includes(scenario));
  const sources = await readColdMaintenanceSources();
  return withLegacyIdentityScratch(async (root) => {
    const native = NodePath.join(root, "identity-native");
    const compilation = await runLegacyIdentityFixtureCommand(
      "/usr/bin/clang",
      [
        "-std=c11",
        "-Wall",
        "-Wextra",
        "-Werror",
        "-O2",
        "-o",
        native,
        NodePath.join(updateSources, "jonesLegacyIdentityNative.c"),
      ],
      root,
      { signal },
    );
    NodeAssert.equal(compilation.code, 0, compilation.stderr);
    await NodeFSP.chmod(native, 0o500);
    const payload = NodePath.join(root, "fixture.json");
    const entry = NodePath.join(root, "fixture.py");
    await NodeFSP.writeFile(
      payload,
      JSON.stringify({ ...sources, case: scenario, nativePath: native }),
    );
    await NodeFSP.writeFile(entry, driver);
    try {
      const result = await runLegacyIdentityFixtureCommand(
        "/usr/bin/python3",
        ["-I", "-B", entry, payload, root],
        root,
        { signal },
      );
      NodeAssert.equal(result.code, 0, result.stderr);
      NodeAssert.match(result.stderr, /"nativeChildrenClosed": true/);
      const report = JSON.parse(result.stdout.trim());
      NodeAssert.equal(report.case, scenario);
      NodeAssert.equal(report.passed, true);
      return report;
    } catch (cause) {
      // A killed Python driver without its final receipt cannot prove closure of
      // its captured native children. Preserve this exact scratch in that case.
      const closure = await NodeFSP.readFile(
        NodePath.join(root, "native-children-closed.json"),
        "utf8",
      )
        .then((raw) => JSON.parse(raw))
        .catch(() => undefined);
      if (closure?.nativeChildrenClosed !== true) cause.preserveScratch = true;
      throw cause;
    }
  });
}
