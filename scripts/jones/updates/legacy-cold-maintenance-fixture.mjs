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

const nativePairCases = {
  "fd-native-clone": "roundtrip",
  "fd-native-fallback": "native-fallback",
  "fd-profile-volume": "profile-volume",
  "fd-nofollow-source": "nofollow-copy-source",
  "fd-nofollow-target": "nofollow-copy-target",
  "fd-nofollow-observe": "nofollow-before-open",
};

export const coldMaintenanceCases = [
  ...Object.keys(nativePairCases),
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
  "state-fence",
  "state-fence-missing",
  "state-fence-corrupt",
];

function literal(source) {
  const marker = "String.raw`";
  const start = source.indexOf(marker);
  NodeAssert.ok(start >= 0 && source.trimEnd().endsWith("`;"));
  return source.slice(start + marker.length, source.lastIndexOf("`;"));
}

function pairedFragments(pairedSource) {
  const declaration = "export const jonesPairedStateSource = Object.freeze({";
  NodeAssert.equal(pairedSource.split(declaration).length, 2);
  let remaining = pairedSource
    .slice(pairedSource.indexOf(declaration) + declaration.length)
    .trimStart();
  const fragments = {};
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
    fragments[name] = fragment;
  }
  NodeAssert.equal(remaining.trimEnd(), "} as const);");
  return fragments;
}

export function materializePairedExecutor(executorSource, pairedSource) {
  NodeAssert.ok(
    executorSource.includes(
      'import { jonesPairedStateSource } from "./jonesPairedStateSource.ts";',
    ),
  );
  let executor = literal(executorSource);
  for (const [name, fragment] of Object.entries(pairedFragments(pairedSource))) {
    const reference = "${jonesPairedStateSource." + name + "}";
    NodeAssert.equal(executor.split(reference).length, 2);
    executor = executor.replace(reference, () => fragment);
  }
  NodeAssert.ok(
    !executor.includes("${") && !executor.includes("`"),
    "Unresolved executor substitution.",
  );
  return executor;
}

export function materializeColdMaintenance(bootstrapText, maintenanceText, coldText, pairedText) {
  NodeAssert.ok(
    maintenanceText.includes('import { jonesColdPairSource } from "./jonesColdPairSource.ts";'),
  );
  NodeAssert.equal(maintenanceText.split("  jonesColdPairSource +").length, 2);
  NodeAssert.ok(
    coldText.includes('import { jonesPairedStateSource } from "./jonesPairedStateSource.ts";'),
  );
  let cold = literal(coldText);
  // Only these four literals and their fixed four-space indentation are supported.
  for (const [name, fragment] of Object.entries(pairedFragments(pairedText))) {
    const reference = "${indentColdStorage(jonesPairedStateSource." + name + ")}";
    NodeAssert.equal(cold.split(reference).length, 2);
    cold = cold.replace(reference, () =>
      fragment
        .split("\n")
        .map((line) => "    " + line)
        .join("\n"),
    );
  }
  const bootstrap = literal(bootstrapText);
  const suffix = "if __name__ == '__main__':\n    sys.exit(main())\n";
  NodeAssert.ok(bootstrap.endsWith(suffix));
  const result = bootstrap.slice(0, -suffix.length) + cold + literal(maintenanceText);
  NodeAssert.ok(
    !result.includes("${") && !result.includes("`"),
    "Unresolved maintenance substitution.",
  );
  return result;
}

export async function readColdMaintenanceSources() {
  const [bootstrapText, maintenanceText, executorText, pairedText, coldText, nativeSource] =
    await Promise.all([
      NodeFSP.readFile(NodePath.join(updateSources, "jonesLegacyBootstrapSource.ts"), "utf8"),
      NodeFSP.readFile(NodePath.join(updateSources, "jonesLegacyMaintenanceSource.ts"), "utf8"),
      NodeFSP.readFile(NodePath.join(updateSources, "jonesNativeHelperSource.ts"), "utf8"),
      NodeFSP.readFile(NodePath.join(updateSources, "jonesPairedStateSource.ts"), "utf8"),
      NodeFSP.readFile(NodePath.join(updateSources, "jonesColdPairSource.ts"), "utf8"),
      NodeFSP.readFile(NodePath.join(updateSources, "jonesLegacyIdentityNative.c")),
    ]);
  const bootstrap = literal(bootstrapText);
  const suffix = "if __name__ == '__main__':\n    sys.exit(main())\n";
  NodeAssert.ok(bootstrap.endsWith(suffix));
  return {
    bootstrap,
    maintenance: materializeColdMaintenance(bootstrapText, maintenanceText, coldText, pairedText),
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
        if case.startswith('state-fence'):
            pointer = json.loads((home / 'runtime/jones-maintenance/current.json').read_text())
            directory = home / 'runtime/jones-maintenance' / pointer['generation']
            journal_path = directory / 'journal.json'
            journal = json.loads(journal_path.read_text())
            journal['stateTransaction'] = {'schema': 1, 'transactionId': '7' * 64, 'intentSha256': '8' * 64}
            journal_path.write_bytes(encode(journal) + b'\n')
            if case != 'state-fence-missing':
                (directory / 'state').mkdir(mode=0o700)
                write(directory / 'state/intent.json', b'not-json' if case == 'state-fence-corrupt' else b'{}')
            before = manifest.read_bytes()
            code, report = invoke('cancel')
            assert code == 1 and report['status'] == 'held' and manifest.read_bytes() == before
            assert stat.S_IMODE((old / 'Contents/MacOS/Jones').stat().st_mode) == 0o400
            assert json.loads(journal_path.read_text()) == journal
        elif case in ('mode-drift', 'identity-drift', 'gate-reopened', 'lock-replaced', 'late-copy'):
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
  if (Object.hasOwn(nativePairCases, scenario)) {
    const report = await runColdPairFixture(nativePairCases[scenario], {
      signal,
      nativeStorage: true,
    });
    NodeAssert.equal(report.nativeStorage.platform, "darwin");
    NodeAssert.equal(report.nativeStorage.coldOwnerQualified, false);
    return {
      ...report,
      case: scenario,
      storageQualification: report.qualification,
      qualification: "synthetic-pre-state-same-kernel",
    };
  }
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

// Portable storage tests replace the resolver only in this owned namespace.
// They exercise FD-bound storage on owned files, not native cold custody.
const pairDriver = String.raw`
import sys
sys.dont_write_bytecode = True
import json, os, pathlib, stat, subprocess, types
payload = json.loads(pathlib.Path(sys.argv[1]).read_text())
root = pathlib.Path(sys.argv[2]).resolve(strict=True)
scope = {'__name__': 'owned_cold_pair_fixture'}
exec(compile(payload['maintenance'], 'maintenance.py', 'exec'), scope)
scenario = payload['case']
class Interrupted(BaseException): pass
def expect_refusal(operation):
    try: operation()
    except (scope['Refused'], OSError, ValueError, KeyError, TypeError): return
    raise AssertionError('Expected retained refusal')
if scenario == 'held':
    class NoAccess:
        def __getattribute__(self, name): raise AssertionError('Production resolver accessed state')
    expect_refusal(lambda: scope['_ColdPairTransaction'](NoAccess()))
else:
    for name in ('runtime', 'state', 'profile', 'generation'):
        (root / name).mkdir(mode=0o700)
    def write(path, content, mode=0o600):
        path.write_bytes(content)
        path.chmod(mode)
    database = root / 'state/state.sqlite'
    write(database, b'original-db', 0o640)
    write(root / 'state/settings.json', b'{"keep":true}')
    write(root / 'profile/Preferences', b'profile-before')
    (root / 'profile/nested').mkdir(mode=0o700)
    write(root / 'profile/nested/value', b'nested-before')
    (root / 'profile/link').symlink_to('nested/value')
    (root / 'profile/SingletonLock').symlink_to('unused-lock')
    (root / 'profile/nested/SingletonSocket').symlink_to('unused-socket')
    if scenario == 'sidecars':
        write(pathlib.Path(str(database) + '-wal'), b'original-wal')
        write(pathlib.Path(str(database) + '-shm'), b'original-shm')
    owner = types.SimpleNamespace(
        directory=root / 'generation',
        original={'databasePath': str(database), 'profile': str(root / 'profile'), 'generation': '1' * 64, 'sourceSha': '2' * 40},
        plan={'maintenanceGeneration': '3' * 64, 'originalManifestSha256': '4' * 64, 'candidateBinding': {'sourceSha': '5' * 40, 'executorSha256': '6' * 64}},
        journal={'phase': 'armed', 'planSha256': '7' * 64})
    owner.journal_raw = scope['encoded'](owner.journal) + b'\n'
    scope['durable_file'](owner.directory / 'journal.json', owner.journal_raw, 0o600)
    owner.save = lambda **updates: scope['Maintenance'].save(owner, **updates)
    live = {'current': True}
    root_proof = (root.stat().st_dev, root.stat().st_ino)
    class OwnedRuntime:
        def check(self, actual):
            assert actual is owner and live['current']
            assert (root.stat().st_dev, root.stat().st_ino) == root_proof
    runtime = OwnedRuntime()
    scope['_cold_pair_runtime'] = lambda: runtime
    # Portable inode evidence is deliberately labelled mechanical, not native APFS proof.
    def portable_identity(actual, path, kind):
        assert actual is owner and path.is_relative_to(root)
        info = path.lstat()
        proof = [str(info.st_dev), str(info.st_ino), info.st_uid, stat.S_IMODE(info.st_mode)]
        if kind == 'file': proof += [info.st_size, info.st_mtime_ns]
        return proof
    scope['_cold_pair_identity'] = portable_identity
    scope['_COLD_PAIR_HEADROOM'] = 16384
    native_storage = payload.get('nativeStorage', False)
    clone_counts = {'attempted': 0, 'cloned': 0, 'fallback': 0}
    original_clone = scope['_cold_pair_clone']
    def tracked_clone(source, parent, name):
        clone_counts['attempted'] += 1
        cloned = original_clone(source, parent, name) if native_storage and scenario != 'native-fallback' else False
        clone_counts['cloned' if cloned else 'fallback'] += 1
        return cloned
    scope['_cold_pair_clone'] = tracked_clone
    def no_subprocess(*args, **kwargs): raise AssertionError('FD-bound storage launched a child')
    subprocess.run = no_subprocess
    transaction_type = scope['_ColdPairTransaction']
    with scope['fixed_lock'](root / 'runtime/jones-active-install.json') as current_lock:
        owner.current_lock = current_lock
        tx = transaction_type(owner)
        initial = tx.observe_members()
        if scenario == 'profile-volume':
            original_identity = tx.root_identity
            observed_profile = []
            def foreign_profile(path):
                result = original_identity(path)
                if path == root / 'profile':
                    observed_profile.append(path)
                    result = dict(result, device=str(int(result['device']) + 1))
                return result
            tx.root_identity = foreign_profile
            expect_refusal(tx.snapshot)
            assert observed_profile and 'stateTransaction' not in owner.journal
            assert not tx.root.exists() and transaction_type(owner).observe_members() == initial
        elif scenario.startswith('nofollow-') or scenario == 'bounded-readback':
            outside = root / 'outside'
            outside.mkdir(mode=0o700)
            write(outside / 'value', b'external-read-sentinel')
            outside_file = (outside / 'value').stat()
            outside_identity = (outside_file.st_dev, outside_file.st_ino)
            outside_directory = (outside.stat().st_dev, outside.stat().st_ino)
            original_open, original_read, original_scandir = os.open, os.read, os.scandir
            original_clone_probe = scope['_cold_pair_clone']
            nested, retained = root / 'profile/nested', root / 'profile/retained-nested'
            nested_proof = (nested.stat().st_dev, nested.stat().st_ino)
            profile_proof = ((root / 'profile').stat().st_dev, (root / 'profile').stat().st_ino)
            fault = {'fired': False, 'externalReads': 0, 'readback': 0, 'visited': 0}
            destination = root / 'copied-profile'
            def substitute(path, saved):
                path.rename(saved)
                path.symlink_to(outside, target_is_directory=True)
                fault['fired'] = True
            def guarded_open(path, flags, *args, **kwargs):
                if scenario == 'nofollow-before-open' and not fault['fired'] and path == 'nested':
                    substitute(nested, retained)
                fd = original_open(path, flags, *args, **kwargs)
                info = os.fstat(fd)
                if (info.st_dev, info.st_ino) in (outside_identity, outside_directory):
                    fault['externalReads'] += 1
                    os.close(fd)
                    raise AssertionError('Open escaped the bound source directory')
                return fd
            def guarded_read(fd, size):
                info = os.fstat(fd)
                if (info.st_dev, info.st_ino) == outside_identity:
                    fault['externalReads'] += 1
                    raise AssertionError('Read escaped the bound source directory')
                return original_read(fd, size)
            class CountedListing:
                def __init__(self, iterator): self.iterator = iterator
                def __enter__(self): return self
                def __exit__(self, *args): self.iterator.close()
                def __iter__(self): return self
                def __next__(self):
                    entry = next(self.iterator)
                    fault['visited'] += 1
                    return entry
            def guarded_scandir(fd):
                info = os.fstat(fd) if isinstance(fd, int) else pathlib.Path(fd).stat()
                proof = (info.st_dev, info.st_ino)
                if proof == outside_directory:
                    fault['externalReads'] += 1
                    raise AssertionError('Enumeration escaped the bound source directory')
                if scenario == 'nofollow-after-open' and not fault['fired'] and proof == nested_proof:
                    substitute(nested, retained)
                if scenario == 'bounded-readback' and proof == profile_proof:
                    fault['readback'] += 1
                    if fault['readback'] == 2:
                        for index in range(64): write(root / 'profile' / ('extra-' + str(index)), b'x')
                        fault['fired'] = True
                        return CountedListing(original_scandir(fd))
                return original_scandir(fd)
            def copy_substitution(source, parent, name):
                if not fault['fired']:
                    if scenario == 'nofollow-copy-source': substitute(nested, retained)
                    elif scenario == 'nofollow-copy-target': substitute(destination, root / 'retained-copy')
                return original_clone_probe(source, parent, name)
            os.open, os.read, os.scandir = guarded_open, guarded_read, guarded_scandir
            if scenario == 'bounded-readback': scope['_COLD_PAIR_BOUNDS'] = dict(scope['_COLD_PAIR_BOUNDS'], entries=16)
            if scenario.startswith('nofollow-copy-'): scope['_cold_pair_clone'] = copy_substitution
            try:
                expect_refusal(lambda: tx.copy(root / 'profile', destination, True) if scenario.startswith('nofollow-copy-') else tx.observe(root / 'profile', True))
                assert fault['fired'] and fault['externalReads'] == 0
                if scenario == 'bounded-readback': assert fault['visited'] == 17
                assert 'stateTransaction' not in owner.journal
            finally:
                os.open, os.read, os.scandir = original_open, original_read, original_scandir
                scope['_cold_pair_clone'] = original_clone_probe
            assert sorted(path.name for path in outside.iterdir()) == ['value']
            assert (outside / 'value').read_bytes() == b'external-read-sentinel'
        elif scenario in ('partial-copy', 'source-changed'):
            original_copy = tx.copy
            def interrupted_copy(source, target, profile):
                result = original_copy(source, target, profile)
                if scenario == 'source-changed': write(database, b'changed-after-copy')
                else: raise Interrupted()
                return result
            tx.copy = interrupted_copy
            try: tx.snapshot()
            except Interrupted: assert scenario == 'partial-copy'
            except scope['Refused']: assert scenario == 'source-changed'
            else: raise AssertionError('Incomplete snapshot accepted')
            assert owner.journal['stateTransaction'] and not (tx.root / 'pair-descriptor.json').exists()
            expect_refusal(lambda: transaction_type(owner).snapshot())
        elif scenario == 'descriptor-published':
            original_write = tx.write
            def stop_descriptor(name, value, previous=None):
                result = original_write(name, value, previous)
                if name == 'pair-descriptor.json': raise Interrupted()
                return result
            tx.write = stop_descriptor
            try: tx.snapshot()
            except Interrupted: pass
            else: raise AssertionError('Missing descriptor interruption')
            assert (tx.root / 'pair-descriptor.json').exists()
            expect_refusal(lambda: transaction_type(owner).snapshot())
        else:
            pair = tx.snapshot()
            assert owner.journal['stateTransaction']['intentSha256'] == tx.journal['intentSha256']
            assert all((pair['copied'][name] is None) == (item is None) for name, item in initial.items())
            if scenario in ('reserve-replaced', 'descriptor-corrupt', 'descriptor-missing'):
                path = tx.root / ('reserve.bin' if scenario == 'reserve-replaced' else 'pair-descriptor.json')
                if scenario == 'reserve-replaced':
                    retained = path.with_name('foreign-reserve-retained')
                    path.rename(retained)
                    write(path, b'foreign')
                elif scenario == 'descriptor-corrupt': write(path, b'{}')
                else: path.unlink()
                expect_refusal(lambda: transaction_type(owner).begin_restore())
                assert database.read_bytes() == b'original-db'
            else:
                tx.preparation_intent('a' * 64)
                if scenario != 'preparation-unknown': tx.preparation_observed('b' * 64)
                write(database, b'advanced-db')
                write(root / 'state/desktop-settings.json', b'advanced-setting')
                write(root / 'profile/Preferences', b'advanced-profile')
                if scenario != 'sidecars': write(pathlib.Path(str(database) + '-wal'), b'advanced-wal')
                if scenario == 'restore-intent-published':
                    original_write = tx.write
                    def stop_restore_intent(name, value, previous=None):
                        result = original_write(name, value, previous)
                        if name == 'restore-intent.json': raise Interrupted()
                        return result
                    tx.write = stop_restore_intent
                    try: tx.begin_restore()
                    except Interrupted: pass
                    else: raise AssertionError('Missing restore-intent interruption')
                    tx = transaction_type(owner)
                tx.begin_restore()
                advanced = tx.observe_members()
                if scenario in ('foreign-destination', 'foreign-source', 'stage-missing', 'owner-lost'):
                    if scenario == 'foreign-destination': write(tx.root / 'advanced-state/database', b'foreign')
                    elif scenario == 'foreign-source':
                        database.rename(database.with_name('foreign-original-retained'))
                        write(database, b'advanced-db')
                    elif scenario == 'stage-missing': (tx.root / 'restore-staging/database').unlink()
                    else: live['current'] = False
                    try: tx.reconcile_restore()
                    except (scope['Refused'], AssertionError): pass
                    else: raise AssertionError('Foreign state accepted')
                    assert database.read_bytes() == b'advanced-db'
                    live['current'] = True
                else:
                    original_rename, original_sync, original_save = os.rename, scope['sync_fd'], tx.save
                    fired = {'value': False}
                    def fail():
                        fired['value'] = True
                        if scenario == 'sticky-sync': raise OSError('owned sync failure')
                        raise Interrupted()
                    def rename(source, target, **kwargs):
                        if not fired['value'] and scenario == 'before-rename': fail()
                        result = original_rename(source, target, **kwargs)
                        if not fired['value']:
                            installed = pathlib.Path(target).name == database.name
                            if scenario == ('after-install' if installed else 'after-retain'): fail()
                        return result
                    def sync(fd):
                        result = original_sync(fd)
                        if not fired['value'] and scenario in ('after-sync', 'sticky-sync') and (tx.root / 'advanced-state/database').exists(): fail()
                        return result
                    def save(**updates):
                        result = original_save(**updates)
                        if not fired['value'] and scenario == 'after-cursor' and updates.get('done'): fail()
                        return result
                    os.rename, scope['sync_fd'], tx.save = rename, sync, save
                    try:
                        result = tx.reconcile_restore()
                    except Interrupted:
                        assert fired['value']
                    except OSError as failure:
                        assert scenario == 'sticky-sync' and str(failure) == 'owned sync failure'
                        expect_refusal(tx.reconcile_restore)
                    finally:
                        os.rename, scope['sync_fd'] = original_rename, original_sync
                    if scenario == 'sticky-sync':
                        assert tx.failure is not None and owner.journal['stateTransaction']
                    else:
                        if scenario in ('before-rename', 'after-retain', 'after-install', 'after-sync', 'after-cursor'): assert fired['value']
                        recovered = transaction_type(owner)
                        result = recovered.reconcile_restore()
                        assert result['status'] == 'pair-restored-gates-retained'
                        actual = recovered.observe_members()
                        for name in initial:
                            assert (actual[name] is None) == (initial[name] is None)
                            if actual[name] is not None: assert actual[name]['content'] == initial[name]['content']
                        assert recovered.observe_members(recovered.root / 'advanced-state') == advanced
                        assert not (root / 'profile/SingletonLock').exists() and not (root / 'profile/SingletonLock').is_symlink()
                        assert owner.journal['stateTransaction']
                        assert recovered.reconcile_restore() == result
report = {'case': scenario, 'passed': True, 'qualification': 'owned-mechanical-storage', 'stateAdmissionAllowed': False}
if payload.get('nativeStorage'):
    assert sys.platform == 'darwin'
    if scenario == 'roundtrip': assert clone_counts['cloned'] > 0
    if scenario == 'native-fallback': assert clone_counts['attempted'] > 0 and clone_counts['cloned'] == 0 and clone_counts['fallback'] == clone_counts['attempted']
    report['nativeStorage'] = {'platform': sys.platform, 'clone': clone_counts, 'coldOwnerQualified': False}
print(json.dumps(report))
`;

export const coldPairCases = [
  "profile-volume",
  "nofollow-before-open",
  "nofollow-after-open",
  "nofollow-copy-source",
  "nofollow-copy-target",
  "bounded-readback",
  "held",
  "roundtrip",
  "sidecars",
  "partial-copy",
  "source-changed",
  "descriptor-published",
  "reserve-replaced",
  "descriptor-corrupt",
  "descriptor-missing",
  "preparation-unknown",
  "restore-intent-published",
  "foreign-destination",
  "foreign-source",
  "stage-missing",
  "owner-lost",
  "before-rename",
  "after-retain",
  "after-install",
  "after-sync",
  "after-cursor",
  "sticky-sync",
];

export async function runColdPairFixture(scenario, { signal, nativeStorage = false } = {}) {
  NodeAssert.ok(
    coldPairCases.includes(scenario) || (nativeStorage && scenario === "native-fallback"),
  );
  const sources = await readColdMaintenanceSources();
  return withLegacyIdentityScratch(async (root) => {
    const payload = NodePath.join(root, "pair.json");
    const entry = NodePath.join(root, "pair.py");
    await NodeFSP.writeFile(
      payload,
      JSON.stringify({ maintenance: sources.maintenance, case: scenario, nativeStorage }),
    );
    await NodeFSP.writeFile(entry, pairDriver);
    const result = await runLegacyIdentityFixtureCommand(
      "/usr/bin/python3",
      ["-I", "-B", entry, payload, root],
      root,
      { signal },
    );
    NodeAssert.equal(result.code, 0, result.stderr);
    NodeAssert.equal(result.stderr, "");
    const report = JSON.parse(result.stdout.trim());
    NodeAssert.equal(report.case, scenario);
    NodeAssert.equal(report.passed, true);
    return report;
  });
}
