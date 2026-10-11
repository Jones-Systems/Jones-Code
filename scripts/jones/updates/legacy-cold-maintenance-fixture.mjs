import * as NodeAssert from "node:assert/strict";
import * as NodeCrypto from "node:crypto";
import * as NodeFS from "node:fs";
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
  "owner-same-boot",
  "owner-lock-binding",
  "owner-native-gate",
  "owner-native-lock",
  "owner-cancel-entered",
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

export function materializeColdMaintenance(
  bootstrapText,
  maintenanceText,
  coldText,
  pairedText,
  ownerText = NodeFS.readFileSync(NodePath.join(updateSources, "jonesColdOwnerSource.ts"), "utf8"),
  workerText = NodeFS.readFileSync(
    NodePath.join(updateSources, "jonesPreparationWorkerSource.ts"),
    "utf8",
  ),
) {
  NodeAssert.ok(
    maintenanceText.includes('import { jonesColdPairSource } from "./jonesColdPairSource.ts";'),
  );
  NodeAssert.equal(maintenanceText.split("  jonesColdPairSource +").length, 2);
  NodeAssert.ok(
    maintenanceText.includes('import { jonesColdOwnerSource } from "./jonesColdOwnerSource.ts";'),
  );
  NodeAssert.equal(maintenanceText.split("  jonesColdOwnerSource +").length, 2);
  NodeAssert.ok(
    maintenanceText.includes(
      'import { jonesPreparationWorkerSource } from "./jonesPreparationWorkerSource.ts";',
    ),
  );
  NodeAssert.equal(maintenanceText.split("  jonesPreparationWorkerSource +").length, 2);
  NodeAssert.match(
    maintenanceText,
    /jonesColdPairSource \+\s+jonesColdOwnerSource \+\s+jonesPreparationWorkerSource \+\s+String\.raw`/,
  );
  const ownerDeclaration = "export const jonesColdOwnerSource = String.raw`";
  NodeAssert.equal(ownerText.split(ownerDeclaration).length, 2);
  NodeAssert.equal(
    ownerText
      .slice(0, ownerText.indexOf(ownerDeclaration))
      .replace(/\/\*[^]*?\*\//g, "")
      .trim(),
    "",
  );
  const owner = literal(ownerText);
  NodeAssert.ok(!owner.includes("${") && !owner.includes("`"), "Owner source must be one literal.");
  const workerDeclaration = "export const jonesPreparationWorkerSource = String.raw`";
  NodeAssert.equal(workerText.split(workerDeclaration).length, 2);
  NodeAssert.equal(
    workerText
      .slice(0, workerText.indexOf(workerDeclaration))
      .replace(/\/\*[^]*?\*\//g, "")
      .trim(),
    "",
  );
  const worker = literal(workerText);
  NodeAssert.ok(
    !worker.includes("${") && !worker.includes("`"),
    "Worker source must be one literal.",
  );
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
  const result =
    bootstrap.slice(0, -suffix.length) + cold + owner + worker + literal(maintenanceText);
  NodeAssert.ok(
    !result.includes("${") && !result.includes("`"),
    "Unresolved maintenance substitution.",
  );
  return result;
}

export async function readColdMaintenanceSources() {
  const [
    bootstrapText,
    maintenanceText,
    executorText,
    pairedText,
    coldText,
    nativeSource,
    ownerText,
    workerText,
  ] = await Promise.all([
    NodeFSP.readFile(NodePath.join(updateSources, "jonesLegacyBootstrapSource.ts"), "utf8"),
    NodeFSP.readFile(NodePath.join(updateSources, "jonesLegacyMaintenanceSource.ts"), "utf8"),
    NodeFSP.readFile(NodePath.join(updateSources, "jonesNativeHelperSource.ts"), "utf8"),
    NodeFSP.readFile(NodePath.join(updateSources, "jonesPairedStateSource.ts"), "utf8"),
    NodeFSP.readFile(NodePath.join(updateSources, "jonesColdPairSource.ts"), "utf8"),
    NodeFSP.readFile(NodePath.join(updateSources, "jonesLegacyIdentityNative.c")),
    NodeFSP.readFile(NodePath.join(updateSources, "jonesColdOwnerSource.ts"), "utf8"),
    NodeFSP.readFile(NodePath.join(updateSources, "jonesPreparationWorkerSource.ts"), "utf8"),
  ]);
  const bootstrap = literal(bootstrapText);
  const suffix = "if __name__ == '__main__':\n    sys.exit(main())\n";
  NodeAssert.ok(bootstrap.endsWith(suffix));
  return {
    bootstrap,
    maintenance: materializeColdMaintenance(
      bootstrapText,
      maintenanceText,
      coldText,
      pairedText,
      ownerText,
      workerText,
    ),
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
        if case.startswith('owner-'):
            pointer = json.loads((home / 'runtime/jones-maintenance/current.json').read_text())
            directory = home / 'runtime/jones-maintenance' / pointer['generation']
            journal_path = directory / 'journal.json'
            before = journal_path.read_bytes()
            if case == 'owner-cancel-entered':
                journal = json.loads(before)
                # This injected record tests only cancellation's durable fence;
                # it is not a native reboot or admission observation.
                epoch = '00000000-0000-4000-8000-000000000001'
                assert epoch.lower() != journal['armedBootSessionUuid'].lower()
                journal['coldOwner'] = {'schema': 1, 'sessionId': '9' * 64, 'bootSessionUuid': epoch, 'phase': 'entered'}
                journal_path.write_bytes(encode(journal) + b'\n')
                code, report = invoke('cancel')
                assert code == 1 and report['status'] == 'held'
                assert json.loads(journal_path.read_text()) == journal and 'stateTransaction' not in journal
            else:
                if case == 'owner-native-gate': (old / 'Contents/MacOS/Jones').chmod(0o500)
                if case == 'owner-native-lock':
                    lock = manifest.parent / 'jones-activation.lock'
                    lock.unlink()
                    write(lock, scope['LOCK_BYTES'])
                guard['enabled'] = True
                refused = False
                verified = []
                real_verify = scope['Maintenance'].verify
                def tracked_verify(owner):
                    result = real_verify(owner)
                    verified.append(result)
                    return result
                scope['Maintenance'].verify = tracked_verify
                try:
                    if case == 'owner-lock-binding':
                        with scope['_cold_owner_session']():
                            raise AssertionError('Unbound native fixture issued a state capability')
                    with scope['fixed_lock'](manifest) as current:
                        with scope['_cold_owner_session']():
                            raise AssertionError('Same-kernel native fixture issued a state capability')
                except scope['Refused']: refused = True
                finally:
                    guard['enabled'] = False
                    scope['Maintenance'].verify = real_verify
                assert refused and journal_path.read_bytes() == before
                assert len(verified) == (1 if case == 'owner-same-boot' else 0)
                assert not (directory / 'state').exists()
            assert json.loads(manifest.read_text())['owner'] == 'maintenance'
        elif case.startswith('state-fence'):
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
        if scenario in ('cold-c-device-renumber', 'cold-c-native-drift', 'cold-c-foreign-volume',
                        'live-root-device-drift', 'live-root-inode-drift', 'live-root-native-drift'):
            # Model retained boot-A stat numbers without changing real FD observations
            # or their durable native proof. This is not an actual APFS remount.
            original_intent = tx.make_intent
            def prior_epoch_intent(transaction_id, roots):
                return original_intent(transaction_id, [dict(item, device=str(int(item['device']) + 1)) for item in roots])
            tx.make_intent = prior_epoch_intent
            tx.snapshot()
            write(database, b'advanced-db')
            write(root / 'profile/Preferences', b'advanced-profile')
            original_root_identity = tx.root_identity
            def prior_advanced_identity(path):
                result = original_root_identity(path)
                return dict(result, device=str(int(result['device']) + 1)) if path == tx.root / 'advanced-state' else result
            tx.root_identity = prior_advanced_identity
            tx.begin_restore()
            tx.root_identity = original_root_identity
            retained = {name: (tx.root / name).read_bytes() for name in ('intent.json', 'pair-descriptor.json', 'restore-intent.json')}
            retained_journal = (tx.root / 'journal.json').read_bytes()
            retained_owner = (owner.directory / 'journal.json').read_bytes()
            before_restore = tx.observe_members()
            if scenario in ('cold-c-native-drift', 'cold-c-foreign-volume'):
                original_method = transaction_type.root_identity
                def changed_root(actual, path):
                    result = original_method(actual, path)
                    if path == database.parent:
                        if scenario == 'cold-c-foreign-volume': result = dict(result, device=str(int(result['device']) + 1))
                        else:
                            native = list(result['native'])
                            native[1] = str(int(native[1]) + 1)
                            result = dict(result, native=native)
                    return result
                transaction_type.root_identity = changed_root
                try: expect_refusal(lambda: transaction_type(owner))
                finally: transaction_type.root_identity = original_method
            else:
                recovered = transaction_type(owner)
                if scenario.startswith('live-root-'):
                    original_method = recovered.root_identity
                    def changed_root(path):
                        result = original_method(path)
                        if path == database.parent:
                            if scenario == 'live-root-native-drift':
                                native = list(result['native'])
                                native[1] = str(int(native[1]) + 1)
                                result = dict(result, native=native)
                            else:
                                field = 'device' if scenario == 'live-root-device-drift' else 'inode'
                                result = dict(result, **{field: str(int(result[field]) + 1)})
                        return result
                    recovered.root_identity = changed_root
                    expect_refusal(recovered.reconcile_restore)
                    expect_refusal(recovered.reconcile_restore)
                else:
                    result = recovered.reconcile_restore()
                    assert result['status'] == 'pair-restored-gates-retained'
                    restored = recovered.observe_members()
                    for name, item in initial.items():
                        assert (restored[name] is None) == (item is None)
                        if item is not None: assert restored[name]['content'] == item['content']
                    assert recovered.observe_members(recovered.root / 'advanced-state') == before_restore
                    assert transaction_type(owner).reconcile_restore() == result
            if scenario != 'cold-c-device-renumber':
                assert tx.observe_members() == before_restore
                assert (tx.root / 'journal.json').read_bytes() == retained_journal
            assert all((tx.root / name).read_bytes() == raw for name, raw in retained.items())
            assert (owner.directory / 'journal.json').read_bytes() == retained_owner
        elif scenario in ('live-advanced-device-drift', 'live-advanced-inode-drift',
                          'recovered-advanced-device-drift', 'recovered-advanced-inode-drift'):
            tx.snapshot()
            write(database, b'advanced-db')
            write(root / 'profile/Preferences', b'advanced-profile')
            advanced = tx.root / 'advanced-state'
            original_identity = tx.root_identity
            recovered_case = scenario.startswith('recovered-')
            if recovered_case:
                # Model prior-epoch stat numbering; no volume is actually remounted.
                def prior_advanced_identity(path):
                    result = original_identity(path)
                    return dict(result, device=str(int(result['device']) + 1)) if path == advanced else result
                tx.root_identity = prior_advanced_identity
            tx.begin_restore()
            tx.root_identity = original_identity
            retained = {name: (tx.root / name).read_bytes() for name in ('intent.json', 'pair-descriptor.json', 'restore-intent.json')}
            retained_owner = (owner.directory / 'journal.json').read_bytes()
            before_restore = tx.observe_members()
            active = transaction_type(owner) if recovered_case else tx
            if recovered_case:
                result = active.reconcile_restore()
                assert result['status'] == 'pair-restored-gates-retained'
                restored = active.observe_members()
                for name, item in initial.items():
                    assert (restored[name] is None) == (item is None)
                    if item is not None: assert restored[name]['content'] == item['content']
                assert active.observe_members(advanced) == before_restore
            retained_journal = (tx.root / 'journal.json').read_bytes()
            def owned_snapshot():
                remaining = {'entries': 128, 'bytes': 1024 * 1024}
                def identity(info):
                    return (info.st_dev, info.st_ino, info.st_uid, info.st_gid, info.st_mode,
                            info.st_nlink, info.st_size, info.st_mtime_ns, info.st_ctime_ns)
                def visit(parent, name, depth):
                    remaining['entries'] -= 1
                    assert remaining['entries'] >= 0 and depth <= 8
                    info = os.stat(name, dir_fd=parent, follow_symlinks=False)
                    if stat.S_ISLNK(info.st_mode):
                        content = os.readlink(name, dir_fd=parent)
                        assert len(content) <= 4096
                        assert identity(os.stat(name, dir_fd=parent, follow_symlinks=False)) == identity(info)
                        return (identity(info), content)
                    assert stat.S_ISDIR(info.st_mode) or stat.S_ISREG(info.st_mode)
                    flags = os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK
                    if stat.S_ISDIR(info.st_mode): flags |= os.O_DIRECTORY
                    fd = os.open(name, flags, dir_fd=parent)
                    try:
                        assert identity(os.fstat(fd)) == identity(info)
                        if stat.S_ISDIR(info.st_mode):
                            names = []
                            with os.scandir(fd) as entries:
                                for entry in entries:
                                    names.append(entry.name)
                                    assert len(names) <= remaining['entries']
                            content = tuple((child, visit(fd, child, depth + 1)) for child in sorted(names))
                        else:
                            chunks = []
                            while True:
                                chunk = os.read(fd, min(65536, remaining['bytes'] + 1))
                                if not chunk: break
                                remaining['bytes'] -= len(chunk)
                                assert remaining['bytes'] >= 0
                                chunks.append(chunk)
                            content = b''.join(chunks)
                        assert identity(os.fstat(fd)) == identity(info)
                        assert identity(os.stat(name, dir_fd=parent, follow_symlinks=False)) == identity(info)
                        return (identity(info), content)
                    finally: os.close(fd)
                fd = os.open(root, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
                try:
                    assert (os.fstat(fd).st_dev, os.fstat(fd).st_ino) == root_proof
                    return tuple((name, visit(fd, name, 0)) for name in ('state', 'profile', 'generation'))
                finally: os.close(fd)
            before_drift = owned_snapshot()
            original_active_identity = active.root_identity
            def changed_advanced(path):
                result = original_active_identity(path)
                if path == advanced:
                    field = 'device' if scenario.endswith('device-drift') else 'inode'
                    result = dict(result, **{field: str(int(result[field]) + 1)})
                return result
            active.root_identity = changed_advanced
            expect_refusal(active.reconcile_restore)
            expect_refusal(active.reconcile_restore)
            assert owned_snapshot() == before_drift
            assert (tx.root / 'journal.json').read_bytes() == retained_journal
            assert all((tx.root / name).read_bytes() == raw for name, raw in retained.items())
            assert (owner.directory / 'journal.json').read_bytes() == retained_owner
        elif scenario == 'profile-volume':
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
  "cold-c-device-renumber",
  "cold-c-native-drift",
  "cold-c-foreign-volume",
  "live-root-device-drift",
  "live-root-inode-drift",
  "live-root-native-drift",
  "live-advanced-device-drift",
  "live-advanced-inode-drift",
  "recovered-advanced-device-drift",
  "recovered-advanced-inode-drift",
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

export const coldOwnerCases = [
  "unbound-lock",
  "same-boot",
  "entry-order",
  "before-entered",
  "after-entered",
  "entry-sync",
  "stale-session",
  "same-boot-reentry",
  "fresh-process-reentry",
  "cold-c-fence",
  "cold-c-incomplete",
  "paired-restore",
  "legacy-state-fence",
  "sticky-pair-sync",
  "journal-drift",
  "owner-drift",
  "plan-drift",
  "tool-drift",
  "lock-replaced",
  "walk-lock-replaced",
  "finalize-sync",
  "lock-close",
  "cancel-entered",
];

// Simulated epochs are confined to this owned fixture's metadata observer.
// The issuer, fixed lock, durable journal and paired FD operations are real;
// this driver does not qualify a reboot, launch closure, SQLite or child custody.
const ownerDriver = String.raw`
import sys
sys.dont_write_bytecode = True
import errno, json, os, pathlib, stat, subprocess
payload = json.loads(pathlib.Path(sys.argv[1]).read_text())
root = pathlib.Path(sys.argv[2]).resolve(strict=True)
scenario = payload['case']
scope = {'__name__': 'owned_cold_owner_fixture'}
exec(compile(payload['maintenance'], 'maintenance.py', 'exec'), scope)
encode, sha = scope['encoded'], scope['sha']
Refused, Maintenance = scope['Refused'], scope['Maintenance']
class Cancelled(BaseException): pass
epoch_a, epoch_b, epoch_c = ('00000000-0000-4000-8000-00000000000' + str(n) for n in (1, 2, 3))
epoch = {'current': epoch_a if scenario == 'same-boot' else epoch_b}
events, sessions = [], []
generation = '2' * 64
manifest = root / 'runtime/jones-active-install.json'
maintenance_root = manifest.parent / 'jones-maintenance'
directory = maintenance_root / generation
database, profile = root / 'userdata/state.sqlite', root / 'profile'
for path in (directory / 'tools', database.parent, profile): path.mkdir(mode=0o700, parents=True, exist_ok=payload.get('reenter', False))
gate = root / 'owned-gate'
def write(path, raw, mode=0o600):
    path.write_bytes(raw)
    path.chmod(mode)
def seed(path, raw, mode=0o600):
    if not payload.get('reenter', False): write(path, raw, mode)
seed(database, b'original-db')
seed(profile / 'Preferences', b'original-profile')
seed(database.parent / 'settings.json', b'{"owned":true}')
seed(gate, b'owned-executable-content', 0o400)
original = {'owner': 'desktop', 'generation': '1' * 64, 'sourceSha': '3' * 40,
            'databasePath': str(database), 'profile': str(profile),
            'appPath': str(root / 'Old.app'), 'executablePath': str(gate)}
maintenance_bytes, native_bytes = b'owned-maintenance-tool', b'owned-native-tool'
bundle = {'maintenanceSha256': sha(maintenance_bytes), 'identityNativeSha256': sha(native_bytes)}
plan = {'maintenanceGeneration': generation, 'originalManifest': encode(original).decode(),
        'originalManifestSha256': sha(encode(original)), 'bundle': bundle,
        'candidateBinding': {'sourceSha': '4' * 40, 'executorSha256': '5' * 64}}
plan_raw = encode(plan) + b'\n'
journal = {'schema': 1, 'maintenanceGeneration': generation, 'planSha256': sha(plan_raw),
           'phase': 'armed', 'done': [str(gate)], 'intent': None, 'armedBootSessionUuid': epoch_a, 'cancel': None}
seed(directory / 'plan.json', plan_raw, 0o400)
seed(directory / 'journal.json', encode(journal) + b'\n')
seed(maintenance_root / 'current.json', encode({'schema': 1, 'generation': generation, 'planSha256': sha(plan_raw)}) + b'\n', 0o400)
seed(directory / 'tools/maintenance.py', maintenance_bytes, 0o500)
seed(directory / 'tools/identity-native', native_bytes, 0o500)
owner_manifest = dict(original, owner='maintenance', generation=generation,
                      maintenance={'schema': 1, 'planSha256': sha(plan_raw)})
seed(manifest, encode(owner_manifest) + b'\n')
def load_owned(self):
    # Only the native inventory/packaging inputs are replaced. The production
    # issuer still calls load itself and cannot accept a caller's boot value.
    assert self.manifest == manifest
    self.directory, self.native = directory, directory / 'tools/identity-native'
    self.plan, self.original, self.bundle = json.loads(plan_raw), dict(original), dict(bundle)
    self.journal_raw = (directory / 'journal.json').read_bytes()
    self.journal = json.loads(self.journal_raw)
    self.gates = [{'path': str(gate), 'app': original['appPath']}]
    self.validate_cursor()
def verify_owned(self):
    events.append('verify')
    self.current_lock()
    self.validate_cursor()
    scope['require'](stat.S_IMODE(gate.stat().st_mode) == 0o400)
    scope['require'](json.loads(manifest.read_bytes()) == self.owner())
    scope['require']((directory / 'tools/identity-native').read_bytes() == native_bytes)
    return epoch['current']
Maintenance.load, Maintenance.verify = load_owned, verify_owned
def forbidden_child(*args, **kwargs): raise AssertionError('Mechanical owner fixture launched a child')
subprocess.Popen = forbidden_child
def portable_identity(owner, path, kind):
    assert path.is_relative_to(root)
    info = path.lstat()
    result = [str(info.st_dev), str(info.st_ino), info.st_uid, stat.S_IMODE(info.st_mode)]
    if kind == 'file': result += [info.st_size, info.st_mtime_ns]
    return result
scope['_cold_pair_identity'] = portable_identity
scope['_cold_pair_clone'] = lambda source, parent, name: False
scope['_COLD_PAIR_HEADROOM'] = 16384
fault = {'enabled': True, 'fired': False}
real_durable, real_sync_evidence = scope['durable_file'], Maintenance.sync_evidence
def durable(path, raw, mode, previous=None):
    entered = path == directory / 'journal.json' and json.loads(raw).get('coldOwner', {}).get('phase') == 'entered'
    if entered: events.append('entered-write')
    if entered and fault['enabled'] and scenario == 'before-entered':
        fault['fired'] = True
        raise OSError(errno.EIO, 'owned before entered write')
    real_durable(path, raw, mode, previous)
    if entered:
        events.append('entered-durable')
        if fault['enabled'] and scenario == 'after-entered':
            fault['fired'] = True
            raise OSError(errno.EIO, 'owned after entered write')
scope['durable_file'] = durable
def sync_evidence(self):
    phase = self.journal.get('coldOwner', {}).get('phase')
    real_sync_evidence(self)
    if phase == 'entered': events.append('entered-synced')
    if fault['enabled'] and ((scenario == 'entry-sync' and phase == 'entered') or
                             (scenario == 'finalize-sync' and phase == 'finalized')):
        fault['fired'] = True
        raise OSError(errno.EIO, 'owned owner evidence sync failure')
Maintenance.sync_evidence = sync_evidence
def audit(event, args):
    if event not in ('open', 'os.scandir', 'os.listdir') or not args or not isinstance(args[0], (str, bytes)): return
    path = pathlib.Path(os.fsdecode(args[0]))
    if any(path == item or path.is_relative_to(item) for item in (database.parent, profile, directory / 'state')):
        assert 'entered-synced' in events, 'State was accessed before entered durability'
        current = json.loads((directory / 'journal.json').read_bytes())
        assert current.get('coldOwner', {}).get('bootSessionUuid') == epoch['current']
sys.addaudithook(audit)
def refusal(operation, error=Refused):
    try: operation()
    except error as failure: return failure
    raise AssertionError('Expected retained owner refusal')
def enter_once(body=None, unbound=False):
    started = len(events)
    if unbound:
        fake_owner = Maintenance(manifest, lambda: None)
        refusal(lambda: scope['_cold_owner_session'](fake_owner), TypeError)
        with scope['_cold_owner_session'](): raise AssertionError('Unbound lock admitted')
    with scope['fixed_lock'](manifest) as current:
        with scope['_cold_owner_session']() as session:
            owner = session.owner
            sessions.append(session)
            events.append('issued')
            on_disk = json.loads((directory / 'journal.json').read_bytes())
            assert on_disk['coldOwner']['phase'] == 'entered' and on_disk['coldOwner']['bootSessionUuid'] == epoch['current']
            observed = events[started:]
            assert observed.index('entered-durable') < observed.index('entered-synced') < observed.index('issued')
            if body is not None: body(session, owner)
def current_journal(): return json.loads((directory / 'journal.json').read_bytes())
def snapshot(session, owner):
    result = session.snapshot()
    assert result['source']['database']['content'] == result['copied']['database']['content']
    assert current_journal()['stateTransaction'] == owner.journal['stateTransaction']
    return result
if scenario == 'fresh-process-reentry' and payload.get('reenter', False):
    before = (directory / 'journal.json').read_bytes()
    assert current_journal()['coldOwner']['phase'] == 'finalized'
    assert current_journal()['coldOwner']['bootSessionUuid'] == epoch_b
    assert current_journal()['stateTransaction']
    refusal(enter_once)
    assert (directory / 'journal.json').read_bytes() == before and not sessions
    assert 'entered-write' not in events
elif scenario in ('same-boot', 'unbound-lock', 'legacy-state-fence'):
    if scenario == 'legacy-state-fence':
        journal['stateTransaction'] = {'schema': 1, 'transactionId': '7' * 64, 'intentSha256': '8' * 64}
        write(directory / 'journal.json', encode(journal) + b'\n')
    refusal(lambda: enter_once(unbound=scenario == 'unbound-lock'))
    assert 'coldOwner' not in current_journal() and not sessions
    assert 'entered-write' not in events
elif scenario in ('before-entered', 'after-entered', 'entry-sync'):
    failure = refusal(enter_once, OSError)
    assert failure.errno == errno.EIO and fault['fired'] and not sessions
    fault['enabled'] = False
    if scenario == 'before-entered':
        assert 'coldOwner' not in current_journal()
        enter_once(snapshot)
    else:
        assert current_journal()['coldOwner']['phase'] == 'entered'
        refusal(enter_once)
        assert not sessions
elif scenario == 'cold-c-incomplete':
    original_mkdir = scope['_ColdPairTransaction'].mkdir
    def interrupted_mkdir(self, path):
        original_mkdir(self, path)
        raise OSError(errno.EIO, 'owned interruption after state fence and directory creation')
    scope['_ColdPairTransaction'].mkdir = interrupted_mkdir
    refusal(lambda: enter_once(snapshot), OSError)
    before = current_journal()['stateTransaction']
    assert current_journal()['coldOwner']['phase'] == 'entered'
    refusal(enter_once)
    scope['_ColdPairTransaction'].mkdir = original_mkdir
    epoch['current'] = epoch_c
    refusal(lambda: enter_once(snapshot), FileNotFoundError)
    assert current_journal()['coldOwner']['bootSessionUuid'] == epoch_c
    assert current_journal()['coldOwner']['phase'] == 'entered'
    assert current_journal()['stateTransaction'] == before
    refusal(enter_once)
elif scenario == 'walk-lock-replaced':
    original_clone = scope['_cold_pair_clone']
    def replace_lock(source, parent, name):
        assert not fault['fired']
        fault['fired'] = True
        lock = manifest.parent / 'jones-activation.lock'
        lock.unlink()
        write(lock, scope['LOCK_BYTES'])
        return original_clone(source, parent, name)
    scope['_cold_pair_clone'] = replace_lock
    refusal(lambda: enter_once(snapshot))
    assert fault['fired'] and current_journal()['stateTransaction']
    assert current_journal()['coldOwner']['phase'] == 'entered'
    assert not (directory / 'state/pair-descriptor.json').exists()
    refusal(enter_once)
elif scenario in ('journal-drift', 'owner-drift', 'plan-drift', 'tool-drift', 'lock-replaced', 'sticky-pair-sync'):
    def changed(session, owner):
        if scenario == 'journal-drift': write(directory / 'journal.json', owner.journal_raw + b' ')
        if scenario == 'owner-drift': write(manifest, encode(dict(owner_manifest, owner='desktop')))
        if scenario == 'plan-drift':
            (directory / 'plan.json').chmod(0o600)
            write(directory / 'plan.json', plan_raw + b' ', 0o400)
        if scenario == 'tool-drift':
            (directory / 'tools/maintenance.py').chmod(0o600)
            write(directory / 'tools/maintenance.py', b'changed-tool', 0o500)
        if scenario == 'lock-replaced':
            lock = manifest.parent / 'jones-activation.lock'
            lock.unlink()
            write(lock, scope['LOCK_BYTES'])
        if scenario == 'sticky-pair-sync':
            original_sync = scope['_ColdPairTransaction'].sync_directory
            def failed_sync(self, path):
                original_sync(self, path)
                fault['fired'] = True
                raise OSError(errno.EIO, 'owned paired directory sync failure')
            scope['_ColdPairTransaction'].sync_directory = failed_sync
            failure = refusal(session.snapshot, OSError)
            assert failure.errno == errno.EIO and fault['fired']
            assert current_journal()['stateTransaction']
        else: refusal(session.snapshot)
        refusal(session.snapshot)
    refusal(lambda: enter_once(changed))
    assert current_journal()['coldOwner']['phase'] == 'entered'
    refusal(enter_once)
elif scenario == 'finalize-sync':
    failure = refusal(lambda: enter_once(snapshot), OSError)
    assert failure.errno == errno.EIO and fault['fired']
    assert current_journal()['coldOwner']['phase'] == 'finalized'
    fault['enabled'] = False
    refusal(enter_once)
elif scenario == 'lock-close':
    fcntl = scope['fcntl']
    real_close, real_flock = os.close, fcntl.flock
    held_fd = {'value': None, 'closed': False}
    def acquired_flock(fd, operation):
        result = real_flock(fd, operation)
        assert operation == (fcntl.LOCK_EX | fcntl.LOCK_NB) and held_fd['value'] is None
        held_fd['value'] = fd
        return result
    def failed_close(fd):
        matched = fd == held_fd['value'] and not held_fd['closed']
        if matched: assert len(sessions) == 1 and all(not session.live for session in sessions)
        real_close(fd)
        if matched: held_fd['closed'] = True
        if fault['enabled'] and matched:
            fault['fired'] = True
            raise OSError(errno.EIO, 'owned failure after actual lock close')
    os.close, fcntl.flock = failed_close, acquired_flock
    try: failure = refusal(lambda: enter_once(snapshot), OSError)
    finally: os.close, fcntl.flock = real_close, real_flock
    assert failure.errno == errno.EIO and fault['fired'] and held_fd['closed']
    assert refusal(lambda: os.fstat(held_fd['value']), OSError).errno == errno.EBADF
    refusal(sessions[0].snapshot)
    sticky = sessions[0].failure
    refusal(sessions[0].snapshot)
    assert sticky is not None and sessions[0].failure is sticky and sessions[0].held['failure'] is sticky
    retained_journal = (directory / 'journal.json').read_bytes()
    refusal(enter_once)
    assert (directory / 'journal.json').read_bytes() == retained_journal and len(sessions) == 1
elif scenario == 'cancel-entered':
    def cancel_scope(session, owner): raise Cancelled()
    refusal(lambda: enter_once(cancel_scope), Cancelled)
    assert current_journal()['coldOwner']['phase'] == 'entered'
    assert 'stateTransaction' not in current_journal()
    before = (directory / 'journal.json').read_bytes()
    with scope['fixed_lock'](manifest) as current:
        owner = Maintenance(manifest, current)
        owner.load()
        refusal(owner.cancel)
    assert (directory / 'journal.json').read_bytes() == before
    assert stat.S_IMODE(gate.stat().st_mode) == 0o400
elif scenario == 'paired-restore':
    def restore(session, owner):
        snapshot(session, owner)
        fence = dict(owner.journal['stateTransaction'])
        write(database, b'advanced-db')
        write(profile / 'Preferences', b'advanced-profile')
        session.begin_restore()
        result = session.reconcile_restore()
        assert result['status'] == 'pair-restored-gates-retained'
        assert database.read_bytes() == b'original-db' and (profile / 'Preferences').read_bytes() == b'original-profile'
        assert (directory / 'state/advanced-state/database').read_bytes() == b'advanced-db'
        assert owner.journal['stateTransaction'] == fence
        refusal(lambda: session.pair.snapshot())
    # A direct call on retained pair mechanics is outside the operation facade
    # and poisons the session even if its caller catches the refusal.
    refusal(lambda: enter_once(restore))
    assert current_journal()['coldOwner']['phase'] == 'entered'
else:
    enter_once(snapshot)
    assert current_journal()['coldOwner']['phase'] == 'finalized'
    if scenario == 'stale-session':
        refusal(sessions[0].snapshot)
        refusal(lambda: scope['_ColdPairTransaction'](sessions[0].owner))
    if scenario in ('same-boot-reentry', 'entry-order'): refusal(enter_once)
    if scenario == 'cold-c-fence':
        before = current_journal()['stateTransaction']
        prior_session = current_journal()['coldOwner']['sessionId']
        epoch['current'] = epoch_c
        enter_once(snapshot)
        assert current_journal()['stateTransaction'] == before
        assert current_journal()['coldOwner']['sessionId'] != prior_session
        assert current_journal()['coldOwner']['bootSessionUuid'] == epoch_c
        refusal(enter_once)
assert all(not session.live for session in sessions)
print(json.dumps({'case': scenario, 'passed': True, 'qualification': 'owned-mechanical-cold-owner',
                  'stateAdmissionAllowed': False, 'nativeColdOwnerQualified': False}))
`;

export async function runColdOwnerFixture(scenario, { signal } = {}) {
  NodeAssert.ok(coldOwnerCases.includes(scenario));
  const sources = await readColdMaintenanceSources();
  return withLegacyIdentityScratch(async (root) => {
    const payload = NodePath.join(root, "owner.json");
    const entry = NodePath.join(root, "owner.py");
    await NodeFSP.writeFile(
      payload,
      JSON.stringify({ maintenance: sources.maintenance, case: scenario }),
    );
    await NodeFSP.writeFile(entry, ownerDriver);
    const result = await runLegacyIdentityFixtureCommand(
      "/usr/bin/python3",
      ["-I", "-B", entry, payload, root],
      root,
      { signal },
    );
    NodeAssert.equal(result.code, 0, result.stderr);
    NodeAssert.equal(result.stderr, "");
    let report = JSON.parse(result.stdout.trim());
    NodeAssert.equal(report.case, scenario);
    NodeAssert.equal(report.passed, true);
    if (scenario === "fresh-process-reentry") {
      await NodeFSP.writeFile(
        payload,
        JSON.stringify({ maintenance: sources.maintenance, case: scenario, reenter: true }),
      );
      const repeated = await runLegacyIdentityFixtureCommand(
        "/usr/bin/python3",
        ["-I", "-B", entry, payload, root],
        root,
        { signal },
      );
      NodeAssert.equal(repeated.code, 0, repeated.stderr);
      NodeAssert.equal(repeated.stderr, "");
      report = JSON.parse(repeated.stdout.trim());
      NodeAssert.equal(report.case, scenario);
      NodeAssert.equal(report.passed, true);
    }
    return report;
  });
}

export const coldPreparationCases = [
  "prepare-success",
  "settled-query-restore",
  "plan-query-restore",
  "plan-id-drift",
  "plan-publication-failure",
  "intent-sync-failure",
  "worker-exit-failure",
  "worker-close-failure",
  "paused-cancel",
  "work-replaced",
  "cold-c-reconcile",
  "v2-held",
  "unretained-runtime",
];

// This framed child models query results, not SQLite or native observations. The
// real issuer, lock, pair, publishing, process handles and teardown remain in use.
const preparationChild = String.raw`
import sys
sys.dont_write_bytecode = True
import hashlib, json, os, pathlib, struct
def canonical(value): return json.dumps(value, ensure_ascii=False, separators=(',', ':'), sort_keys=True).encode()
def digest(raw): return hashlib.sha256(raw).hexdigest()
args = sys.argv[1:]
assert args[0] == '--jones-preparation-worker-v1' and args[1] == '--lock-fd' and args[3] == '--owner-directory-fd' and len(args) == 5
lock = int(args[2])
assert os.pread(lock, 4096, 0)
os.fchdir(int(args[4]))
directory = pathlib.Path.cwd()
plan = json.loads((directory / 'plan.json').read_bytes())
owner = json.loads((directory / 'journal.json').read_bytes())
session = owner['coldOwner']['sessionId']
dispatch = json.loads((directory / 'state' / ('worker-' + session + '.json')).read_bytes())
assert dispatch['parentPid'] == os.getppid() and owner['coldOwner']['phase'] == 'entered'
case = (directory / 'owned-fixture-case').read_text()
runtime = plan['bundle']['preparation']
sequence = 0
def read_exact(length):
    raw = b''
    while len(raw) < length:
        chunk = sys.stdin.buffer.read(length - len(raw))
        assert chunk
        raw += chunk
    return raw
def receive():
    length = struct.unpack('>I', read_exact(4))[0]
    assert 1 <= length <= 16384
    raw = read_exact(length)
    value = json.loads(raw)
    assert canonical(value) == raw and value['sessionId'] == session and value['payloadBytes'] == 0
    return value
def send(kind, payload=b'', **fields):
    value = dict(schema=1, type=kind, sessionId=session, sequence=sequence, payloadBytes=len(payload), **fields)
    raw = canonical(value)
    sys.stdout.buffer.write(struct.pack('>I', len(raw)) + raw + payload)
    sys.stdout.buffer.flush()
send('READY', runtimeSha256=runtime['executable']['sha256'], entrySha256=runtime['entry']['sha256'])
while True:
    command = receive()
    sequence = command['sequence']
    kind = command['type']
    if kind in ('EXIT', 'ABORT'):
        send(kind)
        sys.exit(7 if case in ('worker-exit-failure', 'cold-c-reconcile') and dispatch['purpose'] == 'prepare' else 0)
    assert kind in ('PLAN', 'APPLY', 'RECONCILE')
    purpose = {'PLAN': 'plan', 'APPLY': 'apply', 'RECONCILE': 'reconcile'}[kind]
    scope_id = digest((session + purpose).encode())
    if kind != 'PLAN':
        raw = (directory / 'state/preparation-plan.json').read_bytes()
        envelope = (directory / 'state/preparation-envelope.json').read_bytes()
        journal = json.loads((directory / 'state/journal.json').read_bytes())
        assert digest(raw) == command['planSha256'] == json.loads(envelope)['planSha256'] == journal['planSha256']
        assert digest(envelope) == command['envelopeSha256'] and journal['phase'] in ('preparation-intent', 'prepared')
    send('SQL_SCOPE_OPENED', scopeId=scope_id, purpose=purpose, access='read-only' if kind == 'RECONCILE' else 'read-write')
    continued = receive()
    assert continued['type'] == 'CONTINUE' and continued['sequence'] == sequence and continued['scopeId'] == scope_id
    if kind == 'PLAN':
        if case == 'plan-query-restore':
            send('CLOSED_RESULT', scopeId=scope_id, planSha256=None, outcome='query-failed', errorCode='query-rejected')
        else:
            value = dict(schema=1, id='00000000-0000-4000-8000-000000000000' if case == 'plan-id-drift' else command['planId'],
                         timestamp=command['timestamp'], snapshotSha256=command['snapshotSha256'], settingsSha256=command['settingsSha256'],
                         selections=[], expectedRows=[], preservedRows=[])
            raw = json.dumps(value, separators=(',', ':')).encode()
            send('PLAN_READY', payload=raw, planSha256=digest(raw))
    else:
        failed = case == 'settled-query-restore'
        # Model WAL normalization: the live raw bytes may change while the retained pair stays exact.
        original = json.loads(plan['originalManifest'])
        if kind == 'APPLY': pathlib.Path(original['databasePath']).write_bytes(b'owned-query-normalized-db')
        send('CLOSED_RESULT', scopeId=scope_id, planSha256=command['planSha256'],
             outcome='query-failed' if failed else 'applied', errorCode='query-rejected' if failed else None)
`;

const preparationPrefix = ownerDriver.slice(0, ownerDriver.indexOf("def forbidden_child("));
NodeAssert.ok(
  preparationPrefix.endsWith("Maintenance.load, Maintenance.verify = load_owned, verify_owned\n"),
);
const preparationDriver =
  preparationPrefix +
  String.raw`
import contextlib, fcntl, signal
Process, Worker, Pair = scope['_PreparationProcess'], scope['_PreparationWorker'], scope['_ColdPairTransaction']
commands = []
real_start, real_close = Process.start, Process.close
def tracked_start(self):
    commands.append(self)
    real_start(self)
Process.start = tracked_start
def closure():
    complete = all(not item.spawn_unknown and (not item.spawn_attempted or item.reaped) and item.closed for item in commands)
    value = {'schema': 1, 'complete': complete, 'processes': [item.evidence() for item in commands], 'pidSignalAuthority': False}
    path = root / 'fixture-closure.json'
    with path.open('w') as stream:
        json.dump(value, stream)
        stream.flush()
        os.fsync(stream.fileno())
    return complete
def cancelled(signum, frame): raise KeyboardInterrupt('owned preparation fixture cancellation')
signal.signal(signal.SIGTERM, cancelled)
signal.signal(signal.SIGINT, cancelled)
def portable_identity(owner, path, kind):
    assert path.is_relative_to(root)
    info = path.lstat()
    result = [str(info.st_dev), str(info.st_ino), info.st_uid, stat.S_IMODE(info.st_mode)]
    if kind == 'file': result += [info.st_size, info.st_mtime_ns]
    return result
scope['_cold_pair_identity'] = portable_identity
scope['_cold_pair_clone'] = lambda source, parent, name: False
scope['_COLD_PAIR_HEADROOM'] = 16384
scope['_PREPARATION_LIMITS']['seconds'] = 5
scope['_PREPARATION_LIMITS']['drainSeconds'] = 1
runtime = directory / 'tools/jones-offline-preparation'
runtime_raw = ('#!' + sys.executable + '\n' + payload['child']).encode()
write(runtime, runtime_raw, 0o500)
write(directory / 'owned-fixture-case', scenario.encode())
config = b'{"main":"offlinePreparationEntry.mjs","mainFormat":"module","executable":"node","output":"jones-offline-preparation","disableExperimentalSEAWarning":true,"useSnapshot":false,"useCodeCache":false,"execArgv":[],"execArgvExtension":"none"}\n'
bundle = dict(bundle, schema=3, repository='Jones-Systems/Jones-Code', sourceSha='3' * 40, sourceTree='4' * 40,
              version='1.0.0-preview.20261011.1', bootstrapSha256='5' * 64, executorSha256='6' * 64,
              identityNativeSourceSha256='7' * 64, nativeArchitecture='arm64',
              preparation={'schema': 1, 'protocol': 1, 'provenanceSha256': '8' * 64,
                           'sourceInputHashes': {key: '9' * 64 for key in scope['_PREPARATION_SOURCE_INPUTS']},
                           'entry': {'sha256': 'a' * 64, 'size': 1}, 'seaConfig': {'sha256': sha(config), 'size': len(config)},
                           'executable': {'name': runtime.name, 'sha256': sha(runtime_raw), 'size': len(runtime_raw)},
                           'node': {'version': '26.8.2', 'platform': 'darwin', 'architecture': 'arm64', 'archiveSha256': 'b' * 64,
                                    'archiveBytes': 1, 'checksumsSha256': 'c' * 64, 'runtimeSha256': 'd' * 64, 'runtimeBytes': 1},
                           'sqlite': {'version': '3.50.0', 'sourceId': '2026-10-11 00:00:00 ' + 'e' * 64}})
if scenario == 'v2-held':
    bundle.pop('preparation')
    bundle['schema'] = 2
plan['bundle'] = bundle
plan_raw = encode(plan) + b'\n'
journal['planSha256'] = sha(plan_raw)
owner_manifest = dict(original, owner='maintenance', generation=generation, maintenance={'schema': 1, 'planSha256': sha(plan_raw)})
for path, raw, mode in ((directory / 'plan.json', plan_raw, 0o400),
                        (directory / 'journal.json', encode(journal) + b'\n', 0o600),
                        (maintenance_root / 'current.json', encode({'schema': 1, 'generation': generation, 'planSha256': sha(plan_raw)}) + b'\n', 0o400),
                        (manifest, encode(owner_manifest) + b'\n', 0o600)):
    path.chmod(0o600)
    write(path, raw, mode)
if scenario == 'unretained-runtime': runtime.unlink()
real_durable = scope['durable_file']
fault = {'fired': False}
def durable(path, raw, mode, previous=None):
    is_intent = path == directory / 'state/journal.json' and json.loads(raw)['phase'] == 'preparation-intent'
    if scenario == 'plan-publication-failure' and path == directory / 'state/preparation-envelope.json':
        fault['fired'] = True
        raise OSError(errno.EIO, 'owned envelope publication failure')
    real_durable(path, raw, mode, previous)
    if is_intent:
        events.append('intent-durable')
        assert (directory / 'state/preparation-plan.json').is_file() and (directory / 'state/preparation-envelope.json').is_file()
        if scenario == 'intent-sync-failure':
            fault['fired'] = True
            raise OSError(errno.EIO, 'owned error after intent durability')
scope['durable_file'] = durable
def observed(self, path, capture=False, access=None):
    self.check()
    self.session.boundary()
    assert self.process.child is not None and not self.process.reaped
    assert len(self.inherited) == 2 and self.session.held['fd'] in self.inherited and len(set(self.inherited)) == 2
    assert os.fstat(self.session.held['fd']).st_ino == (manifest.parent / 'jones-activation.lock').stat().st_ino
    lock_test = os.open(manifest.parent / 'jones-activation.lock', os.O_RDWR)
    try:
        try: fcntl.flock(lock_test, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError: pass
        else: raise AssertionError('Captured worker lost the held lock')
    finally: os.close(lock_test)
    if capture:
        assert access is None and path == database
        events.append('ready-captured')
    elif access is None: assert path == self.work / 'state.sqlite'
    else:
        purpose = 'plan' if self.work is not None and path == self.work / 'state.sqlite' else 'reconcile' if access == 'read-only' else 'apply'
        events.append(purpose + '-paused')
        if purpose != 'plan':
            current = json.loads((directory / 'state/journal.json').read_bytes())
            assert current['phase'] in ('preparation-intent', 'prepared')
            assert (directory / 'state/preparation-plan.json').is_file() and (directory / 'state/preparation-envelope.json').is_file()
        if scenario == 'paused-cancel':
            fault['fired'] = True
            raise KeyboardInterrupt('owned cancellation while child waits for CONTINUE')
    self.save_receipt('mechanical-observer')
Worker.observe = observed
def close(self):
    real_close(self)
    if scenario == 'worker-close-failure' and not fault['fired']:
        fault['fired'] = True
        raise OSError(errno.EIO, 'owned failure after actual descriptor close')
Process.close = close
real_clean = Worker.clean_work
def clean(self):
    assert self.process.reaped and self.process.closed
    events.append('worker-reaped')
    if scenario == 'work-replaced' and self.work is not None and not fault['fired']:
        fault['fired'] = True
        self.work.rename(self.work.with_name(self.work.name + '-retained'))
        self.work.mkdir(mode=0o700)
        write(self.work / 'state.sqlite', b'foreign-replacement')
    real_clean(self)
Worker.clean_work = clean
real_observed = Pair.preparation_observed
def preparation_observed(self, digest):
    worker = sessions[-1].worker
    assert worker.settled and worker.process.reaped and worker.process.closed and worker.work is None
    events.append('prepared-observed')
    return real_observed(self, digest)
Pair.preparation_observed = preparation_observed
def refusal(body, kind=BaseException):
    try: body()
    except kind as failure: return failure
    raise AssertionError('Expected preparation refusal')
def enter(body):
    with scope['fixed_lock'](manifest) as current:
        assert callable(current) and not hasattr(current, 'fd')
        with scope['_cold_owner_session']() as session:
            sessions.append(session)
            body(session)
def prepare(session):
    refusal(lambda: session.prepare({'path': str(database), 'authorized': True}), TypeError)
    result = session.prepare()
    assert result['workerReaped'] is True and result['activationAllowed'] is False
    if scenario in ('settled-query-restore', 'plan-query-restore'):
        assert result['status'] == 'query-failed'
        session.begin_restore()
        assert session.reconcile_restore()['status'] == 'pair-restored-gates-retained'
        assert database.read_bytes() == b'original-db'
    else: assert result['status'] == 'applied'
def failed_prepare(session):
    failure = refusal(lambda: prepare(session), Refused)
    if scenario == 'v2-held': assert not isinstance(failure, scope['_PreparationFailure'])
    else:
        assert isinstance(failure, scope['_PreparationFailure'])
        primary = failure.errors[0]
        if scenario in ('plan-publication-failure', 'intent-sync-failure', 'worker-close-failure'):
            assert isinstance(primary, OSError) and primary.errno == errno.EIO
        elif scenario == 'paused-cancel': assert isinstance(primary, KeyboardInterrupt)
        else: assert isinstance(primary, Refused)
    assert session.failure is not None
    refusal(session.begin_restore, Refused)
    refusal(session.prepare, Refused)
fixture_failure = None
try:
    failures = {'plan-id-drift', 'plan-publication-failure', 'intent-sync-failure', 'worker-exit-failure',
                'worker-close-failure', 'paused-cancel', 'work-replaced', 'cold-c-reconcile', 'v2-held', 'unretained-runtime'}
    if scenario in failures:
        refusal(lambda: enter(failed_prepare))
        before = (directory / 'journal.json').read_bytes()
        refusal(lambda: enter(prepare))
        assert (directory / 'journal.json').read_bytes() == before
        if scenario in ('v2-held', 'unretained-runtime'):
            assert not (directory / 'state').exists() and not commands
        else:
            assert commands and all(item.reaped and item.closed for item in commands)
            assert json.loads(before)['coldOwner']['phase'] == 'entered'
            assert (directory / 'state/previous-pair/database').read_bytes() == b'original-db'
            assert not list((directory / 'state').glob('preparation-evidence-*.json'))
        if scenario in ('plan-id-drift', 'plan-publication-failure', 'intent-sync-failure', 'paused-cancel'):
            assert 'apply-paused' not in events
        if scenario == 'work-replaced':
            assert sessions[-1].worker.work.joinpath('state.sqlite').read_bytes() == b'foreign-replacement'
        if scenario == 'cold-c-reconcile':
            original_plan = (directory / 'state/preparation-plan.json').read_bytes()
            original_envelope = (directory / 'state/preparation-envelope.json').read_bytes()
            assert database.read_bytes() == b'owned-query-normalized-db'
            epoch['current'] = epoch_c
            def recover(session):
                assert session.recover_preparation()['status'] == 'applied'
                assert (directory / 'state/preparation-plan.json').read_bytes() == original_plan
                assert (directory / 'state/preparation-envelope.json').read_bytes() == original_envelope
                session.begin_restore()
                session.reconcile_restore()
                assert database.read_bytes() == b'original-db'
            enter(recover)
            assert events.count('plan-paused') == 1 and events.count('reconcile-paused') == 1
    else:
        enter(prepare)
        assert json.loads((directory / 'journal.json').read_bytes())['coldOwner']['phase'] == 'finalized'
        assert all(item.reaped and item.closed for item in commands)
        assert not list((directory / 'state/preparation-work').iterdir())
        if scenario == 'prepare-success':
            assert events.index('intent-durable') < events.index('apply-paused') < events.index('worker-reaped') < events.index('prepared-observed')
        if scenario == 'plan-query-restore': assert not (directory / 'state/preparation-plan.json').exists()
        refusal(lambda: enter(prepare))
    assert all(not session.live for session in sessions)
except BaseException as failure: fixture_failure = failure
finally:
    failures = [] if fixture_failure is None else [fixture_failure]
    for command in commands:
        try: command.stop()
        except BaseException as failure: failures.append(failure)
        try: real_close(command)
        except BaseException as failure: failures.append(failure)
    try:
        if not closure(): failures.append(AssertionError('Owned preparation child custody is unproved'))
    except BaseException as failure: failures.append(failure)
    if failures: raise scope['_PreparationFailure'](failures, str(root / 'fixture-closure.json'))
print(json.dumps({'case': scenario, 'passed': True, 'qualification': 'owned-mechanical-captured-preparation',
                  'stateAdmissionAllowed': False, 'nativeColdOwnerQualified': False, 'sqliteQualified': False}))
`;

export async function runColdPreparationFixture(scenario, { signal } = {}) {
  NodeAssert.ok(coldPreparationCases.includes(scenario));
  const sources = await readColdMaintenanceSources();
  return withLegacyIdentityScratch(async (root) => {
    const payload = NodePath.join(root, "preparation.json");
    const entry = NodePath.join(root, "preparation.py");
    const evidence = NodePath.join(root, "fixture-closure.json");
    await NodeFSP.writeFile(
      payload,
      JSON.stringify({ maintenance: sources.maintenance, case: scenario, child: preparationChild }),
    );
    await NodeFSP.writeFile(entry, preparationDriver);
    await NodeFSP.writeFile(
      evidence,
      JSON.stringify({ schema: 1, complete: false, processes: [], pidSignalAuthority: false }),
    );
    let failure;
    let result;
    try {
      result = await runLegacyIdentityFixtureCommand(
        "/usr/bin/python3",
        ["-I", "-B", entry, payload, root],
        root,
        { signal },
      );
      NodeAssert.equal(result.code, 0, result.stderr);
      NodeAssert.equal(result.stderr, "");
    } catch (cause) {
      failure = cause;
    }
    try {
      const raw = await NodeFSP.readFile(evidence);
      NodeAssert.ok(raw.length <= 32768);
      const closure = JSON.parse(raw);
      NodeAssert.equal(closure.schema, 1);
      NodeAssert.equal(closure.complete, true);
      NodeAssert.equal(closure.pidSignalAuthority, false);
      NodeAssert.ok(Array.isArray(closure.processes) && closure.processes.length <= 16);
      for (const process of closure.processes) {
        NodeAssert.equal(process.spawnUnknown, false);
        NodeAssert.ok(!process.spawnAttempted || process.reaped === true);
        NodeAssert.equal(process.streamsClosed, true);
      }
    } catch (cleanup) {
      const retained = new AggregateError(
        failure ? [failure, cleanup] : [cleanup],
        "Owned preparation closure is unproved; exact scratch retained.",
        { cause: failure ?? cleanup },
      );
      retained.preserveScratch = true;
      retained.evidencePath = evidence;
      throw retained;
    }
    if (failure) throw failure;
    return JSON.parse(result.stdout.trim());
  });
}
