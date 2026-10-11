// @effect-diagnostics nodeBuiltinImport:off - These Python protocol tests create no application runtime or native admission evidence.
import * as NodeChildProcess from "node:child_process";
import { describe, expect, it } from "vite-plus/test";
import { jonesLegacyBootstrapSource } from "./jonesLegacyBootstrapSource.ts";
import { jonesLegacyMaintenanceSource } from "./jonesLegacyMaintenanceSource.ts";

const protocolFixture = String.raw`
import contextlib, io, json, pathlib, sys
payload = json.load(sys.stdin)
scope = {'__name__': 'maintenance_protocol_fixture'}
exec(compile(payload['source'], 'maintenance.py', 'exec'), scope)
fault = payload['case']
if fault == 'anchor':
    reads = []
    def forbidden(path, *args):
        reads.append(str(path))
        raise AssertionError('Invalid anchor reached a filesystem read')
    scope['read_owned'] = forbidden
    actual = sys.platform
    sys.platform = 'darwin'
    try:
        output = io.StringIO()
        with contextlib.redirect_stdout(output):
            code = scope['maintenance_main'](['cancel', '--manifest', '/fixture/private/state.sqlite'])
    finally: sys.platform = actual
    assert code == 1 and reads == []
    report = json.loads(output.getvalue())
    assert report['activationAllowed'] is False and report['stateAdmissionAllowed'] is False
elif fault == 'arguments':
    with contextlib.redirect_stderr(io.StringIO()):
        try: scope['maintenance_main'](['cancel', '--manifest', '/fixture/runtime/jones-active-install.json', '--selection', '/arbitrary'])
        except SystemExit as result: assert result.code == 2
        else: raise AssertionError('Cancel accepted selection authority')
elif fault == 'cursor':
    owner = scope['Maintenance'](pathlib.Path('/fixture/runtime/jones-active-install.json'), lambda: None)
    owner.original = {'appPath': '/fixture/Old.app', 'executablePath': '/fixture/Old.app/Contents/MacOS/Jones', 'generation': '1' * 64}
    owner.plan = {'maintenanceGeneration': '2' * 64}
    owner.gates = [{'path': '/fixture/Old.app/Contents/MacOS/Jones', 'app': '/fixture/Old.app'}, {'path': '/fixture/Retained.app/Contents/MacOS/Jones', 'app': '/fixture/Retained.app'}]
    owner.journal = {'phase': 'armed', 'done': [owner.gates[0]['path']], 'intent': None, 'cancel': None, 'armedBootSessionUuid': None}
    try: owner.validate_cursor()
    except scope['Refused']: pass
    else: raise AssertionError('Incomplete gates were accepted as armed')
elif fault == 'no-bytecode':
    assert sys.dont_write_bytecode is True
    assert payload['source'].index('sys.dont_write_bytecode = True') < payload['source'].index('import argparse')
elif fault == 'cold-owner-cursor':
    value = {'phase': 'armed', 'armedBootSessionUuid': '00000000-0000-4000-8000-000000000001',
             'coldOwner': {'schema': 1, 'sessionId': 'a' * 64, 'phase': 'entered',
                           'bootSessionUuid': '00000000-0000-4000-8000-000000000002'}}
    scope['_validate_cold_owner_cursor'](value)
    for changed in (dict(value['coldOwner'], schema=True), dict(value['coldOwner'], phase='released'),
                    dict(value['coldOwner'], bootSessionUuid=value['armedBootSessionUuid']),
                    dict(value['coldOwner'], callerAuthorized=True), dict(value['coldOwner'], sessionId='a')):
        try: scope['_validate_cold_owner_cursor'](dict(value, coldOwner=changed))
        except scope['Refused']: pass
        else: raise AssertionError('Malformed owner epoch accepted')
elif fault == 'cancel-before-state-fence':
    owner = scope['Maintenance'](pathlib.Path('/fixture/runtime/jones-active-install.json'), lambda: None)
    owner.journal = {'coldOwner': {'phase': 'entered'}}
    owner.verify = lambda: (_ for _ in ()).throw(AssertionError('Cancellation crossed entered fence'))
    try: owner.cancel()
    except scope['Refused']: pass
    else: raise AssertionError('Entered session permitted PRE-STATE cancellation')
elif fault == 'private-worker':
    try: scope['_PreparationWorker']()
    except scope['Refused']: pass
    else: raise AssertionError('Unbound worker received a session')
    for operation in ('prepare', 'recover_preparation', 'snapshot'):
        with contextlib.redirect_stderr(io.StringIO()):
            try: scope['maintenance_main']([operation, '--manifest', '/fixture/runtime/jones-active-install.json'])
            except SystemExit as result: assert result.code == 2
            else: raise AssertionError('Public entry exposed a state operation')
elif fault == 'shim-bytes':
    active = {'appPath': '/fixture/Jones.app', 'executablePath': '/fixture/Jones.app/Contents/MacOS/Jones'}
    assert scope['expected_shim'](pathlib.Path('/fixture/home'), active).decode() == payload['expectedShim']
else: raise AssertionError('Unknown protocol fixture case')
print(json.dumps({'case': fault, 'passed': True}))
`;

const retentionFixture = String.raw`
import sys
sys.dont_write_bytecode = True
import contextlib, errno, hashlib, json, os, pathlib, signal, stat, tempfile
payload = json.load(sys.stdin)
scope = {'__name__': 'owned_retention_fixture'}
exec(compile(payload['source'], 'maintenance.py', 'exec'), scope)
case = payload['case']
def cancelled(signum, frame): raise KeyboardInterrupt('owned retention cancellation')
signal.signal(signal.SIGTERM, cancelled)
with tempfile.TemporaryDirectory(prefix='jones-retention-') as temporary:
    root = pathlib.Path(temporary).resolve(strict=True)
    print(json.dumps({'event': 'retention-fixture-root', 'root': str(root)}), file=sys.stderr, flush=True)
    encode, sha, Refused = scope['encoded'], scope['sha'], scope['Refused']
    manifest = root / 'runtime/jones-active-install.json'
    tools = root / 'Candidate.app/Contents/Resources/jones-bootstrap'
    staging = manifest.parent / 'jones-updates/staging'
    for path in (tools, staging): path.mkdir(mode=0o700, parents=True)
    original = {'owner': 'desktop', 'sourceSha': scope['LEGACY_SOURCE'], 'generation': '1' * 64,
                'appPath': str(root / 'Old.app'), 'executablePath': str(root / 'Old.app/Contents/MacOS/Jones')}
    candidate = {'source': '2' * 40, 'tree': '3' * 40, 'version': '1.0.0-preview.20261011.1'}
    selection_path = staging / (scope['LEGACY_SOURCE'] + '-owned.json')
    selection = {'app': {'appPath': str(root / 'Candidate.app'), 'version': candidate['version']}}
    def write(path, raw, mode=0o600):
        if path.exists(): path.chmod(0o600)
        path.write_bytes(raw)
        path.chmod(mode)
    write(manifest, encode(original))
    write(selection_path, encode(selection))
    selection_sha = sha(selection_path.read_bytes())
    arm64 = bytes.fromhex('cffaedfe0c0000010000000002000000') + b'\0' * 16
    runtime = arm64 + b'x' * (2 * 1024 * 1024 + 7)
    resources = {'bootstrap.py': b'owned bootstrap', 'executor.py': b'owned executor',
                 'maintenance.py': b'owned maintenance', 'identity-native': arm64,
                 'jones-offline-preparation': runtime}
    for name, raw in resources.items(): write(tools / name, raw, 0o500)
    config = b'{"main":"offlinePreparationEntry.mjs","mainFormat":"module","executable":"node","output":"jones-offline-preparation","disableExperimentalSEAWarning":true,"useSnapshot":false,"useCodeCache":false,"execArgv":[],"execArgvExtension":"none"}\n'
    bundle = {'schema': 3, 'repository': 'Jones-Systems/Jones-Code', 'sourceSha': candidate['source'], 'sourceTree': candidate['tree'], 'version': candidate['version'],
              'bootstrapSha256': sha(resources['bootstrap.py']), 'executorSha256': sha(resources['executor.py']),
              'maintenanceSha256': sha(resources['maintenance.py']), 'identityNativeSha256': sha(arm64),
              'identityNativeSourceSha256': '4' * 64, 'nativeArchitecture': 'arm64',
              'preparation': {'schema': 1, 'protocol': 1, 'provenanceSha256': '5' * 64,
                  'sourceInputHashes': {name: '6' * 64 for name in sorted(scope['_PREPARATION_SOURCE_INPUTS'])},
                  'entry': {'sha256': '7' * 64, 'size': 1}, 'seaConfig': {'sha256': sha(config), 'size': len(config)},
                  'executable': {'name': 'jones-offline-preparation', 'sha256': sha(runtime), 'size': len(runtime)},
                  'node': {'version': '26.8.2', 'platform': 'darwin', 'architecture': 'arm64', 'archiveSha256': '8' * 64,
                           'archiveBytes': 1, 'checksumsSha256': '9' * 64, 'runtimeSha256': 'a' * 64, 'runtimeBytes': 1},
                  'sqlite': {'version': '3.50.0', 'sourceId': '2026-10-11 00:00:00 ' + 'b' * 64}}}
    if case == 'old-v2-new-arm': bundle.pop('preparation'); bundle['schema'] = 2
    if case == 'missing-worker': del bundle['preparation']['sourceInputHashes']['apps/server/src/jones/updates/offlinePreparationWorker.ts']
    if case == 'extra-proof': bundle['preparation']['callerApproved'] = True
    if case == 'rebound-source': bundle['sourceSha'] = 'f' * 40
    if case == 'wrong-runtime-hash': bundle['preparation']['executable']['sha256'] = 'f' * 64
    if case == 'wrong-runtime-size': bundle['preparation']['executable']['size'] += 1
    if case == 'too-large-runtime': bundle['preparation']['executable']['size'] = 256 * 1024 * 1024 + 1
    if case == 'wrong-runtime-mode': (tools / 'jones-offline-preparation').chmod(0o700)
    if case == 'missing-runtime': (tools / 'jones-offline-preparation').unlink()
    if case == 'wrong-runtime-architecture':
        runtime = b'not arm64 executable' + runtime[20:]
        write(tools / 'jones-offline-preparation', runtime, 0o500)
        bundle['preparation']['executable'] = dict(bundle['preparation']['executable'], sha256=sha(runtime), size=len(runtime))
    raw_bundle = encode(bundle) + b'\n'
    if case == 'canonical-proof': raw_bundle = json.dumps(bundle, indent=2).encode() + b'\n'
    write(tools / 'BUNDLE.json', raw_bundle, 0o400)
    events, copying, reads = [], {'active': False, 'fault': False}, []
    runtime_name = scope['_PREPARATION_NAME']
    Observation, Maintenance = scope['Observation'], scope['Maintenance']
    real_file, real_raw, real_sync = Observation.file, Observation.raw, scope['sync_fd']
    real_fdopen = os.fdopen
    def fdopen(fd, mode):
        stream = real_fdopen(fd, mode)
        if case != 'copy-close-error' or not copying['active'] or mode != 'wb': return stream
        class Closing:
            def __enter__(self): return stream
            def __exit__(self, *args):
                stream.close()
                copying['fault'] = True
                raise OSError(errno.EIO, 'owned uncertainty after actual copy descriptor close')
        return Closing()
    os.fdopen = fdopen
    @contextlib.contextmanager
    def bounded_file(self, path, limit):
        with real_file(self, path, limit) as stream:
            if path.name != runtime_name: yield stream; return
            assert limit == 256 * 1024 * 1024
            class Reader:
                def fileno(self): return stream.fileno()
                def read(self, size):
                    assert 0 < size <= 1048576
                    reads.append(size)
                    return stream.read(size)
            yield Reader()
    Observation.file = bounded_file
    def bounded_raw(self, path, limit=131072):
        assert path.name != runtime_name, 'SEA reached generic buffered metadata read'
        return real_raw(self, path, limit)
    Observation.raw = bounded_raw
    def anchor(path, active): assert path == manifest and active == original
    scope['maintenance_anchor'] = anchor
    # Artifact inventory and native identities are modeled here; retention,
    # fixed-lock ownership, source observations and durable writes are real.
    gate = {'path': original['executablePath'], 'app': original['appPath'], 'originalMode': 0o500, 'gatedMode': 0o400}
    class Inventory:
        def __init__(self, *args): pass
        def run(self): return {'objects': [gate]}
    scope['StrictInventory'] = Inventory
    def inspect(path, selected, digest, bootstrap):
        assert (path, selected, digest, bootstrap) == (manifest, selection_path, selection_sha, tools / 'bootstrap.py')
        checked = Observation()
        actual, bundle_digest = scope['_bootstrap_bundle'](checked, tools, candidate)
        binding = {'sourceSha': actual['sourceSha'], 'sourceTree': actual['sourceTree'], 'bundleSha256': bundle_digest,
                   'executorSha256': actual['executorSha256'], 'manifestPath': str(manifest), 'manifestSha256': sha(manifest.read_bytes()),
                   'selectionPath': str(selection_path), 'selectionSha256': selection_sha, 'installedGeneration': original['generation'],
                   'installedSource': original['sourceSha'], 'status': 'executor-ready', 'activationAllowed': False}
        return checked, original, None, None, None, resources['executor.py'], binding
    scope['inspect_inputs'] = inspect
    scope['__file__'] = str(tools / 'maintenance.py')
    real_retain = Maintenance.retain_preparation_runtime
    def retain(self, checked, source_tools, retained):
        copying['active'] = True
        if case == 'source-replaced':
            replacement = tools / 'replacement'
            write(replacement, runtime, 0o500)
            os.replace(replacement, tools / runtime_name)
        try: observed = real_retain(self, checked, source_tools, retained)
        finally: copying['active'] = False
        events.append('runtime-retained')
        return observed
    Maintenance.retain_preparation_runtime = retain
    def native(native_path, digest, requests):
        assert events == ['runtime-retained']
        expected = [manifest.parent / 'jones-activation.lock', owner.directory / 'tools/maintenance.py', owner.directory / 'tools/identity-native', owner.directory / 'tools' / runtime_name]
        assert requests == [('file', path) for path in expected]
        assert not (owner.directory / 'plan.json').exists() and not (owner.root / 'current.json').exists()
        assert expected[-1].read_bytes() == runtime and stat.S_IMODE(expected[-1].stat().st_mode) == 0o500
        events.append('native-identities')
        if case == 'retained-replaced':
            replacement = expected[-1].with_name('owned-replacement')
            write(replacement, runtime, 0o500)
            os.replace(replacement, expected[-1])
        return {'objects': [{'path': str(path)} for path in expected]}
    scope['native_identity'], scope['stable_native'] = native, lambda item: item['path']
    real_durable = scope['durable_file']
    def durable(path, raw, mode, previous=None):
        if path.name in ('plan.json', 'journal.json', 'current.json'):
            assert events[:2] == ['runtime-retained', 'native-identities']
            events.append(path.name)
        real_durable(path, raw, mode, previous)
    scope['durable_file'] = durable
    def sync(fd):
        real_sync(fd)
        if case == 'copy-sync-error' and copying['active'] and not copying['fault'] and stat.S_ISREG(os.fstat(fd).st_mode) and os.fstat(fd).st_size == len(runtime):
            copying['fault'] = True
            raise OSError(errno.EIO, 'owned retention sync uncertainty')
    scope['sync_fd'] = sync
    def reconciled(self):
        assert events[:5] == ['runtime-retained', 'native-identities', 'plan.json', 'journal.json', 'current.json']
        events.append('reconcile')
        return scope['maintenance_report']('retention-only-fixture')
    Maintenance.reconcile = reconciled
    failure = None
    with scope['fixed_lock'](manifest) as current:
        def checked_lock():
            current()
            if copying['active'] and case in ('copy-error', 'copy-cancel'):
                path = owner.directory / 'tools' / runtime_name
                if path.exists() and path.stat().st_size > 0 and not copying['fault']:
                    copying['fault'] = True
                    if case == 'copy-cancel': raise KeyboardInterrupt('owned retention interrupted')
                    raise OSError(errno.EIO, 'owned retention copy failure')
        owner = Maintenance(manifest, checked_lock)
        try: result = owner.arm(selection_path, selection_sha)
        except BaseException as error: failure = error
    if case in ('success', 'old-v2-recovery'):
        if failure is not None: raise failure
        assert result['activationAllowed'] is False and result['stateAdmissionAllowed'] is False
        assert owner.plan['toolIdentities'] == [str(owner.directory / 'tools' / name) for name in ('maintenance.py', 'identity-native', runtime_name)]
        assert len(reads) >= 6 and scope['LIMITS']['metadataBytes'] == 64 * 1024 * 1024
        if case == 'old-v2-recovery':
            # A retained v2 plan is loaded as v2; it is never upgraded into preparation authority.
            old_plan = dict(owner.plan, bundle={key: value for key, value in bundle.items() if key != 'preparation'}, toolIdentities=owner.plan['toolIdentities'][:2])
            old_plan['bundle']['schema'] = 2
            old_raw = encode(old_plan) + b'\n'
            old_journal = dict(owner.journal, planSha256=sha(old_raw))
            write(owner.directory / 'plan.json', old_raw, 0o400)
            write(owner.directory / 'journal.json', encode(old_journal) + b'\n')
            write(owner.root / 'current.json', encode({'schema': 1, 'generation': old_plan['maintenanceGeneration'], 'planSha256': sha(old_raw)}) + b'\n', 0o400)
            (owner.directory / 'tools' / runtime_name).unlink()
            scope['__file__'] = str(owner.directory / 'tools/maintenance.py')
            with scope['fixed_lock'](manifest) as current:
                recovered = Maintenance(manifest, current)
                recovered.load()
                recovered.sync_evidence()
                assert recovered.bundle['schema'] == 2 and len(recovered.plan['toolIdentities']) == 2
    else:
        assert failure is not None and isinstance(failure, (Refused, OSError, KeyboardInterrupt))
        assert manifest.read_bytes() == encode(original) and 'reconcile' not in events
        assert not (owner.root / 'current.json').exists()
        if case in ('copy-error', 'copy-cancel', 'copy-sync-error', 'copy-close-error', 'source-replaced', 'retained-replaced'):
            assert owner.root.is_dir() and not (owner.directory / 'plan.json').exists() and not (owner.directory / 'journal.json').exists()
            if case in ('copy-error', 'copy-cancel'):
                partial = owner.directory / 'tools' / runtime_name
                assert 0 < partial.stat().st_size < len(runtime) and stat.S_IMODE(partial.stat().st_mode) == 0o600
            if case in ('copy-sync-error', 'copy-close-error'): assert copying['fault'] and isinstance(failure, OSError) and failure.errno == errno.EIO
        else: assert not owner.root.exists()
assert not root.exists()
print(json.dumps({'case': case, 'passed': True, 'cleanupObserved': True, 'nativeQualified': False, 'stateAdmissionAllowed': False}))
`;

describe("standalone pre-state maintenance protocol", () => {
  it.each([
    "success",
    "old-v2-recovery",
    "old-v2-new-arm",
    "canonical-proof",
    "missing-worker",
    "extra-proof",
    "rebound-source",
    "missing-runtime",
    "wrong-runtime-hash",
    "wrong-runtime-size",
    "wrong-runtime-mode",
    "wrong-runtime-architecture",
    "too-large-runtime",
    "copy-error",
    "copy-sync-error",
    "copy-close-error",
    "source-replaced",
    "retained-replaced",
    "copy-cancel",
  ])("retains the fixed preparation runtime for %s", (scenario) => {
    const result = NodeChildProcess.spawnSync("python3", ["-I", "-B", "-c", retentionFixture], {
      input: JSON.stringify({ source: jonesLegacyMaintenanceSource, case: scenario }),
      encoding: "utf8",
      timeout: 15000,
      maxBuffer: 1024 * 1024,
    });
    expect(result.error).toBeUndefined();
    expect(result.status, result.stderr).toBe(0);
    expect(JSON.parse(result.stdout)).toEqual({
      case: scenario,
      passed: true,
      cleanupObserved: true,
      nativeQualified: false,
      stateAdmissionAllowed: false,
    });
    expect(JSON.parse(result.stderr).event).toBe("retention-fixture-root");
  });
  it("composes shared definitions while replacing only the checked entry", () => {
    const entry = "if __name__ == '__main__':\n    sys.exit(main())\n";
    expect(jonesLegacyBootstrapSource.endsWith(entry)).toBe(true);
    expect(
      jonesLegacyMaintenanceSource.startsWith(jonesLegacyBootstrapSource.slice(0, -entry.length)),
    ).toBe(true);
    expect(jonesLegacyMaintenanceSource.endsWith("    sys.exit(maintenance_main())\n")).toBe(true);
    expect(jonesLegacyMaintenanceSource).not.toContain("$" + "{");
    expect(jonesLegacyMaintenanceSource).not.toContain("`");
  });

  it.each([
    "anchor",
    "arguments",
    "cursor",
    "no-bytecode",
    "shim-bytes",
    "cold-owner-cursor",
    "cancel-before-state-fence",
    "private-worker",
  ])("enforces %s before granting any maintenance effect", (scenario) => {
    const result = NodeChildProcess.spawnSync("python3", ["-I", "-B", "-c", protocolFixture], {
      input: JSON.stringify({
        source: jonesLegacyMaintenanceSource,
        case: scenario,
        expectedShim: [
          "#!/bin/sh",
          "# Written by T3 Code: runs the desktop app's bundled t3 CLI.",
          "export T3CODE_CLI_PATH='/fixture/home/bin/t3'",
          "home='/fixture/home'",
          'export T3CODE_HOME="${T3CODE_HOME:-$home}"',
          "export ELECTRON_RUN_AS_NODE=1",
          "app='/fixture/Jones.app/Contents/MacOS/Jones'",
          'if [ ! -x "$app" ]; then',
          '  echo "T3 Code has moved or been removed. Open the app once to update this command." >&2',
          "  exit 127",
          "fi",
          'exec "$app" \'/fixture/Jones.app/Contents/Resources/app.asar/apps/server/dist/bin.mjs\' "$@"',
          "",
        ].join("\n"),
      }),
      encoding: "utf8",
      timeout: 10000,
      maxBuffer: 1024 * 1024,
    });
    expect(result.error).toBeUndefined();
    expect(result.status, result.stderr).toBe(0);
    expect(JSON.parse(result.stdout)).toEqual({ case: scenario, passed: true });
  });
});
