// @effect-diagnostics-next-line nodeBuiltinImport:off -- Run only the embedded controller against captured synthetic Python children.
import * as NodeChildProcess from "node:child_process";
import { describe, expect, it } from "vite-plus/test";
import { jonesPreparationWorkerSource } from "./jonesPreparationWorkerSource.ts";

const controllerFixture = String.raw`
import contextlib, copy, datetime, errno, fcntl, hashlib, json, os, pathlib, re, shutil, signal, stat, struct, subprocess, sys, tempfile, time, types
payload = json.load(sys.stdin)
class Refused(Exception): pass
def require(value):
    if not value: raise Refused('Owned controller refusal')
scope = {'__name__': 'owned_preparation_controller', 'Refused': Refused, 'require': require,
         'json': json, 'os': os, 'subprocess': subprocess, 'struct': struct, 'datetime': datetime, 're': re,
         'contextlib': contextlib, 'pathlib': pathlib, 'decode': json.loads, '_COLD_PAIR_BOUNDS': {'bytes': 4096},
         'sha': lambda raw: hashlib.sha256(raw).hexdigest(),
         'hex_value': lambda value, length: isinstance(value, str) and re.fullmatch('[a-f0-9]{' + str(length) + '}', value) is not None,
         'integer': lambda value: type(value) is int and 1 <= value <= 9007199254740991,
         'BUNDLE_V2_KEYS': {'schema', 'repository', 'sourceSha', 'sourceTree', 'version', 'bootstrapSha256', 'executorSha256', 'maintenanceSha256', 'identityNativeSha256', 'identityNativeSourceSha256', 'nativeArchitecture'}}
exec(compile(payload['source'], 'preparation-controller.py', 'exec'), scope)
Process = scope['_PreparationProcess']
scope['_PREPARATION_LIMITS']['drainSeconds'] = 0.1
case = payload['case']
commands = []
test_owned_partial_children = {}
def fixture_settled(command):
    if command.spawn_unknown:
        captured = test_owned_partial_children.get(id(command))
        return captured is not None and captured.returncode is not None and all(stream is None or stream.closed for stream in (captured.stdin, captured.stdout, captured.stderr))
    return (not command.spawn_attempted or command.reaped) and command.closed
def cancelled(signum, frame): raise KeyboardInterrupt('owned fixture signal')
signal.signal(signal.SIGTERM, cancelled)
@contextlib.contextmanager
def owned_root():
    root = pathlib.Path(tempfile.mkdtemp(prefix='jones-preparation-controller-')).resolve(strict=True)
    try: yield root
    finally:
        if all(fixture_settled(command) for command in commands): shutil.rmtree(root)
        else:
            evidence = {'schema': 1, 'processes': [item.evidence() for item in commands], 'pidSignalAuthority': False}
            path = root / 'retained-custody.json'
            with path.open('x') as stream:
                json.dump(evidence, stream)
                stream.flush()
                os.fsync(stream.fileno())
            raise AssertionError('Owned fixture custody is unresolved; evidence retained at ' + str(path))
with owned_root() as root:
    def launch(source, limit=4096, descriptors=()):
        command = Process([sys.executable, '-I', '-B', '-c', source], descriptors, limit)
        commands.append(command)
        command.start()
        return command
    def worker_unit():
        # Only this owned fixture supplies method-level state; the production constructor remains unbound.
        worker = object.__new__(scope['_PreparationWorker'])
        worker.session = types.SimpleNamespace(poison=lambda failure: poison.append(failure), boundary=lambda: None,
                                              record={'bootSessionUuid': '00000000-0000-4000-8000-000000000001'})
        worker.owner = types.SimpleNamespace(native=root / 'observer', bundle={'identityNativeSha256': 'a' * 64})
        worker.pair = types.SimpleNamespace(stat_at=lambda fd, name: os.stat(name, dir_fd=fd, follow_symlinks=False) if (root / name).exists() else None)
        worker.process, worker.observers, worker.tokens = None, [], None
        worker.failed, worker.settled = False, False
        worker.receipt, worker.receipt_raw = {'schema': 1}, None
        worker.receipt_path = root / 'worker-custody.json'
        worker.inherited = []
        worker.check = lambda: None
        return worker
    def durable(path, raw, mode, previous=None):
        assert path.parent == root
        with path.open('wb') as stream:
            stream.write(raw)
            stream.flush()
            os.fsync(stream.fileno())
    scope['durable_file'] = durable
    poison = []
    try:
        if case in ('partial-spawn-error', 'partial-spawn-interruption'):
            worker = worker_unit()
            command = Process([sys.executable, '-I', '-B', '-c', 'import os; os.write(1,b"r"); os.read(0,1)'], (), 4096)
            worker.process = command
            commands.append(command)
            original_popen = subprocess.Popen
            initiating = OSError(errno.EINTR, 'owned constructor interruption') if case == 'partial-spawn-error' else KeyboardInterrupt('owned constructor cancellation')
            def partial_popen(*args, **kwargs):
                captured = original_popen(*args, **kwargs)
                test_owned_partial_children[id(command)] = captured
                assert captured.stdout.read(1) == b'r'
                raise initiating
            subprocess.Popen = partial_popen
            worker.perform = lambda recovery: command.start()
            try:
                try: worker.run(False)
                except scope['_PreparationFailure'] as failure:
                    assert failure.errors[0] is initiating and failure.evidence_path == str(worker.receipt_path)
                    assert len(failure.errors) >= 3
                else: raise AssertionError('Unknown constructor became ordinary completion')
                assert command.spawn_attempted and command.spawn_unknown and command.child is None
                assert not command.reaped and not command.closed and not worker.settled
                assert not fixture_settled(command)
                receipt = json.loads(worker.receipt_path.read_bytes())
                assert receipt['spawnUnknown'] is True and receipt['pid'] is None
                assert receipt['reaped'] is False and receipt['streamsClosed'] is False and receipt['pidSignalAuthority'] is False
                assert root.exists() and test_owned_partial_children[id(command)].poll() is None
                try: command.start()
                except Refused: pass
                else: raise AssertionError('Unknown spawn was retried')
            finally:
                subprocess.Popen = original_popen
                captured = test_owned_partial_children.get(id(command))
                if captured is not None:
                    captured.kill()
                    captured.wait(timeout=5)
                    for stream in (captured.stdin, captured.stdout, captured.stderr):
                        if stream is not None: stream.close()
                # Test-owned recovery is deliberately not implementation reaping evidence.
                assert command.child is None and not command.reaped and not command.closed
        elif case == 'worker-observer-double-failure':
            worker = worker_unit()
            command = launch('import os; os.write(1,b"r"); os.read(0,1)')
            assert command.read(1, time.monotonic() + 5) == b'r'
            worker.process = command
            database = root / 'state.sqlite'
            database.write_bytes(b'owned-placeholder')
            native = worker.owner.native
            native.write_text('#!' + sys.executable + '\nimport os\nos.write(1,b"{")\n')
            native.chmod(0o500)
            class OwnedObservation:
                def digest(self, path, limit):
                    assert path == native and path.stat().st_size <= limit
                    return 'a' * 64
                @contextlib.contextmanager
                def directory(self, path):
                    assert path == root
                    fd = os.open(path, os.O_RDONLY | os.O_DIRECTORY)
                    try: yield fd
                    finally: os.close(fd)
                @contextlib.contextmanager
                def file(self, path, limit):
                    assert path == database
                    with path.open('rb') as stream: yield stream
                def current(self): pass
            scope['Observation'] = OwnedObservation
            real_stop, real_close = Process.stop, Process.close
            stop_failure, close_failure = OSError(errno.EIO, 'owned observer stop'), OSError(errno.ENOSPC, 'owned observer close')
            def stop_fault(item):
                real_stop(item)
                if item.args[0] == str(native): raise stop_failure
            def close_fault(item):
                real_close(item)
                if item.args[0] == str(native): raise close_failure
            Process.stop, Process.close = stop_fault, close_fault
            try:
                try: worker.observe(database, capture=True)
                except scope['_PreparationFailure'] as failure:
                    assert isinstance(failure.errors[0], json.JSONDecodeError)
                    assert failure.errors[1:] == (stop_failure, close_failure)
                    assert failure.__cause__ is failure.errors[0]
                else: raise AssertionError('Observer failures disappeared')
                assert len(worker.observers) == 1 and worker.observers[0].reaped
            finally:
                Process.stop, Process.close = real_stop, real_close
                commands.extend(worker.observers)
        elif case == 'worker-run-cleanup-publication-failure':
            worker = worker_unit()
            command = launch('import os; os.write(1,b"r"); os.read(0,1)')
            assert command.read(1, time.monotonic() + 5) == b'r'
            worker.process = command
            initiating = OSError(errno.EINVAL, 'owned operation failure')
            stop_failure, close_failure = OSError(errno.EIO, 'owned drain stop'), OSError(errno.ENOSPC, 'owned drain close')
            publication_failure = OSError(errno.EROFS, 'owned receipt publication')
            real_stop, real_close = command.stop, command.close
            def failed_perform(recovery): raise initiating
            def failed_stop():
                real_stop()
                raise stop_failure
            def failed_close():
                real_close()
                raise close_failure
            def failed_publication(path, raw, mode, previous=None):
                durable(path, raw, mode, previous)
                raise publication_failure
            worker.perform = failed_perform
            command.stop, command.close = failed_stop, failed_close
            scope['durable_file'] = failed_publication
            try:
                try: worker.run(False)
                except scope['_PreparationFailure'] as failure:
                    assert failure.errors == (initiating, stop_failure, close_failure, publication_failure)
                    assert failure.evidence_path == str(worker.receipt_path) and failure.__cause__ is initiating
                else: raise AssertionError('A cleanup/publication failure disappeared')
                receipt = json.loads(worker.receipt_path.read_bytes())
                assert [item['errno'] for item in receipt['failures']] == [errno.EINVAL, errno.EIO, errno.ENOSPC]
                assert poison == [initiating] and worker.failed and not worker.settled
            finally:
                command.stop, command.close = real_stop, real_close
                scope['durable_file'] = durable
        elif case.startswith('bundle-'):
            config = b'{"main":"offlinePreparationEntry.mjs","mainFormat":"module","executable":"node","output":"jones-offline-preparation","disableExperimentalSEAWarning":true,"useSnapshot":false,"useCodeCache":false,"execArgv":[],"execArgvExtension":"none"}\n'
            worker_input = 'apps/server/src/jones/updates/offlinePreparationWorker.ts'
            inputs = ['pnpm-lock.yaml', 'pnpm-workspace.yaml', 'package.json', 'apps/server/package.json', 'apps/desktop/package.json', 'apps/web/package.json', 'packages/contracts/package.json', 'apps/server/vite.config.ts', 'vite.config.ts', 'scripts/jones/updates/offline-preparation-build.ts', 'apps/server/src/jones/updates/offlinePreparationEntry.ts', 'apps/server/src/jones/updates/offlinePreparation.ts', 'apps/server/src/jones/updates/offlinePreparationCore.ts', 'apps/server/src/jones/updates/offlinePreparationPlan.ts', worker_input]
            assert len(inputs) == 15 and set(inputs) == scope['_PREPARATION_SOURCE_INPUTS']
            preparation = {'schema': 1, 'protocol': 1, 'provenanceSha256': 'a' * 64, 'sourceInputHashes': {key: 'b' * 64 for key in inputs},
                'entry': {'sha256': 'c' * 64, 'size': 1}, 'seaConfig': {'sha256': hashlib.sha256(config).hexdigest(), 'size': len(config)},
                'executable': {'name': 'jones-offline-preparation', 'sha256': 'd' * 64, 'size': 1},
                'node': {'version': '26.8.2', 'platform': 'darwin', 'architecture': 'arm64', 'archiveSha256': 'e' * 64, 'archiveBytes': 1, 'checksumsSha256': 'f' * 64, 'runtimeSha256': 'a' * 64, 'runtimeBytes': 1},
                'sqlite': {'version': '3.50.2', 'sourceId': '2026-02-28 01:02:03 ' + 'a' * 64}}
            bundle = {'schema': 3, 'repository': 'Jones-Systems/Jones-Code', 'sourceSha': 'a' * 40, 'sourceTree': 'b' * 40, 'version': '0.3.0-preview.20261011.1', 'nativeArchitecture': 'arm64', 'preparation': preparation}
            bundle.update({key: 'c' * 64 for key in ('bootstrapSha256', 'executorSha256', 'maintenanceSha256', 'identityNativeSha256', 'identityNativeSourceSha256')})
            if case == 'bundle-missing-source': del preparation['sourceInputHashes'][inputs[0]]
            elif case == 'bundle-unknown-source': preparation['sourceInputHashes']['unreviewed.ts'] = 'a' * 64
            elif case == 'bundle-missing-worker': del preparation['sourceInputHashes'][worker_input]
            elif case == 'bundle-worker-hash': preparation['sourceInputHashes'][worker_input] = 'b' * 63 + '\n'
            elif case == 'bundle-worker-rebound':
                preparation['sourceInputHashes']['apps/server/src/jones/updates/../updates/offlinePreparationWorker.ts'] = preparation['sourceInputHashes'].pop(worker_input)
            elif case == 'bundle-worker-extra': preparation['sourceInputHashes']['apps/server/src/jones/updates/offlinePreparationWorker.test.ts'] = 'b' * 64
            elif case == 'bundle-zero-size': preparation['entry']['size'] = 0
            elif case == 'bundle-boolean-size': preparation['executable']['size'] = True
            elif case == 'bundle-oversize': preparation['node']['archiveBytes'] = 128 * 1024 * 1024 + 1
            elif case == 'bundle-config-hash': preparation['seaConfig']['sha256'] = 'a' * 64
            elif case == 'bundle-config-size': preparation['seaConfig']['size'] += 1
            elif case == 'bundle-sqlite-leading-zero': preparation['sqlite']['version'] = '03.50.2'
            elif case == 'bundle-sqlite-wide': preparation['sqlite']['version'] = '3000.50.2'
            elif case == 'bundle-sqlite-newline': preparation['sqlite']['sourceId'] += '\n'
            elif case == 'bundle-sqlite-calendar': preparation['sqlite']['sourceId'] = '2026-02-30 01:02:03 ' + 'a' * 64
            elif case == 'bundle-sqlite-digest': preparation['sqlite']['sourceId'] = '2026-02-28 01:02:03 ' + 'g' * 64
            elif case == 'bundle-outer-extra': bundle['callerApproved'] = True
            elif case != 'bundle-valid': raise AssertionError('Unknown bundle case')
            if case == 'bundle-valid': assert scope['_preparation_bundle'](bundle) == preparation
            else:
                try: scope['_preparation_bundle'](bundle)
                except Refused: pass
                else: raise AssertionError('Malformed preparation proof accepted')
        elif case == 'pipe-setup-failure':
            command = Process([sys.executable, '-I', '-B', '-c', 'import os; os.read(0,1)'], (), 4096)
            commands.append(command)
            original_register = command.selector.register
            def failed_register(*args): raise OSError(errno.EIO, 'owned selector setup failure')
            command.selector.register = failed_register
            try:
                try: command.start()
                except OSError as failure: assert failure.errno == errno.EIO
                else: raise AssertionError('Injected setup failure was ignored')
                assert command.child is not None
            finally: command.selector.register = original_register
        elif case == 'roundtrip':
            command = launch('import os; value=os.read(0,4); os.write(1,value)')
            command.send(b'ping', time.monotonic() + 5)
            assert command.read(4, time.monotonic() + 5) == b'ping'
            assert command.finish(time.monotonic() + 5) == 0 and command.reaped
            command.close()
            assert command.closed and not command.output and not command.error
        elif case == 'stdout-overflow':
            command = launch('import os; os.write(1,b"x"*65)', 64)
            try: command.read(1, time.monotonic() + 5)
            except Refused: pass
            else: raise AssertionError('Oversized output was accepted')
        elif case == 'stderr-overflow':
            command = launch('import os; os.write(2,b"x"*8193)')
            try: command.finish(time.monotonic() + 5)
            except Refused: pass
            else: raise AssertionError('Oversized stderr was accepted')
        elif case == 'deadline':
            command = launch('import os; os.read(0,1)')
            try: command.read(1, time.monotonic() - 1)
            except Refused: pass
            else: raise AssertionError('Expired operation was accepted')
        elif case == 'cancel':
            command = launch('import os; os.read(0,1)')
            try: raise KeyboardInterrupt('owned cancellation')
            except KeyboardInterrupt:
                command.stop()
                command.close()
            assert command.reaped and command.closed
        elif case == 'repeated-signal-denial':
            command = launch('import os; os.write(1,b"r"); os.read(0,1)')
            assert command.read(1, time.monotonic() + 5) == b'r'
            terminate, kill = command.child.terminate, command.child.kill
            def denied(): raise PermissionError(errno.EPERM, 'owned signal denial')
            command.child.terminate = command.child.kill = denied
            try:
                try: command.stop()
                except Refused: pass
                else: raise AssertionError('Unreaped child became settled')
                assert not command.reaped and command.child.poll() is None
                assert command.signals == [
                    {'operation': 'terminate', 'outcome': 'failed', 'errno': errno.EPERM},
                    {'operation': 'kill', 'outcome': 'failed', 'errno': errno.EPERM}]
            finally:
                command.child.terminate, command.child.kill = terminate, kill
        elif case == 'inherited-lock':
            path = root / 'activation.lock'
            original = os.open(path, os.O_RDWR | os.O_CREAT | os.O_EXCL, 0o600)
            contender = None
            try:
                fcntl.flock(original, fcntl.LOCK_EX | fcntl.LOCK_NB)
                command = launch('import os; os.write(1,b"r"); os.read(0,1)', descriptors=(original,))
                assert command.read(1, time.monotonic() + 5) == b'r'
                os.close(original)
                original = None
                contender = os.open(path, os.O_RDWR)
                try: fcntl.flock(contender, fcntl.LOCK_EX | fcntl.LOCK_NB)
                except BlockingIOError: pass
                else: raise AssertionError('Parent close released the inherited live lock')
                command.stop()
                assert command.reaped
                fcntl.flock(contender, fcntl.LOCK_EX | fcntl.LOCK_NB)
            finally:
                if original is not None: os.close(original)
                if contender is not None: os.close(contender)
        elif case == 'unbound-constructor':
            try: scope['_PreparationWorker']()
            except Refused: pass
            else: raise AssertionError('A caller created an admitted owner')
            assert not commands
        else: raise AssertionError('Unknown controller fixture')
    finally:
        for command in commands:
            if command.spawn_unknown:
                assert fixture_settled(command), 'Unknown implementation custody needs independent test-owned recovery'
                continue
            try: command.stop()
            finally: command.close()
        assert all(fixture_settled(command) for command in commands)
assert not root.exists()
print(json.dumps({'case': case, 'passed': True, 'cleanupObserved': True,
                  'qualification': 'owned-controller-mechanics', 'stateAdmissionAllowed': False}))
`;

describe("captured preparation controller", () => {
  it.each([
    "partial-spawn-error",
    "partial-spawn-interruption",
    "worker-observer-double-failure",
    "worker-run-cleanup-publication-failure",
    "bundle-valid",
    "bundle-missing-source",
    "bundle-unknown-source",
    "bundle-missing-worker",
    "bundle-worker-hash",
    "bundle-worker-rebound",
    "bundle-worker-extra",
    "bundle-zero-size",
    "bundle-boolean-size",
    "bundle-oversize",
    "bundle-config-hash",
    "bundle-config-size",
    "bundle-sqlite-leading-zero",
    "bundle-sqlite-wide",
    "bundle-sqlite-newline",
    "bundle-sqlite-calendar",
    "bundle-sqlite-digest",
    "bundle-outer-extra",
    "pipe-setup-failure",
    "roundtrip",
    "stdout-overflow",
    "stderr-overflow",
    "deadline",
    "cancel",
    "repeated-signal-denial",
    "inherited-lock",
    "unbound-constructor",
  ])("preserves captured custody for %s", (scenario) => {
    const result = NodeChildProcess.spawnSync("python3", ["-I", "-B", "-c", controllerFixture], {
      input: JSON.stringify({ source: jonesPreparationWorkerSource, case: scenario }),
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
      qualification: "owned-controller-mechanics",
      stateAdmissionAllowed: false,
    });
  });
});
