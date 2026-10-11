// @effect-diagnostics-next-line nodeBuiltinImport:off -- Run only the embedded controller against captured synthetic Python children.
import * as NodeChildProcess from "node:child_process";
import { describe, expect, it } from "vite-plus/test";
import { jonesPreparationWorkerSource } from "./jonesPreparationWorkerSource.ts";

const controllerFixture = String.raw`
import contextlib, copy, datetime, errno, fcntl, hashlib, json, os, pathlib, re, shutil, signal, stat, struct, subprocess, sys, tempfile, time
payload = json.load(sys.stdin)
class Refused(Exception): pass
def require(value):
    if not value: raise Refused('Owned controller refusal')
scope = {'__name__': 'owned_preparation_controller', 'Refused': Refused, 'require': require,
         'json': json, 'os': os, 'subprocess': subprocess, 'struct': struct, 'datetime': datetime, 're': re,
         'sha': lambda raw: hashlib.sha256(raw).hexdigest(),
         'hex_value': lambda value, length: isinstance(value, str) and re.fullmatch('[a-f0-9]{' + str(length) + '}', value) is not None,
         'integer': lambda value: type(value) is int and 1 <= value <= 9007199254740991,
         'BUNDLE_V2_KEYS': {'schema', 'repository', 'sourceSha', 'sourceTree', 'version', 'bootstrapSha256', 'executorSha256', 'maintenanceSha256', 'identityNativeSha256', 'identityNativeSourceSha256', 'nativeArchitecture'}}
exec(compile(payload['source'], 'preparation-controller.py', 'exec'), scope)
Process = scope['_PreparationProcess']
scope['_PREPARATION_LIMITS']['drainSeconds'] = 0.1
case = payload['case']
commands = []
def cancelled(signum, frame): raise KeyboardInterrupt('owned fixture signal')
signal.signal(signal.SIGTERM, cancelled)
@contextlib.contextmanager
def owned_root():
    root = pathlib.Path(tempfile.mkdtemp(prefix='jones-preparation-controller-')).resolve(strict=True)
    try: yield root
    finally:
        if all((command.child is None or command.reaped) and command.closed for command in commands): shutil.rmtree(root)
        else:
            evidence = {'schema': 1, 'processes': [{'pid': None if item.child is None else item.child.pid, 'reaped': item.reaped, 'signals': item.signals} for item in commands], 'pidSignalAuthority': False}
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
    try:
        if case.startswith('bundle-'):
            config = b'{"main":"offlinePreparationEntry.mjs","mainFormat":"module","executable":"node","output":"jones-offline-preparation","disableExperimentalSEAWarning":true,"useSnapshot":false,"useCodeCache":false,"execArgv":[],"execArgvExtension":"none"}\n'
            inputs = ['pnpm-lock.yaml', 'pnpm-workspace.yaml', 'package.json', 'apps/server/package.json', 'apps/desktop/package.json', 'apps/web/package.json', 'packages/contracts/package.json', 'apps/server/vite.config.ts', 'vite.config.ts', 'scripts/jones/updates/offline-preparation-build.ts', 'apps/server/src/jones/updates/offlinePreparationEntry.ts', 'apps/server/src/jones/updates/offlinePreparation.ts', 'apps/server/src/jones/updates/offlinePreparationCore.ts', 'apps/server/src/jones/updates/offlinePreparationPlan.ts']
            preparation = {'schema': 1, 'protocol': 1, 'provenanceSha256': 'a' * 64, 'sourceInputHashes': {key: 'b' * 64 for key in inputs},
                'entry': {'sha256': 'c' * 64, 'size': 1}, 'seaConfig': {'sha256': hashlib.sha256(config).hexdigest(), 'size': len(config)},
                'executable': {'name': 'jones-offline-preparation', 'sha256': 'd' * 64, 'size': 1},
                'node': {'version': '26.8.2', 'platform': 'darwin', 'architecture': 'arm64', 'archiveSha256': 'e' * 64, 'archiveBytes': 1, 'checksumsSha256': 'f' * 64, 'runtimeSha256': 'a' * 64, 'runtimeBytes': 1},
                'sqlite': {'version': '3.50.2', 'sourceId': '2026-02-28 01:02:03 ' + 'a' * 64}}
            bundle = {'schema': 3, 'repository': 'Jones-Systems/Jones-Code', 'sourceSha': 'a' * 40, 'sourceTree': 'b' * 40, 'version': '0.3.0-preview.20261011.1', 'nativeArchitecture': 'arm64', 'preparation': preparation}
            bundle.update({key: 'c' * 64 for key in ('bootstrapSha256', 'executorSha256', 'maintenanceSha256', 'identityNativeSha256', 'identityNativeSourceSha256')})
            if case == 'bundle-missing-source': del preparation['sourceInputHashes'][inputs[0]]
            elif case == 'bundle-unknown-source': preparation['sourceInputHashes']['unreviewed.ts'] = 'a' * 64
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
            try: command.stop()
            finally: command.close()
        assert all((command.child is None or command.reaped) and command.closed for command in commands)
assert not root.exists()
print(json.dumps({'case': case, 'passed': True, 'cleanupObserved': True,
                  'qualification': 'owned-controller-mechanics', 'stateAdmissionAllowed': False}))
`;

describe("captured preparation controller", () => {
  it.each([
    "bundle-valid",
    "bundle-missing-source",
    "bundle-unknown-source",
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
