import * as NodeAssert from "node:assert/strict";
import * as NodeChildProcess from "node:child_process";
import * as NodeCrypto from "node:crypto";
import * as NodeFSP from "node:fs/promises";
import * as NodeOS from "node:os";
import * as NodePath from "node:path";
import * as NodeURL from "node:url";
import * as NodeUtil from "node:util";

// oxlint-disable-next-line t3code/no-global-process-runtime -- Standalone native qualification observes the actual host without loading the application runtime.
const nativePlatform = process.platform;

const execFile = NodeUtil.promisify(NodeChildProcess.execFile);
const sha256 = (bytes) => NodeCrypto.createHash("sha256").update(bytes).digest("hex");
export const legacyMaintenanceSource = Object.freeze({
  repository: "Jones-Systems/Jones-Code",
  commit: "bd20d2d2fc0ba89797698192b877a7f1312f8fe3",
  path: "apps/desktop/src/jones/updates/jonesNativeHelperSource.ts",
  blob: "ea34d9320ee371b7bcdff23172c4b9d9dc167c51",
  sourceSha256: "80d28be959cc0f3ea862b6327fae4c0f9b1577bea141469fed6b4de0ecf199b7",
  helperSha256: "399fc969ab11523ef66f87915b59a68a2eeddf4dbcca9833984a79933764d9cc",
});

export function extractPinnedLegacyHelper(source) {
  NodeAssert.equal(sha256(source), legacyMaintenanceSource.sourceSha256);
  const marker = "export const jonesNativeHelperSource = String.raw`";
  NodeAssert.equal(source.split(marker).length, 2);
  NodeAssert.ok(source.endsWith("`;\n"));
  const helper = source.slice(source.indexOf(marker) + marker.length, -3);
  NodeAssert.equal(sha256(helper), legacyMaintenanceSource.helperSha256);
  return helper;
}

export async function readPinnedLegacyHelper(repositoryRoot) {
  const reference = `${legacyMaintenanceSource.commit}:${legacyMaintenanceSource.path}`;
  const options = { cwd: repositoryRoot, encoding: "utf8", maxBuffer: 128 * 1024, timeout: 10000 };
  const { stdout: blob } = await execFile("git", ["rev-parse", reference], options);
  NodeAssert.equal(blob.trim(), legacyMaintenanceSource.blob);
  const { stdout: source } = await execFile("git", ["cat-file", "blob", reference], options);
  return extractPinnedLegacyHelper(source);
}

export class MaintenanceCustodyError extends AggregateError {
  constructor(primary, controllerCustody) {
    super(
      [primary, new Error("Owned child reaping was not proved.")],
      "Fixture failed; scratch is retained because child custody is unresolved.",
    );
    this.preservedScratchRoots = [];
    this.controllerCustody = controllerCustody;
  }
}

export async function withMaintenanceScratch(parent, body) {
  const allocated = await NodeFSP.mkdtemp(NodePath.join(parent, "jones-maintenance-"));
  let failure;
  try {
    return await body(await NodeFSP.realpath(allocated));
  } catch (cause) {
    failure = cause;
    if (cause instanceof MaintenanceCustodyError) {
      cause.preservedScratchRoots.push(allocated);
      if (cause.controllerCustody !== undefined) {
        try {
          const evidence = await NodeFSP.open(
            NodePath.join(allocated, "maintenance-custody.json"),
            "wx",
            0o600,
          );
          try {
            await evidence.writeFile(`${JSON.stringify(cause.controllerCustody)}\n`);
            await evidence.sync();
          } finally {
            await evidence.close();
          }
        } catch (recordingFailure) {
          cause.errors.push(recordingFailure);
        }
      }
    }
    throw cause;
  } finally {
    if (!(failure instanceof MaintenanceCustodyError)) {
      try {
        await NodeFSP.rm(allocated, { recursive: true, force: false });
        await NodeAssert.rejects(NodeFSP.lstat(allocated), { code: "ENOENT" });
      } catch (cleanup) {
        if (failure !== undefined)
          throw new AggregateError([failure, cleanup], "Fixture failed and cleanup failed.");
        throw cleanup;
      }
    }
  }
}

// The helper is compiled byte-for-byte. Tracing only supplies a pipe barrier
// before its native flock and stops after its first durable journal mutation.
// No helper function, platform value, syscall, or branch is substituted.
const pythonFixture = String.raw`
import fcntl, hashlib, json, os, pathlib, plistlib, signal, sqlite3, struct, subprocess, sys

class FixtureStop(BaseException): pass
class Cancelled(BaseException): pass

def encoded(value): return (json.dumps(value, sort_keys=True) + '\n').encode()
def write(path, value):
    with open(path, 'xb') as stream:
        stream.write(encoded(value)); stream.flush(); os.fsync(stream.fileno())
def event(value): print(json.dumps(value, sort_keys=True), flush=True)

def worker(config):
    root = pathlib.Path(config['root']).resolve()
    helper = str(root / 'legacy-exact.py')
    raw = pathlib.Path(helper).read_bytes()
    if hashlib.sha256(raw).hexdigest() != config['helperSha256']: raise RuntimeError('Helper bytes changed.')
    source = raw.decode()
    line = next(index for index, text in enumerate(source.splitlines(), 1)
                if text == '    fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)')
    # Both guards have a positive control before the exact helper executes.
    controls = {'execution': False, 'outsideWrite': False}
    violations = []
    testing = True
    def audit(name, args):
        forbidden = name in ('subprocess.Popen', 'os.system', 'os.exec', 'os.posix_spawn', 'os.fork', 'os.kill', 'os.killpg')
        if name == 'open' and isinstance(args[0], (str, bytes)):
            mode, flags = args[1], args[2]
            writes = (isinstance(mode, str) and any(letter in mode for letter in 'wax+')) or (isinstance(flags, int) and flags & (os.O_WRONLY | os.O_RDWR | os.O_CREAT | os.O_TRUNC))
            if writes and not pathlib.Path(os.fsdecode(args[0])).resolve().is_relative_to(root): forbidden = True
        if forbidden:
            if not testing: violations.append(name)
            raise RuntimeError('Fixture audit refused ' + name)
    sys.addaudithook(audit)
    try: subprocess.Popen(['/fixture-must-never-execute'])
    except RuntimeError: controls['execution'] = True
    try: open(root.parent / 'fixture-must-never-write', 'wb')
    except RuntimeError: controls['outsideWrite'] = True
    testing = False
    def trace(frame, kind, argument):
        if frame.f_code.co_filename != helper: return trace
        if frame.f_code.co_name == '<module>' and kind == 'line' and frame.f_lineno == line:
            event({'event': 'pre-flock'})
            if sys.stdin.buffer.read(1) != b'G': raise FixtureStop()
        if frame.f_code.co_name == 'durable' and kind == 'return':
            path = pathlib.Path(frame.f_locals['path'])
            if path.name != 'journal.json': raise RuntimeError('Unexpected first durable mutation.')
            event({'event': 'journal-durable', 'phase': json.loads(path.read_text())['phase']})
            raise FixtureStop()
        return trace
    sys.argv = [helper, '--manifest', config['manifest']] + config['arguments']
    outcome = {'code': 0, 'message': ''}
    sys.settrace(trace)
    try:
        exec(compile(raw, helper, 'exec'), {'__name__': '__main__', '__file__': helper})
    except FixtureStop:
        outcome['stoppedAfterDurable'] = True
    except SystemExit as failure:
        outcome['code'] = failure.code if type(failure.code) is int else 1
        outcome['message'] = str(failure.code)[:1000]
    except Exception as failure:
        outcome['code'] = 1
        outcome['message'] = str(failure)[:1000]
        outcome['exception'] = type(failure).__name__
    finally:
        sys.settrace(None)
    event(dict(outcome, event='result', controls=controls, violations=violations))

def run(config):
    if sys.platform != 'darwin': raise RuntimeError('Native legacy maintenance qualification requires macOS.')
    root = pathlib.Path(config['root']).resolve()
    if any(part in ('AppTranslocation', 'Volumes', 'tmp', 'jones-updates') for part in root.parts):
        raise RuntimeError('Scratch parent cannot be rejected by the exact legacy stable-app contract.')
    children = []
    def spawn(arguments):
        child = subprocess.Popen([sys.executable] + arguments, stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        children.append(child)
        return child
    def finish(child, data=None):
        out, err = child.communicate(data, timeout=10)
        if len(out) + len(err) > 65536: raise RuntimeError('Fixture child output exceeded its bound.')
        if err: raise RuntimeError('Fixture child failed: ' + err.decode()[:1500])
        if child.returncode != 0: raise RuntimeError('Fixture child exited ' + str(child.returncode))
        return [json.loads(item) for item in out.splitlines()]
    def holder(lock):
        child = spawn(['-I', '-B', '-c', 'import fcntl,sys; f=open(sys.argv[1],"r+"); fcntl.flock(f,fcntl.LOCK_EX|fcntl.LOCK_NB); print("locked",flush=True); sys.stdin.buffer.read(1)', str(lock)])
        if child.stdout.readline() != b'locked\n': raise RuntimeError('Controller did not acquire the native lock.')
        return child
    def kill_controller(child):
        child.kill(); child.wait(timeout=10)
        if child.returncode != -signal.SIGKILL: raise RuntimeError('Controller death was not observed.')
    def begin(manifest, arguments):
        child = spawn(['-I', '-B', __file__, '--worker', json.dumps(dict(config, manifest=str(manifest), arguments=arguments))])
        if json.loads(child.stdout.readline()) != {'event': 'pre-flock'}: raise RuntimeError('Exact helper did not reach the pre-flock barrier.')
        return child
    def observe(child):
        events = finish(child, b'G')
        result = events[-1]
        if result['event'] != 'result' or result['controls'] != {'execution': True, 'outsideWrite': True} or result['violations']:
            raise RuntimeError('Helper containment controls failed.')
        return result, events[:-1]
    try:
        app = root / 'Fixture.app'
        (app / 'Contents/MacOS').mkdir(parents=True)
        (app / 'Contents/Resources').mkdir()
        executable = app / 'Contents/MacOS/fixture'
        executable.write_bytes(b'fixture executable must never be launched\n'); executable.chmod(0o700)
        source = {'repository': 'Jones-Systems/Jones-Code', 'sha': config['commit'], 'tree': 'b' * 40}
        metadata = encoded({'version': '1.0.0', 'jonesSource': source})
        header = json.dumps({'files': {'package.json': {'offset': '0', 'size': len(metadata)}}}).encode()
        (app / 'Contents/Resources/app.asar').write_bytes(struct.pack('<IIII', 4, len(header) + 8, 0, len(header)) + header + metadata)
        with open(app / 'Contents/Info.plist', 'wb') as stream:
            plistlib.dump({'CFBundleExecutable': 'fixture', 'CFBundleShortVersionString': '1.0.0', 'CFBundleIdentifier': 'fixture.jones'}, stream)
        home = root / 'home'
        runtime = home / 'runtime'; runtime.mkdir(parents=True)
        userdata = home / 'userdata'; userdata.mkdir()
        database = userdata / 'statev2.sqlite'
        connection = sqlite3.connect(database)
        try:
            connection.execute('CREATE TABLE fixture_sentinel(value TEXT)')
            connection.execute("INSERT INTO fixture_sentinel VALUES('preserve-current-state')"); connection.commit()
        finally: connection.close()
        profile = root / 'profile'; profile.mkdir()
        (profile / 'sentinel').write_bytes(b'preserve-profile\n')
        manifest = runtime / 'jones-active-install.json'
        lock = runtime / 'jones-activation.lock'; lock.write_bytes(b'jones-activation-lock-v1\n'); lock.chmod(0o600)
        lock_identity = (lock.stat().st_dev, lock.stat().st_ino)
        active = {'protocol': 1, 'owner': 'desktop', 'generation': 'a' * 64, 'transactionId': 'bootstrap',
                  'home': str(home), 'databasePath': str(database), 'profile': str(profile), 'environmentId': 'fixture',
                  'appPath': str(app), 'executablePath': str(executable), 'version': '1.0.0',
                  'sourceSha': config['commit'], 'sourceTree': 'b' * 40, 'bundleIdentifier': 'fixture.jones'}
        write(manifest, active)
        sentinels = [database, profile / 'sentinel', executable, app / 'Contents/Info.plist', app / 'Contents/Resources/app.asar']
        before = [(str(path), path.read_bytes(), path.stat().st_mode) for path in sentinels]
        cases = []
        def reset_manifest(value):
            adjacent = manifest.with_name('fixture-manifest.next')
            write(adjacent, value); os.replace(adjacent, manifest)
            descriptor = os.open(runtime, os.O_RDONLY)
            try: os.fsync(descriptor)
            finally: os.close(descriptor)
        def transaction(number, phase=None, mismatch=False, missing=False):
            handle = format(number, '064x')
            tx = runtime / 'jones-updates/transactions' / handle; tx.mkdir(parents=True)
            receipt = {'protocol': 1, 'transactionId': handle, 'home': str(home), 'databasePath': str(database), 'profile': str(profile), 'environmentId': 'fixture', 'prepared': True}
            write(tx / 'prepare-intent.json', receipt)
            if not missing: write(tx / 'continuation.json', dict(receipt, environmentId='different') if mismatch else receipt)
            staged = {'handle': handle, 'receiptPath': str(tx / 'app-receipt.json'), 'startupGateProtocol': 1, 'bundleIdentifier': 'fixture.jones', 'sourceSha': 'c' * 40, 'sourceTree': 'd' * 40}
            candidate = {'repository': 'Jones-Systems/Jones-Code', 'source': staged['sourceSha'], 'tree': staged['sourceTree'], 'installedSource': active['sourceSha']}
            write(tx / 'app-receipt.json', {'app': staged, 'candidate': candidate, 'artifact': {'candidate': candidate}})
            intent = {'protocol': 1, 'transactionId': handle, 'expected': active, 'staged': staged, 'continuationReceipt': str(tx / 'continuation.json'), 'processes': [], 'listener': 'http://127.0.0.1:1/'}
            write(tx / 'intent.json', intent)
            if phase is not None: write(tx / 'journal.json', {'intent': intent, 'phase': phase})
            return tx
        tx = transaction(1)
        control = holder(lock)
        child = begin(manifest, ['--activate', str(tx / 'intent.json')])
        event({'event': 'children-ready', 'childCount': 2})
        if sys.stdin.buffer.read(1) != b'G': raise Cancelled('Controller pipe closed.')
        result, events = observe(child)
        assert result['code'] == 1 and result['exception'] == 'BlockingIOError' and not events
        assert not (tx / 'journal.json').exists()
        cases.append({'name': 'fixed-lock-exclusion', 'outcome': 'refused'})
        kill_controller(control)
        for action in ('activate', 'launch', 'cli'):
            control = holder(lock)
            reset_manifest(dict(active, owner='desktop-maintenance'))
            arguments = ['--activate', str(tx / 'intent.json')] if action == 'activate' else ['--launch'] if action == 'launch' else ['--cli', '--', '--help']
            child = begin(manifest, arguments)
            kill_controller(control)
            result, events = observe(child)
            assert result['code'] == 1 and result['message'] == 'Native launcher bootstrap is required.' and not events
            assert not (tx / 'journal.json').exists()
            cases.append({'name': 'maintenance-after-controller-death-' + action, 'outcome': 'refused'})
        # Restoring the identical manifest re-enables this delayed old intent.
        # Observe its real first journal publication, then stop before any stop/copy/launch.
        child = begin(manifest, ['--activate', str(tx / 'intent.json')])
        reset_manifest(active)
        result, events = observe(child)
        assert result.get('stoppedAfterDurable') is True and events == [{'event': 'journal-durable', 'phase': 'intent'}]
        assert json.loads((tx / 'journal.json').read_text())['phase'] == 'intent'
        cases.append({'name': 'same-original-rollback-negative-control', 'outcome': 'journal-intent-written'})
        tx = transaction(2)
        child = begin(manifest, ['--activate', str(tx / 'intent.json')])
        reset_manifest(dict(active, generation='e' * 64))
        result, events = observe(child)
        assert result['code'] == 1 and result['message'] == 'Active binding changed; install refused.' and not events
        assert not (tx / 'journal.json').exists()
        cases.append({'name': 'rotated-epoch-old-intent', 'outcome': 'refused'})
        reset_manifest(active)
        for number, phase in enumerate(('resumed', 'rolled-back', 'blocked', 'preparing', 'trial'), 3):
            tx = transaction(number, phase)
            original = (tx / 'journal.json').read_bytes()
            result, events = observe(begin(manifest, ['--activate', str(tx / 'intent.json')]))
            if phase in ('resumed', 'rolled-back', 'blocked'):
                assert result['code'] == 0 and not events and (tx / 'journal.json').read_bytes() == original
            else:
                assert result.get('stoppedAfterDurable') is True and events == [{'event': 'journal-durable', 'phase': 'blocked'}]
                assert json.loads((tx / 'journal.json').read_text())['phase'] == 'blocked'
            cases.append({'name': 'journal-' + phase, 'outcome': 'unchanged' if not events else 'blocked-written'})
        for number, name, mismatch, missing in ((8, 'legacy-receipt-mismatch', True, False), (9, 'legacy-incomplete-preparation', False, True)):
            tx = transaction(number, mismatch=mismatch, missing=missing)
            result, events = observe(begin(manifest, ['--activate', str(tx / 'intent.json')]))
            assert result['code'] == 1 and not events and not (tx / 'journal.json').exists()
            if mismatch: assert result['message'] == 'Continuation receipt has a stale native binding.'
            else: assert result['exception'] == 'FileNotFoundError'
            cases.append({'name': name, 'outcome': 'refused'})
        assert before == [(str(path), path.read_bytes(), path.stat().st_mode) for path in sentinels]
        assert lock_identity == (lock.stat().st_dev, lock.stat().st_ino)
        assert manifest.read_bytes() == encoded(active)
        return {'schema': 1, 'operation': 'qualify-legacy-maintenance', 'status': 'qualified-fixture', 'cases': cases,
                'interpreter': {'path': sys.executable, 'version': '.'.join(map(str, sys.version_info[:3])), 'scope': 'fixture-controller-and-exact-helper-children'},
                'helperSha256': config['helperSha256'], 'stateUnchanged': True, 'lockInodeUnchanged': True,
                'externalExecution': False, 'productionTransitionImplemented': False, 'electronAdmissionProved': False,
                'receiptScope': 'legacy-helper-validation-only; callback-not-executed', 'childrenReaped': True}
    finally:
        failures = []
        for child in children:
            try:
                if child.poll() is None: child.kill()
                child.wait(timeout=10)
                for stream in (child.stdin, child.stdout, child.stderr):
                    if stream is not None: stream.close()
            except Exception as failure: failures.append(str(failure))
        if failures: raise RuntimeError('Owned child cleanup failed: ' + '; '.join(failures))
        event({'event': 'children-reaped', 'count': len(children)})

def cancel(signum, frame): raise Cancelled('Fixture cancelled')
signal.signal(signal.SIGTERM, cancel)
signal.signal(signal.SIGINT, cancel)
if sys.argv[1] == '--worker': worker(json.loads(sys.argv[2]))
else: event(run(json.loads(sys.argv[1])))
`;

export async function runMaintenanceProcess(
  python,
  arguments_,
  root,
  signal,
  onChildrenReady,
  onControllerExit,
) {
  signal?.throwIfAborted();
  const child = NodeChildProcess.spawn(python, arguments_, {
    cwd: root,
    detached: true,
    env: { PATH: "/usr/bin:/bin", HOME: root, TMPDIR: root, PYTHONDONTWRITEBYTECODE: "1" },
    stdio: ["pipe", "pipe", "pipe"],
  });
  let stdout = "";
  let stderr = "";
  let failure;
  let escalation;
  let drainTimeout;
  let ready = false;
  let closed = false;
  let exited = false;
  let stopping = false;
  let custodyUnresolved = false;
  let driverHandleDetached = false;
  const deniedSignals = [];
  let finish;
  const completion = new Promise((resolve) => {
    finish = resolve;
  });
  const controllerExited = () => exited || child.exitCode !== null || child.signalCode !== null;
  const boundDrain = () => {
    drainTimeout ??= setTimeout(() => {
      custodyUnresolved = true;
      failure ??= new Error("Owned controller streams did not drain after shutdown.");
      child.stdin.destroy();
      child.stdout.destroy();
      child.stderr.destroy();
      // Retain custody evidence, but let a failed standalone driver exit even
      // when permission denial leaves its captured controller alive.
      child.unref();
      driverHandleDetached = true;
      finish({ code: child.exitCode, stoppedBy: child.signalCode });
    }, 3000);
  };
  const signalGroup = (kind) => {
    // Once the captured controller exits, its numeric group may be stale.
    // Missing descendant proof then retains scratch instead of signaling it.
    if (!child.pid || closed || controllerExited()) return;
    try {
      process.kill(-child.pid, kind);
    } catch (error) {
      if (error.code !== "ESRCH") {
        custodyUnresolved = true;
        deniedSignals.push({ signal: kind, code: error.code ?? "unknown" });
        failure = new AggregateError([failure, error]);
      }
    }
  };
  const terminate = (cause) => {
    failure ??= cause;
    if (!child.pid || closed || stopping) return;
    stopping = true;
    if (controllerExited()) {
      boundDrain();
      return;
    }
    signalGroup("SIGTERM");
    escalation ??= setTimeout(() => {
      signalGroup("SIGKILL");
      boundDrain();
    }, 3000);
  };
  child.once("exit", (code, stoppedBy) => {
    exited = true;
    boundDrain();
    try {
      onControllerExit?.({ code, stoppedBy });
    } catch (cause) {
      terminate(cause);
    }
  });
  child.once("close", (code, stoppedBy) => {
    closed = true;
    finish({ code, stoppedBy });
  });
  child.stdin.on("error", terminate);
  child.on("error", terminate);
  const abort = () => terminate(signal.reason ?? new Error("Fixture cancelled."));
  signal?.addEventListener("abort", abort, { once: true });
  const timeout = setTimeout(
    () => terminate(new Error("Legacy maintenance fixture exceeded 60 seconds.")),
    60000,
  );
  const collect = (kind, bytes) => {
    if (kind === "stdout") stdout += bytes.toString();
    else stderr += bytes.toString();
    if (Buffer.byteLength(stdout) + Buffer.byteLength(stderr) > 128 * 1024)
      terminate(new Error("Fixture output exceeded its bound."));
    if (!ready && stdout.includes("\n")) {
      ready = true;
      try {
        const event = JSON.parse(stdout.slice(0, stdout.indexOf("\n")));
        NodeAssert.deepEqual(event, { event: "children-ready", childCount: 2 });
        onChildrenReady?.(event, child.stdin);
        if (failure === undefined)
          child.stdin.end("G", (cause) => {
            if (cause) terminate(cause);
          });
      } catch (cause) {
        terminate(cause);
      }
    }
  };
  child.stdout.on("data", (bytes) => collect("stdout", bytes));
  child.stderr.on("data", (bytes) => collect("stderr", bytes));
  try {
    const outcome = await completion;
    const lines = stdout.trim().split("\n");
    const reaped = lines.some((line) => {
      try {
        const event = JSON.parse(line);
        return (
          event.event === "children-reaped" && Number.isInteger(event.count) && event.count >= 2
        );
      } catch {
        return false;
      }
    });
    try {
      NodeAssert.deepEqual(outcome, { code: 0, stoppedBy: null }, stderr.slice(0, 4000));
      NodeAssert.equal(stderr, "");
      NodeAssert.equal(ready, true);
      NodeAssert.equal(reaped, true);
    } catch (cause) {
      failure ??= cause;
    }
    // A closed controller is not evidence that its descendants were reaped.
    // Preserve the owned tree on every missing-proof path, even before readiness.
    if (child.pid && (!reaped || custodyUnresolved)) {
      throw new MaintenanceCustodyError(failure, {
        schema: 1,
        controllerPid: child.pid,
        processGroupId: child.pid,
        driverPid: process.pid,
        executable: python,
        argumentsSha256: sha256(JSON.stringify(arguments_)),
        scratchRoot: root,
        controllerExitObserved: controllerExited(),
        descendantReapingProved: reaped,
        driverHandleDetached,
        deniedSignals,
        recoveryAuthority: "evidence-only; numeric identity does not authorize signaling",
      });
    }
    if (failure !== undefined) throw failure;
    NodeAssert.equal(lines.length, 3);
    return JSON.parse(lines[2]);
  } finally {
    clearTimeout(timeout);
    clearTimeout(escalation);
    clearTimeout(drainTimeout);
    signal?.removeEventListener("abort", abort);
  }
}

export async function runLegacyMaintenanceFixture({
  repositoryRoot = NodeURL.fileURLToPath(new URL("../../../", import.meta.url)),
  scratchParent = process.env.RUNNER_TEMP ?? NodeOS.tmpdir(),
  python = "/usr/bin/python3",
  signal,
  onChildrenReady,
} = {}) {
  NodeAssert.equal(
    nativePlatform,
    "darwin",
    "Native qualification requires macOS; no platform substitution is permitted.",
  );
  const helper = await readPinnedLegacyHelper(repositoryRoot);
  const result = await withMaintenanceScratch(scratchParent, async (root) => {
    await NodeFSP.writeFile(NodePath.join(root, "legacy-exact.py"), helper, {
      flag: "wx",
      mode: 0o400,
    });
    const runner = NodePath.join(root, "fixture.py");
    await NodeFSP.writeFile(runner, pythonFixture, { flag: "wx", mode: 0o400 });
    return runMaintenanceProcess(
      python,
      ["-I", "-B", runner, JSON.stringify({ root, ...legacyMaintenanceSource })],
      root,
      signal,
      onChildrenReady,
    );
  });
  return { ...result, source: legacyMaintenanceSource, scratchRemoved: true };
}

if (
  process.argv[1] &&
  import.meta.url === NodeURL.pathToFileURL(NodePath.resolve(process.argv[1])).href
) {
  const controller = new AbortController();
  const cancel = () => controller.abort(new Error("Legacy maintenance fixture cancelled."));
  process.once("SIGINT", cancel);
  process.once("SIGTERM", cancel);
  try {
    NodeAssert.equal(
      process.argv.length,
      2,
      "This fixture accepts no installed-state or command arguments.",
    );
    console.log(JSON.stringify(await runLegacyMaintenanceFixture({ signal: controller.signal })));
  } catch (cause) {
    console.error(
      JSON.stringify({
        schema: 1,
        operation: "qualify-legacy-maintenance",
        status: "failed",
        message: String(cause).slice(0, 4000),
        ...(cause instanceof MaintenanceCustodyError
          ? {
              preservedScratchRoots: cause.preservedScratchRoots,
              controllerCustody: cause.controllerCustody,
            }
          : {}),
      }),
    );
    process.exitCode = 1;
  } finally {
    process.removeListener("SIGINT", cancel);
    process.removeListener("SIGTERM", cancel);
  }
}
