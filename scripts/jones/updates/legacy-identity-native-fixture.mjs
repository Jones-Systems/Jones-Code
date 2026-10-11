import * as NodeAssert from "node:assert/strict";
import * as NodeChildProcess from "node:child_process";
import * as NodeCrypto from "node:crypto";
import * as NodeFS from "node:fs";
import * as NodeFSP from "node:fs/promises";
import * as NodeOS from "node:os";
import * as NodePath from "node:path";
import * as NodeURL from "node:url";

// oxlint-disable-next-line t3code/no-global-process-runtime -- This standalone ABI qualifier needs the actual native host without loading the application runtime.
const nativePlatform = process.platform;
// Darwin's public sys/fcntl.h defines O_EVTONLY; Node does not expose it.
const darwinEventOnly = 0x00008000;
const source = NodeURL.fileURLToPath(
  new URL("../../../apps/desktop/src/jones/updates/jonesLegacyIdentityNative.c", import.meta.url),
);
const uuid = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i;
const sha256 = (bytes) => NodeCrypto.createHash("sha256").update(bytes).digest("hex");
const commandPhaseNames = new Set([
  "fixture",
  "identity-compile",
  "identity-boot",
  "identity-boot-reference",
  "identity-inspect",
  "image-create",
  "image-attach",
  "image-detach",
  "observer-compile",
  "observer-wrapper-compile",
  "observer-driver",
]);
// Keep the original error identity, including frozen cancellation reasons.
// Diagnostics retain only these closed phase names, never argv, env or streams.
const commandFailurePhases = new WeakMap();
function phaseFailure(cause, phase) {
  if (cause !== null && (typeof cause === "object" || typeof cause === "function"))
    commandFailurePhases.set(cause, phase);
  return cause;
}

function custodyEvidence(cause) {
  const pending = [cause];
  const seen = new Set();
  const receipts = new Map();
  const phases = new Set();
  let preserveScratch = false;
  while (pending.length && seen.size < 64) {
    const current = pending.pop();
    if (!current || typeof current !== "object" || seen.has(current)) continue;
    seen.add(current);
    const phase = commandFailurePhases.get(current);
    if (phase) phases.add(phase);
    preserveScratch ||= current.preserveScratch === true;
    if (current.commandCustody)
      receipts.set(current.commandCustody.commandId, current.commandCustody);
    pending.push(current.cause);
    if (current instanceof AggregateError) pending.push(...current.errors.slice(0, 32));
  }
  return { preserveScratch, receipts: [...receipts.values()], phases: [...phases] };
}

export function legacyIdentityFailureReport(cause) {
  const evidence = custodyEvidence(cause);
  return {
    status: "failed",
    message: String(cause.message).slice(0, 512),
    retainedScratch: cause.scratchRoot,
    commandCustody: evidence.receipts,
    commandPhases: evidence.phases,
    activationAllowed: false,
  };
}

export async function withLegacyIdentityScratch(
  body,
  { temporaryDirectory = NodeOS.tmpdir() } = {},
) {
  const parent = await NodeFSP.realpath(temporaryDirectory);
  const root = await NodeFSP.mkdtemp(NodePath.join(parent, "jones-identity-native-"));
  // Emit the literal ownership locator before body can launch any subprocess.
  process.stderr.write(`${JSON.stringify({ event: "identity-fixture-root", root })}\n`);
  const owner = await NodeFSP.lstat(root, { bigint: true });
  let failure;
  let result;
  try {
    result = await body(root);
  } catch (cause) {
    failure = cause;
  }
  if (custodyEvidence(failure).preserveScratch) {
    failure.preserveScratch = true;
    failure.scratchRoot = root;
    throw failure;
  }
  try {
    const current = await NodeFSP.lstat(root, { bigint: true });
    NodeAssert.ok(current.isDirectory() && current.dev === owner.dev && current.ino === owner.ino);
    await NodeFSP.rm(root, { recursive: true, force: false });
    await NodeAssert.rejects(NodeFSP.lstat(root), { code: "ENOENT" });
  } catch (cleanup) {
    const retained = failure
      ? new AggregateError([failure, cleanup], failure.message, { cause: failure })
      : cleanup;
    retained.scratchRoot = root;
    throw retained;
  }
  if (failure) throw failure;
  return result;
}

export async function runLegacyIdentityFixtureCommand(
  executable,
  args,
  root,
  { signal, fds = [], spawn = NodeChildProcess.spawn, phase: requestedPhase = "fixture" } = {},
) {
  signal?.throwIfAborted();
  const phase = commandPhaseNames.has(requestedPhase) ? requestedPhase : "fixture";
  const commandId = NodeCrypto.randomUUID();
  const createdAt = new Date().toISOString();
  let child;
  try {
    child = spawn(executable, args, {
      cwd: root,
      env: {
        PATH: "/usr/bin:/bin",
        HOME: root,
        TMPDIR: root,
        CLANG_MODULE_CACHE_PATH: NodePath.join(root, "clang-cache"),
      },
      stdio: ["ignore", "pipe", "pipe", ...fds],
    });
  } catch (cause) {
    throw phaseFailure(cause, phase);
  }
  let stdout = "";
  let stderr = "";
  let failure;
  let escalation;
  let custodyDeadline;
  let closed = false;
  let exited = false;
  let spawned = false;
  let stopping = false;
  let unresolved = false;
  const signalAttempts = [];
  const commandErrors = [];
  let activeAttempt;
  let finish;
  const completion = new Promise((resolve) => {
    finish = resolve;
  });
  const unknownCustody = () => {
    unresolved = true;
    finish({ code: null, stoppedBy: null });
  };
  const recordError = (cause) => {
    failure ??= cause;
    commandErrors.push(cause);
    if (activeAttempt) activeAttempt.errors.push(cause);
  };
  const sendSignal = (kind) => {
    if (closed || child.exitCode !== null || child.signalCode !== null) return;
    const attempt = { signal: kind, returned: null, errors: [] };
    signalAttempts.push(attempt);
    activeAttempt = attempt;
    try {
      attempt.returned = child.kill(kind);
    } catch (cause) {
      recordError(cause);
    } finally {
      activeAttempt = undefined;
    }
  };
  const stop = (cause) => {
    failure ??= cause;
    if (stopping) return;
    stopping = true;
    custodyDeadline ??= setTimeout(unknownCustody, 6000);
    sendSignal("SIGTERM");
    escalation = setTimeout(() => sendSignal("SIGKILL"), 3000);
  };
  const abort = () => stop(signal.reason ?? new Error("Native identity fixture cancelled."));
  signal?.addEventListener("abort", abort, { once: true });
  const timeout = setTimeout(
    () => stop(new Error(`Native identity command (${phase}) exceeded 30 seconds.`)),
    30000,
  );
  child.stdout.on("data", (bytes) => {
    if (Buffer.byteLength(stdout) + bytes.length > 128 * 1024)
      stop(new Error("Native identity output exceeded its bound."));
    else stdout += bytes;
  });
  child.stderr.on("data", (bytes) => {
    if (Buffer.byteLength(stderr) + bytes.length > 16 * 1024)
      stop(new Error("Native identity diagnostics exceeded their bound."));
    else stderr += bytes;
  });
  // kill() may emit synchronously more than once. Keep this listener for the
  // captured handle's lifetime, including after an unresolved command returns.
  child.on("error", recordError);
  child.once("spawn", () => {
    spawned = true;
  });
  child.once("exit", () => {
    exited = true;
  });
  child.once("close", (code, stoppedBy) => {
    closed = true;
    finish({ code, stoppedBy });
  });
  if (signal?.aborted) abort();
  try {
    const result = await completion;
    if (unresolved) {
      const errorSummary = (cause) => ({
        name: String(cause.name ?? "Error").slice(0, 48),
        code: String(cause.code ?? "unknown").slice(0, 48),
      });
      const receipt = {
        schema: 1,
        commandId,
        phase,
        createdAt,
        observedAt: new Date().toISOString(),
        driverPid: process.pid,
        controllerPid: child.pid ?? null,
        identityScope: "captured-child-handle-only",
        pidMayBeReused: true,
        permitsPidSignalling: false,
        permitsProcessAdoption: false,
        spawnObserved: spawned,
        exitObserved: exited,
        closeObserved: closed,
        exitCode: child.exitCode,
        signalCode: child.signalCode,
        driverWillReleaseHandle: true,
        signalAttempts: signalAttempts.map((attempt) => ({
          signal: attempt.signal,
          returned: attempt.returned,
          errors: attempt.errors.slice(0, 32).map(errorSummary),
          errorCount: attempt.errors.length,
        })),
        errors: commandErrors.slice(0, 32).map(errorSummary),
        errorCount: commandErrors.length,
        evidencePath: NodePath.join(root, `identity-command-${commandId}.json`),
      };
      const failures = failure
        ? [failure, ...commandErrors.filter((item) => item !== failure)]
        : commandErrors;
      const custody = new AggregateError(
        failures,
        "Native command closure is unproved; exact scratch retained.",
        { cause: failure },
      );
      custody.preserveScratch = true;
      custody.commandCustody = { ...receipt, persisted: false };
      try {
        const evidence = await NodeFSP.open(receipt.evidencePath, "wx", 0o600);
        try {
          const encoded = `${JSON.stringify(receipt)}\n`;
          NodeAssert.ok(
            Buffer.byteLength(encoded) <= 16 * 1024,
            "Command custody receipt exceeded its bound.",
          );
          await evidence.writeFile(encoded);
          await evidence.sync();
        } finally {
          await evidence.close();
        }
        const directory = await NodeFSP.open(
          root,
          NodeFS.constants.O_RDONLY | NodeFS.constants.O_DIRECTORY | NodeFS.constants.O_NOFOLLOW,
        );
        try {
          await directory.sync();
        } finally {
          await directory.close();
        }
        custody.commandCustody.persisted = true;
      } catch (cause) {
        custody.errors.push(cause);
        custody.commandCustody.persistenceError = errorSummary(cause);
      } finally {
        // The receipt records observations, never authority to signal a saved
        // PID. Persist it before relinquishing this actual captured handle.
        child.stdout.destroy();
        child.stderr.destroy();
        child.unref();
      }
      throw phaseFailure(custody, phase);
    }
    if (failure) {
      const errors = commandErrors.filter((item) => item !== failure);
      if (errors.length)
        throw phaseFailure(
          new AggregateError([failure, ...errors], failure.message, { cause: failure }),
          phase,
        );
      throw phaseFailure(failure, phase);
    }
    return { ...result, stdout, stderr, phase };
  } finally {
    clearTimeout(timeout);
    clearTimeout(escalation);
    clearTimeout(custodyDeadline);
    signal?.removeEventListener("abort", abort);
  }
}

const command = runLegacyIdentityFixtureCommand;

export function validateLegacyIdentityObservation(value, entries = []) {
  NodeAssert.equal(value.schema, 1);
  NodeAssert.equal(value.status, "observed");
  NodeAssert.equal(value.operation, entries.length ? "inspect" : "boot");
  NodeAssert.equal(value.observationOnly, true);
  NodeAssert.equal(value.activationAllowed, false);
  NodeAssert.equal(value.ancestorPolicy, "nofollow-currentness-only");
  NodeAssert.match(value.bootSessionUuid, uuid);
  NodeAssert.notEqual(value.bootSessionUuid, "00000000-0000-0000-0000-000000000000");
  NodeAssert.equal(value.objects.length, entries.length);
  for (const [index, item] of value.objects.entries()) {
    NodeAssert.equal(item.index, index);
    NodeAssert.equal(item.path, entries[index].path);
    NodeAssert.equal(item.kind, entries[index].kind);
    NodeAssert.equal(item.current, true);
    NodeAssert.match(item.persistent.volumeUuid, uuid);
    NodeAssert.notEqual(item.persistent.volumeUuid, "00000000-0000-0000-0000-000000000000");
    NodeAssert.match(item.persistent.fileId, /^[1-9][0-9]{0,19}$/);
    for (const field of ["device", "inode", "nlink", "size", "mtimeNs", "ctimeNs", "birthtimeNs"])
      NodeAssert.match(item.stat[field], /^[0-9]{1,20}$/);
    for (const field of ["uid", "gid", "mode", "flags"])
      NodeAssert.ok(Number.isSafeInteger(item.stat[field]) && item.stat[field] >= 0);
    NodeAssert.equal(item.stat.mode & 0o022, 0);
    if (item.kind === "file") NodeAssert.equal(item.stat.nlink, "1");
    NodeAssert.equal(item.mount.filesystem, "apfs");
    NodeAssert.ok(NodePath.isAbsolute(item.mount.mountPoint));
    for (const field of [
      "local",
      "ownershipHonored",
      "executable",
      "persistentObjectIds",
      "objectIds64Bit",
    ])
      NodeAssert.equal(item.mount[field], true);
  }
  return value;
}

export async function runLegacyIdentityNativeFixture({ signal, temporaryDirectory } = {}) {
  signal?.throwIfAborted();
  if (nativePlatform !== "darwin")
    return {
      schema: 1,
      status: "unsupported",
      reason: "darwin-required",
      observationOnly: true,
      activationAllowed: false,
    };
  return withLegacyIdentityScratch(
    async (root) => {
      const binary = NodePath.join(root, "identity-native");
      const image = NodePath.join(root, "family.dmg");
      const mount = NodePath.join(root, "mount");
      let mounted = false;
      let creationUncertain = false;
      let attachUncertain = false;
      let failure;
      let result;
      const successful = (value) => {
        try {
          NodeAssert.equal(value.stoppedBy, null);
          NodeAssert.equal(
            value.code,
            0,
            `${value.phase}: ${value.stderr.slice(0, 2000)} ${value.stdout.slice(0, 2000)}`,
          );
        } catch (cause) {
          throw phaseFailure(cause, value.phase);
        }
        return value;
      };
      const invoke = async (entries, { refusal = false, overrideArgs } = {}) => {
        const handles = [];
        let invocationFailure;
        try {
          for (const entry of entries)
            handles.push(
              await NodeFSP.open(
                entry.fdPath ?? entry.path,
                darwinEventOnly | NodeFS.constants.O_NOFOLLOW,
              ),
            );
          const args = overrideArgs ?? [
            "inspect",
            ...entries.flatMap((entry, index) => [
              `--${entry.kind}`,
              String(index + 3),
              entry.path,
            ]),
          ];
          const outcome = await command(binary, args, root, {
            signal,
            fds: handles.map((handle) => handle.fd),
            phase: "identity-inspect",
          });
          NodeAssert.equal(outcome.stoppedBy, null);
          const value = JSON.parse(outcome.stdout);
          if (refusal) {
            NodeAssert.equal(outcome.code, 1);
            NodeAssert.equal(value.status, "refused");
            NodeAssert.deepEqual(value.objects, []);
            NodeAssert.equal(value.activationAllowed, false);
            return value;
          }
          successful(outcome);
          return validateLegacyIdentityObservation(value, entries);
        } catch (cause) {
          invocationFailure = cause;
          throw cause;
        } finally {
          const closures = await Promise.allSettled(handles.map((handle) => handle.close()));
          const errors = closures
            .filter((item) => item.status === "rejected")
            .map((item) => item.reason);
          if (errors.length)
            throw new AggregateError(
              invocationFailure ? [invocationFailure, ...errors] : errors,
              "Inherited identity descriptor closure failed.",
              { cause: invocationFailure ?? errors[0] },
            );
        }
      };
      const attach = async () => {
        signal?.throwIfAborted();
        attachUncertain = true;
        // Attach is allowed to settle before observing cancellation, so its known
        // mount can be detached by this owner instead of losing the mount handle.
        successful(
          await command(
            "/usr/bin/hdiutil",
            ["attach", image, "-nobrowse", "-owners", "on", "-mountpoint", mount],
            root,
            { phase: "image-attach" },
          ),
        );
        mounted = true;
        attachUncertain = false;
        signal?.throwIfAborted();
      };
      const detach = async () => {
        successful(
          await command("/usr/bin/hdiutil", ["detach", mount], root, { phase: "image-detach" }),
        );
        mounted = false;
      };
      try {
        successful(
          await command(
            "/usr/bin/clang",
            ["-std=c11", "-Wall", "-Wextra", "-Werror", source, "-o", binary],
            root,
            { signal, phase: "identity-compile" },
          ),
        );
        const boot = validateLegacyIdentityObservation(
          JSON.parse(
            successful(await command(binary, ["boot"], root, { signal, phase: "identity-boot" }))
              .stdout,
          ),
        );
        const independentBoot = successful(
          await command("/usr/sbin/sysctl", ["-n", "kern.bootsessionuuid"], root, {
            signal,
            phase: "identity-boot-reference",
          }),
        ).stdout.trim();
        NodeAssert.equal(boot.bootSessionUuid, independentBoot);
        signal?.throwIfAborted();
        creationUncertain = true;
        // Formatting may attach internally. Let it settle before cancellation;
        // unsuccessful creation does not prove those intermediate effects ended.
        successful(
          await command(
            "/usr/bin/hdiutil",
            [
              "create",
              "-size",
              "64m",
              "-fs",
              "APFS",
              "-type",
              "UDIF",
              "-volname",
              "JonesIdentityFixture",
              image,
            ],
            root,
            { phase: "image-create" },
          ),
        );
        creationUncertain = false;
        signal?.throwIfAborted();
        await NodeFSP.mkdir(mount, { mode: 0o700 });
        await attach();
        const family = NodePath.join(mount, "family");
        await NodeFSP.mkdir(family, { mode: 0o700 });
        const executable = NodePath.join(family, "console");
        await NodeFSP.copyFile(binary, executable, NodeFS.constants.COPYFILE_EXCL);
        await NodeFSP.chmod(executable, 0o750);
        const entries = [
          { kind: "file", path: executable },
          { kind: "directory", path: family },
        ];
        const before = await invoke(entries);
        NodeAssert.equal(before.bootSessionUuid, independentBoot);
        NodeAssert.equal(before.objects[0].mount.mountPoint, mount);
        const nativeStat = await NodeFSP.lstat(executable, { bigint: true });
        NodeAssert.equal(before.objects[0].stat.inode, String(nativeStat.ino));
        NodeAssert.equal(before.objects[0].stat.device, String(nativeStat.dev));
        NodeAssert.equal(before.objects[0].stat.birthtimeNs, String(nativeStat.birthtimeNs));
        const bytesBefore = sha256(await NodeFSP.readFile(executable));
        await NodeFSP.chmod(executable, 0o640);
        const gated = await invoke(entries);
        NodeAssert.deepEqual(gated.objects[0].persistent, before.objects[0].persistent);
        NodeAssert.equal(gated.objects[0].stat.mode, 0o640);
        // Preparation requires owner-readable payloads. Zero-mode drift has no
        // permission-bypass recovery in this observer and must fail closed.
        await NodeFSP.chmod(executable, 0o000);
        await NodeAssert.rejects(invoke(entries), { code: "EACCES" });
        await NodeFSP.chmod(executable, 0o640);
        const alias = NodePath.join(mount, "alias");
        await NodeFSP.symlink(family, alias);
        const aliasRefusal = await invoke(
          [{ kind: "file", path: NodePath.join(alias, "console"), fdPath: executable }],
          { refusal: true },
        );
        const hardlink = NodePath.join(family, "hardlink");
        await NodeFSP.link(executable, hardlink);
        const hardlinkRefusal = await invoke([entries[0]], { refusal: true });
        await NodeFSP.unlink(hardlink);
        const mismatchRefusal = await invoke([{ kind: "file", path: executable, fdPath: family }], {
          refusal: true,
        });
        const duplicateRefusal = await invoke([entries[0]], {
          refusal: true,
          overrideArgs: ["inspect", "--file", "3", executable, "--file", "3", executable],
        });
        await detach();
        await attach();
        const after = await invoke(entries);
        NodeAssert.equal(after.bootSessionUuid, before.bootSessionUuid);
        NodeAssert.deepEqual(after.objects[0].persistent, before.objects[0].persistent);
        for (const field of ["birthtimeNs", "uid", "gid", "flags", "size"])
          NodeAssert.equal(after.objects[0].stat[field], before.objects[0].stat[field]);
        NodeAssert.equal(after.objects[0].stat.mode, 0o640);
        await NodeFSP.chmod(executable, 0o750);
        NodeAssert.equal(sha256(await NodeFSP.readFile(executable)), bytesBefore);
        const restored = await invoke(entries);
        NodeAssert.equal(restored.objects[0].stat.mode, 0o750);
        result = {
          schema: 1,
          status: "passed",
          observationOnly: true,
          activationAllowed: false,
          productionMembershipQualified: false,
          offlineAdmissionQualified: false,
          bootSessionUuid: before.bootSessionUuid,
          before: before.objects[0],
          after: after.objects[0],
          restored: restored.objects[0],
          refusalReasons: [aliasRefusal, hardlinkRefusal, mismatchRefusal, duplicateRefusal].map(
            (value) => value.reason,
          ),
          unreadableEndpointRefused: true,
          sourceSha256: sha256(await NodeFSP.readFile(source)),
          binarySha256: bytesBefore,
          kernel: NodeOS.release(),
          nodeVersion: process.version,
          sameKernelRemountOnly: true,
          cleanupProved: true,
        };
      } catch (cause) {
        failure = cause;
      }
      try {
        if (mounted) await detach();
        if (creationUncertain || attachUncertain)
          throw new Error(
            "Native image creation or attachment is uncertain; exact scratch retained.",
          );
      } catch (cleanup) {
        const retained = new AggregateError(
          failure ? [failure, cleanup] : [cleanup],
          "Native image custody is unresolved; exact scratch retained.",
          { cause: failure ?? cleanup },
        );
        retained.preserveScratch = true;
        throw retained;
      }
      if (failure) throw failure;
      return result;
    },
    { temporaryDirectory },
  );
}

export const legacyIdentityWorkerCases = [
  "native-descriptor",
  "read-only-descriptor",
  "owner-witness",
  "witness-only",
  "inherited-witness",
  "pre-open-excluded",
  "empty-worker",
  "ambiguous-descriptors",
  "family-descriptors",
  "wrong-object",
  "invalid-family-path",
  "family-alias",
  "capture-parent-pid",
  "stale-unique-id",
  "stale-pid-version",
  "wrong-parent",
  "full-fd-list",
  "truncated-fd-list",
  "inaccessible-fd-list",
  "changing-fd-list",
  "changing-nonvnode-fd-list",
  "truncated-process-identity",
  "inaccessible-process-identity",
  "changing-process-identity",
  "truncated-vnode",
  "changing-vnode",
];

const workerObserverDriver = String.raw`
import sys
sys.dont_write_bytecode = True
import json, os, pathlib, resource, select, signal, subprocess, time
root = pathlib.Path(sys.argv[1]).resolve(strict=True)
payload = json.loads((root / 'observer.json').read_text())
class Cancelled(BaseException): pass
cancelled, cleaning = False, False
def cancel(signum, frame):
    global cancelled
    cancelled = True
    if not cleaning: raise Cancelled()
for signum in (signal.SIGTERM, signal.SIGINT, signal.SIGHUP): signal.signal(signum, cancel)
def tokens(value):
    return [value['uniqueId'], str(value['pidVersion']), value['parentUniqueId'], str(value['uid'])]
def reap(pid):
    deadline = time.monotonic() + 0.5
    while time.monotonic() < deadline:
        waited, status = os.waitpid(pid, os.WNOHANG)
        if waited == pid: return status
        time.sleep(0.01)
    # Only this captured, still-unreaped direct child may be signalled.
    os.kill(pid, signal.SIGKILL)
    deadline = time.monotonic() + 0.5
    while time.monotonic() < deadline:
        waited, status = os.waitpid(pid, os.WNOHANG)
        if waited == pid: return status
        time.sleep(0.01)
    raise AssertionError('Owned worker reap is unproved')
def receive(fd):
    assert select.select([fd], [], [], 5)[0], 'Worker readiness timed out'
    raw = os.read(fd, 4096)
    assert raw.endswith(b'\n')
    return json.loads(raw)
def observe(executable, args, local):
    result = subprocess.run([executable, *args], pass_fds=tuple(local), capture_output=True, timeout=5, check=False)
    assert len(result.stdout) < 262144 and result.stderr == b''
    value = json.loads(result.stdout)
    assert value['observationOnly'] is True and value['suppliedFamilyOnly'] is True
    assert value['processCustody'] == 'unobserved'
    for field in ('sqliteOpenProved', 'databaseSyncCloseProved', 'workerReaped', 'activationAllowed',
                  'stateAdmissionAllowed', 'launchMembershipComplete', 'familyMembershipComplete'):
        assert value[field] is False
    return result, value
reports = []
owned = set()
def record_custody():
    raw = json.dumps({'capturedUnreapedChildren':sorted(owned), 'driverPid':os.getpid(),
                      'permitsPidSignalling':False, 'permitsProcessAdoption':False}).encode()
    fd = os.open(root / 'observer-custody.json', os.O_WRONLY | os.O_CREAT | os.O_TRUNC | os.O_NOFOLLOW, 0o600)
    try:
        assert os.write(fd, raw) == len(raw)
        os.fsync(fd)
    finally: os.close(fd)
try:
    for case in payload['cases']:
        if cancelled: raise Cancelled()
        directory = root / case
        directory.mkdir(mode=0o700)
        path = directory / 'state.sqlite'
        path.write_bytes(b'owned descriptor bytes, not a SQLite DatabaseSync handle')
        path.chmod(0o600)
        other = directory / 'other'
        other.write_bytes(b'other object')
        other.chmod(0o600)
        sidecars = [pathlib.Path(str(path) + suffix) for suffix in ('-wal', '-shm', '-journal')]
        if case == 'family-descriptors':
            for sidecar in sidecars:
                sidecar.write_bytes(b'owned sidecar identity')
                sidecar.chmod(0o600)
        inherited = os.open(path, os.O_RDWR | os.O_NOFOLLOW)
        # This intentional negative model records the FD before fork. A real
        # worker inherits no database witness and never nominates its SQLite FD.
        excluded = [inherited] if case == 'pre-open-excluded' else []
        command_read, command_write = os.pipe()
        report_read, report_write = os.pipe()
        cleaning = True
        pid = os.fork()
        if pid == 0:
            for signum in (signal.SIGTERM, signal.SIGINT, signal.SIGHUP): signal.signal(signum, signal.SIG_DFL)
            os.close(command_write)
            os.close(report_read)
            try:
                retained = case in ('inherited-witness', 'pre-open-excluded')
                if not retained: os.close(inherited)
                os.write(report_write, b'{"ready":true}\n')
                request = os.read(command_read, 1)
                if request == b'': os._exit(0)
                assert request == b'o'
                targets = []
                if retained: targets = [inherited]
                elif case not in ('owner-witness', 'empty-worker'):
                    flags = 0x8000 if case == 'witness-only' else (os.O_RDONLY if case == 'read-only-descriptor' else os.O_RDWR)
                    targets = [os.open(path, flags | os.O_NOFOLLOW)]
                    if case == 'ambiguous-descriptors': targets.append(os.open(path, os.O_RDWR | os.O_NOFOLLOW))
                    if case == 'family-descriptors':
                        targets.extend(os.open(item, os.O_RDWR | os.O_NOFOLLOW) for item in sidecars)
                extra = []
                if case == 'full-fd-list':
                    soft, hard = resource.getrlimit(resource.RLIMIT_NOFILE)
                    assert hard == resource.RLIM_INFINITY or hard >= 512
                    raised = soft if soft == resource.RLIM_INFINITY else max(soft, 512)
                    resource.setrlimit(resource.RLIMIT_NOFILE, (raised, hard))
                    extra = [os.open(path, os.O_RDONLY | os.O_NOFOLLOW) for _ in range(256)]
                os.write(report_write, json.dumps({'fds':targets}).encode() + b'\n')
                os.close(report_write)
                assert os.read(command_read, 1) in (b'', b'q')
                for fd in extra + targets: os.close(fd)
                os.close(command_read)
                os._exit(0)
            except BaseException: os._exit(1)
        owned.add(pid)
        os.close(command_read)
        os.close(report_write)
        local = []
        phase = 'capture'
        try:
            record_custody()
            cleaning = False
            if cancelled: raise Cancelled()
            assert receive(report_read) == {'ready':True}
            witness_path = other if case == 'wrong-object' else path
            local.append(os.open(witness_path, 0x8000 | os.O_NOFOLLOW))
            family_args = ['--database', str(local[0]), str(witness_path)]
            exclude_args = [arg for fd in excluded for arg in ('--exclude-worker-fd', str(fd))]
            capture_args = ['capture-worker', str(os.getpid() if case == 'capture-parent-pid' else pid), *exclude_args, *family_args]
            captured, baseline = observe(payload['binary'], capture_args, local)
            assert baseline['operation'] == 'capture-worker'
            if case == 'capture-parent-pid':
                assert captured.returncode == 1 and baseline['reason'] == 'invalid-process-binding', baseline
                reports.append({'case':case, 'status':'refused'})
                continue
            assert captured.returncode == 0 and baseline['status'] == 'observed', baseline
            assert baseline['processes'][0]['pid'] == os.getpid() and baseline['processes'][1]['pid'] == pid
            assert not any(item['eligibleOpenObservation'] for item in baseline['matches'])
            owner, worker = [tokens(item) for item in baseline['processes']]
            assert worker[2] == owner[0]
            if case == 'pre-open-excluded':
                os.close(inherited)
                inherited = None
            if case == 'family-alias': os.link(path, sidecars[0])
            if case in ('family-descriptors', 'invalid-family-path', 'family-alias'):
                items = sidecars if case == 'family-descriptors' else [other if case == 'invalid-family-path' else sidecars[0]]
                for role, item in zip(('wal', 'shm', 'journal'), items):
                    local.append(os.open(item, 0x8000 | os.O_NOFOLLOW))
                    family_args.extend(['--' + role, str(local[-1]), str(item)])
            os.write(command_write, b'o')
            remote_fds = receive(report_read)['fds']
            os.close(report_read)
            report_read = -1
            if case == 'stale-unique-id': worker[0] = str(int(worker[0]) + 1)
            if case == 'stale-pid-version': worker[1] = str((int(worker[1]) + 1) & 0xffffffff)
            args = ['observe-worker', *owner, str(pid), *worker, *exclude_args, *family_args]
            faults = {'truncated-fd-list':'short', 'inaccessible-fd-list':'denied', 'changing-fd-list':'changed',
                      'changing-nonvnode-fd-list':'changed-nonvnode',
                      'wrong-parent':'parent', 'truncated-process-identity':'identity-short',
                      'inaccessible-process-identity':'identity-denied', 'changing-process-identity':'identity-changed',
                      'truncated-vnode':'vnode-short', 'changing-vnode':'vnode-changed'}
            executable = payload['instrumented'] if case in faults else payload['binary']
            if case in faults: args = [faults[case], *args]
            phase = 'observe'
            result, value = observe(executable, args, local)
            assert value['operation'] == 'observe-worker'
            positive = ('native-descriptor', 'read-only-descriptor', 'witness-only', 'owner-witness',
                        'inherited-witness', 'pre-open-excluded', 'empty-worker', 'ambiguous-descriptors',
                        'family-descriptors', 'wrong-object')
            phase = 'positive-controls' if case in positive else 'refusal-controls'
            if case in positive:
                assert result.returncode == 0 and value['status'] == 'observed', value
                for observed, expected in zip(value['processes'], (owner, worker)):
                    assert tokens(observed) == expected and 0 < observed['fdCount'] < 256
                assert value['excludedWorkerFds'] == excluded
                assert value['family'][0]['role'] == 'database'
                assert value['family'][0]['object']['persistent']['fileId'] == str(witness_path.stat().st_ino)
                matches = [item for item in value['matches'] if item['process'] == 'worker']
                assert all(not item['eligibleOpenObservation'] and item['exclusion'] == 'owner'
                           for item in value['matches'] if item['process'] == 'owner')
                if case in ('empty-worker', 'owner-witness', 'wrong-object'):
                    assert matches == [], matches
                else:
                    assert sorted(item['fd'] for item in matches) == sorted(remote_fds), matches
                    for item in matches:
                        expected_exclusion = {'witness-only':'event-only', 'inherited-witness':'shared',
                                              'pre-open-excluded':'pre-open-inherited'}.get(case)
                        assert item['exclusion'] == expected_exclusion
                        assert item['eligibleOpenObservation'] == (expected_exclusion is None)
                        access = 'event-only' if case == 'witness-only' else ('read-only' if case == 'read-only-descriptor' else 'read-write')
                        assert item['access'] == access, item
                    if case == 'family-descriptors':
                        assert sorted(item['familyRole'] for item in matches) == ['database', 'journal', 'shm', 'wal']
                    elif case == 'ambiguous-descriptors':
                        assert len(matches) == 2 and all(item['familyRole'] == 'database' for item in matches)
                    else: assert len(matches) == 1 and matches[0]['familyRole'] == 'database'
            else:
                reasons = {'stale-unique-id':'worker-identity-mismatch', 'stale-pid-version':'worker-identity-mismatch',
                           'wrong-parent':'worker-identity-mismatch', 'invalid-family-path':'invalid-family-path',
                           'family-alias':'endpoint-type-links-or-ownership',
                           'full-fd-list':'fd-list-incomplete', 'truncated-fd-list':'fd-list-incomplete',
                           'inaccessible-fd-list':'fd-list-incomplete', 'changing-fd-list':'owner-fd-snapshot-changed',
                           'changing-nonvnode-fd-list':'owner-fd-snapshot-changed',
                           'truncated-process-identity':'process-identity-unavailable',
                           'inaccessible-process-identity':'process-identity-unavailable',
                           'changing-process-identity':'process-bracket-changed',
                           'truncated-vnode':'vnode-observation-unavailable', 'changing-vnode':'owner-fd-snapshot-changed'}
                assert result.returncode == 1 and value['status'] == 'refused' and value['reason'] == reasons[case], value
                assert value['processes'] == [] and value['matches'] == [] and value['family'] == []
            reports.append({'case':case, 'status':value['status']})
        except Exception as cause:
            raise AssertionError('Observer case ' + case + ' phase ' + phase) from cause
        finally:
            cleaning = True
            if report_read >= 0: os.close(report_read)
            for fd in local: os.close(fd)
            if inherited is not None: os.close(inherited)
            os.close(command_write)
            status = reap(pid)
            owned.remove(pid)
            record_custody()
            cleaning = False
            assert os.WIFEXITED(status) and os.WEXITSTATUS(status) == 0, 'Worker cleanup failed'
finally:
    if not owned:
        (root / 'observer-children-closed.json').write_text(json.dumps({'childrenClosed':True}))
print(json.dumps({'cases':reports, 'observationOnly':True, 'sqliteLifecycleQualified':False,
                  'offlineAdmissionQualified':False, 'cleanupProved':True}))
`;

export async function runLegacyIdentityWorkerFixture({ signal } = {}) {
  signal?.throwIfAborted();
  if (nativePlatform !== "darwin") return { status: "unsupported", reason: "darwin-required" };
  return withLegacyIdentityScratch(async (root) => {
    const binary = NodePath.join(root, "observer-native");
    const instrumented = NodePath.join(root, "observer-instrumented");
    const wrapper = NodePath.join(root, "observer-wrapper.c");
    const driver = NodePath.join(root, "observer.py");
    await NodeFSP.writeFile(
      wrapper,
      `#include <errno.h>
#include <libproc.h>
#include <stdint.h>
#include <string.h>
static int fixture_pidinfo(int, int, uint64_t, void *, int);
static int fixture_pidfdinfo(int, int, int, void *, int);
#define proc_pidinfo fixture_pidinfo
#define proc_pidfdinfo fixture_pidfdinfo
#define main observer_main
#include ${JSON.stringify(source)}
#undef main
#undef proc_pidinfo
#undef proc_pidfdinfo
static const char *fault;
static int lists, identities, changed_lists;
static int fixture_pidinfo(int pid, int flavor, uint64_t arg, void *buffer, int size) {
  int result = proc_pidinfo(pid, flavor, arg, buffer, size);
  if (flavor == PROC_PIDTBSDINFO && result == (int)sizeof(struct proc_bsdinfo) &&
      !strcmp(fault, "parent") && pid != getppid()) ((struct proc_bsdinfo *)buffer)->pbi_ppid = 0;
  if (flavor == OWNER_UNIQUE_FLAVOR && result == (int)sizeof(struct owner_unique_info)) {
    identities++;
    if (!strcmp(fault, "identity-short")) return result - 1;
    if (!strcmp(fault, "identity-denied")) { errno = EACCES; return 0; }
    if (!strcmp(fault, "identity-changed") && identities == 5) ((struct owner_unique_info *)buffer)->unique_id++;
  }
  if (flavor != PROC_PIDLISTFDS || result <= 0) return result;
  lists++;
  if (!strcmp(fault, "short")) return result - 1;
  if (!strcmp(fault, "denied")) { errno = EACCES; return 0; }
  if ((!strcmp(fault, "changed") || !strcmp(fault, "changed-nonvnode")) && lists == 3) {
    struct proc_fdinfo *fds = buffer;
    int count = result / (int)sizeof(*fds);
    /* Model a close between complete snapshots. Never invent a descriptor:
       vnode readback must remain valid before the snapshot comparison. */
    for (int i = 0; i < count && count > 1; i++) {
      int vnode = fds[i].proc_fdtype == PROX_FDTYPE_VNODE;
      if (vnode != !strcmp(fault, "changed")) continue;
      memmove(&fds[i], &fds[i + 1], (size_t)(count - i - 1) * sizeof(*fds));
      changed_lists++;
      return result - (int)sizeof(*fds);
    }
  }
  return result;
}
static int fixture_pidfdinfo(int pid, int fd, int flavor, void *buffer, int size) {
  int result = proc_pidfdinfo(pid, fd, flavor, buffer, size);
  if (flavor == PROC_PIDFDVNODEINFO && result == (int)sizeof(struct vnode_fdinfo)) {
    if (!strcmp(fault, "vnode-short")) return result - 1;
    if (!strcmp(fault, "vnode-changed") && lists >= 3)
      ((struct vnode_fdinfo *)buffer)->pfi.fi_offset++;
  }
  return result;
}
int main(int argc, char **argv) {
  if (argc < 2) return 2;
  fault = argv[1];
  int result = observer_main(argc - 1, argv + 1);
  if ((!strcmp(fault, "changed") || !strcmp(fault, "changed-nonvnode")) && changed_lists != 1) return 78;
  return result;
}
`,
      { flag: "wx", mode: 0o600 },
    );
    for (const [input, output] of [
      [source, binary],
      [wrapper, instrumented],
    ]) {
      const result = await command(
        "/usr/bin/clang",
        ["-std=c11", "-Wall", "-Wextra", "-Werror", input, "-o", output],
        root,
        { signal, phase: input === source ? "observer-compile" : "observer-wrapper-compile" },
      );
      try {
        NodeAssert.equal(result.code, 0, `${result.phase}: ${result.stderr}`);
      } catch (cause) {
        throw phaseFailure(cause, result.phase);
      }
    }
    await NodeFSP.writeFile(driver, workerObserverDriver, { flag: "wx", mode: 0o600 });
    await NodeFSP.writeFile(
      NodePath.join(root, "observer.json"),
      JSON.stringify({ binary, instrumented, cases: legacyIdentityWorkerCases }),
      { flag: "wx", mode: 0o600 },
    );
    let originalFailure;
    try {
      const result = await command("/usr/bin/python3", ["-I", "-B", driver, root], root, {
        signal,
        phase: "observer-driver",
      });
      NodeAssert.equal(result.code, 0, result.stderr);
      NodeAssert.equal(result.stoppedBy, null);
      return JSON.parse(result.stdout);
    } catch (cause) {
      originalFailure = cause;
      throw phaseFailure(cause, "observer-driver");
    } finally {
      const closure = await NodeFSP.readFile(
        NodePath.join(root, "observer-children-closed.json"),
        "utf8",
      )
        .then((raw) => JSON.parse(raw))
        .catch(() => undefined);
      if (closure?.childrenClosed !== true) {
        const failure = new AggregateError(
          originalFailure ? [originalFailure] : [],
          "Native observer child closure is unproved; scratch retained.",
          { cause: originalFailure },
        );
        failure.preserveScratch = true;
        throw failure;
      }
    }
  });
}

if (
  process.argv[1] &&
  NodeURL.pathToFileURL(NodePath.resolve(process.argv[1])).href === import.meta.url
) {
  const cancellation = new AbortController();
  const handlers = new Map(
    ["SIGINT", "SIGTERM", "SIGHUP"].map((name) => [
      name,
      () => cancellation.abort(new Error(`Native identity fixture received ${name}.`)),
    ]),
  );
  for (const [name, handler] of handlers) process.on(name, handler);
  try {
    NodeAssert.equal(process.argv.length, 2, "The qualifier accepts no production paths.");
    const result = await runLegacyIdentityNativeFixture({ signal: cancellation.signal });
    process.stdout.write(`${JSON.stringify(result)}\n`);
    if (result.status === "unsupported") process.exitCode = 77;
  } catch (cause) {
    process.stderr.write(`${JSON.stringify(legacyIdentityFailureReport(cause))}\n`);
    process.exitCode = 1;
  } finally {
    for (const [name, handler] of handlers) process.removeListener(name, handler);
  }
}
