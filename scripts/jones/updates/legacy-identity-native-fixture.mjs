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
  if (failure?.preserveScratch) {
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

async function command(executable, args, root, { signal, fds = [] } = {}) {
  signal?.throwIfAborted();
  const child = NodeChildProcess.spawn(executable, args, {
    cwd: root,
    env: {
      PATH: "/usr/bin:/bin",
      HOME: root,
      TMPDIR: root,
      CLANG_MODULE_CACHE_PATH: NodePath.join(root, "clang-cache"),
    },
    stdio: ["ignore", "pipe", "pipe", ...fds],
  });
  let stdout = "";
  let stderr = "";
  let failure;
  let escalation;
  let custodyDeadline;
  let closed = false;
  let finish;
  const completion = new Promise((resolve) => {
    finish = resolve;
  });
  const unknownCustody = () => {
    const cause = new Error("Native command closure is unproved; exact scratch retained.", {
      cause: failure,
    });
    cause.preserveScratch = true;
    cause.ownedPid = child.pid;
    failure = cause;
    child.stdout.destroy();
    child.stderr.destroy();
    child.unref();
    finish({ code: null, stoppedBy: null });
  };
  const stop = (cause) => {
    failure ??= cause;
    custodyDeadline ??= setTimeout(unknownCustody, 6000);
    if (closed || child.exitCode !== null || child.signalCode !== null) return;
    child.kill("SIGTERM");
    escalation ??= setTimeout(() => {
      if (!closed && child.exitCode === null && child.signalCode === null) child.kill("SIGKILL");
    }, 3000);
  };
  const abort = () => stop(signal.reason ?? new Error("Native identity fixture cancelled."));
  signal?.addEventListener("abort", abort, { once: true });
  const timeout = setTimeout(
    () => stop(new Error("Native identity command exceeded 30 seconds.")),
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
  child.once("error", (cause) => {
    failure ??= cause;
  });
  child.once("close", (code, stoppedBy) => {
    closed = true;
    finish({ code, stoppedBy });
  });
  if (signal?.aborted) abort();
  try {
    const result = await completion;
    if (failure) throw failure;
    return { ...result, stdout, stderr };
  } finally {
    clearTimeout(timeout);
    clearTimeout(escalation);
    clearTimeout(custodyDeadline);
    signal?.removeEventListener("abort", abort);
  }
}

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
        NodeAssert.equal(value.stoppedBy, null);
        NodeAssert.equal(
          value.code,
          0,
          `${value.stderr.slice(0, 2000)} ${value.stdout.slice(0, 2000)}`,
        );
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
          ),
        );
        mounted = true;
        attachUncertain = false;
        signal?.throwIfAborted();
      };
      const detach = async () => {
        successful(await command("/usr/bin/hdiutil", ["detach", mount], root));
        mounted = false;
      };
      try {
        successful(
          await command(
            "/usr/bin/clang",
            ["-std=c11", "-Wall", "-Wextra", "-Werror", source, "-o", binary],
            root,
            { signal },
          ),
        );
        const boot = validateLegacyIdentityObservation(
          JSON.parse(successful(await command(binary, ["boot"], root, { signal })).stdout),
        );
        const independentBoot = successful(
          await command("/usr/sbin/sysctl", ["-n", "kern.bootsessionuuid"], root, { signal }),
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
    process.stderr.write(
      `${JSON.stringify({ status: "failed", message: cause.message, retainedScratch: cause.scratchRoot, activationAllowed: false })}\n`,
    );
    process.exitCode = 1;
  } finally {
    for (const [name, handler] of handlers) process.removeListener(name, handler);
  }
}
