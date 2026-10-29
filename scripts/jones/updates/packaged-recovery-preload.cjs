"use strict";

const fs = require("node:fs");
const fsp = require("node:fs/promises");
const path = require("node:path");
const { syncBuiltinESMExports } = require("node:module");

const planPath = process.env.JONES_PACKAGED_RECOVERY_FIXTURE;
if (planPath !== undefined) {
  install(planPath);
  delete process.env.JONES_PACKAGED_RECOVERY_FIXTURE;
}

function ensure(condition, message) {
  if (!condition) throw new Error("Packaged recovery fixture refused: " + message);
}

function exactKeys(value, keys) {
  return (
    value !== null &&
    typeof value === "object" &&
    !Array.isArray(value) &&
    Object.keys(value).sort().join("\0") === [...keys].sort().join("\0")
  );
}

function privateJson(filename) {
  const fd = fs.openSync(filename, fs.constants.O_RDONLY | fs.constants.O_NOFOLLOW);
  try {
    const stat = fs.fstatSync(fd);
    ensure(
      stat.isFile() &&
        stat.uid === process.getuid() &&
        stat.nlink === 1 &&
        (stat.mode & 0o077) === 0 &&
        stat.size <= 8192,
      "unowned or unbounded fixture record",
    );
    return JSON.parse(fs.readFileSync(fd, "utf8"));
  } finally {
    fs.closeSync(fd);
  }
}

function identity(filename) {
  const stat = fs.statSync(filename, { bigint: true });
  return Object.fromEntries(
    ["dev", "ino", "size", "mtimeNs", "ctimeNs"].map((key) => [key, String(stat[key])]),
  );
}

function sameIdentity(left, right) {
  const keys = ["dev", "ino", "size", "mtimeNs", "ctimeNs"];
  return (
    exactKeys(left, keys) && exactKeys(right, keys) && keys.every((key) => left[key] === right[key])
  );
}

function install(filename) {
  ensure(
    // oxlint-disable-next-line t3code/no-global-process-runtime -- This standalone fixture binds its actual host without loading application services.
    process.platform === "linux" && typeof process.getuid === "function",
    "Linux fixture required",
  );
  ensure(
    path.isAbsolute(filename) && path.basename(filename) === "fault-plan.json",
    "invalid plan path",
  );
  const root = path.dirname(filename);
  ensure(
    fs.realpathSync(root) === root && path.basename(root).startsWith("jones-packaged-recovery-"),
    "dedicated physical fixture root required",
  );
  const stat = fs.lstatSync(root);
  ensure(
    stat.isDirectory() && stat.uid === process.getuid() && (stat.mode & 0o077) === 0,
    "private fixture root required",
  );
  const plan = privateJson(filename);
  ensure(
    exactKeys(plan, [
      "protocol",
      "fixtureId",
      "executable",
      "identity",
      "method",
      "target",
      "arm",
      "operationId",
    ]) &&
      plan.protocol === 1 &&
      /^[a-f0-9]{32}$/.test(plan.fixtureId),
    "invalid plan",
  );
  ensure(
    [
      "readFile-before",
      "mkdir-before",
      "rename-after",
      "rm-after",
      "unlink-after",
      "link-after",
      "write-after",
      "sync-after",
      "directory-sync-after",
      "outcome-sync-after",
    ].includes(plan.method),
    "unsupported fault boundary",
  );
  ensure(
    ["immediate", "pending-update"].includes(plan.arm) &&
      /^[a-f0-9]{8}-[a-f0-9]{4}-4[a-f0-9]{3}-[89ab][a-f0-9]{3}-[a-f0-9]{12}$/.test(
        plan.operationId,
      ),
    "invalid acceptance binding",
  );
  ensure(
    typeof plan.target === "string" &&
      plan.target.length <= 4096 &&
      plan.target.startsWith("runtime/") &&
      !plan.target.includes("\\") &&
      !plan.target.includes("\0") &&
      !plan.target.split("/").some((part) => part === "" || part === "." || part === ".."),
    "invalid relative runtime target",
  );
  const stagingTarget = "runtime/jones-update-settlements.staging";
  if (["write-after", "sync-after"].includes(plan.method))
    ensure(plan.target === stagingTarget, "settlement staging target required");
  if (plan.method === "directory-sync-after")
    ensure(plan.target === "runtime/jones-update-settlements", "settlement directory required");
  if (plan.method === "outcome-sync-after")
    ensure(
      plan.target === "runtime/jones-update-operations",
      "operation outcome directory required",
    );
  if (plan.method === "link-after")
    ensure(
      [
        "runtime/jones-update-settlements/" + plan.operationId + ".json",
        "runtime/jones-update-operations/" + plan.operationId + ".outcome.json",
      ].includes(plan.target),
      "operation receipt target required",
    );
  if (plan.method === "unlink-after")
    ensure(
      plan.target === "runtime/db-backup/" + plan.operationId + "/database",
      "operation recovery database target required",
    );
  const home = path.join(root, "home");
  ensure(
    process.env.T3CODE_HOME === home && fs.realpathSync(home) === home,
    "isolated home differs",
  );
  const executable = fs.realpathSync(process.execPath);
  ensure(
    plan.executable === executable &&
      exactKeys(plan.identity, ["dev", "ino", "size", "mtimeNs", "ctimeNs"]) &&
      sameIdentity(plan.identity, identity(executable)) &&
      sameIdentity(plan.identity, identity("/proc/self/exe")),
    "executable identity differs",
  );
  const marker = privateJson(path.join(root, "fixture-owner.json"));
  ensure(
    exactKeys(marker, ["protocol", "purpose", "fixtureId", "executable", "identity"]) &&
      marker.protocol === 1 &&
      marker.purpose === "jones-packaged-recovery" &&
      marker.fixtureId === plan.fixtureId &&
      marker.executable === executable &&
      sameIdentity(marker.identity, plan.identity),
    "fixture ownership differs",
  );

  const target = path.join(home, plan.target);
  const claimPath = path.join(root, "fault-fired.json");
  let fired = false;
  function checkClaim(value) {
    ensure(
      exactKeys(value, ["protocol", "fixtureId", "operationId", "method", "target"]) &&
        value.protocol === 1 &&
        value.fixtureId === plan.fixtureId &&
        value.operationId === plan.operationId &&
        value.method === plan.method &&
        value.target === plan.target,
      "existing fault claim differs",
    );
  }
  try {
    checkClaim(privateJson(claimPath));
    fired = true;
  } catch (error) {
    if (error.code !== "ENOENT") throw error;
  }

  function armed() {
    if (plan.arm === "immediate") return true;
    const statePath = path.join(home, "runtime", "service-state.json");
    let fd;
    try {
      fd = fs.openSync(statePath, fs.constants.O_RDONLY | fs.constants.O_NOFOLLOW);
    } catch (error) {
      if (error.code === "ENOENT") return false;
      throw error;
    }
    try {
      ensure(fs.fstatSync(fd).size <= 65536, "unbounded service state");
      const state = JSON.parse(fs.readFileSync(fd, "utf8"));
      return state.update?.id === plan.operationId && state.update?.status === "pending";
    } finally {
      fs.closeSync(fd);
    }
  }

  function matches(value) {
    if (typeof value !== "string" || fired) return false;
    let resolved = path.resolve(value);
    if (resolved !== target && plan.method === "unlink-after") {
      const pinned = /^\/proc\/self\/fd\/([0-9]+)\/([^/]+)$/.exec(value);
      if (pinned === null || !Number.isSafeInteger(Number(pinned[1]))) return false;
      const descriptor = "/proc/self/fd/" + pinned[1];
      try {
        const directory = fs.realpathSync(descriptor);
        if (directory !== path.dirname(target)) return false;
        const actual = fs.fstatSync(Number(pinned[1]));
        const expected = fs.lstatSync(directory);
        ensure(
          actual.isDirectory() &&
            expected.isDirectory() &&
            actual.uid === process.getuid() &&
            (actual.mode & 0o022) === 0 &&
            actual.dev === expected.dev &&
            actual.ino === expected.ino,
          "pinned cleanup directory differs",
        );
        resolved = path.join(directory, pinned[2]);
      } catch (error) {
        if (error.code === "ENOENT" || error.code === "EBADF") return false;
        throw error;
      }
    }
    if (resolved !== target) return false;
    let cursor = home;
    for (const component of plan.target.split("/")) {
      cursor = path.join(cursor, component);
      try {
        ensure(!fs.lstatSync(cursor).isSymbolicLink(), "symlink in fault target");
      } catch (error) {
        if (error.code !== "ENOENT") throw error;
        break;
      }
    }
    return armed();
  }

  function claim() {
    const record = {
      protocol: 1,
      fixtureId: plan.fixtureId,
      operationId: plan.operationId,
      method: plan.method,
      target: plan.target,
    };
    let fd;
    try {
      fd = fs.openSync(
        claimPath,
        fs.constants.O_CREAT |
          fs.constants.O_EXCL |
          fs.constants.O_WRONLY |
          fs.constants.O_NOFOLLOW,
        0o600,
      );
    } catch (error) {
      if (error.code !== "EEXIST") throw error;
      checkClaim(privateJson(claimPath));
      fired = true;
      return false;
    }
    try {
      fs.writeFileSync(fd, JSON.stringify(record) + "\n");
      fs.fsyncSync(fd);
    } finally {
      fs.closeSync(fd);
    }
    const directory = fs.openSync(root, fs.constants.O_RDONLY | fs.constants.O_DIRECTORY);
    try {
      fs.fsyncSync(directory);
    } finally {
      fs.closeSync(directory);
    }
    fired = true;
    return true;
  }

  if (
    ["write-after", "sync-after", "directory-sync-after", "outcome-sync-after"].includes(
      plan.method,
    )
  ) {
    const originalOpen = fsp.open;
    fsp.open = async function (...args) {
      const filename = args[0];
      const staged =
        typeof filename === "string" &&
        matches(path.dirname(path.resolve(filename))) &&
        /^[a-f0-9]{8}-[a-f0-9]{4}-4[a-f0-9]{3}-[89ab][a-f0-9]{3}-[a-f0-9]{12}$/.test(
          path.basename(filename),
        );
      const directory =
        ["directory-sync-after", "outcome-sync-after"].includes(plan.method) && matches(filename);
      const handle = await Reflect.apply(originalOpen, this, args);
      if (staged && !["directory-sync-after", "outcome-sync-after"].includes(plan.method)) {
        const originalWrite = handle.writeFile;
        const originalSync = handle.sync;
        let selected = false;
        handle.writeFile = async function (...values) {
          const result = await Reflect.apply(originalWrite, this, values);
          const data = values[0];
          if (
            (typeof data === "string" || Buffer.isBuffer(data)) &&
            Buffer.byteLength(data) <= 8192
          ) {
            try {
              const row = JSON.parse(String(data));
              selected =
                row.schema === 1 &&
                row.operationId === plan.operationId &&
                row.status === "failed" &&
                row.settlement === "no-effect";
            } catch {
              selected = false;
            }
          } else selected = false;
          if (
            selected &&
            plan.method === "write-after" &&
            matches(path.dirname(filename)) &&
            claim()
          )
            process.kill(process.pid, "SIGKILL");
          return result;
        };
        handle.sync = async function (...values) {
          const result = await Reflect.apply(originalSync, this, values);
          if (
            selected &&
            plan.method === "sync-after" &&
            matches(path.dirname(filename)) &&
            claim()
          )
            process.kill(process.pid, "SIGKILL");
          return result;
        };
      } else if (directory) {
        const originalSync = handle.sync;
        handle.sync = async function (...values) {
          const result = await Reflect.apply(originalSync, this, values);
          let row;
          try {
            row = privateJson(
              path.join(
                target,
                plan.operationId +
                  (plan.method === "outcome-sync-after" ? ".outcome.json" : ".json"),
              ),
            );
          } catch (error) {
            if (error.code !== "ENOENT") throw error;
          }
          if (
            row?.schema === 1 &&
            row.operationId === plan.operationId &&
            row.status === "failed" &&
            (plan.method === "outcome-sync-after"
              ? row.binding !== undefined && !Object.hasOwn(row, "reason")
              : row.settlement === "no-effect") &&
            matches(filename) &&
            claim()
          )
            process.kill(process.pid, "SIGKILL");
          return result;
        };
      }
      return handle;
    };
    syncBuiltinESMExports();
    return;
  }

  const [method, phase] = plan.method.split("-");
  const original = fsp[method];
  fsp[method] = async function (...args) {
    const affected = matches(method === "rename" || method === "link" ? args[1] : args[0]);
    if (affected && phase === "before" && claim()) {
      const error = new Error("Injected packaged recovery " + plan.method);
      error.code = "EIO";
      throw error;
    }
    const result = await Reflect.apply(original, this, args);
    if (affected && phase === "after" && claim()) {
      process.kill(process.pid, "SIGKILL");
    }
    return result;
  };
  syncBuiltinESMExports();
}
