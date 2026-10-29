"use strict";

const fs = require("node:fs");
const fsp = require("node:fs/promises");
const path = require("node:path");
const crypto = require("node:crypto");
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
  return privateRecord(filename).value;
}

function privateRecord(filename) {
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
    const bytes = fs.readFileSync(fd);
    return {
      value: JSON.parse(bytes.toString("utf8")),
      sha256: crypto.createHash("sha256").update(bytes).digest("hex"),
    };
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
      "open-before",
      "readFileSync-before",
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
    ["immediate", "pending-update", "accepted-update"].includes(plan.arm) &&
      /^[a-f0-9]{8}-[a-f0-9]{4}-4[a-f0-9]{3}-[89ab][a-f0-9]{3}-[a-f0-9]{12}$/.test(
        plan.operationId,
      ),
    "invalid acceptance binding",
  );
  ensure(
    typeof plan.target === "string" &&
      plan.target.length <= 4096 &&
      (plan.target.startsWith("runtime/") ||
        (plan.method === "readFileSync-before" &&
          plan.arm === "accepted-update" &&
          plan.target === "userdata/server-runtime.json")) &&
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
      if (state.update?.id !== plan.operationId || state.update?.status !== "pending") return false;
      if (plan.arm !== "accepted-update") return true;
      let receipt;
      try {
        receipt = privateJson(
          path.join(root, "ipc-observations", plan.operationId + ".accepted.json"),
        );
      } catch (error) {
        if (error.code === "ENOENT") return false;
        throw error;
      }
      ensure(
        receipt.protocol === 2 &&
          receipt.fixtureId === plan.fixtureId &&
          receipt.operationId === plan.operationId &&
          receipt.launcherPid === process.pid &&
          receipt.phase === "accepted" &&
          receipt.sendCallbackCompleted === true,
        "live acceptance receipt differs",
      );
      return state.update.phase === "accepted";
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

  if (plan.method === "readFileSync-before") {
    const original = fs.readFileSync;
    fs.readFileSync = function (...args) {
      if (matches(args[0]) && claim()) {
        const error = new Error("Injected packaged recovery readFileSync-before");
        error.code = "EIO";
        throw error;
      }
      return Reflect.apply(original, this, args);
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

const observerPath = process.env.JONES_PACKAGED_RECOVERY_OBSERVER;
if (observerPath !== undefined) {
  delete process.env.JONES_PACKAGED_RECOVERY_OBSERVER;
  installObserver(observerPath);
}

function installObserver(filename) {
  // oxlint-disable-next-line t3code/no-global-process-runtime -- This standalone fixture observes its actual Linux child without loading application services.
  ensure(process.platform === "linux", "Linux observer required");
  ensure(
    path.isAbsolute(filename) && path.basename(filename) === "observer-plan.json",
    "invalid observer plan path",
  );
  const root = path.dirname(filename);
  const rootStat = fs.lstatSync(root);
  ensure(
    fs.realpathSync(root) === root &&
      path.basename(root).startsWith("jones-packaged-recovery-") &&
      rootStat.isDirectory() &&
      rootStat.uid === process.getuid() &&
      (rootStat.mode & 0o077) === 0,
    "private physical observer fixture required",
  );
  const plan = privateJson(filename);
  const installing = plan.protocol === 2;
  const uuid = /^[a-f0-9]{8}-[a-f0-9]{4}-4[a-f0-9]{3}-[89ab][a-f0-9]{3}-[a-f0-9]{12}$/;
  ensure(
    exactKeys(plan, [
      "protocol",
      "fixtureId",
      "executable",
      "identity",
      "operationId",
      ...(installing ? ["binding"] : ["requestIds"]),
      "childExecutable",
      "childIdentity",
      "childVersion",
    ]) &&
      (plan.protocol === 1 || installing) &&
      /^[a-f0-9]{32}$/.test(plan.fixtureId) &&
      uuid.test(plan.operationId) &&
      (installing ||
        (Array.isArray(plan.requestIds) &&
          plan.requestIds.length === 2 &&
          plan.requestIds.every((id) => typeof id === "string" && uuid.test(id)) &&
          new Set(plan.requestIds).size === 2)) &&
      typeof plan.childVersion === "string" &&
      plan.childVersion.length > 0 &&
      plan.childVersion.length <= 256,
    "invalid observer bindings",
  );
  const executable = fs.realpathSync(process.execPath);
  const home = path.join(root, "home");
  ensure(
    process.env.T3CODE_HOME === home &&
      fs.realpathSync(home) === home &&
      plan.executable === executable &&
      sameIdentity(plan.identity, identity(executable)) &&
      sameIdentity(plan.identity, identity("/proc/self/exe")),
    "observer executable or home differs",
  );
  ensure(
    typeof plan.childExecutable === "string" &&
      path.isAbsolute(plan.childExecutable) &&
      fs.realpathSync(plan.childExecutable) === plan.childExecutable &&
      sameIdentity(plan.childIdentity, identity(plan.childExecutable)),
    "observer child identity differs",
  );
  if (installing) {
    const keys = [
      "baseDir",
      "dbPath",
      "environmentId",
      "currentVersion",
      "expectedInstalledSource",
      "targetSource",
      "stagedHandle",
      "targetVersion",
    ];
    ensure(
      exactKeys(plan.binding, keys) &&
        keys.every(
          (key) =>
            typeof plan.binding[key] === "string" &&
            plan.binding[key].length > 0 &&
            plan.binding[key].length <= 4096,
        ) &&
        plan.binding.baseDir === home &&
        plan.binding.dbPath === path.join(home, "userdata", "statev2.sqlite") &&
        plan.binding.currentVersion === plan.childVersion,
      "invalid Install binding",
    );
    const state = privateJson(path.join(home, "runtime", "service-state.json"));
    ensure(
      state.protocol === 4 &&
        state.activeVersion === plan.childVersion &&
        state.update === undefined,
      "Install observation requires idle native state",
    );
    const reservationPath = path.join(
      home,
      "runtime",
      "jones-update-operations",
      plan.operationId + ".json",
    );
    try {
      fs.lstatSync(reservationPath);
      ensure(false, "Install ID is already reserved");
    } catch (error) {
      if (error.code !== "ENOENT") throw error;
    }
  }
  const owner = privateJson(path.join(root, "fixture-owner.json"));
  ensure(
    exactKeys(owner, ["protocol", "purpose", "fixtureId", "executable", "identity"]) &&
      owner.protocol === 1 &&
      owner.purpose === "jones-packaged-recovery" &&
      owner.fixtureId === plan.fixtureId &&
      owner.executable === executable &&
      sameIdentity(owner.identity, plan.identity),
    "observer ownership differs",
  );
  const directory = path.join(root, "ipc-observations");
  fs.mkdirSync(directory, { mode: 0o700 });
  const allocation = fs.lstatSync(directory);
  function record(name, value) {
    const stat = fs.lstatSync(directory);
    ensure(
      fs.realpathSync(root) === root &&
        fs.realpathSync(directory) === directory &&
        stat.dev === allocation.dev &&
        stat.ino === allocation.ino &&
        stat.isDirectory() &&
        stat.uid === process.getuid() &&
        (stat.mode & 0o077) === 0,
      "observer allocation differs",
    );
    const target = path.join(directory, name + ".json");
    const bytes = JSON.stringify(value) + "\n";
    ensure(Buffer.byteLength(bytes) <= 2048, "observer projection exceeded bound");
    const temporary = target + ".writing";
    const fd = fs.openSync(
      temporary,
      fs.constants.O_WRONLY | fs.constants.O_CREAT | fs.constants.O_EXCL | fs.constants.O_NOFOLLOW,
      0o600,
    );
    const owned = fs.fstatSync(fd);
    try {
      fs.writeFileSync(fd, bytes);
      fs.fsyncSync(fd);
    } finally {
      fs.closeSync(fd);
    }
    try {
      fs.linkSync(temporary, target);
    } catch (error) {
      if (error.code !== "EEXIST") throw error;
      ensure(
        JSON.stringify(privateJson(target)) === JSON.stringify(value),
        "observer receipt collision",
      );
    } finally {
      const retained = fs.lstatSync(temporary);
      ensure(
        retained.dev === owned.dev && retained.ino === owned.ino,
        "observer temporary identity differs",
      );
      fs.unlinkSync(temporary);
    }
    const parent = fs.openSync(
      directory,
      fs.constants.O_RDONLY | fs.constants.O_DIRECTORY | fs.constants.O_NOFOLLOW,
    );
    try {
      fs.fsyncSync(parent);
    } finally {
      fs.closeSync(parent);
    }
  }
  const childProcess = require("node:child_process");
  const originalSpawn = childProcess.spawn;
  let captured = false;
  childProcess.spawn = function (command, args, options) {
    const contextText = options?.env?.T3_SERVICE_LAUNCHER_CONTEXT;
    if (typeof contextText !== "string" || options.env.T3CODE_HOME !== home)
      return Reflect.apply(originalSpawn, this, arguments);
    ensure(contextText.length <= 8192, "observer context exceeded bound");
    const context = JSON.parse(contextText);
    if (
      context.protocol !== 4 ||
      context.childVersion !== plan.childVersion ||
      context.update !== undefined
    )
      return Reflect.apply(originalSpawn, this, arguments);
    ensure(
      !captured &&
        typeof command === "string" &&
        fs.realpathSync(command) === plan.childExecutable &&
        sameIdentity(plan.childIdentity, identity(command)) &&
        Array.isArray(args) &&
        args.length === 1 &&
        args[0] === "serve" &&
        Array.isArray(options.stdio) &&
        options.stdio.join(",") === "inherit,inherit,inherit,ipc",
      "observer active child launch differs",
    );
    const child = Reflect.apply(originalSpawn, this, arguments);
    captured = true;
    if (installing) {
      let requested = false;
      let accepted = false;
      const common = {
        protocol: 2,
        fixtureId: plan.fixtureId,
        operationId: plan.operationId,
        launcherPid: process.pid,
        childPid: child.pid,
      };
      child.on("message", (message) => {
        if (message?.type !== "request-update" || message.operationId !== plan.operationId) return;
        ensure(
          !requested &&
            message.targetVersion === plan.binding.targetVersion &&
            message.dbPath === plan.binding.dbPath &&
            message.stagedHandle === plan.binding.stagedHandle &&
            Object.keys(message).every((key) =>
              [
                "type",
                "operationId",
                "targetVersion",
                "dbPath",
                "stagedHandle",
                "stateRecovery",
                "qualifiedRecoveryProtocol",
              ].includes(key),
            ),
          "Install request differs",
        );
        record(plan.operationId + ".request", { ...common, phase: "request" });
        requested = true;
      });
      const originalSend = child.send;
      child.send = function (message, ...rest) {
        if (message?.type !== "update-accepted" || message.updateId !== plan.operationId)
          return Reflect.apply(originalSend, this, [message, ...rest]);
        ensure(
          requested &&
            !accepted &&
            Object.keys(message).every((key) =>
              ["type", "updateId", "migrationPlan"].includes(key),
            ),
          "Install acceptance differs",
        );
        const callback = rest.at(-1);
        ensure(typeof callback === "function", "Install acceptance requires native send callback");
        rest[rest.length - 1] = function (error) {
          if (error == null) {
            const state = privateRecord(path.join(home, "runtime", "service-state.json"));
            const reserved = privateRecord(
              path.join(home, "runtime", "jones-update-operations", plan.operationId + ".json"),
            );
            const update = state.value.update;
            ensure(
              state.value.protocol === 4 &&
                state.value.activeVersion === plan.childVersion &&
                update?.id === plan.operationId &&
                update.status === "pending" &&
                update.phase === "accepted" &&
                update.fromVersion === plan.childVersion &&
                update.targetVersion === plan.binding.targetVersion &&
                update.dbPath === plan.binding.dbPath &&
                update.qualified?.stagedHandle === plan.binding.stagedHandle &&
                reserved.value.schema === 1 &&
                reserved.value.operationId === plan.operationId &&
                exactKeys(reserved.value.binding, Object.keys(plan.binding)) &&
                Object.keys(plan.binding).every(
                  (key) => reserved.value.binding[key] === plan.binding[key],
                ),
              "Install acceptance lacks matching durable native evidence",
            );
            record(plan.operationId + ".accepted", {
              ...common,
              phase: "accepted",
              sendCallbackCompleted: true,
              stateSha256: state.sha256,
              reservationSha256: reserved.sha256,
            });
            accepted = true;
          }
          return Reflect.apply(callback, this, arguments);
        };
        return Reflect.apply(originalSend, this, [message, ...rest]);
      };
      return child;
    }
    const seen = new Set();
    const projection = (requestId, phase, extra = {}) => ({
      protocol: 1,
      fixtureId: plan.fixtureId,
      operationId: plan.operationId,
      requestId,
      phase,
      launcherPid: process.pid,
      childPid: child.pid,
      ...extra,
    });
    child.on("message", (message) => {
      if (
        message?.type !== "reconcile-failed-update" ||
        message.operationId !== plan.operationId ||
        !plan.requestIds.includes(message.requestId)
      )
        return;
      ensure(
        exactKeys(message, ["type", "operationId", "requestId"]) &&
          Number.isSafeInteger(child.pid) &&
          child.pid > 0,
        "observer request differs",
      );
      record(message.requestId + ".request", projection(message.requestId, "request"));
      seen.add(message.requestId);
    });
    const originalSend = child.send;
    child.send = function (message, ...rest) {
      if (
        message?.type !== "update-reconciled" ||
        message.operationId !== plan.operationId ||
        !plan.requestIds.includes(message.requestId)
      )
        return Reflect.apply(originalSend, this, [message, ...rest]);
      const refused = message.result === "refused";
      ensure(
        seen.has(message.requestId) &&
          exactKeys(message, [
            "type",
            "operationId",
            "requestId",
            "result",
            ...(refused ? ["reason"] : []),
          ]) &&
          ["settled", "already-settled", "refused"].includes(message.result) &&
          (!refused ||
            [
              "not-failed",
              "not-found",
              "insufficient-evidence",
              "restore-pending",
              "active-changed",
              "trial-evidence-present",
              "retired-evidence-mismatch",
              "settlement-write-failed",
            ].includes(message.reason)),
        "observer reply differs",
      );
      const callback = rest.at(-1);
      if (typeof callback === "function") {
        const receipt = projection(message.requestId, "reply", {
          result: message.result,
          ...(refused ? { reason: message.reason } : {}),
          sendCallbackCompleted: true,
        });
        rest[rest.length - 1] = function (error) {
          if (error == null) record(message.requestId + ".reply", receipt);
          return Reflect.apply(callback, this, arguments);
        };
      }
      return Reflect.apply(originalSend, this, [message, ...rest]);
    };
    return child;
  };
  syncBuiltinESMExports();
  record("ready", {
    protocol: plan.protocol,
    fixtureId: plan.fixtureId,
    operationId: plan.operationId,
    ...(installing ? {} : { requestIds: plan.requestIds }),
    launcherPid: process.pid,
  });
}
