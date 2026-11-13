"use strict";

const fs = require("node:fs");
const path = require("node:path");
const os = require("node:os");
const F = require("./files.cjs");
const Native = require("./native.cjs");

function privateDirectory(directory, uid) {
  fs.mkdirSync(directory, { recursive: true, mode: 0o700 });
  const st = fs.lstatSync(directory);
  F.insist(
    st.isDirectory() &&
      fs.realpathSync(directory) === directory &&
      st.uid === uid &&
      (st.mode & 0o077) === 0,
    "Control directory must be canonical, private, and owned by the operator",
  );
}
function lockPaths(p, lockDirectory = path.join(os.userInfo().homedir, ".server-swap-locks")) {
  const serviceKey = F.sha256(
    `${p.uid}:${p.service.manager}:${p.service.domain || ""}:${p.service.name}`,
  );
  return [
    path.join(p.home, ".server-swap-lock.sqlite"),
    path.join(lockDirectory, `${serviceKey}.sqlite`),
  ].sort();
}
const ownershipPaths = (p) => lockPaths(p).map((file) => `${file}.owner.json`);
function acquireLocks(p, lockDirectory = path.join(os.userInfo().homedir, ".server-swap-locks")) {
  const { DatabaseSync } = require("node:sqlite");
  const databases = [];
  try {
    privateDirectory(lockDirectory, p.uid);
    for (const file of lockPaths(p, lockDirectory)) {
      try {
        const fd = fs.openSync(file, "wx", 0o600);
        fs.closeSync(fd);
      } catch (error) {
        if (error.code !== "EEXIST") throw error;
      }
      const st = F.regular(file);
      F.insist(
        st.uid === p.uid && (st.mode & 0o077) === 0,
        "Swap lock ownership or permissions differ",
      );
      const db = new DatabaseSync(file);
      databases.push(db);
      db.exec("PRAGMA busy_timeout=0; BEGIN IMMEDIATE;");
    }
  } catch (error) {
    for (const db of databases.reverse()) db.close();
    throw new Error("Exclusive service/state ownership unavailable; no stale locks are removed", {
      cause: error,
    });
  }
  return () => {
    for (const db of databases.reverse()) db.close();
  };
}
const journalPath = (p) => path.join(p.transactionDir, "journal.json");
function journal(p) {
  if (!fs.existsSync(journalPath(p))) return null;
  const state = F.readJson(journalPath(p));
  F.insist(
    state.schema === "server-swap-journal/1" && state.planDigest === p.digest && state.id === p.id,
    "Journal belongs to another exact plan",
  );
  return state;
}
function save(p, state, patch) {
  Object.assign(state, patch, { updatedAt: new Date().toISOString() });
  F.writeJson(journalPath(p), state);
}
function configDigest(p) {
  F.regular(p.service.configPath);
  return F.sha256(fs.readFileSync(p.service.configPath));
}
function backup(p, files) {
  const directory = path.join(p.transactionDir, "backup");
  fs.mkdirSync(directory, { mode: 0o700 });
  const records = [];
  for (const file of files) {
    if (file.absent) {
      records.push({ name: file.name, absent: true });
      continue;
    }
    const destination = path.join(directory, file.name);
    fs.mkdirSync(path.dirname(destination), { recursive: true, mode: 0o700 });
    fs.copyFileSync(file.path, destination, fs.constants.COPYFILE_EXCL);
    fs.chmodSync(destination, 0o600);
    const fd = fs.openSync(destination, "r");
    try {
      fs.fsyncSync(fd);
    } finally {
      fs.closeSync(fd);
    }
    F.syncDirectory(path.dirname(destination));
    records.push({ name: file.name, sha256: F.hashFile(destination), mode: file.mode });
  }
  const serviceConfig = F.checked(p.source.config);
  F.atomic(path.join(directory, "original-service-config"), serviceConfig);
  const receipt = {
    schema: "server-swap-backup/1",
    planDigest: p.digest,
    source: p.source,
    configSha256: p.source.config.sha256,
    records,
  };
  F.writeJson(path.join(directory, "receipt.json"), receipt);
  return receipt;
}
function verifyBackup(p, compareCurrent = false, targetRuntimeState = false) {
  const directory = path.join(p.transactionDir, "backup");
  const receipt = F.readJson(path.join(directory, "receipt.json"));
  F.insist(
    receipt.schema === "server-swap-backup/1" &&
      receipt.planDigest === p.digest &&
      receipt.configSha256 === p.source.config.sha256 &&
      F.canonical(receipt.records.map((f) => f.name)) === F.canonical(p.stateFiles),
    "Backup receipt identity mismatch",
  );
  F.checked({
    path: path.join(directory, "original-service-config"),
    sha256: receipt.configSha256,
  });
  for (const record of receipt.records) {
    if (!record.absent)
      F.verifyArtifact({ path: path.join(directory, record.name), sha256: record.sha256 });
    if (compareCurrent) {
      const current = path.join(p.home, record.name);
      if (p.runtimeState && targetRuntimeState && record.name === "runtime/service-state.json")
        F.verifyArtifact({ path: current, sha256: p.runtimeState.target.sha256 });
      else if (record.absent)
        F.insist(!fs.existsSync(current), "Previously absent state appeared after backup");
      else F.verifyArtifact({ path: current, sha256: record.sha256 });
    }
  }
  return receipt;
}
async function replaceConfig(p, service) {
  F.insist(
    configDigest(p) === p.source.config.sha256,
    "Original service config changed before replacement",
  );
  if (p.service.elevation === "sudo-noninteractive") {
    await service.installConfig();
    F.insist(configDigest(p) === p.target.config.sha256, "Elevated service config readback failed");
    return;
  }
  const st = F.regular(p.service.configPath);
  F.insist(
    st.uid === p.uid && st.nlink === 1,
    "Service config must be a single owned regular file",
  );
  F.atomic(p.service.configPath, F.checked(p.target.config), st.mode & 0o777);
  F.insist(
    configDigest(p) === p.target.config.sha256,
    "Service config replacement readback failed",
  );
}
function createEngine(adapters = {}) {
  const native = { ...Native, ...adapters };
  const verifyHost = adapters.verifyHost || F.verifyHost;
  const lock = adapters.acquireLocks || acquireLocks;
  const ownerPaths = adapters.ownershipPaths || ownershipPaths;
  function assertOwnership(p, allowCommitted = false) {
    for (const file of ownerPaths(p)) {
      if (!fs.existsSync(file)) continue;
      const previous = F.readJson(file);
      F.insist(
        previous.schema === "server-swap-owner/1",
        "Unknown durable service/state owner format",
      );
      const same =
        previous.planDigest === p.digest &&
        previous.id === p.id &&
        previous.transactionDir === p.transactionDir;
      F.insist(
        same || (allowCommitted && previous.status === "committed"),
        `Service/state has an unresolved prior transaction: ${previous.id}; reconcile its exact plan first`,
      );
    }
  }
  function updateOwnership(p, status, allowCommitted = false) {
    assertOwnership(p, allowCommitted);
    for (const file of ownerPaths(p))
      F.writeJson(file, {
        schema: "server-swap-owner/1",
        id: p.id,
        planDigest: p.digest,
        transactionDir: p.transactionDir,
        planPath: p.planPath,
        status,
        updatedAt: new Date().toISOString(),
      });
  }
  function sourceHasNoPendingUpdate(p) {
    const file = path.join(p.home, "runtime/service-state.json");
    if (fs.existsSync(file)) {
      const state = F.readJson(file);
      F.insist(
        [3, 4].includes(state.protocol) &&
          (state.update === undefined ||
            ["committed", "rolled-back", "failed"].includes(state.update?.status)),
        "Existing native update is pending or unknown",
      );
    }
    F.insist(
      !fs.existsSync(path.join(p.home, "runtime/.restart-pending")),
      "Existing native restart intent must be reconciled first",
    );
  }
  function capacity(p) {
    const files = native.inventory(p);
    const bytes = files
      .filter((file) => !file.absent)
      .reduce((sum, file) => sum + BigInt(fs.statSync(file.path).size), 0n);
    const q = F.verifyQualification(p);
    const additional = q.migrations.requiredAdditionalBytes ?? Number(bytes);
    F.insist(
      Number.isSafeInteger(additional) && additional >= 0,
      "Invalid migration storage reserve",
    );
    let directory = p.stateDir;
    while (!fs.existsSync(directory)) directory = path.dirname(directory);
    const stats = fs.statfsSync(directory, { bigint: true });
    const required = bytes + BigInt(additional) + 1024n * 1024n * 1024n;
    F.insist(
      stats.bavail * stats.bsize >= required,
      "Insufficient backup capacity including migration reserve and 1 GiB headroom",
    );
    const homeStats = fs.statfsSync(p.home, { bigint: true });
    F.insist(
      homeStats.bavail * homeStats.bsize >= BigInt(additional) + 1024n * 1024n * 1024n,
      "Insufficient state-home migration capacity",
    );
    return { stateBytes: String(bytes), requiredBackupFreeBytes: String(required) };
  }
  async function inspect(p) {
    verifyHost(p);
    F.verifyPayload(p.source);
    F.verifyPayload(p.target);
    F.verifyQualification(p);
    F.verifyRuntimeState(p);
    await native.validateConfig(p, p.source);
    await native.validateConfig(p, p.target);
    return native.serviceAdapter(p);
  }
  async function plan(file) {
    const p = F.loadPlan(file);
    const service = await inspect(p);
    sourceHasNoPendingUpdate(p);
    if (p.runtimeState)
      F.verifyArtifact({
        path: path.join(p.home, "runtime/service-state.json"),
        sha256: p.runtimeState.source.sha256,
      });
    F.insist(
      configDigest(p) === p.source.config.sha256,
      "Installed source configuration differs from plan",
    );
    const observation = await service.observe();
    await service.detached(observation);
    const source = await native.runtimeIdentity(p, p.source, service);
    return {
      id: p.id,
      planDigest: p.digest,
      status: "ready",
      bindingDigest: F.bindingDigest(p),
      source,
      capacity: capacity(p),
      target: { distribution: p.target.distribution, version: p.target.version },
      transactionDir: p.transactionDir,
    };
  }
  async function status(file) {
    const p = F.loadPlan(file);
    verifyHost(p);
    const state = journal(p);
    if (state?.phase === "committed" && state.status === "committed")
      return {
        ...state,
        status: "committed-unverified",
        historicalStatus: "committed",
        verification: "historical",
      };
    return state || { id: p.id, planDigest: p.digest, status: "not-started", phase: "not-started" };
  }
  async function effect(p, state, name, nextPhase, run) {
    save(p, state, { status: "in-progress", pending: name });
    await run();
    save(p, state, { pending: null, phase: nextPhase });
  }
  async function finish(p, state, service) {
    F.insist(configDigest(p) === p.target.config.sha256, "Target service config drifted");
    F.verifyQualification(p);
    const identity = await native.runtimeIdentity(p, p.target, service);
    verifyBackup(p);
    save(p, state, {
      status: "committed",
      phase: "committed",
      pending: null,
      identity,
      verification: "current",
      verifiedAt: new Date().toISOString(),
    });
    updateOwnership(p, "committed");
    return state;
  }
  async function advance(p, state, service) {
    F.insist(!state.pending, "Interrupted effect must be reconciled before resume");
    if (state.status === "committed") return state;
    if (
      state.status === "failed-before-start" &&
      ["prepared", "stopped", "backed-up", "runtime-configured", "configured", "loaded"].includes(
        state.phase,
      )
    )
      save(p, state, { status: "in-progress", error: null });
    F.insist(
      state.status === "in-progress",
      "Terminal or uncertain transaction cannot be replayed",
    );
    if (state.phase === "prepared") {
      capacity(p);
      sourceHasNoPendingUpdate(p);
      await native.validateConfig(p, p.source);
      await native.validateConfig(p, p.target);
      if (p.runtimeState)
        F.verifyArtifact({
          path: path.join(p.home, "runtime/service-state.json"),
          sha256: p.runtimeState.source.sha256,
        });
      F.insist(configDigest(p) === p.source.config.sha256, "Original config drifted");
      await native.runtimeIdentity(p, p.source, service);
      await service.detached(await service.observe());
      await effect(p, state, "stop", "stopped", async () => {
        await service.stop();
        await native.quiescence(p, service);
      });
    }
    if (state.phase === "stopped") {
      const files = await native.quiescence(p, service);
      await effect(p, state, "backup", "backed-up", async () => {
        backup(p, files);
        const after = await native.quiescence(p, service);
        F.insist(
          F.canonical(after) === F.canonical(files),
          "State changed while copying stopped SQLite and WAL",
        );
        verifyBackup(p, true);
      });
    }
    if (state.phase === "backed-up" && p.runtimeState) {
      await native.quiescence(p, service);
      verifyBackup(p, true);
      F.verifyRuntimeState(p);
      await effect(p, state, "install-runtime-state", "runtime-configured", () => {
        const file = path.join(p.home, "runtime/service-state.json");
        F.verifyArtifact({ path: file, sha256: p.runtimeState.source.sha256 });
        F.atomic(file, F.checked(p.runtimeState.target));
        F.verifyArtifact({ path: file, sha256: p.runtimeState.target.sha256 });
      });
    }
    if (state.phase === "backed-up" || state.phase === "runtime-configured") {
      await native.quiescence(p, service);
      verifyBackup(p, true, state.phase === "runtime-configured");
      F.verifyPayload(p.target);
      F.verifyQualification(p);
      await native.validateConfig(p, p.source);
      await native.validateConfig(p, p.target);
      await effect(p, state, "install-config", "configured", () => replaceConfig(p, service));
    }
    if (state.phase === "configured") {
      F.insist(configDigest(p) === p.target.config.sha256, "Target config drifted before reload");
      await native.quiescence(p, service);
      verifyBackup(p, true, Boolean(p.runtimeState));
      await effect(p, state, "reload", "loaded", () => service.reload());
    }
    if (state.phase === "loaded") {
      await native.quiescence(p, service);
      verifyBackup(p, true, Boolean(p.runtimeState));
      F.verifyPayload(p.target);
      F.verifyQualification(p);
      await native.validateConfig(p, p.target);
      F.insist(configDigest(p) === p.target.config.sha256, "Target config drifted before start");
      const observed = await service.observe();
      F.insist(!observed.reloadNeeded, "Manager has not loaded the target config");
      // A durable start intent is the irreversible database compatibility boundary.
      await effect(p, state, "start", "target-started", () => service.start());
    }
    if (state.phase === "target-started") {
      const deadline = Date.now() + p.timeoutMs;
      let last;
      do {
        try {
          return await finish(p, state, service);
        } catch (error) {
          last = error;
        }
        await new Promise((resolve) => setTimeout(resolve, 250));
      } while (Date.now() < deadline);
      save(p, state, { status: "recovery-required", pending: null, error: last.message });
    }
    return state;
  }
  async function mutate(file, operation) {
    const p = F.loadPlan(file);
    verifyHost(p);
    const release = lock(p);
    let state;
    try {
      privateDirectory(p.controlDir, p.uid);
      privateDirectory(p.transactionDir, p.uid);
      state = journal(p);
      assertOwnership(p, operation === "apply" && !state);
      if (state && operation !== "apply") updateOwnership(p, state.status);
      const service = await inspect(p);
      if (operation === "apply") {
        F.insist(!state, "Transaction already exists; use status, resume, or reconcile");
        F.insist(
          configDigest(p) === p.source.config.sha256,
          "Installed source configuration differs",
        );
        await service.detached(await service.observe());
        const sourceIdentity = await native.runtimeIdentity(p, p.source, service);
        F.atomic(path.join(p.transactionDir, "plan.json"), fs.readFileSync(file));
        state = {
          schema: "server-swap-journal/1",
          id: p.id,
          planDigest: p.digest,
          status: "in-progress",
          phase: "prepared",
          pending: null,
          sourceIdentity,
          createdAt: new Date().toISOString(),
        };
        save(p, state, {});
        updateOwnership(p, "in-progress", true);
      } else F.insist(state, "No transaction exists");
      if (operation === "reconcile" || state.phase === "committed")
        return await reconcileState(p, state, service);
      return await advance(p, state, service);
    } catch (error) {
      if (state?.phase === "committed" && operation !== "apply") {
        save(p, state, {
          status: "unknown",
          historicalStatus: "committed",
          verification: "failed",
          error: error.message,
        });
        try {
          updateOwnership(p, "unknown");
        } catch {
          /* A newer transaction retains its durable ownership. */
        }
        return state;
      }
      if (state && state.status !== "committed") {
        const mayHaveStarted =
          state.pending === "start" || ["target-started", "committed"].includes(state.phase);
        save(p, state, {
          status: mayHaveStarted
            ? "recovery-required"
            : state.pending
              ? "unknown"
              : "failed-before-start",
          error: error.message,
        });
        try {
          updateOwnership(p, state.status);
        } catch {
          /* The existing foreign owner remains authoritative. */
        }
      }
      throw error;
    } finally {
      release();
    }
  }
  async function reconcileState(p, state, service) {
    if (state.phase === "committed") {
      try {
        return await finish(p, state, service);
      } catch (error) {
        save(p, state, {
          status: "unknown",
          historicalStatus: "committed",
          verification: "failed",
          error: error.message,
        });
        updateOwnership(p, "unknown");
        return state;
      }
    }
    if (state.pending === "start" || state.phase === "target-started") {
      try {
        return await finish(p, state, service);
      } catch (error) {
        save(p, state, { status: "recovery-required", error: error.message });
        return state;
      }
    }
    if (!state.pending) return state;
    await native.quiescence(p, service);
    let next;
    if (state.pending === "stop") {
      F.insist(
        configDigest(p) === p.source.config.sha256,
        "Config changed during interrupted stop",
      );
      next = "stopped";
    } else if (state.pending === "backup") {
      verifyBackup(p, true);
      next = "backed-up";
    } else if (state.pending === "install-runtime-state") {
      const digest = F.hashFile(path.join(p.home, "runtime/service-state.json"));
      F.insist(
        [p.runtimeState.source.sha256, p.runtimeState.target.sha256].includes(digest),
        "Interrupted runtime metadata has unknown contents",
      );
      const target = digest === p.runtimeState.target.sha256;
      verifyBackup(p, true, target);
      next = target ? "runtime-configured" : "backed-up";
    } else if (state.pending === "install-config") {
      verifyBackup(p, true, Boolean(p.runtimeState));
      const digest = configDigest(p);
      F.insist(
        [p.source.config.sha256, p.target.config.sha256].includes(digest),
        "Interrupted config replacement has unknown content",
      );
      next =
        digest === p.target.config.sha256
          ? "configured"
          : p.runtimeState
            ? "runtime-configured"
            : "backed-up";
    } else if (state.pending === "reload") {
      F.insist(
        configDigest(p) === p.target.config.sha256 && !(await service.observe()).reloadNeeded,
        "Reload outcome is unknown",
      );
      verifyBackup(p, true, Boolean(p.runtimeState));
      next = "loaded";
    } else throw new Error("Unknown journal effect; no replay allowed");
    save(p, state, { status: "in-progress", phase: next, pending: null, error: null });
    return state;
  }
  return {
    plan,
    status,
    apply: (file) => mutate(file, "apply"),
    resume: (file) => mutate(file, "resume"),
    reconcile: (file) => mutate(file, "reconcile"),
  };
}
module.exports = {
  ...createEngine(),
  loadPlan: F.loadPlan,
  bindingDigest: F.bindingDigest,
  createEngine,
  acquireLocks,
  ownershipPaths,
  verifyBackup,
};
