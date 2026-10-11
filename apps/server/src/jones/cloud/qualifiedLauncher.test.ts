// @effect-diagnostics nodeBuiltinImport:off
import { assert, it } from "@effect/vitest";
import * as NodeAssert from "node:assert/strict";
import * as NodeFSP from "node:fs/promises";
import * as NodeOS from "node:os";
import * as NodePath from "node:path";
import * as NodeSqlite from "node:sqlite";
import packageJson from "../../../package.json" with { type: "json" };
import { Launcher, main, readServiceState, writeServiceState } from "../../serviceLauncher.ts";
import { readQualifiedBackupReceipt, type QualifiedBackupAdapter } from "./qualifiedBackup.ts";
import type { QualifiedQuiescenceAdapter } from "./qualifiedQuiescence.ts";
import { QUALIFIED_STARTUP_STATE_FILES } from "./qualifiedQuiescence.ts";
import { archiveUpdateOperation, reserveUpdateOperation } from "../updates/launcherOperation.ts";
import {
  currentQualifiedRuntimeBinding,
  qualifiedPayloadDigest,
  stageQualifiedRuntime,
  QUALIFIED_RUNTIME_RECEIPT,
  type QualifiedRuntimeReceipt,
} from "./qualifiedRuntime.ts";

const baseline = "0.0.0-preview.20261002.100";
const target = "0.0.0-preview.20261002.101.1";
// oxlint-disable-next-line t3code/no-global-process-runtime -- These controlled launcher executables must bind the actual native fixture host.
const fixturePlatform = NodeOS.platform() === "darwin" ? "darwin" : "linux";
const readMarker = (file: string) => {
  const database = new NodeSqlite.DatabaseSync(file, { readOnly: true });
  try {
    return database.prepare("SELECT value FROM marker").get()?.value;
  } finally {
    database.close();
  }
};

async function runScenario(
  mode: "commit" | "rollback" | "stale-child" | "missing-gate" | "wrong-gate" | "blocked-candidate",
  body: (base: string, launcherFailure: unknown) => Promise<void>,
  quiescenceAdapter: QualifiedQuiescenceAdapter = { scan: async () => [] },
  onLauncher?: (launcher: Launcher) => void,
  options: {
    readonly backupAdapter?: QualifiedBackupAdapter;
    readonly productionMain?: boolean;
    readonly startupGateProtocol?: 1 | "missing";
    readonly operationFailure?: "archive" | "reserve";
  } = {},
) {
  const allocated = await NodeFSP.mkdtemp(NodePath.join(NodeOS.tmpdir(), "jones-launcher-test-"));
  try {
    const base = await NodeFSP.realpath(allocated);
    const userdata = NodePath.join(base, "userdata");
    await NodeFSP.mkdir(userdata, { mode: 0o700 });
    const dbPath = NodePath.join(userdata, "statev2.sqlite");
    const database = new NodeSqlite.DatabaseSync(dbPath);
    database.exec("CREATE TABLE marker(value TEXT); INSERT INTO marker VALUES ('before');");
    database.close();
    await NodeFSP.writeFile(
      NodePath.join(userdata, "environment-id"),
      "synthetic-jones-environment",
    );
    await NodeFSP.writeFile(NodePath.join(userdata, "settings.json"), "before-settings");
    const child = `#!${process.execPath}
import { readFileSync, writeFileSync } from "node:fs";
import * as NodeSqlite from "node:sqlite";
import * as NodeNet from "node:net";
import { makeQualifiedTrialReceipt, reserveQualifiedResume, sameQualifiedTrialIdentity } from "${NodePath.join(import.meta.dirname, "qualifiedStartup.ts")}";
if (process.argv.includes("__service-preflight")) {
  if (${JSON.stringify(mode)} === "blocked-candidate") {
    console.log(JSON.stringify({status:"blocked",version:${JSON.stringify(target)},reason:"candidate-older-than-database"}));
    process.exit(1);
  }
  console.log(JSON.stringify({status:"ready",version:${JSON.stringify(target)},launcherProtocol:4,
    ...(${JSON.stringify(mode)} === "missing-gate" ? {} : {startupGateProtocol:${JSON.stringify(mode)} === "wrong-gate" ? 2 : 1})}));
  process.exit(0);
}
const context = JSON.parse(process.env.T3_SERVICE_LAUNCHER_CONTEXT);
if (context.update?.status === "pending") {
  const db = new NodeSqlite.DatabaseSync(${JSON.stringify(dbPath)});
  db.exec("UPDATE marker SET value='after'"); db.close();
  writeFileSync(${JSON.stringify(NodePath.join(userdata, "settings.json"))}, "after-settings");
  writeFileSync(${JSON.stringify(NodePath.join(userdata, "keybindings.json"))}, "created-by-trial");
  if (${JSON.stringify(mode)} === "rollback") process.exit(1);
  const server = NodeNet.createServer();
  await new Promise(resolve => server.listen(0, "127.0.0.1", resolve));
  const socket = server.address();
  const receipt = await makeQualifiedTrialReceipt({
    updateId: context.update.id, qualified: context.update.qualified,
    witness: {
      home: process.env.T3CODE_HOME, databasePath: ${JSON.stringify(dbPath)},
      serviceUserdata: ${JSON.stringify(userdata)},
      environmentId: readFileSync(${JSON.stringify(NodePath.join(userdata, "environment-id"))}, "utf8").trim(),
      version: ${JSON.stringify(target)},
      buildMetadata: {jonesSource: {repository:"Jones-Systems/Jones-Code", sha:${JSON.stringify("a".repeat(40))}, tree:${JSON.stringify("b".repeat(40))}}},
      processId: process.pid,
      listener: {_tag:"InetAddressV4",address:{toString:()=>socket.address},port:socket.port},
    },
  });
  if (${JSON.stringify(mode)} === "stale-child") receipt.processId += 1;
  process.on("message", async m => {
    if (m.type !== "committed") return;
    if (m.startupGateProtocol !== 1 || m.qualified?.generation !== context.update.id ||
      !sameQualifiedTrialIdentity(receipt, m.qualified)) process.exit(2);
    writeFileSync(${JSON.stringify(NodePath.join(base, "runtime", "observed-trial-launcher.json"))}, JSON.stringify({
      receipt: JSON.parse(readFileSync(${JSON.stringify(NodePath.join(base, "runtime", "jones-launcher-capability.json"))}, "utf8")), childPid:process.pid,
    }));
    await reserveQualifiedResume({receipt});
    process.exit(0);
  });
  process.send({type:"prepared", updateId:context.update.id, startupGateProtocol:1, qualified:receipt});
} else if (context.update === undefined) {
  const handle = readFileSync(${JSON.stringify(NodePath.join(base, "runtime", "test-handle"))}, "utf8");
  process.on("message", m => {if (m.type === "update-accepted") {
    writeFileSync(${JSON.stringify(NodePath.join(base, "runtime", "observed-active-launcher.json"))}, JSON.stringify({
      receipt: JSON.parse(readFileSync(${JSON.stringify(NodePath.join(base, "runtime", "jones-launcher-capability.json"))}, "utf8")), childPid:process.pid,
    }));
  } else if (m.type === "update-rejected") {
    writeFileSync(${JSON.stringify(NodePath.join(base, "runtime", "rejected.json"))}, JSON.stringify(m));
    setImmediate(() => {
      writeFileSync(${JSON.stringify(NodePath.join(base, "runtime", "old-child-survived.json"))}, JSON.stringify({pid:process.pid, context}));
      process.exit(0);
    });
  }});
  process.send({type:"request-update", targetVersion:${JSON.stringify(target)}, dbPath:${JSON.stringify(dbPath)}, stagedHandle:handle,
    ...(${JSON.stringify(options.operationFailure !== undefined)} ? {operationId:"12345678-1234-4234-8234-123456789abc"} : {})});
  setInterval(() => {}, 1000);
} else {
  writeFileSync(${JSON.stringify(NodePath.join(base, "runtime", "restart-context.json"))}, JSON.stringify(context));
  process.exit(0);
}
`;
    const active = NodePath.join(base, "runtime", "versions", baseline);
    const payload = NodePath.join(base, "payload");
    for (const directory of [active, payload]) {
      await NodeFSP.mkdir(directory, { recursive: true, mode: 0o700 });
      await NodeFSP.writeFile(NodePath.join(directory, "t3"), child, { mode: 0o755 });
    }
    const metadata: Omit<QualifiedRuntimeReceipt, "payloadSha256"> = {
      protocol: 1,
      repository: "Jones-Systems/Jones-Code",
      channel: "jones-main",
      version: baseline,
      sourceSha: "c".repeat(40),
      sourceTree: "b".repeat(40),
      installedSourceSha: "d".repeat(40),
      workflow:
        fixturePlatform === "darwin"
          ? ".github/workflows/artifact-desktop-mac.yml"
          : ".github/workflows/artifact-cli-linux.yml",
      runId: 100,
      runAttempt: 1,
      artifactId: 99,
      artifactDigest: `sha256:${"e".repeat(64)}`,
      archiveSha256: "f".repeat(64),
      platform: fixturePlatform,
      // oxlint-disable-next-line t3code/no-global-process-runtime -- Standalone launcher fixture must match the native host running its controlled executable.
      architecture: NodeOS.arch() === "arm64" ? "arm64" : "x64",
    };
    await NodeFSP.writeFile(
      NodePath.join(active, QUALIFIED_RUNTIME_RECEIPT),
      JSON.stringify({ ...metadata, payloadSha256: await qualifiedPayloadDigest(active) }),
      { mode: 0o600 },
    );
    await NodeFSP.writeFile(NodePath.join(active, ".install-complete"), baseline);
    const { protocol: _protocol, ...candidate } = metadata;
    const staged = await stageQualifiedRuntime({
      artifact: {
        ...candidate,
        version: target,
        sourceSha: "a".repeat(40),
        installedSourceSha: metadata.sourceSha,
        runId: 101,
        payloadDirectory: payload,
      },
      binding: await currentQualifiedRuntimeBinding(base, baseline),
      validate: async () => {},
    });
    await NodeFSP.writeFile(NodePath.join(base, "runtime", "test-handle"), staged.stagedHandle);
    const statePath = NodePath.join(base, "runtime", "service-state.json");
    await writeServiceState(statePath, { protocol: 4, activeVersion: baseline });
    const launcher = new Launcher(base, await readServiceState(statePath), {
      // These controlled lifecycle fixtures isolate native process observation;
      // qualifiedQuiescence tests cover refusal and exact-file writer proof.
      quiescenceAdapter,
      ...(options.startupGateProtocol === "missing" ? {} : { startupGateProtocol: 1 as const }),
      ...(options.backupAdapter === undefined ? {} : { backupAdapter: options.backupAdapter }),
      ...(options.operationFailure === undefined
        ? {}
        : {
            updateOperationIO: {
              archive:
                options.operationFailure === "archive"
                  ? async () => {
                      throw Object.assign(new Error("fixture archive EIO"), { code: "EIO" });
                    }
                  : archiveUpdateOperation,
              reserve:
                options.operationFailure === "reserve"
                  ? async (...args: Parameters<typeof reserveUpdateOperation>) => {
                      // A receipt can exist even though its final durability step failed.
                      await reserveUpdateOperation(...args);
                      throw Object.assign(new Error("fixture reserve fsync EIO"), { code: "EIO" });
                    }
                  : reserveUpdateOperation,
            },
          }),
    });
    onLauncher?.(launcher);
    // Captured children exit or are reaped before the launcher settles; cleanup never polls by process name.
    const previousHome = process.env.T3CODE_HOME;
    let launcherFailure: unknown;
    try {
      if (options.productionMain) process.env.T3CODE_HOME = base;
      await (options.productionMain ? main({ quiescenceAdapter }) : launcher.run()).then(
        () => {
          throw new Error("Unexpected launcher completion");
        },
        (cause: unknown) => {
          launcherFailure = cause;
        },
      );
    } finally {
      if (options.productionMain) {
        if (previousHome === undefined) delete process.env.T3CODE_HOME;
        else process.env.T3CODE_HOME = previousHome;
      }
    }
    await body(base, launcherFailure);
  } finally {
    await NodeFSP.rm(allocated, { recursive: true, force: true });
    await NodeAssert.rejects(NodeFSP.lstat(allocated), { code: "ENOENT" });
  }
}

it.each(["archive", "reserve"] as const)(
  "keeps the active child and pointer unchanged when operation %s fails before acceptance",
  async (operationFailure) => {
    await runScenario(
      "commit",
      async (base, failure) => {
        const rejected = JSON.parse(
          await NodeFSP.readFile(NodePath.join(base, "runtime", "rejected.json"), "utf8"),
        );
        assert.match(rejected.reason, /^operation-reconciliation-required:/);
        assert.equal(rejected.operationId, "12345678-1234-4234-8234-123456789abc");
        const survived = JSON.parse(
          await NodeFSP.readFile(NodePath.join(base, "runtime", "old-child-survived.json"), "utf8"),
        );
        assert.equal(survived.context.childVersion, baseline);
        assert.isAbove(survived.pid, 0);
        assert.match(String(failure), /exited unexpectedly/);
        assert.deepEqual(
          await readServiceState(NodePath.join(base, "runtime", "service-state.json")),
          { protocol: 4, activeVersion: baseline },
        );
        assert.equal(readMarker(NodePath.join(base, "userdata", "statev2.sqlite")), "before");
        await NodeAssert.rejects(
          NodeFSP.access(NodePath.join(base, "runtime", "observed-active-launcher.json")),
          { code: "ENOENT" },
        );
        assert.deepEqual(await NodeFSP.readdir(NodePath.join(base, "runtime", "db-backup")), []);
        const reserved = NodePath.join(
          base,
          "runtime",
          "jones-update-operations",
          `${rejected.operationId}.json`,
        );
        if (operationFailure === "reserve") await NodeFSP.access(reserved);
        else await NodeAssert.rejects(NodeFSP.access(reserved), { code: "ENOENT" });
      },
      undefined,
      undefined,
      { operationFailure },
    );
  },
);

it("commits a qualified trial after readiness and retains its previous binary/state pair", async () => {
  await runScenario("commit", async (base) => {
    for (const [role, childVersion] of [
      ["active", baseline],
      ["trial", target],
    ] as const) {
      const observed = JSON.parse(
        await NodeFSP.readFile(
          NodePath.join(base, "runtime", `observed-${role}-launcher.json`),
          "utf8",
        ),
      );
      assert.deepEqual(observed.receipt, {
        schema: 1,
        baseDir: base,
        launcherVersion: packageJson.version,
        launcherPid: process.pid,
        launcherProtocol: 4,
        qualifiedUpdatesProtocol: 1,
        startupGateProtocol: 1,
        childPid: observed.childPid,
        childVersion,
      });
    }
    await NodeAssert.rejects(
      NodeFSP.access(NodePath.join(base, "runtime", "jones-launcher-capability.json")),
    );
    const state = await readServiceState(NodePath.join(base, "runtime", "service-state.json"));
    assert.equal(state.activeVersion, target);
    assert.equal(state.update?.status, "committed");
    assert.equal(state.update?.startupReceipt?.startupGateProtocol, 1);
    const backup = NodePath.join(base, "runtime", "db-backup", state.update!.id);
    assert.equal(readMarker(NodePath.join(backup, "database")), "before");
    assert.equal(
      await NodeFSP.readFile(NodePath.join(backup, "settings.json"), "utf8"),
      "before-settings",
    );
    assert.equal(readMarker(NodePath.join(base, "userdata", "statev2.sqlite")), "after");
    await NodeFSP.access(NodePath.join(base, "runtime", "versions", baseline, "t3"));
    for (const directory of [
      NodePath.join(base, "runtime"),
      NodePath.join(base, "runtime", "db-backup"),
      backup,
    ]) {
      const stat = await NodeFSP.lstat(directory);
      assert.equal(stat.isDirectory(), true);
      assert.equal(stat.isSymbolicLink(), false);
      assert.equal(stat.mode & 0o777, 0o700);
    }
    const reservation = NodePath.join(backup, "resume-dispatched.json");
    assert.deepEqual(
      JSON.parse(await NodeFSP.readFile(reservation, "utf8")),
      state.update!.startupReceipt,
    );
    const statePath = NodePath.join(base, "runtime", "service-state.json");
    const beforeRecovery = await NodeFSP.readFile(statePath, "utf8");
    const beforeReservation = await NodeFSP.readFile(reservation, "utf8");
    await NodeAssert.rejects(
      new Launcher(base, state, {
        quiescenceAdapter: { scan: async () => [] },
        startupGateProtocol: 1,
      }).run(),
      /Active child exited unexpectedly/,
    );
    assert.equal(await NodeFSP.readFile(statePath, "utf8"), beforeRecovery);
    assert.equal(await NodeFSP.readFile(reservation, "utf8"), beforeReservation);
    const restarted = JSON.parse(
      await NodeFSP.readFile(NodePath.join(base, "runtime", "restart-context.json"), "utf8"),
    );
    assert.equal(restarted.childVersion, target);
    assert.equal(restarted.update.status, "committed");
    assert.equal(restarted.startupGateProtocol, 1);
  });
});

it("holds a stale PID proof until restart proves writers stopped and restores the previous pair", async () => {
  await runScenario("stale-child", async (base) => {
    const state = await readServiceState(NodePath.join(base, "runtime", "service-state.json"));
    assert.equal(state.activeVersion, baseline);
    assert.equal(state.update?.status, "pending");
    assert.equal(state.update?.startupReceipt, undefined);
    const backup = NodePath.join(base, "runtime", "db-backup", state.update!.id);
    assert.equal(readMarker(NodePath.join(backup, "database")), "before");
    assert.equal(readMarker(NodePath.join(base, "userdata", "statev2.sqlite")), "after");
    await NodeAssert.rejects(NodeFSP.access(NodePath.join(backup, "resume-dispatched.json")));
    const statePath = NodePath.join(base, "runtime", "service-state.json");
    const beforeRecovery = await NodeFSP.readFile(statePath, "utf8");
    await NodeAssert.rejects(
      new Launcher(base, state, {
        quiescenceAdapter: { scan: async ({ files }) => [{ pid: 999, path: files[0]!.path }] },
        startupGateProtocol: 1,
      }).run(),
      /writer-active/,
    );
    assert.equal(await NodeFSP.readFile(statePath, "utf8"), beforeRecovery);
    assert.equal(readMarker(NodePath.join(base, "userdata", "statev2.sqlite")), "after");
    await NodeAssert.rejects(
      new Launcher(base, state, {
        quiescenceAdapter: { scan: async () => [] },
        startupGateProtocol: 1,
      }).run(),
      /Active child exited unexpectedly/,
    );
    const recovered = await readServiceState(statePath);
    assert.equal(recovered.activeVersion, baseline);
    assert.equal(recovered.update?.status, "rolled-back");
    assert.equal(
      recovered.update?.status === "pending" ? undefined : recovered.update?.reason,
      "launcher-restarted",
    );
    assert.equal(readMarker(NodePath.join(base, "userdata", "statev2.sqlite")), "before");
    assert.equal(readMarker(NodePath.join(backup, "advanced-state", "database")), "after");
    await NodeAssert.rejects(NodeFSP.access(NodePath.join(backup, "resume-dispatched.json")));
  });
});

it("rejects insufficient copy capacity before accepting Install or stopping the active child", async () => {
  await runScenario(
    "commit",
    async (base) => {
      const state = await readServiceState(NodePath.join(base, "runtime", "service-state.json"));
      assert.equal(state.update, undefined);
      assert.equal(state.activeVersion, baseline);
      const rejected = JSON.parse(
        await NodeFSP.readFile(NodePath.join(base, "runtime", "rejected.json"), "utf8"),
      );
      assert.match(rejected.reason, /recovery-capacity/);
      assert.equal(readMarker(NodePath.join(base, "userdata", "statev2.sqlite")), "before");
      assert.deepEqual(await NodeFSP.readdir(NodePath.join(base, "runtime", "db-backup")), []);
    },
    { scan: async () => [] },
    undefined,
    {
      backupAdapter: {
        availableBytes: async () => 0,
        copyFile: async () => {
          throw Object.assign(new Error("synthetic unsupported clone"), { code: "ENOTSUP" });
        },
      },
    },
  );
});

it("binds the qualified startup gate through production main", async () => {
  let scans = 0;
  await runScenario(
    "commit",
    async (base, launcherFailure) => {
      const state = await readServiceState(NodePath.join(base, "runtime", "service-state.json"));
      assert.equal(
        state.activeVersion,
        target,
        `Launcher failure: ${String(launcherFailure)}; state: ${JSON.stringify(state)}`,
      );
      assert.equal(state.update?.status, "committed");
      assert.ok(scans > 0, "Production main must use the supplied fixture process observer.");
    },
    {
      scan: async () => {
        scans++;
        return [];
      },
    },
    undefined,
    { productionMain: true },
  );
});

it("rejects a staged Install when the launcher's startup gate is absent", async () => {
  await runScenario(
    "commit",
    async (base) => {
      const state = await readServiceState(NodePath.join(base, "runtime", "service-state.json"));
      assert.equal(state.update, undefined);
      const rejected = JSON.parse(
        await NodeFSP.readFile(NodePath.join(base, "runtime", "rejected.json"), "utf8"),
      );
      assert.match(rejected.reason, /bootstrap-required/);
    },
    { scan: async () => [] },
    undefined,
    { startupGateProtocol: "missing" },
  );
});

it("reports a candidate's bound migration refusal before accepting or stopping the server", async () => {
  await runScenario("blocked-candidate", async (base) => {
    const state = await readServiceState(NodePath.join(base, "runtime", "service-state.json"));
    assert.equal(state.update, undefined);
    assert.equal(state.activeVersion, baseline);
    const rejected = JSON.parse(
      await NodeFSP.readFile(NodePath.join(base, "runtime", "rejected.json"), "utf8"),
    );
    assert.equal(rejected.reason, "candidate-older-than-database");
    assert.equal(readMarker(NodePath.join(base, "userdata", "statev2.sqlite")), "before");
    await NodeAssert.rejects(NodeFSP.access(NodePath.join(base, "runtime", "db-backup")));
  });
});

it("resumes an interrupted rollback without starting a candidate or recapturing restored files", async () => {
  await runScenario("stale-child", async (base) => {
    const statePath = NodePath.join(base, "runtime", "service-state.json");
    const state = await readServiceState(statePath);
    const backup = NodePath.join(base, "runtime", "db-backup", state.update!.id);
    const advanced = NodePath.join(backup, "advanced-state");
    const database = NodePath.join(base, "userdata", "statev2.sqlite");
    await NodeFSP.mkdir(advanced, { mode: 0o700 });
    const names = ["database", ...QUALIFIED_STARTUP_STATE_FILES];
    const entries = [];
    for (const name of names) {
      const source = name === "database" ? database : NodePath.join(base, "userdata", name);
      const stat = await NodeFSP.stat(source, { bigint: true }).catch(
        (cause: NodeJS.ErrnoException) => {
          if (cause.code === "ENOENT") return undefined;
          throw cause;
        },
      );
      if (stat !== undefined)
        entries.push({ name, device: stat.dev.toString(), inode: stat.ino.toString() });
    }
    await NodeFSP.writeFile(
      NodePath.join(advanced, "rename-journal.json"),
      JSON.stringify(entries),
    );
    await NodeFSP.writeFile(NodePath.join(backup, ".restore-pending"), "");
    await NodeFSP.rename(database, NodePath.join(advanced, "database"));
    await NodeFSP.copyFile(NodePath.join(backup, "database"), database);
    await NodeAssert.rejects(
      new Launcher(base, state, {
        quiescenceAdapter: { scan: async () => [] },
        startupGateProtocol: 1,
      }).run(),
      /Active child exited unexpectedly/,
    );
    const recovered = await readServiceState(statePath);
    assert.equal(recovered.update?.status, "rolled-back");
    assert.equal(readMarker(database), "before");
    assert.equal(readMarker(NodePath.join(advanced, "database")), "after");
    assert.equal(
      (await NodeFSP.stat(NodePath.join(advanced, "database"), { bigint: true })).ino.toString(),
      entries[0]!.inode,
    );
    assert.equal(
      await NodeFSP.readFile(NodePath.join(base, "userdata", "settings.json"), "utf8"),
      "before-settings",
    );
    assert.equal(
      await NodeFSP.readFile(NodePath.join(advanced, "settings.json"), "utf8"),
      "after-settings",
    );
  });
});

it("records stop intent during readiness proof before committing or granting qualified resume", async () => {
  let captured: Launcher | undefined;
  let stop: Promise<void> | undefined;
  await runScenario(
    "commit",
    async (base) => {
      await stop;
      const state = await readServiceState(NodePath.join(base, "runtime", "service-state.json"));
      assert.equal(state.activeVersion, baseline);
      assert.equal(state.update?.status, "pending");
      assert.equal(state.update?.startupReceipt, undefined);
      await NodeFSP.access(NodePath.join(base, "runtime", ".service-stopping"));
      await NodeAssert.rejects(
        NodeFSP.access(
          NodePath.join(base, "runtime", "db-backup", state.update!.id, "resume-dispatched.json"),
        ),
      );
    },
    {
      scan: async ({ allowedProcessIds }) => {
        if (allowedProcessIds.length > 0) {
          if (captured === undefined) throw new Error("missing controlled launcher");
          stop = captured.stop("SIGTERM");
        }
        return [];
      },
    },
    (launcher) => {
      captured = launcher;
    },
  );
});

it("restores paired settings and SQLite after a failed trial and retains its advanced state", async () => {
  await runScenario("rollback", async (base) => {
    const state = await readServiceState(NodePath.join(base, "runtime", "service-state.json"));
    assert.equal(state.activeVersion, baseline);
    assert.equal(state.update?.status, "rolled-back");
    const backup = NodePath.join(base, "runtime", "db-backup", state.update!.id);
    const recovery = await readQualifiedBackupReceipt(base, state.update!.id);
    assert.isDefined(recovery);
    assert.isTrue(recovery!.method === "clone" || recovery!.method === "copy");
    assert.isAbove(recovery!.bytes, 0);
    assert.equal(readMarker(NodePath.join(base, "userdata", "statev2.sqlite")), "before");
    assert.equal(
      await NodeFSP.readFile(NodePath.join(base, "userdata", "settings.json"), "utf8"),
      "before-settings",
    );
    assert.equal(
      await NodeFSP.lstat(NodePath.join(base, "userdata", "keybindings.json")).then(
        () => true,
        () => false,
      ),
      false,
    );
    assert.equal(readMarker(NodePath.join(backup, "advanced-state", "database")), "after");
    assert.equal(
      await NodeFSP.readFile(NodePath.join(backup, "advanced-state", "settings.json"), "utf8"),
      "after-settings",
    );
  });
});

it("retains the active pointer and unchanged state when an unknown same-home writer blocks explicit Install", async () => {
  await runScenario(
    "commit",
    async (base) => {
      const state = await readServiceState(NodePath.join(base, "runtime", "service-state.json"));
      assert.equal(state.activeVersion, baseline);
      assert.equal(state.update?.status, "pending");
      assert.equal(readMarker(NodePath.join(base, "userdata", "statev2.sqlite")), "before");
      assert.equal(
        await NodeFSP.readFile(NodePath.join(base, "userdata", "settings.json"), "utf8"),
        "before-settings",
      );
      await NodeAssert.rejects(
        NodeFSP.access(NodePath.join(base, "runtime", "db-backup", state.update!.id)),
      );
      await NodeAssert.rejects(
        new Launcher(base, state, {
          quiescenceAdapter: { scan: async () => [] },
          startupGateProtocol: 1,
        }).run(),
        /Active child exited unexpectedly/,
      );
      const recovered = await readServiceState(
        NodePath.join(base, "runtime", "service-state.json"),
      );
      assert.equal(recovered.activeVersion, baseline);
      assert.equal(recovered.update?.status, "failed");
      assert.equal(
        recovered.update?.status === "pending" ? undefined : recovered.update?.reason,
        "launcher-restarted",
      );
      assert.equal(readMarker(NodePath.join(base, "userdata", "statev2.sqlite")), "before");
    },
    { scan: async ({ files }) => [{ pid: 999, path: files[0]!.path }] },
  );
});

it.each(["missing-gate", "wrong-gate"] as const)(
  "rejects candidate %s before recording or launching a qualified transaction",
  async (mode) => {
    await runScenario(mode, async (base) => {
      const state = await readServiceState(NodePath.join(base, "runtime", "service-state.json"));
      assert.equal(state.activeVersion, baseline);
      assert.equal(state.update, undefined);
      assert.equal(readMarker(NodePath.join(base, "userdata", "statev2.sqlite")), "before");
      assert.equal(
        await NodeFSP.readFile(NodePath.join(base, "userdata", "settings.json"), "utf8"),
        "before-settings",
      );
      await NodeAssert.rejects(NodeFSP.access(NodePath.join(base, "runtime", "db-backup")));
      await NodeFSP.access(NodePath.join(base, "runtime", "test-handle"));
    });
  },
);
