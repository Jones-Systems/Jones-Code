// @effect-diagnostics nodeBuiltinImport:off
import { assert, it } from "@effect/vitest";
import * as NodeAssert from "node:assert/strict";
import * as NodeFSP from "node:fs/promises";
import * as NodeOS from "node:os";
import * as NodePath from "node:path";
import * as NodeSqlite from "node:sqlite";
import { Launcher, readServiceState, writeServiceState } from "../../serviceLauncher.ts";
import type { QualifiedQuiescenceAdapter } from "./qualifiedQuiescence.ts";
import {
  currentQualifiedRuntimeBinding,
  qualifiedPayloadDigest,
  stageQualifiedRuntime,
  QUALIFIED_RUNTIME_RECEIPT,
  type QualifiedRuntimeReceipt,
} from "./qualifiedRuntime.ts";

const baseline = "0.0.0-preview.20261002.100";
const target = "0.0.0-preview.20261002.101.1";
const readMarker = (file: string) => {
  const database = new NodeSqlite.DatabaseSync(file, { readOnly: true });
  try {
    return database.prepare("SELECT value FROM marker").get()?.value;
  } finally {
    database.close();
  }
};

async function runScenario(
  mode: "commit" | "rollback" | "stale-child" | "missing-gate" | "wrong-gate",
  body: (base: string) => Promise<void>,
  quiescenceAdapter: QualifiedQuiescenceAdapter = { scan: async () => [] },
  onLauncher?: (launcher: Launcher) => void,
) {
  const base = await NodeFSP.mkdtemp(NodePath.join(NodeOS.tmpdir(), "jones-launcher-test-"));
  try {
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
    await reserveQualifiedResume({receipt});
    process.exit(0);
  });
  process.send({type:"prepared", updateId:context.update.id, startupGateProtocol:1, qualified:receipt});
} else if (context.update === undefined) {
  const handle = readFileSync(${JSON.stringify(NodePath.join(base, "runtime", "test-handle"))}, "utf8");
  process.on("message", m => {if (m.type === "update-rejected") process.exit(0);});
  process.send({type:"request-update", targetVersion:${JSON.stringify(target)}, dbPath:${JSON.stringify(dbPath)}, stagedHandle:handle});
  setInterval(() => {}, 1000);
} else process.exit(0);
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
      workflow: ".github/workflows/artifact-cli-linux.yml",
      runId: 100,
      runAttempt: 1,
      artifactId: 99,
      artifactDigest: `sha256:${"e".repeat(64)}`,
      archiveSha256: "f".repeat(64),
      platform: "linux",
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
      startupGateProtocol: 1,
    });
    onLauncher?.(launcher);
    // Captured children exit or are reaped before the launcher settles; cleanup never polls by process name.
    await launcher.run().then(
      () => {
        throw new Error("Unexpected launcher completion");
      },
      () => undefined,
    );
    await body(base);
  } finally {
    await NodeFSP.rm(base, { recursive: true, force: true });
  }
}

it("commits a qualified trial after readiness and retains its previous binary/state pair", async () => {
  await runScenario("commit", async (base) => {
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
    await NodeAssert.rejects(
      new Launcher(base, state, {
        quiescenceAdapter: { scan: async () => [] },
        startupGateProtocol: 1,
      }).run(),
      /held for reconciliation/,
    );
    assert.equal(await NodeFSP.readFile(statePath, "utf8"), beforeRecovery);
    await NodeFSP.access(reservation);
  });
});

it("holds a stale captured PID proof without committing or automatically restoring advanced state", async () => {
  await runScenario("stale-child", async (base) => {
    const state = await readServiceState(NodePath.join(base, "runtime", "service-state.json"));
    assert.equal(state.activeVersion, baseline);
    assert.equal(state.update?.status, "pending");
    assert.equal(state.update?.startupReceipt, undefined);
    const backup = NodePath.join(base, "runtime", "db-backup", state.update!.id);
    assert.equal(readMarker(NodePath.join(backup, "database")), "before");
    assert.equal(readMarker(NodePath.join(base, "userdata", "statev2.sqlite")), "after");
    await NodeAssert.rejects(NodeFSP.access(NodePath.join(backup, "resume-dispatched.json")));
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
