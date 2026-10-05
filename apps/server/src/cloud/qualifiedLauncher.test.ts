// @effect-diagnostics nodeBuiltinImport:off
import { assert, it } from "@effect/vitest";
import * as NodeAssert from "node:assert/strict";
import * as NodeFSP from "node:fs/promises";
import * as NodeOS from "node:os";
import * as NodePath from "node:path";
import * as NodeSqlite from "node:sqlite";
import { Launcher, readServiceState, writeServiceState } from "../serviceLauncher.ts";
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
  mode: "commit" | "rollback",
  body: (base: string) => Promise<void>,
  quiescenceAdapter: QualifiedQuiescenceAdapter = { scan: async () => [] },
) {
  const base = await NodeFSP.mkdtemp(NodePath.join(NodeOS.tmpdir(), "jones-launcher-test-"));
  try {
    const userdata = NodePath.join(base, "userdata");
    await NodeFSP.mkdir(userdata);
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
const context = JSON.parse(process.env.T3_SERVICE_LAUNCHER_CONTEXT);
if (context.update?.status === "pending") {
  const db = new NodeSqlite.DatabaseSync(${JSON.stringify(dbPath)});
  db.exec("UPDATE marker SET value='after'"); db.close();
  writeFileSync(${JSON.stringify(NodePath.join(userdata, "settings.json"))}, "after-settings");
  writeFileSync(${JSON.stringify(NodePath.join(userdata, "keybindings.json"))}, "created-by-trial");
  if (${JSON.stringify(mode)} === "rollback") process.exit(1);
  process.send({type:"prepared", updateId:context.update.id});
  process.on("message", m => { if (m.type === "committed") process.exit(0); });
} else if (context.update === undefined) {
  const handle = readFileSync(${JSON.stringify(NodePath.join(base, "runtime", "test-handle"))}, "utf8");
  process.send({type:"request-update", targetVersion:${JSON.stringify(target)}, dbPath:${JSON.stringify(dbPath)}, stagedHandle:handle});
  setInterval(() => {}, 1000);
} else process.exit(0);
`;
    const active = NodePath.join(base, "runtime", "versions", baseline);
    const payload = NodePath.join(base, "payload");
    for (const directory of [active, payload]) {
      await NodeFSP.mkdir(directory, { recursive: true });
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
    });
    // The controlled active child exits after receiving its committed/rollback
    // context. Completion follows that observable exit, without test polling.
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
    const backup = NodePath.join(base, "runtime", "db-backup", state.update!.id);
    assert.equal(readMarker(NodePath.join(backup, "database")), "before");
    assert.equal(
      await NodeFSP.readFile(NodePath.join(backup, "settings.json"), "utf8"),
      "before-settings",
    );
    assert.equal(readMarker(NodePath.join(base, "userdata", "statev2.sqlite")), "after");
    await NodeFSP.access(NodePath.join(base, "runtime", "versions", baseline, "t3"));
  });
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
