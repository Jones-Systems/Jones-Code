// @effect-diagnostics nodeBuiltinImport:off - Isolated private-journal fixtures; each captured root is removed in teardown.
import * as NodeFSP from "node:fs/promises";
import * as NodeOS from "node:os";
import * as NodePath from "node:path";
import * as NodeCrypto from "node:crypto";
import { afterEach, describe, expect, it } from "vite-plus/test";
import { EnvironmentId } from "@t3tools/contracts";
import { startPlan } from "@t3tools/client-runtime/jones/install-plan/model";
import { readPrivateJson } from "../storage/privateJsonStore.ts";
import { archiveCurrent, commitRevision } from "./journal.ts";
import { createInstallPlanStore, type PlanSnapshot } from "./store.ts";

const roots = new Set<string>();
afterEach(async () => {
  for (const root of roots) {
    await NodeFSP.rm(root, { recursive: true, force: true });
    roots.delete(root);
  }
});
const target = {
  repository: "Jones-Systems/Jones-Code",
  sourceSha: "a".repeat(40),
  sourceTree: "b".repeat(40),
  version: "new",
} as const;
const plan = () =>
  startPlan({
    planId: "11111111-1111-4111-8111-111111111111",
    origin: "update-button",
    target,
    from: { version: "old" },
    laptopEnvironmentId: EnvironmentId.make("laptop"),
    select: [],
    now: "2026-10-27T00:00:00Z",
  });
async function fixture() {
  const home = await NodeFSP.mkdtemp(NodePath.join(NodeOS.tmpdir(), "jones-plan-"));
  roots.add(home);
  const directory = NodePath.join(
    home,
    "runtime",
    "jones-install-plan",
    `desktop-${NodeCrypto.createHash("sha256").update("").digest("hex")}`,
  );
  const store = createInstallPlanStore({ home, profile: undefined });
  const read = () => store.transact(async (snapshot) => ({ ...snapshot, result: snapshot }));
  return { home, directory, store, read };
}
describe("native install plan storage", () => {
  it("retains the prebinding and a paused gate with its legacy blocked latch", async () => {
    const f = await fixture();
    const base = plan();
    const generation = "d".repeat(64);
    const current = {
      ...base,
      laptop: {
        ...base.laptop,
        operations: [
          {
            operationId: "33333333-3333-4333-8333-333333333333",
            step: "install",
            requestedAt: base.createdAt,
            prebinding: {
              fromGeneration: generation,
              fromSource: target.sourceSha,
              fromTree: target.sourceTree,
              stagedHandle: "stage",
            },
          },
        ],
        gate: {
          openedAt: base.createdAt,
          basis: "prior-verified",
          transactionId: generation,
          activeGeneration: generation,
          activeSource: target.sourceSha,
          activeTree: target.sourceTree,
        },
        gatePause: true,
        gateClosed: { at: base.createdAt, reason: "blocked" },
      },
    } satisfies NonNullable<PlanSnapshot["document"]["current"]>;
    const restarted = createInstallPlanStore({ home: f.home, profile: undefined });
    await f.store.transact(async (s) => ({
      ...s,
      document: { ...s.document, current },
      result: undefined,
    }));
    const saved = await restarted.transact(async (s) => ({ ...s, result: s.document }));
    expect(saved.current).toEqual(current);

    const { gateClosed: _blocked, ...laptop } = current.laptop;
    await expect(
      restarted.transact(async (s) => ({
        ...s,
        document: { ...s.document, current: { ...current, laptop } },
        result: undefined,
      })),
    ).rejects.toMatchObject({ code: "bounds" });
    expect((await f.read()).document.current).toEqual(current);
  });

  it("retains the exact client command binding beside its native reservation across store recreation", async () => {
    const f = await fixture(),
      current = plan();
    const entry = {
      body: "e".repeat(64),
      receipt: {
        command: {
          commandId: "22222222-2222-4222-8222-222222222222",
          environmentId: current.laptop.environmentId,
          targetSource: target.sourceSha,
          action: "download" as const,
        },
        requestType: "start" as const,
        planId: current.planId,
        operationId: "33333333-3333-4333-8333-333333333333",
        outcome: "reserved" as const,
      },
    };
    await f.store.transact(async (s) => ({
      ...s,
      document: { ...s.document, current, commands: [entry] },
      result: undefined,
    }));
    const restarted = createInstallPlanStore({ home: f.home, profile: undefined });
    const saved = await restarted.transact(async (s) => ({ ...s, result: s.document }));
    expect(saved.commands).toEqual([entry]);
    await expect(
      restarted.transact(async (s) => ({
        ...s,
        document: { ...s.document, commands: [entry, entry] },
        result: undefined,
      })),
    ).rejects.toThrow("bound");
    expect((await f.read()).document.commands).toEqual([entry]);
  });
  it("keeps the plan, last result and stop disposition across store recreation without touching fleet state", async () => {
    const f = await fixture();
    const current = plan();
    await f.store.transact(async (s) => ({
      document: { ...s.document, last: { ...current, dismissedAt: "2026-10-27T01:00:00Z" } },
      dispositions: {
        schema: 1,
        targets: [
          {
            targetSource: target.sourceSha,
            targetTree: target.sourceTree,
            planId: current.planId,
            createdAt: current.createdAt,
            laptopStopped: true,
          },
        ],
      },
      result: undefined,
    }));
    const restarted = createInstallPlanStore({ home: f.home, profile: undefined });
    const saved = await restarted.transact(async (s) => ({ ...s, result: s }));
    expect(saved.document.last?.planId).toBe(current.planId);
    expect(saved.dispositions.targets[0]?.laptopStopped).toBe(true);
    for (const file of ["plan.json", "dispositions.json", "lock.sqlite"])
      expect((await NodeFSP.stat(NodePath.join(f.directory, file))).mode & 0o777).toBe(0o600);
    await expect(
      NodeFSP.stat(NodePath.join(f.home, "runtime", "jones-fleet")),
    ).rejects.toMatchObject({ code: "ENOENT" });
  });

  it.each(["newer", "invalid", "oversized", "hardlink", "symlink"])(
    "retains %s evidence and prevents mutation",
    async (kind) => {
      const f = await fixture();
      await f.read();
      const file = NodePath.join(f.directory, "plan.json");
      const bytes =
        kind === "newer"
          ? '{"schema":3,"future":"preserve"}'
          : kind === "oversized"
            ? "x".repeat(1024 * 1024 + 1)
            : "{invalid";
      const original = NodePath.join(f.home, "original.json");
      if (kind === "symlink" || kind === "hardlink") {
        await NodeFSP.writeFile(original, bytes, { mode: 0o600 });
        if (kind === "symlink") await NodeFSP.symlink(original, file);
        else await NodeFSP.link(original, file);
      } else await NodeFSP.writeFile(file, bytes, { mode: 0o600 });
      const snapshot = await f.read();
      expect(snapshot.readOnly?.code).toBe(kind === "newer" ? "newer-schema" : "invalid");
      await expect(
        f.store.transact(async (s) => ({
          ...s,
          document: { ...s.document, current: plan() },
          result: undefined,
        })),
      ).rejects.toThrow("Read-only");
      expect(await NodeFSP.readFile(file, "utf8")).toBe(bytes);
    },
  );

  it("refuses bounds without truncating an operation or overwriting retained files", async () => {
    const f = await fixture();
    const current = plan();
    await f.store.transact(async (s) => ({
      ...s,
      document: { ...s.document, current },
      result: undefined,
    }));
    const before = await NodeFSP.readFile(NodePath.join(f.directory, "plan.json"), "utf8");
    const operations = Array.from({ length: 17 }, () => ({
      operationId: current.planId,
      step: "download" as const,
      requestedAt: current.createdAt,
    }));
    await expect(
      f.store.transact(async (s) => ({
        ...s,
        document: {
          ...s.document,
          current: { ...current, laptop: { ...current.laptop, operations } },
        },
        result: undefined,
      })),
    ).rejects.toMatchObject({ code: "bounds" });
    expect(await NodeFSP.readFile(NodePath.join(f.directory, "plan.json"), "utf8")).toBe(before);
  });

  it("migrates a nearly full legacy journal without losing either original plan", async () => {
    const f = await fixture();
    const latest = { ...plan(), dismissedAt: "2026-10-27T01:00:00Z" };
    const active = { ...plan(), planId: "22222222-2222-4222-8222-222222222222" };
    const original = { schema: 1, current: active, last: latest } as const;
    const limit = 1024 * 1024;
    const padding = limit - Buffer.byteLength(JSON.stringify(original)) - 2;
    const legacy = { ...original, current: { ...active, from: { version: "a".repeat(padding) } } };
    await f.store.transact(async (s) => ({ ...s, document: legacy, result: undefined }));
    const file = NodePath.join(f.directory, "plan.json");
    const before = await NodeFSP.readFile(file, "utf8");
    expect(Buffer.byteLength(before)).toBeLessThanOrEqual(limit);
    expect(Buffer.byteLength(before)).toBeGreaterThan(limit - 32);
    expect((await f.read()).document.schema).toBe(1);
    expect(await NodeFSP.readFile(file, "utf8")).toBe(before);
    await f.store.transact(async (s) => ({
      ...s,
      document: commitRevision(
        s.document,
        archiveCurrent(s.document, {
          ...s.document.current!,
          dismissedAt: latest.dismissedAt,
          cancelled: true,
          cancelledFromRevision: active.revision,
          revision: active.revision + 1,
        }),
      ),
      result: undefined,
    }));
    const migrated = await NodeFSP.readFile(file, "utf8");
    expect(Buffer.byteLength(migrated)).toBeGreaterThan(limit);
    await expect(readPrivateJson(file, limit)).rejects.toThrow();
    expect(await NodeFSP.readFile(file, "utf8")).toBe(migrated);
    const restarted = createInstallPlanStore({ home: f.home, profile: undefined });
    const saved = await restarted.transact(async (s) => ({ ...s, result: s.document }));
    expect(saved.schema).toBe(2);
    if (saved.schema !== 2) throw new Error("Missing migrated document.");
    expect(saved.documentRevision).toBe(1);
    expect(saved.last?.planId).toBe(active.planId);
    expect(saved.last?.from.version.length).toBe(padding);
    expect(saved.older).toEqual([latest]);
  });

  it("fails closed under a second process lock and resumes after its owner releases it", async () => {
    const f = await fixture();
    await f.read();
    const NodeSqlite = await import("node:sqlite");
    const lock = new NodeSqlite.DatabaseSync(NodePath.join(f.directory, "lock.sqlite"));
    try {
      lock.exec("BEGIN EXCLUSIVE");
      await expect(f.read()).rejects.toThrow();
    } finally {
      lock.close();
    }
    const saved: PlanSnapshot = await f.read();
    expect(saved.readOnly).toBeUndefined();
  });
});
