// @effect-diagnostics nodeBuiltinImport:off - Isolated private-journal fixtures; each captured root is removed in teardown.
import * as FS from "node:fs/promises";
import * as OS from "node:os";
import * as Path from "node:path";
import { createHash } from "node:crypto";
import { afterEach, describe, expect, it } from "vite-plus/test";
import { EnvironmentId } from "@t3tools/contracts";
import { startPlan } from "@t3tools/client-runtime/jones/install-plan/model";
import { createInstallPlanStore, type PlanSnapshot } from "./store.ts";

const roots = new Set<string>();
afterEach(async () => { for (const root of roots) { await FS.rm(root, { recursive: true, force: true }); roots.delete(root); } });
const target = { repository: "Jones-Systems/Jones-Code", sourceSha: "a".repeat(40), sourceTree: "b".repeat(40), version: "new" } as const;
const plan = () => startPlan({ planId: "11111111-1111-4111-8111-111111111111", origin: "update-button", target,
  from: { version: "old" }, laptopEnvironmentId: EnvironmentId.make("laptop"), select: [], now: "2026-10-27T00:00:00Z" });
async function fixture() {
  const home = await FS.mkdtemp(Path.join(OS.tmpdir(), "jones-plan-")); roots.add(home);
  const directory = Path.join(home, "runtime", "jones-install-plan", `desktop-${createHash("sha256").update("").digest("hex")}`);
  const store = createInstallPlanStore({ home, profile: undefined });
  const read = () => store.transact(async (snapshot) => ({ ...snapshot, result: snapshot }));
  return { home, directory, store, read };
}
describe("native install plan storage", () => {
  it("keeps the plan, last result and stop disposition across store recreation without touching fleet state", async () => {
    const f = await fixture();
    const current = plan();
    await f.store.transact(async (s) => ({ document: { ...s.document, last: { ...current, dismissedAt: "2026-10-27T01:00:00Z" } },
      dispositions: { schema: 1, targets: [{ targetSource: target.sourceSha, targetTree: target.sourceTree, planId: current.planId, createdAt: current.createdAt, laptopStopped: true }] }, result: undefined }));
    const restarted = createInstallPlanStore({ home: f.home, profile: undefined });
    const saved = await restarted.transact(async (s) => ({ ...s, result: s }));
    expect(saved.document.last?.planId).toBe(current.planId);
    expect(saved.dispositions.targets[0]?.laptopStopped).toBe(true);
    for (const file of ["plan.json", "dispositions.json", "lock.sqlite"])
      expect((await FS.stat(Path.join(f.directory, file))).mode & 0o777).toBe(0o600);
    await expect(FS.stat(Path.join(f.home, "runtime", "jones-fleet"))).rejects.toMatchObject({ code: "ENOENT" });
  });

  it.each(["newer", "invalid", "oversized", "hardlink", "symlink"])("retains %s evidence and prevents mutation", async (kind) => {
    const f = await fixture(); await f.read();
    const file = Path.join(f.directory, "plan.json");
    const bytes = kind === "newer" ? '{"schema":2,"future":"preserve"}' : kind === "oversized" ? "x".repeat(1024 * 1024 + 1) : "{invalid";
    const original = Path.join(f.home, "original.json");
    if (kind === "symlink" || kind === "hardlink") {
      await FS.writeFile(original, bytes, { mode: 0o600 });
      if (kind === "symlink") await FS.symlink(original, file); else await FS.link(original, file);
    } else await FS.writeFile(file, bytes, { mode: 0o600 });
    const snapshot = await f.read();
    expect(snapshot.readOnly?.code).toBe(kind === "newer" ? "newer-schema" : "invalid");
    await expect(f.store.transact(async (s) => ({ ...s, document: { ...s.document, current: plan() }, result: undefined }))).rejects.toThrow("Read-only");
    expect(await FS.readFile(file, "utf8")).toBe(bytes);
  });

  it("refuses bounds without truncating an operation or overwriting retained files", async () => {
    const f = await fixture();
    const current = plan();
    await f.store.transact(async (s) => ({ ...s, document: { ...s.document, current }, result: undefined }));
    const before = await FS.readFile(Path.join(f.directory, "plan.json"), "utf8");
    const operations = Array.from({ length: 17 }, () => ({ operationId: current.planId, step: "download" as const, requestedAt: current.createdAt }));
    await expect(f.store.transact(async (s) => ({ ...s, document: { ...s.document, current: { ...current, laptop: { ...current.laptop, operations } } }, result: undefined }))).rejects.toMatchObject({ code: "bounds" });
    expect(await FS.readFile(Path.join(f.directory, "plan.json"), "utf8")).toBe(before);
  });

  it("fails closed under a second process lock and resumes after its owner releases it", async () => {
    const f = await fixture(); await f.read();
    const { DatabaseSync } = await import("node:sqlite");
    const lock = new DatabaseSync(Path.join(f.directory, "lock.sqlite"));
    try {
      lock.exec("BEGIN EXCLUSIVE");
      await expect(f.read()).rejects.toThrow();
    } finally { lock.close(); }
    const saved: PlanSnapshot = await f.read();
    expect(saved.readOnly).toBeUndefined();
  });
});
