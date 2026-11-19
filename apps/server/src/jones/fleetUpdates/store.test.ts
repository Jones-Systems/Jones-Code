// @effect-diagnostics nodeBuiltinImport:off -- Synthetic native journal fixtures exercise filesystem custody and retain exact-root cleanup.
import * as NodeFSP from "node:fs/promises";
import * as NodePath from "node:path";
import * as NodeOS from "node:os";
import { describe, expect, it, vi } from "vite-plus/test";
import { EnvironmentId } from "@t3tools/contracts";
import { createFleetHostStore } from "./store.ts";

vi.mock("node:fs/promises", async (importOriginal) => ({
  ...(await importOriginal<typeof import("node:fs/promises")>()),
}));

const enrollment = {
  enrollmentId: "11111111-1111-4111-8111-111111111111",
  environmentId: EnvironmentId.make("host"),
  enabled: true,
  continueRunningThreads: false,
};
async function fixture(run: (home: string) => Promise<void>) {
  const home = await NodeFSP.mkdtemp(
    NodePath.join(NodeOS.tmpdir(), "jones-fleet-host-store-test-"),
  );
  try {
    await run(home);
  } finally {
    await NodeFSP.rm(home, { recursive: true, force: true });
  }
}
describe("fleet host receipt storage", () => {
  it("retains enrollment across store recreation outside userdata", async () =>
    fixture(async (home) => {
      await createFleetHostStore(home).editEnrollment(() => enrollment);
      expect(await createFleetHostStore(home).readEnrollment()).toEqual(enrollment);
      expect(
        JSON.parse(
          await NodeFSP.readFile(
            NodePath.join(home, "runtime/jones-fleet/enrollment.json"),
            "utf8",
          ),
        ),
      ).toEqual(enrollment);
    }));
  it("preserves corrupt records instead of replacing them", async () =>
    fixture(async (home) => {
      const store = createFleetHostStore(home);
      await store.editEnrollment(() => enrollment);
      const path = NodePath.join(home, "runtime/jones-fleet/enrollment.json");
      await NodeFSP.writeFile(path, "corrupt retained evidence", { mode: 0o600 });
      await expect(
        store.editEnrollment(() => ({ ...enrollment, enabled: false })),
      ).rejects.toThrow();
      expect(await NodeFSP.readFile(path, "utf8")).toBe("corrupt retained evidence");
    }));
  it("rejects receipt symlinks and malformed operation paths", async () =>
    fixture(async (home) => {
      const store = createFleetHostStore(home);
      await store.readEnrollment();
      const target = NodePath.join(home, "owned-fixture.json");
      await NodeFSP.writeFile(target, JSON.stringify(enrollment), { mode: 0o600 });
      await NodeFSP.symlink(target, NodePath.join(home, "runtime/jones-fleet/enrollment.json"));
      await expect(store.readEnrollment()).rejects.toThrow();
      await expect(store.readOperation("../outside")).rejects.toThrow("Invalid fleet operation");
      expect(JSON.parse(await NodeFSP.readFile(target, "utf8"))).toEqual(enrollment);
    }));
});

it("preserves a plan directive across store restart and leader-only writes without changing enrollment", async () =>
  fixture(async (home) => {
    const store = createFleetHostStore(home);
    await store.editEnrollment(() => enrollment);
    const directive = {
      directiveId: "22222222-2222-4222-8222-222222222222",
      enrollmentId: enrollment.enrollmentId,
      targetSource: "b".repeat(40),
      targetTree: "c".repeat(40),
      mode: "manual-review" as const,
      issuedAt: "2026-10-27T00:00:00Z",
      leaderAtIssue: null,
    };
    await store.editFollower(() => ({
      schema: 1,
      leader: null,
      operation: null,
      lastAutomaticActivationAt: null,
      directive,
    }));
    const restarted = createFleetHostStore(home);
    await restarted.editFollower((old) => ({
      ...old!,
      leader: {
        enrollmentId: enrollment.enrollmentId,
        committedSource: directive.targetSource,
        committedTree: directive.targetTree,
        publishedAt: "2026-10-27T00:01:00Z",
      },
    }));
    expect((await restarted.readFollower())?.directive).toEqual(directive);
    expect(await restarted.readEnrollment()).toEqual(enrollment);
  }));

it("serializes directive transactions across stores and retains the fence on disk", async () =>
  fixture(async (home) => {
    const first = createFleetHostStore(home);
    const second = createFleetHostStore(home);
    await first.editEnrollment(() => enrollment);
    const fence = {
      directiveId: "33333333-3333-4333-8333-333333333333",
      bindingHash: "c".repeat(64),
    };
    const fenced = first.transactDirective((record, observed) => {
      expect(record).toBeNull();
      expect(observed).toEqual(enrollment);
      return {
        record: {
          schema: 1,
          leader: null,
          operation: null,
          lastAutomaticActivationAt: null,
          fencedDirectives: [fence],
        },
        result: fence.directiveId,
      };
    });
    const competing = second.transactDirective((record, observed) => {
      expect(observed).toEqual(enrollment);
      return { record, result: record?.fencedDirectives?.[0] };
    });
    expect(await Promise.all([fenced, competing])).toEqual([fence.directiveId, fence]);
    expect((await createFleetHostStore(home).readFollower())?.fencedDirectives).toEqual([fence]);
  }));

it("requires a directory sync before returning proof from a write with a failed sync", async () =>
  fixture(async (home) => {
    const directory = NodePath.join(home, "runtime", "jones-fleet");
    const journal = NodePath.join(directory, "follower.json");
    const fence = {
      directiveId: "33333333-3333-4333-8333-333333333333",
      bindingHash: "c".repeat(64),
      rollbackDenied: true as const,
    };
    const directive = {
      directiveId: "22222222-2222-4222-8222-222222222222",
      enrollmentId: enrollment.enrollmentId,
      targetSource: "b".repeat(40),
      targetTree: "c".repeat(40),
      mode: "manual-review" as const,
      issuedAt: "2026-10-27T00:00:00Z",
      leaderAtIssue: null,
    };
    await createFleetHostStore(home).editEnrollment(() => enrollment);
    const reconcile = () =>
      createFleetHostStore(home).transactDirective((record) => ({
        record: record ?? {
          schema: 1,
          leader: null,
          operation: null,
          lastAutomaticActivationAt: null,
          directive,
          fencedDirectives: [fence],
          releasedDirectives: [fence.directiveId],
        },
        result: fence.directiveId,
      }));

    const failure = new Error("synthetic journal directory sync failure");
    let failSync = true;
    let syncAttempts = 0;
    const originalOpen = NodeFSP.open;
    const rename = vi.spyOn(NodeFSP, "rename");
    const open = vi.spyOn(NodeFSP, "open").mockImplementation(async (...args) => {
      const handle = await originalOpen(...args);
      if (args[0] === directory) {
        const sync = handle.sync.bind(handle);
        handle.sync = async () => {
          ++syncAttempts;
          if (failSync) throw failure;
          await sync();
        };
      }
      return handle;
    });
    try {
      await expect(reconcile()).rejects.toThrow(failure);
      expect(JSON.parse(await NodeFSP.readFile(journal, "utf8"))).toMatchObject({
        fencedDirectives: [fence],
        releasedDirectives: [fence.directiveId],
      });
      expect(syncAttempts).toBe(1);

      const restarted = createFleetHostStore(home);
      await expect(restarted.readConfirmedFollower()).rejects.toThrow(failure);
      expect(syncAttempts).toBe(2);

      failSync = false;
      expect((await restarted.readConfirmedFollower())?.directive).toEqual(directive);
      await expect(reconcile()).resolves.toBe(fence.directiveId);
      expect(syncAttempts).toBe(4);

      failSync = true;
      await expect(reconcile()).rejects.toThrow(failure);
      expect(syncAttempts).toBe(5);
      expect(rename).toHaveBeenCalledTimes(1);
    } finally {
      open.mockRestore();
      rename.mockRestore();
    }
  }));
