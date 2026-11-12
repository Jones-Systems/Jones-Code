// @effect-diagnostics nodeBuiltinImport:off -- Each fixture owns and removes its synthetic ledger.
import * as NodeFSP from "node:fs/promises";
import * as NodeOS from "node:os";
import * as NodePath from "node:path";
import { describe, expect, it, vi } from "vite-plus/test";
import { EnvironmentId } from "@t3tools/contracts";
import type { JonesOperationStartInput } from "@t3tools/contracts/jones/jonesOperations";
import { JonesUpdater } from "../JonesUpdater.ts";
import { inspectHeldPreparationCleanup } from "../preparation/nativeEffectsRecovery.ts";
import { createOperationLedger } from "./ledger.ts";
import { createTargetOperations } from "./operations.ts";

vi.mock("../preparation/nativeEffectsRecovery.ts", () => ({
  inspectHeldPreparationCleanup: vi.fn(),
}));

async function fixture(run: (f: Awaited<ReturnType<typeof make>>) => Promise<void>) {
  const base = await NodeFSP.mkdtemp(NodePath.join(NodeOS.tmpdir(), "jones-cleanup-review-"));
  try {
    await run(await make(base));
  } finally {
    vi.mocked(inspectHeldPreparationCleanup).mockReset();
    vi.restoreAllMocks();
    await NodeFSP.rm(base, { recursive: true, force: true });
    await expect(NodeFSP.stat(base)).rejects.toMatchObject({ code: "ENOENT" });
  }
}

async function make(base: string) {
  const input: JonesOperationStartInput = {
    operationId: "11111111-1111-4111-8111-111111111111",
    environmentId: EnvironmentId.make("synthetic-cleanup-target"),
    expectedInstalledSource: "a".repeat(40),
    action: { kind: "download-exact", targetSource: "b".repeat(40) },
  };
  const jobId = "22222222-2222-4222-8222-222222222222";
  const ledger = createOperationLedger(base);
  await ledger.accept(input);
  const held = await ledger.transition(input.operationId, (receipt) => ({
    ...receipt,
    jobId,
    phase: "held",
    code: "cleanup-unconfirmed",
    stop: {
      stopId: "33333333-3333-4333-8333-333333333333",
      requestedAt: "2026-11-12T00:00:00Z",
      result: "held",
    },
  }));
  const unexpected = vi.fn((): never => {
    throw new Error("Cleanup review must not dispatch unrelated work.");
  });
  const updater = new JonesUpdater(
    {
      initialState: {
        source: "jones-actions",
        channel: "jones-main",
        phase: "no-new",
        environmentId: input.environmentId,
        capability: { check: true, download: true, install: true, operations: 1 },
      },
      platform: "linux",
      architecture: "x64",
      stateChanged: () => {},
      installedSource: async () => input.expectedInstalledSource,
      prepare: unexpected,
      install: unexpected,
    },
    { check: unexpected },
  );
  updater.setHeldJobs([jobId]);
  const download = vi.spyOn(updater, "download");
  const stop = vi.spyOn(updater, "stopPreparation");
  const install = vi.spyOn(updater, "install");
  const apply = vi.fn(async (authorize: () => void) => {
    authorize();
    return true;
  });
  vi.mocked(inspectHeldPreparationCleanup).mockImplementation(async (key, receipt) => {
    expect(key).toBe(base);
    expect(receipt).toEqual(held);
    return apply;
  });
  const operations = createTargetOperations({
    key: base,
    ledger,
    updater,
    preparation: {
      start: unexpected,
      run: unexpected,
      inspect: () => "unknown",
      sweep: unexpected,
    },
    refreshPolicy: unexpected,
    install: unexpected,
    reconcileNative: unexpected,
    onFailure: unexpected,
  });
  const read = { environmentId: input.environmentId, operationIds: [input.operationId] };
  const inspect = async () => {
    const result = await operations.read({ ...read, inspectCleanup: input.operationId });
    expect(result.receipts).toEqual([{ operationId: input.operationId, receipt: held }]);
    expect(result.cleanupInspection).toMatchObject({
      operationId: input.operationId,
      status: "eligible",
    });
    const recoveryToken = result.cleanupInspection?.recoveryToken;
    expect(recoveryToken).toEqual(expect.any(String));
    if (recoveryToken === undefined) throw new Error("Missing server-owned cleanup token.");
    return {
      environmentId: input.environmentId,
      operationId: input.operationId,
      jobId,
      recoveryToken,
      confirmed: true as const,
    };
  };
  return {
    operations,
    ledger,
    updater,
    held,
    read,
    inspect,
    apply,
    unexpected,
    download,
    stop,
    install,
  };
}

describe("server cleanup review ownership", () => {
  it("supersedes A with B, applies B once despite a lost caller reply and concurrent duplicate, and reads back the same stopped receipt", async () =>
    fixture(async (f) => {
      const a = await f.inspect();
      const b = await f.inspect();
      expect(b.recoveryToken).not.toBe(a.recoveryToken);
      expect(await f.operations.recoverCleanup(a)).toEqual({ recoveredCleanup: null });
      expect(f.apply).not.toHaveBeenCalled();
      expect(f.updater.admissionHeld()).toBe(true);

      const entered = Promise.withResolvers<void>();
      const release = Promise.withResolvers<void>();
      f.apply.mockImplementation(async (authorize) => {
        authorize();
        entered.resolve();
        await release.promise;
        return true;
      });
      const replyLost = new Error("Synthetic caller lost the completed cleanup reply.");
      const caller = f.operations.recoverCleanup(b).then(() => {
        throw replyLost;
      });
      const observedCaller = expect(caller).rejects.toBe(replyLost);
      await entered.promise;
      const concurrentDuplicate = f.operations.recoverCleanup(b);
      release.resolve();
      await observedCaller;
      expect(await concurrentDuplicate).toEqual({ recoveredCleanup: null });
      expect(await f.operations.recoverCleanup(b)).toEqual({ recoveredCleanup: null });
      expect(f.apply).toHaveBeenCalledTimes(1);

      const result = await f.operations.read(f.read);
      const recovered = result.receipts[0]?.receipt;
      expect(result.receipts).toHaveLength(1);
      expect(recovered).toEqual({
        ...f.held,
        updatedAt: expect.any(String),
        phase: "stopped",
        code: "cleanup-recovered",
        message: "Cleanup was verified and the preparation hold was released.",
        stop: { ...f.held.stop, result: "stopped" },
      });
      expect(await f.ledger.read(b.operationId)).toEqual(recovered);
      expect(f.updater.snapshot().operation).toEqual(recovered);
      expect(f.updater.admissionHeld()).toBe(false);
      for (const dispatch of [f.unexpected, f.download, f.stop, f.install])
        expect(dispatch).not.toHaveBeenCalled();
    }));

  it("consumes the review before a refused native apply, retaining the hold without replay", async () =>
    fixture(async (f) => {
      const review = await f.inspect();
      f.apply.mockResolvedValue(false);
      expect(await f.operations.recoverCleanup(review)).toEqual({ recoveredCleanup: null });
      expect(await f.operations.recoverCleanup(review)).toEqual({ recoveredCleanup: null });
      expect(f.apply).toHaveBeenCalledTimes(1);
      expect(await f.ledger.read(review.operationId)).toEqual(f.held);
      expect(f.updater.admissionHeld()).toBe(true);
      for (const dispatch of [f.unexpected, f.download, f.stop, f.install])
        expect(dispatch).not.toHaveBeenCalled();
    }));

  it.each(["expired", "changed receipt"] as const)(
    "refuses %s review before native apply and retains the hold",
    async (reason) =>
      fixture(async (f) => {
        const now = vi.spyOn(performance, "now").mockReturnValue(1000);
        const review = await f.inspect();
        if (reason === "expired") now.mockReturnValue(121_000);
        else
          await f.ledger.transition(review.operationId, (receipt) => ({
            ...receipt,
            message: "Synthetic receipt changed after review.",
          }));
        expect(await f.operations.recoverCleanup(review)).toEqual({ recoveredCleanup: null });
        expect(f.apply).not.toHaveBeenCalled();
        expect((await f.operations.read(f.read)).receipts[0]?.receipt).toMatchObject({
          operationId: review.operationId,
          jobId: review.jobId,
          phase: "held",
          code: "cleanup-unconfirmed",
          stop: { result: "held" },
        });
        expect(f.updater.admissionHeld()).toBe(true);
        for (const dispatch of [f.unexpected, f.download, f.stop, f.install])
          expect(dispatch).not.toHaveBeenCalled();
      }),
  );
});
