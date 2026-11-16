import {
  CommandId,
  EnvironmentId,
  ThreadId,
  type SettlementRestoreInspection,
  type SettlementRestoreRecord,
} from "@t3tools/contracts";
import * as Cause from "effect/Cause";
import { AsyncResult } from "effect/reactivity";
import { describe, expect, it, vi } from "vite-plus/test";

import type { AtomCommandResult } from "../../state/runtime.ts";
import { createSettlementRestoreController, type SettlementRestorePorts } from "./controller.ts";
import { settlementRestoreMessage } from "./model.ts";

const target = { environmentId: EnvironmentId.make("env"), threadId: ThreadId.make("thread") };
const settle = CommandId.make("settle-1");

const rejection = (code: string) => ({
  _tag: "OrchestrationV2DispatchCommandError",
  message: `Settlement restore rejected: ${code}: details.`,
  detail: `Settlement restore rejected: ${code}: details.`,
});
const transportLost = { _tag: "RpcClientError", message: "Socket closed" };

function record(overrides: Partial<SettlementRestoreRecord> = {}): SettlementRestoreRecord {
  return {
    settleCommandId: settle,
    settledSequence: 4,
    settledAt: "2026-10-20T10:00:00.000Z",
    capturedAt: "2026-10-20T10:00:00.000Z",
    prior: {
      settledOverride: null,
      unsettledAt: null,
      pinnedAt: "2026-10-01T00:00:00.000Z",
      pinOrderKey: "a1",
      activeOrderKey: null,
    },
    snoozedUntilAtSettle: null,
    clearedWatchCount: 0,
    restore: null,
    notRestored: [],
    ...overrides,
  };
}

function inspection(
  overrides: Partial<SettlementRestoreInspection> = {},
): SettlementRestoreInspection {
  return { threadId: target.threadId, status: "restorable", record: record(), ...overrides };
}

function harness() {
  let ids = 0;
  const dispatch = vi.fn<SettlementRestorePorts["dispatch"]>(async () =>
    AsyncResult.success({ sequence: 9 }),
  );
  const inspect = vi.fn<SettlementRestorePorts["inspect"]>(async () =>
    AsyncResult.success(inspection()),
  );
  const controller = createSettlementRestoreController({
    dispatch,
    inspect,
    newCommandId: () => CommandId.make(`restore-${++ids}`),
  });
  return { controller, dispatch, inspect };
}

const fail = (error: unknown): AtomCommandResult<never, unknown> =>
  AsyncResult.failure(Cause.fail(error));
const restoreIds = (dispatch: ReturnType<typeof harness>["dispatch"]) =>
  dispatch.mock.calls.map(([, command]) => command.commandId);

describe("observed-settle restore", () => {
  it("sends one restore bound to the observed settle", async () => {
    const { controller, dispatch, inspect } = harness();
    expect(await controller.restore(target, settle)).toEqual({
      type: "restored",
      restoreCommandId: "restore-1",
    });
    expect(dispatch).toHaveBeenCalledExactlyOnceWith(target.environmentId, {
      commandId: "restore-1",
      threadId: target.threadId,
      settleCommandId: settle,
    });
    expect(inspect).not.toHaveBeenCalled();
  });

  it("reports a server rejection without inspecting, and a new attempt is a new command", async () => {
    const { controller, dispatch, inspect } = harness();
    dispatch.mockResolvedValueOnce(fail(rejection("superseded")));
    const outcome = await controller.restore(target, settle);
    expect(outcome).toEqual({ type: "rejected", code: "superseded" });
    expect(settlementRestoreMessage(outcome as never)).toMatch(/changed after it was settled/);
    expect(inspect).not.toHaveBeenCalled();
    await controller.restore(target, settle);
    expect(restoreIds(dispatch)).toEqual(["restore-1", "restore-2"]);
  });

  it("confirms a restore whose response was lost from the server's record", async () => {
    const { controller, dispatch, inspect } = harness();
    dispatch.mockResolvedValueOnce(fail(transportLost));
    inspect.mockResolvedValueOnce(
      AsyncResult.success(
        inspection({
          status: "restored",
          record: record({
            restore: {
              commandId: CommandId.make("restore-1"),
              restoredAt: "2026-10-20T10:00:05.000Z",
              sequence: 9,
            },
          }),
        }),
      ),
    );
    expect(await controller.restore(target, settle)).toEqual({
      type: "restored",
      restoreCommandId: "restore-1",
    });
    expect(inspect).toHaveBeenCalledExactlyOnceWith(target.environmentId, {
      threadId: target.threadId,
      settleCommandId: settle,
    });
  });

  it("keeps the same command ID after an unknown effect, never issuing an independent retry", async () => {
    const { controller, dispatch, inspect } = harness();
    dispatch.mockResolvedValueOnce(fail(transportLost));
    // Still restorable: the command may not have reached the server, or not yet applied.
    expect(await controller.restore(target, settle)).toEqual({
      type: "unknown",
      restoreCommandId: "restore-1",
    });
    dispatch.mockResolvedValueOnce(fail(transportLost));
    inspect.mockResolvedValueOnce(fail(transportLost));
    expect(await controller.restore(target, settle)).toMatchObject({ type: "unknown" });
    expect(await controller.restore(target, settle)).toMatchObject({ type: "restored" });
    expect(restoreIds(dispatch)).toEqual(["restore-1", "restore-1", "restore-1"]);
  });

  it("drops a command ID the server already rejected", async () => {
    const { controller, dispatch } = harness();
    dispatch.mockResolvedValueOnce(fail(transportLost));
    await controller.restore(target, settle);
    dispatch.mockResolvedValueOnce(
      fail({ message: "Command restore-1 was previously rejected: missing detail" }),
    );
    expect(await controller.restore(target, settle)).toMatchObject({ type: "unknown" });
    await controller.restore(target, settle);
    expect(restoreIds(dispatch)).toEqual(["restore-1", "restore-1", "restore-2"]);
  });

  it("reports why an ambiguous restore did not apply", async () => {
    const { controller, dispatch, inspect } = harness();
    dispatch.mockResolvedValue(fail(transportLost));
    inspect.mockResolvedValueOnce(
      AsyncResult.success(
        inspection({
          status: "superseded",
          blocker: "stale_settle_command",
          historicalOutcome: "unavailable",
          record: record({ settleCommandId: CommandId.make("settle-2") }),
        }),
      ),
    );
    expect(await controller.restore(target, settle)).toEqual({
      type: "rejected",
      code: "stale_settle_command",
    });
  });

  it("shares one dispatch between a shortcut and a menu choice for the same settle", async () => {
    const { controller, dispatch } = harness();
    const [first, second] = await Promise.all([
      controller.restore(target, settle),
      controller.restore(target, settle),
    ]);
    expect(first).toBe(second);
    expect(dispatch).toHaveBeenCalledOnce();
  });

  it("never sends the restore to a server without the capability", async () => {
    const { controller, dispatch, inspect } = harness();
    dispatch.mockResolvedValueOnce(fail({ _tag: "SettlementRestoreUnsupportedError" }));
    expect(await controller.restore(target, settle)).toEqual({ type: "unsupported" });
    expect(inspect).not.toHaveBeenCalled();
  });
});

describe("current-settle restore from the menu", () => {
  it("undoes the settle the server's record names", async () => {
    const { controller, dispatch, inspect } = harness();
    expect(await controller.restoreCurrent(target)).toMatchObject({ type: "restored" });
    expect(inspect).toHaveBeenCalledExactlyOnceWith(target.environmentId, {
      threadId: target.threadId,
    });
    expect(dispatch.mock.calls[0]?.[1].settleCommandId).toBe(settle);
  });

  it.each([
    [inspection({ status: "none", record: null }), "none"],
    [inspection({ status: "superseded", blocker: "state_changed" }), "superseded"],
    [inspection({ status: "superseded", blocker: "thread_missing" }), "thread_missing"],
    [
      inspection({
        status: "restored",
        record: record({
          restore: {
            commandId: CommandId.make("someone-else"),
            restoredAt: "2026-10-20T10:00:05.000Z",
            sequence: 9,
          },
        }),
      }),
      "already_restored",
    ],
  ] as const)("does not restore when the record is %#", async (found, code) => {
    const { controller, dispatch, inspect } = harness();
    inspect.mockResolvedValueOnce(AsyncResult.success(found));
    expect(await controller.restoreCurrent(target)).toEqual({ type: "rejected", code });
    expect(dispatch).not.toHaveBeenCalled();
  });

  it("recognizes its own earlier restore whose response was lost", async () => {
    const { controller, dispatch, inspect } = harness();
    dispatch.mockResolvedValueOnce(fail(transportLost));
    await controller.restore(target, settle);
    inspect.mockResolvedValueOnce(
      AsyncResult.success(
        inspection({
          status: "restored",
          record: record({
            restore: {
              commandId: CommandId.make("restore-1"),
              restoredAt: "2026-10-20T10:00:05.000Z",
              sequence: 9,
            },
          }),
        }),
      ),
    );
    expect(await controller.restoreCurrent(target)).toEqual({
      type: "restored",
      restoreCommandId: "restore-1",
    });
    expect(dispatch).toHaveBeenCalledOnce();
  });

  it("reports an unavailable record read without dispatching", async () => {
    const { controller, dispatch, inspect } = harness();
    inspect.mockResolvedValueOnce(fail(transportLost));
    expect(await controller.restoreCurrent(target)).toEqual({ type: "unavailable" });
    expect(dispatch).not.toHaveBeenCalled();
  });
});
