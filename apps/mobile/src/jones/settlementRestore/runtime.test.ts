import { it as effectIt } from "@effect/vitest";
import { makePromisePorts } from "./promisePorts.testkit";
import {
  AuthOrchestrationOperateScope,
  CommandId,
  EnvironmentId,
  SETTLEMENT_RESTORE_CAPABILITY,
  ThreadId,
  type SettlementRestoreInspection,
  type SettlementRestoreRecord,
} from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import { AsyncResult } from "effect/reactivity";
import { beforeEach, describe, expect, it, vi } from "vite-plus/test";

const state = vi.hoisted(() => ({
  schema: "jones.settlement-restore/v1" as string | undefined,
  allowed: true,
  ids: 0,
  run: undefined as undefined | (<A, E>(effect: Effect.Effect<A, E>) => Promise<A>),
  inspect: vi.fn(),
  dispatch: vi.fn(),
  alert: vi.fn(),
  invalidations: vi.fn(),
  calls: [] as { label: string; environmentId: string; input: unknown; options: unknown }[],
}));
vi.mock("../../state/atom-registry", () => ({
  appAtomRegistry: {
    get: () =>
      new Map(
        ["one", "two"].map((id) => [
          id,
          {
            environment: {
              capabilities: {
                settlementRestore:
                  state.schema === undefined ? undefined : { schemaVersion: state.schema },
              },
            },
          },
        ]),
      ),
  },
}));
vi.mock("../../state/server", () => ({ environmentServerConfigsAtom: {} }));
vi.mock("../../state/session", () => ({
  readEnvironmentScope: (_environmentId: string, scope: string) =>
    state.allowed && scope === AuthOrchestrationOperateScope,
}));
vi.mock("../../components/ui/Dialog", () => ({
  alert: (...args: unknown[]) => state.alert(...args),
}));
vi.mock("../../lib/uuid", () => ({ uuidv4: () => `restore-${++state.ids}` }));
vi.mock("../../connection/runtime", () => ({ connectionAtomRuntime: {} }));
vi.mock("@t3tools/client-runtime/state/thread-undo", () => ({
  invalidate: (...args: unknown[]) => state.invalidations(...args),
}));
vi.mock("@t3tools/client-runtime/jones/settlement-restore", async (original) => ({
  ...(await original<typeof import("@t3tools/client-runtime/jones/settlement-restore")>()),
  inspectEnvironmentSettlementRestore: (environmentId: string, input: unknown) =>
    Effect.suspend(() => state.inspect(environmentId, input)),
  dispatchSettlementRestore: (input: unknown) => Effect.suspend(() => state.dispatch(input)),
}));
vi.mock("@t3tools/client-runtime/state/runtime", async (original) => ({
  ...(await original<typeof import("@t3tools/client-runtime/state/runtime")>()),
  createEnvironmentCommand: (_runtime: unknown, command: unknown) => command,
  runAtomCommand: async (
    _registry: unknown,
    command: {
      label: string;
      execute: (
        input: unknown,
        registry: unknown,
        environmentId: string,
      ) => Effect.Effect<unknown, unknown>;
    },
    target: { environmentId: string; input: unknown },
    options: unknown,
  ) => {
    state.calls.push({ label: command.label, ...target, options });
    const exit = await state.run!(
      Effect.exit(command.execute(target.input, _registry, target.environmentId)),
    );
    return Exit.isSuccess(exit) ? AsyncResult.success(exit.value) : AsyncResult.failure(exit.cause);
  },
}));

import { undoSettleThread } from "./runtime";
import { settlementRestoreMenuItems } from "./menu";

const thread = { environmentId: EnvironmentId.make("one"), id: ThreadId.make("thread") };
const record: SettlementRestoreRecord = {
  settleCommandId: CommandId.make("durable-settle"),
  settledSequence: 4,
  settledAt: "2026-10-20T10:00:00.000Z",
  capturedAt: "2026-10-20T10:00:00.000Z",
  prior: {
    settledOverride: null,
    unsettledAt: null,
    pinnedAt: "2026-10-01T00:00:00.000Z",
    pinOrderKey: "a1",
    activeOrderKey: "b2",
  },
  snoozedUntilAtSettle: null,
  clearedWatchCount: 0,
  restore: null,
  notRestored: [],
};
const inspection = (
  overrides: Partial<SettlementRestoreInspection> = {},
): SettlementRestoreInspection => ({
  threadId: thread.id,
  status: "restorable",
  record,
  ...overrides,
});

beforeEach(() => {
  state.schema = SETTLEMENT_RESTORE_CAPABILITY.schemaVersion;
  state.allowed = true;
  state.calls = [];
  state.inspect.mockReset().mockImplementation(() => Effect.succeed(inspection()));
  state.dispatch.mockReset().mockImplementation(() => Effect.void);
  state.alert.mockClear();
  state.invalidations.mockClear();
});

const withRuntimeCommands = (test: () => Promise<void>) =>
  Effect.scoped(
    Effect.gen(function* () {
      state.run = yield* makePromisePorts();
      return yield* Effect.promise(test);
    }),
  );

describe("mobile persistent Undo settle runtime", () => {
  it.each([undefined, "jones.settlement-restore/v2", "denied"])(
    "%s sends neither inspect nor restore",
    async (mode) => {
      if (mode === "denied") state.allowed = false;
      else state.schema = mode;
      expect(await undoSettleThread(thread)).toBe(false);
      expect(state.calls).toEqual([]);
      expect(state.inspect).not.toHaveBeenCalled();
      expect(state.dispatch).not.toHaveBeenCalled();
      expect(state.invalidations).not.toHaveBeenCalled();
    },
  );

  effectIt.effect(
    "coalesces taps before inspection, invalidates only the matching transient settle claim, and sends only the durable identity",
    () =>
      withRuntimeCommands(async () => {
        const [a, b] = await Promise.all([undoSettleThread(thread), undoSettleThread(thread)]);
        expect([a, b]).toEqual([true, true]);
        expect(state.inspect).toHaveBeenCalledExactlyOnceWith(thread.environmentId, {
          threadId: thread.id,
        });
        expect(state.invalidations).toHaveBeenCalledExactlyOnceWith("settle", "one:thread");
        expect(state.dispatch).toHaveBeenCalledExactlyOnceWith({
          commandId: expect.any(String),
          threadId: thread.id,
          settleCommandId: record.settleCommandId,
        });
        expect(state.calls.map((call) => call.label)).toEqual([
          "mobile:settlement-restore:inspect",
          "mobile:settlement-restore:dispatch",
        ]);
        expect(
          state.calls.every(
            (call) =>
              call.environmentId === thread.environmentId &&
              JSON.stringify(call.options) ===
                JSON.stringify({ reportFailure: false, reportDefect: false }),
          ),
        ).toBe(true);
      }),
  );

  effectIt.effect(
    "retains the exact unknown ID across new screen handlers and separates identical thread IDs by environment",
    () =>
      withRuntimeCommands(async () => {
        state.dispatch.mockImplementation(() => Effect.fail(new Error("response lost")));
        const mountScreen = () => ({ undo: () => undoSettleThread(thread) });
        expect(await mountScreen().undo()).toBe(false);
        const originalId = state.dispatch.mock.calls[0]![0].commandId;
        expect(await mountScreen().undo()).toBe(false);
        expect(state.dispatch.mock.calls[1]![0].commandId).toBe(originalId);
        state.dispatch.mockImplementation(() => Effect.void);
        expect(
          await undoSettleThread({ ...thread, environmentId: EnvironmentId.make("two") }),
        ).toBe(true);
        expect(state.dispatch.mock.calls[2]![0].commandId).not.toBe(originalId);
        expect(await mountScreen().undo()).toBe(true);
        expect(state.dispatch.mock.calls[3]![0].commandId).toBe(originalId);
        expect(state.alert.mock.calls[0]![1]).toContain("Couldn't confirm the undo");
      }),
  );

  effectIt.effect(
    "binds a new settle to a new ID while the earlier settle's outcome remains unknown",
    () =>
      withRuntimeCommands(async () => {
        state.dispatch.mockImplementation(() => Effect.fail(new Error("response lost")));
        expect(await undoSettleThread(thread)).toBe(false);
        const first = state.dispatch.mock.calls[0]![0];
        state.inspect.mockImplementation(() =>
          Effect.succeed(
            inspection({ record: { ...record, settleCommandId: CommandId.make("new-settle") } }),
          ),
        );
        state.dispatch.mockImplementation(() => Effect.void);
        expect(await undoSettleThread(thread)).toBe(true);
        expect(state.dispatch.mock.calls[1]![0]).toMatchObject({ settleCommandId: "new-settle" });
        expect(state.dispatch.mock.calls[1]![0].commandId).not.toBe(first.commandId);
        state.inspect.mockImplementation(() => Effect.succeed(inspection()));
        await undoSettleThread(thread);
      }),
  );

  effectIt.effect.each(["none", "superseded"] as const)(
    "preserves the shared %s outcome without dispatch",
    (status) =>
      withRuntimeCommands(async () => {
        state.inspect.mockImplementation(() =>
          Effect.succeed(inspection({ status, record: status === "none" ? null : record })),
        );
        expect(await undoSettleThread(thread)).toBe(false);
        expect(state.dispatch).not.toHaveBeenCalled();
        expect(state.alert.mock.calls[0]![1]).toContain(
          status === "none" ? "no settle to undo" : "overwrite newer changes",
        );
      }),
  );

  effectIt.effect(
    "reports unavailable inspection and rechecks permission inside the command before dispatch",
    () =>
      withRuntimeCommands(async () => {
        state.inspect.mockImplementationOnce(() => Effect.fail(new Error("offline")));
        expect(await undoSettleThread(thread)).toBe(false);
        expect(state.alert.mock.calls[0]![1]).toContain("Couldn't check");
        state.inspect.mockImplementation(() => {
          state.allowed = false;
          return Effect.succeed(inspection());
        });
        expect(await undoSettleThread(thread)).toBe(false);
        expect(state.dispatch).not.toHaveBeenCalled();
        state.allowed = true;
        await undoSettleThread(thread);
      }),
  );
});

it("offers persistent undo only for a settled, operable exact-schema server", () => {
  const capabilities = { settlementRestore: SETTLEMENT_RESTORE_CAPABILITY };
  expect(settlementRestoreMenuItems({ settled: true, canOperate: true, capabilities })).toEqual([
    { id: "undo-settle", title: "Undo settle", image: "arrow.uturn.backward" },
  ]);
  for (const input of [
    { settled: false, canOperate: true, capabilities },
    { settled: true, canOperate: false, capabilities },
    { settled: true, canOperate: true, capabilities: undefined },
  ]) {
    expect(settlementRestoreMenuItems(input)).toEqual([]);
  }
});
