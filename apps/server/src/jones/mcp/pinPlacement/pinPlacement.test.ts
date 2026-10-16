import * as NodeCrypto from "@effect/platform-node/NodeCrypto";
import { describe, expect, it } from "@effect/vitest";
import { sortPinnedThreadsByOrderKey } from "@t3tools/client-runtime/state/thread-sort";
import {
  EnvironmentId,
  EventId,
  ProjectId,
  ProviderInstanceId,
  RunId,
  ThreadId,
  type OrchestrationV2AppThread,
  type OrchestrationV2ServerCommand,
  type OrchestrationV2ThreadShell,
  type RuntimeMode,
} from "@t3tools/contracts";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Stream from "effect/Stream";

import * as McpInvocationContext from "../../../mcp/McpInvocationContext.ts";
import * as McpToolAccess from "../../../mcp/McpToolAccess.ts";
import * as ThreadHandlers from "../../../mcp/toolkits/thread/handlers.ts";
import { ThreadToolkit } from "../../../mcp/toolkits/thread/tools.ts";
import { emptyProjection } from "../../../orchestration-v2/ProjectionStore.ts";
import * as ThreadManagement from "../../../orchestration-v2/ThreadManagementService.ts";
import * as ThreadSearch from "../../../orchestration-v2/ThreadSearch.ts";
import { v2PullRequestThread } from "../../../orchestration-v2/testkit/pullRequestFixtures.ts";
import * as ScheduledTasks from "../../../scheduledTasks/ScheduledTaskService.ts";

const providerInstanceId = ProviderInstanceId.make("codex");
const callerId = ThreadId.make("thread:caller");

interface ShellOptions {
  readonly pinOrderKey?: string | null;
  readonly pinned?: boolean;
  readonly runtimeMode?: RuntimeMode;
  readonly archived?: boolean;
  readonly minute?: number;
}

const shell = (id: string, options: ShellOptions = {}): OrchestrationV2ThreadShell => {
  const createdAt = `2026-10-16T10:${String(options.minute ?? 0).padStart(2, "0")}:00Z`;
  const base = v2PullRequestThread({
    id: ThreadId.make(id),
    projectId: ProjectId.make("project:pin-placement"),
    title: id,
    modelSelection: { instanceId: providerInstanceId, model: "gpt-5" },
    runtimeMode: options.runtimeMode ?? "full-access",
    interactionMode: "default",
    branch: null,
    worktreePath: null,
    pullRequests: [],
    latestUserMessageAt: null,
    createdAt,
    updatedAt: createdAt,
    archivedAt: options.archived ? createdAt : null,
    settledOverride: null,
    settledAt: null,
  });
  const pinned = options.pinned ?? options.pinOrderKey !== undefined;
  return {
    ...base,
    pinnedAt: pinned ? DateTime.makeUnsafe(createdAt) : null,
    pinOrderKey: pinned ? (options.pinOrderKey ?? null) : null,
  };
};

/**
 * An environment whose dispatch applies pin commands the way the orchestrator
 * does: a fresh pin takes the offered key, a re-pin keeps its key, and a
 * reorder on an unpinned thread is a defect the planner must never cause.
 */
const makeEnvironment = (
  threads: ReadonlyArray<OrchestrationV2ThreadShell>,
  callerMode: RuntimeMode = "full-access",
) => {
  const shells = new Map<ThreadId, OrchestrationV2ThreadShell>(
    threads.map((thread) => [thread.id, thread]),
  );
  shells.set(callerId, {
    ...shell(callerId, { runtimeMode: callerMode, minute: 59 }),
    activeRunId: RunId.make("run:caller"),
  });
  const commands: OrchestrationV2ServerCommand[] = [];
  const scope: McpInvocationContext.McpInvocationScope = {
    environmentId: EnvironmentId.make("environment:pin-placement"),
    requestNamespace: "session:pin-placement",
    thread: { threadId: callerId, providerSessionId: "session:pin-placement", providerInstanceId },
    client: undefined,
    capabilities: new Set(["orchestration"]),
    issuedAt: 1,
  };
  const records = (id: ThreadId) => {
    const thread: OrchestrationV2AppThread = { ...shells.get(id)!, lastVisitedAt: null };
    return emptyProjection({
      type: "thread.created",
      id: EventId.make(`fixture:${id}`),
      threadId: id,
      occurredAt: thread.createdAt,
      payload: thread,
    });
  };
  const apply = (command: OrchestrationV2ServerCommand) => {
    if (command.type !== "thread.pin" && command.type !== "thread.pin.reorder") return;
    const current = shells.get(command.threadId)!;
    const pinned = current.pinnedAt != null;
    if (command.type === "thread.pin.reorder") {
      if (!pinned) throw new Error(`reorder of unpinned ${command.threadId}`);
      shells.set(command.threadId, { ...current, pinOrderKey: command.orderKey });
      return;
    }
    shells.set(command.threadId, {
      ...current,
      pinnedAt: pinned ? current.pinnedAt : DateTime.makeUnsafe("2026-10-16T12:00:00Z"),
      ...(pinned || command.orderKey === undefined ? {} : { pinOrderKey: command.orderKey }),
    });
  };
  const dependencies = Layer.mergeAll(
    NodeCrypto.layer,
    Layer.mock(ThreadSearch.ThreadSearch)({}),
    Layer.mock(ScheduledTasks.ScheduledTaskService)({}),
    Layer.mock(ThreadManagement.ThreadManagementService)({
      getThreadShell: (id) => Effect.succeed(shells.get(id) ?? null),
      getProjectThreadRecords: ({ threadId }) => Effect.succeed(records(threadId)),
      getShellSnapshot: (options) =>
        Effect.sync(() => {
          const all = [...shells.values()];
          const archived = all.filter((thread) => thread.archivedAt !== null);
          return {
            schemaVersion: 1,
            snapshotSequence: commands.length,
            threads:
              options?.location === "archive"
                ? []
                : all.filter((thread) => thread.archivedAt === null),
            archivedThreads: options?.location === "archive" ? archived : [],
          };
        }),
      dispatch: (command) =>
        Effect.sync(() => {
          commands.push(command);
          apply(command);
          return { sequence: commands.length, storedEvents: [] };
        }),
    }),
  );
  const invoke = (args: Record<string, unknown>) =>
    Effect.gen(function* () {
      const toolkit = yield* ThreadToolkit.pipe(
        Effect.provide(
          McpToolAccess.HandlersLayer.layer(ThreadHandlers.layer).pipe(Layer.provide(dependencies)),
        ),
      );
      return yield* toolkit.handle("t3_thread_organize", args as never).pipe(
        Stream.unwrap,
        Stream.runCollect,
        Effect.map((results) => results.at(-1)!),
        Effect.provideService(McpInvocationContext.McpInvocationContext, scope),
        Effect.provide(dependencies),
      );
    });
  /** The pinned block exactly as the sidebar sorts it. */
  const pinnedOrder = () =>
    sortPinnedThreadsByOrderKey(
      [...shells.values()]
        .filter((thread) => thread.pinnedAt != null && thread.archivedAt === null)
        .map((thread) => ({
          id: thread.id as string,
          createdAt: DateTime.formatIso(thread.createdAt),
          pinOrderKey: thread.pinOrderKey ?? null,
        })),
    ).map((thread) => thread.id);
  const keyOf = (id: string) => shells.get(ThreadId.make(id))?.pinOrderKey ?? null;
  const commandTargets = () =>
    commands.map((command) => ("threadId" in command ? command.threadId : null));
  return { shells, commands, invoke, pinnedOrder, keyOf, commandTargets };
};

const below = (anchorThreadId: string) => ({ position: "below", anchorThreadId });

describe("t3_thread_organize placement", () => {
  it.effect("pins below an anchor whose next neighbor is keyed with one write", () =>
    Effect.gen(function* () {
      const env = makeEnvironment([
        shell("x", { pinOrderKey: "f" }),
        shell("anchor", { pinOrderKey: "m" }),
        shell("y", { pinOrderKey: "t" }),
        shell("target"),
      ]);
      const result = yield* env.invoke({
        action: "pin",
        threadId: "target",
        placement: below("anchor"),
      });
      expect(result.isFailure).toBe(false);
      expect(env.pinnedOrder()).toEqual(["x", "anchor", "target", "y"]);
      expect(env.commands).toHaveLength(1);
      expect(env.commands[0]).toMatchObject({ type: "thread.pin", threadId: "target" });
      expect(result.result).toEqual({
        sequence: 1,
        pinOrderKey: env.keyOf("target"),
        reorderedThreads: [],
      });
      // The MCP response is the encoded result; it must keep the readback fields.
      expect(result.encodedResult).toEqual(result.result);
      expect(env.keyOf("target")! > "m" && env.keyOf("target")! < "t").toBe(true);
    }),
  );

  it.effect("chains five pins below each other under the observed long anchor key", () =>
    Effect.gen(function* () {
      const children = ["a1", "b2", "c3", "d4", "e5"];
      const env = makeEnvironment([
        shell("anchor", { pinOrderKey: `${"a".repeat(149)}b` }),
        ...children.map((id, index) => shell(id, { minute: index + 1 })),
      ]);
      let previous = "anchor";
      for (const id of children) {
        const result = yield* env.invoke({
          action: "pin",
          threadId: id,
          placement: below(previous),
        });
        expect(result.isFailure).toBe(false);
        previous = id;
      }
      expect(env.pinnedOrder()).toEqual(["anchor", ...children]);
    }),
  );

  it.effect("chains below a keyed anchor with keyed and keyless neighbors after it", () =>
    Effect.gen(function* () {
      const children = ["a1", "b2", "c3", "d4", "e5"];
      const env = makeEnvironment([
        shell("anchor", { pinOrderKey: "m" }),
        shell("y", { pinOrderKey: "n" }),
        shell("old-keyless", { pinned: true, minute: 1 }),
        ...children.map((id, index) => shell(id, { minute: index + 10 })),
      ]);
      let previous = "anchor";
      for (const id of children) {
        yield* env.invoke({ action: "pin", threadId: id, placement: below(previous) });
        previous = id;
      }
      expect(env.pinnedOrder()).toEqual(["anchor", ...children, "y", "old-keyless"]);
    }),
  );

  it.effect(
    "lands after every thread that shares the anchor's key, and above lands before them",
    () =>
      Effect.gen(function* () {
        const env = makeEnvironment([
          shell("before", { pinOrderKey: "c" }),
          // Delegated children share their parent's key and sort around it by id.
          shell("anchor", { pinOrderKey: "m" }),
          shell("aa-child", { pinOrderKey: "m" }),
          shell("zz-child", { pinOrderKey: "m" }),
          shell("after", { pinOrderKey: "t" }),
          shell("under"),
          shell("over"),
        ]);
        expect(env.pinnedOrder()).toEqual(["before", "aa-child", "anchor", "zz-child", "after"]);
        yield* env.invoke({ action: "pin", threadId: "under", placement: below("anchor") });
        yield* env.invoke({
          action: "pin",
          threadId: "over",
          placement: { position: "above", anchorThreadId: "anchor" },
        });
        expect(env.pinnedOrder()).toEqual([
          "before",
          "over",
          "aa-child",
          "anchor",
          "zz-child",
          "under",
          "after",
        ]);
        // Single writes: the shared-key group keeps its key.
        expect(env.commandTargets()).toEqual(["under", "over"]);
        expect(env.keyOf("aa-child")).toBe("m");
        expect(env.keyOf("zz-child")).toBe("m");
      }),
  );

  it.effect("materializes keyless neighbors without changing their relative order", () =>
    Effect.gen(function* () {
      const env = makeEnvironment([
        shell("keyed", { pinOrderKey: "g" }),
        shell("newest", { pinned: true, minute: 30 }),
        shell("middle", { pinned: true, minute: 20 }),
        shell("oldest", { pinned: true, minute: 10 }),
        shell("target", { minute: 40 }),
      ]);
      expect(env.pinnedOrder()).toEqual(["keyed", "newest", "middle", "oldest"]);
      const result = yield* env.invoke({
        action: "pin",
        threadId: "target",
        placement: below("middle"),
      });
      expect(result.isFailure).toBe(false);
      expect(env.pinnedOrder()).toEqual(["keyed", "newest", "middle", "target", "oldest"]);
      for (const id of ["keyed", "newest", "middle", "target", "oldest"]) {
        expect(env.keyOf(id)).not.toBeNull();
      }
      // Neighbors are rewritten with reorders first; the target's pin comes last.
      expect(env.commands.at(-1)).toMatchObject({ type: "thread.pin", threadId: "target" });
      expect(
        env.commands.slice(0, -1).every((command) => command.type === "thread.pin.reorder"),
      ).toBe(true);
      const output = result.result as {
        pinOrderKey: string;
        reorderedThreads: ReadonlyArray<{ threadId: string; pinOrderKey: string | null }>;
      };
      expect(output.pinOrderKey).toBe(env.keyOf("target"));
      expect(output.reorderedThreads.map((thread) => thread.threadId).toSorted()).toEqual(
        env.commandTargets().slice(0, -1).toSorted(),
      );
      for (const thread of output.reorderedThreads) {
        expect(thread.pinOrderKey).toBe(env.keyOf(thread.threadId));
      }
    }),
  );

  it.effect("moves an already pinned target with a reorder, and repeats in place", () =>
    Effect.gen(function* () {
      const env = makeEnvironment([
        shell("target", { pinOrderKey: "c" }),
        shell("anchor", { pinOrderKey: "m" }),
        shell("y", { pinOrderKey: "t" }),
      ]);
      yield* env.invoke({ action: "pin", threadId: "target", placement: below("anchor") });
      expect(env.commands).toEqual([
        expect.objectContaining({ type: "thread.pin.reorder", threadId: "target" }),
      ]);
      expect(env.pinnedOrder()).toEqual(["anchor", "target", "y"]);
      yield* env.invoke({ action: "pin", threadId: "target", placement: below("anchor") });
      expect(env.pinnedOrder()).toEqual(["anchor", "target", "y"]);
    }),
  );

  it.effect("places at the top and bottom of the pinned block", () =>
    Effect.gen(function* () {
      const env = makeEnvironment([
        shell("first", { pinOrderKey: "f" }),
        shell("second", { pinOrderKey: "t" }),
        shell("keyless", { pinned: true }),
        shell("top"),
        shell("bottom"),
      ]);
      yield* env.invoke({ action: "pin", threadId: "top", placement: { position: "top" } });
      expect(env.commands).toHaveLength(1);
      yield* env.invoke({ action: "pin", threadId: "bottom", placement: { position: "bottom" } });
      expect(env.pinnedOrder()).toEqual(["top", "first", "second", "keyless", "bottom"]);
    }),
  );

  it.effect("rejects bad anchors without dispatching", () =>
    Effect.gen(function* () {
      const env = makeEnvironment([
        shell("target"),
        shell("unpinned"),
        shell("archived", { pinOrderKey: "m", archived: true }),
        shell("pinned", { pinOrderKey: "m" }),
      ]);
      const cases: ReadonlyArray<readonly [Record<string, unknown>, string]> = [
        [{ placement: { position: "below" } }, "invalid_request"],
        [{ placement: { position: "top", anchorThreadId: "pinned" } }, "invalid_request"],
        [{ placement: below("target") }, "invalid_request"],
        [{ placement: below("unpinned") }, "invalid_request"],
        [{ placement: below("archived") }, "invalid_request"],
        [{ placement: below("missing") }, "thread_not_found"],
        [{ action: "unpin", placement: below("pinned") }, "invalid_request"],
      ];
      for (const [args, code] of cases) {
        const result = yield* env.invoke({ action: "pin", threadId: "target", ...args });
        expect(result.isFailure, JSON.stringify(args)).toBe(true);
        expect(result.result, JSON.stringify(args)).toMatchObject({ code });
      }
      expect(env.commands).toEqual([]);
    }),
  );

  it.effect("checks the anchor and rewritten neighbors against the caller's modes", () =>
    Effect.gen(function* () {
      const env = makeEnvironment(
        [
          shell("target", { runtimeMode: "approval-required" }),
          shell("anchor", { pinOrderKey: "m", runtimeMode: "full-access" }),
          shell("own-anchor", { pinned: true, runtimeMode: "approval-required", minute: 2 }),
          shell("broad-keyless", { pinned: true, runtimeMode: "full-access", minute: 1 }),
        ],
        "approval-required",
      );
      const anchored = yield* env.invoke({
        action: "pin",
        threadId: "target",
        placement: below("anchor"),
      });
      expect(anchored.isFailure).toBe(true);
      expect(anchored.result).toMatchObject({ code: "runtime_mode_escalation_denied" });
      // Materializing keyless neighbors would rewrite a thread above the caller's modes.
      const rewriting = yield* env.invoke({
        action: "pin",
        threadId: "target",
        placement: below("own-anchor"),
      });
      expect(rewriting.isFailure).toBe(true);
      expect(rewriting.result).toMatchObject({ code: "runtime_mode_escalation_denied" });
      expect(env.commands).toEqual([]);
    }),
  );

  it.effect("keeps a pin without placement unchanged", () =>
    Effect.gen(function* () {
      const env = makeEnvironment([shell("anchor", { pinOrderKey: "m" }), shell("target")]);
      const result = yield* env.invoke({ action: "pin", threadId: "target" });
      expect(result.result).toEqual({ sequence: 1 });
      expect(env.commands).toEqual([
        { type: "thread.pin", commandId: expect.any(String), threadId: "target" },
      ]);
      expect(env.keyOf("target")).toBeNull();
    }),
  );
});
