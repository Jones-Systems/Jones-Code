import { describe, expect, it } from "@effect/vitest";
import {
  CommandId,
  EnvironmentId,
  ProjectId,
  ProviderInstanceId,
  ThreadId,
  type OrchestrationV2ShellSnapshot,
} from "@t3tools/contracts";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as SecureRandom from "effect/crypto/SecureRandom";
import { Atom, AtomRegistry } from "effect/reactivity";

import type { SettleThreadInput } from "../operations/commands.ts";
import {
  makeThreadTriageOutboxStore,
  ThreadTriageOutboxStore,
} from "../platform/threadTriageOutboxStore.ts";
import { EnvironmentRpcUnavailableError } from "../rpc/client.ts";
import type { EnvironmentShellStatus } from "./shell.ts";
import { createThreadTriageOutbox } from "./threadTriageOutbox.ts";

const environmentId = EnvironmentId.make("remote");
const threadId = ThreadId.make("thread");
const now = DateTime.makeUnsafe("2026-09-12T10:00:00.000Z");
const providerInstanceId = ProviderInstanceId.make("codex");
const snapshot: OrchestrationV2ShellSnapshot = {
  snapshotSequence: 1,
  schemaVersion: 2,
  archivedThreads: [],
  projects: [],
  threads: [
    {
      id: threadId,
      projectId: ProjectId.make("project"),
      title: "Thread",
      modelSelection: { instanceId: providerInstanceId, model: "gpt-5.4" },
      runtimeMode: "full-access",
      interactionMode: "default",
      branch: null,
      worktreePath: null,
      createdBy: "user",
      creationSource: "web",
      providerInstanceId,
      lineage: { rootThreadId: threadId, parentThreadId: null, relationshipToParent: null },
      forkedFrom: null,
      activeProviderThreadId: null,
      latestRunId: null,
      activeRunId: null,
      status: "idle",
      pendingRuntimeRequest: null,
      pendingBackgroundTasks: [],
      providerInstanceHistory: [],
      latestVisibleMessage: null,
      itemCount: 0,
      visibleItemCount: 0,
      deletedAt: null,
      createdAt: now,
      updatedAt: now,
      archivedAt: null,
      settledOverride: null,
      settledAt: null,
      pullRequests: [],
      latestUserMessageAt: null,
      hasActionableProposedPlan: false,
    },
  ],
};

describe("offline Settle", () => {
  it.effect("queues only a settle that needs neither an exact receipt nor a guard", () =>
    Effect.gen(function* () {
      const registry = yield* Effect.acquireRelease(Effect.sync(AtomRegistry.make), (registry) =>
        Effect.sync(() => registry.dispose()),
      );
      let stored: string | null = null;
      const store = makeThreadTriageOutboxStore((next) =>
        Effect.sync(() => {
          stored = next(stored) ?? stored;
        }),
      );
      const snapshotAtom = Atom.make<OrchestrationV2ShellSnapshot | null>(snapshot);
      const statusAtom = Atom.make<EnvironmentShellStatus>("cached");
      const outbox = createThreadTriageOutbox({
        runtime: Atom.runtime(
          Layer.merge(
            Layer.succeed(
              SecureRandom.SecureRandom,
              SecureRandom.make((size) => new Uint8Array(size)),
            ),
            Layer.succeed(ThreadTriageOutboxStore, store),
          ),
        ),
        sourceSnapshotAtom: () => snapshotAtom,
        shellStatusAtom: () => statusAtom,
        apply: (thread) => ({ ...thread, settledOverride: "settled" }),
        send: () => Effect.die("Offline changes cannot be replayed"),
      });
      const unavailable = new EnvironmentRpcUnavailableError({
        environmentId,
        message: "Test environment is offline.",
      });
      const settle = outbox.queueWhenOffline<SettleThreadInput, EnvironmentRpcUnavailableError>(
        { label: "test:settle", run: () => Effect.fail(unavailable) },
        () => ({ type: "settle" }),
      );
      const exact = { threadId, commandId: CommandId.make("exact-settle") };
      const guard = {
        policy: "preserve_pending_work",
        expectedUpdatedAt: "2026-09-12T10:00:00.000Z",
      } as const;
      for (const input of [exact, { threadId, guard }]) {
        expect(yield* Effect.flip(settle.run(registry, { environmentId, input }))).toBe(
          unavailable,
        );
      }
      expect(yield* store.update((entries) => entries)).toEqual([]);
      yield* settle.run(registry, { environmentId, input: { threadId } });
      expect(yield* store.update((entries) => entries)).toMatchObject([
        { threadId, action: { type: "settle" } },
      ]);
    }),
  );
});
