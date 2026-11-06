import { expect, it } from "@effect/vitest";
import { EventId, NodeId, ProviderDriverKind, RunId, ThreadId } from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Result from "effect/Result";
import { SqlError, LockTimeoutError } from "effect/sql/SqlError";
import * as Stream from "effect/Stream";
import {
  loadShellSnapshotParts,
  shellSnapshotForSubscriber,
} from "../../orchestration-v2/ShellStream.ts";
import { subscribeOrchestrationV2Shell } from "../../ws.ts";
import { readRemoteTaskOwnerIds } from "./ShellRetention.ts";
import { createRetentionHarness, now, parent, remoteTask } from "./ShellRetention.testkit.ts";

it.effect.each(["?omitOlderSettled=1", "?omitOlderSettled=1&omitIdleSubagents=1"])(
  "retains a completed remote task's older parent on HTTP reload %s",
  (query) =>
    Effect.gen(function* () {
      const shell = yield* createRetentionHarness();
      const full = yield* shell.http("");
      expect(full.threads).toHaveLength(27);
      const reduced = yield* shell.http(query);
      expect(reduced.threads.map((thread) => thread.id)).toContain(parent.id);
      // Retention does not change the shelf's paging/count contract.
      expect(reduced.olderSettled?.counts).toEqual([{ projectId: parent.projectId, count: 2 }]);
      expect(reduced.threads.filter((thread) => thread.id !== parent.id)).toHaveLength(25);
    }),
);

it.effect.each(["?omitIdleSubagents=1", "?omitOlderSettled=1&omitIdleSubagents=1"])(
  "retains an idle nested owner of completed remote work on HTTP reload %s",
  (query) =>
    Effect.gen(function* () {
      const shell = yield* createRetentionHarness({ nestedOwner: true });
      const full = yield* shell.http("");
      expect(full.threads).toHaveLength(30);
      const reduced = yield* shell.http(query);
      const ids = reduced.threads.map((thread) => thread.id);
      expect(ids).toContain("local-remote-owner");
      expect(ids).toContain("local-middle");
      expect(ids).toContain(parent.id);
      expect(ids).not.toContain("unrelated-idle-sibling");
    }),
);

const flagCases = [
  { omitIdleSubagents: false, omitOlderSettled: false },
  { omitIdleSubagents: true, omitOlderSettled: false },
  { omitIdleSubagents: false, omitOlderSettled: true },
  { omitIdleSubagents: true, omitOlderSettled: true },
];
const queryFor = (flags: (typeof flagCases)[number]) =>
  `?omitIdleSubagents=${Number(flags.omitIdleSubagents)}&omitOlderSettled=${Number(flags.omitOlderSettled)}`;

it.effect.each([false, true])(
  "HTTP and WebSocket flag matrix retains remote owners (nested=%s)",
  (nestedOwner) =>
    Effect.gen(function* () {
      const harness = yield* createRetentionHarness({ nestedOwner });
      const full = yield* harness.http();
      for (const flags of flagCases) {
        const baselineResult = shellSnapshotForSubscriber(full, { ...flags, now });
        const baseline = baselineResult.snapshot;
        const retainedResult = shellSnapshotForSubscriber(full, {
          ...flags,
          now,
          retainedThreadIds: yield* readRemoteTaskOwnerIds(harness.sql),
        });
        expect([...retainedResult.subagentThreadIds]).toEqual([
          ...baselineResult.subagentThreadIds,
        ]);
        for (const actual of [yield* harness.http(queryFor(flags)), yield* harness.ws(flags)]) {
          expect(actual.olderSettled).toEqual(baseline.olderSettled);
          expect(actual.snapshotSequence).toBe(full.snapshotSequence);
          expect(actual.projects).toEqual(full.projects);
          expect(actual.archivedThreads).toEqual([]);
          const ids = actual.threads.map((thread) => thread.id);
          expect(ids).toContain(harness.owner.id);
          expect(ids).toContain(parent.id);
          if (nestedOwner) {
            expect(ids).toContain(harness.middle.id);
            expect(ids.includes(harness.sibling.id)).toBe(!flags.omitIdleSubagents);
          }
          const additions = actual.threads.filter(
            (thread) => !baseline.threads.some((row) => row.id === thread.id),
          );
          expect(additions.map((thread) => thread.id)).toEqual(
            nestedOwner && flags.omitIdleSubagents
              ? [harness.leaf.id]
              : !nestedOwner && flags.omitOlderSettled
                ? [parent.id]
                : [],
          );
          expect(actual.threads.filter((thread) => !additions.includes(thread))).toEqual(
            baseline.threads,
          );
          if (!flags.omitIdleSubagents && !flags.omitOlderSettled) expect(actual).toEqual(full);
        }
      }
    }),
);

it.effect(
  "bound running, completed and failed tasks retain owners; admission failures and local tasks do not",
  () =>
    Effect.gen(function* () {
      const harness = yield* createRetentionHarness({ bind: false });
      const cases = [
        { name: "running", status: "running", driver: "remote", bound: true },
        { name: "completed", status: "completed", driver: "remote", bound: true },
        { name: "failed-bound", status: "failed", driver: "remote", bound: true },
        { name: "failed-admission", status: "failed", driver: "remote", bound: false },
        { name: "completed-local", status: "completed", driver: "codex", bound: true },
      ] as const;
      const owners = cases.map(({ name }) => {
        const id = ThreadId.make(`owner-${name}`);
        return { ...parent, id, lineage: { ...parent.lineage, rootThreadId: id } };
      });
      yield* harness.eventSink.write({
        events: owners.map((thread) => ({
          id: EventId.make(`create-${thread.id}`),
          type: "thread.created" as const,
          threadId: thread.id,
          occurredAt: now,
          payload: thread,
        })),
      });
      yield* harness.eventSink.write({
        events: cases.map((test, index) => ({
          id: EventId.make(`task-${test.name}`),
          type: "subagent.updated" as const,
          threadId: owners[index]!.id,
          occurredAt: now,
          payload: {
            ...remoteTask,
            id: NodeId.make(`task-${test.name}`),
            threadId: owners[index]!.id,
            driver: ProviderDriverKind.make(test.driver),
            status: test.status,
            remoteChild: test.bound ? remoteTask.remoteChild : undefined,
          },
        })),
      });
      const retained = yield* readRemoteTaskOwnerIds(harness.sql);
      expect([...retained].sort()).toEqual(
        owners
          .slice(0, 3)
          .map((row) => row.id)
          .sort(),
      );
      for (const actual of [
        yield* harness.http("?omitOlderSettled=1&omitIdleSubagents=1"),
        yield* harness.ws({ omitIdleSubagents: true, omitOlderSettled: true }),
      ]) {
        const ids = actual.threads.map((row) => row.id);
        for (const [index, owner] of owners.entries())
          expect(ids.includes(owner.id)).toBe(index < 3);
        expect(actual.olderSettled?.counts).toEqual([{ projectId: parent.projectId, count: 7 }]);
      }
    }),
);

it.effect(
  "retention respects archive, unarchive and delete and does not spread to unrelated forks",
  () =>
    Effect.gen(function* () {
      const harness = yield* createRetentionHarness();
      const flags = { omitIdleSubagents: true, omitOlderSettled: true };
      const assertOwner = (present: boolean) =>
        Effect.gen(function* () {
          for (const snapshot of [yield* harness.http(queryFor(flags)), yield* harness.ws(flags)])
            expect(snapshot.threads.some((row) => row.id === parent.id)).toBe(present);
        });
      yield* assertOwner(true);
      for (const [type, archivedAt, deletedAt, expected] of [
        ["thread.archived", now, null, false],
        ["thread.unarchived", null, null, true],
        ["thread.deleted", null, now, false],
      ] as const) {
        yield* harness.eventSink.write({
          events: [
            {
              id: EventId.make(type),
              type,
              threadId: parent.id,
              occurredAt: now,
              payload: { ...parent, archivedAt, deletedAt },
            },
          ],
        });
        yield* assertOwner(expected);
      }
      const forks = [false, true].map((bound) => ({
        ...parent,
        id: ThreadId.make(`fork-${bound}`),
        lineage: {
          parentThreadId: parent.id,
          rootThreadId: parent.id,
          relationshipToParent: "fork" as const,
        },
        forkedFrom: {
          type: "run" as const,
          threadId: parent.id,
          runId: RunId.make("synthetic-source-run"),
        },
      }));
      yield* harness.eventSink.write({
        events: forks.map((thread) => ({
          id: EventId.make(`create-${thread.id}`),
          type: "thread.created" as const,
          threadId: thread.id,
          occurredAt: now,
          payload: thread,
        })),
      });
      yield* harness.eventSink.write({
        events: [
          {
            id: EventId.make("bind-fork"),
            type: "subagent.updated",
            threadId: forks[1]!.id,
            occurredAt: now,
            payload: { ...remoteTask, id: NodeId.make("fork-task"), threadId: forks[1]!.id },
          },
        ],
      });
      for (const actual of [yield* harness.http(queryFor(flags)), yield* harness.ws(flags)]) {
        const ids = actual.threads.map((row) => row.id);
        expect(ids).not.toContain(forks[0]!.id);
        expect(ids).toContain(forks[1]!.id);
        expect(ids).not.toContain(parent.id);
      }
    }),
);

it.effect(
  "HTTP refresh and WS resubscribe observe a newly bound task without removing its owner",
  () =>
    Effect.gen(function* () {
      const harness = yield* createRetentionHarness({ bind: false });
      const flags = { omitIdleSubagents: true, omitOlderSettled: true };
      const before = yield* harness.ws(flags);
      expect(before.threads.map((row) => row.id)).not.toContain(parent.id);
      expect((yield* harness.http(queryFor(flags))).threads.map((row) => row.id)).not.toContain(
        parent.id,
      );
      yield* harness.eventSink.write({
        events: [
          {
            id: EventId.make("late-bind"),
            type: "subagent.updated",
            threadId: parent.id,
            occurredAt: now,
            payload: remoteTask,
          },
        ],
      });
      for (const snapshot of [yield* harness.http(queryFor(flags)), yield* harness.ws(flags)]) {
        expect(snapshot.threads.map((row) => row.id)).toContain(parent.id);
        expect(snapshot.olderSettled).toEqual({
          ...before.olderSettled,
          asOfSequence: snapshot.snapshotSequence,
        });
      }
      const replay = yield* harness.withWs(
        Effect.scoped(
          Effect.gen(function* () {
            const stream = yield* subscribeOrchestrationV2Shell({
              ...flags,
              afterSequence: before.snapshotSequence,
              requestCompletionMarker: true,
            });
            return yield* Stream.runCollect(
              stream.pipe(Stream.takeUntil((item) => item.kind === "synchronized")),
            );
          }),
        ),
      );
      expect(
        Array.from(replay).some(
          (item) => item.kind === "thread.updated" && item.thread.id === parent.id,
        ),
      ).toBe(true);
      expect(
        Array.from(replay).some(
          (item) =>
            (item.kind === "thread.removed" || item.kind === "thread.omitted") &&
            item.threadId === parent.id,
        ),
      ).toBe(false);
    }),
);

it.effect("retained lookup errors stay typed and decoding follows successful raw reads", () =>
  Effect.gen(function* () {
    const harness = yield* createRetentionHarness();
    const failure = new SqlError({ reason: new LockTimeoutError({ cause: null }) });
    let decoded = false;
    const result = yield* loadShellSnapshotParts({
      sql: harness.sql,
      readThreads: Effect.succeed(
        Effect.sync(() => {
          decoded = true;
          return { schemaVersion: 1, snapshotSequence: 17, threads: [], archivedThreads: [] };
        }),
      ),
      listProjects: Effect.succeed([]),
      latestSequence: Effect.succeed(17),
      readRetainedThreadIds: Effect.fail(failure),
    }).pipe(Effect.result);
    expect(Result.isFailure(result)).toBe(true);
    if (Result.isFailure(result)) expect(result.failure).toBe(failure);
    expect(decoded).toBe(false);
    const order: string[] = [];
    const read = yield* loadShellSnapshotParts({
      sql: harness.sql,
      readThreads: Effect.sync(() => {
        order.push("read");
        return Effect.sync(() => {
          order.push("decode");
          return { schemaVersion: 1, snapshotSequence: 17, threads: [], archivedThreads: [] };
        });
      }),
      listProjects: Effect.succeed([]),
      latestSequence: Effect.succeed(17),
      readRetainedThreadIds: Effect.sync(() => {
        order.push("retain");
        return new Set([parent.id]);
      }),
    });
    expect(order).toEqual(["read", "retain", "decode"]);
    expect(read.retainedThreadIds.has(parent.id)).toBe(true);
  }),
);

it.effect("invalid task JSON is excluded without failing HTTP or WS snapshots", () =>
  Effect.gen(function* () {
    const harness = yield* createRetentionHarness();
    const { sql } = harness;
    // A second valid owner must survive even when one persisted payload is corrupt.
    const id = ThreadId.make("valid-second-owner");
    yield* harness.eventSink.write({
      events: [
        {
          id: EventId.make("second-owner"),
          type: "thread.created",
          threadId: id,
          occurredAt: now,
          payload: { ...parent, id, lineage: { ...parent.lineage, rootThreadId: id } },
        },
        {
          id: EventId.make("second-task"),
          type: "subagent.updated",
          threadId: id,
          occurredAt: now,
          payload: { ...remoteTask, id: NodeId.make("second-task"), threadId: id },
        },
      ],
    });
    yield* sql`UPDATE orchestration_v2_projection_subagents SET payload_json = '{invalid' WHERE thread_id = ${parent.id}`;
    expect([...(yield* readRemoteTaskOwnerIds(sql))]).toEqual([id]);
    for (const actual of [
      yield* harness.http("?omitOlderSettled=1"),
      yield* harness.ws({ omitOlderSettled: true }),
    ]) {
      expect(actual.threads.map((row) => row.id)).not.toContain(parent.id);
      expect(actual.threads.map((row) => row.id)).toContain(id);
    }
  }),
);

it.effect(
  "missing retention table fails opted-in HTTP and WS rather than silently dropping owners",
  () =>
    Effect.gen(function* () {
      const harness = yield* createRetentionHarness();
      yield* harness.sql`DROP TABLE orchestration_v2_projection_subagents`;
      // The full snapshot does not opt into the extra read.
      expect((yield* harness.httpResponse()).status).toBe(200);
      expect((yield* harness.ws()).threads.map((row) => row.id)).toContain(parent.id);
      for (const flags of flagCases.slice(1)) {
        expect((yield* harness.httpResponse(queryFor(flags))).status).not.toBe(200);
        const result = yield* harness.ws(flags).pipe(Effect.exit);
        expect(Exit.isFailure(result)).toBe(true);
      }
    }),
);
