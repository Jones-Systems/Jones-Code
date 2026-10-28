import { expect, it } from "@effect/vitest";
import { EnvironmentId, ThreadId, type OrchestrationV2ThreadShell } from "@t3tools/contracts";
import * as NodeSqliteClient from "@t3tools/shared/nodeSqliteClient";
import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";
import * as Deferred from "effect/Deferred";
import * as Fiber from "effect/Fiber";
import * as NodeServices from "@effect/platform-node/NodeServices";
import * as FileSystem from "effect/FileSystem";
import * as Path from "effect/Path";
import { RecordedAppThreadJson } from "../../orchestration-v2/RecordedTypes.ts";
import * as Layer from "effect/Layer";
import * as SqlClient from "effect/sql/SqlClient";
import * as Environment from "../../environment/ServerEnvironment.ts";
import * as Threads from "../../orchestration-v2/ThreadManagementService.ts";
import migration from "../persistence/Migrations/122_JonesThreadLinks.ts";
import * as LinkedThreads from "./LinkedThreads.ts";
import { idleThreadProjection, liveThreadShell } from "../../mcp/McpToolAccess.testkit.ts";
import * as DateTime from "effect/DateTime";
import {
  MessageId,
  type OrchestrationV2ThreadProjection,
  type OrchestrationV2TurnItem,
} from "@t3tools/contracts";
import { linkedThreadIdentity } from "./identity.ts";

const here = EnvironmentId.make("env:local");
const away = EnvironmentId.make("env:remote");
const endpoint = (id: string, environmentId = here) => ({
  environmentId,
  threadId: ThreadId.make(id),
});
const a = endpoint("a"),
  b = endpoint("b"),
  c = endpoint("c");
const user = { type: "user" } as const;
const encodeThread = Schema.encodeSync(Schema.fromJsonString(RecordedAppThreadJson));
const insertThread = (id: string) =>
  Effect.gen(function* () {
    const sql = yield* SqlClient.SqlClient;
    const payload = encodeThread(idleThreadProjection(liveThreadShell(ThreadId.make(id))).thread);
    yield* sql`INSERT INTO orchestration_v2_projection_threads VALUES (${id}, NULL, ${payload})`;
  });
const setup = Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  yield* sql`PRAGMA journal_mode = WAL`;
  yield* migration;
  yield* sql`CREATE TABLE orchestration_v2_projection_threads (thread_id TEXT PRIMARY KEY, deleted_at TEXT, payload_json TEXT NOT NULL)`;
  for (const id of ["a", "b", "c"]) yield* insertThread(id);
});
const shell = (id: ThreadId) =>
  Effect.gen(function* () {
    const sql = yield* SqlClient.SqlClient;
    const row = (yield* sql<{
      thread_id: string;
      deleted_at: string | null;
    }>`SELECT * FROM orchestration_v2_projection_threads WHERE thread_id = ${id}`)[0];
    return row === undefined
      ? null
      : ({
          id,
          deletedAt: row.deleted_at,
          runtimeMode: "full-access",
          interactionMode: "default",
        } as OrchestrationV2ThreadShell);
  });
const dependencies = (overrides: Partial<Threads.ThreadManagementService["Service"]> = {}) =>
  Layer.mergeAll(
    Layer.succeed(Environment.ServerEnvironment, {
      getEnvironmentId: Effect.succeed(here),
      getDescriptor: Effect.die("not used"),
    }),
    Layer.effect(
      Threads.ThreadManagementService,
      Effect.gen(function* () {
        const sql = yield* SqlClient.SqlClient;
        return {
          getThreadShell: (id: ThreadId) =>
            shell(id).pipe(Effect.provideService(SqlClient.SqlClient, sql)),
          ...overrides,
        } as Threads.ThreadManagementService["Service"];
      }),
    ),
  );
const run = <A, E>(
  program: Effect.Effect<A, E, LinkedThreads.LinkedThreads | SqlClient.SqlClient>,
  overrides: Partial<Threads.ThreadManagementService["Service"]> = {},
  filename = ":memory:",
) =>
  Effect.gen(function* () {
    yield* setup;
    return yield* program.pipe(
      Effect.provide(LinkedThreads.layer.pipe(Layer.provide(dependencies(overrides)))),
    );
  }).pipe(
    Effect.provide(
      NodeSqliteClient.layer({
        filename,
        readerWorker: filename !== ":memory:",
        readerFailureMode: "fail",
      }),
    ),
  );

it.effect("keeps launch claims invisible until creation, and never revives a removed claim", () =>
  run(
    Effect.gen(function* () {
      const service = yield* LinkedThreads.LinkedThreads;
      const input = {
        from: a,
        toThreadId: ThreadId.make("pending"),
        launchCommandId: "create:pending",
        actor: user,
      };
      const id = yield* service.claimLaunch(input);
      expect((yield* service.list(user, { thread: a })).links).toEqual([]);
      yield* insertThread("pending");
      expect((yield* service.list(user, { thread: a })).links.map((link) => link.linkId)).toEqual([
        id,
      ]);
      yield* service.unlink(user, { linkId: id });
      yield* service.claimLaunch(input);
      expect((yield* service.list(user, { thread: a })).links).toEqual([]);
    }),
  ),
);

it.effect(
  "rejects self links and cycles, preserves acceptance, and requires explicit relink after unlink",
  () =>
    run(
      Effect.gen(function* () {
        const service = yield* LinkedThreads.LinkedThreads;
        expect(
          (yield* service
            .link(user, { kind: "peer", from: a, to: a, basis: "manual" })
            .pipe(Effect.flip)).reason,
        ).toBe("self_link");
        const first = yield* service.link(user, {
          kind: "continuation",
          from: a,
          to: b,
          basis: "manual",
        });
        yield* service.link(user, { kind: "continuation", from: b, to: c, basis: "manual" });
        expect(
          (yield* service
            .link(user, { kind: "continuation", from: c, to: a, basis: "manual" })
            .pipe(Effect.flip)).reason,
        ).toBe("cycle");
        expect(
          (yield* service
            .accept({ type: "thread", threadId: a.threadId }, { from: a, to: b })
            .pipe(Effect.flip)).reason,
        ).toBe("not_receiver");
        yield* service.accept({ type: "thread", threadId: b.threadId }, { from: a, to: b });
        expect(
          (yield* service.link(user, { kind: "continuation", from: a, to: b, basis: "handoff" }))
            .link.acceptance,
        ).toBe("accepted");
        yield* service.unlink(user, { linkId: first.link.linkId });
        expect((yield* service.accept(user, { from: a, to: b }).pipe(Effect.flip)).reason).toBe(
          "not_found",
        );
        expect(
          (yield* service.link(user, { kind: "continuation", from: a, to: b, basis: "manual" }))
            .link.acceptance,
        ).toBe("accepted");
      }),
    ),
);

it.effect("accept creates a missing receiver copy and linked actors need an exact origin", () =>
  run(
    Effect.gen(function* () {
      const service = yield* LinkedThreads.LinkedThreads;
      const from = endpoint("foreign", away);
      expect((yield* service.accept(user, { from, to: a })).link.acceptance).toBe("accepted");
      const actor = {
        type: "linked",
        sessionId: "link-session",
        label: "Peer",
        origin: null,
        fenced: false,
      } as const;
      expect(
        (yield* service
          .link(actor, { kind: "peer", from, to: a, basis: "manual" })
          .pipe(Effect.flip)).reason,
      ).toBe("forbidden");
      expect(
        (yield* service
          .link(
            { ...actor, origin: endpoint("other", away) },
            { kind: "peer", from, to: a, basis: "manual" },
          )
          .pipe(Effect.flip)).reason,
      ).toBe("forbidden");
      expect(
        (yield* service
          .link(
            { ...actor, origin: from, fenced: true },
            { kind: "peer", from, to: a, basis: "manual" },
          )
          .pipe(Effect.flip)).reason,
      ).toBe("forbidden");
      expect(
        (yield* service.link(
          { ...actor, origin: from },
          { kind: "peer", from, to: a, basis: "manual" },
        )).link.reverse,
      ).toBe("recorded");
    }),
  ),
);

it.effect("rejects origin-side user acceptance and requires the exact linked receiver", () =>
  run(
    Effect.gen(function* () {
      const service = yield* LinkedThreads.LinkedThreads;
      const to = endpoint("receiver", away);
      const input = { from: a, to };
      const actor = {
        type: "linked",
        sessionId: "receiver-session",
        label: "Peer",
        origin: to,
        fenced: false,
      } as const;
      expect((yield* service.accept(user, input).pipe(Effect.flip)).reason).toBe("not_receiver");
      for (const origin of [null, a, endpoint("wrong-receiver", away)]) {
        expect((yield* service.accept({ ...actor, origin }, input).pipe(Effect.flip)).reason).toBe(
          "not_receiver",
        );
      }
      expect((yield* service.list(user, { thread: a })).links).toEqual([]);
      const accepted = yield* service.accept(actor, input);
      expect(accepted.link.acceptance).toBe("accepted");
      expect(accepted.link.reverse).toBe("recorded");
    }),
  ),
);

it.effect("acceptance preserves evidence of the other copy independently of synchronization", () =>
  run(
    Effect.gen(function* () {
      const service = yield* LinkedThreads.LinkedThreads;
      const cases = [
        { before: "recorded", next: "unavailable", expected: "recorded" },
        { before: "recorded", next: "unknown", expected: "recorded" },
        { before: "recorded", next: undefined, expected: "recorded" },
        { before: "unknown", next: "unavailable", expected: "unavailable" },
        { before: "unknown", next: "recorded", expected: "recorded" },
        { before: "unavailable", next: "unknown", expected: "unavailable" },
        { before: "unavailable", next: "recorded", expected: "recorded" },
      ] as const;
      for (const [index, testCase] of cases.entries()) {
        const from = endpoint(`foreign-${index}`, away);
        yield* service.link(user, {
          kind: "continuation",
          from,
          to: a,
          basis: "handoff",
          reverse: testCase.before,
        });
        const accepted = yield* service.accept(user, {
          from,
          to: a,
          ...(testCase.next === undefined ? {} : { reverse: testCase.next }),
        });
        expect(accepted.link.acceptance).toBe("accepted");
        expect(accepted.link.reverse).toBe(testCase.expected);
        expect(
          (yield* service.list(user, { thread: a })).links.find(
            (link) => link.linkId === accepted.link.linkId,
          )?.reverse,
        ).toBe(testCase.expected);
      }
      expect(
        (yield* service.accept(user, { from: b, to: c, reverse: "unavailable" })).link.reverse,
      ).toBe("local");
    }),
  ),
);

it.effect("lists deduplicated siblings with truncation and hides deleted endpoints", () =>
  run(
    Effect.gen(function* () {
      const service = yield* LinkedThreads.LinkedThreads;
      yield* service.claimLaunch({
        from: a,
        toThreadId: b.threadId,
        launchCommandId: "create:b",
        actor: user,
      });
      yield* service.claimLaunch({
        from: a,
        toThreadId: c.threadId,
        launchCommandId: "create:c",
        actor: user,
      });
      yield* service.claimLaunch({
        from: a,
        toThreadId: b.threadId,
        launchCommandId: "create:b",
        actor: user,
      });
      expect((yield* service.list(user, { thread: a })).links.length).toBe(2);
      expect(
        (yield* service.list(user, { thread: b })).siblings.map((s) => s.link.to.threadId),
      ).toEqual([c.threadId]);
      expect((yield* service.list(user, { thread: a, limit: 1 })).truncated).toBe(true);
      const sql = yield* SqlClient.SqlClient;
      yield* sql`UPDATE orchestration_v2_projection_threads SET deleted_at = 'now' WHERE thread_id = 'c'`;
      expect((yield* service.list(user, { thread: b })).siblings).toEqual([]);
    }),
  ),
);

it("pins peer identity ordering and directed identity separation", () => {
  expect(linkedThreadIdentity("peer", a, b)).toEqual(linkedThreadIdentity("peer", b, a));
  expect(linkedThreadIdentity("launch", a, b).linkId).not.toBe(
    linkedThreadIdentity("launch", b, a).linkId,
  );
  expect(linkedThreadIdentity("continuation", a, b).linkId).not.toBe(
    linkedThreadIdentity("launch", a, b).linkId,
  );
});

it.effect("recovers only exact per-thread evidence and preserves removal", () => {
  const base = (id: string): OrchestrationV2ThreadProjection => {
    const projection = idleThreadProjection(liveThreadShell(ThreadId.make(id)));
    return {
      ...projection,
      thread: { ...projection.thread, createdBy: "agent", creationSource: "mcp" },
    };
  };
  const message = (id: string, threadId: string) => ({
    id: MessageId.make(id),
    threadId: ThreadId.make(threadId),
    senderThreadId: b.threadId,
    role: "user" as const,
    text: "task",
    attachments: [],
    streaming: false,
    createdBy: "agent" as const,
    creationSource: "mcp" as const,
    runId: null,
    nodeId: null,
    createdAt: DateTime.makeUnsafe("2026-10-24T12:00:00Z"),
    updatedAt: DateTime.makeUnsafe("2026-10-24T12:00:00Z"),
  });
  const created = { type: "thread_created", targetThreadId: c.threadId } as Extract<
    OrchestrationV2TurnItem,
    { type: "thread_created" }
  >;
  const reads: string[] = [];
  const projections = new Map<string, OrchestrationV2ThreadProjection>([
    ["a", { ...base("a"), messages: [message("a", "a")] }],
    ["b", { ...base("b"), turnItems: [created] }],
    ["c", { ...base("c"), messages: [message("first-c", "c")] }],
  ]);
  return run(
    Effect.gen(function* () {
      const service = yield* LinkedThreads.LinkedThreads;
      const exact = yield* service.recover(user, { threadId: a.threadId });
      expect(exact.evidence).toBe("launch_first_message");
      expect((yield* service.recover(user, { threadId: c.threadId })).evidence).toBe(
        "created_thread_record",
      );
      expect((yield* service.recover(user, { threadId: b.threadId })).outcome).toBe(
        "no_exact_evidence",
      );
      expect((yield* service.recover(user, { threadId: a.threadId })).outcome).toBe(
        "already_recorded",
      );
      yield* service.unlink(user, { linkId: exact.link!.linkId });
      expect((yield* service.recover(user, { threadId: a.threadId })).outcome).toBe(
        "previously_removed",
      );
      expect(reads.every((id) => ["a", "b", "c"].includes(id))).toBe(true);
    }),
    {
      getThreadProjection: (id) => {
        reads.push(id);
        return Effect.succeed(projections.get(id)!);
      },
    },
  );
});

it.effect("records old-peer origins one-sided and retains tombstones on replay", () =>
  run(
    Effect.gen(function* () {
      const service = yield* LinkedThreads.LinkedThreads;
      const request = {
        from: a,
        to: endpoint("remote", away),
        title: "Remote",
        environmentLabel: "Peer",
        actor: user,
      };
      const old = yield* service.recordRemoteLaunch(request);
      expect(old).toMatchObject({ state: "recorded", reverse: "unavailable" });
      expect((yield* service.list(user, { thread: a })).links.length).toBe(1);
      yield* service.unlink(user, { linkId: old.linkId });
      yield* service.recordRemoteLaunch({
        ...request,
        reported: { linkId: old.linkId, state: "recorded" },
      });
      expect((yield* service.list(user, { thread: a })).links).toEqual([]);
    }),
  ),
);

it.effect("authorizes list reads from metadata without constructing historical shells", () =>
  run(
    Effect.gen(function* () {
      const service = yield* LinkedThreads.LinkedThreads;
      const sql = yield* SqlClient.SqlClient;
      const remote = endpoint("origin", away);
      const actor = {
        type: "linked",
        sessionId: "session",
        label: "Peer",
        origin: remote,
        fenced: true,
      } as const;
      expect(
        (yield* service.list(user, { thread: endpoint("missing") }).pipe(Effect.flip)).reason,
      ).toBe("endpoint_not_found");
      expect((yield* service.list(actor, { thread: a }).pipe(Effect.flip)).reason).toBe(
        "forbidden",
      );
      yield* sql`UPDATE orchestration_v2_projection_threads SET payload_json = json_set(payload_json, '$.linkOrigin', json(${JSON.stringify({ sessionId: "session", label: "Peer" })})) WHERE thread_id = 'a'`;
      expect((yield* service.list(actor, { thread: a })).links).toEqual([]);
      expect(
        (yield* service.list({ ...actor, sessionId: "other" }, { thread: a }).pipe(Effect.flip))
          .reason,
      ).toBe("forbidden");
      yield* sql`UPDATE orchestration_v2_projection_threads SET payload_json = json_set(payload_json, '$.deletedAt', '2026-01-01T00:00:00.000Z'), deleted_at = '2026-01-01T00:00:00.000Z' WHERE thread_id = 'a'`;
      expect((yield* service.list(user, { thread: a }).pipe(Effect.flip)).reason).toBe(
        "endpoint_not_found",
      );
    }),
    { getThreadShell: () => Effect.die("List reads must not construct a shell") },
  ),
);

it.effect(
  "lists on the reader while the writer is occupied and preserves write-transaction affinity",
  () =>
    Effect.scoped(
      Effect.gen(function* () {
        const fs = yield* FileSystem.FileSystem;
        const path = yield* Path.Path;
        const directory = yield* fs.makeTempDirectoryScoped({ prefix: "linked-thread-reads-" });
        yield* run(
          Effect.gen(function* () {
            const sql = yield* SqlClient.SqlClient;
            const service = yield* LinkedThreads.LinkedThreads;
            yield* service.link(user, { kind: "peer", from: a, to: b, basis: "manual" });
            const acquired = yield* Deferred.make<void>();
            const release = yield* Deferred.make<void>();
            const writer = yield* Effect.forkChild(
              sql.withTransaction(
                Effect.gen(function* () {
                  yield* Deferred.succeed(acquired, undefined);
                  yield* Deferred.await(release);
                }),
              ),
            );
            yield* Deferred.await(acquired);
            const result = yield* service
              .list(user, { thread: a })
              .pipe(Effect.ensuring(Deferred.succeed(release, undefined)));
            expect(result.links).toHaveLength(1);
            yield* Fiber.join(writer);
            yield* sql.withTransaction(
              Effect.gen(function* () {
                yield* insertThread("uncommitted");
                yield* service.link(user, {
                  kind: "peer",
                  from: a,
                  to: endpoint("uncommitted"),
                  basis: "manual",
                });
                expect(
                  (yield* service.list(user, { thread: endpoint("uncommitted") })).links,
                ).toHaveLength(1);
              }),
            );
          }),
          {},
          path.join(directory, "state.sqlite"),
        );
      }),
    ).pipe(Effect.provide(NodeServices.layer)),
);
