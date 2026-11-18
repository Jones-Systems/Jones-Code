import * as NodeCrypto from "@effect/platform-node/NodeCrypto";
import * as NodeServices from "@effect/platform-node/NodeServices";
import { assert, expect, it } from "@effect/vitest";
import { describe } from "vite-plus/test";
import {
  AuthSessionId,
  EnvironmentAuthenticatedAuth,
  EnvironmentAuthenticatedPrincipal,
  EnvironmentAuthInvalidError,
  EnvironmentHttpApi,
  EnvironmentId,
  ThreadId,
  type AuthEnvironmentScope,
} from "@t3tools/contracts";
import * as P from "@t3tools/contracts/jones/workstreamsPersonal";
import {
  PERSONAL_WORKSTREAMS_ASSIGN_PATH,
  PERSONAL_WORKSTREAMS_LIST_PATH,
  PersonalWorkstreamHttpAssignResult,
  PersonalWorkstreamsHttpList,
} from "@t3tools/contracts/jones/workstreamsPersonalHttp";
import * as NodeSqliteClient from "@t3tools/shared/nodeSqliteClient";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";
import * as Layer from "effect/Layer";
import * as Schema from "effect/Schema";
import * as Stream from "effect/Stream";
import * as HttpApi from "effect/http-api/HttpApi";
import * as HttpApiBuilder from "effect/http-api/HttpApiBuilder";
import * as HttpRouter from "effect/http/HttpRouter";
import * as HttpServer from "effect/http/HttpServer";
import * as SqlClient from "effect/sql/SqlClient";
import * as Config from "../../../config.ts";
import * as Environment from "../../../environment/ServerEnvironment.ts";
import migration from "../../persistence/Migrations/153_JonesPersonalWorkstreams.ts";
import lifecycleMigration from "../../persistence/Migrations/159_JonesWorkstreamLifecycleRevision.ts";
import * as Agent from "./agentService.ts";
import * as BearerHttp from "./bearerHttp.ts";
import * as Personal from "./service.ts";

const environmentId = EnvironmentId.make("synthetic-bearer-environment");
const threadId = ThreadId.make("synthetic-bearer-thread");
const deletedThreadId = ThreadId.make("synthetic-deleted-thread");
const created = "2026-01-01T00:00:00.000Z";
const definition = (id: string, orderKey: string) =>
  P.newPersonalWorkstream({ id, name: `Name ${id}`, orderKey, now: created, origin: "fixture" });

const database = Layer.effectDiscard(
  Effect.gen(function* () {
    const sql = yield* SqlClient.SqlClient;
    yield* sql`CREATE TABLE orchestration_v2_projection_threads(thread_id TEXT PRIMARY KEY, deleted_at TEXT)`;
    yield* sql`INSERT INTO orchestration_v2_projection_threads(thread_id) VALUES(${threadId})`;
    yield* sql`INSERT INTO orchestration_v2_projection_threads(thread_id, deleted_at) VALUES(${deletedThreadId}, ${created})`;
    yield* migration;
    yield* lifecycleMigration;
  }),
).pipe(Layer.provideMerge(NodeSqliteClient.layer({ filename: ":memory:" })));
const dependencies = Layer.mergeAll(
  database,
  Config.ServerConfig.layerTest(process.cwd(), { prefix: "personal-workstreams-bearer-test-" }),
  Layer.succeed(Environment.ServerEnvironmentIdentity, {
    getEnvironmentId: Effect.succeed(environmentId),
  }),
).pipe(Layer.provideMerge(Layer.merge(NodeServices.layer, NodeCrypto.layer)));
// Definitions: zeta (c), alpha and beta (n, tie broken by id), and a deleted "gone" (b).
const seeded = Layer.effectDiscard(
  Effect.gen(function* () {
    const service = yield* Personal.PersonalWorkstreams;
    const gone = definition("gone", "b");
    for (const w of [
      definition("beta", "n"),
      definition("zeta", "c"),
      definition("alpha", "n"),
      gone,
    ])
      yield* service.put(w);
    yield* service.put(P.deletePersonalWorkstream(gone, "2026-01-02T00:00:00.000Z", "fixture"));
  }),
);
const services = Layer.merge(Agent.layer, seeded).pipe(
  Layer.provideMerge(Personal.layer),
  Layer.provideMerge(dependencies),
);

const setExpecting = (input: Parameters<Agent.AgentWorkstreams["Service"]["setExpecting"]>[0]) =>
  Agent.AgentWorkstreams.pipe(Effect.flatMap((agent) => agent.setExpecting(input)));
const unassigned = { kind: "unassigned" } as const;

// Commits competing placements just before the agent's first write, after its reads, so the
// agent's compare-and-swap deterministically observes a concurrent winner.
const racing = (competitors: readonly (string | null)[]) => {
  let writes = 0;
  const layer = Layer.effect(
    Personal.PersonalWorkstreams,
    Effect.gen(function* () {
      const real = yield* Personal.PersonalWorkstreams;
      return Personal.PersonalWorkstreams.of({
        ...real,
        assign: (input) =>
          Effect.gen(function* () {
            if (writes++ === 0)
              for (const id of competitors) {
                const { assignment } = yield* real.readThread(input.threadId);
                const workstream = id === null ? null : (yield* real.readDefinitions([id]))[0];
                yield* real.assign({
                  threadId: input.threadId,
                  workstream: workstream ?? null,
                  expectedRevision: assignment.revision,
                });
              }
            return yield* real.assign(input);
          }),
      });
    }),
  );
  return {
    writes: () => writes,
    services: Layer.merge(Agent.layer.pipe(Layer.provide(layer)), seeded).pipe(
      Layer.provideMerge(Personal.layer),
      Layer.provideMerge(dependencies),
    ),
  };
};

describe("personal Workstreams expecting assignment", () => {
  it.effect("evaluates outcomes in contract precedence and keeps retries side-effect free", () =>
    Effect.gen(function* () {
      const missingThread = yield* setExpecting({
        threadId: ThreadId.make("synthetic-absent-thread"),
        workstreamId: "alpha",
        expected: unassigned,
      });
      assert.equal(missingThread.outcome, "thread_not_found");
      assert.equal(missingThread.assignment, null);
      // A deleted thread outranks an unknown Workstream.
      const deletedThread = yield* setExpecting({
        threadId: deletedThreadId,
        workstreamId: "missing",
        expected: unassigned,
      });
      assert.equal(deletedThread.outcome, "thread_not_found");
      for (const workstreamId of ["missing", "gone"]) {
        const missing = yield* setExpecting({ threadId, workstreamId, expected: unassigned });
        assert.equal(missing.outcome, "workstream_not_found");
        assert.deepEqual(missing.assignment, {
          threadId,
          workstreamId: null,
          revision: 0,
          updatedAt: null,
        });
        assert.equal(missing.workstream, null);
      }

      const applied = yield* setExpecting({
        threadId,
        workstreamId: "alpha",
        expected: unassigned,
      });
      assert.equal(applied.outcome, "applied");
      assert.equal(applied.environmentId, environmentId);
      assert.equal(applied.assignment?.workstreamId, "alpha");
      assert.equal(applied.assignment?.revision, 1);
      assert.equal(applied.workstream?.id, "alpha");
      assert.isTrue(applied.sequence > missingThread.sequence);

      const retry = yield* setExpecting({ threadId, workstreamId: "alpha", expected: unassigned });
      assert.equal(retry.outcome, "already_satisfied");
      assert.deepEqual(retry.assignment, applied.assignment);
      assert.equal(retry.sequence, applied.sequence);

      const conflict = yield* setExpecting({
        threadId,
        workstreamId: "beta",
        expected: unassigned,
      });
      assert.equal(conflict.outcome, "precondition_failed");
      assert.deepEqual(conflict.assignment, applied.assignment);
      assert.equal(conflict.workstream?.id, "alpha");
      assert.equal(conflict.sequence, applied.sequence);

      const staleRevision = yield* setExpecting({
        threadId,
        workstreamId: "beta",
        expected: { kind: "revision", revision: 0 },
      });
      assert.equal(staleRevision.outcome, "precondition_failed");

      const moved = yield* setExpecting({
        threadId,
        workstreamId: "beta",
        expected: { kind: "revision", revision: 1 },
      });
      assert.equal(moved.outcome, "applied");
      assert.equal(moved.assignment?.revision, 2);

      // A held target is satisfied even when the stated precondition is stale.
      const held = yield* setExpecting({ threadId, workstreamId: "beta", expected: unassigned });
      assert.equal(held.outcome, "already_satisfied");

      const removed = yield* setExpecting({
        threadId,
        workstreamId: null,
        expected: { kind: "revision", revision: 2 },
      });
      assert.equal(removed.outcome, "applied");
      assert.equal(removed.assignment?.workstreamId, null);
      assert.equal(removed.workstream, null);
    }).pipe(Effect.provide(services)),
  );

  it.effect("lets exactly one concurrent unassigned placement win and reports the winner", () =>
    Effect.gen(function* () {
      const [left, right] = yield* Effect.all(
        [
          setExpecting({ threadId, workstreamId: "alpha", expected: unassigned }),
          setExpecting({ threadId, workstreamId: "beta", expected: unassigned }),
        ],
        { concurrency: "unbounded" },
      );
      const outcomes = [left.outcome, right.outcome].sort();
      assert.deepEqual(outcomes, ["applied", "precondition_failed"]);
      const winner = left.outcome === "applied" ? left : right;
      const loser = left.outcome === "applied" ? right : left;
      assert.deepEqual(loser.assignment, winner.assignment);
      assert.equal(loser.workstream?.id, winner.workstream?.id);
      assert.equal(winner.assignment?.revision, 1);
    }).pipe(Effect.provide(services)),
  );

  it.effect("reports a winner that commits to another target between read and write", () => {
    const race = racing(["beta"]);
    return Effect.gen(function* () {
      const result = yield* setExpecting({ threadId, workstreamId: "alpha", expected: unassigned });
      assert.equal(result.outcome, "precondition_failed");
      assert.equal(result.assignment?.workstreamId, "beta");
      assert.equal(result.assignment?.revision, 1);
      assert.equal(result.workstream?.id, "beta");
      // The lost write is not retried, because the thread is no longer unassigned.
      assert.equal(race.writes(), 1);
      const service = yield* Personal.PersonalWorkstreams;
      assert.equal((yield* service.snapshot()).sequence, result.sequence);
    }).pipe(Effect.provide(race.services));
  });

  it.effect("reports a concurrent winner on the same target as satisfied without a bump", () => {
    const race = racing(["alpha"]);
    return Effect.gen(function* () {
      const service = yield* Personal.PersonalWorkstreams;
      const before = (yield* service.snapshot()).sequence;
      const result = yield* setExpecting({ threadId, workstreamId: "alpha", expected: unassigned });
      assert.equal(result.outcome, "already_satisfied");
      assert.equal(result.assignment?.workstreamId, "alpha");
      assert.equal(result.assignment?.revision, 1);
      // Only the competing placement advanced the sequence.
      assert.equal(result.sequence, before + 1);
      assert.equal((yield* service.snapshot()).sequence, before + 1);
    }).pipe(Effect.provide(race.services));
  });

  it.effect("retries the write once when the thread is unassigned again after a lost race", () => {
    const race = racing(["beta", null]);
    return Effect.gen(function* () {
      const result = yield* setExpecting({ threadId, workstreamId: "alpha", expected: unassigned });
      assert.equal(result.outcome, "applied");
      assert.equal(result.assignment?.workstreamId, "alpha");
      assert.equal(result.assignment?.revision, 3);
      assert.equal(race.writes(), 2);
    }).pipe(Effect.provide(race.services));
  });

  it.effect("publishes one subscription snapshot for an applied placement", () =>
    Effect.gen(function* () {
      const service = yield* Personal.PersonalWorkstreams;
      const ready = yield* Deferred.make<void>();
      const watcher = yield* service.changes.pipe(
        Stream.tap(() => Deferred.succeed(ready, undefined)),
        Stream.take(2),
        Stream.runCollect,
        Effect.forkChild,
      );
      yield* Deferred.await(ready);
      const applied = yield* setExpecting({ threadId, workstreamId: "zeta", expected: unassigned });
      const [before, after] = yield* Fiber.join(watcher);
      assert.equal(after?.sequence, applied.sequence);
      assert.equal((before?.sequence ?? 0) + 1, after?.sequence);
      assert.deepEqual(
        after?.assignments.find((a) => a.threadId === threadId),
        applied.assignment ?? undefined,
      );
    }).pipe(Effect.provide(services)),
  );

  it.effect("lists live definitions by order key, then id", () =>
    Effect.gen(function* () {
      const agent = yield* Agent.AgentWorkstreams;
      const list = yield* agent.list();
      assert.deepEqual(
        list.workstreams.map((w) => w.id),
        ["zeta", "alpha", "beta"],
      );
      assert.equal(list.environmentId, environmentId);
    }).pipe(Effect.provide(services)),
  );
});

function server(scopes: AuthEnvironmentScope[] | null) {
  const auth = Layer.succeed(EnvironmentAuthenticatedAuth, (effect) =>
    scopes === null
      ? Effect.fail(
          new EnvironmentAuthInvalidError({
            code: "auth_invalid",
            reason: "missing_credential",
            traceId: "fixture",
          }),
        )
      : effect.pipe(
          Effect.provideService(EnvironmentAuthenticatedPrincipal, {
            sessionId: AuthSessionId.make("personal-workstreams-bearer"),
            subject: "fixture",
            method: "bearer-access-token",
            scopes: new Set(scopes),
          }),
        ),
  );
  return HttpRouter.toWebHandler(
    HttpApiBuilder.layer(
      HttpApi.make("environment").add(EnvironmentHttpApi.groups.workstreamsPersonal),
    ).pipe(
      Layer.provide(BearerHttp.layer),
      Layer.provide(services),
      Layer.provide(auth),
      Layer.provide(HttpServer.layerServices),
    ),
    { disableLogger: true },
  );
}
const assignBody = {
  schema: "jones.personal-workstream-assign/v1",
  operationId: "op-synthetic-bearer-0001",
  origin: { kind: "queue", requestId: "req-synthetic-0001" },
  threadId,
  workstreamId: "alpha",
  expected: { kind: "unassigned" },
};
const post = (body: unknown) =>
  new Request(`http://localhost${PERSONAL_WORKSTREAMS_ASSIGN_PATH}`, {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify(body),
  });
const decodeList = Schema.decodeUnknownSync(PersonalWorkstreamsHttpList);
const decodeResult = Schema.decodeUnknownSync(PersonalWorkstreamHttpAssignResult);

describe("personal Workstreams bearer HTTP", () => {
  it.each([
    [null, 401, 401],
    [[], 403, 403],
    [["orchestration:read"], 200, 403],
    [["orchestration:read", "orchestration:operate"], 200, 200],
  ] as const)("enforces authentication and scopes %j", async (scopes, listStatus, assignStatus) => {
    const http = server(scopes === null ? null : [...scopes]);
    try {
      const list = await http.handler(
        new Request(`http://localhost${PERSONAL_WORKSTREAMS_LIST_PATH}`),
      );
      expect(list.status).toBe(listStatus);
      const assign = await http.handler(post(assignBody));
      expect(assign.status).toBe(assignStatus);
    } finally {
      await http.dispose();
    }
  });

  it("serves live ordered definitions without caching", async () => {
    const http = server(["orchestration:read"]);
    try {
      const response = await http.handler(
        new Request(`http://localhost${PERSONAL_WORKSTREAMS_LIST_PATH}`),
      );
      expect(response.status).toBe(200);
      expect(response.headers.get("cache-control")).toBe("no-store");
      const body = decodeList(await response.json());
      expect(body.environmentId).toBe(environmentId);
      expect(body.workstreams.map((w) => w.id)).toEqual(["zeta", "alpha", "beta"]);
      expect(Object.keys(body.workstreams[0] ?? {}).sort()).toEqual(
        ["color", "id", "lifecycle", "name", "orderKey", "rev", "updatedAt"].sort(),
      );
    } finally {
      await http.dispose();
    }
  });

  it("returns domain outcomes as 200 results echoing the operation id", async () => {
    const http = server(["orchestration:read", "orchestration:operate"]);
    try {
      const first = await http.handler(post(assignBody));
      expect(first.status).toBe(200);
      expect(first.headers.get("cache-control")).toBe("no-store");
      const applied = decodeResult(await first.json());
      expect(applied).toMatchObject({
        operationId: assignBody.operationId,
        environmentId,
        outcome: "applied",
        assignment: { threadId, workstreamId: "alpha", revision: 1 },
        workstream: { id: "alpha", name: "Name alpha", rev: 1 },
      });
      const retry = decodeResult(await (await http.handler(post(assignBody))).json());
      expect(retry.outcome).toBe("already_satisfied");
      expect(retry.sequence).toBe(applied.sequence);
      const missing = decodeResult(
        await (await http.handler(post({ ...assignBody, threadId: "synthetic-absent" }))).json(),
      );
      expect(missing).toMatchObject({
        outcome: "thread_not_found",
        assignment: null,
        workstream: null,
      });
    } finally {
      await http.dispose();
    }
  });

  it.each([
    ["an excess property", { ...assignBody, force: true }],
    ["an excess origin property", { ...assignBody, origin: { kind: "queue", extra: 1 } }],
    ["a short operation id", { ...assignBody, operationId: "short" }],
    ["an unknown expectation", { ...assignBody, expected: { kind: "any" } }],
  ])("rejects %s with 400 before any write", async (_label, body) => {
    const http = server(["orchestration:read", "orchestration:operate"]);
    try {
      expect((await http.handler(post(body))).status).toBe(400);
      const after = decodeResult(await (await http.handler(post(assignBody))).json());
      expect(after.outcome).toBe("applied");
    } finally {
      await http.dispose();
    }
  });
});
