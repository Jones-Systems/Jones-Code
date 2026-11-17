import * as NodeCrypto from "@effect/platform-node/NodeCrypto";
import * as NodeServices from "@effect/platform-node/NodeServices";
import { assert, it } from "@effect/vitest";
import {
  EnvironmentId,
  ProjectId,
  ThreadId,
  type OrchestrationV2ThreadShell,
} from "@t3tools/contracts";
import * as P from "@t3tools/contracts/jones/workstreamsPersonal";
import * as NodeSqliteClient from "@t3tools/shared/nodeSqliteClient";
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as SqlClient from "effect/sql/SqlClient";
import * as Config from "../../../config.ts";
import * as Environment from "../../../environment/ServerEnvironment.ts";
import * as Threads from "../../../orchestration-v2/ThreadManagementService.ts";
import * as ServerSettings from "../../../serverSettings.ts";
import personalMigration from "../../persistence/Migrations/153_JonesPersonalWorkstreams.ts";
import autoRouteMigration from "../../persistence/Migrations/158_JonesWorkstreamAutoRoute.ts";
import * as Personal from "../personal/service.ts";
import * as AutoRoute from "./WorkstreamAutoRoute.ts";
import * as Model from "./WorkstreamClassifierModel.ts";

const environmentId = EnvironmentId.make("synthetic-auto-route-server");
const projectId = ProjectId.make("synthetic-project");
const now = "2026-01-01T00:00:00.000Z";
const SECRET_PROMPT = "synthetic-dispatched-prompt-body-7f3c";

type Shell = Pick<
  OrchestrationV2ThreadShell,
  "id" | "projectId" | "title" | "lineage" | "delegatedFrom" | "archivedAt"
> & { deletedAt?: unknown };

const makeFixture = () => {
  const shells = new Map<string, Shell>();
  const model = {
    calls: 0,
    prompts: [] as string[],
    next: (): Effect.Effect<string, Model.WorkstreamClassifierModelError> =>
      Effect.succeed('{"choice":null,"confidence":"low","runnerUp":null,"basis":"none"}'),
  };
  const database = Layer.effectDiscard(
    Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;
      yield* sql`CREATE TABLE orchestration_v2_projection_threads(thread_id TEXT PRIMARY KEY,
        project_id TEXT NOT NULL, title TEXT NOT NULL, updated_at TEXT NOT NULL,
        deleted_at TEXT, payload_json TEXT NOT NULL)`;
      yield* sql`CREATE TABLE projection_projects(project_id TEXT PRIMARY KEY, title TEXT NOT NULL, deleted_at TEXT)`;
      yield* sql`INSERT INTO projection_projects VALUES(${projectId}, 'Synthetic project', NULL)`;
      yield* personalMigration;
      yield* autoRouteMigration;
    }),
  ).pipe(Layer.provideMerge(NodeSqliteClient.layer({ filename: ":memory:" })));
  const base = Personal.layer.pipe(
    Layer.provideMerge(
      Layer.mergeAll(
        database,
        Config.ServerConfig.layerTest(process.cwd(), { prefix: "auto-route-test-" }),
        Layer.succeed(Environment.ServerEnvironmentIdentity, {
          getEnvironmentId: Effect.succeed(environmentId),
        }),
      ).pipe(Layer.provideMerge(Layer.merge(NodeServices.layer, NodeCrypto.layer))),
    ),
    Layer.provideMerge(ServerSettings.layerTest()),
    Layer.provideMerge(
      Layer.mock(Threads.ThreadManagementService)({
        getThreadShell: (id) =>
          Effect.succeed((shells.get(id) ?? null) as OrchestrationV2ThreadShell | null),
      }),
    ),
    Layer.provideMerge(
      Layer.succeed(
        Model.WorkstreamClassifierModel,
        Model.WorkstreamClassifierModel.of({
          run: (input) =>
            Effect.gen(function* () {
              model.calls += 1;
              model.prompts.push(input.prompt);
              const requested = { instanceId: "fake", model: "fake-model" };
              if (input.onRequested) yield* input.onRequested(requested);
              return { text: yield* model.next(), requested };
            }),
        }),
      ),
    ),
  );
  const thread = (
    id: string,
    title: string,
    options: { lineage?: Shell["lineage"]; delegated?: boolean; updatedAt?: string } = {},
  ) =>
    Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;
      const threadId = ThreadId.make(id);
      const lineage = options.lineage ?? {
        parentThreadId: null,
        relationshipToParent: null,
        rootThreadId: threadId,
      };
      const payload = {
        lineage,
        ...(options.delegated
          ? { delegatedFrom: { environmentId, threadId: "peer", title: "x" } }
          : {}),
      };
      yield* sql`INSERT INTO orchestration_v2_projection_threads VALUES(${threadId}, ${projectId},
        ${title}, ${options.updatedAt ?? now}, NULL, ${JSON.stringify(payload)})`;
      shells.set(threadId, {
        id: threadId,
        projectId,
        title,
        lineage,
        archivedAt: null,
        ...(options.delegated
          ? {
              delegatedFrom: {
                environmentId,
                threadId: ThreadId.make("peer"),
                title: "x",
              },
            }
          : {}),
      } as Shell);
      return threadId;
    });
  return { base, model, thread, shells };
};

const route = Layer.build(AutoRoute.layerWorker).pipe(
  Effect.map((context) => Context.get(context, AutoRoute.WorkstreamAutoRoute)),
);
const w = (id: string, name = id) =>
  P.newPersonalWorkstream({ id, name, orderKey: "n", now, origin: "fixture" });
const observation = (threadId: ThreadId, commandId = "cmd-1") => ({
  threadId,
  commandId,
  messageId: `msg-${commandId}`,
  text: SECRET_PROMPT,
});
const reply = (choice: number | null, extra: Record<string, unknown> = {}) =>
  JSON.stringify({ choice, confidence: "high", runnerUp: null, basis: "both", ...extra });
const rows = Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  return yield* sql<
    Record<string, unknown>
  >`SELECT * FROM jones_workstream_auto_route_decisions ORDER BY created_at, rowid`;
});

/** Two groups: "voice" with one member thread, "updates" empty; candidates order voice=1, updates=2. */
const seed = (fixture: ReturnType<typeof makeFixture>) =>
  Effect.gen(function* () {
    const s = yield* Personal.PersonalWorkstreams;
    const member = yield* fixture.thread("member-1", "Voice cleanup prompt tuning");
    yield* s.assign({ threadId: member, workstream: w("voice", "Voice"), expectedRevision: 0 });
    yield* s.put(w("updates", "Updates"));
    return s;
  });

const run = <A, E>(body: (fixture: ReturnType<typeof makeFixture>) => Effect.Effect<A, E, any>) => {
  const fixture = makeFixture();
  return Effect.scoped(body(fixture)).pipe(Effect.provide(fixture.base)) as Effect.Effect<A, E>;
};

it.effect("assigns a clear match, marks it automatic, and stores no prompt text", () =>
  run((f) =>
    Effect.gen(function* () {
      const s = yield* seed(f);
      const target = yield* f.thread("target", "Tighten voice cleanup wording");
      const router = yield* route;
      f.model.next = () => Effect.succeed(reply(1));
      const result = yield* router.process(observation(target));
      assert.deepStrictEqual(result.type === "decided" && result.outcome.state, "assigned");
      const read = yield* s.readThread(target);
      assert.deepStrictEqual(read.assignment.workstreamId, "voice");
      assert.strictEqual(read.assignment.revision, 1);
      assert.deepStrictEqual(read.autoRoute?.state, "assigned");
      const snapshot = yield* s.snapshot();
      assert.deepStrictEqual(
        snapshot.autoAssignments?.map((a) => [a.threadId, a.workstreamId, a.revision]),
        [[target, "voice", 1]],
      );
      const stored = yield* rows;
      assert.strictEqual(stored.length, 1);
      assert.include(f.model.prompts[0]!, SECRET_PROMPT);
      assert.include(f.model.prompts[0]!, "Voice cleanup prompt tuning");
      assert.notInclude(JSON.stringify(stored), SECRET_PROMPT);
      assert.notInclude(JSON.stringify(stored), "Tighten voice");
      assert.deepStrictEqual(stored[0]?.requested_model, "fake-model");
      assert.deepStrictEqual(stored[0]?.origin, "queue_http");
    }),
  ),
);

it.effect("abstains without assigning when the model names a runner-up", () =>
  run((f) =>
    Effect.gen(function* () {
      const s = yield* seed(f);
      const target = yield* f.thread("target", "Something");
      const router = yield* route;
      f.model.next = () => Effect.succeed(reply(1, { runnerUp: 2 }));
      const result = yield* router.process(observation(target));
      assert.deepStrictEqual(result.type === "decided" && result.outcome, {
        state: "abstained",
        code: "ambiguous",
        confidence: "high",
        basis: "both",
        candidateCount: 2,
      });
      assert.strictEqual((yield* s.readThread(target)).assignment.revision, 0);
    }),
  ),
);

it.effect(
  "skips assigned, previously cleared, subagent and delegated threads without a model call",
  () =>
    run((f) =>
      Effect.gen(function* () {
        const s = yield* seed(f);
        const router = yield* route;
        const assigned = yield* f.thread("assigned", "A");
        yield* s.assign({
          threadId: assigned,
          workstream: w("voice", "Voice"),
          expectedRevision: 0,
        });
        const cleared = yield* f.thread("cleared", "B");
        yield* s.assign({
          threadId: cleared,
          workstream: w("voice", "Voice"),
          expectedRevision: 0,
        });
        yield* s.assign({ threadId: cleared, workstream: null, expectedRevision: 1 });
        const sub = yield* f.thread("sub", "C", {
          lineage: {
            parentThreadId: assigned,
            relationshipToParent: "subagent",
            rootThreadId: assigned,
          },
        });
        const delegated = yield* f.thread("delegated", "D", { delegated: true });
        const codes = [];
        for (const id of [assigned, cleared, sub, delegated]) {
          const result = yield* router.process(observation(id));
          codes.push(
            result.type === "decided" && result.outcome.state === "skipped" && result.outcome.code,
          );
        }
        assert.deepStrictEqual(codes, [
          "already_assigned",
          "previously_assigned",
          "not_main_thread",
          "not_main_thread",
        ]);
        assert.strictEqual(f.model.calls, 0);
        assert.strictEqual((yield* s.readThread(assigned)).assignment.revision, 1);
      }),
    ),
);

it.effect("a concurrent human assignment wins the compare-and-swap", () =>
  run((f) =>
    Effect.gen(function* () {
      const s = yield* seed(f);
      const target = yield* f.thread("target", "Race");
      const router = yield* route;
      f.model.next = () =>
        s
          .assign({ threadId: target, workstream: w("updates", "Updates"), expectedRevision: 0 })
          .pipe(Effect.orDie, Effect.as(reply(1)));
      const result = yield* router.process(observation(target));
      assert.deepStrictEqual(result.type === "decided" && result.outcome, {
        state: "skipped",
        code: "assignment_changed",
      });
      assert.strictEqual((yield* s.readThread(target)).assignment.workstreamId, "updates");
      assert.deepStrictEqual((yield* s.snapshot()).autoAssignments, []);
    }),
  ),
);

it.effect("model failures and bad replies leave the thread unassigned", () =>
  run((f) =>
    Effect.gen(function* () {
      const s = yield* seed(f);
      const router = yield* route;
      const cases: [string, () => Effect.Effect<string, Model.WorkstreamClassifierModelError>][] = [
        [
          "model_unavailable",
          () =>
            Effect.fail(
              new Model.WorkstreamClassifierModelError({
                category: "unavailable",
                requested: null,
                cause: "provider",
              }),
            ),
        ],
        [
          "model_timeout",
          () =>
            Effect.fail(
              new Model.WorkstreamClassifierModelError({
                category: "timeout",
                requested: null,
                cause: "timeout",
              }),
            ),
        ],
        ["invalid_output", () => Effect.succeed("not json")],
        ["invalid_choice", () => Effect.succeed(reply(7))],
      ];
      for (const [code, next] of cases) {
        const target = yield* f.thread(`t-${code}`, code);
        f.model.next = next;
        const result = yield* router.process(observation(target));
        assert.deepStrictEqual(result.type === "decided" && result.outcome.state, "failed");
        assert.deepStrictEqual(
          result.type === "decided" && "code" in result.outcome && result.outcome.code,
          code,
        );
        assert.strictEqual((yield* s.readThread(target)).assignment.revision, 0);
      }
    }),
  ),
);

it.effect("a duplicate dispatch is decided once; the switch off writes nothing", () =>
  run((f) =>
    Effect.gen(function* () {
      yield* seed(f);
      const target = yield* f.thread("target", "Dup");
      const router = yield* route;
      yield* router.observe(observation(target));
      yield* router.observe(observation(target));
      yield* router.drain;
      assert.strictEqual((yield* rows).length, 1);
      assert.strictEqual(f.model.calls, 1);
      const settings = yield* ServerSettings.ServerSettingsService;
      yield* settings.updateSettings({ workstreamAutoRouteEnabled: false });
      const off = yield* f.thread("off", "Off");
      assert.deepStrictEqual(yield* router.process(observation(off)), { type: "disabled" });
      assert.strictEqual((yield* rows).length, 1);
      assert.strictEqual(f.model.calls, 1);
    }),
  ),
);

it.effect("stops calling the model after three attempts on one thread", () =>
  run((f) =>
    Effect.gen(function* () {
      yield* seed(f);
      const target = yield* f.thread("target", "Unclear");
      const router = yield* route;
      for (const command of ["c1", "c2", "c3", "c4"])
        yield* router.process(observation(target, command));
      assert.strictEqual(f.model.calls, 3);
      const last = (yield* rows).at(-1);
      assert.deepStrictEqual([last?.state, last?.code], ["skipped", "attempt_limit"]);
    }),
  ),
);

it.effect("never offers Storage or excluded groups, so only they remain means no candidates", () =>
  run((f) =>
    Effect.gen(function* () {
      const s = yield* Personal.PersonalWorkstreams;
      yield* s.put(w("storage", "Storage"));
      yield* s.put(w("excluded", "Voice work"));
      const settings = yield* ServerSettings.ServerSettingsService;
      yield* settings.updateSettings({ workstreamAutoRouteExcludedIds: ["excluded"] });
      const target = yield* f.thread("target", "Old conversation");
      const router = yield* route;
      const result = yield* router.process(observation(target));
      assert.deepStrictEqual(result.type === "decided" && result.outcome, {
        state: "abstained",
        code: "no_candidates",
      });
      assert.strictEqual(f.model.calls, 0);
      yield* s.put(w("voice", "Voice"));
      const other = yield* f.thread("other", "New");
      yield* router.process(observation(other));
      assert.notInclude(f.model.prompts[0]!, "Storage");
      assert.notInclude(f.model.prompts[0]!, "Voice work");
    }),
  ),
);

it.effect("recovery fails pending rows and reconciles assigning rows by the assignment", () =>
  run((f) =>
    Effect.gen(function* () {
      const s = yield* seed(f);
      const sql = yield* SqlClient.SqlClient;
      const pending = yield* f.thread("pending", "P");
      const landed = yield* f.thread("landed", "L");
      const lost = yield* f.thread("lost", "X");
      yield* s.assign({ threadId: landed, workstream: w("voice", "Voice"), expectedRevision: 0 });
      const insert = (threadId: string, state: string, workstream: string | null) =>
        sql`INSERT INTO jones_workstream_auto_route_decisions(thread_id, command_id, message_id,
          origin, state, workstream_id, assignment_revision, created_at)
          VALUES(${threadId}, 'c', 'm', 'queue_http', ${state}, ${workstream},
            ${workstream === null ? null : 1}, ${now})`;
      yield* insert(pending, "pending", null);
      yield* insert(landed, "assigning", "voice");
      yield* insert(lost, "assigning", "voice");
      yield* route;
      const states = (yield* rows).map((r) => [r.thread_id, r.state, r.code]);
      assert.deepStrictEqual(states, [
        [pending, "failed", "interrupted"],
        [landed, "assigned", null],
        [lost, "failed", "interrupted"],
      ]);
      assert.strictEqual(f.model.calls, 0);
      assert.deepStrictEqual(
        (yield* s.snapshot()).autoAssignments?.map((a) => a.threadId),
        [landed],
      );
    }),
  ),
);

it.effect("an owner correction drops the marker and informs the next decision", () =>
  run((f) =>
    Effect.gen(function* () {
      const s = yield* seed(f);
      const target = yield* f.thread("target", "Corrected thread title");
      const router = yield* route;
      f.model.next = () => Effect.succeed(reply(1));
      yield* router.process(observation(target));
      yield* s.assign({
        threadId: target,
        workstream: w("updates", "Updates"),
        expectedRevision: 1,
      });
      assert.deepStrictEqual((yield* s.snapshot()).autoAssignments, []);
      const next = yield* f.thread("next", "Next");
      f.model.next = () => Effect.succeed(reply(null, { basis: "none", confidence: "low" }));
      yield* router.process(observation(next));
      const prompt = f.model.prompts[1]!;
      const data = JSON.parse(prompt.slice(prompt.indexOf("Data:\n") + 6));
      assert.deepStrictEqual(data.ownerCorrections, [
        {
          thread: "Corrected thread title",
          assignedAutomaticallyTo: "Voice",
          ownerChose: "Updates",
        },
      ]);
    }),
  ),
);

it.effect("a group deleted during classification is not assigned", () =>
  run((f) =>
    Effect.gen(function* () {
      const s = yield* seed(f);
      const target = yield* f.thread("target", "Deleted");
      const router = yield* route;
      f.model.next = () =>
        Effect.gen(function* () {
          const [voice] = yield* s.readDefinitions(["voice"]).pipe(Effect.orDie);
          yield* s
            .put(P.deletePersonalWorkstream(voice!, "2026-01-02T00:00:00.000Z", "fixture"))
            .pipe(Effect.orDie);
          return reply(1);
        });
      const result = yield* router.process(observation(target));
      assert.deepStrictEqual(result.type === "decided" && result.outcome, {
        state: "failed",
        code: "workstream_unavailable",
      });
      assert.strictEqual((yield* s.readThread(target)).assignment.workstreamId, null);
    }),
  ),
);
