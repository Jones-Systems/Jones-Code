import {
  CommandId,
  MessageId,
  EnvironmentId,
  EventId,
  ThreadId,
  RunId,
  TurnItemId,
  ProjectId,
  ProviderInstanceId,
  type OrchestrationV2DomainEvent,
} from "@t3tools/contracts";
import type { RecordedEvent } from "../../orchestration-v2/RecordedTypes.ts";
import * as NodeServices from "@effect/platform-node/NodeServices";
import * as FileSystem from "effect/FileSystem";
import * as Path from "effect/Path";
import { expect, it } from "@effect/vitest";
import * as NodeSqliteClient from "@t3tools/shared/nodeSqliteClient";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Layer from "effect/Layer";
import * as SqlClient from "effect/sql/SqlClient";
import { ServerEnvironmentIdentity } from "../../environment/ServerEnvironment.ts";
import { canonical, CheckoutError } from "../ordinaryCheckout/CheckoutProtocol.ts";
import * as Sources from "./CheckoutSource.ts";
import * as Provenance from "./Provenance.ts";
import * as Store from "./TransitionStore.ts";
import { LifecycleError } from "./Protocol.ts";
import migration from "../persistence/Migrations/136_JonesWorkspaceProvenance.ts";

const at = "2026-11-05T12:00:00.000Z";
const identity = (inode: string) => ({
  version: 1 as const,
  device: "device",
  inode,
  birth: inode,
});
const fixture = Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  yield* sql`CREATE TABLE projection_projects(project_id TEXT, workspace_root TEXT, updated_at TEXT, deleted_at TEXT)`;
  yield* sql`CREATE TABLE orchestration_v2_projection_threads(thread_id TEXT PRIMARY KEY, payload_json TEXT)`;
  yield* sql`CREATE TABLE orchestration_v2_projection_subagents(subagent_id TEXT,child_thread_id TEXT)`;
  yield* sql`INSERT INTO projection_projects VALUES('project','/repo',${at},NULL)`;
  const state = {
    inode: "one",
    path: "/linked",
    branch: "refs/heads/work" as string | null,
    head: "a".repeat(40),
    race: false,
  };
  const assign = (id: string) =>
    sql`INSERT INTO orchestration_v2_projection_threads VALUES (${id},${canonical({
      id,
      projectId: "project",
      updatedAt: at,
      createdBy: "agent",
      worktreePath: state.path,
      branch: state.branch === null ? null : state.branch.slice(11),
    })}) ON CONFLICT(thread_id) DO UPDATE SET payload_json=excluded.payload_json`;
  yield* assign("thread");
  yield* migration;
  const observe = (
    input: Parameters<Sources.CheckoutSourceObservation["Service"]["capture"]>[0],
  ): Sources.CheckoutSource => ({
    target: {
      ...input,
      version: 1,
      canonicalProjectRoot: "/repo",
      cwd: state.path,
      checkoutKey: state.path,
      directoryIdentity: identity(state.inode),
      gitCommonDirectory: "/repo/.git",
      gitCommonIdentity: identity("common"),
      gitDirectory: `/repo/.git/worktrees/${state.inode}`,
      gitDirectoryIdentity: identity(`admin-${state.inode}`),
      branch: state.branch,
    },
    head: state.head,
  });
  const source = () =>
    observe({
      projectId: "project",
      threadId: "thread",
      projectRoot: "/repo",
      worktreePath: state.path,
      threadBranch: state.branch === null ? null : state.branch.slice(11),
      projectRevision: at,
      threadRevision: at,
    });
  const observations = Sources.CheckoutSourceObservation.of({
    capture: (input) => Effect.succeed(observe(input)),
    revalidate: (captured) =>
      state.race || canonical(observe(captured.target)) !== canonical(captured)
        ? Effect.fail(new CheckoutError({ reason: "stale" }))
        : Effect.void,
  });
  const layer = Provenance.layer.pipe(
    Layer.provideMerge(Store.provenanceLayer),
    Layer.provide(Layer.succeed(Sources.CheckoutSourceObservation, observations)),
    Layer.provide(
      Layer.succeed(ServerEnvironmentIdentity, {
        getEnvironmentId: Effect.succeed(EnvironmentId.make("environment")),
      }),
    ),
  );
  return { state, layer, assign, source, sql };
});
const memory = NodeSqliteClient.layer({ filename: ":memory:" });

it.effect(
  "creation, delegation and handoff preserve many-to-many history including detached references",
  () =>
    Effect.gen(function* () {
      const f = yield* fixture;
      yield* Effect.gen(function* () {
        const p = yield* Provenance.Provenance;
        const initial = yield* p.capture({
          threadId: "thread",
          reason: "launch",
          createdBy: "system",
          baseRef: "origin/main",
          baseIsCurrentHead: true,
        });
        expect(initial.reason).toBe("launch");
        expect(initial.created_by).toBe("agent");
        expect(initial.base_head).toBe(f.state.head);
        yield* f.assign("child");
        yield* f.sql`INSERT INTO orchestration_v2_projection_subagents VALUES('task-one','child')`;
        const child = yield* p.capture({
          threadId: "child",
          reason: "launch",
          createdBy: "system",
        });
        expect(child.incarnation_id).toBe(initial.incarnation_id);
        const shared = yield* p.lookup({ path: "/linked" });
        expect(shared[0]?.threads).toEqual(["child", "thread"]);
        expect(shared[0]?.tasks).toEqual(["task-one"]);
        f.state.inode = "two";
        f.state.path = "/handoff";
        f.state.branch = null;
        yield* f.assign("thread");
        const handoff = yield* p.capture({
          threadId: "thread",
          reason: "handoff",
          createdBy: "agent",
        });
        expect(handoff.predecessor).toBe(initial.incarnation_id);
        expect(handoff.target.branch).toBeNull();
        expect((yield* p.lookup({ threadId: "thread" })).length).toBe(2);
        expect((yield* p.lookup({ path: "/linked" }))[0]?.provenance.incarnation_id).toBe(
          initial.incarnation_id,
        );
      }).pipe(Effect.provide(f.layer));
    }).pipe(Effect.provide(memory)),
);

it.effect(
  "new delegate captures its real task while recreation gets a distinct incarnation and predecessor",
  () =>
    Effect.gen(function* () {
      const f = yield* fixture;
      yield* f.sql`INSERT INTO orchestration_v2_projection_subagents VALUES('task','thread')`;
      yield* Effect.gen(function* () {
        const p = yield* Provenance.Provenance;
        const initial = yield* p.capture({
          threadId: "thread",
          reason: "launch",
          createdBy: "system",
        });
        expect(initial.reason).toBe("delegate");
        f.state.inode = "replacement";
        const next = yield* p.capture({
          threadId: "thread",
          reason: "recreate",
          createdBy: "system",
        });
        expect(next.incarnation_id).not.toBe(initial.incarnation_id);
        expect(next.predecessor).toBe(initial.incarnation_id);
        expect((yield* p.lookup({ path: "/linked" })).length).toBe(2);
      }).pipe(Effect.provide(f.layer));
    }).pipe(Effect.provide(memory)),
);

it.effect(
  "backfill requires physical binding and exact HEAD; replacement, relocation and revalidation races fail closed",
  () =>
    Effect.gen(function* () {
      const f = yield* fixture;
      yield* Effect.gen(function* () {
        const p = yield* Provenance.Provenance;
        const request = { threadId: "thread", reason: "backfill" as const, createdBy: "system" };
        expect(Exit.isFailure(yield* Effect.exit(p.capture(request)))).toBe(true);
        const expected = f.source();
        f.state.head = "b".repeat(40);
        expect(Exit.isFailure(yield* Effect.exit(p.capture({ ...request, expected })))).toBe(true);
        f.state.head = expected.head;
        f.state.inode = "replacement";
        expect(Exit.isFailure(yield* Effect.exit(p.capture({ ...request, expected })))).toBe(true);
        f.state.inode = "one";
        f.state.path = "/relocated";
        expect(Exit.isFailure(yield* Effect.exit(p.capture({ ...request, expected })))).toBe(true);
        f.state.path = "/linked";
        f.state.race = true;
        expect(Exit.isFailure(yield* Effect.exit(p.capture({ ...request, expected })))).toBe(true);
        expect(yield* p.lookup({ threadId: "thread" })).toEqual([]);
        f.state.race = false;
        expect((yield* p.capture({ ...request, expected })).reason).toBe("backfill");
      }).pipe(Effect.provide(f.layer));
    }).pipe(Effect.provide(memory)),
);

it.effect(
  "owner PR-link and merge events are durable and replay-safe without reading conversation content",
  () =>
    Effect.gen(function* () {
      const f = yield* fixture;
      yield* Effect.gen(function* () {
        const p = yield* Provenance.Provenance;
        const store = yield* Store.ProvenanceStore;
        yield* p.capture({ threadId: "thread", reason: "reference", createdBy: "system" });
        const event: OrchestrationV2DomainEvent = {
          id: EventId.make("pr-event"),
          threadId: ThreadId.make("thread"),
          occurredAt: DateTime.makeUnsafe(at),
          type: "thread.pull-request-link-updated",
          payload: {
            host: "github.com",
            repository: "Jones-Systems/Synthetic",
            number: 1,
            url: "https://github.com/Jones-Systems/Synthetic/pull/1",
            source: "agent",
            linkedAt: at,
            stack: null,
            snapshot: {
              state: "merged",
              title: "Never copied",
              headBranch: "work",
              baseBranch: "main",
              isDraft: false,
              updatedAt: at,
              syncedAt: at,
              mergedAt: at,
            },
          },
        };
        const privateEvent: RecordedEvent = {
          id: EventId.make("private-preflight"),
          threadId: ThreadId.make("thread"),
          occurredAt: DateTime.makeUnsafe(at),
          type: "legacy-bootstrap.preflight-intent",
          payload: {
            policy: {
              version: 1,
              createCommandId: CommandId.make("private-create"),
              birthCommandId: CommandId.make("private-birth"),
              releaseCommandId: CommandId.make("private-release"),
              projectId: ProjectId.make("project"),
              threadId: ThreadId.make("thread"),
              messageId: MessageId.make("private-message"),
              payloadHash: "a".repeat(64),
              ownsNewThread: true,
            },
            canonicalPayload: "private payload is never inspected",
            fetch: {
              cwd: "/repo",
              baseRef: "main",
              startFromOrigin: false,
              requireWorktree: true,
              remote: "origin",
            },
          },
        };
        yield* store.events([privateEvent, event, event]);
        const entries = yield* p.lookup({ path: "/linked" });
        expect(entries[0]?.facts.map((f) => f.kind).sort()).toEqual(["pr_link", "pr_merge"]);
        expect(canonical(entries)).not.toContain("Never copied");
        expect(canonical(entries)).not.toContain("private payload");
      }).pipe(Effect.provide(f.layer));
    }).pipe(Effect.provide(memory)),
);

it.effect(
  "stale production/receipt observations cannot attach to a replaced directory or overwrite existing facts",
  () =>
    Effect.gen(function* () {
      const f = yield* fixture;
      yield* Effect.gen(function* () {
        const p = yield* Provenance.Provenance;
        const captured = yield* p.capture({
          threadId: "thread",
          reason: "launch",
          createdBy: "system",
          runId: "run",
        });
        const source = f.source();
        const fact = {
          id: "receipt",
          incarnation_id: captured.incarnation_id,
          thread_id: "thread",
          kind: "receipt" as const,
          recorded_at: at,
          reference: "owner:receipt:1",
        };
        yield* p.record(source, fact);
        expect(
          Exit.isFailure(yield* Effect.exit(p.record(source, { ...fact, reference: "other" }))),
        ).toBe(true);
        f.state.inode = "replacement";
        expect(Exit.isFailure(yield* Effect.exit(p.record(source, { ...fact, id: "stale" })))).toBe(
          true,
        );
        expect(
          Exit.isFailure(
            yield* Effect.exit(
              p.capture({
                threadId: "thread",
                reason: "recreate",
                createdBy: "system",
                runId: "run",
              }),
            ),
          ),
        ).toBe(true);
        expect((yield* p.lookup({ threadId: "thread" })).length).toBe(1);
        expect((yield* p.lookup({ threadId: "thread" }))[0]?.facts).toEqual([fact]);
      }).pipe(Effect.provide(f.layer));
    }).pipe(Effect.provide(memory)),
);

it.effect(
  "completed owner file changes capture only the first write; settlement records a thread outcome without assessment authority",
  () =>
    Effect.gen(function* () {
      const f = yield* fixture;
      yield* Effect.gen(function* () {
        const p = yield* Provenance.Provenance;
        const store = yield* Store.ProvenanceStore;
        yield* p.capture({
          threadId: "thread",
          reason: "launch",
          createdBy: "system",
          runId: "run",
        });
        const now = DateTime.makeUnsafe(at);
        const threadId = ThreadId.make("thread");
        const file: OrchestrationV2DomainEvent = {
          type: "turn-item.updated",
          id: EventId.make("file"),
          threadId,
          occurredAt: now,
          payload: {
            type: "file_change",
            id: TurnItemId.make("file"),
            threadId,
            runId: RunId.make("run"),
            nodeId: null,
            providerThreadId: null,
            providerTurnId: null,
            nativeItemRef: null,
            parentItemId: null,
            ordinal: 0,
            status: "completed",
            title: null,
            startedAt: now,
            completedAt: now,
            updatedAt: now,
            fileName: "never-opened.txt",
            diffStr: "never copied",
          },
        };
        yield* store.events([{ ...file, payload: { ...file.payload, status: "running" } }]);
        expect((yield* p.lookup({ threadId }))[0]?.facts).toEqual([]);
        yield* store.events([file, { ...file, id: EventId.make("second-file") }]);
        const provider = ProviderInstanceId.make("codex");
        const settled: OrchestrationV2DomainEvent = {
          type: "thread.settled",
          id: EventId.make("settled"),
          threadId,
          occurredAt: now,
          payload: {
            id: threadId,
            projectId: ProjectId.make("project"),
            title: "Never copied",
            createdBy: "agent",
            creationSource: "mcp",
            providerInstanceId: provider,
            modelSelection: { instanceId: provider, model: "synthetic" },
            runtimeMode: "full-access",
            interactionMode: "default",
            branch: "work",
            worktreePath: "/linked",
            activeProviderThreadId: null,
            lineage: { parentThreadId: null, relationshipToParent: null, rootThreadId: threadId },
            forkedFrom: null,
            createdAt: now,
            updatedAt: now,
            archivedAt: null,
            settledOverride: "settled",
            settledAt: now,
            lastVisitedAt: null,
            deletedAt: null,
          },
        };
        yield* store.events([settled, settled]);
        const result = yield* p.lookup({ threadId });
        expect(result[0]?.facts.map((f) => f.kind).sort()).toEqual([
          "first_meaningful_write",
          "settlement_outcome",
        ]);
        expect(canonical(result)).not.toContain("never copied");
        expect(result[0]?.provenance.authority_effect).toBe("none");
      }).pipe(Effect.provide(f.layer));
    }).pipe(Effect.provide(memory)),
);

it.effect(
  "admin sidecars publish atomically, retain conflicts and reject symlinks with scoped scratch cleanup",
  () =>
    Effect.scoped(
      Effect.gen(function* () {
        const f = yield* fixture;
        const fs = yield* FileSystem.FileSystem;
        const path = yield* Path.Path;
        const root = yield* fs.makeTempDirectoryScoped({ prefix: "workspace-provenance-" });
        const record = yield* Effect.flatMap(Provenance.Provenance, (p) =>
          p.capture({ threadId: "thread", reason: "launch", createdBy: "system" }),
        ).pipe(Effect.provide(f.layer));
        const sidecar = yield* Provenance.ProvenanceSidecar.pipe(
          Effect.provide(Provenance.sidecarLayer),
        );
        const payload = { ...record, target: { ...record.target, gitDirectory: root } };
        const file = path.join(root, "workspace-provenance.json");
        yield* sidecar.publish(payload);
        yield* sidecar.publish(payload);
        expect(yield* fs.readDirectory(root)).toEqual(["workspace-provenance.json"]);
        const original = yield* fs.readFileString(file);
        expect(
          Exit.isFailure(
            yield* Effect.exit(sidecar.publish({ ...payload, incarnation_id: "replacement" })),
          ),
        ).toBe(true);
        expect(yield* fs.readFileString(file)).toBe(original);
        expect(yield* fs.readDirectory(root)).toEqual(["workspace-provenance.json"]);
        yield* fs.remove(file);
        yield* fs.writeFileString(file, "{invalid-json");
        expect(Exit.isFailure(yield* Effect.exit(sidecar.publish(payload)))).toBe(true);
        expect(yield* fs.readFileString(file)).toBe("{invalid-json");
        expect(yield* fs.readDirectory(root)).toEqual(["workspace-provenance.json"]);
        yield* fs.remove(file);
        const sentinel = path.join(root, "sentinel");
        yield* fs.writeFileString(sentinel, "must remain unchanged");
        yield* fs.symlink(sentinel, file);
        expect(Exit.isFailure(yield* Effect.exit(sidecar.publish(payload)))).toBe(true);
        expect(yield* fs.readFileString(sentinel)).toBe("must remain unchanged");
        expect((yield* fs.readDirectory(root)).sort()).toEqual([
          "sentinel",
          "workspace-provenance.json",
        ]);
      }),
    ).pipe(Effect.provide(Layer.merge(memory, NodeServices.layer))),
);

it.effect("unavailable provenance lookup remains absent for older compositions", () =>
  Effect.gen(function* () {
    const lookup = yield* Provenance.optionalLookup;
    expect(yield* lookup({ path: "/unknown" })).toBeUndefined();
  }),
);

it.effect("producer facts use the existing incarnation and refuse a different worktree", () =>
  Effect.gen(function* () {
    const f = yield* fixture;
    yield* Effect.gen(function* () {
      const p = yield* Provenance.Provenance;
      const initial = yield* p.capture({
        threadId: "thread",
        reason: "launch",
        createdBy: "agent",
      });
      yield* p.production!("thread", "/admin/output-obligation.json", "/linked");
      yield* p.production!("thread", "/admin/output-obligation.json", "/linked");
      const entries = yield* p.lookup({ threadId: "thread" });
      expect(entries[0]?.provenance.incarnation_id).toBe(initial.incarnation_id);
      expect(entries[0]?.facts.filter((fact) => fact.kind === "production")).toHaveLength(1);
      const refused = yield* Effect.exit(
        p.production!("thread", "/old-admin/obligation.json", "/old-checkout"),
      );
      expect(Exit.isFailure(refused)).toBe(true);
      expect(
        (yield* p.lookup({ threadId: "thread" }))[0]?.facts.filter(
          (fact) => fact.kind === "production",
        ),
      ).toHaveLength(1);
    }).pipe(Effect.provide(f.layer));
  }).pipe(Effect.provide(memory), Effect.provide(NodeServices.layer)),
);

it.effect("automatic capture skips an unbounded sink without starting or accumulating work", () =>
  Effect.gen(function* () {
    let starts = 0;
    const service = Provenance.Provenance.of({
      capture: () =>
        Effect.sync(() => {
          starts++;
        }).pipe(Effect.andThen(Effect.never)),
      lookup: () => Effect.succeed([]),
      record: () => Effect.void,
    });
    const capture = yield* Provenance.optionalCapture.pipe(
      Effect.provideService(Provenance.Provenance, service),
    );
    // Even oversized identifiers never reach the unbounded SQL/Git/sidecar adapter.
    for (let i = 0; i < 3; i++)
      yield* capture({ threadId: "x".repeat(65537), reason: "launch", createdBy: "system" });
    expect(starts).toBe(0);
  }),
);

it.effect(
  "optional lookup failure is unavailable, while an observed empty lookup stays empty",
  () =>
    Effect.gen(function* () {
      const service = Provenance.Provenance.of({
        capture: () => Effect.fail(new LifecycleError({ reason: "storage" })),
        lookup: () => Effect.fail(new LifecycleError({ reason: "storage" })),
        record: () => Effect.void,
      });
      const lookup = yield* Provenance.optionalLookup.pipe(
        Effect.provideService(Provenance.Provenance, service),
      );
      expect(yield* lookup({ path: "/missing-sidecar" })).toBeUndefined();
      const empty = yield* Provenance.optionalLookup.pipe(
        Effect.provideService(Provenance.Provenance, {
          ...service,
          lookup: () => Effect.succeed([]),
        }),
      );
      expect(yield* empty({ path: "/ordinary" })).toEqual([]);
    }),
);

it.effect("oversized stored lineage is unavailable instead of a partial empty observation", () =>
  Effect.gen(function* () {
    const f = yield* fixture;
    yield* Effect.gen(function* () {
      const p = yield* Provenance.Provenance;
      const record = yield* p.capture({
        threadId: "thread",
        reason: "launch",
        createdBy: "system",
      });
      const oversizedFact = canonical({
        id: "oversized",
        incarnation_id: record.incarnation_id,
        thread_id: "thread",
        kind: "pr_link",
        recorded_at: at,
        reference: "x".repeat(65537),
      });
      expect(new TextEncoder().encode(oversizedFact).byteLength).toBeGreaterThan(65536);
      yield* f.sql`INSERT INTO jones_workspace_provenance_facts VALUES ('oversized',${record.incarnation_id},${oversizedFact})`;
      const lookup = yield* Provenance.optionalLookup;
      expect(yield* lookup({ threadId: "thread" })).toBeUndefined();
    }).pipe(Effect.provide(f.layer));
  }).pipe(Effect.provide(memory)),
);
