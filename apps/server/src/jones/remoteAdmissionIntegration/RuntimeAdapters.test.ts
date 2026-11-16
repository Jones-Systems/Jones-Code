import { expect, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Schema from "effect/Schema";
import { liveThreadShell } from "../../mcp/McpToolAccess.testkit.ts";
import { ProjectId, ThreadId } from "@t3tools/contracts";
import * as SqlClient from "effect/sql/SqlClient";
import * as Sqlite from "@t3tools/shared/nodeSqliteClient";
import type {
  ManagedPolicyDocument,
  OrchestrationV2ThreadShell,
  RemoteAdmissionRecord,
} from "@t3tools/contracts";
import { ProjectRow } from "../../orchestration-v2/ProjectStore.ts";
import { makeCurrentAuthority } from "./RuntimeAdapters.ts";
const record: RemoteAdmissionRecord = {
  id: "request",
  idempotencyKey: "key",
  requestDigest: "a".repeat(64),
  bindingDigest: "b".repeat(64),
  status: "approved",
  commandId: null,
  reason: null,
  category: null,
  resolvedAt: null,
  resolvedBy: null,
  createdAt: "2026-11-16",
  updatedAt: "2026-11-16",
  envelope: {
    identity: { clientId: "session", linkId: "session" },
    operation: "t3_thread_send",
    receiverPolicy: { enabled: false, revision: "initial", modelSelection: null },
    target: {
      projectId: "project",
      threadId: "target",
      runtimeMode: "auto",
      interactionMode: "default",
    },
    payload: {
      params: { threadId: "target" },
      invocation: {
        environmentId: "receiver",
        requestNamespace: "client:session",
        client: { sessionId: "session", label: "peer", access: "auto", linked: true },
      },
    },
  },
};
const memory = Layer.effectDiscard(
  Effect.gen(function* () {
    const sql = yield* SqlClient.SqlClient;
    yield* sql`CREATE TABLE auth_sessions(session_id TEXT,subject TEXT,method TEXT,scopes TEXT,revoked_at TEXT,expires_at TEXT)`;
    yield* sql`INSERT INTO auth_sessions VALUES('session','mcp-client','bearer-access-token','["orchestration:operate"]',NULL,'2099-01-01')`;
    yield* sql`CREATE TABLE jones_remote_admission_policy(singleton INTEGER,policy_json TEXT)`;
  }),
).pipe(Layer.provideMerge(Sqlite.layer({ filename: ":memory:" })));
const setup = Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  const baseShell = {
    ...liveThreadShell(ThreadId.make("target"), { runtimeMode: "auto" }),
    projectId: ProjectId.make("project"),
    linkOrigin: { sessionId: "session", label: "Synthetic peer" },
  };
  let shell: OrchestrationV2ThreadShell | null = baseShell;
  const project = yield* Schema.decodeEffect(Schema.toCodecJson(ProjectRow))({
    projectId: "project",
    title: "Synthetic project",
    workspaceRoot: "/synthetic/project",
    defaultModelSelection: null,
    defaultThreadEnvMode: null,
    autoPull: false,
    faviconPath: null,
    projectIcon: null,
    scripts: [],
    pinnedAt: null,
    archivedAt: null,
    createdAt: "2026-11-16T00:00:00.000Z",
    updatedAt: "2026-11-16T00:00:00.000Z",
    deletedAt: null,
  });
  let document: ManagedPolicyDocument | undefined;
  const authorize = makeCurrentAuthority(
    sql,
    { getThreadShell: () => Effect.succeed(shell) },
    {
      get: () => Effect.succeedSome(project),
    },
    {
      get document() {
        return document;
      },
    },
  );
  return {
    sql,
    authorize,
    baseShell,
    setShell: (next: typeof shell) => {
      shell = next;
    },
    setPolicy: (next: ManagedPolicyDocument) => {
      document = next;
    },
  };
});
it.effect("uses current exact session and scopes rather than saved full-access metadata", () =>
  Effect.gen(function* () {
    const { sql, authorize } = yield* setup;
    expect(yield* authorize(record)).toBe(true);
    yield* sql`UPDATE auth_sessions SET revoked_at='2026-11-16'`;
    expect(yield* authorize(record)).toBe(false);
    yield* sql`UPDATE auth_sessions SET revoked_at=NULL,scopes='["orchestration:read"]'`;
    expect(yield* authorize(record)).toBe(false);
    yield* sql`UPDATE auth_sessions SET scopes='["orchestration:operate"]',expires_at='1969-01-01'`;
    expect(yield* authorize(record)).toBe(false);
  }).pipe(Effect.provide(memory)),
);
it.effect("retains verified narrowing and enforces loaded administrator ceiling", () =>
  Effect.gen(function* () {
    const { authorize, setPolicy } = yield* setup;
    const narrowed = {
      ...record,
      envelope: {
        ...record.envelope,
        payload: {
          params: {},
          invocation: {
            environmentId: "receiver",
            requestNamespace: "client:session",
            client: {
              sessionId: "session",
              label: "peer",
              access: "full-access",
              linked: true,
              narrowedTo: { runtimeMode: "approval-required", interactionMode: "default" },
            },
          },
        },
      },
    };
    expect(yield* authorize(narrowed)).toBe(false);
    setPolicy({
      schemaVersion: 1,
      organization: "Synthetic",
      limits: { maxRuntimeMode: "approval-required" },
    });
    expect(yield* authorize(record)).toBe(false);
  }).pipe(Effect.provide(memory)),
);
it.effect(
  "requires live receiver project/destination and exact linked ownership below full access",
  () =>
    Effect.gen(function* () {
      const { authorize, setShell, baseShell } = yield* setup;
      setShell(null);
      expect(yield* authorize(record)).toBe(false);
      setShell({
        ...baseShell,
        projectId: ProjectId.make("project"),
        linkOrigin: { sessionId: "foreign", label: "Synthetic peer" },
      });
      expect(yield* authorize(record)).toBe(false);
      setShell({
        ...baseShell,
        projectId: ProjectId.make("foreign-project"),
        linkOrigin: { sessionId: "session", label: "Synthetic peer" },
      });
      expect(yield* authorize(record)).toBe(false);
    }).pipe(Effect.provide(memory)),
);
