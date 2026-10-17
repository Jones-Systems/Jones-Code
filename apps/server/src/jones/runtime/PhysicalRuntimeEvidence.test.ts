import { describe, expect, it } from "@effect/vitest";
import {
  ProviderDriverKind,
  ProviderInstanceId,
  ProviderSessionId,
  ProviderThreadId,
  ThreadId,
  type ProviderRuntimeBinding,
} from "@t3tools/contracts";
import * as NodeSqliteClient from "@t3tools/shared/nodeSqliteClient";
import * as Effect from "effect/Effect";
import * as SqlClient from "effect/sql/SqlClient";
import migration from "../persistence/Migrations/107_JonesPhysicalProcessEvidence.ts";
import * as PhysicalRuntimeEvidence from "./PhysicalRuntimeEvidence.ts";
import type { PhysicalProcessLease } from "@t3tools/provider-core/server/jones/runtimeTypes";

const binding = (generation = "g1", child = false): ProviderRuntimeBinding => ({
  threadId: ThreadId.make(child ? "child" : "parent"),
  providerThreadId: ProviderThreadId.make(child ? "provider-child" : "provider-parent"),
  providerSessionId: ProviderSessionId.make("session"),
  providerInstanceId: ProviderInstanceId.make("codex"),
  driver: ProviderDriverKind.make("codex"),
  nativeThreadId: child ? "native-child" : "native-parent",
  runtimeGeneration: generation,
});
const lease = (generation = "g1"): PhysicalProcessLease => ({
  runtimeGeneration: generation,
  capture: Effect.succeed({
    status: "observed",
    runtimeGeneration: generation,
    launcherPid: 42,
    source: "codex.server/diagnostics+local-os",
    responder: {
      platform: "linux",
      bootId: "boot",
      pidNamespace: "pid:[123]",
      pid: 42,
      startTime: "123",
      startTimeUnit: "linux-clock-ticks-since-boot",
      executableIdentity: "1:2:3:4",
      build: "unknown",
    },
  }),
});
const setup = Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  // Synthetic upstream shape is enough to prove sparse ownership without opening application data.
  yield* sql`CREATE TABLE orchestration_v2_projection_provider_threads(provider_thread_id TEXT PRIMARY KEY, thread_id TEXT)`;
  yield* sql`INSERT INTO orchestration_v2_projection_provider_threads VALUES ('provider-parent','parent'),('provider-child','child')`;
  yield* migration;
  return sql;
});
const memory = NodeSqliteClient.layer({ filename: ":memory:" });

describe("physical evidence sidecar", () => {
  it.effect("preserves history but never restores a current lease after restart", () =>
    Effect.gen(function* () {
      const sql = yield* setup;
      const owner = yield* PhysicalRuntimeEvidence.make;
      yield* owner.bind(binding(), lease());
      expect(yield* owner.read(binding())).toMatchObject({ status: "observed" });
      const restarted = yield* PhysicalRuntimeEvidence.make;
      expect(yield* restarted.read(binding())).toMatchObject({ reason: "not_captured" });
      const rows = yield* sql`SELECT * FROM jones_physical_process_evidence`;
      expect(rows.length).toBe(2);
      expect(
        yield* sql`SELECT * FROM pragma_foreign_key_list('jones_physical_process_evidence')`,
      ).toEqual([]);
    }).pipe(Effect.provide(memory)),
  );
  it.effect(
    "fences old finalizers and associates explicitly bound children with the shared producer",
    () =>
      Effect.gen(function* () {
        yield* setup;
        const owner = yield* PhysicalRuntimeEvidence.make;
        yield* owner.bind(binding(), lease());
        expect(yield* owner.read(binding("g1", true))).toMatchObject({ reason: "not_captured" });
        const shared = lease("g2");
        yield* owner.bind(binding("g2"), shared);
        yield* owner.bind(binding("g2", true), shared);
        yield* owner.abandon("g1");
        expect(yield* owner.read(binding("g2"))).toMatchObject({
          status: "observed",
          launcherPid: 42,
        });
        expect(yield* owner.read(binding("g2", true))).toMatchObject({
          status: "observed",
          launcherPid: 42,
        });
        yield* owner.abandon("g2");
        expect(yield* owner.read(binding("g2", true))).toMatchObject({ reason: "not_captured" });
      }).pipe(Effect.provide(memory)),
  );
  it.effect("records unsupported outcomes without attaching absent upstream rows", () =>
    Effect.gen(function* () {
      const sql = yield* setup;
      const owner = yield* PhysicalRuntimeEvidence.make;
      yield* owner.bind(binding(), {
        runtimeGeneration: "g1",
        capture: Effect.succeed({
          status: "unknown",
          runtimeGeneration: "g1",
          reason: "unsupported",
        }),
      });
      expect(yield* owner.read(binding())).toMatchObject({ reason: "unsupported" });
      yield* sql`DELETE FROM orchestration_v2_projection_provider_threads WHERE provider_thread_id='provider-parent'`;
      expect(yield* owner.read(binding())).toMatchObject({ reason: "storage_unavailable" });
    }).pipe(Effect.provide(memory)),
  );
  it.effect("cannot claim current physical evidence without durable storage", () =>
    Effect.gen(function* () {
      const owner = yield* PhysicalRuntimeEvidence.make;
      yield* owner.bind(binding(), lease());
      expect(yield* owner.read(binding())).toMatchObject({ reason: "storage_unavailable" });
    }),
  );
  it.effect("rejects a cross-generation observation and discards extra payload fields", () =>
    Effect.gen(function* () {
      const sql = yield* setup;
      const owner = yield* PhysicalRuntimeEvidence.make;
      yield* owner.bind(binding(), { runtimeGeneration: "g1", capture: lease("other").capture });
      expect(yield* owner.read(binding())).toMatchObject({
        reason: "retired",
        runtimeGeneration: "g1",
      });
      yield* owner.bind(binding(), {
        runtimeGeneration: "g1",
        capture: lease().capture.pipe(
          Effect.map((observation) => ({ ...observation, secret: "private-value" })),
        ),
      });
      expect(yield* owner.read(binding())).toMatchObject({ status: "observed" });
      const rows = yield* sql<{
        observation_json: string;
      }>`SELECT observation_json FROM jones_physical_process_evidence`;
      expect(rows.every((row) => !row.observation_json.includes("private-value"))).toBe(true);
    }).pipe(Effect.provide(memory)),
  );
});
