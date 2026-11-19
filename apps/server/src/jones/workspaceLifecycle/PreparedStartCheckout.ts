import type { RunId, ThreadId } from "@t3tools/contracts";
import * as Cause from "effect/Cause";
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";
import * as SqlClient from "effect/sql/SqlClient";
import { ServerEnvironmentIdentity } from "../../environment/ServerEnvironment.ts";
import type { EventSinkV2Shape } from "../../orchestration-v2/EventSink.ts";
import * as Targets from "../ordinaryCheckout/CheckoutTarget.ts";
import * as TargetNode from "../ordinaryCheckout/CheckoutTargetNode.ts";
import {
  canonical,
  CheckoutError,
  Target,
  TargetSnapshot,
} from "../ordinaryCheckout/CheckoutProtocol.ts";
import { makeJonesTablePresence } from "../persistence/JonesTablePresence.ts";
import { preparedStartBirth } from "../provider/PreparedStartBirth.ts";

type Release = Parameters<EventSinkV2Shape["commitCommand"]>[0];
type Committed = Effect.Success<ReturnType<EventSinkV2Shape["commitCommand"]>>;
type AcceptedRelease = Pick<Committed, "receipt" | "storedEvents">;
type RunKey = { readonly threadId: ThreadId; readonly id: RunId };

const Witness = Schema.Struct({
  version: Schema.Literal(1),
  environmentId: Schema.String,
  target: Target,
  runtimeMode: Schema.String,
  interactionMode: Schema.String,
});
const decodeWitness = Schema.decodeEffect(Schema.fromJsonString(Witness), {
  onExcessProperty: "error",
});
const Snapshot = Schema.Struct({
  ...TargetSnapshot.fields,
  runtimeMode: Schema.String,
  interactionMode: Schema.String,
});
const decodeSnapshot = Schema.decodeUnknownEffect(Snapshot);
const unavailable = () => new CheckoutError({ reason: "unavailable" });
const holdOnError = <A, E, R>(effect: Effect.Effect<A, E, R>) =>
  effect.pipe(
    Effect.catchCause((cause) =>
      Cause.hasInterruptsOnly(cause) ? Effect.interrupt : Effect.undefined,
    ),
  );

// Reconciliation can consume checked evidence in its writer transaction. The
// physical observation and the close-to-dispatch check always run outside it.
export interface CheckedPreparedCheckout {
  readonly matches: (releaseCommandId: string) => Effect.Effect<boolean>;
  readonly revalidate: (cwd: string | null | undefined) => Effect.Effect<void, CheckoutError>;
}

export class PreparedStartCheckout extends Context.Service<
  PreparedStartCheckout,
  {
    readonly captureRelease: (
      input: Release,
    ) => Effect.Effect<((accepted: AcceptedRelease) => Effect.Effect<void>) | undefined>;
    readonly preflight: (
      threadId: ThreadId,
    ) => Effect.Effect<ReadonlyMap<string, CheckedPreparedCheckout>>;
    readonly forClaim: (run: RunKey) => Effect.Effect<CheckedPreparedCheckout | undefined>;
  }
>()("t3/jones/workspaceLifecycle/PreparedStartCheckout") {}

const binding = (target: Target | TargetSnapshot) => {
  const { threadId, projectId, projectRoot, worktreePath, threadBranch } = target;
  return { threadId, projectId, projectRoot, worktreePath, threadBranch };
};
const physical = (target: Target) => {
  const {
    canonicalProjectRoot,
    cwd,
    checkoutKey,
    directoryIdentity,
    gitCommonDirectory,
    gitDirectory,
    gitCommonIdentity,
    gitDirectoryIdentity,
    branch,
  } = target;
  return {
    canonicalProjectRoot,
    cwd,
    checkoutKey,
    directoryIdentity,
    gitCommonDirectory,
    gitDirectory,
    gitCommonIdentity,
    gitDirectoryIdentity,
    branch,
  };
};
const supportsRecovery = (target: Target, cwd: string) =>
  cwd === target.cwd &&
  cwd === target.checkoutKey &&
  target.branch !== null &&
  target.gitCommonDirectory !== null &&
  target.gitDirectory !== null &&
  [target.directoryIdentity, target.gitCommonIdentity, target.gitDirectoryIdentity].every(
    (identity) => identity !== null && /^[1-9][0-9]*$/.test(identity.birth),
  );

const make = Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  const targets = yield* Targets.CheckoutTarget;
  const environment = yield* ServerEnvironmentIdentity;
  const hasTable = makeJonesTablePresence(sql);
  const outsideTransaction = Effect.gen(function* () {
    if (Option.isSome(yield* Effect.serviceOption(sql.transactionService)))
      return yield* unavailable();
  });

  const readSnapshot = Effect.fn("PreparedStartCheckout.readSnapshot")(function* (
    threadId: ThreadId,
  ) {
    const rows = yield* sql`
      SELECT t.thread_id AS threadId, t.project_id AS projectId,
        json_extract(t.payload_json, '$.worktreePath') AS worktreePath,
        json_extract(t.payload_json, '$.branch') AS threadBranch,
        json_extract(t.payload_json, '$.updatedAt') AS threadRevision,
        t.runtime_mode AS runtimeMode, t.interaction_mode AS interactionMode,
        p.workspace_root AS projectRoot, p.updated_at AS projectRevision
      FROM orchestration_v2_projection_threads AS t
      JOIN projection_projects AS p ON p.project_id = t.project_id
      WHERE t.thread_id = ${threadId} AND t.deleted_at IS NULL AND p.deleted_at IS NULL
    `;
    if (rows.length !== 1) return yield* unavailable();
    const snapshot = yield* decodeSnapshot(rows[0]);
    if (
      snapshot.worktreePath === null &&
      (yield* hasTable("jones_conversation_bindings")) &&
      (yield* sql`SELECT 1 FROM jones_conversation_bindings WHERE thread_id = ${threadId}`).length >
        0
    )
      return yield* unavailable();
    return snapshot;
  });

  const read = Effect.fn("PreparedStartCheckout.read")(function* (run: RunKey) {
    const rows = yield* sql<{
      readonly command_id: string;
      readonly release_event_id: string;
      readonly witness_json: string;
      readonly cwd: string;
    }>`
      SELECT witness.release_command_id AS command_id, witness.release_event_id, witness.witness_json,
        json_extract(scope.payload_json, '$.cwd') AS cwd
      FROM jones_prepared_start_checkouts AS witness
      JOIN orchestration_command_receipts AS release ON release.command_id = witness.release_command_id
      JOIN orchestration_events AS finish ON finish.sequence = release.result_sequence
        AND finish.event_id = witness.release_event_id AND finish.command_id = release.command_id
      JOIN orchestration_events AS scope ON scope.sequence = release.result_sequence - 3
        AND scope.command_id = release.command_id AND scope.stream_id = release.aggregate_id
      JOIN orchestration_v2_effect_outbox AS effect ON effect.effect_id =
        'effect:' || release.command_id || ':provider-turn.start:' || witness.run_id
      WHERE witness.run_id = ${run.id} AND release.aggregate_kind = 'thread'
        AND release.aggregate_id = ${run.threadId} AND release.command_type = 'prepared-run.release'
        AND release.status = 'accepted' AND release.error IS NULL
        AND scope.aggregate_kind = 'thread' AND scope.application_event_version = 2
        AND scope.event_type = 'checkpoint-scope.created'
        AND json_extract(scope.payload_json, '$.runId') = witness.run_id
        AND effect.thread_id = release.aggregate_id AND effect.command_id = release.command_id
        AND effect.effect_type = 'provider-turn.start' AND effect.status = 'cancelled'
        AND effect.attempt_count = 0 AND effect.lease_owner IS NULL AND effect.completed_at IS NOT NULL
        AND effect.payload_json = json_object('type', 'provider-turn.start', 'runId', witness.run_id)
    `;
    if (rows.length !== 1 || rows[0] === undefined) return yield* unavailable();
    const row = rows[0];
    const record = yield* decodeWitness(row.witness_json);
    const snapshot = yield* readSnapshot(run.threadId);
    if (
      record.environmentId !== (yield* environment.getEnvironmentId) ||
      canonical(binding(record.target)) !== canonical(binding(snapshot)) ||
      record.runtimeMode !== snapshot.runtimeMode ||
      record.interactionMode !== snapshot.interactionMode ||
      !supportsRecovery(record.target, row.cwd)
    )
      return yield* unavailable();
    return { row, record, snapshot };
  });

  const observe = Effect.fn("PreparedStartCheckout.observe")(function* (
    source: Effect.Success<ReturnType<typeof read>>,
  ) {
    yield* outsideTransaction;
    const {
      runtimeMode: _runtimeMode,
      interactionMode: _interactionMode,
      ...snapshot
    } = source.snapshot;
    const target = yield* targets.capture(snapshot);
    if (canonical(physical(target)) !== canonical(physical(source.record.target)))
      return yield* new CheckoutError({ reason: "stale" });
  });

  const readBirth = (run: RunKey, commandId: string) =>
    preparedStartBirth(sql, {
      threadId: sql`${run.threadId}`,
      runId: sql`${run.id}`,
      releaseCommandId: sql`${commandId}`,
    });

  const check = Effect.fn("PreparedStartCheckout.check")(function* (run: RunKey) {
    yield* outsideTransaction;
    if (!(yield* hasTable("jones_prepared_start_checkouts"))) return;
    const recorded = yield* read(run);
    const assertBirth = Effect.gen(function* () {
      const origins = yield* readBirth(run, recorded.row.command_id);
      if (origins.length !== 1 || origins[0]?.release_event_id !== recorded.row.release_event_id)
        return yield* unavailable();
    });
    yield* assertBirth;
    yield* observe(recorded);
    const matches = (releaseCommandId: string) =>
      holdOnError(
        read(run).pipe(
          Effect.map(
            ({ row }) =>
              row.command_id === releaseCommandId && canonical(row) === canonical(recorded.row),
          ),
        ),
      ).pipe(Effect.map((matched) => matched === true));
    return {
      matches,
      revalidate: Effect.fn("PreparedStartCheckout.revalidate")(
        function* (cwd) {
          yield* outsideTransaction;
          if (cwd !== recorded.row.cwd) return yield* unavailable();
          const current = yield* read(run);
          if (canonical(current.row) !== canonical(recorded.row)) return yield* unavailable();
          yield* observe(current);
          yield* assertBirth;
          if (!(yield* matches(recorded.row.command_id))) return yield* unavailable();
        },
        Effect.mapError((cause) => new CheckoutError({ reason: "stale", cause })),
      ),
    } satisfies CheckedPreparedCheckout;
  }, holdOnError);

  return PreparedStartCheckout.of({
    captureRelease: Effect.fn("PreparedStartCheckout.captureRelease")(function* (input) {
      if (
        input.commandType !== "prepared-run.release" ||
        input.legacyBootstrap ||
        input.nativeCreation
      )
        return;
      const [scope, , , start] = input.events;
      if (
        input.events.length !== 4 ||
        scope?.type !== "checkpoint-scope.created" ||
        start?.type !== "run.updated" ||
        start.payload.status !== "starting" ||
        start.payload.workspacePreparation === undefined ||
        start.payload.workspacePreparation.type === "auto" ||
        start.payload.activeAttemptId === null ||
        start.payload.contextHandoffId !== null ||
        start.threadId !== input.threadId ||
        input.effects.length !== 1 ||
        input.effects[0]?.request.type !== "provider-turn.start" ||
        input.effects[0].request.runId !== start.payload.id ||
        input.effects[0].nativeCreationExecutionReference !== undefined
      )
        return;
      yield* outsideTransaction;
      if (!(yield* hasTable("jones_prepared_start_checkouts"))) return;
      const unattended = yield* sql`
        SELECT 1 FROM orchestration_v2_projection_messages
        WHERE message_id = ${start.payload.userMessageId} AND run_id = ${start.payload.id}
          AND thread_id = ${input.threadId} AND role = 'user'
          AND (json_extract(payload_json, '$.scheduledTaskId') IS NOT NULL
            OR (json_extract(payload_json, '$.createdBy') = 'agent'
              AND json_extract(payload_json, '$.creationSource') = 'mcp'))
      `;
      if (unattended.length !== 1) return;
      const snapshot = yield* readSnapshot(input.threadId);
      const strategy = start.payload.workspacePreparation;
      if (
        (strategy.type === "root") !== (snapshot.worktreePath === null) ||
        (strategy.type === "existing_worktree" && strategy.worktreePath !== snapshot.worktreePath)
      )
        return;
      const { runtimeMode, interactionMode, ...targetInput } = snapshot;
      const target = yield* targets.capture(targetInput);
      if (!supportsRecovery(target, scope.payload.cwd)) return;
      const witness = canonical({
        version: 1,
        environmentId: yield* environment.getEnvironmentId,
        target,
        runtimeMode,
        interactionMode,
      } satisfies typeof Witness.Type);
      return ({ receipt, storedEvents }: AcceptedRelease) =>
        holdOnError(
          Effect.gen(function* () {
            if (
              receipt.commandId !== input.commandId ||
              receipt.threadId !== input.threadId ||
              receipt.status !== "accepted" ||
              storedEvents.at(-1)?.event.id !== start.id ||
              canonical(yield* readSnapshot(input.threadId)) !== canonical(snapshot)
            )
              return;
            const origins = yield* readBirth(start.payload, input.commandId);
            if (origins.length !== 1 || origins[0]?.release_event_id !== start.id) return;
            yield* sql`INSERT INTO jones_prepared_start_checkouts(run_id, release_command_id, release_event_id, witness_json)
            VALUES (${start.payload.id}, ${input.commandId}, ${start.id}, ${witness}) ON CONFLICT DO NOTHING`;
          }),
        ).pipe(Effect.asVoid);
    }, holdOnError),
    preflight: Effect.fn("PreparedStartCheckout.preflight")(
      function* (threadId) {
        yield* outsideTransaction;
        const checked = new Map<string, CheckedPreparedCheckout>();
        if (!(yield* hasTable("jones_prepared_start_checkouts"))) return checked;
        const runs = yield* sql<{ readonly run_id: RunId }>`
        SELECT run.run_id FROM orchestration_v2_projection_runs AS run
        JOIN jones_prepared_start_checkouts AS witness ON witness.run_id = run.run_id
        WHERE run.thread_id = ${threadId} AND run.status = 'starting'
          AND json_type(run.payload_json, '$.startedAt') = 'null'
      `;
        for (const { run_id } of runs) {
          const observed = yield* check({ threadId, id: run_id });
          if (observed !== undefined) checked.set(run_id, observed);
        }
        return checked;
      },
      (effect) => holdOnError(effect).pipe(Effect.map((result) => result ?? new Map())),
    ),
    forClaim: check,
  });
});

export const layer = Layer.effect(PreparedStartCheckout, make).pipe(
  Layer.provide(Targets.layer),
  Layer.provide(TargetNode.layer),
);
