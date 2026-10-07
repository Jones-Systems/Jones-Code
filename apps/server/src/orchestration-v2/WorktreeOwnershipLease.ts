import { NonNegativeInt, ThreadId, TrimmedNonEmptyString } from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";
import * as SqlClient from "effect/unstable/sql/SqlClient";
import * as SqlSchema from "effect/unstable/sql/SqlSchema";

import { toPersistenceSqlError, type PersistenceSqlError } from "../persistence/Errors.ts";
import { readApplicationThreadBirth } from "./ApplicationThreadBirth.ts";
import { ordinaryApplicationIncarnationV1 } from "./OrdinaryCheckoutOwnership.ts";

export const WORKTREE_OWNERSHIP_LEASE_DURATION_MS = 5 * 60 * 1_000;

export const WorktreeOwnershipLease = Schema.Struct({
  resourcePath: TrimmedNonEmptyString,
  leaseId: TrimmedNonEmptyString,
  ownerThreadId: ThreadId,
  ownerIncarnation: TrimmedNonEmptyString,
  branch: Schema.NullOr(Schema.String),
  acquiredAtMs: NonNegativeInt,
  renewedAtMs: NonNegativeInt,
  expiresAtMs: NonNegativeInt,
});
export type WorktreeOwnershipLease = typeof WorktreeOwnershipLease.Type;

const AcquireLeaseInput = Schema.Struct({
  resourcePath: TrimmedNonEmptyString,
  leaseId: TrimmedNonEmptyString,
  ownerThreadId: ThreadId,
  branch: Schema.NullOr(Schema.String),
  nowMs: Schema.Number,
  expiresAtMs: Schema.Number,
});

const RenewLeaseInput = Schema.Struct({
  resourcePath: TrimmedNonEmptyString,
  leaseId: TrimmedNonEmptyString,
  ownerThreadId: ThreadId,
  ownerIncarnation: TrimmedNonEmptyString,
  nowMs: Schema.Number,
  expiresAtMs: Schema.Number,
});

const ReleaseLeaseInput = Schema.Struct({
  resourcePath: TrimmedNonEmptyString,
  leaseId: TrimmedNonEmptyString,
  ownerThreadId: ThreadId,
  ownerIncarnation: TrimmedNonEmptyString,
});

export interface WorktreeOwnershipLeaseStore {
  readonly getOrdinaryThreadIncarnation: (
    threadId: ThreadId,
  ) => Effect.Effect<Option.Option<string>, PersistenceSqlError>;
  readonly getThreadIncarnation: (
    threadId: ThreadId,
  ) => Effect.Effect<Option.Option<string>, PersistenceSqlError>;
  readonly acquire: (
    input: typeof AcquireLeaseInput.Type,
  ) => Effect.Effect<Option.Option<WorktreeOwnershipLease>, PersistenceSqlError>;
  readonly renew: (
    input: typeof RenewLeaseInput.Type,
  ) => Effect.Effect<boolean, PersistenceSqlError>;
  readonly release: (
    input: typeof ReleaseLeaseInput.Type,
  ) => Effect.Effect<void, PersistenceSqlError>;
  readonly listAll: () => Effect.Effect<ReadonlyArray<WorktreeOwnershipLease>, PersistenceSqlError>;
}

export const makeWorktreeOwnershipLeaseStore = Effect.fn("makeWorktreeOwnershipLeaseStore")(
  function* () {
    const sql = yield* SqlClient.SqlClient;

    const acquireLease = SqlSchema.findOneOption({
      Request: AcquireLeaseInput,
      Result: WorktreeOwnershipLease,
      execute: (input) => sql`
        INSERT INTO worktree_ownership_leases (
          resource_path,
          lease_id,
          owner_thread_id,
          owner_incarnation,
          branch,
          acquired_at_ms,
          renewed_at_ms,
          expires_at_ms
        )
        SELECT
          ${input.resourcePath},
          ${input.leaseId},
          ${input.ownerThreadId},
          json_array('t3.orchestration-v2.thread-birth/v1', creation.event_id, creation.sequence),
          ${input.branch},
          ${input.nowMs},
          ${input.nowMs},
          ${input.expiresAtMs}
        FROM (
          SELECT event_id, sequence
          FROM orchestration_events
          WHERE aggregate_kind = 'thread'
            AND application_event_version = 2
            AND stream_id = ${input.ownerThreadId}
            AND event_type = 'thread.created'
          ORDER BY sequence DESC
          LIMIT 1
        ) AS creation
        -- An outer WHERE disambiguates SQLite's SELECT ... ON CONFLICT parser.
        WHERE NOT EXISTS (
          SELECT 1 FROM orchestration_v2_worktree_path_admissions
          WHERE canonical_path = ${input.resourcePath} AND state NOT IN ('no_effect', 'released')
        )
        ON CONFLICT (resource_path) DO UPDATE SET
          owner_thread_id = excluded.owner_thread_id,
          owner_incarnation = excluded.owner_incarnation,
          lease_id = excluded.lease_id,
          branch = excluded.branch,
          acquired_at_ms = CASE
            WHEN worktree_ownership_leases.owner_thread_id = excluded.owner_thread_id
              AND worktree_ownership_leases.owner_incarnation = excluded.owner_incarnation
              THEN worktree_ownership_leases.acquired_at_ms
            ELSE excluded.acquired_at_ms
          END,
          renewed_at_ms = excluded.renewed_at_ms,
          expires_at_ms = excluded.expires_at_ms
        WHERE worktree_ownership_leases.owner_thread_id = excluded.owner_thread_id
          AND worktree_ownership_leases.owner_incarnation = excluded.owner_incarnation
        RETURNING
          resource_path AS "resourcePath",
          lease_id AS "leaseId",
          owner_thread_id AS "ownerThreadId",
          owner_incarnation AS "ownerIncarnation",
          branch,
          acquired_at_ms AS "acquiredAtMs",
          renewed_at_ms AS "renewedAtMs",
          expires_at_ms AS "expiresAtMs"
      `,
    });

    const renewLease = SqlSchema.findOneOption({
      Request: RenewLeaseInput,
      Result: WorktreeOwnershipLease,
      execute: (input) => sql`
        UPDATE worktree_ownership_leases
        SET renewed_at_ms = ${input.nowMs}, expires_at_ms = ${input.expiresAtMs}
        WHERE resource_path = ${input.resourcePath}
          AND lease_id = ${input.leaseId}
          AND owner_thread_id = ${input.ownerThreadId}
          AND owner_incarnation = ${input.ownerIncarnation}
          AND NOT EXISTS (
            SELECT 1 FROM orchestration_v2_worktree_path_admissions
            WHERE canonical_path = ${input.resourcePath} AND state NOT IN ('no_effect', 'released')
          )
        RETURNING
          resource_path AS "resourcePath",
          lease_id AS "leaseId",
          owner_thread_id AS "ownerThreadId",
          owner_incarnation AS "ownerIncarnation",
          branch,
          acquired_at_ms AS "acquiredAtMs",
          renewed_at_ms AS "renewedAtMs",
          expires_at_ms AS "expiresAtMs"
      `,
    });

    const releaseLease = SqlSchema.void({
      Request: ReleaseLeaseInput,
      execute: (input) => sql`
        DELETE FROM worktree_ownership_leases
        WHERE resource_path = ${input.resourcePath}
          AND lease_id = ${input.leaseId}
          AND owner_thread_id = ${input.ownerThreadId}
          AND owner_incarnation = ${input.ownerIncarnation}
          AND NOT EXISTS (
            SELECT 1 FROM orchestration_v2_worktree_path_admissions
            WHERE canonical_path = ${input.resourcePath} AND state NOT IN ('no_effect', 'released')
          )
      `,
    });

    const listLeases = SqlSchema.findAll({
      Request: Schema.Void,
      Result: WorktreeOwnershipLease,
      execute: () => sql`
        SELECT
          resource_path AS "resourcePath",
          lease_id AS "leaseId",
          owner_thread_id AS "ownerThreadId",
          owner_incarnation AS "ownerIncarnation",
          branch,
          acquired_at_ms AS "acquiredAtMs",
          renewed_at_ms AS "renewedAtMs",
          expires_at_ms AS "expiresAtMs"
        FROM worktree_ownership_leases
        ORDER BY resource_path ASC
      `,
    });

    const findThreadIncarnation = SqlSchema.findOneOption({
      Request: Schema.Struct({ threadId: ThreadId }),
      Result: Schema.Struct({ ownerIncarnation: TrimmedNonEmptyString }),
      execute: ({ threadId }) => sql`
        SELECT json_array('t3.orchestration-v2.thread-birth/v1', event_id, sequence) AS "ownerIncarnation"
        FROM orchestration_events
        WHERE aggregate_kind = 'thread'
            AND application_event_version = 2
            AND stream_id = ${threadId}
          AND event_type = 'thread.created'
        ORDER BY sequence DESC
        LIMIT 1
      `,
    });

    return {
      getOrdinaryThreadIncarnation: (threadId: ThreadId) =>
        sql
          .withTransaction(
            readApplicationThreadBirth(threadId).pipe(
              Effect.provideService(SqlClient.SqlClient, sql),
            ),
          )
          .pipe(
            Effect.map((birth) =>
              birth === null ? Option.none() : Option.some(ordinaryApplicationIncarnationV1(birth)),
            ),
            Effect.mapError(
              toPersistenceSqlError(
                "WorktreeOwnershipLeaseStore.getOrdinaryThreadIncarnation:query",
              ),
            ),
          ),
      getThreadIncarnation: (threadId: ThreadId) =>
        findThreadIncarnation({ threadId }).pipe(
          Effect.map(Option.map((row) => row.ownerIncarnation)),
          Effect.mapError(
            toPersistenceSqlError("WorktreeOwnershipLeaseStore.getThreadIncarnation:query"),
          ),
        ),
      acquire: (input: typeof AcquireLeaseInput.Type) =>
        acquireLease(input).pipe(
          Effect.mapError(toPersistenceSqlError("WorktreeOwnershipLeaseStore.acquire:query")),
        ),
      renew: (input: typeof RenewLeaseInput.Type) =>
        renewLease(input).pipe(
          Effect.map(Option.isSome),
          Effect.mapError(toPersistenceSqlError("WorktreeOwnershipLeaseStore.renew:query")),
        ),
      release: (input: typeof ReleaseLeaseInput.Type) =>
        releaseLease(input).pipe(
          Effect.mapError(toPersistenceSqlError("WorktreeOwnershipLeaseStore.release:query")),
        ),
      listAll: () =>
        listLeases(undefined).pipe(
          Effect.mapError(toPersistenceSqlError("WorktreeOwnershipLeaseStore.listAll:query")),
        ),
    } satisfies WorktreeOwnershipLeaseStore;
  },
);
