import { ThreadId } from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Path from "effect/Path";
import * as Schema from "effect/Schema";
import * as SqlClient from "effect/sql/SqlClient";
import { readApplicationBirthRecord } from "../importedHistory/ApplicationBirth.ts";
import { overlaps } from "./CheckoutTarget.ts";
import {
  associationId,
  Binding,
  Birth,
  Origin,
  Relation,
  canonical,
  CheckoutError,
  digest,
  Evidence,
  operationId,
  Source,
  Target,
  terminal,
  Utc,
  type Admission,
  type LifecycleEvent,
  type Owner,
  type Use,
} from "./CheckoutProtocol.ts";
import {
  CheckoutAcceptanceAuthority,
  CheckoutExecutorAuthority,
  CheckoutOwnershipAuthority,
  OrdinaryCheckout,
  type Accept,
  type CommandIdentity,
  type Conflict,
  type EventInput,
  type Link,
  type TransitionInput,
} from "./OrdinaryCheckout.ts";

const decode = <S extends Schema.Codec<unknown, unknown, never, never>>(
  schema: S,
  value: unknown,
) =>
  Schema.decodeUnknownEffect(schema, { onExcessProperty: "error" })(value).pipe(
    Effect.mapError((cause) => new CheckoutError({ reason: "identity", cause })),
  );
const safe = <A, E, R>(effect: Effect.Effect<A, E, R>) =>
  effect.pipe(
    Effect.mapError((cause) =>
      Schema.is(CheckoutError)(cause) ? cause : new CheckoutError({ reason: "storage", cause }),
    ),
  );

const directoryWitness = (target: Target) =>
  canonical({
    version: 1,
    directory: target.directoryIdentity,
    gitDirectory: target.gitDirectory,
    gitDirectoryIdentity: target.gitDirectoryIdentity,
    gitCommonIdentity: target.gitCommonIdentity,
  });

const make = Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  const path = yield* Path.Path;
  const transaction = <A, E, R>(effect: Effect.Effect<A, E, R>) =>
    safe(sql.withTransaction(effect));
  const owner = (key: string) =>
    sql<Owner>`SELECT * FROM jones_ordinary_checkout_owners WHERE checkout_key=${key}`.pipe(
      Effect.map((rows) => rows[0] ?? null),
    );
  const admission = (id: string) =>
    sql<Admission>`SELECT * FROM jones_ordinary_checkout_admissions WHERE admission_id=${id}`.pipe(
      Effect.map((rows) => rows[0] ?? null),
    );
  const use = Effect.fn("OrdinaryCheckoutSqlite.use")(function* (id: string) {
    const rows =
      yield* sql<Use>`SELECT * FROM jones_ordinary_checkout_uses WHERE operation_id=${id}`;
    if (rows.length !== 1) return yield* new CheckoutError({ reason: "unavailable" });
    return rows[0]!;
  });
  const receipt = (id: string) =>
    sql<{
      readonly aggregate_id: string;
      readonly aggregate_kind: string;
      readonly command_type: string;
      readonly result_sequence: number;
      readonly status: string;
    }>`SELECT aggregate_id,aggregate_kind,command_type,result_sequence,status
    FROM orchestration_command_receipts WHERE command_id=${id}`.pipe(
      Effect.map((rows) => rows[0] ?? null),
    );
  const accepted = Effect.fn("OrdinaryCheckoutSqlite.accepted")(function* (row: Admission) {
    const current = yield* receipt(row.command_id);
    if (current === null) return false;
    if (
      current.aggregate_kind !== "thread" ||
      current.aggregate_id !== row.receipt_thread_id ||
      current.command_type !== row.command_type ||
      current.result_sequence !== row.receipt_sequence ||
      current.status !== "accepted"
    )
      return yield* new CheckoutError({ reason: "identity" });
    const basis =
      yield* sql`SELECT event_id FROM orchestration_events WHERE event_id=${row.basis_event_id}
      AND sequence=${row.basis_sequence} AND stream_id=${row.subject_thread_id} AND application_event_version=2`;
    if (basis.length !== 1) return false;
    for (const [table, column, id] of [
      ["orchestration_v2_projection_runs", "run_id", row.run_id],
      ["orchestration_v2_projection_run_attempts", "attempt_id", row.attempt_id],
      ["orchestration_v2_projection_nodes", "node_id", row.node_id],
      ["orchestration_v2_projection_checkpoint_scopes", "scope_id", row.checkpoint_scope_id],
    ] as const) {
      if (id === null) continue;
      const found =
        yield* sql`SELECT thread_id FROM ${sql(table)} WHERE ${sql(column)}=${id} AND thread_id=${row.subject_thread_id}`;
      if (found.length !== 1) return false;
    }
    return true;
  });
  const currentBirth = Effect.fn("OrdinaryCheckoutSqlite.currentBirth")(function* (
    threadId: string,
    eventId: string,
    sequence: number,
  ) {
    const birth = yield* readApplicationBirthRecord(ThreadId.make(threadId)).pipe(
      Effect.provideService(SqlClient.SqlClient, sql),
    );
    if (birth === null || birth.eventId !== eventId || birth.sequence !== sequence)
      return yield* new CheckoutError({ reason: "identity" });
  });
  const currentTarget = Effect.fn("OrdinaryCheckoutSqlite.currentTarget")(function* (
    target: Target,
  ) {
    yield* decode(Target, target);
    const threads = yield* sql<{
      readonly project_id: string;
      readonly worktree_path: string | null;
      readonly branch: string | null;
      readonly revision: string;
      readonly deleted: string | null;
    }>`SELECT project_id, json_extract(payload_json,'$.worktreePath') AS worktree_path,
      json_extract(payload_json,'$.branch') AS branch, json_extract(payload_json,'$.updatedAt') AS revision,
      json_extract(payload_json,'$.deletedAt') AS deleted
      FROM orchestration_v2_projection_threads WHERE thread_id=${target.threadId}`;
    const projects = yield* sql<{
      readonly workspace_root: string;
      readonly updated_at: string;
      readonly deleted_at: string | null;
    }>`
      SELECT workspace_root,updated_at,deleted_at FROM projection_projects WHERE project_id=${target.projectId}`;
    const thread = threads[0];
    const project = projects[0];
    if (
      !thread ||
      !project ||
      thread.deleted !== null ||
      project.deleted_at !== null ||
      thread.project_id !== target.projectId ||
      thread.worktree_path !== target.worktreePath ||
      thread.revision !== target.threadRevision ||
      project.updated_at !== target.projectRevision ||
      project.workspace_root !== target.projectRoot ||
      thread.branch !== target.threadBranch ||
      (thread.branch !== null &&
        (thread.branch.startsWith("refs/") ? thread.branch : `refs/heads/${thread.branch}`) !==
          target.branch)
    )
      return yield* new CheckoutError({ reason: "stale" });
  });
  const matchesTarget = (row: Owner, target: Target) =>
    row.checkout_key === target.checkoutKey &&
    row.directory_identity_json === directoryWitness(target) &&
    row.git_common_directory === target.gitCommonDirectory &&
    row.branch === target.branch;
  const currentOwner = (row: Admission, allowUnknown = false) => {
    const runCaptured = Effect.fn("OrdinaryCheckoutSqlite.currentOwner")(function* () {
      const lease = yield* owner(row.checkout_key);
      if (
        lease === null ||
        lease.lease_id !== row.lease_id ||
        lease.owner_epoch !== row.owner_epoch ||
        lease.target_revision !== row.target_revision ||
        lease.owner_thread_id !== row.subject_thread_id ||
        lease.birth_event_id !== row.birth_event_id ||
        lease.birth_sequence !== row.birth_sequence ||
        (lease.state !== "owned" && !(allowUnknown && lease.state === "unknown"))
      )
        return yield* new CheckoutError({ reason: "stale" });
      return lease;
    });
    return runCaptured();
  };
  const liveAdmission = (id: string, allowUnknown = false) => {
    const runCaptured = Effect.fn("OrdinaryCheckoutSqlite.liveAdmission")(function* () {
      const row = yield* admission(id);
      if (row === null || !(yield* accepted(row)))
        return yield* new CheckoutError({ reason: "unavailable" });
      yield* currentBirth(row.subject_thread_id, row.birth_event_id, row.birth_sequence);
      yield* currentOwner(row, allowUnknown);
      return row;
    });
    return runCaptured();
  };
  const replay = (input: CommandIdentity, allowNewSubject = false) => {
    const runCaptured = Effect.fn("OrdinaryCheckoutSqlite.replay")(function* () {
      const rows =
        yield* sql<Admission>`SELECT * FROM jones_ordinary_checkout_admissions WHERE command_id=${input.commandId}`;
      const r = yield* receipt(input.commandId);
      if (r === null) {
        if (rows.length > 0) return yield* new CheckoutError({ reason: "unknown" });
        return { kind: "absent" as const };
      }
      if (
        r.aggregate_kind !== "thread" ||
        r.aggregate_id !== input.receiptThreadId ||
        r.command_type !== input.commandType
      )
        return yield* new CheckoutError({ reason: "replay" });
      if (rows.length === 0)
        return { kind: "historical" as const, receiptSequence: r.result_sequence };
      if (
        rows.some(
          (row) =>
            row.command_digest !== digest(input.command) ||
            row.command_type !== input.commandType ||
            row.receipt_thread_id !== input.receiptThreadId ||
            row.receipt_sequence !== r.result_sequence,
        ) ||
        r.status !== "accepted"
      )
        return yield* new CheckoutError({ reason: "replay" });
      const selected = rows.filter((row) => row.subject_thread_id === input.subjectThreadId);
      if (
        (!allowNewSubject && selected.length === 0) ||
        selected.some(
          (row) =>
            row.origin_kind !== input.origin.kind ||
            row.origin_parent_thread_id !== input.origin.parentThreadId ||
            row.request_id !== input.origin.requestId,
        )
      )
        return yield* new CheckoutError({ reason: "replay" });
      // Deliberately no current-target, expiry, renewal or executor check on exact replay.
      return { kind: "accepted" as const, admissions: selected };
    });
    return runCaptured();
  };
  const conflicts = Effect.fn("OrdinaryCheckoutSqlite.conflicts")(function* (key: string) {
    if (!path.isAbsolute(key) || path.normalize(key) !== key)
      return yield* new CheckoutError({ reason: "identity" });
    const result: Conflict[] = [];
    // Ancestor primary-key probes and a bounded lexical descendant range avoid LIKE wildcard aliases.
    const ancestors: string[] = [];
    for (let ancestor = key; ; ancestor = path.dirname(ancestor)) {
      ancestors.push(ancestor);
      if (path.dirname(ancestor) === ancestor) break;
    }
    const prefix = key.endsWith(path.sep) ? key : `${key}${path.sep}`;
    // The prefix ends in an ASCII separator, so incrementing that separator is
    // its exclusive BINARY successor, including every Unicode descendant suffix.
    const successor = `${prefix.slice(0, -1)}${String.fromCharCode(path.sep.charCodeAt(0) + 1)}`;
    const rows = yield* sql<Owner>`SELECT * FROM jones_ordinary_checkout_owners
      WHERE ${sql.in("checkout_key", ancestors)} OR (checkout_key>=${prefix} AND checkout_key<${successor})`;
    for (const row of rows)
      if (row.state !== "released" && overlaps(key, row.checkout_key, path.sep))
        result.push({ kind: "ordinary", checkoutKey: row.checkout_key, owner: row });
    // These old Jones tables have no shared canonical index. Their original-owner fences remain conservative.
    const native = yield* sql<{
      readonly worktree_path: string | null;
    }>`SELECT worktree_path FROM native_creation_intents
      UNION SELECT worktree_path FROM jones_native_workspace_admissions`;
    for (const row of native)
      if (row.worktree_path !== null && overlaps(key, row.worktree_path, path.sep))
        result.push({ kind: "native", checkoutKey: row.worktree_path, owner: null });
    const deletion = yield* sql<{
      readonly worktree_path: string;
    }>`SELECT worktree_path FROM jones_deletion_worktree_admissions WHERE state!='completed'`;
    for (const row of deletion)
      if (overlaps(key, row.worktree_path, path.sep))
        result.push({ kind: "deletion", checkoutKey: row.worktree_path, owner: null });
    const legacy = yield* sql<{
      readonly resource_path: string;
    }>`SELECT resource_path FROM worktree_ownership_leases`;
    for (const row of legacy)
      if (overlaps(key, row.resource_path, path.sep))
        result.push({ kind: "legacy", checkoutKey: row.resource_path, owner: null });
    // No schema adoption or interpretation of copied foreign admission JSON.
    const foreign =
      yield* sql`SELECT migration_id FROM jones_sql_migrations WHERE migration_id IN (9,11)`;
    if (foreign.length > 0) result.push({ kind: "legacy", checkoutKey: key, owner: null });
    return result;
  });
  const effectRequest = Effect.fn("OrdinaryCheckoutSqlite.effectRequest")(function* (
    effectId: string,
  ) {
    const rows = yield* sql<{
      readonly command_id: string;
      readonly thread_id: string;
      readonly payload_json: string;
    }>`
      SELECT command_id,thread_id,payload_json FROM orchestration_v2_effect_outbox WHERE effect_id=${effectId}`;
    const row = rows[0];
    if (!row) return null;
    const request = yield* Schema.decodeUnknownEffect(Schema.fromJsonString(Schema.Unknown))(
      row.payload_json,
    );
    return { ...row, request };
  });
  const link = Effect.fn("OrdinaryCheckoutSqlite.link")(function* (
    row: Admission,
    input: Link,
    now: string,
  ) {
    const actual = yield* effectRequest(input.effectId);
    if (
      actual === null ||
      actual.command_id !== row.command_id ||
      actual.thread_id !== row.subject_thread_id ||
      digest(actual.request) !== digest(input.request)
    )
      return yield* new CheckoutError({ reason: "identity" });
    yield* sql`INSERT INTO jones_ordinary_checkout_effect_links ${sql.insert({
      effect_id: input.effectId,
      admission_id: row.admission_id,
      request_digest: digest(actual.request),
      role: input.role,
      recorded_at: now,
    })}`;
  });
  const pendingAdmissions = (key: string) => sql<{ readonly admission_id: string }>`
    SELECT a.admission_id FROM jones_ordinary_checkout_admissions a WHERE a.checkout_key=${key}
      AND (NOT EXISTS(SELECT 1 FROM jones_ordinary_checkout_uses u WHERE u.admission_id=a.admission_id)
        OR EXISTS(SELECT 1 FROM jones_ordinary_checkout_uses u WHERE u.admission_id=a.admission_id AND u.state NOT IN ('completed','no_effect')))`;
  const accept = (input: Accept) =>
    transaction(
      Effect.gen(function* () {
        yield* decode(Utc, input.now);
        yield* decode(Utc, input.expiresAt);
        yield* decode(Birth, input.birth);
        yield* decode(Origin, input.origin);
        yield* decode(Relation, input.relation);
        if (input.expiresAt < input.now || input.subjectThreadId !== input.target.threadId)
          return yield* new CheckoutError({ reason: "identity" });
        const replayed = yield* replay(input, true);
        if (replayed.kind === "accepted") {
          const old = replayed.admissions.find(
            (row) => row.checkout_key === input.target.checkoutKey,
          );
          if (old) return old;
        }
        yield* (yield* CheckoutAcceptanceAuthority).assertNewAcceptance(input);
        // The receiving hook runs after its upstream receipt/event/effect writes in the same transaction.
        const r = yield* receipt(input.commandId);
        if (!r || r.status !== "accepted" || r.result_sequence !== input.receiptSequence)
          return yield* new CheckoutError({ reason: "unavailable" });
        yield* currentBirth(input.subjectThreadId, input.birth.eventId, input.birth.sequence);
        yield* currentTarget(input.target);
        const basis =
          yield* sql`SELECT event_id FROM orchestration_events WHERE event_id=${input.basisEventId}
      AND sequence=${input.basisSequence} AND stream_id=${input.subjectThreadId} AND command_id=${input.commandId}
      AND application_event_version=2`;
        if (basis.length !== 1 || input.basisSequence > input.receiptSequence)
          return yield* new CheckoutError({ reason: "identity" });
        const old = yield* owner(input.target.checkoutKey);
        for (const conflict of yield* conflicts(input.target.checkoutKey)) {
          if (conflict.kind !== "ordinary" || conflict.checkoutKey !== input.target.checkoutKey)
            return yield* new CheckoutError({ reason: "conflict" });
        }
        if (old !== null) {
          if (
            old.state !== "owned" ||
            old.owner_thread_id !== input.subjectThreadId ||
            old.birth_event_id !== input.birth.eventId ||
            old.birth_sequence !== input.birth.sequence ||
            !matchesTarget(old, input.target) ||
            input.expectedOwner === null ||
            input.expectedOwner.epoch !== old.owner_epoch ||
            input.expectedOwner.revision !== old.revision
          )
            return yield* new CheckoutError({ reason: "conflict" });
          if ((yield* pendingAdmissions(old.checkout_key)).length > 0)
            return yield* new CheckoutError({ reason: "conflict" });
        } else {
          if (input.expectedOwner !== null) return yield* new CheckoutError({ reason: "stale" });
          yield* sql`INSERT INTO jones_ordinary_checkout_owners ${sql.insert({
            checkout_key: input.target.checkoutKey,
            project_root: input.target.canonicalProjectRoot,
            directory_identity_json: directoryWitness(input.target),
            git_common_directory: input.target.gitCommonDirectory,
            lease_id: input.leaseId,
            owner_thread_id: input.subjectThreadId,
            birth_event_id: input.birth.eventId,
            birth_sequence: input.birth.sequence,
            branch: input.target.branch,
            acquired_at: input.now,
            renewed_at: input.now,
            expires_at: input.expiresAt,
            owner_epoch: 0,
            target_revision: 0,
            revision: 0,
            state: "owned",
          })}`;
        }
        const row: Admission = {
          admission_id: input.admissionId,
          command_id: input.commandId,
          receipt_thread_id: input.receiptThreadId,
          subject_thread_id: input.subjectThreadId,
          command_type: input.commandType,
          command_digest: digest(input.command),
          origin_kind: input.origin.kind,
          origin_parent_thread_id: input.origin.parentThreadId,
          request_id: input.origin.requestId,
          checkout_key: input.target.checkoutKey,
          lease_id: old?.lease_id ?? input.leaseId,
          owner_epoch: old?.owner_epoch ?? 0,
          target_revision: old?.target_revision ?? 0,
          birth_event_id: input.birth.eventId,
          birth_sequence: input.birth.sequence,
          receipt_sequence: input.receiptSequence,
          basis_event_id: input.basisEventId,
          basis_sequence: input.basisSequence,
          run_id: input.relation.runId,
          attempt_id: input.relation.attemptId,
          node_id: input.relation.nodeId,
          message_id: input.relation.messageId,
          checkpoint_scope_id: input.relation.checkpointScopeId,
          recorded_at: input.now,
        };
        if (!(yield* accepted(row))) return yield* new CheckoutError({ reason: "identity" });
        yield* sql`INSERT INTO jones_ordinary_checkout_admissions ${sql.insert({ ...row })}`;
        for (const effect of input.effects) yield* link(row, effect, input.now);
        return row;
      }),
    );
  const linkDerived: OrdinaryCheckout["Service"]["linkDerived"] = (input) =>
    transaction(
      Effect.gen(function* () {
        yield* decode(Utc, input.now);
        const a = yield* liveAdmission(input.admissionId);
        const row = yield* use(input.operationId);
        if (
          row.admission_id !== a.admission_id ||
          row.revision !== input.expectedRevision ||
          terminal(row.state) ||
          row.state === "unknown"
        )
          return yield* new CheckoutError({ reason: "stale" });
        yield* (yield* CheckoutAcceptanceAuthority).assertDerivedLink(input, a);
        const actual = yield* effectRequest(input.link.effectId);
        if (
          !actual ||
          actual.thread_id !== a.subject_thread_id ||
          digest(actual.request) !== digest(input.link.request)
        )
          return yield* new CheckoutError({ reason: "identity" });
        const basis =
          yield* sql`SELECT event_id FROM orchestration_events WHERE event_id=${input.basisEventId}
      AND sequence=${input.basisSequence} AND command_id=${actual.command_id} AND stream_id=${a.subject_thread_id}
      AND application_event_version=2`;
        if (basis.length !== 1) return yield* new CheckoutError({ reason: "identity" });
        yield* sql`INSERT INTO jones_ordinary_checkout_effect_links ${sql.insert({
          effect_id: input.link.effectId,
          admission_id: a.admission_id,
          request_digest: digest(actual.request),
          role: input.link.role,
          recorded_at: input.now,
        })}`;
        const updated = yield* sql<Use>`UPDATE jones_ordinary_checkout_uses SET revision=revision+1
      WHERE operation_id=${row.operation_id} AND revision=${row.revision} RETURNING *`;
        if (updated.length !== 1) return yield* new CheckoutError({ reason: "stale" });
        return updated[0]!;
      }),
    );
  const events = (id: string) =>
    sql<LifecycleEvent>`SELECT * FROM jones_ordinary_checkout_lifecycle_events WHERE operation_id=${id} ORDER BY ordinal`;
  const latestAssociations = (id: string) => sql<LifecycleEvent>`
    SELECT e.* FROM jones_ordinary_checkout_lifecycle_events e WHERE e.operation_id=${id}
      AND e.ordinal=(SELECT MAX(last.ordinal) FROM jones_ordinary_checkout_lifecycle_events last
        WHERE last.operation_id=e.operation_id AND last.association_id=e.association_id)`;
  const assertCoveredLinks = Effect.fn("OrdinaryCheckoutSqlite.assertCoveredLinks")(function* (
    a: Admission,
    participants: readonly LifecycleEvent[],
  ) {
    const coveredEffects = new Set<string>();
    for (const participant of participants) {
      const bound = yield* decode(Schema.fromJsonString(Binding), participant.binding_json);
      if (bound.source.kind === "outbox") coveredEffects.add(bound.source.effectId);
    }
    const links = yield* sql<{
      readonly effect_id: string;
    }>`SELECT effect_id FROM jones_ordinary_checkout_effect_links WHERE admission_id=${a.admission_id}`;
    for (const link of links) {
      if (coveredEffects.has(link.effect_id)) continue;
      const settled =
        yield* sql`SELECT operation_id FROM jones_ordinary_checkout_uses WHERE admission_id=${a.admission_id}
        AND effect_id=${link.effect_id} AND state IN ('completed','no_effect') LIMIT 1`;
      if (settled.length === 0) return yield* new CheckoutError({ reason: "unknown" });
    }
  });
  const sourceMatches = (row: Use, source: Source) =>
    row.source_kind === source.kind &&
    (source.kind === "outbox"
      ? row.effect_id === source.effectId &&
        row.worker_id === source.workerId &&
        row.attempt_count === source.attempt &&
        row.claim_expires_at === source.claimExpiresAt
      : row.producer_id === source.producerId);
  const verifyBinding = (row: Use, binding: Binding, a: Admission, joining = false) => {
    const runCaptured = Effect.fn("OrdinaryCheckoutSqlite.verifyBinding")(function* () {
      yield* decode(Binding, binding);
      if (
        binding.operationId !== row.operation_id ||
        binding.admissionId !== row.admission_id ||
        binding.relation.runId !== a.run_id ||
        (binding.executor === "managed_run" &&
          (binding.provider === null || binding.relation.runId === null))
      )
        return yield* new CheckoutError({ reason: "identity" });
      const prior = (yield* latestAssociations(row.operation_id)).find(
        (event) => event.association_id === associationId(binding),
      );
      if (prior) {
        if (prior.binding_json !== canonical(binding))
          return yield* new CheckoutError({ reason: "identity" });
      } else if (
        !joining &&
        (!sourceMatches(row, binding.source) ||
          binding.relation.attemptId !== a.attempt_id ||
          binding.relation.nodeId !== a.node_id ||
          binding.relation.checkpointScopeId !== a.checkpoint_scope_id ||
          binding.relation.messageId !== a.message_id)
      )
        return yield* new CheckoutError({ reason: "identity" });
    });
    return runCaptured();
  };
  const verifyLinkedSource = Effect.fn("OrdinaryCheckoutSqlite.verifyLinkedSource")(function* (
    a: Admission,
    source: Source,
  ) {
    if (source.kind !== "outbox") return;
    const actual = yield* effectRequest(source.effectId);
    const links = yield* sql<{
      readonly request_digest: string;
    }>`SELECT request_digest FROM jones_ordinary_checkout_effect_links
      WHERE effect_id=${source.effectId} AND admission_id=${a.admission_id}`;
    if (
      actual === null ||
      actual.thread_id !== a.subject_thread_id ||
      links.length !== 1 ||
      links[0]!.request_digest !== digest(actual.request)
    )
      return yield* new CheckoutError({ reason: "identity" });
  });
  const verifyClaim = Effect.fn("OrdinaryCheckoutSqlite.verifyClaim")(function* (
    a: Admission,
    source: Source,
    now: string,
  ) {
    yield* verifyLinkedSource(a, source);
    if (source.kind !== "outbox") return;
    const claims =
      yield* sql`SELECT effect_id FROM orchestration_v2_effect_outbox WHERE effect_id=${source.effectId}
      AND status='running' AND lease_owner=${source.workerId} AND attempt_count=${source.attempt}
      AND lease_expires_at=${source.claimExpiresAt} AND lease_expires_at>${now}`;
    if (claims.length !== 1) return yield* new CheckoutError({ reason: "stale" });
  });
  const append = (
    row: Use,
    input: EventInput,
    kind: LifecycleEvent["event_kind"],
    state: Use["state"] = row.state,
  ) => {
    const runCaptured = Effect.fn("OrdinaryCheckoutSqlite.append")(function* () {
      yield* decode(Binding, input.binding);
      yield* decode(Evidence, input.evidence);
      if (
        input.binding.operationId !== row.operation_id ||
        input.binding.admissionId !== row.admission_id ||
        input.evidence.issuerId !== input.binding.issuerId ||
        row.revision !== input.expectedRevision ||
        terminal(row.state)
      )
        return yield* new CheckoutError({ reason: "stale" });
      const bindingJson = canonical(input.binding);
      const evidenceJson = canonical(input.evidence);
      if (bindingJson.length > 8192 || evidenceJson.length > 8192)
        return yield* new CheckoutError({ reason: "identity" });
      yield* sql`INSERT INTO jones_ordinary_checkout_lifecycle_events ${sql.insert({
        operation_id: row.operation_id,
        ordinal: row.last_event_ordinal + 1,
        predecessor_ordinal: row.last_event_ordinal,
        association_id: associationId(input.binding),
        event_kind: kind,
        binding_json: bindingJson,
        evidence_json: evidenceJson,
        recorded_at: input.evidence.observedAt,
      })}`;
      const updated =
        yield* sql<Use>`UPDATE jones_ordinary_checkout_uses SET state=${state}, revision=revision+1,
      last_event_ordinal=last_event_ordinal+1,
      started_at=${state === "started" && row.started_at === null ? input.evidence.observedAt : row.started_at},
      settled_at=${terminal(state) ? input.evidence.observedAt : null}
      WHERE operation_id=${row.operation_id} AND revision=${row.revision} AND owner_epoch=${row.owner_epoch} RETURNING *`;
      if (updated.length !== 1) return yield* new CheckoutError({ reason: "stale" });
      return updated[0]!;
    });
    return runCaptured();
  };
  // Authorization admits an invocation; it is not an observation of physical work.
  const admitsInvocation = (evidence: Evidence) =>
    evidence.outcome === "active" ||
    (evidence.outcome === "authorized" && evidence.coverage === "unknown");
  const reserve: OrdinaryCheckout["Service"]["reserve"] = (input) =>
    transaction(
      Effect.gen(function* () {
        yield* decode(Source, input.source);
        yield* decode(Utc, input.now);
        const a = yield* liveAdmission(input.admissionId);
        const id = operationId(a.admission_id, input.source);
        const row: Use = {
          operation_id: id,
          admission_id: a.admission_id,
          checkout_key: a.checkout_key,
          lease_id: a.lease_id,
          owner_epoch: a.owner_epoch,
          source_kind: input.source.kind,
          effect_id: input.source.kind === "outbox" ? input.source.effectId : null,
          worker_id: input.source.kind === "outbox" ? input.source.workerId : null,
          attempt_count: input.source.kind === "outbox" ? input.source.attempt : null,
          claim_expires_at: input.source.kind === "outbox" ? input.source.claimExpiresAt : null,
          producer_id: input.source.kind === "outbox" ? null : input.source.producerId,
          state: "reserved",
          revision: 0,
          last_event_ordinal: 0,
          started_at: null,
          settled_at: null,
        };
        yield* verifyBinding(row, input.binding, a);
        yield* verifyClaim(a, input.source, input.now);
        const authority = yield* CheckoutExecutorAuthority;
        yield* decode(Evidence, input.evidence);
        if (!admitsInvocation(input.evidence) || input.evidence.observedAt !== input.now)
          return yield* new CheckoutError({ reason: "identity" });
        yield* authority.assertCurrent(input.binding);
        yield* authority.reconcile(input.binding, input.evidence);
        const prior =
          yield* sql<Use>`SELECT * FROM jones_ordinary_checkout_uses WHERE operation_id=${id}`;
        if (prior.length > 0) {
          if (
            canonical({ ...prior[0], revision: 0, last_event_ordinal: 0 }) !== canonical(row) ||
            !(yield* events(id)).some(
              (event) =>
                event.event_kind === "bind" && event.binding_json === canonical(input.binding),
            )
          )
            return yield* new CheckoutError({ reason: "stale" });
          return prior[0]!;
        }
        // Joining a running operation uses its participant journal, never a second independent writer.
        const active = yield* sql`SELECT operation_id FROM jones_ordinary_checkout_uses
      WHERE checkout_key=${a.checkout_key} AND state NOT IN ('completed','no_effect') LIMIT 1`;
        if (active.length > 0) return yield* new CheckoutError({ reason: "conflict" });
        yield* sql`INSERT INTO jones_ordinary_checkout_uses ${sql.insert({ ...row })}`;
        return yield* append(row, { ...input, expectedRevision: 0 }, "bind");
      }),
    );
  const currentEvent = (input: EventInput, allowUnknown = false, joining = false) => {
    const runCaptured = Effect.fn("OrdinaryCheckoutSqlite.currentEvent")(function* () {
      const row = yield* use(input.binding.operationId);
      if (
        row.revision !== input.expectedRevision ||
        terminal(row.state) ||
        (!allowUnknown && row.state === "unknown")
      )
        return yield* new CheckoutError({ reason: "stale" });
      if (
        !allowUnknown &&
        (yield* latestAssociations(row.operation_id)).some(
          (event) => event.event_kind === "unknown",
        )
      )
        return yield* new CheckoutError({ reason: "unknown" });
      const a = yield* liveAdmission(row.admission_id, allowUnknown);
      yield* verifyBinding(row, input.binding, a, joining);
      yield* decode(Evidence, input.evidence);
      return { row, admission: a };
    });
    return runCaptured();
  };
  const start: OrdinaryCheckout["Service"]["start"] = (input) =>
    transaction(
      Effect.gen(function* () {
        const { row, admission: a } = yield* currentEvent(input);
        if (
          row.state !== "reserved" ||
          !admitsInvocation(input.evidence) ||
          !sourceMatches(row, input.binding.source) ||
          (yield* latestAssociations(row.operation_id)).some(
            (event) =>
              event.association_id === associationId(input.binding) &&
              event.event_kind === "retire",
          )
        )
          return yield* new CheckoutError({ reason: "stale" });
        yield* currentTarget(input.target);
        if (
          input.target.threadId !== a.subject_thread_id ||
          !matchesTarget(yield* currentOwner(a), input.target)
        )
          return yield* new CheckoutError({ reason: "stale" });
        yield* verifyClaim(a, input.binding.source, input.evidence.observedAt);
        const authority = yield* CheckoutExecutorAuthority;
        yield* authority.assertCurrent(input.binding);
        yield* authority.reconcile(input.binding, input.evidence);
        // Persist before invocation. Failure or interruption after this boundary is never an automatic retry.
        return yield* append(row, input, "activate", "started");
      }),
    );
  const join: OrdinaryCheckout["Service"]["join"] = (input) =>
    transaction(
      Effect.gen(function* () {
        const { row, admission: a } = yield* currentEvent(input, false, true);
        yield* currentTarget(input.target);
        if (
          input.target.threadId !== a.subject_thread_id ||
          !matchesTarget(yield* currentOwner(a), input.target)
        )
          return yield* new CheckoutError({ reason: "stale" });
        if (row.state !== "started" && row.state !== "draining")
          return yield* new CheckoutError({ reason: "stale" });
        if (!admitsInvocation(input.evidence))
          return yield* new CheckoutError({ reason: "identity" });
        const existing = yield* sql`SELECT ordinal FROM jones_ordinary_checkout_lifecycle_events
      WHERE operation_id=${row.operation_id} AND association_id=${associationId(input.binding)} LIMIT 1`;
        if (existing.length > 0) return yield* new CheckoutError({ reason: "stale" });
        yield* verifyClaim(a, input.binding.source, input.evidence.observedAt);
        const authority = yield* CheckoutExecutorAuthority;
        yield* authority.assertCurrent(input.binding);
        yield* authority.reconcile(input.binding, input.evidence);
        return yield* append(row, input, "join", "draining");
      }),
    );
  const renew: OrdinaryCheckout["Service"]["renew"] = (input) =>
    transaction(
      Effect.gen(function* () {
        const { row, admission: a } = yield* currentEvent(input);
        const lease = yield* currentOwner(a);
        yield* currentTarget(input.target);
        if (input.target.threadId !== a.subject_thread_id || !matchesTarget(lease, input.target))
          return yield* new CheckoutError({ reason: "stale" });
        yield* decode(Utc, input.expiresAt);
        const latest = (yield* latestAssociations(row.operation_id)).find(
          (event) => event.association_id === associationId(input.binding),
        );
        if (
          !latest ||
          latest.event_kind === "retire" ||
          latest.event_kind === "unknown" ||
          input.evidence.outcome !== "active" ||
          lease.revision !== input.expectedOwnerRevision ||
          input.expiresAt < lease.expires_at ||
          input.evidence.observedAt < lease.renewed_at ||
          input.expiresAt < input.evidence.observedAt
        )
          return yield* new CheckoutError({ reason: "stale" });
        const authority = yield* CheckoutExecutorAuthority;
        yield* authority.assertCurrent(input.binding);
        yield* authority.reconcile(input.binding, input.evidence);
        const changed =
          yield* sql`UPDATE jones_ordinary_checkout_owners SET renewed_at=${input.evidence.observedAt},
      expires_at=${input.expiresAt},revision=revision+1 WHERE checkout_key=${lease.checkout_key}
      AND revision=${lease.revision} AND owner_epoch=${lease.owner_epoch} RETURNING checkout_key`;
        if (changed.length !== 1) return yield* new CheckoutError({ reason: "stale" });
        return yield* append(row, input, "renew");
      }),
    );
  const reconcileEvidence: OrdinaryCheckout["Service"]["reconcileEvidence"] = (input) =>
    transaction(
      Effect.gen(function* () {
        if (input.evidence.outcome === "authorized")
          return yield* new CheckoutError({ reason: "identity" });
        const candidate = yield* use(input.binding.operationId);
        const cancellingLink =
          candidate.started_at === null &&
          input.binding.source.kind === "outbox" &&
          input.evidence.outcome === "not_invoked" &&
          input.evidence.coverage === "complete";
        const { row, admission: a } = yield* currentEvent(input, true, cancellingLink);
        const latest = (yield* latestAssociations(row.operation_id)).find(
          (event) => event.association_id === associationId(input.binding),
        );
        if (
          (!latest && row.state !== "reserved" && row.started_at !== null) ||
          latest?.event_kind === "retire"
        )
          return yield* new CheckoutError({ reason: "identity" });
        if (!latest) {
          if (!cancellingLink) return yield* new CheckoutError({ reason: "identity" });
          // Add only an already-retired exact link: no join or invocation capability
          // is issued, and a pending outbox row is never claimed on its behalf.
          yield* verifyLinkedSource(a, input.binding.source);
        }
        const authority = yield* CheckoutExecutorAuthority;
        // Reconciliation is deliberately separate from current invocation authority: old, expired executors can prove retirement.
        yield* authority.reconcile(input.binding, input.evidence);
        if (input.evidence.outcome === "unknown" || input.evidence.coverage !== "complete")
          return yield* append(row, input, "unknown", "unknown");
        if (input.evidence.outcome !== "quiescent" && input.evidence.outcome !== "not_invoked")
          return yield* new CheckoutError({ reason: "identity" });
        if (input.evidence.outcome === "not_invoked") {
          const activated = yield* sql`SELECT ordinal FROM jones_ordinary_checkout_lifecycle_events
          WHERE operation_id=${row.operation_id} AND association_id=${associationId(input.binding)}
          AND event_kind='activate' LIMIT 1`;
          if (activated.length > 0) return yield* new CheckoutError({ reason: "unknown" });
        }
        const otherUnknown = (yield* latestAssociations(row.operation_id)).some(
          (event) =>
            event.association_id !== associationId(input.binding) && event.event_kind === "unknown",
        );
        return yield* append(
          row,
          input,
          "retire",
          otherUnknown ? "unknown" : row.started_at === null ? "reserved" : "draining",
        );
      }),
    );
  const complete: OrdinaryCheckout["Service"]["complete"] = (input) =>
    transaction(
      Effect.gen(function* () {
        yield* decode(Utc, input.now);
        const row = yield* use(input.operationId);
        if (row.revision !== input.expectedRevision || terminal(row.state))
          return yield* new CheckoutError({ reason: "stale" });
        const a = yield* liveAdmission(row.admission_id, true);
        yield* currentTarget(input.target);
        if (
          input.target.threadId !== a.subject_thread_id ||
          !matchesTarget(yield* currentOwner(a, true), input.target)
        )
          return yield* new CheckoutError({ reason: "stale" });
        const participants = yield* latestAssociations(row.operation_id);
        if (
          participants.length === 0 ||
          participants.some((event) => event.event_kind !== "retire")
        )
          return yield* new CheckoutError({ reason: "unknown" });
        if (row.started_at === null)
          for (const participant of participants) {
            const evidence = yield* decode(
              Schema.fromJsonString(Evidence),
              participant.evidence_json,
            );
            if (evidence.outcome !== "not_invoked")
              return yield* new CheckoutError({ reason: "unknown" });
          }
        // A checkpoint/follow-up link cannot disappear behind outbox success or a root participant's retirement.
        yield* assertCoveredLinks(a, participants);
        const updated =
          yield* sql<Use>`UPDATE jones_ordinary_checkout_uses SET state=${row.started_at === null ? "no_effect" : "completed"},
      settled_at=${input.now},revision=revision+1 WHERE operation_id=${row.operation_id} AND revision=${row.revision} RETURNING *`;
        if (updated.length !== 1) return yield* new CheckoutError({ reason: "stale" });
        return updated[0]!;
      }),
    );
  const markRestartUnknown: OrdinaryCheckout["Service"]["markRestartUnknown"] = (input) =>
    transaction(
      Effect.gen(function* () {
        yield* decode(Utc, input.now);
        const row = yield* use(input.operationId);
        if (terminal(row.state) || row.revision !== input.expectedRevision)
          return yield* new CheckoutError({ reason: "stale" });
        // Preserve attributed association identities; recovery records ignorance, never claims their retirement.
        let ordinal = row.last_event_ordinal;
        for (const event of yield* latestAssociations(row.operation_id)) {
          if (event.event_kind === "retire") continue;
          yield* sql`INSERT INTO jones_ordinary_checkout_lifecycle_events ${sql.insert({
            operation_id: row.operation_id,
            ordinal: ++ordinal,
            predecessor_ordinal: ordinal - 1,
            association_id: event.association_id,
            event_kind: "unknown",
            binding_json: event.binding_json,
            evidence_json: canonical({
              version: 1,
              issuerId: "checkout-recovery",
              reference: `restart:${row.operation_id}:${row.revision}`,
              outcome: "unknown",
              coverage: "unknown",
              observedAt: input.now,
            }),
            recorded_at: input.now,
          })}`;
        }
        const rows =
          yield* sql<Use>`UPDATE jones_ordinary_checkout_uses SET state='unknown',revision=revision+1,last_event_ordinal=${ordinal}
      WHERE operation_id=${row.operation_id} AND revision=${row.revision} RETURNING *`;
        if (rows.length !== 1) return yield* new CheckoutError({ reason: "stale" });
        return rows[0]!;
      }),
    );
  const transitionOperation = Effect.fn("OrdinaryCheckoutSqlite.transitionOperation")(function* (
    row: Use,
  ) {
    const history = yield* events(row.operation_id);
    if (history.length === 0) return yield* new CheckoutError({ reason: "identity" });
    const original = yield* decode(Schema.fromJsonString(Binding), history[0]!.binding_json);
    if (original.executor !== "transition") return yield* new CheckoutError({ reason: "identity" });
    const participants = yield* latestAssociations(row.operation_id);
    if (
      participants.some(
        (event) => !["retire", "release", "target_transition"].includes(event.event_kind),
      )
    )
      return yield* new CheckoutError({ reason: "unknown" });
  });
  const transition = (input: TransitionInput, after: Target | null) =>
    transaction(
      Effect.gen(function* () {
        const row = yield* use(input.operationId);
        const a = yield* liveAdmission(row.admission_id);
        const lease = yield* currentOwner(a);
        if (
          row.state === "unknown" ||
          row.revision !== input.expectedRevision ||
          lease.revision !== input.expectedOwner.revision ||
          lease.owner_epoch !== input.expectedOwner.epoch ||
          !matchesTarget(lease, input.target) ||
          input.binding.executor !== "transition" ||
          input.binding.operationId !== row.operation_id ||
          input.evidence.outcome !== "quiescent" ||
          input.evidence.coverage !== "complete" ||
          input.evidence.issuerId !== input.binding.issuerId
        )
          return yield* new CheckoutError({ reason: "stale" });
        yield* transitionOperation(row);
        yield* assertCoveredLinks(a, yield* latestAssociations(row.operation_id));
        if (after !== null && row.started_at === null)
          return yield* new CheckoutError({ reason: "unknown" });
        if (
          (yield* pendingAdmissions(lease.checkout_key)).some(
            (pending) => pending.admission_id !== a.admission_id,
          )
        )
          return yield* new CheckoutError({ reason: "unknown" });
        const others =
          yield* sql`SELECT operation_id FROM jones_ordinary_checkout_uses WHERE checkout_key=${row.checkout_key}
      AND operation_id!=${row.operation_id} AND state NOT IN ('completed','no_effect') LIMIT 1`;
        if (others.length > 0) return yield* new CheckoutError({ reason: "unknown" });
        yield* verifyBinding(row, input.binding, a);
        const authority = yield* CheckoutExecutorAuthority;
        yield* authority.reconcile(input.binding, input.evidence);
        yield* currentTarget(after ?? input.target);
        if (
          after !== null &&
          (after.checkoutKey !== input.target.checkoutKey ||
            canonical({
              ...after,
              branch: input.target.branch,
              threadBranch: input.target.threadBranch,
              threadRevision: input.target.threadRevision,
            }) !== canonical(input.target))
        )
          return yield* new CheckoutError({ reason: "identity" });
        yield* decode(Evidence, input.evidence);
        yield* (yield* CheckoutOwnershipAuthority).authorize({
          action: after === null ? "release" : "target_transition",
          owner: lease,
          operationId: row.operation_id,
          nextThreadId: null,
        });
        const transitionEvidence = {
          ...input.evidence,
          transition: {
            beforeEpoch: lease.owner_epoch,
            afterEpoch: lease.owner_epoch,
            beforeLease: lease.lease_id,
            afterLease: lease.lease_id,
            beforeThread: lease.owner_thread_id,
            afterThread: lease.owner_thread_id,
            beforeBirth: lease.birth_event_id,
            afterBirth: lease.birth_event_id,
            beforeBranch: lease.branch,
            afterBranch: after === null ? lease.branch : after.branch,
          },
        };
        // Only the dedicated transition operation can settle and advance this ownership boundary.
        const last = (yield* events(row.operation_id)).at(-1)?.ordinal ?? 0;
        yield* sql`INSERT INTO jones_ordinary_checkout_lifecycle_events ${sql.insert({
          operation_id: row.operation_id,
          ordinal: last + 1,
          predecessor_ordinal: last,
          association_id: associationId(input.binding),
          event_kind: after === null ? "release" : "target_transition",
          binding_json: canonical(input.binding),
          evidence_json: canonical(transitionEvidence),
          recorded_at: input.evidence.observedAt,
        })}`;
        const advanced =
          yield* sql`UPDATE jones_ordinary_checkout_uses SET revision=revision+1,last_event_ordinal=${last + 1},
      state=${terminal(row.state) ? row.state : row.started_at === null ? "no_effect" : "completed"},
      settled_at=${row.settled_at ?? input.evidence.observedAt}
      WHERE operation_id=${row.operation_id} AND revision=${row.revision} RETURNING operation_id`;
        if (advanced.length !== 1) return yield* new CheckoutError({ reason: "stale" });
        const changed =
          yield* sql<Owner>`UPDATE jones_ordinary_checkout_owners SET state=${after === null ? "released" : "owned"},
      branch=${after === null ? lease.branch : after.branch}, target_revision=target_revision+${after === null ? 0 : 1},revision=revision+1
      WHERE checkout_key=${lease.checkout_key} AND owner_epoch=${lease.owner_epoch} AND revision=${lease.revision} RETURNING *`;
        if (changed.length !== 1) return yield* new CheckoutError({ reason: "stale" });
        return changed[0]!;
      }),
    );
  const handoff: OrdinaryCheckout["Service"]["handoff"] = (input) =>
    transaction(
      Effect.gen(function* () {
        const row = yield* use(input.operationId);
        const a = yield* admission(row.admission_id);
        const lease = yield* owner(row.checkout_key);
        if (
          !a ||
          !(yield* accepted(a)) ||
          !lease ||
          lease.state !== "released" ||
          !terminal(row.state) ||
          row.revision !== input.expectedRevision ||
          lease.revision !== input.expectedOwner.revision ||
          lease.owner_epoch !== input.expectedOwner.epoch ||
          row.owner_epoch !== lease.owner_epoch ||
          !matchesTarget(lease, input.target) ||
          !matchesTarget(lease, input.next.target) ||
          input.binding.executor !== "transition" ||
          input.evidence.outcome !== "quiescent" ||
          input.evidence.coverage !== "complete" ||
          input.evidence.issuerId !== input.binding.issuerId
        )
          return yield* new CheckoutError({ reason: "stale" });
        yield* transitionOperation(row);
        yield* verifyBinding(row, input.binding, a);
        yield* decode(Evidence, input.evidence);
        yield* decode(Utc, input.next.expiresAt);
        if (input.next.expiresAt < input.evidence.observedAt)
          return yield* new CheckoutError({ reason: "identity" });
        yield* currentTarget(input.next.target);
        yield* currentBirth(
          input.next.target.threadId,
          input.next.birth.eventId,
          input.next.birth.sequence,
        );
        if (
          (yield* conflicts(row.checkout_key)).length > 0 ||
          (yield* pendingAdmissions(row.checkout_key)).length > 0
        )
          return yield* new CheckoutError({ reason: "conflict" });
        yield* (yield* CheckoutExecutorAuthority).reconcile(input.binding, input.evidence);
        yield* (yield* CheckoutOwnershipAuthority).authorize({
          action: "handoff",
          owner: lease,
          operationId: row.operation_id,
          nextThreadId: input.next.target.threadId,
        });
        const evidence = {
          ...input.evidence,
          transition: {
            beforeEpoch: lease.owner_epoch,
            afterEpoch: lease.owner_epoch + 1,
            beforeLease: lease.lease_id,
            afterLease: input.next.leaseId,
            beforeThread: lease.owner_thread_id,
            afterThread: input.next.target.threadId,
            beforeBirth: lease.birth_event_id,
            afterBirth: input.next.birth.eventId,
            beforeBranch: lease.branch,
            afterBranch: input.next.target.branch,
          },
        };
        const ordinal = row.last_event_ordinal + 1;
        yield* sql`INSERT INTO jones_ordinary_checkout_lifecycle_events ${sql.insert({
          operation_id: row.operation_id,
          ordinal,
          predecessor_ordinal: ordinal - 1,
          association_id: associationId(input.binding),
          event_kind: "handoff",
          binding_json: canonical(input.binding),
          evidence_json: canonical(evidence),
          recorded_at: input.evidence.observedAt,
        })}`;
        const advanced =
          yield* sql`UPDATE jones_ordinary_checkout_uses SET revision=revision+1,last_event_ordinal=${ordinal}
      WHERE operation_id=${row.operation_id} AND revision=${row.revision} RETURNING operation_id`;
        if (advanced.length !== 1) return yield* new CheckoutError({ reason: "stale" });
        const changed =
          yield* sql<Owner>`UPDATE jones_ordinary_checkout_owners SET owner_epoch=owner_epoch+1,revision=revision+1,
      target_revision=target_revision+1,state='owned',lease_id=${input.next.leaseId},owner_thread_id=${input.next.target.threadId},
      birth_event_id=${input.next.birth.eventId},birth_sequence=${input.next.birth.sequence},project_root=${input.next.target.canonicalProjectRoot},
      acquired_at=${input.evidence.observedAt},renewed_at=${input.evidence.observedAt},expires_at=${input.next.expiresAt}
      WHERE checkout_key=${row.checkout_key} AND revision=${lease.revision} AND owner_epoch=${lease.owner_epoch} RETURNING *`;
        if (changed.length !== 1) return yield* new CheckoutError({ reason: "stale" });
        return changed[0]!;
      }),
    );
  return OrdinaryCheckout.of({
    replay: (input) => safe(replay(input)),
    accept,
    linkDerived,
    acceptMany: (inputs) =>
      transaction(
        Effect.gen(function* () {
          if (
            inputs.length === 0 ||
            inputs.length > 32 ||
            new Set(inputs.map((input) => input.target.checkoutKey)).size !== inputs.length
          )
            return yield* new CheckoutError({ reason: "identity" });
          if (
            inputs.some(
              (input) =>
                input.commandId !== inputs[0]!.commandId ||
                input.receiptThreadId !== inputs[0]!.receiptThreadId ||
                digest(input.command) !== digest(inputs[0]!.command),
            )
          )
            return yield* new CheckoutError({ reason: "identity" });
          const rows: Admission[] = [];
          for (const input of [...inputs].sort((a, b) =>
            a.target.checkoutKey < b.target.checkoutKey
              ? -1
              : a.target.checkoutKey > b.target.checkoutKey
                ? 1
                : 0,
          ))
            rows.push(yield* accept(input));
          return rows;
        }),
      ),
    readOwner: (key) => safe(owner(key)),
    readConflict: (key) => safe(conflicts(key)),
    readAdmission: (id) =>
      safe(
        Effect.gen(function* () {
          const row = yield* admission(id);
          return row !== null && (yield* accepted(row)) ? row : null;
        }),
      ),
    readEffect: (id) =>
      safe(
        Effect.gen(function* () {
          const actual = yield* effectRequest(id);
          if (actual === null) return [];
          const links = yield* sql<{
            readonly admission_id: string;
            readonly request_digest: string;
          }>`SELECT admission_id,request_digest FROM jones_ordinary_checkout_effect_links WHERE effect_id=${id}`;
          const rows: Admission[] = [];
          for (const link of links) {
            const row = yield* admission(link.admission_id);
            if (row && link.request_digest === digest(actual.request) && (yield* accepted(row)))
              rows.push(row);
          }
          return rows;
        }),
      ),
    reserve,
    start,
    join,
    renew,
    reconcileEvidence,
    complete,
    markRestartUnknown,
    listUnsettled: (after, limit) =>
      safe(
        Effect.gen(function* () {
          if (!Number.isInteger(limit) || limit < 1 || limit > 256)
            return yield* new CheckoutError({ reason: "identity" });
          return yield* sql<Use>`SELECT * FROM jones_ordinary_checkout_uses WHERE state IN ('reserved','started','draining','unknown')
        AND operation_id>${after ?? ""} ORDER BY operation_id LIMIT ${limit}`;
        }),
      ),
    events: (id) => safe(events(id)),
    handoff,
    release: (input) => transition(input, null),
    transitionBranch: (input) => transition(input, input.after),
  });
});
export const layer = Layer.effect(OrdinaryCheckout, make);
