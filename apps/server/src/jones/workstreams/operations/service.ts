import type {
  MembershipEpisode,
  T3WorkstreamBinding,
  Workstream,
  WorkstreamCommand,
  WorkstreamReceipt,
  WorkstreamReadContext,
} from "@t3tools/contracts";
import {
  WORKSTREAM_OPERATIONS_CONTRACT,
  parseWorkstreamOperationResult,
  type WorkstreamOperationRequest,
  type WorkstreamOperationResult,
  type WorkstreamOperationReadRequest,
  type WorkstreamOperationReadResponse,
  type WorkstreamOperationSubmitRequest,
  type WorkstreamSetupStatus,
} from "@t3tools/contracts/jones/workstreamOperations";
import * as Context from "effect/Context";
import * as Crypto from "effect/Crypto";
import * as DateTime from "effect/DateTime";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Hex from "effect/encoding/Hex";
import * as Layer from "effect/Layer";
import * as Schema from "effect/Schema";
import * as Scope from "effect/Scope";
import * as Semaphore from "effect/Semaphore";
import {
  WorkstreamGatewayError,
  type WorkstreamRecoverySubmitResult,
} from "../../../workstreams/WorkstreamGateway.ts";
import { workstreamChildId } from "./childIds.ts";
import { canonicalJson } from "./encoding.ts";
import { Ports } from "./ports.ts";
import { planSetPrimary } from "./planner.ts";

export class OperationConflict extends Schema.TaggedError<OperationConflict>()(
  "OperationConflict",
  {
    code: Schema.Literals(["operation_id_conflict", "thread_operation_in_progress"]),
    open_operation_id: Schema.optional(Schema.String),
  },
) {}
export class OperationInternalError extends Schema.TaggedError<OperationInternalError>()(
  "OperationInternalError",
  {},
) {}

export class WorkstreamOperations extends Context.Service<
  WorkstreamOperations,
  {
    readonly submit: (
      input: WorkstreamOperationSubmitRequest,
    ) => Effect.Effect<WorkstreamOperationResult, OperationConflict | OperationInternalError>;
    readonly read: (
      input: WorkstreamOperationReadRequest,
    ) => Effect.Effect<WorkstreamOperationReadResponse>;
    readonly setupStatus: () => Effect.Effect<WorkstreamSetupStatus>;
  }
>()("t3/jones/workstreams/operations/service/WorkstreamOperations") {}

type WorkstreamOperationObserved = NonNullable<WorkstreamOperationResult["observed"]>;
type FailureResult = Extract<WorkstreamOperationResult, { state: "rejected" | "unavailable" }>;
type Failure = FailureResult extends infer R
  ? R extends FailureResult
    ? Pick<R, "state" | "reason" | "detail">
    : never
  : never;
class StopRun extends Schema.TaggedError<StopRun>()("StopRun", { result: Schema.Unknown }) {}
class Replan extends Schema.TaggedError<Replan>()("Replan", {
  delay: Schema.optional(Schema.Number),
  backpressure: Schema.optional(Schema.Boolean),
}) {}
type Kind = "mem" | "cre" | "reg" | "ver";
type Committed = Extract<WorkstreamReceipt, { state: "committed" }>;
type Child = {
  readonly command: WorkstreamCommand;
  readonly body: string;
  readonly digest: string;
  readonly kind: Kind;
  readonly ownerId: string;
  readonly principalId: string;
};
type Entry = {
  readonly request: WorkstreamOperationRequest;
  readonly digest: string;
  readonly ids: Record<Kind, readonly string[]>;
  result: WorkstreamOperationResult;
  phase: "running" | "sealing" | "finished";
  done: Deferred.Deferred<void>;
  wake: Deferred.Deferred<void>;
  epoch?: number;
  firstAbsent?: number;
  regAbsent: boolean;
  unknown?: Child;
  failure?: Failure;
  finishedAt?: number;
};
type Scan = {
  readonly binding: T3WorkstreamBinding;
  readonly absent: number;
  readonly committed?: Committed;
  readonly pending?: WorkstreamReceipt;
};
const unavailable = (
  reason:
    | "offline"
    | "not_configured"
    | "contention"
    | "placements_not_ready"
    | "destination_scan_bound"
    | "verification_pending"
    | "verification_unresolved"
    | "backpressure",
): Failure => ({ state: "unavailable", reason, detail: null });
const rejected = (
  reason:
    | "foreign_thread"
    | "thread_not_found"
    | "registry_mismatch"
    | "permission_denied"
    | "workstream_not_found"
    | "identity_conflict"
    | "verification_rejected",
): Failure => ({ state: "rejected", reason, detail: null });
const configFailure = (failure: Failure) =>
  [
    "foreign_thread",
    "registry_mismatch",
    "permission_denied",
    "offline",
    "not_configured",
    "registration_unavailable",
  ].includes(failure.reason);
const settled = (result: WorkstreamOperationResult) =>
  result.state === "committed" ||
  result.state === "already_satisfied" ||
  result.state === "conflict";
const threadKey = (thread: { readonly environment_id: string; readonly thread_id: string }) =>
  JSON.stringify([thread.environment_id, thread.thread_id]);
const sameContext = (context: WorkstreamReadContext, binding: T3WorkstreamBinding) =>
  context.owner_id === binding.ownerId &&
  context.server_generation === binding.serverGeneration &&
  context.registry_version === binding.registryVersion;

const make = Effect.gen(function* () {
  const ports = yield* Ports;
  const crypto = yield* Crypto.Crypto;
  const scope = yield* Scope.Scope;
  const gateway = ports.gateway;
  const admission = yield* Semaphore.make(1);
  const entries = new Map<string, Entry>();
  const locks = new Map<string, Entry>();
  const assignments = new Map<string, { observed: WorkstreamOperationObserved; expires: number }>();
  const digest = (body: string) =>
    crypto.digest("SHA-256", new TextEncoder().encode(body)).pipe(Effect.map(Hex.encode));
  const queue: { priority: number; run: Effect.Effect<void> }[] = [];
  let writing = false;
  const write = (entry: Entry, child: Child) =>
    Effect.gen(function* () {
      const done = yield* Deferred.make<WorkstreamRecoverySubmitResult, WorkstreamGatewayError>();
      queue.push({
        priority: entry.request.origin.kind === "queue" ? 1 : 0,
        run: gateway.resubmit(child.command.command_id, child.body).pipe(
          Effect.matchEffect({
            onSuccess: (value) => Deferred.succeed(done, value),
            onFailure: (error) => Deferred.fail(done, error),
          }),
          Effect.asVoid,
        ),
      });
      if (!writing) {
        writing = true;
        yield* Effect.gen(function* () {
          while (queue.length > 0) {
            queue.sort((a, b) => a.priority - b.priority);
            yield* queue.shift()!.run;
          }
          writing = false;
        }).pipe(Effect.forkIn(scope));
      }
      return yield* Deferred.await(done);
    });
  const update = (entry: Entry, patch: object) =>
    Effect.gen(function* () {
      const at = DateTime.formatIso(DateTime.makeUnsafe(yield* ports.now));
      entry.result = parseWorkstreamOperationResult({ ...entry.result, ...patch, updated_at: at });
    });
  const record = (entry: Entry, receipt: WorkstreamReceipt) => {
    const child = {
      command_id: receipt.command_id,
      operation: receipt.operation,
      outcome: receipt.state === "unresolved" ? ("pending" as const) : receipt.state,
    };
    const prior = entry.result.children.findIndex((c) => c.command_id === child.command_id);
    entry.result = {
      ...entry.result,
      children:
        prior < 0
          ? [...entry.result.children, child].slice(-16)
          : entry.result.children.map((value, index) => (index === prior ? child : value)),
    };
  };
  const stop = (failure: Failure) => Effect.fail(new StopRun({ result: failure }));
  const retry = (delay?: number, backpressure?: boolean) =>
    Effect.fail(
      new Replan({
        ...(delay === undefined ? {} : { delay }),
        ...(backpressure === undefined ? {} : { backpressure }),
      }),
    );
  const finalKind = (entry: Entry): Kind =>
    entry.request.kind === "create_workstream" ? "cre" : "mem";
  const validReceipt = (entry: Entry, kind: Kind, id: string, receipt: WorkstreamReceipt) => {
    const operationMatches =
      kind === "reg"
        ? receipt.operation === "register_reference"
        : kind === "ver"
          ? receipt.operation === "verify_reference"
          : kind === "cre"
            ? receipt.operation === "create_workstream"
            : ["attach_primary", "reattach_primary", "move_primary", "remove_membership"].includes(
                receipt.operation,
              );
    const registry =
      entry.request.kind === "create_workstream" ? entry.request.registry : entry.request.target;
    // The gateway validates the current principal/generation. Here, bind recovery to this request.
    return receipt.command_id === id && receipt.owner_id === registry.owner_id && operationMatches;
  };
  const invalidReceipt = () =>
    new WorkstreamGatewayError({
      reason: "invalid-response",
      detail: "Operation receipt correlation failed.",
    });
  const scan = (
    entry: Entry,
    kind = finalKind(entry),
  ): Effect.Effect<Scan, WorkstreamGatewayError> =>
    Effect.gen(function* () {
      // R8: sealing assumes the predecessor exited before this process's first CP read.
      // This source implementation cannot establish that operational ordering for a deployment.
      const binding = yield* gateway.readSession();
      entry.epoch ??= binding.registryVersion;
      let committed: Committed | undefined;
      for (let index = 1; index <= 9; index++) {
        const id = entry.ids[kind][index - 1]!;
        const read = yield* gateway.readReceipt(id);
        if (read.kind === "absent") {
          if (kind === finalKind(entry)) entry.firstAbsent ??= index;
          if (kind === "reg" && index === 1) entry.regAbsent = true;
          return { binding, absent: index, ...(committed ? { committed } : {}) };
        }
        if (!validReceipt(entry, kind, id, read.receipt)) return yield* invalidReceipt();
        record(entry, read.receipt);
        if (read.receipt.state === "committed") {
          if (kind === finalKind(entry)) return { binding, absent: index, committed: read.receipt };
          committed = read.receipt;
          continue;
        }
        if (read.receipt.state !== "rejected") {
          if (kind === "ver") return { binding, absent: index, pending: read.receipt };
          return yield* invalidReceipt();
        }
      }
      if (kind === finalKind(entry)) entry.firstAbsent ??= 10;
      return { binding, absent: 10, ...(committed ? { committed } : {}) };
    });
  const observed = (entry: Entry, binding?: T3WorkstreamBinding) =>
    Effect.gen(function* () {
      const request = entry.request;
      if (request.kind !== "set_primary") return null;
      const placements = yield* gateway.readThreadPlacements({
        identities: [
          {
            source_instance_id: request.thread.environment_id,
            native_thread_id: request.thread.thread_id,
          },
        ],
      });
      if (binding && !sameContext(placements.page.context, binding)) return yield* invalidReceipt();
      if (placements.readiness !== "ready") return yield* invalidReceipt();
      const primary = placements.page.items.filter((p) => p.kind === "primary");
      if (primary.length > 1) return yield* invalidReceipt();
      const current = primary[0];
      if (current) {
        const trust = placements.trustedEnvironments.find(
          (t) => t.environmentId === request.thread.environment_id,
        );
        if (
          !trust ||
          current.authority_namespace !== trust.authorityNamespace ||
          current.store_generation !== trust.storeGeneration
        )
          return yield* invalidReceipt();
      }
      const value: WorkstreamOperationObserved = current
        ? {
            kind: "primary",
            workstream_id: current.workstream_id,
            name: (yield* gateway.readDetail(current.workstream_id)).workstream.name,
          }
        : { kind: "unassigned" };
      assignments.set(threadKey(request.thread), {
        observed: value,
        expires: current ? Date.parse(current.expires_at) : (yield* ports.now) + 60_000,
      });
      return value;
    });
  const commit = (entry: Entry, receipt: Committed) =>
    Effect.gen(function* () {
      const membership = receipt.effects.membership_ids.at(-1) ?? null;
      const workstream =
        entry.request.kind === "create_workstream"
          ? (receipt.effects.workstream_versions[0]?.workstream_id ?? null)
          : entry.request.target.kind === "workstream"
            ? entry.request.target.workstream_id
            : null;
      if (
        (entry.request.kind === "create_workstream" && workstream === null) ||
        (entry.request.kind === "set_primary" && membership === null)
      )
        return yield* invalidReceipt();
      record(entry, receipt);
      delete entry.unknown;
      // Readback is descriptive only: later owner moves cannot overwrite the committed outcome.
      const readback = yield* observed(entry).pipe(Effect.catch(() => Effect.succeed(null)));
      yield* update(entry, {
        state: "committed",
        reason: null,
        detail: null,
        poll_after_ms: null,
        observed: readback,
        result: {
          workstream_id: workstream,
          membership_id: membership,
          registry_version: receipt.registry_version,
        },
      });
    });
  const qualify = (entry: Entry, binding: T3WorkstreamBinding) =>
    Effect.gen(function* () {
      const request = entry.request;
      if (request.kind === "set_primary") {
        if (request.thread.environment_id !== ports.environmentId)
          return yield* stop(rejected("foreign_thread"));
        const exists = yield* ports
          .threadExists(request.thread.thread_id)
          .pipe(Effect.mapError(() => new StopRun({ result: unavailable("offline") })));
        if (!exists) return yield* stop(rejected("thread_not_found"));
      }
      if (ports.activation.state !== "enabled") return yield* stop(unavailable("not_configured"));
      if (ports.registryId === null) return yield* stop(unavailable("not_configured"));
      if (!binding.permissions.includes("workstreams:write"))
        return yield* stop(rejected("permission_denied"));
      const registry = request.kind === "create_workstream" ? request.registry : request.target;
      if (registry.registry_id !== ports.registryId || registry.owner_id !== binding.ownerId)
        return yield* stop(rejected("registry_mismatch"));
      const registration = yield* ports.registration.read().pipe(
        Effect.mapError(
          () =>
            new StopRun({
              result: {
                state: "unavailable",
                reason: "registration_unavailable",
                detail: { availability: "authority_unavailable" },
              },
            }),
        ),
      );
      if (registration.t3Availability.state !== "ready")
        return yield* stop({
          state: "unavailable",
          reason: "registration_unavailable",
          detail: { availability: registration.t3Availability.reason },
        });
      const context = registration.context;
      if (
        !sameContext(context, binding) ||
        context.principal_id !== binding.principalId ||
        context.authorization_revision !== binding.authorizationRevision
      )
        return yield* retry();
      const sources = context.sources.filter(
        (s) => s.provider === "t3" && s.source_instance_id === ports.environmentId,
      );
      if (context.state !== "ready" || sources.length !== 1)
        return yield* stop({
          state: "unavailable",
          reason: "registration_unavailable",
          detail: { availability: "source_mismatch" },
        });
      return sources[0]!;
    });
  const receiptFailure = (receipt: Extract<WorkstreamReceipt, { state: "rejected" }>): Failure => {
    if (receipt.error.code === "identity_conflict") return rejected("identity_conflict");
    if (receipt.operation === "verify_reference") return rejected("verification_rejected");
    return { state: "rejected", reason: "registry_rejected", detail: { code: receipt.error.code } };
  };
  const handleRejected = (receipt: Extract<WorkstreamReceipt, { state: "rejected" }>) =>
    ["version_conflict", "membership_conflict", "backpressure"].includes(receipt.error.code)
      ? retry(
          (receipt.error.retry_after_seconds ?? 0) * 1000,
          receipt.error.code === "backpressure",
        )
      : stop(receiptFailure(receipt));
  const recover = (
    entry: Entry,
    child: Child,
  ): Effect.Effect<WorkstreamReceipt | null, WorkstreamGatewayError> =>
    Effect.gen(function* () {
      const response = yield* write(entry, child).pipe(Effect.result);
      if (response._tag === "Success" && response.success.kind === "receipt") {
        const receipt = response.success.receipt;
        if (
          !validReceipt(entry, child.kind, child.command.command_id, receipt) ||
          receipt.request_sha256 !== child.digest ||
          receipt.actor.principal_id !== child.principalId ||
          receipt.server_generation !== child.command.expected_server_generation
        )
          return yield* invalidReceipt();
        record(entry, receipt);
        delete entry.unknown;
        return receipt;
      }
      const read = yield* gateway.readReceipt(child.command.command_id);
      if (read.kind === "receipt") {
        if (!validReceipt(entry, child.kind, child.command.command_id, read.receipt))
          return yield* invalidReceipt();
        record(entry, read.receipt);
        delete entry.unknown;
        return read.receipt;
      }
      // G1 observes the version BEFORE absence; reversing that order is unsafe.
      if (read.witness_version > child.command.expected_registry_version) {
        delete entry.unknown;
        return null;
      }
      return yield* new WorkstreamGatewayError({
        reason: "unknown-effect",
        detail: "Child remains unresolved.",
      });
    });
  const send = (entry: Entry, pass: Scan, kind: Kind, action: WorkstreamCommand["action"]) =>
    Effect.gen(function* () {
      if (entry.unknown) return yield* invalidReceipt();
      if (pass.absent > 9) return yield* stop(unavailable("contention"));
      const command: WorkstreamCommand = {
        command_id: entry.ids[kind][pass.absent - 1]!,
        expected_server_generation: pass.binding.serverGeneration,
        expected_registry_version: pass.binding.registryVersion,
        action,
      };
      const body = canonicalJson(command);
      const child: Child = {
        command,
        body,
        digest: yield* digest(body).pipe(Effect.mapError(() => invalidReceipt())),
        kind,
        ownerId: pass.binding.ownerId,
        principalId: pass.binding.principalId,
      };
      entry.unknown = child;
      entry.result = {
        ...entry.result,
        children: [
          ...entry.result.children.filter((c) => c.command_id !== command.command_id),
          {
            command_id: command.command_id,
            operation: action.operation,
            outcome: "unknown" as const,
          },
        ].slice(-16),
      };
      const started = yield* ports.now;
      let delay = 250;
      for (;;) {
        const answer = yield* recover(entry, child).pipe(Effect.result);
        if (answer._tag === "Success") return answer.success;
        yield* update(entry, {
          state: "reconciling",
          reason: kind === "cre" ? "create_effect_unknown" : "membership_effect_unknown",
          poll_after_ms: (yield* ports.now) - started >= 60_000 ? 30_000 : delay,
        });
        yield* ports.sleep((yield* ports.now) - started >= 60_000 ? 30_000 : delay);
        delay = Math.min(delay * 2, 4000);
      }
    });
  const pollVerification = (entry: Entry, initial: WorkstreamReceipt) =>
    Effect.gen(function* () {
      const deadline = (yield* ports.now) + 30_000;
      let receipt = initial;
      while (receipt.state === "pending" || receipt.state === "unresolved") {
        const remaining = deadline - (yield* ports.now);
        if (remaining <= 0)
          return yield* stop(
            unavailable(
              receipt.state === "pending" ? "verification_pending" : "verification_unresolved",
            ),
          );
        yield* update(entry, {
          state: "reconciling",
          reason: "verification_pending",
          poll_after_ms: Math.min(30_000, receipt.retry_after_seconds * 1000),
        });
        yield* ports.sleep(Math.min(remaining, receipt.retry_after_seconds * 1000));
        const read = yield* gateway.readReceipt(receipt.command_id);
        if (read.kind === "absent" || !validReceipt(entry, "ver", receipt.command_id, read.receipt))
          return yield* stop(unavailable("verification_unresolved"));
        receipt = read.receipt;
        record(entry, receipt);
      }
      if (receipt.state === "rejected") return yield* handleRejected(receipt);
      return receipt;
    });
  const readGroup = (id: string, binding: T3WorkstreamBinding) =>
    Effect.gen(function* () {
      const detail = yield* gateway.readDetail(id).pipe(Effect.result);
      if (detail._tag === "Success") return detail.success;
      // Legacy detail errors intentionally hide HTTP status. Only a complete, live,
      // coherent metadata listing can establish absence; an arbitrary 404 cannot.
      let cursor: string | undefined;
      const cursors = new Set<string>();
      for (let pageNumber = 0; pageNumber < 10; pageNumber++) {
        const page = yield* gateway.readMetadata({
          limit: 100,
          ...(cursor === undefined ? {} : { cursor }),
        });
        if (page.source !== "live" || page.stale) return yield* detail.failure;
        if (
          page.binding.registryVersion !== binding.registryVersion ||
          page.binding.serverGeneration !== binding.serverGeneration ||
          page.binding.ownerId !== binding.ownerId
        )
          return yield* retry();
        if (page.items.some((group) => group.workstreamId === id)) return yield* detail.failure;
        if (page.nextCursor === null) return yield* stop(rejected("workstream_not_found"));
        if (cursors.has(page.nextCursor)) break;
        cursors.add(page.nextCursor);
        cursor = page.nextCursor;
      }
      return yield* detail.failure;
    });
  const runPass = (entry: Entry) =>
    Effect.gen(function* () {
      if (
        entry.request.kind === "set_primary" &&
        entry.request.thread.environment_id !== ports.environmentId
      )
        return yield* stop(rejected("foreign_thread"));
      if (ports.activation.state !== "enabled") return yield* stop(unavailable("not_configured"));
      const pass = yield* scan(entry);
      if (pass.committed) {
        yield* commit(entry, pass.committed);
        return true;
      }
      if (pass.absent > 9) return yield* stop(unavailable("contention"));
      const source = yield* qualify(entry, pass.binding);
      const request = entry.request;
      if (request.kind === "create_workstream") {
        const receipt = yield* send(entry, pass, "cre", {
          operation: "create_workstream",
          ...request.workstream,
        });
        if (!receipt) return yield* retry();
        if (receipt.state === "rejected") return yield* handleRejected(receipt);
        if (receipt.state !== "committed") return yield* invalidReceipt();
        yield* commit(entry, receipt);
        return true;
      }
      const registered = yield* scan(entry, "reg");
      const registration = registered.committed;
      if (!registration) {
        yield* qualify(entry, registered.binding);
        const receipt = yield* send(entry, registered, "reg", {
          operation: "register_reference",
          identity: {
            provider: "t3",
            source_instance_id: request.thread.environment_id,
            native_id: request.thread.thread_id,
            resource_kind: "thread",
            id_kind: "internal",
            account_provenance: { kind: "not_account_scoped" },
          },
          pr_locator: null,
        });
        if (!receipt) return yield* retry();
        if (receipt.state === "rejected") return yield* handleRejected(receipt);
        if (receipt.state !== "committed") return yield* invalidReceipt();
        return false;
      }
      const referenceId = registration.effects.native_reference_id;
      if (!referenceId) return yield* stop(rejected("identity_conflict"));
      const detail = yield* gateway.readReference(referenceId);
      const reference = detail.reference;
      if (!sameContext(detail.context, pass.binding)) return yield* retry();
      const identity = reference.identity;
      if (
        reference.native_reference_id !== referenceId ||
        reference.owner_id !== pass.binding.ownerId ||
        identity.provider !== "t3" ||
        identity.source_instance_id !== request.thread.environment_id ||
        identity.native_id !== request.thread.thread_id ||
        identity.resource_kind !== "thread" ||
        identity.id_kind !== "internal" ||
        identity.account_provenance.kind !== "not_account_scoped" ||
        reference.pr_locator !== null
      )
        return yield* stop(rejected("identity_conflict"));
      const now = yield* ports.now;
      const registrationState = reference.registration;
      if (
        registrationState.state !== "attested" ||
        registrationState.expires_at === null ||
        Date.parse(registrationState.expires_at) < now + 60_000
      ) {
        const verification = yield* scan(entry, "ver");
        if (verification.pending) {
          yield* pollVerification(entry, verification.pending);
          return false;
        }
        yield* qualify(entry, verification.binding);
        const receipt = yield* send(entry, verification, "ver", {
          operation: "verify_reference",
          native_reference_id: referenceId,
          expected_attestation_version: reference.registration.attestation_version,
        });
        if (!receipt) return yield* retry();
        yield* pollVerification(entry, receipt);
        const verified = yield* gateway.readReference(referenceId);
        if (
          verified.reference.registration.state !== "attested" ||
          verified.reference.registration.expires_at === null ||
          Date.parse(verified.reference.registration.expires_at) < (yield* ports.now) + 60_000
        )
          return yield* stop(unavailable("verification_unresolved"));
        return false;
      }
      const evidence = registrationState.evidence;
      if (
        !evidence ||
        evidence.authority_namespace !== source.authority_namespace ||
        evidence.store_generation !== source.store_generation ||
        evidence.provider !== "t3" ||
        evidence.source_instance_id !== request.thread.environment_id ||
        evidence.native_id !== request.thread.thread_id ||
        Date.parse(registrationState.attested_at!) > now
      )
        return yield* stop(rejected("identity_conflict"));
      const placements = yield* gateway.readThreadPlacements({
        identities: [
          {
            source_instance_id: request.thread.environment_id,
            native_thread_id: request.thread.thread_id,
          },
        ],
      });
      const trust = placements.trustedEnvironments.find(
        (t) => t.environmentId === request.thread.environment_id,
      );
      if (
        placements.readiness !== "ready" ||
        !trust ||
        trust.authorityNamespace !== source.authority_namespace ||
        trust.storeGeneration !== source.store_generation
      )
        return yield* stop(unavailable("placements_not_ready"));
      if (
        !sameContext(placements.page.context, pass.binding) ||
        placements.page.context.principal_id !== pass.binding.principalId ||
        placements.page.context.authorization_revision !== pass.binding.authorizationRevision
      )
        return yield* retry();
      const primaries = placements.page.items.filter((p) => p.kind === "primary");
      if (primaries.length > 1) return yield* stop(unavailable("placements_not_ready"));
      const current = primaries[0];
      const groupIds = new Set([
        ...(current ? [current.workstream_id] : []),
        ...(request.target.kind === "workstream" ? [request.target.workstream_id] : []),
      ]);
      const groups: Workstream[] = [];
      for (const id of groupIds) {
        const group = yield* readGroup(id, pass.binding);
        if (!sameContext(group.context, pass.binding)) return yield* retry();
        if (
          group.workstream.workstream_id !== id ||
          group.workstream.owner_id !== pass.binding.ownerId
        )
          return yield* stop(rejected("workstream_not_found"));
        groups.push(group.workstream);
      }
      const history: MembershipEpisode[] = [];
      if (!current && request.target.kind === "workstream") {
        let cursor: string | undefined;
        const seen = new Set<string>();
        for (let pageNumber = 0; pageNumber < 10; pageNumber++) {
          const page = yield* gateway.readMemberships(request.target.workstream_id, {
            limit: 100,
            ...(cursor === undefined ? {} : { cursor }),
          });
          if (!sameContext(page.context, pass.binding)) return yield* retry();
          if (
            page.items.some(
              (m) =>
                m.workstream_id !==
                (request.target.kind === "workstream" ? request.target.workstream_id : null),
            )
          )
            return yield* stop(unavailable("placements_not_ready"));
          history.push(...page.items);
          if (page.next_cursor === null) break;
          if (pageNumber === 9 || seen.has(page.next_cursor))
            return yield* stop(unavailable("destination_scan_bound"));
          seen.add(page.next_cursor);
          cursor = page.next_cursor;
        }
      }
      const after = yield* gateway.readSession();
      if (
        after.registryVersion !== pass.binding.registryVersion ||
        after.serverGeneration !== pass.binding.serverGeneration ||
        after.ownerId !== pass.binding.ownerId ||
        after.principalId !== pass.binding.principalId ||
        after.authorizationRevision !== pass.binding.authorizationRevision ||
        canonicalJson(after.permissions) !== canonicalJson(pass.binding.permissions)
      )
        return yield* retry();
      const value: WorkstreamOperationObserved = current
        ? {
            kind: "primary",
            workstream_id: current.workstream_id,
            name: groups.find((g) => g.workstream_id === current.workstream_id)!.name,
          }
        : { kind: "unassigned" };
      assignments.set(threadKey(request.thread), {
        observed: value,
        expires: Date.parse(registrationState.expires_at),
      });
      yield* update(entry, { observed: value });
      const plan = yield* Effect.try({
        try: () =>
          planSetPrimary({
            request,
            registry: request.target,
            reference,
            placements: placements.page.items,
            workstreams: groups,
            destinationMemberships: history,
          }),
        catch: () => new StopRun({ result: unavailable("placements_not_ready") }),
      });
      if (plan.kind !== "action") {
        yield* update(entry, {
          state: plan.kind,
          reason: plan.kind === "conflict" ? "precondition_failed" : null,
          detail: null,
          result: null,
          poll_after_ms: null,
        });
        return true;
      }
      // The command owns a reg receipt and uses V from before the mem scan, never V′.
      const receipt = yield* send(entry, pass, "mem", plan.action);
      if (!receipt) return yield* retry();
      if (receipt.state === "rejected") return yield* handleRejected(receipt);
      if (receipt.state !== "committed") return yield* invalidReceipt();
      yield* commit(entry, receipt);
      return true;
    });
  const releaseLock = (entry: Entry) => {
    if (entry.request.kind === "set_primary") {
      const key = threadKey(entry.request.thread);
      if (locks.get(key) === entry) locks.delete(key);
    }
  };
  const closing = (entry: Entry, failure: Failure) =>
    Effect.gen(function* () {
      const closingScan = yield* scan(entry).pipe(Effect.result);
      if (closingScan._tag === "Success" && closingScan.success.committed) {
        yield* commit(entry, closingScan.success.committed);
        return true;
      }
      if (configFailure(failure) && !entry.unknown) {
        yield* update(entry, { ...failure, result: null, poll_after_ms: null });
        return true;
      }
      if (closingScan._tag === "Success" && !entry.unknown) {
        const pass = closingScan.success;
        if (!entry.regAbsent && entry.request.kind === "set_primary") {
          const reg = yield* gateway.readReceipt(entry.ids.reg[0]!).pipe(Effect.result);
          if (reg._tag === "Success" && reg.success.kind === "absent") entry.regAbsent = true;
        }
        if (
          pass.absent === 10 ||
          pass.absent > (entry.firstAbsent ?? 10) ||
          pass.binding.registryVersion > (entry.epoch ?? pass.binding.registryVersion) ||
          (entry.request.kind === "set_primary" && entry.regAbsent)
        ) {
          yield* update(entry, { ...failure, result: null, poll_after_ms: null });
          return true;
        }
      }
      entry.phase = "sealing";
      entry.failure = failure;
      releaseLock(entry);
      yield* update(entry, {
        state: "reconciling",
        reason:
          entry.request.kind === "create_workstream"
            ? "create_effect_unknown"
            : "membership_effect_unknown",
        detail: null,
        result: null,
        poll_after_ms: 30_000,
      });
      return false;
    });
  const execute = (entry: Entry) =>
    Effect.gen(function* () {
      let failure: Failure | undefined;
      let replans = 0;
      while (!failure) {
        const attempt = yield* runPass(entry).pipe(Effect.result);
        if (attempt._tag === "Success") {
          if (attempt.success) break;
          continue;
        }
        const error = attempt.failure;
        if (error._tag === "Replan") {
          if (replans++ < 3) {
            yield* ports.sleep(
              Math.max(error.delay ?? 0, 250 + Math.floor((yield* ports.jitter) * 250)),
            );
            continue;
          }
          failure = unavailable(error.backpressure ? "backpressure" : "contention");
        } else if (error._tag === "StopRun") failure = error.result as Failure;
        else
          failure = unavailable(error.reason === "not-configured" ? "not_configured" : "offline");
      }
      if (failure) {
        while (!(yield* closing(entry, failure).pipe(Effect.catch(() => Effect.succeed(false))))) {
          yield* Effect.raceFirst(ports.sleep(30_000), Deferred.await(entry.wake));
          entry.wake = yield* Deferred.make<void>();
        }
      }
      entry.phase = "finished";
      entry.finishedAt = yield* ports.now;
      releaseLock(entry);
      yield* Deferred.succeed(entry.done, undefined);
    });
  const prune = (now: number) => {
    for (const [key, value] of assignments) if (value.expires <= now) assignments.delete(key);
    while (assignments.size > 1000) assignments.delete(assignments.keys().next().value!);
    const finished = [...entries.values()]
      .filter((e) => e.phase === "finished")
      .sort((a, b) => b.finishedAt! - a.finishedAt!);
    for (const [index, entry] of finished.entries())
      if (index >= 1000 || now - entry.finishedAt! >= 86_400_000)
        entries.delete(entry.request.operation_id);
  };
  const admit = (input: WorkstreamOperationSubmitRequest) =>
    Effect.gen(function* () {
      const hash = yield* digest(canonicalJson(input.request)).pipe(
        Effect.mapError(() => new OperationInternalError({})),
      );
      const now = yield* ports.now;
      prune(now);
      let entry = entries.get(input.request.operation_id);
      if (entry && entry.digest !== hash)
        return yield* new OperationConflict({ code: "operation_id_conflict" });
      if (entry && settled(entry.result)) return entry.result;
      if (!entry || entry.phase === "finished") {
        if (input.request.kind === "set_primary") {
          const locked = locks.get(threadKey(input.request.thread));
          if (locked)
            return yield* new OperationConflict({
              code: "thread_operation_in_progress",
              open_operation_id: locked.request.operation_id,
            });
        }
        if (!entry) {
          const ids: Record<Kind, string[]> = { reg: [], ver: [], mem: [], cre: [] };
          for (const kind of ["reg", "ver", "mem", "cre"] as const)
            for (let attempt = 1; attempt <= 9; attempt++) {
              ids[kind].push(
                yield* workstreamChildId({
                  environment_id: ports.environmentId,
                  operation_id: input.request.operation_id,
                  request_sha256: hash,
                  kind,
                  attempt,
                }).pipe(
                  Effect.provideService(Crypto.Crypto, crypto),
                  Effect.mapError(() => new OperationInternalError({})),
                ),
              );
            }
          // Hashing yields: recheck ownership before publishing the entry and lock together.
          const raced = entries.get(input.request.operation_id);
          if (raced) {
            if (raced.digest !== hash)
              return yield* new OperationConflict({ code: "operation_id_conflict" });
            entry = raced;
          } else {
            if (input.request.kind === "set_primary") {
              const locked = locks.get(threadKey(input.request.thread));
              if (locked)
                return yield* new OperationConflict({
                  code: "thread_operation_in_progress",
                  open_operation_id: locked.request.operation_id,
                });
            }
            const at = DateTime.formatIso(DateTime.makeUnsafe(now));
            entry = {
              request: input.request,
              digest: hash,
              ids,
              phase: "finished",
              regAbsent: false,
              done: yield* Deferred.make<void>(),
              wake: yield* Deferred.make<void>(),
              result: {
                contract: WORKSTREAM_OPERATIONS_CONTRACT,
                operation_id: input.request.operation_id,
                kind: input.request.kind,
                origin: input.request.origin,
                request_sha256: hash,
                state: "in_progress",
                reason: null,
                detail: null,
                observed: null,
                result: null,
                children: [],
                accepted_at: at,
                updated_at: at,
                poll_after_ms: 250,
              },
            };
            entries.set(input.request.operation_id, entry);
          }
        }
        if (entry.phase === "finished") {
          entry.done = yield* Deferred.make<void>();
          entry.phase = "running";
          delete entry.finishedAt;
          delete entry.failure;
          if (entry.request.kind === "set_primary")
            locks.set(threadKey(entry.request.thread), entry);
          yield* update(entry, {
            state: "in_progress",
            reason: null,
            detail: null,
            result: null,
            poll_after_ms: 250,
          });
          yield* execute(entry).pipe(Effect.forkIn(scope));
        }
      } else if (entry.phase === "sealing") yield* Deferred.succeed(entry.wake, undefined);
      return entry;
    }).pipe(Semaphore.withPermit(admission));
  const submit = (input: WorkstreamOperationSubmitRequest) =>
    Effect.gen(function* () {
      const admitted = yield* admit(input);
      if (!("phase" in admitted)) return admitted;
      if (input.wait_ms > 0 && admitted.phase !== "finished")
        yield* Effect.raceFirst(Deferred.await(admitted.done), ports.sleep(input.wait_ms));
      return admitted.result;
    });
  const read = (
    input: WorkstreamOperationReadRequest,
  ): Effect.Effect<WorkstreamOperationReadResponse> =>
    Effect.gen(function* () {
      const now = yield* ports.now;
      prune(now);
      if ("operation_id" in input)
        return { operation: entries.get(input.operation_id)?.result ?? null };
      if ("open" in input)
        return {
          operations: [...entries.values()]
            .filter((e) => e.phase !== "finished")
            .slice(0, 100)
            .map((e) => e.result),
        };
      const key = threadKey(input.thread);
      const cached = assignments.get(key);
      const open =
        locks.get(key) ??
        [...entries.values()]
          .toReversed()
          .find(
            (e) =>
              e.phase !== "finished" &&
              e.request.kind === "set_primary" &&
              threadKey(e.request.thread) === key,
          );
      return {
        assignment: cached && cached.expires > now ? cached.observed : { kind: "unverified" },
        open_operation: open?.result ?? null,
      };
    });
  let setupCache: { at: number; value: WorkstreamSetupStatus } | undefined;
  const setupStatus = () =>
    Effect.gen(function* () {
      const now = yield* ports.now;
      if (setupCache && now - setupCache.at < 10_000)
        return {
          ...setupCache.value,
          operations: { open: [...entries.values()].filter((e) => e.phase !== "finished").length },
        };
      const binding = yield* gateway.readSession().pipe(Effect.result);
      const registration = yield* ports.registration.read().pipe(Effect.result);
      const placements = yield* gateway
        .readThreadPlacements({ identities: [] })
        .pipe(Effect.result);
      const source: WorkstreamSetupStatus["t3_source"] =
        registration._tag === "Success" && registration.success.t3Availability.state === "ready"
          ? { state: "ready", reason: null }
          : {
              state: "unavailable",
              reason:
                registration._tag === "Success" &&
                registration.success.t3Availability.state === "unavailable"
                  ? registration.success.t3Availability.reason
                  : "authority_unavailable",
            };
      const value: WorkstreamSetupStatus = {
        contract: WORKSTREAM_OPERATIONS_CONTRACT,
        environment_id: ports.environmentId,
        observed_at: DateTime.formatIso(DateTime.makeUnsafe(now)),
        authority_effect: "none",
        activation: ports.activation,
        registry:
          binding._tag === "Success" && ports.registryId !== null
            ? {
                state: "ready",
                registry_id: ports.registryId!,
                owner_id: binding.success.ownerId,
                can_write: binding.success.permissions.includes("workstreams:write"),
              }
            : {
                state: ports.activation.state === "enabled" ? "unavailable" : "not_configured",
                registry_id: null,
                owner_id: null,
                can_write: false,
              },
        t3_source: source,
        placements: {
          readiness: placements._tag === "Success" ? placements.success.readiness : "unavailable",
        },
        operations: { open: [...entries.values()].filter((e) => e.phase !== "finished").length },
        next_steps: [
          ...(ports.activation.state !== "enabled" || ports.registryId === null
            ? [
                {
                  code: "configure_activation" as const,
                  doc: "docs/operations/workstreams-activation.md" as const,
                },
              ]
            : []),
          ...(source.state !== "ready"
            ? [
                {
                  code: "enroll_native_source" as const,
                  doc: "docs/operations/workstreams-native-enrollment.md" as const,
                },
              ]
            : []),
          ...(placements._tag === "Failure" || placements.success.readiness !== "ready"
            ? [
                {
                  code: "verify_reference" as const,
                  doc: "docs/operations/workstreams-verification.md" as const,
                },
              ]
            : []),
        ],
      };
      setupCache = { at: now, value };
      return value;
    });
  return WorkstreamOperations.of({ submit, read, setupStatus });
});

export const layer = Layer.effect(WorkstreamOperations, make);
