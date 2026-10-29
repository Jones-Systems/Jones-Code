// @effect-diagnostics nodeBuiltinImport:off -- Stable IDs and RAW intent digests survive process loss.
import * as NodeCrypto from "node:crypto";
import { WorkstreamCommand, WorkstreamAppearanceWrite } from "@t3tools/contracts";
import { WorkstreamOperationRequest } from "@t3tools/contracts/jones/workstreamOperations";
import * as Wire from "@t3tools/contracts/jones/workstreamsForwarding";
import type { WorkstreamsNativeSuppliedAttestation } from "@t3tools/contracts/jones/workstreamsNative";
import * as DateTime from "effect/DateTime";
import * as Crypto from "effect/Crypto";
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";
import * as Semaphore from "effect/Semaphore";
import { canonicalJson } from "../../operations/encoding.ts";
import { workstreamChildId } from "../../operations/childIds.ts";
import { RegistryError } from "../registry/session.ts";
import * as Admission from "../runtime/memberAdmission.ts";
import type { MemberWriteTarget } from "../runtime/memberRecordedCaller.ts";
import * as Store from "../store/operationStore.ts";
import * as Home from "./homeClient.ts";

export interface ForwardSubmission {
  readonly kind: "command" | "operation" | "appearance";
  readonly body: string;
  readonly parent_operation_id?: string | null;
  readonly evidence?: typeof WorkstreamsNativeSuppliedAttestation.Type;
}
export interface ForwardView {
  readonly operation_id: string;
  readonly state: Store.OperationState;
  readonly next_poll_at: string | null;
  readonly accepted_at: string;
  readonly updated_at: string;
  readonly result: Wire.WorkstreamsForwardResult | null;
}
/** Parent supplies source-owned reference/membership-to-thread resolution, never client target assertions. */
export class MemberCommandTargets extends Context.Service<
  MemberCommandTargets,
  {
    readonly resolve: (
      command: WorkstreamCommand,
    ) => Effect.Effect<MemberWriteTarget, RegistryError>;
  }
>()("t3/jones/workstreams/native/forwarding/forwarder/MemberCommandTargets") {}

/** Share once across request layers. Durable CAS additionally prevents two owners dispatching accepted intent. */
export class MemberForwardLocks extends Context.Service<
  MemberForwardLocks,
  {
    readonly run: <A, E, R>(id: string, effect: Effect.Effect<A, E, R>) => Effect.Effect<A, E, R>;
  }
>()("t3/jones/workstreams/native/forwarding/forwarder/MemberForwardLocks") {}
export const locksLayer = Layer.effect(
  MemberForwardLocks,
  Effect.sync(() => {
    const locks = new Map<string, { semaphore: Semaphore.Semaphore; users: number }>();
    return MemberForwardLocks.of({
      run: (id, effect) =>
        Effect.suspend(() => {
          let lock = locks.get(id);
          if (!lock) {
            lock = { semaphore: Semaphore.makeUnsafe(1), users: 0 };
            locks.set(id, lock);
          }
          lock.users++;
          const entry = lock;
          return entry.semaphore
            .withPermits(1)(effect)
            .pipe(
              Effect.ensuring(
                Effect.sync(() => {
                  entry.users--;
                  if (entry.users === 0) locks.delete(id);
                }),
              ),
            );
        }),
    });
  }),
);

export class MemberForwarder extends Context.Service<
  MemberForwarder,
  {
    readonly submit: (input: ForwardSubmission) => Effect.Effect<ForwardView, RegistryError>;
    readonly read: (id: string) => Effect.Effect<ForwardView | null, RegistryError>;
    readonly open: () => Effect.Effect<readonly ForwardView[], RegistryError>;
    readonly recover: (id: string) => Effect.Effect<ForwardView | null, RegistryError>;
    readonly recoverOpen: () => Effect.Effect<readonly ForwardView[], RegistryError>;
  }
>()("t3/jones/workstreams/native/forwarding/forwarder/MemberForwarder") {}

const sha = (body: string) => NodeCrypto.createHash("sha256").update(body).digest("hex");
const same = (a: unknown, b: unknown) => canonicalJson(a) === canonicalJson(b);
const fail = (code: string) => new RegistryError({ code });
const unavailable = () => fail("unavailable");
const encodeForward = Schema.encodeEffect(Schema.fromJsonString(Wire.WorkstreamsForwardRequest));
const decodeId = Schema.decodeUnknownEffect(Wire.WorkstreamsForwardCommandId);
const decodeForward = Schema.decodeUnknownEffect(Wire.WorkstreamsForwardRequest);
const decodeResultBody = Schema.decodeUnknownEffect(
  Schema.fromJsonString(Wire.WorkstreamsForwardResult),
);
const decodeCommand = Schema.decodeUnknownEffect(Schema.fromJsonString(WorkstreamCommand));
const decodeOperation = Schema.decodeUnknownEffect(
  Schema.fromJsonString(WorkstreamOperationRequest),
);
const decodeAppearance = Schema.decodeUnknownEffect(
  Schema.fromJsonString(WorkstreamAppearanceWrite),
);
const parse = Effect.fn("MemberForwarder.parse")(function* (input: ForwardSubmission) {
  if (
    Object.keys(input).some(
      (k) => !["kind", "body", "parent_operation_id", "evidence"].includes(k),
    ) ||
    typeof input.body !== "string" ||
    new TextEncoder().encode(input.body).length > 1_048_576
  )
    return yield* fail("invalid_request");
  if (input.kind === "command") {
    const request = yield* decodeCommand(input.body, { onExcessProperty: "error" }).pipe(
      Effect.mapError(() => fail("invalid_request")),
    );
    return { kind: input.kind, request, id: request.command_id } as const;
  }
  if (input.kind === "operation") {
    const request = yield* decodeOperation(input.body, { onExcessProperty: "error" }).pipe(
      Effect.mapError(() => fail("invalid_request")),
    );
    return { kind: input.kind, request, id: request.operation_id } as const;
  }
  if (input.kind === "appearance") {
    const request = yield* decodeAppearance(input.body, { onExcessProperty: "error" }).pipe(
      Effect.mapError(() => fail("invalid_request")),
    );
    return { kind: input.kind, request, id: request.command_id } as const;
  }
  return yield* fail("invalid_request");
});
const terminal = (state: Store.OperationState) =>
  !["accepted", "dispatching", "in_progress", "reconciling"].includes(state);
const resultState = (result: Wire.WorkstreamsForwardResult): Store.OperationState => {
  if (result.kind === "appearance") return "committed";
  return result.result.state === "pending"
    ? "in_progress"
    : result.result.state === "unresolved" || result.result.state === "unavailable"
      ? "reconciling"
      : result.result.state;
};
const stamp = DateTime.now.pipe(Effect.map(DateTime.formatIso));
const later = DateTime.now.pipe(
  Effect.map((now) => DateTime.formatIso(DateTime.addDuration(now, "30 seconds"))),
);
const retained = (row: Store.MemberForwardOperation) =>
  Effect.try({
    try: () => Wire.decodeWorkstreamsForwardBody(row.member_forward.forward_request_body),
    catch: unavailable,
  });
const storeError = (cause: unknown) => {
  const reason =
    typeof cause === "object" && cause !== null && "reason" in cause ? cause.reason : null;
  return fail(reason === "idempotency_conflict" ? "operation_id_conflict" : "unavailable");
};
const make = Effect.gen(function* () {
  const store = yield* Store.OperationStore,
    home = yield* Home.HomeForwardClient;
  const crypto = yield* Crypto.Crypto;
  const admission = yield* Admission.MemberAdmission,
    locks = yield* MemberForwardLocks;
  const targets = yield* Effect.serviceOption(MemberCommandTargets);
  const authorize = (row: Store.MemberForwardOperation, recovery: boolean) =>
    recovery
      ? admission.readmitWriteRetained(row.member_forward.recorded_caller)
      : admission.readmitWrite(row.member_forward.recorded_caller);
  const view = Effect.fn("MemberForwarder.view")(function* (
    row: Store.MemberForwardOperation,
    admitted: Admission.AdmittedMemberWrite,
  ) {
    const request = yield* retained(row);
    const result =
      row.result_body === null
        ? null
        : yield* decodeResultBody(row.result_body).pipe(
            Effect.mapError(() => fail("invalid_response")),
            Effect.flatMap((value) => Home.validateForwardResult(request, value, admitted)),
          );
    yield* admitted.checkCurrent();
    return {
      operation_id: row.operation_id,
      state: row.state,
      next_poll_at: row.next_poll_at,
      accepted_at: row.accepted_at,
      updated_at: row.updated_at,
      result,
    } satisfies ForwardView;
  });
  const replay = Effect.fn("MemberForwarder.replay")(function* (
    row: Store.MemberForwardOperation,
    admitted: Admission.AdmittedMemberWrite,
  ) {
    const request = yield* retained(row);
    const response = yield* home.readback(request, admitted);
    if (response.state !== "present" || row.result_body === null) return yield* fail("unavailable");
    const result = yield* Home.validateForwardResult(request, response.result, admitted);
    if (canonicalJson(result) !== row.result_body) return yield* fail("invalid_response");
    return yield* view(row, admitted);
  });
  const reconcile = Effect.fn("MemberForwarder.reconcile")(function* (
    row: Store.MemberForwardOperation,
    admitted: Admission.AdmittedMemberWrite,
  ) {
    const request = yield* retained(row);
    yield* admitted.checkCurrent();
    const outcome = yield* Effect.exit(home.readback(request, admitted));
    // Released SQLite guards forbid accepted self-transitions. Retain the known pre-dispatch fence.
    if (row.state === "accepted") {
      yield* admitted.checkCurrent();
      return yield* view(row, admitted);
    }
    if (outcome._tag === "Success" && outcome.value.state === "present") {
      const result = yield* Home.validateForwardResult(request, outcome.value.result, admitted);
      yield* admitted.checkCurrent();
      const state = resultState(result);
      const saved = yield* store
        .advanceMemberForward(
          row.operation_id,
          row.state,
          state,
          yield* stamp,
          terminal(state) ? null : yield* later,
          canonicalJson(result),
        )
        .pipe(Effect.mapError(storeError));
      return yield* view(saved, admitted);
    }
    // Unknown/not_found never proves absence. A crash after dispatch remains fenced indefinitely.
    const saved = yield* store
      .advanceMemberForward(row.operation_id, row.state, "reconciling", yield* stamp, yield* later)
      .pipe(Effect.mapError(storeError));
    yield* admitted.checkCurrent();
    return yield* view(saved, admitted);
  });
  const recover = Effect.fn("MemberForwarder.recover")((id: string) =>
    locks.run(
      id,
      Effect.gen(function* () {
        const row = yield* store.readMemberForward(id).pipe(Effect.mapError(storeError));
        if (!row) return null;
        const admitted = yield* authorize(row, true);
        return terminal(row.state) ? yield* replay(row, admitted) : yield* reconcile(row, admitted);
      }),
    ),
  );
  const submit = Effect.fn("MemberForwarder.submit")(function* (input: ForwardSubmission) {
    const decoded = yield* parse(input);
    yield* decodeId(decoded.id).pipe(Effect.mapError(() => fail("invalid_request")));
    return yield* locks.run(
      decoded.id,
      Effect.gen(function* () {
        const old = yield* store.readMemberForward(decoded.id).pipe(Effect.mapError(storeError));
        if (old) {
          const request = yield* retained(old);
          const evidence = request.kind === "appearance" ? undefined : request.request.evidence;
          if (
            old.request_body !== input.body ||
            request.kind !== input.kind ||
            request.identity.parent_operation_id !== (input.parent_operation_id ?? null) ||
            (input.evidence !== undefined && !same(evidence, input.evidence))
          )
            return yield* fail("operation_id_conflict");
          // Reauthorize the exact retained caller; a new invocation/run may not replace it.
          const admitted = yield* authorize(old, false);
          return terminal(old.state)
            ? yield* replay(old, admitted)
            : yield* reconcile(old, admitted);
        }
        const target: MemberWriteTarget =
          decoded.kind === "operation" && decoded.request.kind === "set_primary"
            ? { kind: "thread", ...decoded.request.thread }
            : decoded.kind === "command"
              ? Option.isSome(targets)
                ? yield* targets.value.resolve(decoded.request)
                : yield* fail("target_unavailable")
              : { kind: "metadata" };
        const caller = yield* admission.captureWrite(target);
        const admitted = yield* admission.readmitWrite(caller);
        const a = caller.association,
          digest = sha(input.body);
        if (!same(caller.local.target, target)) return yield* fail("forbidden");
        const origin = decoded.kind === "operation" ? decoded.request.origin : caller.local.origin;
        if (!same(origin, caller.local.origin)) return yield* fail("forbidden");
        if (decoded.kind === "operation") {
          const registry =
            decoded.request.kind === "create_workstream"
              ? decoded.request.registry
              : decoded.request.target;
          if (
            registry.registry_id !== a.home.registry_instance_id ||
            registry.owner_id !== a.home.owner_id
          )
            return yield* fail("registry_mismatch");
        }
        const childKinds =
          decoded.kind === "operation"
            ? decoded.request.kind === "create_workstream"
              ? ["cre" as const]
              : ["reg" as const, "ver" as const, "mem" as const]
            : [];
        const children: string[] = [];
        for (const kind of childKinds) {
          for (let attempt = 1; attempt <= (kind === "mem" ? 9 : 1); attempt++)
            children.push(
              yield* workstreamChildId({
                environment_id: a.source.environment_id,
                operation_id: decoded.id,
                request_sha256: digest,
                kind,
                attempt,
              }).pipe(Effect.provideService(Crypto.Crypto, crypto), Effect.mapError(unavailable)),
            );
        }
        if (decoded.kind !== "operation") children.push(decoded.id);
        const identity: Wire.WorkstreamsForwardIdentity = {
          forward_id: "jwf-" + sha(a.source.environment_id + "\0" + decoded.id).slice(0, 48),
          source_operation_id: decoded.id,
          parent_operation_id: input.parent_operation_id ?? null,
          command_ids: children,
          request_sha256: digest,
          recorded_caller_sha256: sha(canonicalJson(caller)),
          expected_home: a.home,
          canonical_origin: a.canonical_origin,
          source_authority: a.source,
          source_enrollment_id: a.enrollment_id,
          source_enrollment_session_id: a.enrollment_session_id,
          principal_id: a.principal_id,
          singleton_binding_version: a.singleton_binding_version,
          source_binding_version: a.source_binding_version,
          member_pin_id: a.member_pin_id,
          member_pin_bound_revision: a.pin_bound_revision,
          outbound_link_generation: a.link_generation,
        };
        const common = {
          expected_home: a.home,
          relay: {
            source_environment_id: a.source.environment_id,
            origin,
            linked_origin_environment_id: null,
            hop: 1 as const,
          },
        };
        const evidence = input.evidence === undefined ? {} : { evidence: input.evidence };
        if (decoded.kind === "appearance" && input.evidence !== undefined)
          return yield* fail("invalid_request");
        const envelope = {
          contract: Wire.WORKSTREAMS_NATIVE_FORWARDING_CONTRACT,
          identity,
          ...(decoded.kind === "command"
            ? {
                kind: "command",
                request: { ...common, ...evidence, body: input.body, body_sha256: digest },
              }
            : decoded.kind === "operation"
              ? {
                  kind: "operation",
                  request: {
                    ...common,
                    ...evidence,
                    request: decoded.request,
                    request_sha256: digest,
                  },
                  request_body: input.body,
                }
              : {
                  kind: "appearance",
                  request: {
                    ...common,
                    request: decoded.request,
                    request_body: input.body,
                    request_sha256: digest,
                  },
                }),
        };
        const request = yield* decodeForward(envelope, { onExcessProperty: "error" }).pipe(
          Effect.mapError(() => fail("invalid_request")),
        );
        yield* home.preflight(request, admitted);
        yield* admitted.checkCurrent();
        const body = yield* encodeForward(request).pipe(
          Effect.mapError(() => fail("invalid_request")),
        );
        const accepted = yield* store
          .acceptMemberForward({
            operation_id: decoded.id,
            kind: decoded.kind === "operation" ? decoded.request.kind : decoded.kind,
            thread_environment_id: target.kind === "thread" ? target.environment_id : null,
            thread_id: target.kind === "thread" ? target.thread_id : null,
            origin,
            destination: {
              registry_instance_id: a.home.registry_instance_id,
              owner_id: a.home.owner_id,
              home_environment_id: a.home.home_environment_id,
              home_generation: a.home.server_generation,
              principal_id: a.principal_id,
              binding_version: a.singleton_binding_version,
            },
            child_command_ids: children,
            request_body: input.body,
            accepted_at: yield* stamp,
            member_forward: {
              schema: "jones-workstreams-member-forward-accepted/1",
              kind: "MEMBER-FORWARD",
              forward_request_body: body,
              forward_request_sha256: sha(body),
              recorded_caller: caller,
            },
          })
          .pipe(Effect.mapError(storeError));
        yield* admitted.checkCurrent();
        // This is the sole dispatch boundary. Interruption or crash after it can only take the readback path.
        const dispatching = yield* store
          .advanceMemberForward(
            accepted.operation_id,
            "accepted",
            "dispatching",
            yield* stamp,
            yield* later,
          )
          .pipe(Effect.mapError(storeError));
        const response = yield* Effect.exit(home.forward(request, admitted));
        if (response._tag === "Failure") {
          const saved = yield* store
            .advanceMemberForward(
              dispatching.operation_id,
              "dispatching",
              "reconciling",
              yield* stamp,
              yield* later,
            )
            .pipe(Effect.mapError(storeError));
          yield* admitted.checkCurrent();
          return yield* view(saved, admitted);
        }
        const result = yield* Home.validateForwardResult(request, response.value, admitted);
        yield* admitted.checkCurrent();
        const state = resultState(result);
        const saved = yield* store
          .advanceMemberForward(
            dispatching.operation_id,
            "dispatching",
            state,
            yield* stamp,
            terminal(state) ? null : yield* later,
            canonicalJson(result),
          )
          .pipe(Effect.mapError(storeError));
        return yield* view(saved, admitted);
      }),
    );
  });
  const read = Effect.fn("MemberForwarder.read")(function* (id: string) {
    const row = yield* store.readMemberForward(id).pipe(Effect.mapError(storeError));
    if (!row) return null;
    const admitted = yield* authorize(row, false);
    return terminal(row.state) ? yield* replay(row, admitted) : yield* view(row, admitted);
  });
  const open = Effect.fn("MemberForwarder.open")(function* () {
    const rows = yield* store.openMemberForwards().pipe(Effect.mapError(storeError));
    return yield* Effect.forEach(rows, (row) =>
      authorize(row, false).pipe(Effect.flatMap((a) => view(row, a))),
    );
  });
  return MemberForwarder.of({
    submit,
    read,
    open,
    recover,
    recoverOpen: Effect.fn("MemberForwarder.recoverOpen")(function* () {
      const rows = yield* store.openMemberForwards().pipe(Effect.mapError(storeError));
      const views: ForwardView[] = [];
      for (const row of rows) {
        const result = yield* Effect.exit(recover(row.operation_id));
        if (result._tag === "Success" && result.value) views.push(result.value);
      }
      return views;
    }),
  });
});
/** Request-scoped admission/client; root shares locks and owns scheduling and mounts. */
export const layer = Layer.effect(MemberForwarder, make);
