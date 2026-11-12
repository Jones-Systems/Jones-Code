import { WorkstreamReceipt, type T3WorkstreamBinding } from "@t3tools/contracts";
import type { WorkstreamsNativeReceiptLookup } from "@t3tools/contracts/jones/workstreamsNative";
import type {
  WorkstreamReceiptRecovery,
  WorkstreamRecoverySubmitResult,
} from "../../../../workstreams/WorkstreamGateway.ts";
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Schema from "effect/Schema";
import * as Option from "effect/Option";
import * as Callers from "../runtime/recordedCaller.ts";
import { Checkpoint } from "./durableEntries.ts";
import { WorkstreamGatewayError } from "../../../../workstreams/WorkstreamGateway.ts";
import * as BindingStore from "../store/bindingStore.ts";
import type { Operation } from "../store/operationStore.ts";

export class HomeTransport extends Context.Service<
  HomeTransport,
  {
    readonly lookup: (
      destination: Operation["destination"],
      commandId: string,
    ) => Effect.Effect<WorkstreamsNativeReceiptLookup, WorkstreamGatewayError>;
    readonly resubmit: (
      destination: Operation["destination"],
      commandId: string,
      body: string,
    ) => Effect.Effect<WorkstreamRecoverySubmitResult, WorkstreamGatewayError>;
  }
>()("t3/jones/workstreams/native/operations/homeExecutor/HomeTransport") {}

export class HomeExecutor extends Context.Service<
  HomeExecutor,
  {
    readonly assertSession: (
      operation: Operation,
      session: T3WorkstreamBinding,
    ) => Effect.Effect<void, WorkstreamGatewayError>;
    readonly readReceipt: (
      operation: Operation,
      commandId: string,
    ) => Effect.Effect<WorkstreamReceiptRecovery, WorkstreamGatewayError>;
    readonly dispatch: (
      operation: Operation,
      commandId: string,
      body: string,
    ) => Effect.Effect<WorkstreamRecoverySubmitResult, WorkstreamGatewayError>;
  }
>()("t3/jones/workstreams/native/operations/homeExecutor") {}

const isNativeReceipt = Schema.is(WorkstreamReceipt);
const decodeCheckpoint = Schema.decodeUnknownEffect(Schema.fromJsonString(Checkpoint));

const make = Effect.gen(function* () {
  const bindings = yield* BindingStore.BindingStore;
  const transport = yield* HomeTransport;
  const beforeDispatch = Effect.fn("HomeExecutor.beforeDispatch")(function* (operation: Operation) {
    if ("member_forward" in operation)
      return yield* new WorkstreamGatewayError({
        reason: "unknown-effect",
        detail: "Member forwarding intent requires its source-owned recovery executor.",
      });
    const callers = yield* Effect.serviceOption(Callers.RecordedCallers);
    if (Option.isSome(callers) || operation.recorded_caller) {
      if (Option.isNone(callers) || !operation.recorded_caller)
        return yield* new WorkstreamGatewayError({
          reason: "unknown-effect",
          detail: "Recorded admission is unavailable.",
        });
      const record = operation.recorded_caller;
      yield* Effect.gen(function* () {
        const caller = yield* callers.value.readmitRecorded(record);
        yield* caller.check("workstreams:write", operation.thread_id);
      }).pipe(
        Effect.mapError(
          () =>
            new WorkstreamGatewayError({
              reason: "unknown-effect",
              detail: "Recorded admission is fenced.",
            }),
        ),
      );
    }
    yield* bindings.assertDestination(operation.destination).pipe(
      Effect.mapError(
        () =>
          new WorkstreamGatewayError({
            reason: "unknown-effect",
            detail: "Operation destination is no longer current.",
          }),
      ),
    );
  });
  const assertSession = Effect.fn("HomeExecutor.assertSession")(function* (
    operation: Operation,
    session: T3WorkstreamBinding,
  ) {
    yield* beforeDispatch(operation);
    const destination = operation.destination;
    if (
      session.ownerId !== destination.owner_id ||
      session.principalId !== destination.principal_id ||
      session.serverGeneration !== destination.home_generation
    )
      return yield* new WorkstreamGatewayError({
        reason: "unknown-effect",
        detail: "Operation session does not match the accepted destination.",
      });
  });
  const readReceipt = Effect.fn("HomeExecutor.readReceipt")(function* (
    operation: Operation,
    commandId: string,
  ) {
    yield* beforeDispatch(operation);
    if (!operation.child_command_ids.includes(commandId))
      return yield* new WorkstreamGatewayError({
        reason: "unknown-effect",
        detail: "Child is not owned by the operation.",
      });
    const result = yield* transport.lookup(operation.destination, commandId);
    yield* beforeDispatch(operation);
    if (result.kind === "present") {
      const receipt = result.receipt;
      if (
        !isNativeReceipt(receipt) ||
        receipt.command_id !== commandId ||
        receipt.owner_id !== operation.destination.owner_id ||
        receipt.actor.principal_id !== operation.destination.principal_id ||
        receipt.server_generation !== operation.destination.home_generation
      )
        return yield* new WorkstreamGatewayError({
          reason: "unknown-effect",
          detail: "Receipt destination mismatch.",
        });
      return {
        kind: "receipt" as const,
        witness_version: receipt.state === "committed" ? receipt.registry_version : 0,
        receipt,
      };
    }
    if (
      result.kind !== "proven_absent" ||
      result.witness.server_generation !== operation.destination.home_generation ||
      result.witness.archive !== "complete"
    )
      return yield* new WorkstreamGatewayError({
        reason: "unknown-effect",
        detail: "Operation absence is not proven at its accepted destination.",
      });
    return { kind: "absent" as const, witness_version: result.witness.registry_version };
  });
  const dispatch = Effect.fn("HomeExecutor.dispatch")(function* (
    operation: Operation,
    commandId: string,
    body: string,
  ) {
    yield* beforeDispatch(operation);
    const saved =
      operation.result_body === null
        ? null
        : yield* decodeCheckpoint(operation.result_body).pipe(
            Effect.mapError(
              () =>
                new WorkstreamGatewayError({
                  reason: "unknown-effect",
                  detail: "Child checkpoint is unavailable.",
                }),
            ),
          );
    if (
      !saved?.unknown ||
      saved.unknown.command.command_id !== commandId ||
      saved.unknown.body !== body ||
      saved.unknown.command.expected_server_generation !== operation.destination.home_generation ||
      saved.unknown.principalId !== operation.destination.principal_id ||
      saved.unknown.ownerId !== operation.destination.owner_id ||
      !["dispatching", "in_progress", "reconciling"].includes(operation.state) ||
      !operation.child_command_ids.includes(commandId)
    )
      return yield* new WorkstreamGatewayError({
        reason: "unknown-effect",
        detail: "Child intent is not accepted.",
      });
    const prior = yield* readReceipt(operation, commandId);
    if (prior.kind === "receipt") {
      if (
        prior.receipt.request_sha256 !== saved.unknown.digest ||
        prior.receipt.operation !== saved.unknown.command.action.operation
      )
        return { kind: "idempotency_conflict" as const };
      return { kind: "receipt" as const, receipt: prior.receipt };
    }
    yield* beforeDispatch(operation);
    return yield* transport.resubmit(operation.destination, commandId, body);
  });
  return HomeExecutor.of({ assertSession, readReceipt, dispatch });
});

export const layer = Layer.effect(HomeExecutor, make);
