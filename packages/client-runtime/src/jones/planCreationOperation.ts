import {
  CommandId,
  EnvironmentId,
  ModelSelection,
  ORCHESTRATION_V2_WS_METHODS,
  PlanId,
  ProjectId,
  ProviderInteractionMode,
  RuntimeMode,
  ThreadId,
} from "@t3tools/contracts";
import * as Schema from "effect/Schema";

import type { CreateThreadInput, PreparedThreadTurn } from "../operations/commands.ts";
import { freezePreparedTurnRequest } from "../operations/jonesPreparedTurn.ts";
import { readDispatchDeliveryEvidence } from "./deliveryEvidence.ts";
import { PreparedThreadTurnJson, encodePreparedThreadTurn } from "./preparedThreadTurn.ts";

export interface PlanCreationSlot {
  readonly environmentId: EnvironmentId;
  readonly sourceThreadId: ThreadId;
  readonly sourcePlanId: PlanId;
  readonly action: "implement-in-new-thread";
}

export const planCreationSlotKey = (slot: PlanCreationSlot) =>
  JSON.stringify([slot.environmentId, slot.sourceThreadId, slot.sourcePlanId, slot.action]);

const Envelope = Schema.Struct({
  schemaVersion: Schema.Literal(1),
  environmentId: EnvironmentId,
  sourceThreadId: ThreadId,
  sourcePlanId: PlanId,
  action: Schema.Literal("implement-in-new-thread"),
  phase: Schema.Literals([
    "create-pending",
    "create-rejected",
    "message-pending",
    "message-rejected",
    "accepted",
  ]),
  create: Schema.Struct({
    commandId: CommandId,
    threadId: ThreadId,
    projectId: ProjectId,
    title: Schema.String,
    modelSelection: ModelSelection,
    runtimeMode: RuntimeMode,
    interactionMode: ProviderInteractionMode,
    branch: Schema.NullOr(Schema.String),
    worktreePath: Schema.NullOr(Schema.String),
    createdAt: Schema.String,
  }),
  prepared: Schema.Unknown,
});

export interface PlanCreationOperation extends PlanCreationSlot {
  readonly schemaVersion: 1;
  readonly phase: typeof Envelope.Type.phase;
  readonly create: CreateThreadInput & {
    readonly commandId: CommandId;
    readonly createdAt: string;
  };
  readonly prepared: Extract<
    PreparedThreadTurn,
    { readonly method: typeof ORCHESTRATION_V2_WS_METHODS.dispatchCommand }
  >;
}

const PLAN_CREATION_OPERATION_MAX_BYTES = 256 * 1024;

// Admit only this action's command intent; uploads and unrelated commands are held.
const compiledMainSchema13 = Schema.decodeUnknownSync(PreparedThreadTurnJson);

const compiledMainSchema12 = Schema.decodeUnknownSync(Envelope);

export function decodePlanCreationOperation(value: unknown): PlanCreationOperation {
  const envelope = compiledMainSchema12(value, { onExcessProperty: "error" });
  const prepared = compiledMainSchema13(envelope.prepared, {
    onExcessProperty: "error",
  });
  if (
    prepared.method !== ORCHESTRATION_V2_WS_METHODS.dispatchCommand ||
    prepared.payload.type !== "message.dispatch" ||
    prepared.payload.threadId !== envelope.create.threadId ||
    prepared.payload.commandId === envelope.create.commandId ||
    prepared.payload.attachments.length !== 0 ||
    prepared.payload.dispatchMode.type !== "start_immediately" ||
    prepared.payload.creationSource !== "web" ||
    prepared.payload.sourcePlanRef?.threadId !== envelope.sourceThreadId ||
    prepared.payload.sourcePlanRef?.planId !== envelope.sourcePlanId ||
    typeof prepared.payload.modelSelectionExplicit !== "boolean" ||
    JSON.stringify(prepared.payload.modelSelection) !==
      JSON.stringify(envelope.create.modelSelection)
  ) {
    throw new Error("Saved plan action needs reconciliation. No replacement was created.");
  }
  const operation = { ...envelope, prepared };
  if (
    new TextEncoder().encode(JSON.stringify(operation)).byteLength >
    PLAN_CREATION_OPERATION_MAX_BYTES
  )
    throw new Error("Plan action is too large to retain safely. No further work was sent.");
  return freezePreparedTurnRequest(operation);
}

export function encodePlanCreationOperation(operation: PlanCreationOperation): unknown {
  return {
    ...decodePlanCreationOperation(operation),
    prepared: encodePreparedThreadTurn(operation.prepared),
  };
}

export function planCreateDelivery(error: unknown, operation: PlanCreationOperation) {
  return readDispatchDeliveryEvidence(error, {
    commandType: "thread.create",
    commandId: operation.create.commandId,
    threadId: operation.create.threadId,
  }).status;
}

export function advancePlanCreation(
  operation: PlanCreationOperation,
  phase: PlanCreationOperation["phase"],
): PlanCreationOperation {
  if (operation.phase === phase || operation.phase === "accepted") return operation;
  const allowed =
    (operation.phase === "create-pending" &&
      (phase === "create-rejected" || phase === "message-pending")) ||
    (operation.phase === "message-pending" &&
      (phase === "accepted" || phase === "message-rejected")) ||
    (operation.phase === "message-rejected" && phase === "accepted");
  if (!allowed) throw new Error("Plan action stage does not match its retained intent.");
  return decodePlanCreationOperation({ ...operation, phase });
}
