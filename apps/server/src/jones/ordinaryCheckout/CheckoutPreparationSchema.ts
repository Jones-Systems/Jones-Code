import {
  CommandId,
  EnvironmentId,
  ImportedHistoryStart,
  OrchestrationV2Command,
  OrchestrationV2DomainEventJson,
  OrchestrationV2InternalCommand,
  OrchestrationV2LegacyBootstrapPolicy,
  ThreadId,
  RunId,
  ThreadSelfConfigurationInput,
  type OrchestrationV2ServerCommand,
  type OrchestrationV2DomainEvent,
} from "@t3tools/contracts";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";
import {
  OrchestrationEffectRequestV2,
  NativeOrchestrationEffectPayloadV2,
  type PendingOrchestrationEffectV2,
} from "../../orchestration-v2/EffectOutbox.ts";
import { CheckoutError, digest } from "./CheckoutProtocol.ts";
import {
  LegacyOwnedTerminalControl,
  LegacyNoTerminalControl,
} from "../../orchestration-v2/RecordedTypes.ts";

const privateDeleteIdentity = {
  commandId: CommandId,
  threadId: ThreadId,
  runId: RunId,
  legacyBootstrap: OrchestrationV2LegacyBootstrapPolicy,
};
const PrivateDeleteCommand = Schema.Union([
  Schema.Struct({
    ...privateDeleteIdentity,
    type: Schema.Literal("legacy-bootstrap.guard-rejection-delete"),
    legacyOwnedControl: LegacyOwnedTerminalControl,
  }),
  Schema.Struct({
    ...privateDeleteIdentity,
    type: Schema.Literal("legacy-bootstrap.guard-rejection-delete"),
    legacyNoControl: LegacyNoTerminalControl,
  }),
  Schema.Struct({
    ...privateDeleteIdentity,
    type: Schema.Literal("legacy-bootstrap.failure-delete"),
    legacyNoControl: LegacyNoTerminalControl,
  }),
]);

const encodeEvent = Schema.encodeSync(OrchestrationV2DomainEventJson);
const decodeEvent = Schema.decodeUnknownSync(OrchestrationV2DomainEventJson);
const encodeRequest = Schema.encodeSync(OrchestrationEffectRequestV2);
const encodeNative = Schema.encodeSync(NativeOrchestrationEffectPayloadV2);
// Use the contracts owner's exact internal schema so its authority fields and
// decoder cannot drift across syncs. Public wire schemas remain unchanged.
const internalCommand = OrchestrationV2InternalCommand;
const SelfConfigurationCommand = Schema.Struct({
  type: Schema.Literal("jones.thread.self-configure"),
  commandId: CommandId,
  threadId: ThreadId,
  environmentId: EnvironmentId,
  input: ThreadSelfConfigurationInput,
});
const withLegacyInputs = <const Fields extends Schema.Struct.Fields>(
  command: Schema.Struct<Fields>,
) =>
  command.mapFields(
    (fields) => ({
      ...fields,
      legacyBootstrap: Schema.optionalKey(OrchestrationV2LegacyBootstrapPolicy),
      legacyPreparationFailureKnown: Schema.optionalKey(Schema.Boolean),
    }),
    { unsafePreserveChecks: true },
  );
// Keep each member's inferred field correlations, including its original checks.
const publicServerCommand = Schema.Union([
  withLegacyInputs(OrchestrationV2Command.members[0]),
  withLegacyInputs(OrchestrationV2Command.members[1]),
  withLegacyInputs(OrchestrationV2Command.members[2]),
  withLegacyInputs(OrchestrationV2Command.members[3]),
  withLegacyInputs(OrchestrationV2Command.members[4]),
  withLegacyInputs(OrchestrationV2Command.members[5]),
  withLegacyInputs(OrchestrationV2Command.members[6]),
  withLegacyInputs(OrchestrationV2Command.members[7]),
  withLegacyInputs(OrchestrationV2Command.members[8]),
  withLegacyInputs(OrchestrationV2Command.members[9]),
  withLegacyInputs(OrchestrationV2Command.members[10]),
  withLegacyInputs(OrchestrationV2Command.members[11]),
  withLegacyInputs(OrchestrationV2Command.members[12]),
  withLegacyInputs(OrchestrationV2Command.members[13]),
  withLegacyInputs(OrchestrationV2Command.members[14]),
  withLegacyInputs(OrchestrationV2Command.members[15]),
  withLegacyInputs(OrchestrationV2Command.members[16]),
  withLegacyInputs(OrchestrationV2Command.members[17]),
  withLegacyInputs(OrchestrationV2Command.members[18]),
  withLegacyInputs(OrchestrationV2Command.members[19]),
  withLegacyInputs(OrchestrationV2Command.members[20]),
  withLegacyInputs(OrchestrationV2Command.members[21]),
  withLegacyInputs(OrchestrationV2Command.members[22]),
  withLegacyInputs(OrchestrationV2Command.members[23]),
  withLegacyInputs(OrchestrationV2Command.members[24]),
  withLegacyInputs(OrchestrationV2Command.members[25]),
  withLegacyInputs(OrchestrationV2Command.members[26]),
  withLegacyInputs(OrchestrationV2Command.members[27]),
  withLegacyInputs(OrchestrationV2Command.members[28]),
  withLegacyInputs(OrchestrationV2Command.members[29]),
  withLegacyInputs(OrchestrationV2Command.members[30]),
  withLegacyInputs(OrchestrationV2Command.members[31]),
  withLegacyInputs(OrchestrationV2Command.members[32]),
  withLegacyInputs(OrchestrationV2Command.members[33]),
  withLegacyInputs(OrchestrationV2Command.members[34]),
  withLegacyInputs(OrchestrationV2Command.members[35]),
  withLegacyInputs(OrchestrationV2Command.members[36]),
  withLegacyInputs(OrchestrationV2Command.members[37]),
  withLegacyInputs(OrchestrationV2Command.members[38]),
  withLegacyInputs(OrchestrationV2Command.members[39]),
  withLegacyInputs(OrchestrationV2Command.members[40]),
  withLegacyInputs(OrchestrationV2Command.members[41]),
  withLegacyInputs(OrchestrationV2Command.members[42]),
  withLegacyInputs(OrchestrationV2Command.members[43]),
  withLegacyInputs(OrchestrationV2Command.members[44]),
  withLegacyInputs(OrchestrationV2Command.members[45]),
  withLegacyInputs(OrchestrationV2Command.members[46]),
  withLegacyInputs(OrchestrationV2Command.members[47]),
  withLegacyInputs(OrchestrationV2Command.members[48]),
  withLegacyInputs(OrchestrationV2Command.members[49]),
  withLegacyInputs(OrchestrationV2Command.members[50]),
  withLegacyInputs(OrchestrationV2Command.members[51]),
  withLegacyInputs(OrchestrationV2Command.members[52]),
  withLegacyInputs(OrchestrationV2Command.members[53]),
  withLegacyInputs(OrchestrationV2Command.members[54]),
  withLegacyInputs(OrchestrationV2Command.members[55]),
  withLegacyInputs(OrchestrationV2Command.members[56]),
  withLegacyInputs(OrchestrationV2Command.members[57]),
]) satisfies Schema.Constraint & { readonly Type: OrchestrationV2ServerCommand };
type ServerCommandCoverage<
  A extends typeof publicServerCommand.Type | typeof internalCommand.Type,
> = A;
type CheckoutCommand =
  | ServerCommandCoverage<OrchestrationV2ServerCommand>
  | typeof ImportedHistoryStart.Type
  | typeof SelfConfigurationCommand.Type
  | typeof PrivateDeleteCommand.Type;
const CheckoutCommand = Schema.Union([
  publicServerCommand,
  internalCommand,
  ImportedHistoryStart,
  SelfConfigurationCommand,
  PrivateDeleteCommand,
]);
const decodeFullCommand = Schema.decodeUnknownEffect(CheckoutCommand, {
  onExcessProperty: "error",
});
const encodeFullCommand = Schema.encodeEffect(CheckoutCommand);
// Only TurnItemPositionStore's assigned ordinal may differ. Every other byte of
// the schema-encoded ordered plan is authority-bearing, including extra effects.
const events = (input: readonly OrchestrationV2DomainEvent[], authority: boolean) =>
  input.map((event) => {
    // Match the persisted contract defaults before comparing complete plans.
    const encoded = encodeEvent(decodeEvent(encodeEvent(event)));
    return authority && encoded.type === "turn-item.updated"
      ? { ...encoded, payload: { ...encoded.payload, ordinal: 0 } }
      : encoded;
  });
const effects = (input: readonly PendingOrchestrationEffectV2[]) =>
  input.map((effect) => ({
    id: effect.id,
    commandId: effect.commandId,
    threadId: effect.threadId,
    payload:
      effect.nativeCreationExecutionReference === undefined
        ? encodeRequest(effect.request)
        : encodeNative({
            request: effect.request,
            nativeCreationExecutionReference: effect.nativeCreationExecutionReference,
          }),
    availableAt: effect.availableAt === undefined ? null : DateTime.formatIso(effect.availableAt),
  }));
export const planDigests = (plan: {
  readonly events: readonly OrchestrationV2DomainEvent[];
  readonly effects: readonly PendingOrchestrationEffectV2[];
}) => ({
  authority: digest({ events: events(plan.events, true), effects: effects(plan.effects) }),
  actual: digest({ events: events(plan.events, false), effects: effects(plan.effects) }),
});
const decodedJson = (value: unknown): unknown => {
  if (DateTime.isDateTime(value)) return DateTime.formatIso(value);
  if (Array.isArray(value)) return value.map(decodedJson);
  if (value !== null && typeof value === "object")
    return Object.fromEntries(
      Object.entries(value).map(([key, field]) => [key, decodedJson(field)]),
    );
  return value;
};
export const decodedCommand = Effect.fn("CheckoutPreparationSchema.decodedCommand")(
  function* (input: unknown) {
    const command = yield* decodeFullCommand(input);
    // Wire compatibility fallbacks may normalize legacy or unknown nested values.
    // This seam receives decoded server input; normalization cannot erase identity.
    if (digest(decodedJson(input)) !== digest(decodedJson(command)))
      return yield* new CheckoutError({ reason: "identity" });
    return {
      command: command satisfies CheckoutCommand,
      digest: digest(decodedJson(yield* encodeFullCommand(command))),
    };
  },
  Effect.mapError((cause) => new CheckoutError({ reason: "identity", cause })),
);
