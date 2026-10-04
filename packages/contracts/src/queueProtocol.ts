import * as Schema from "effect/Schema";

const queueDispatchCapabilityFields = {
  schemaVersion: Schema.Literal("t3.queue-dispatch-capability/v1"),
  orchestrationProtocolVersion: Schema.Literal(1),
  dispatchGuard: Schema.Literal("t3.thread-turn-dispatch-guard/v1"),
  commandObservation: Schema.Literal("t3.command-observation/v1"),
  persistedRejection: Schema.Literal("t3.command-rejection/v1"),
  providerInventory: Schema.Literal("t3.provider-queue-inventory/v1"),
  qualifiedQuota: Schema.Literal("codex.t3-qualified-quota/v1"),
  authSession: Schema.Literal("t3.auth-session-cli/v1"),
};

// Validate original wire keys before a struct decoder can strip unknown claims.
export const QueueDispatchCapability = Schema.flip(
  Schema.flip(Schema.Struct(queueDispatchCapabilityFields)).check(
    Schema.makeFilter((value) =>
      Reflect.ownKeys(value).every((key) => Object.hasOwn(queueDispatchCapabilityFields, key)),
    ),
  ),
);
export type QueueDispatchCapability = typeof QueueDispatchCapability.Type;

// These literals describe legacy protocol-1 dispatch, observation, inventory and CLI semantics.
// Retain them for older descriptors; protocol-2 servers must not advertise this capability.
// They do not qualify a particular provider account or authorize thread creation.
export const QUEUE_DISPATCH_CAPABILITY = {
  schemaVersion: "t3.queue-dispatch-capability/v1",
  orchestrationProtocolVersion: 1,
  dispatchGuard: "t3.thread-turn-dispatch-guard/v1",
  commandObservation: "t3.command-observation/v1",
  persistedRejection: "t3.command-rejection/v1",
  providerInventory: "t3.provider-queue-inventory/v1",
  qualifiedQuota: "codex.t3-qualified-quota/v1",
  authSession: "t3.auth-session-cli/v1",
} as const satisfies QueueDispatchCapability;
