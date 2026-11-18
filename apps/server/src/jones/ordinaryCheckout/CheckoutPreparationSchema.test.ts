import { assert, it } from "@effect/vitest";
import {
  CommandId,
  EventId,
  ProviderDriverKind,
  ProviderInstanceId,
  ProviderThreadId,
  ThreadId,
  type OrchestrationV2DomainEvent,
} from "@t3tools/contracts";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import { decodedCommand, planDigests } from "./CheckoutPreparationSchema.ts";

const now = DateTime.makeUnsafe("2026-11-16T12:00:00.000Z");
const event = {
  id: EventId.make("checkout-plan-provider-event"),
  type: "provider-thread.updated",
  threadId: ThreadId.make("checkout-plan-thread"),
  occurredAt: now,
  payload: {
    id: ProviderThreadId.make("checkout-plan-provider"),
    driver: ProviderDriverKind.make("codex"),
    providerInstanceId: ProviderInstanceId.make("codex"),
    providerSessionId: null,
    appThreadId: ThreadId.make("checkout-plan-thread"),
    ownerNodeId: null,
    nativeThreadRef: null,
    nativeConversationHeadRef: null,
    status: "not_loaded",
    firstRunOrdinal: null,
    lastRunOrdinal: null,
    handoffIds: [],
    forkedFrom: null,
    createdAt: now,
    updatedAt: now,
  },
} satisfies OrchestrationV2DomainEvent;

it("compares omitted contract defaults with the values materialized by event persistence", () => {
  const persisted = {
    ...event,
    payload: {
      ...event.payload,
      pendingBackgroundTasks: [],
      contextUsage: null,
      nativeMetadata: null,
      goal: null,
    },
  } satisfies OrchestrationV2DomainEvent;
  assert.deepEqual(
    planDigests({ events: [event], effects: [] }),
    planDigests({ events: [persisted], effects: [] }),
  );
});

it("retains explicit provider state, event identity and event order in plan authority", () => {
  const changed = {
    ...event,
    id: EventId.make("checkout-plan-provider-changed"),
    payload: { ...event.payload, status: "idle" },
  } satisfies OrchestrationV2DomainEvent;
  assert.notEqual(
    planDigests({ events: [event], effects: [] }).authority,
    planDigests({ events: [changed], effects: [] }).authority,
  );
  assert.notEqual(
    planDigests({ events: [event, changed], effects: [] }).authority,
    planDigests({ events: [changed, event], effects: [] }).authority,
  );
});

it.effect("binds provider switches to their full command and rejects extra authority", () =>
  Effect.gen(function* () {
    const command = {
      type: "provider.switch" as const,
      commandId: CommandId.make("checkout-provider-switch"),
      threadId: ThreadId.make("checkout-plan-thread"),
      modelSelection: {
        instanceId: ProviderInstanceId.make("codex"),
        model: "gpt-6.1-sol",
        options: [{ id: "reasoningEffort", value: "high" }],
      },
    };
    const accepted = yield* decodedCommand(command);
    assert.deepEqual(accepted.command, command);
    const changed = yield* decodedCommand({
      ...command,
      modelSelection: { ...command.modelSelection, model: "gpt-6-astra" },
    });
    assert.notEqual(changed.digest, accepted.digest);
    const extra = yield* decodedCommand({ ...command, unexpectedAuthority: true }).pipe(
      Effect.result,
    );
    assert.equal(extra._tag, "Failure");
  }),
);
