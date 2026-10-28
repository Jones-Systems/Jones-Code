import type {
  OrchestrationV2ConversationMessage,
  OrchestrationV2ProviderThread,
  OrchestrationV2Run,
} from "@t3tools/contracts";
import * as Schema from "effect/Schema";

export class GoalNotificationNotDelivered extends Schema.TaggedError<GoalNotificationNotDelivered>()(
  "GoalNotificationNotDelivered",
  {},
) {}

export class GoalNotificationDeliveryUnknown extends Schema.TaggedError<GoalNotificationDeliveryUnknown>()(
  "GoalNotificationDeliveryUnknown",
  { cause: Schema.Defect() },
) {}

export function goalNotificationMaySteer(input: {
  readonly message: Pick<
    OrchestrationV2ConversationMessage,
    "createdBy" | "creationSource" | "notification"
  >;
  readonly run: OrchestrationV2Run | undefined;
  readonly providerThread: OrchestrationV2ProviderThread | undefined;
  readonly alreadyRecorded: boolean;
}): boolean {
  return (
    input.message.notification !== undefined &&
    input.message.createdBy === "agent" &&
    input.message.creationSource === "server" &&
    !input.alreadyRecorded &&
    input.run?.status === "running" &&
    input.providerThread?.goal?.status === "active"
  );
}

export function isGoalNotificationNotDelivered(cause: unknown): boolean {
  return (
    Schema.is(GoalNotificationNotDelivered)(cause) ||
    (typeof cause === "object" &&
      cause !== null &&
      "cause" in cause &&
      Schema.is(GoalNotificationNotDelivered)(cause.cause))
  );
}

export function isGoalNotificationDeliveryUnknown(cause: unknown): boolean {
  return (
    Schema.is(GoalNotificationDeliveryUnknown)(cause) ||
    (typeof cause === "object" &&
      cause !== null &&
      "cause" in cause &&
      Schema.is(GoalNotificationDeliveryUnknown)(cause.cause))
  );
}
