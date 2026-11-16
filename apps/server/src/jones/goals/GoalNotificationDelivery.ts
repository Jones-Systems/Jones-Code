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

const compiledIs1 = Schema.is(GoalNotificationNotDelivered);

export function isGoalNotificationNotDelivered(cause: unknown): boolean {
  return (
    compiledIs1(cause) ||
    (typeof cause === "object" && cause !== null && "cause" in cause && compiledIs1(cause.cause))
  );
}

const compiledIs2 = Schema.is(GoalNotificationDeliveryUnknown);

export function isGoalNotificationDeliveryUnknown(cause: unknown): boolean {
  return (
    compiledIs2(cause) ||
    (typeof cause === "object" && cause !== null && "cause" in cause && compiledIs2(cause.cause))
  );
}
