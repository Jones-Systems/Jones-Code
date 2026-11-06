import type {
  CommandId,
  MessageId,
  OrchestrationV2ServerCommand,
  OrchestrationV2ThreadProjection,
} from "@t3tools/contracts";
import type { ThreadTitleVerdict } from "@t3tools/provider-core/server/jones/ThreadTitleAssessment";
import type { TitleState } from "./store.ts";
import { eligibleOwnerMessages } from "./evidence.ts";
import { isNativeMaintenanceCommand } from "../../orchestration-v2/CommandPolicy.ts";

export type TitleCommand = Extract<
  OrchestrationV2ServerCommand,
  {
    readonly type:
      | "message.dispatch"
      | "thread.title.regeneration.complete"
      | "thread.metadata.update";
  }
>;

export function titlePolicyApplies(command: {
  readonly type: string;
  readonly title?: unknown;
  readonly regenerateTitle?: unknown;
}): command is TitleCommand {
  return (
    command.type === "message.dispatch" ||
    command.type === "thread.title.regeneration.complete" ||
    (command.type === "thread.metadata.update" &&
      (command.title !== undefined || command.regenerateTitle !== undefined))
  );
}

export interface TitleDecision {
  readonly state?: TitleState;
  readonly arm?: {
    readonly requestId: CommandId;
    readonly messageId: MessageId;
    readonly initial: boolean;
  };
  readonly seed?: string;
}

export function decideTitle(input: {
  readonly command: TitleCommand;
  readonly projection: Pick<OrchestrationV2ThreadProjection, "thread" | "messages" | "runs">;
  readonly state: TitleState | null;
  readonly completingRequestId?: CommandId;
  readonly verdict?: ThreadTitleVerdict;
}): TitleDecision {
  const { command, projection } = input;
  let state = input.state;
  if (
    command.type === "thread.metadata.update" &&
    (command.title !== undefined || command.regenerateTitle !== undefined)
  ) {
    return {
      state: {
        status: "stopped",
        accepted: state?.accepted ?? [],
        covered: state?.covered ?? [],
        knownInsufficient: state?.knownInsufficient ?? false,
        lastRequestId: state?.lastRequestId ?? null,
        pending: null,
      },
    };
  }
  const owners = eligibleOwnerMessages(projection);
  // A fourth acceptance must never trigger a delayed first/next opportunity.
  const accepting =
    command.type === "message.dispatch"
      ? owners.findIndex((message) => message.id === command.messageId)
      : -1;
  if (command.type === "message.dispatch" && accepting < 0) return {};
  if (state === null) {
    const previousUsers = projection.messages.filter(
      (message) =>
        message.role === "user" &&
        (command.type !== "message.dispatch" || message.id !== command.messageId),
    );
    const seed =
      command.type === "message.dispatch"
        ? (command.titleSeed ??
          (projection.thread.title === "New thread" &&
          previousUsers.length > 0 &&
          previousUsers.every(isNativeMaintenanceCommand)
            ? projection.thread.title
            : undefined))
        : undefined;
    if (
      command.type !== "message.dispatch" ||
      accepting !== 0 ||
      seed === undefined ||
      projection.thread.historyOrigin === "v1_import" ||
      projection.thread.titleRegeneration != null ||
      (projection.thread.forkedFrom == null &&
        projection.thread.title !== "New thread" &&
        projection.thread.title !== seed)
    )
      return {};
    const evidence = [command.messageId];
    return {
      state: {
        status: "active",
        accepted: evidence,
        covered: [],
        knownInsufficient: false,
        lastRequestId: null,
        pending: { requestId: command.commandId, evidence },
      },
      arm: { requestId: command.commandId, messageId: command.messageId, initial: true },
      seed,
    };
  }
  if (state.status !== "active") return {};
  if (command.type === "message.dispatch") {
    if (state.accepted.includes(command.messageId)) return {};
    if (state.accepted.length >= 3) return {};
    state = { ...state, accepted: [...state.accepted, command.messageId] };
  }
  if (command.type === "thread.title.regeneration.complete") {
    if (
      state.pending?.requestId !== command.requestId ||
      input.completingRequestId !== command.requestId
    )
      return {};
    const covered = state.pending.evidence;
    const verdict = input.verdict ?? "unknown";
    state = {
      ...state,
      covered,
      lastRequestId: command.requestId,
      pending: null,
      knownInsufficient: state.knownInsufficient || verdict === "insufficient",
      status: verdict === "sufficient" ? "sufficient" : covered.length >= 3 ? "capped" : "active",
    };
  } else if (command.type !== "message.dispatch") return {};
  if (
    state.status !== "active" ||
    state.pending !== null ||
    projection.thread.titleRegeneration != null
  ) {
    return state === input.state ? {} : { state };
  }
  const eligibleIds = new Set(owners.map((message) => message.id));
  const evidence = state.accepted.filter((id) => eligibleIds.has(id));
  // Evidence is a strict extension, not a role=user count. Rollback/reordering
  // cannot replay or replace an opportunity already consumed.
  if (
    evidence.length <= state.covered.length ||
    !state.covered.every((id, index) => evidence[index] === id)
  ) {
    return state === input.state ? {} : { state };
  }
  const requestId = command.commandId;
  return {
    state: { ...state, pending: { requestId, evidence } },
    arm: { requestId, messageId: evidence.at(-1)!, initial: false },
  };
}
