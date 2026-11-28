// @effect-diagnostics nodeBuiltinImport:off - compares a private digest in native provider callbacks.
import * as NodeCrypto from "node:crypto";
import type {
  CommandId,
  MessageId,
  OrchestrationV2ThreadLaunchInput,
  ProjectId,
  ProviderInstanceId,
  RunId,
  ThreadId,
} from "@t3tools/contracts";
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";

import {
  canonicalLegacyPayload,
  legacyPayloadHash,
} from "../../orchestration-v2/LegacyBootstrap.ts";

export interface AccountBinding {
  readonly instanceId: ProviderInstanceId;
  readonly digest: string;
}

export interface AccountLaunchAdmission {
  readonly commandId: CommandId;
  readonly binding: AccountBinding;
}

export interface AutoLaunchRequest {
  readonly commandId: CommandId;
  readonly projectId: ProjectId;
  readonly threadId: ThreadId | undefined;
  readonly digest: string;
}

export interface AutoLaunchWorkspace {
  readonly projectRoot: string;
  readonly worktreePath: string | null;
}

export interface AutoLaunchCommit {
  readonly request: AutoLaunchRequest;
  readonly workspace: AutoLaunchWorkspace;
}

export interface AutoLaunchAcceptance {
  readonly sequence: number;
  readonly firstMessage: { readonly messageId: MessageId; readonly runId: RunId } | null;
}

export interface AutoLaunchDecision extends AutoLaunchWorkspace {
  readonly projectId: ProjectId;
  readonly threadId: ThreadId | null;
  readonly digest: string;
  readonly refusal: string | null;
  readonly acceptance?: AutoLaunchAcceptance;
}

export const autoLaunchRequest = (
  input: OrchestrationV2ThreadLaunchInput,
): AutoLaunchRequest | undefined =>
  input.accountRouting === undefined
    ? undefined
    : {
        commandId: input.commandId,
        projectId: input.projectId,
        threadId: input.threadId,
        digest: legacyPayloadHash(
          canonicalLegacyPayload([
            "jones-auto-account-launch-v1",
            { ...input, creationSource: input.creationSource ?? "web" },
          ]),
        ),
      };

export const matchesAutoLaunch = (
  request: AutoLaunchRequest | undefined,
  decision: AutoLaunchDecision,
) =>
  request !== undefined &&
  request.projectId === decision.projectId &&
  request.digest === decision.digest &&
  ((decision.refusal === null && request.threadId === undefined) ||
    (request.threadId ?? null) === decision.threadId);

export const isReservedAutoLaunchCommand = (commandId: CommandId) =>
  /^(server:|jones:|native:|voice-command-|mcp-launch:)/.test(commandId) ||
  commandId.includes(":native:v2:");

export const AutoLaunchIntent = Context.Reference<
  { readonly request: AutoLaunchRequest; readonly workspace?: AutoLaunchWorkspace } | undefined
>("jones/accountRouting/AutoLaunchIntent", { defaultValue: () => undefined });

export const accountIdentityDigest = (family: "codex" | "claudeAgent", identity: string): string =>
  NodeCrypto.createHash("sha256")
    .update(JSON.stringify(["jones-native-account-v1", family, identity]))
    .digest("hex");

export const AccountLaunchIntent = Context.Reference<AccountLaunchAdmission | undefined>(
  "jones/accountRouting/AccountLaunchIntent",
  { defaultValue: () => undefined },
);

export const withAccountLaunchBinding = <A, E, R>(
  effect: Effect.Effect<A, E, R>,
  commandId: CommandId,
  binding: AccountBinding | undefined,
) =>
  binding === undefined
    ? effect
    : Effect.provideService(effect, AccountLaunchIntent, { commandId, binding });
