// @effect-diagnostics nodeBuiltinImport:off - Pure deterministic identity hashing has no runtime service dependency; golden vectors pin the wire IDs.
import * as NodeCrypto from "node:crypto";
import {
  CommandId,
  MessageId,
  ThreadId,
  type ModelSelection,
  type ProjectId,
  type ProviderInteractionMode,
  type RuntimeMode,
} from "@t3tools/contracts";
import type { ThreadLaunchWorkspaceStrategy } from "../../../orchestration-v2/ThreadLaunchService.ts";

export interface ClientLaunchRequest {
  readonly clientRequestId: string;
  readonly projectId?: ProjectId | undefined;
  readonly title: string;
  readonly modelSelection?: ModelSelection | undefined;
  readonly runtimeMode?: RuntimeMode | undefined;
  readonly interactionMode?: ProviderInteractionMode | undefined;
  readonly workspaceStrategy?: ThreadLaunchWorkspaceStrategy | undefined;
  readonly message?: string | undefined;
  readonly scratch?: boolean | undefined;
  readonly attachments?: ReadonlyArray<unknown> | undefined;
}

// RFC 8785 uses UTF-16 key order and ECMAScript primitive serialization;
// malformed Unicode and non-finite numbers must fail instead of hashing a replacement.
export function canonicalRequestJson(value: unknown): string {
  if (value === null || typeof value === "boolean") return JSON.stringify(value);
  if (typeof value === "string") {
    if (!value.isWellFormed()) throw new TypeError("Invalid Unicode in canonical request");
    return JSON.stringify(value);
  }
  if (typeof value === "number") {
    if (!Number.isFinite(value)) throw new TypeError("Invalid number in canonical request");
    return JSON.stringify(value);
  }
  if (Array.isArray(value)) return `[${value.map(canonicalRequestJson).join(",")}]`;
  if (typeof value === "object") {
    return `{${Object.keys(value)
      .sort()
      .map(
        (key) =>
          `${canonicalRequestJson(key)}:${canonicalRequestJson((value as Record<string, unknown>)[key])}`,
      )
      .join(",")}}`;
  }
  throw new TypeError("Non-JSON canonical request");
}
const sha256 = (value: string) => NodeCrypto.createHash("sha256").update(value).digest("hex");

export function clientLaunchIds(namespace: string, key: string) {
  const commandId = CommandId.make(
    `mcp-launch:v1:${sha256(`jones-mcp-launch\0v1\0${namespace}\0${key}`)}`,
  );
  return { commandId, threadId: ThreadId.make(commandId), messageId: MessageId.make(commandId) };
}

export function clientLaunchDigest(input: ClientLaunchRequest, projectId: ProjectId): string {
  const strategy = input.workspaceStrategy ?? { type: "root" };
  const workspaceStrategy =
    strategy.type === "root"
      ? { type: "root" }
      : strategy.type === "worktree"
        ? {
            type: strategy.type,
            baseRef: strategy.baseRef ?? null,
            branch: strategy.branch ?? null,
            startFromOrigin: strategy.startFromOrigin ?? false,
          }
        : {
            type: strategy.type,
            worktreePath: strategy.worktreePath,
            branch: strategy.branch ?? null,
          };
  return sha256(
    canonicalRequestJson({
      v: 1,
      op: "thread-launch",
      projectId,
      title: input.title.trim(),
      modelSelection: input.modelSelection ?? null,
      runtimeMode: input.runtimeMode ?? null,
      interactionMode: input.interactionMode ?? null,
      workspaceStrategy,
      message: input.message ?? null,
    }),
  );
}
