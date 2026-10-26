import { LinkedRequestContext } from "@t3tools/contracts";
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";

import type { McpInvocationScope } from "../../mcp/McpInvocationContext.ts";

export const LINKED_REQUEST_CONTEXT_HEADER = "t3-linked-request-context";
export const REQUEST_CONTEXT_MAX_SENT_CHARS = 2048;
export const REQUEST_CONTEXT_MAX_ACCEPTED_CHARS = 4096;
export const PEER_REQUEST_HEADER_BUDGET = 8192;
export const UNCONTROLLED_HEADER_ALLOWANCE = 1024;

export interface BoundLinkedRequestContext {
  readonly context: LinkedRequestContext;
  readonly linkSessionId: string;
}

export const IncomingLinkedRequestContext = Context.Reference<
  BoundLinkedRequestContext | undefined
>("jones/peerTransport/IncomingLinkedRequestContext", { defaultValue: () => undefined });
export const LinkedRequestSentAtOverride = Context.Reference<string | undefined>(
  "jones/peerTransport/LinkedRequestSentAtOverride",
  { defaultValue: () => undefined },
);

const decodeContext = Schema.decodeUnknownOption(LinkedRequestContext);
const decodeJson = Schema.decodeUnknownOption(Schema.fromJsonString(Schema.Unknown));
const encodeContext = Schema.encodeSync(Schema.fromJsonString(LinkedRequestContext));
const isRecord = (value: unknown): value is Record<string, unknown> =>
  typeof value === "object" && value !== null && !Array.isArray(value);

/** Advisory failures never reject an authenticated call; v1 origin binding remains strict. */
export const bindLinkedRequestContext = (
  scope: McpInvocationScope,
  header: string | undefined,
): BoundLinkedRequestContext | undefined => {
  if (
    scope.client?.linked !== true ||
    scope.thread !== undefined ||
    typeof header !== "string" ||
    header.length === 0 ||
    header.length > REQUEST_CONTEXT_MAX_ACCEPTED_CHARS ||
    !/^[A-Za-z0-9_-]+$/.test(header)
  )
    return undefined;
  const bytes = Buffer.from(header, "base64url");
  if (bytes.toString("base64url") !== header) return undefined;
  let json: string;
  try {
    json = new TextDecoder("utf-8", { fatal: true }).decode(bytes);
  } catch {
    return undefined;
  }
  const parsed = decodeJson(json);
  if (Option.isNone(parsed)) return undefined;
  const value = parsed.value;
  let decoded = decodeContext(value);
  if (
    Option.isNone(decoded) &&
    isRecord(value) &&
    isRecord(value.project) &&
    "icon" in value.project
  ) {
    const { icon: _icon, ...project } = value.project;
    decoded = decodeContext({ ...value, project });
  }
  if (Option.isNone(decoded)) return undefined;
  const context = decoded.value;
  const origin = scope.client.remoteOrigin;
  if (
    origin?.linkSessionId === scope.client.sessionId &&
    origin.thread !== undefined &&
    (context.thread !== undefined ||
      context.parent !== undefined ||
      context.project !== undefined) &&
    context.thread?.threadId !== origin.thread.threadId
  )
    return undefined;
  return { context, linkSessionId: scope.client.sessionId };
};

// Read at invocation time, never when the intake or tool handler is constructed.
export const readBoundLinkedRequestContext: (
  scope: McpInvocationScope,
) => Effect.Effect<BoundLinkedRequestContext | undefined> = Effect.fn(
  "LinkedRequestContext.readBoundLinkedRequestContext",
)(function* (scope: McpInvocationScope) {
  const bound = yield* IncomingLinkedRequestContext;
  return scope.client?.linked === true &&
    scope.thread === undefined &&
    bound?.linkSessionId === scope.client.sessionId
    ? bound
    : undefined;
});

export const encodeLinkedRequestContext = (context: LinkedRequestContext): string =>
  Buffer.from(encodeContext(context), "utf8").toString("base64url");

export const fitLinkedRequestContext = (
  context: LinkedRequestContext,
  maxChars: number,
): string | undefined => {
  let candidate = { ...context };
  const fits = () => {
    const encoded = encodeLinkedRequestContext(candidate);
    return encoded.length <= maxChars ? encoded : undefined;
  };
  let fitted = fits();
  if (fitted !== undefined) return fitted;
  if (candidate.project !== undefined) {
    const { icon: _icon, ...project } = candidate.project;
    candidate = { ...candidate, project };
  }
  fitted = fits();
  if (fitted !== undefined) return fitted;
  if (candidate.parent !== undefined) {
    const { title: _title, ...parent } = candidate.parent;
    candidate = { ...candidate, parent };
  }
  fitted = fits();
  if (fitted !== undefined) return fitted;
  if (candidate.project !== undefined) {
    const { title: _title, ...project } = candidate.project;
    candidate = { ...candidate, project };
  }
  fitted = fits();
  if (fitted !== undefined) return fitted;
  const { parent: _parent, ...withoutParent } = candidate;
  candidate = withoutParent;
  fitted = fits();
  if (fitted !== undefined) return fitted;
  const { project: _project, ...withoutProject } = candidate;
  candidate = withoutProject;
  fitted = fits();
  if (fitted !== undefined) return fitted;
  if (candidate.thread !== undefined) {
    const { title: _title, ...thread } = candidate.thread;
    candidate = { ...candidate, thread };
  }
  fitted = fits();
  if (fitted !== undefined) return fitted;
  const { thread: _thread, ...base } = candidate;
  candidate = base;
  return fits();
};

export const normalizeAdvisoryTitle = (title: string | null | undefined): string | undefined => {
  if (title == null) return undefined;
  const normalized = title
    .replace(/(?:(?![\u200C\u200D])[\p{Cc}\p{Cf}\p{Cs}\p{Zl}\p{Zp}])+/gu, " ")
    .replace(/\s+/gu, " ")
    .trim();
  if (normalized.length === 0) return undefined;
  if (normalized.length <= 120) return normalized;
  let end = 119;
  if (/[\uD800-\uDBFF]/u.test(normalized[end - 1]!) && /[\uDC00-\uDFFF]/u.test(normalized[end]!)) {
    end -= 1;
  }
  return `${normalized.slice(0, end).trimEnd()}…`;
};

/** Controlled header bytes; the sender separately reserves room for the request line and runtime headers. */
export const requestHeaderBytes = (headers: Readonly<Record<string, string>>): number =>
  Object.entries(headers).reduce(
    (total, [name, value]) =>
      total + Buffer.byteLength(name, "utf8") + Buffer.byteLength(value, "utf8") + 4,
    0,
  );
