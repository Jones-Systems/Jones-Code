import * as Context from "effect/Context";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";

import type { McpClientCaller, McpInvocationScope } from "../../mcp/McpInvocationContext.ts";

export const LINKED_MESSAGE_ORIGIN_HEADER = "t3-linked-message-origin";
const MAX_HEADER_LENGTH = 4096;
const identity = Schema.String.check(
  Schema.isMaxLength(256),
  Schema.isPattern(/\S/u),
  Schema.isPattern(/^[^\p{Cc}\p{Cf}\p{Cs}\p{Zl}\p{Zp}]+$/u),
);
const Origin = Schema.Struct({
  schemaVersion: Schema.Literal("t3.linked-message-origin/v1"),
  environmentId: identity,
  thread: Schema.optionalKey(
    Schema.Struct({
      threadId: identity,
      title: Schema.optionalKey(Schema.String.check(Schema.isMaxLength(512))),
    }),
  ),
});
export type LinkedMessageOrigin = typeof Origin.Type;
export interface BoundLinkedMessageOrigin extends LinkedMessageOrigin {
  readonly linkSessionId: string;
}
const decodeOrigin = Schema.decodeUnknownOption(Schema.fromJsonString(Origin), {
  onExcessProperty: "error",
});

// Private dispatch context survives to the acceptance lock without giving a foreign ID local authority.
export const IncomingLinkedMessage = Context.Reference<boolean>(
  "jones/peerTransport/IncomingLinkedMessage",
  { defaultValue: () => false },
);

export const encodeLinkedMessageOrigin = (origin: LinkedMessageOrigin): string =>
  Buffer.from(JSON.stringify(origin), "utf8").toString("base64url");

/** Bind only after authentication. The peer asserts these labels; its session remains the authority. */
export const withLinkedMessageOrigin = (
  scope: McpInvocationScope,
  header: string | undefined,
): McpInvocationScope | undefined => {
  if (header === undefined) return scope;
  if (
    scope.client?.linked !== true ||
    scope.thread !== undefined ||
    header.length === 0 ||
    header.length > MAX_HEADER_LENGTH ||
    !/^[A-Za-z0-9_-]+$/.test(header)
  )
    return undefined;
  const bytes = Buffer.from(header, "base64url");
  if (bytes.toString("base64url") !== header) return undefined;
  const json = bytes.toString("utf8");
  if (!Buffer.from(json, "utf8").equals(bytes)) return undefined;
  const decoded = decodeOrigin(json);
  if (Option.isNone(decoded)) return undefined;
  return {
    ...scope,
    client: {
      ...scope.client,
      remoteOrigin: { ...decoded.value, linkSessionId: scope.client.sessionId },
    },
  };
};

const escapeLabel = (value: string) =>
  value
    .replace(/[\u0000-\u001f\u007f]/g, " ")
    .replace(/\s+/g, " ")
    .trim()
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(/[\\`*_[\]{}()#+.!|~-]/g, "\\$&");

// A foreign ID may contain Markdown syntax; a longer fence keeps its exact text literal.
const literalIdentity = (value: string): string => {
  const longestBackticks = Math.max(
    0,
    ...Array.from(value.matchAll(/`+/g), (match) => match[0].length),
  );
  const fence = "`".repeat(longestBackticks + 1);
  const padding = /^[` ]|[` ]$/.test(value) ? " " : "";
  return `${fence}${padding}${value}${padding}${fence}`;
};

export const formatLinkedMessageOrigin = (client: McpClientCaller): string => {
  const via = `linked client ${escapeLabel(client.label)}`;
  const origin = client.remoteOrigin;
  if (origin === undefined || origin.linkSessionId !== client.sessionId) {
    return `${via} (remote sender unavailable)`;
  }
  const environment = `remote environment ${literalIdentity(origin.environmentId)}`;
  if (origin.thread === undefined) return `${via} (${environment}; remote thread unavailable)`;
  const title = escapeLabel(origin.thread.title ?? "") || "Untitled thread";
  return `${title} (${environment}, thread ${literalIdentity(origin.thread.threadId)}; via ${via})`;
};
