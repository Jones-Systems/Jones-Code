import { OrchestratorMcpFailure, type PeerLinkError } from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Ref from "effect/Ref";
import * as Schema from "effect/Schema";
import * as FetchHttpClient from "effect/http/FetchHttpClient";
import * as HttpBody from "effect/http/HttpBody";
import * as HttpClient from "effect/http/HttpClient";
import * as HttpClientRequest from "effect/http/HttpClientRequest";

import * as McpInvocationContext from "../../mcp/McpInvocationContext.ts";
import * as PeerLinks from "../../peer/PeerLinks.ts";
import type { PeerMcpClient } from "../../peer/PeerMcpClient.ts";

/** The protocol negotiated by this client with the peer's `/mcp`. */
const PROTOCOL_VERSION = "2025-06-18";
const Envelope = Schema.Struct({
  jsonrpc: Schema.Literal("2.0"),
  id: Schema.Union([Schema.Number, Schema.String]),
  result: Schema.optional(Schema.Unknown),
  error: Schema.optional(
    Schema.Struct({
      code: Schema.Number,
      message: Schema.String,
      data: Schema.optional(Schema.Unknown),
    }),
  ),
});
const ToolResult = Schema.Struct({
  content: Schema.Array(
    Schema.Struct({ type: Schema.String, text: Schema.optional(Schema.String) }),
  ),
  structuredContent: Schema.optional(Schema.Unknown),
  isError: Schema.optional(Schema.Boolean),
});
const Initialization = Schema.Struct({
  protocolVersion: Schema.Literal(PROTOCOL_VERSION),
  capabilities: Schema.Record(Schema.String, Schema.Unknown),
  serverInfo: Schema.Struct({ name: Schema.String, version: Schema.String }),
});
const Failure = Schema.Struct({
  code: OrchestratorMcpFailure.fields.code,
  message: Schema.String,
  remoteAdmission: OrchestratorMcpFailure.fields.remoteAdmission,
});
const decodeJson = Schema.decodeSync(Schema.fromJsonString(Schema.Unknown));
const encodeSessionKey = Schema.encodeSync(
  Schema.fromJsonString(Schema.Tuple([Schema.String, Schema.String, Schema.String])),
);
const decodeEnvelope = Schema.decodeUnknownEffect(Envelope);
const decodeToolResult = Schema.decodeUnknownEffect(ToolResult);
const decodeInitialization = Schema.decodeUnknownEffect(Initialization);
const decodeFailure = Schema.decodeUnknownOption(Failure);
const decodeTextFailure = Schema.decodeUnknownOption(Schema.fromJsonString(Failure));
const malformed = () =>
  new OrchestratorMcpFailure({
    code: "orchestration_error",
    message: "The linked environment answered in an unexpected shape.",
  });

/** A peer link problem, as the agent that asked hears it. */
const fromLinkError = (error: PeerLinkError) =>
  new OrchestratorMcpFailure({
    code:
      error.reason === "unknown_link"
        ? "invalid_request"
        : error.reason === "expired" || error.reason === "incompatible"
          ? "capability_denied"
          : "orchestration_error",
    message: error.message,
  });

const readEnvelope = (
  response: { readonly headers: Readonly<Record<string, string>>; readonly text: string },
  id: number,
) =>
  Effect.gen(function* () {
    const text = response.text;
    const raw = yield* Effect.try({
      try: () => {
        if (!response.headers["content-type"]?.toLowerCase().includes("text/event-stream")) {
          return decodeJson(text);
        }
        // Each SSE event is one message; only its own data lines may be joined.
        const matches: Array<unknown> = [];
        for (const event of text
          .split(/\r\n|\r|\n/)
          .join("\n")
          .split("\n\n")) {
          const data = event
            .split("\n")
            .filter((line) => line.startsWith("data:") || line === "data")
            .map((line) => line.slice(5).replace(/^ /, ""))
            .join("\n");
          if (data === "") continue;
          const message = decodeJson(data);
          if (
            typeof message !== "object" ||
            message === null ||
            !("jsonrpc" in message) ||
            message.jsonrpc !== "2.0"
          ) {
            throw new Error("Invalid JSON-RPC event");
          }
          if ("id" in message) {
            if (message.id === id) matches.push(message);
          } else if (!("method" in message) || typeof message.method !== "string") {
            throw new Error("Invalid JSON-RPC notification");
          }
        }
        if (matches.length !== 1) throw new Error("Missing or duplicate response");
        return matches[0];
      },
      catch: malformed,
    });
    const envelope = yield* decodeEnvelope(raw).pipe(Effect.mapError(malformed));
    if (
      envelope.id !== id ||
      Object.hasOwn(envelope, "result") === Object.hasOwn(envelope, "error")
    ) {
      return yield* malformed();
    }
    if (envelope.error !== undefined) {
      const refused = decodeFailure(envelope.error.data);
      return yield* refused._tag === "Some"
        ? new OrchestratorMcpFailure(refused.value)
        : new OrchestratorMcpFailure({ code: "invalid_request", message: envelope.error.message });
    }
    return envelope;
  });

export const make = Effect.gen(function* () {
  const links = yield* PeerLinks.PeerLinks;
  const httpClient = yield* HttpClient.HttpClient;
  const ids = yield* Ref.make(0);
  const nextId = Ref.getAndUpdate(ids, (current) => current + 1);
  // A session object is its generation: a late 404 must not evict a replacement.
  // Including the token prevents reusing a session after the link is replaced.
  const sessions = yield* Ref.make<ReadonlyMap<string, { readonly id: string }>>(new Map());

  const post = (url: string, token: string, body: unknown, headers: Record<string, string>) =>
    httpClient
      .execute(
        HttpClientRequest.post(`${url}/mcp`).pipe(
          HttpClientRequest.setHeaders({
            accept: "application/json, text/event-stream",
            authorization: `Bearer ${token}`,
            ...headers,
          }),
          HttpClientRequest.setBody(HttpBody.text(JSON.stringify(body), "application/json")),
        ),
      )
      .pipe(
        // Resolve verified the original destination; a redirect has not proved peer identity.
        Effect.provideService(FetchHttpClient.RequestInit, { redirect: "manual" }),
        // Drain even error and notification bodies so connections can be reused.
        Effect.flatMap((response) =>
          response.text.pipe(
            Effect.map((text) => ({ status: response.status, headers: response.headers, text })),
          ),
        ),
        Effect.flatMap((response) => {
          if (response.status >= 300 && response.status < 400)
            return Effect.fail(
              new OrchestratorMcpFailure({
                code: "orchestration_error",
                message:
                  "The linked environment redirected the request. Verify its address before retrying.",
              }),
            );
          if (response.status === 401)
            return Effect.fail(
              new OrchestratorMcpFailure({
                code: "capability_denied",
                message:
                  "The linked environment no longer accepts this link. It may have been revoked there; link it again.",
              }),
            );
          return Effect.succeed(response);
        }),
      );

  const openSession = (url: string, token: string, limitHeader: Record<string, string>) =>
    Effect.gen(function* () {
      const id = yield* nextId;
      const initialized = yield* post(
        url,
        token,
        {
          jsonrpc: "2.0",
          id,
          method: "initialize",
          params: {
            protocolVersion: PROTOCOL_VERSION,
            capabilities: {},
            clientInfo: { name: "t3code-peer", version: "1" },
          },
        },
        limitHeader,
      );
      const sessionId = initialized.headers["mcp-session-id"];
      if (initialized.status !== 200 || !sessionId)
        return yield* new OrchestratorMcpFailure({
          code: "orchestration_error",
          message: `The linked environment refused to start an MCP session (HTTP ${initialized.status}).`,
        });
      const envelope = yield* readEnvelope(initialized, id);
      yield* decodeInitialization(envelope.result).pipe(Effect.mapError(malformed));
      const notified = yield* post(
        url,
        token,
        { jsonrpc: "2.0", method: "notifications/initialized" },
        {
          ...limitHeader,
          "mcp-session-id": sessionId,
          "mcp-protocol-version": PROTOCOL_VERSION,
        },
      );
      if (notified.status !== 200 && notified.status !== 202)
        return yield* new OrchestratorMcpFailure({
          code: "orchestration_error",
          message: `The linked environment refused MCP initialization (HTTP ${notified.status}).`,
        });
      return { id: sessionId };
    });

  const call: PeerMcpClient["Service"]["call"] = (input) =>
    Effect.gen(function* () {
      const resolved = yield* links
        .resolve(input.environmentId)
        .pipe(Effect.mapError(fromLinkError));
      const limitHeader = {
        [McpInvocationContext.MODE_LIMIT_HEADER]: `${input.limits.runtimeMode}/${input.limits.interactionMode}`,
      };
      const sessionKey = encodeSessionKey([input.environmentId, resolved.url, resolved.token]);
      const existing = (yield* Ref.get(sessions)).get(sessionKey);
      const opened = existing ?? (yield* openSession(resolved.url, resolved.token, limitHeader));
      // Concurrent cold opens may finish together; keep the first published session.
      const session = yield* Ref.modify(sessions, (current) => {
        const cached = current.get(sessionKey);
        return cached === undefined
          ? [opened, new Map(current).set(sessionKey, opened)]
          : [cached, current];
      });
      const id = yield* nextId;
      const response = yield* post(
        resolved.url,
        resolved.token,
        {
          jsonrpc: "2.0",
          id,
          method: "tools/call",
          params: { name: input.tool, arguments: input.arguments },
        },
        { ...limitHeader, "mcp-session-id": session.id, "mcp-protocol-version": PROTOCOL_VERSION },
      );
      if (response.status === 404) {
        yield* Ref.update(sessions, (current) => {
          if (current.get(sessionKey) !== session) return current;
          const next = new Map(current);
          next.delete(sessionKey);
          return next;
        });
        // HTTP failure does not prove that a mutation was not committed remotely.
        return yield* new OrchestratorMcpFailure({
          code: "orchestration_error",
          message:
            "The linked environment lost the MCP session (HTTP 404). The call was not replayed. Reconcile its outcome before explicitly retrying with the same operation key.",
        });
      }
      if (response.status !== 200)
        return yield* new OrchestratorMcpFailure({
          code: "orchestration_error",
          message: `The linked environment answered HTTP ${response.status}.`,
        });
      const envelope = yield* readEnvelope(response, id);
      const result = yield* decodeToolResult(envelope.result).pipe(Effect.mapError(malformed));
      const text = result.content
        .filter((part) => part.type === "text")
        .map((part) => part.text ?? "")
        .join("");
      const hasStructured = Object.hasOwn(result, "structuredContent");
      if (result.isError === true) {
        // A refusal keeps the peer's own code, so the agent sees why it was refused.
        const refused = hasStructured
          ? decodeFailure(result.structuredContent)
          : decodeTextFailure(text);
        return yield* refused._tag === "Some"
          ? new OrchestratorMcpFailure(refused.value)
          : new OrchestratorMcpFailure({
              code: "orchestration_error",
              message: text || "The linked environment refused the call.",
            });
      }
      return yield* (
        hasStructured
          ? // oxlint-disable-next-line t3code/no-inline-schema-compile -- The caller supplies its result schema per invocation.
            Schema.decodeUnknownEffect(input.success)(result.structuredContent)
          : // oxlint-disable-next-line t3code/no-inline-schema-compile -- The caller supplies its result schema per invocation.
            Schema.decodeUnknownEffect(Schema.fromJsonString(input.success))(text)
      ).pipe(
        Effect.mapError(
          () =>
            new OrchestratorMcpFailure({
              code: "orchestration_error",
              message:
                "The linked environment's answer did not match this version. Update both environments.",
            }),
        ),
      );
    }).pipe(
      Effect.tapError((error) =>
        links.recordOutcome(input.environmentId, { error: error.message }),
      ),
      Effect.tap(() => links.recordOutcome(input.environmentId, { error: null })),
      // Recorded above with the transport's own message; the agent hears it plainly.
      Effect.catchTags({
        HttpClientError: () =>
          Effect.fail(
            new OrchestratorMcpFailure({
              code: "orchestration_error",
              message: "The linked environment stopped answering.",
            }),
          ),
      }),
    );
  return { call };
});
