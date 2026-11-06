// @effect-diagnostics nodeBuiltinImport:off -- Measures the parser of a cleanup-owned, ephemeral Node server.
import * as NodeHttp from "node:http";
import { describe, expect, it } from "@effect/vitest";
import {
  EnvironmentId,
  ProviderInstanceId,
  ThreadId,
  type LinkedRequestContext,
} from "@t3tools/contracts";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";
import * as Layer from "effect/Layer";

import type { McpInvocationScope } from "../../mcp/McpInvocationContext.ts";
import * as RemoteAdmissionIntake from "../remoteAdmissionIntegration/RemoteAdmissionIntake.ts";
import { LINKED_MESSAGE_ORIGIN_HEADER, encodeLinkedMessageOrigin } from "./LinkedMessageOrigin.ts";
import {
  LINKED_REQUEST_CONTEXT_HEADER,
  PEER_REQUEST_HEADER_BUDGET,
  REQUEST_CONTEXT_MAX_SENT_CHARS,
  type BoundLinkedRequestContext,
  encodeLinkedRequestContext,
  readBoundLinkedRequestContext,
  requestHeaderBytes,
} from "./LinkedRequestContext.ts";
import { LINKED_MCP_PROBE_TOOL, serveLinkedMcpProbe } from "./LinkedMcpProbe.testkit.ts";

const scope = (sessionId: string): McpInvocationScope => ({
  environmentId: EnvironmentId.make("receiver"),
  capabilities: new Set(["orchestration"]),
  issuedAt: 1,
  requestNamespace: `client:${sessionId}`,
  thread: undefined,
  client: { sessionId, label: `Synthetic ${sessionId}`, access: "auto", linked: true },
});
const base: LinkedRequestContext = {
  schemaVersion: "t3.linked-request-context/v1",
  sentAt: "2026-10-26T12:34:56.789Z",
};
const contextFor = (id: string): LinkedRequestContext => ({
  ...base,
  thread: { threadId: `thread-${id}`, title: `Thread ${id}` },
  parent: { threadId: `parent-${id}`, title: `Parent ${id}` },
  project: { projectId: `project-${id}`, title: `Project ${id}` },
});
const originFor = (id: string) => ({
  schemaVersion: "t3.linked-message-origin/v1" as const,
  environmentId: `sender-${id}`,
  thread: { threadId: `thread-${id}`, title: `Origin ${id}` },
});
const raw = (value: unknown) => Buffer.from(JSON.stringify(value), "utf8").toString("base64url");
const headersFor = (id: string) => ({
  [LINKED_MESSAGE_ORIGIN_HEADER]: encodeLinkedMessageOrigin(originFor(id)),
  [LINKED_REQUEST_CONTEXT_HEADER]: encodeLinkedRequestContext(contextFor(id)),
});
interface Observation {
  readonly operation: string;
  readonly params: Record<string, unknown>;
  readonly invocation: McpInvocationScope;
  readonly bound: BoundLinkedRequestContext | undefined;
}

/** Built before HTTP starts; only invoke's effect reads the per-request Reference. */
const recordingIntake = (
  seen: Array<Observation>,
  registered: Array<string>,
  beforeRead: (invocation: McpInvocationScope) => Effect.Effect<void> = () => Effect.void,
) =>
  Layer.mock(RemoteAdmissionIntake.RemoteAdmissionIntake)({
    register: (operation) => {
      registered.push(operation);
    },
    invoke: (operation, params, invocation) =>
      Effect.gen(function* () {
        yield* beforeRead(invocation);
        const bound = yield* readBoundLinkedRequestContext(invocation);
        seen.push({ operation, params, invocation, bound });
        return { ok: true };
      }),
  });

describe("real HTTP middleware → toolkit → admission propagation (K0-P)", () => {
  it.effect(
    "isolates concurrent A/B sessions inside invoke and clears both headers on session reuse",
    () =>
      Effect.scoped(
        Effect.gen(function* () {
          const bothInsideInvoke = yield* Deferred.make<void>();
          const release = yield* Deferred.make<void>();
          const arrived = new Set<string>();
          const seen: Array<Observation> = [];
          const registered: Array<string> = [];
          const intake = recordingIntake(seen, registered, (invocation) =>
            Effect.gen(function* () {
              if (arrived.size >= 2) return;
              arrived.add(invocation.client!.sessionId);
              if (arrived.size === 2) yield* Deferred.succeed(bothInsideInvoke, undefined);
              yield* Deferred.await(release);
            }),
          );
          const probe = yield* serveLinkedMcpProbe(intake, {
            "token-a": scope("a"),
            "token-b": scope("b"),
          });
          // Registration must traverse McpToolAccess.toLayer's real RemoteAdmissionMcp.wrap.
          expect(registered).toEqual([LINKED_MCP_PROBE_TOOL]);
          expect(seen).toEqual([]);
          const a = yield* probe.open("token-a");
          const b = yield* probe.open("token-b");
          const calls = yield* Effect.all(
            [
              probe.call(a, { headers: headersFor("a"), params: { marker: "a" } }),
              probe.call(b, { headers: headersFor("b"), params: { marker: "b" } }),
            ],
            { concurrency: "unbounded" },
          ).pipe(Effect.forkChild);
          yield* Deferred.await(bothInsideInvoke);
          expect(arrived).toEqual(new Set(["a", "b"]));
          expect(seen).toEqual([]);
          yield* Deferred.succeed(release, undefined);
          expect((yield* Fiber.join(calls)).map((response) => response.status)).toEqual([200, 200]);
          expect(seen).toHaveLength(2);
          for (const id of ["a", "b"]) {
            const observed = seen.find((value) => value.params.marker === id)!;
            expect(observed.operation).toBe(LINKED_MCP_PROBE_TOOL);
            expect(observed.invocation.client?.sessionId).toBe(id);
            expect(observed.bound).toEqual({ context: contextFor(id), linkSessionId: id });
            expect(observed.invocation.client?.remoteOrigin).toEqual({
              ...originFor(id),
              linkSessionId: id,
            });
          }
          expect((yield* probe.call(a)).status).toBe(200);
          expect(seen).toHaveLength(3);
          expect(seen[2]?.bound).toBeUndefined();
          expect(seen[2]?.invocation.client?.remoteOrigin).toBeUndefined();
          expect(seen[2]?.invocation.client?.sessionId).toBe("a");
        }),
      ),
  );

  it.effect(
    "drops malformed advisory headers with 200 but keeps strict origin and mode-limit rejection",
    () =>
      Effect.scoped(
        Effect.gen(function* () {
          const seen: Array<Observation> = [];
          const probe = yield* serveLinkedMcpProbe(recordingIntake(seen, []), { peer: scope("a") });
          const session = yield* probe.open("peer");
          for (const header of [
            "!bad!",
            "e31",
            `${raw(base)}=`,
            `${raw(base)},${raw(base)}`,
            Buffer.from([0xff]).toString("base64url"),
            Buffer.from("not JSON").toString("base64url"),
            raw({ ...base, schemaVersion: "t3.linked-request-context/v2" }),
            raw({ ...base, thread: { threadId: "\u202eforged" } }),
            raw({ ...base, sentAt: "not-a-time" }),
            "a".repeat(4097),
          ]) {
            const before = seen.length;
            const response = yield* probe.call(session, {
              headers: { [LINKED_REQUEST_CONTEXT_HEADER]: header },
            });
            expect(response.status).toBe(200);
            expect(seen).toHaveLength(before + 1);
            expect(seen.at(-1)?.bound).toBeUndefined();
          }
          const before = seen.length;
          const invalidOrigin = yield* probe.call(session, {
            headers: {
              [LINKED_MESSAGE_ORIGIN_HEADER]: "!bad!",
              [LINKED_REQUEST_CONTEXT_HEADER]: encodeLinkedRequestContext(contextFor("a")),
            },
          });
          expect(invalidOrigin.status).toBe(400);
          expect(JSON.parse(invalidOrigin.text)).toMatchObject({
            error: "invalid_linked_message_origin",
          });
          const invalidMode = yield* probe.call(session, {
            headers: {
              ...headersFor("a"),
              "t3-mode-limit": "not/a/mode",
            },
          });
          expect(invalidMode.status).toBe(400);
          expect(JSON.parse(invalidMode.text)).toMatchObject({ error: "invalid_mode_limit" });
          expect(seen).toHaveLength(before);
        }),
      ),
  );

  it.effect(
    "ignores ordinary and thread-scoped clients, checks origin matches, and strips only bad icons",
    () =>
      Effect.scoped(
        Effect.gen(function* () {
          const seen: Array<Observation> = [];
          const plain = scope("plain");
          const threaded = scope("threaded");
          const probe = yield* serveLinkedMcpProbe(recordingIntake(seen, []), {
            peer: scope("a"),
            plain: { ...plain, client: { ...plain.client!, linked: false } },
            threaded: {
              ...threaded,
              thread: {
                threadId: ThreadId.make("local"),
                providerSessionId: "provider",
                providerInstanceId: ProviderInstanceId.make("codex"),
              },
            },
          });
          for (const token of ["plain", "threaded"]) {
            const session = yield* probe.open(token);
            const before = seen.length;
            expect(
              (yield* probe.call(session, {
                headers: {
                  [LINKED_REQUEST_CONTEXT_HEADER]: encodeLinkedRequestContext(contextFor("a")),
                },
              })).status,
            ).toBe(200);
            expect(seen).toHaveLength(before + 1);
            expect(seen.at(-1)?.bound).toBeUndefined();
          }
          const peer = yield* probe.open("peer");
          expect(
            (yield* probe.call(peer, {
              headers: {
                ...headersFor("a"),
                [LINKED_REQUEST_CONTEXT_HEADER]: encodeLinkedRequestContext(contextFor("b")),
              },
            })).status,
          ).toBe(200);
          expect(seen.at(-1)?.bound).toBeUndefined();
          expect(seen.at(-1)?.invocation.client?.remoteOrigin?.thread?.threadId).toBe("thread-a");
          expect((yield* probe.call(peer, { headers: headersFor("a") })).status).toBe(200);
          expect(seen.at(-1)?.bound).toEqual({ context: contextFor("a"), linkSessionId: "a" });
          expect(
            (yield* probe.call(peer, {
              headers: {
                ...headersFor("a"),
                [LINKED_REQUEST_CONTEXT_HEADER]: encodeLinkedRequestContext(base),
              },
            })).status,
          ).toBe(200);
          expect(seen.at(-1)?.bound).toEqual({ context: base, linkSessionId: "a" });
          const context = contextFor("a");
          expect(
            (yield* probe.call(peer, {
              headers: {
                [LINKED_REQUEST_CONTEXT_HEADER]: raw({
                  ...context,
                  project: { ...context.project, icon: { kind: "emoji", emoji: 123 } },
                }),
              },
            })).status,
          ).toBe(200);
          expect(seen.at(-1)?.bound?.context).toEqual(context);
          expect(
            (yield* probe.call(peer, {
              headers: {
                [LINKED_REQUEST_CONTEXT_HEADER]: raw({
                  ...context,
                  project: { ...context.project, icon: { kind: "future-kind" } },
                }),
              },
            })).status,
          ).toBe(200);
          expect(seen.at(-1)?.bound?.context).toEqual({
            ...context,
            project: { ...context.project, icon: null },
          });
        }),
      ),
  );
});

/** Native request uses no global agent and destroys its owned socket on completion or cancellation. */
const nativeCall = (url: string, headers: Record<string, string>, body: string) =>
  Effect.promise((signal) => {
    let request: NodeHttp.ClientRequest | undefined;
    return new Promise<{ readonly status: number } | { readonly dropped: true }>(
      (resolve, reject) => {
        request = NodeHttp.request(
          url,
          { method: "POST", headers, signal, agent: false },
          (response) => {
            response.once("error", reject);
            response.once("end", () => resolve({ status: response.statusCode ?? 0 }));
            response.resume();
          },
        );
        request.once("error", (error) => {
          if ("code" in error && error.code === "ECONNRESET") resolve({ dropped: true });
          else reject(error);
        });
        request.setTimeout(3000, () =>
          request?.destroy(new Error("Probe request deadline exceeded")),
        );
        request.end(body);
      },
    ).finally(() => request?.destroy());
  });

describe("Node receiver header qualification (K0-Q; no proxy compatibility claim)", () => {
  it.effect(
    "admits an estimated 8192-byte head and rejects a control beyond the actual parser limit",
    () =>
      Effect.scoped(
        Effect.gen(function* () {
          expect(NodeHttp.maxHeaderSize).toBeGreaterThanOrEqual(2 * PEER_REQUEST_HEADER_BUDGET);
          const seen: Array<Observation> = [];
          const probe = yield* serveLinkedMcpProbe(recordingIntake(seen, []), { peer: scope("a") });
          const session = yield* probe.open("peer");
          const context = contextFor("a");
          const unpadded = { ...context, padding: "" };
          const paddedContext = raw({
            ...unpadded,
            padding: "p".repeat(1536 - Buffer.byteLength(JSON.stringify(unpadded), "utf8")),
          });
          expect(paddedContext).toHaveLength(REQUEST_CONTEXT_MAX_SENT_CHARS);
          const headers = probe.callHeaders(session, {
            ...headersFor("a"),
            [LINKED_REQUEST_CONTEXT_HEADER]: paddedContext,
            "t3-mode-limit": "auto/default",
            "x-probe-padding": "",
          });
          headers["x-probe-padding"] = "p".repeat(
            PEER_REQUEST_HEADER_BUDGET - requestHeaderBytes(headers),
          );
          expect(requestHeaderBytes(headers)).toBeGreaterThanOrEqual(8192);
          const accepted = yield* nativeCall(probe.url, headers, probe.callBody());
          expect(accepted).toEqual({ status: 200 });
          expect(seen).toHaveLength(1);
          expect(seen[0]?.bound).toEqual({ context, linkSessionId: "a" });
          expect(seen[0]?.invocation.client?.remoteOrigin).toEqual({
            ...originFor("a"),
            linkSessionId: "a",
          });
          const oversized = {
            ...headers,
            "x-probe-padding": "p".repeat(NodeHttp.maxHeaderSize + 1024),
          };
          expect(requestHeaderBytes(oversized)).toBeGreaterThan(NodeHttp.maxHeaderSize);
          const rejected = yield* nativeCall(probe.url, oversized, probe.callBody());
          expect("status" in rejected ? rejected.status === 431 : rejected.dropped).toBe(true);
          expect(seen).toHaveLength(1);
        }),
      ),
  );
});
