import { NodeHttpServer } from "@effect/platform-node";
import { expect, it } from "@effect/vitest";
import {
  EnvironmentId,
  OrchestrationV2LinkedSender,
  OrchestratorMcpFailure,
  ThreadId,
} from "@t3tools/contracts";
import * as Context from "effect/Context";
import * as DateTime from "effect/DateTime";
import * as Deferred from "effect/Deferred";
import * as Duration from "effect/Duration";
import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";
import * as Layer from "effect/Layer";
import * as Queue from "effect/Queue";
import * as Ref from "effect/Ref";
import * as Schema from "effect/Schema";
import { Tool, Toolkit } from "effect/ai";
import * as TestClock from "effect/testing/TestClock";
import { HttpClient, HttpClientResponse } from "effect/http";

import * as McpHttpServer from "../mcp/McpHttpServer.ts";
import * as McpInvocationContext from "../mcp/McpInvocationContext.ts";
import * as McpToolAccess from "../mcp/McpToolAccess.ts";
import * as PeerLinks from "./PeerLinks.ts";
import * as PeerMcpClient from "./PeerMcpClient.ts";
import { descriptorOf, layerLinkingEnvironment, linkTo, servePeer } from "./PeerLinks.testkit.ts";

const Result = Schema.Struct({ modes: Schema.String });
const toolkit = Toolkit.make(
  Tool.make("probe", {
    description: "Returns this request's mode limits.",
    success: Result,
    dependencies: [McpInvocationContext.McpInvocationContext],
  }),
  Tool.make("hold", {
    description: "Holds a tool request open.",
    success: Result,
    dependencies: [McpInvocationContext.McpInvocationContext],
  }),
  Tool.make("refuse", {
    description: "Refuses the call.",
    success: Result,
    failure: OrchestratorMcpFailure,
    failureMode: "return",
  }),
);

/** One `probe` call to a linked peer at a remote address, answered by `http`. */
const probeThrough = Effect.fn(function* (http: HttpClient.HttpClient) {
  const box = descriptorOf("environment-box", "Box");
  const context = yield* Layer.build(
    PeerMcpClient.layer.pipe(
      Layer.provide(
        Layer.mergeAll(
          Layer.succeed(HttpClient.HttpClient, http),
          Layer.mock(PeerLinks.PeerLinks)({
            resolve: () =>
              Effect.succeed({
                link: {
                  environmentId: box.environmentId,
                  label: box.label,
                  urls: ["https://peer.example"],
                  access: "auto",
                  linkedAt: DateTime.makeUnsafe(0),
                  expiresAt: DateTime.makeUnsafe(1),
                  lastReachedAt: null,
                  lastError: null,
                },
                url: "https://peer.example",
                token: "fixture",
                generation: "fixture-generation",
                capabilities: box.capabilities,
              }),
            // Every call checks that the link it resolved is still the current one.
            currentGeneration: () => Effect.succeedSome("fixture-generation"),
            recordOutcome: () => Effect.void,
          }),
        ),
      ),
    ),
  );
  return Context.get(context, PeerMcpClient.PeerMcpClient).call({
    environmentId: box.environmentId,
    tool: "probe",
    arguments: {},
    limits: { runtimeMode: "approval-required", interactionMode: "plan" },
    success: Result,
  });
});

/** The JSON-RPC answer to request `id`: an initialized session, or the probe's result. */
const answerTo = (id: number, method: "initialize" | "tools/call") =>
  JSON.stringify({
    jsonrpc: "2.0",
    id,
    result:
      method === "initialize"
        ? {
            protocolVersion: "2025-06-18",
            capabilities: {},
            serverInfo: { name: "fixture", version: "1" },
          }
        : { content: [{ type: "text", text: '{"modes":"approval-required/plan"}' }] },
  });

const refusal = new OrchestratorMcpFailure({
  code: "thread_not_found",
  message: "Thread thread-gone was not found.",
});

const senderToolkit = Toolkit.make(
  Tool.make("sender", {
    description: "Returns the thread this request is made for.",
    success: Schema.Struct({ senderThread: Schema.NullOr(OrchestrationV2LinkedSender) }),
    dependencies: [McpInvocationContext.McpInvocationContext],
  }),
);

it.effect("tells the peer which thread a call is made for", () =>
  Effect.scoped(
    Effect.gen(function* () {
      const handlers = McpToolAccess.toLayer(senderToolkit, {
        sender: McpToolAccess.reads(() =>
          McpInvocationContext.McpInvocationContext.pipe(
            Effect.map((scope) => ({ senderThread: scope.client?.senderThread ?? null })),
          ),
        ),
      });
      const box = descriptorOf("environment-box", "Box");
      const b = yield* servePeer(box, McpHttpServer.toolkitRegistration(senderToolkit, handlers));
      const context = yield* Layer.build(
        layerLinkingEnvironment(descriptorOf("environment-a", "A")),
      );
      const links = Context.get(context, PeerLinks.PeerLinks);
      const client = Context.get(context, PeerMcpClient.PeerMcpClient);
      yield* linkTo(links, b);
      // A thread id may hold any character; it arrives as sent.
      const sender = {
        environmentId: EnvironmentId.make("environment-a"),
        threadId: ThreadId.make("import:claude/orc lead"),
        title: "orc lead / opus → fable",
      };
      const call = (input: { readonly sender?: typeof sender }) =>
        client.call({
          environmentId: box.environmentId,
          tool: "sender",
          arguments: {},
          limits: { runtimeMode: "full-access", interactionMode: "default" },
          ...input,
          success: Schema.Struct({ senderThread: Schema.NullOr(OrchestrationV2LinkedSender) }),
        });
      expect(yield* call({ sender })).toEqual({ senderThread: sender });
      expect(yield* call({})).toEqual({ senderThread: null });
      // A long title is cut to whole characters rather than breaking the call.
      const long = yield* call({ sender: { ...sender, title: "🧵".repeat(5000) } });
      expect(long.senderThread?.title).toBe("🧵".repeat(200));
      // A title holding an unpaired surrogate still lets the call through.
      const broken = yield* call({ sender: { ...sender, title: "Review \ud800" } });
      expect(broken.senderThread?.title).toBe("Review \ufffd");
    }),
  ).pipe(Effect.provide(NodeHttpServer.layerTest)),
);

it.effect.each([false, true])(
  "reuses an initialized session before its first call finishes (cancelled: %s)",
  (cancelFirst) =>
    Effect.scoped(
      Effect.gen(function* () {
        const entered = yield* Deferred.make<void>();
        const release = yield* Deferred.make<void>();
        const modes = McpInvocationContext.McpInvocationContext.pipe(
          Effect.map((scope) => {
            const ceiling = McpInvocationContext.clientModeCeiling(scope.client);
            return { modes: `${ceiling.runtimeMode}/${ceiling.interactionMode}` };
          }),
        );
        const handlers = McpToolAccess.toLayer(toolkit, {
          probe: McpToolAccess.reads(() => modes),
          refuse: McpToolAccess.reads(() => Effect.fail(refusal)),
          hold: McpToolAccess.reads(() =>
            Deferred.succeed(entered, undefined).pipe(
              Effect.andThen(Deferred.await(release)),
              Effect.andThen(modes),
            ),
          ),
        });
        const box = descriptorOf("environment-box", "Box");
        const b = yield* servePeer(box, McpHttpServer.toolkitRegistration(toolkit, handlers));
        const context = yield* Layer.build(
          layerLinkingEnvironment(descriptorOf("environment-a", "A")),
        );
        const links = Context.get(context, PeerLinks.PeerLinks);
        const client = Context.get(context, PeerMcpClient.PeerMcpClient);
        yield* linkTo(links, b);
        const held = yield* client
          .call({
            environmentId: box.environmentId,
            tool: "hold",
            arguments: {},
            limits: { runtimeMode: "full-access", interactionMode: "default" },
            success: Result,
          })
          .pipe(Effect.forkScoped);
        yield* Deferred.await(entered);
        if (cancelFirst) yield* Fiber.interrupt(held);
        const before = (yield* Ref.get(b.bearers)).length;
        expect(
          yield* client.call({
            environmentId: box.environmentId,
            tool: "probe",
            arguments: {},
            limits: { runtimeMode: "approval-required", interactionMode: "plan" },
            success: Result,
          }),
        ).toEqual({ modes: "approval-required/plan" });
        // One tool POST; no second initialize or initialized notification.
        expect((yield* Ref.get(b.bearers)).length - before).toBe(1);
        yield* Deferred.succeed(release, undefined);
        if (!cancelFirst) expect(yield* Fiber.join(held)).toEqual({ modes: "auto/default" });
      }),
    ).pipe(Effect.provide(NodeHttpServer.layerTest)),
);

it.effect("keeps a link healthy when the peer refuses a call", () =>
  Effect.scoped(
    Effect.gen(function* () {
      const handlers = McpToolAccess.toLayer(toolkit, {
        probe: McpToolAccess.reads(() => Effect.succeed({ modes: "" })),
        hold: McpToolAccess.reads(() => Effect.succeed({ modes: "" })),
        refuse: McpToolAccess.reads(() => Effect.fail(refusal)),
      });
      const box = descriptorOf("environment-box", "Box");
      const b = yield* servePeer(box, McpHttpServer.toolkitRegistration(toolkit, handlers));
      const context = yield* Layer.build(
        layerLinkingEnvironment(descriptorOf("environment-a", "A")),
      );
      const links = Context.get(context, PeerLinks.PeerLinks);
      const client = Context.get(context, PeerMcpClient.PeerMcpClient);
      yield* linkTo(links, b);
      const { generation } = yield* links.resolve(box.environmentId);
      yield* links.recordOutcome(box.environmentId, generation, { error: "earlier outage" });
      const refused = yield* client
        .call({
          environmentId: box.environmentId,
          tool: "refuse",
          arguments: {},
          limits: { runtimeMode: "full-access", interactionMode: "default" },
          success: Result,
        })
        .pipe(Effect.flip);
      // The agent hears the peer's own refusal; the link answered, so it is not failing.
      expect(refused).toMatchObject({ code: refusal.code, message: refusal.message });
      const link = yield* links.get(box.environmentId);
      expect(link._tag === "Some" ? link.value.lastError : "missing").toBeNull();
    }),
  ).pipe(Effect.provide(NodeHttpServer.layerTest)),
);

it.effect(
  "does not replay a possibly delivered call after 404 and renews only for a subsequent request",
  () =>
    Effect.gen(function* () {
      const sessionHeaders: Array<string | undefined> = [];
      const statuses = [200, 202, 404, 200, 202, 200, 200, 200];
      const outcomes: Array<{ readonly generation: string; readonly error: string | null }> = [];
      const http = HttpClient.make((request) =>
        Effect.sync(() => {
          const index = sessionHeaders.length;
          sessionHeaders.push(request.headers["mcp-session-id"]);
          const status = statuses[index];
          if (status === undefined) throw new Error("Unexpected retry");
          const initialization = index === 0 || index === 3;
          const body =
            status === 202 || status === 404
              ? ""
              : index === 7
                ? "{}"
                : JSON.stringify({
                    jsonrpc: "2.0",
                    id: initialization ? (index === 0 ? 0 : 2) : index === 5 ? 3 : 4,
                    result: initialization
                      ? {
                          protocolVersion: "2025-06-18",
                          capabilities: {},
                          serverInfo: { name: "fixture", version: "1" },
                        }
                      : { content: [{ type: "text", text: '{"modes":"approval-required/plan"}' }] },
                  });
          return HttpClientResponse.fromWeb(
            request,
            new Response(body, {
              status,
              headers:
                index === 0
                  ? {
                      "mcp-session-id": "old",
                      [McpInvocationContext.LINK_EXPIRES_AT_HEADER]: "1970-01-02T00:00:00.000Z",
                    }
                  : index === 3
                    ? { "mcp-session-id": "renewed" }
                    : {},
            }),
          );
        }),
      );
      const box = descriptorOf("environment-box", "Box");
      const context = yield* Layer.build(
        PeerMcpClient.layer.pipe(
          Layer.provide(
            Layer.mergeAll(
              Layer.succeed(HttpClient.HttpClient, http),
              Layer.mock(PeerLinks.PeerLinks)({
                resolve: () =>
                  Effect.succeed({
                    link: {
                      environmentId: box.environmentId,
                      label: box.label,
                      urls: ["https://peer.example"],
                      access: "auto",
                      linkedAt: DateTime.makeUnsafe(0),
                      expiresAt: DateTime.makeUnsafe(1),
                      lastReachedAt: null,
                      lastError: null,
                    },
                    url: "https://peer.example",
                    token: "fixture",
                    generation: "fixture-generation",
                    capabilities: box.capabilities,
                  }),
                currentGeneration: () => Effect.succeedSome("fixture-generation"),
                recordOutcome: (_environmentId, generation, outcome) =>
                  Effect.sync(() => {
                    outcomes.push({ generation, error: outcome.error });
                  }),
              }),
            ),
          ),
        ),
      );
      const client = Context.get(context, PeerMcpClient.PeerMcpClient);
      const failed = yield* client
        .call({
          environmentId: box.environmentId,
          tool: "probe",
          arguments: {},
          limits: { runtimeMode: "approval-required", interactionMode: "plan" },
          success: Result,
        })
        .pipe(Effect.flip);
      expect(failed.code).toBe("orchestration_error");
      expect(failed.message).toContain("not replayed");
      expect(failed.message).toContain("Reconcile its outcome");
      // Initialization, notification, and exactly one possibly applied tools/call POST.
      expect(sessionHeaders).toEqual([undefined, "old", "old"]);
      for (let call = 0; call < 2; call++) {
        expect(
          yield* client.call({
            environmentId: box.environmentId,
            tool: "probe",
            arguments: {},
            limits: { runtimeMode: "approval-required", interactionMode: "plan" },
            success: Result,
          }),
        ).toEqual({ modes: "approval-required/plan" });
      }
      // An answer with neither a result nor an error is the link's problem, not the call's.
      const empty = yield* client
        .call({
          environmentId: box.environmentId,
          tool: "probe",
          arguments: {},
          limits: { runtimeMode: "approval-required", interactionMode: "plan" },
          success: Result,
        })
        .pipe(Effect.flip);
      expect(empty.message).toBe("The linked environment answered in an unexpected shape.");
      // The lost session and the empty answer mark the link failing for the generation the
      // call resolved; each answered call clears it.
      expect(outcomes).toEqual([
        { generation: "fixture-generation", error: failed.message },
        { generation: "fixture-generation", error: null },
        { generation: "fixture-generation", error: null },
        { generation: "fixture-generation", error: empty.message },
      ]);
      expect(sessionHeaders).toEqual([
        undefined,
        "old",
        "old",
        undefined,
        "renewed",
        "renewed",
        "renewed",
        "renewed",
      ]);
    }).pipe(Effect.scoped),
);

it.effect("bounds the session handshake, not a tool call that waits longer", () =>
  Effect.gen(function* () {
    const arrived = yield* Queue.unbounded<number>();
    const answer = yield* Deferred.make<void>();
    let requests = 0;
    const http = HttpClient.make((request) =>
      Effect.gen(function* () {
        const index = requests++;
        yield* Queue.offer(arrived, index);
        // The first initialize, then the next initialized notification, never answer;
        // the third session's tool call answers late.
        if (index === 0 || index === 2) return yield* Effect.never;
        if (index === 5) yield* Deferred.await(answer);
        // Each request with an id gets one from the client's counter: the first two
        // sessions' initializes take 0 and 1, the third's 2, and its tool call 3.
        const body =
          index === 1
            ? answerTo(1, "initialize")
            : index === 3
              ? answerTo(2, "initialize")
              : index === 5
                ? answerTo(3, "tools/call")
                : "";
        return HttpClientResponse.fromWeb(
          request,
          new Response(body, {
            status: index === 4 ? 202 : 200,
            headers: { "mcp-session-id": "session" },
          }),
        );
      }),
    );
    const probe = yield* probeThrough(http);

    for (const requestsBeforeStall of [1, 2]) {
      const stalled = yield* probe.pipe(Effect.flip, Effect.forkScoped);
      yield* Queue.takeN(arrived, requestsBeforeStall);
      yield* TestClock.adjust(Duration.seconds(10));
      expect(yield* Fiber.join(stalled)).toMatchObject({
        code: "orchestration_error",
        message: "The linked environment stopped answering.",
      });
    }

    const waiting = yield* Effect.forkScoped(probe);
    yield* Queue.takeN(arrived, 3);
    // Five times the handshake bound. A tool call keeps the transport's 60-second
    // per-request deadline (PeerMcpSessions.test.ts), so the wait stays inside it, as the
    // longest supported remote wait does.
    yield* TestClock.adjust(Duration.seconds(50));
    yield* Deferred.succeed(answer, undefined);
    expect(yield* Fiber.join(waiting)).toEqual({ modes: "approval-required/plan" });
  }),
);
