// @effect-diagnostics nodeBuiltinImport:off -- The opt-in simulator owns and cleans its Chromium profile.
import * as NodeFSP from "node:fs/promises";
import * as NodePath from "node:path";
import * as NodeHttpPlatform from "@effect/platform-node/NodeHttpPlatform";
import * as NodeHttpServer from "@effect/platform-node/NodeHttpServer";
import * as NodeServices from "@effect/platform-node/NodeServices";
import { expect, it } from "@effect/vitest";
import { vi } from "vite-plus/test";
import {
  AuthOrchestrationOperateScope,
  AuthOrchestrationReadScope,
  AuthPreviewOperateScope,
  AuthSessionId,
  EnvironmentId,
  ProviderInstanceId,
  ThreadId,
  type CompanionDown,
  type CompanionUp,
  type PreviewAutomationSnapshot,
  type PreviewAutomationStatus,
} from "@t3tools/contracts";
import * as Context from "effect/Context";
import * as Clock from "effect/Clock";
import * as HttpClient from "effect/http/HttpClient";
import * as HttpBody from "effect/http/HttpBody";
import * as FetchHttpClient from "effect/http/FetchHttpClient";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Fiber from "effect/Fiber";
import * as Queue from "effect/Queue";
import * as Schema from "effect/Schema";
import { HttpRouter, HttpServer, HttpServerResponse } from "effect/http";
import { chromium, type CDPSession, type Page } from "playwright-core";
import {
  createCdpRelayConnection,
  type CdpRelayConnection,
} from "../../../../desktop/src/preview/CdpRelay.ts";
import * as ServerConfig from "../../config.ts";
import * as ServerEnvironment from "../../environment/ServerEnvironment.ts";
import * as EnvironmentAuth from "../../auth/EnvironmentAuth.ts";
import * as Broker from "../../mcp/PreviewAutomationBroker.ts";
import * as DesktopChannel from "../../preview/DesktopBrowserChannel.ts";
import * as Manager from "../../preview/Manager.ts";
import * as PreviewBrowser from "../../preview/PreviewBrowser.ts";
import * as ServerBrowser from "../../preview/ServerBrowser.ts";
import { ServerBrowserContexts } from "../../preview/ServerBrowserContexts.ts";
import * as PageOperations from "../../preview/ServerBrowserPage.ts";
import * as CompanionRegistration from "./registration.ts";
import * as CompanionHttp from "./http.ts";
import * as Registry from "./CompanionHostRegistry.ts";
import { CompanionFrameDecoder, encodeFrames } from "./framing.ts";

class SimulatorFixtureError extends Schema.TaggedError<SimulatorFixtureError>()(
  "SimulatorFixtureError",
  { cause: Schema.Defect() },
) {
  override get message() {
    return "The companion simulator did not satisfy its expected behavior.";
  }
}

const executable = process.env.JONES_PREVIEW_COMPANION_CHROMIUM;
const scratch = process.env.JONES_PREVIEW_COMPANION_SCRATCH;
const identity = {
  schemaVersion: 1,
  runtimeKind: "electron",
  runtimeInstanceId: "node-simulator",
  appVersion: "test",
  buildCommit: null,
} as const;
const scope = {
  environmentId: EnvironmentId.make("companion-simulator"),
  thread: {
    threadId: ThreadId.make("simulator-thread"),
    providerSessionId: "simulator-agent",
    providerInstanceId: ProviderInstanceId.make("codex"),
  },
  client: undefined,
  requestNamespace: "companion-simulator",
  capabilities: new Set(["preview"] as const),
  issuedAt: 1,
};
const cdpEvents = [
  "Page.frameAttached",
  "Page.frameDetached",
  "Page.frameNavigated",
  "Page.navigatedWithinDocument",
  "Page.lifecycleEvent",
  "Page.frameStoppedLoading",
  "Page.domContentEventFired",
  "Page.loadEventFired",
  "Page.javascriptDialogOpening",
  "Page.javascriptDialogClosed",
  "Page.screencastFrame",
  "Runtime.executionContextCreated",
  "Runtime.executionContextDestroyed",
  "Runtime.executionContextsCleared",
  "Runtime.consoleAPICalled",
  "Runtime.exceptionThrown",
  "Runtime.bindingCalled",
  "Network.requestWillBeSent",
  "Network.requestWillBeSentExtraInfo",
  "Network.responseReceived",
  "Network.responseReceivedExtraInfo",
  "Network.loadingFinished",
  "Network.loadingFailed",
  "Target.attachedToTarget",
  "Target.detachedFromTarget",
] as const;

it.live.skipIf(!executable || !scratch)(
  "mounts through the authenticated uplink and real CDP relay, drives and streams the companion page",
  () =>
    Effect.gen(function* () {
      const clock = yield* Clock.Clock;
      const directory = yield* Effect.acquireRelease(
        Effect.promise(() => NodeFSP.mkdtemp(NodePath.join(scratch!, "companion-simulator-"))),
        (directory) =>
          Effect.promise(() => NodeFSP.rm(directory, { recursive: true, force: true })),
      );
      const context = yield* Effect.acquireRelease(
        Effect.promise(() =>
          chromium.launchPersistentContext(NodePath.join(directory, "profile"), {
            executablePath: executable!,
            headless: true,
            chromiumSandbox: true,
            viewport: { width: 800, height: 600 },
          }),
        ),
        (context) => Effect.promise(() => context.close()),
      );
      const fallback = vi.spyOn(ServerBrowserContexts.prototype, "contextFor");
      const encoder = vi.spyOn(ServerBrowserContexts.prototype, "scratchPage");
      const clear = vi.spyOn(ServerBrowserContexts.prototype, "clearProfile");
      const upload = vi.spyOn(PageOperations, "setInputFiles");
      yield* Effect.addFinalizer(() =>
        Effect.sync(() => {
          fallback.mockRestore();
          encoder.mockRestore();
          clear.mockRestore();
          upload.mockRestore();
        }),
      );
      const common = Layer.mergeAll(
        Broker.layer,
        Manager.layer,
        Layer.succeed(ServerEnvironment.ServerEnvironment, {
          getEnvironmentId: Effect.succeed(scope.environmentId),
          getDescriptor: Effect.die("unused"),
        }),
        Layer.succeed(PreviewBrowser.PreviewBrowser, {
          executable: Effect.die("companion must never launch a server browser"),
          installed: Effect.die("unused"),
        }),
        Layer.mock(EnvironmentAuth.EnvironmentAuth, {
          authenticateHttpRequest: () =>
            Effect.succeed({
              sessionId: AuthSessionId.make("simulator"),
              subject: "human",
              method: "bearer-access-token",
              scopes: [
                AuthPreviewOperateScope,
                AuthOrchestrationReadScope,
                AuthOrchestrationOperateScope,
              ],
            }),
          authenticateWebSocketUpgrade: () =>
            Effect.succeed({
              sessionId: AuthSessionId.make("simulator"),
              subject: "human",
              method: "bearer-access-token",
              scopes: [
                AuthPreviewOperateScope,
                AuthOrchestrationReadScope,
                AuthOrchestrationOperateScope,
              ],
            }),
        }),
      ).pipe(
        Layer.provideMerge(ServerConfig.layerTest(process.cwd(), directory)),
        Layer.provideMerge(NodeServices.layer),
      );
      const services = yield* Layer.build(
        ServerBrowser.layer.pipe(
          Layer.provideMerge(CompanionRegistration.servicesLayer),
          Layer.provideMerge(DesktopChannel.layer),
          Layer.provideMerge(common),
        ),
      );
      const serverServices = yield* Layer.build(
        HttpRouter.serve(
          Layer.mergeAll(
            CompanionRegistration.routeLayer,
            CompanionHttp.routeLayer,
            HttpRouter.use((router) =>
              router.add(
                "GET",
                "/simulator-page",
                Effect.succeed(
                  HttpServerResponse.html("<input autofocus><p>companion evidence</p>"),
                ),
              ),
            ),
          ).pipe(
            Layer.provide(Layer.succeedContext(services)),
            Layer.provide(NodeHttpPlatform.layer.pipe(Layer.provide(NodeServices.layer))),
          ),
          { disableListenLog: true },
        ).pipe(Layer.provideMerge(NodeHttpServer.layerTest)),
      );
      const origin = HttpServer.formatAddress(
        Context.get(serverServices, HttpServer.HttpServer).address,
      );
      const socket = yield* Effect.acquireRelease(
        Effect.sync(
          () =>
            new WebSocket(
              origin.replace(/^http/, "ws") + "/api/jones/preview-companion/ws?wsTicket=synthetic",
            ),
        ),
        (socket) => Effect.sync(() => socket.close()),
      );
      const welcome = Promise.withResolvers<void>();
      const errors = Promise.withResolvers<never>();
      const actionDispatched = Promise.withResolvers<void>();
      const heldExpression = "new Promise(() => {}) /* companion-loss */";
      const decoder = new CompanionFrameDecoder();
      let sequence = 0;
      const send = (message: CompanionUp) => {
        for (const frame of encodeFrames(message, String(++sequence))) socket.send(frame);
      };
      const pages = new Map<string, { page: Page; cdp: CDPSession; relay: CdpRelayConnection }>();
      const mounting = new Map<string, Promise<void>>();
      const mount = ({ threadId, tabId }: { threadId: string; tabId: string }): Promise<void> => {
        const key = { threadId, tabId };
        const existing = mounting.get(key.tabId);
        if (existing) return existing;
        const work = (async () => {
          if (pages.has(key.tabId)) return;
          const page = await context.newPage();
          const cdp = await context.newCDPSession(page);
          const info = await cdp.send("Target.getTargetInfo");
          const relay = createCdpRelayConnection(
            {
              send: (method, params, child) => {
                if (method === "Runtime.evaluate" && params.expression === heldExpression)
                  actionDispatched.resolve();
                return child
                  ? Promise.reject(
                      new SimulatorFixtureError({
                        cause: "Unexpected child session in simple simulator fixture",
                      }),
                    )
                  : cdp.send(method as Parameters<CDPSession["send"]>[0], params);
              },
              targetId: async () => info.targetInfo.targetId,
              url: () => page.url(),
              title: () => "simulator",
              userAgent: () => "Chrome/130.0.0.0",
              setDownloadDirectory: () => {},
            },
            (message) => send({ type: "browser", event: { type: "cdp", ...key, message } }),
          );
          for (const name of cdpEvents)
            cdp.on(name, (params) => relay.event(name, params, undefined));
          pages.set(key.tabId, { page, cdp, relay });
          send({ type: "browser", event: { type: "attached", ...key, runtimeIdentity: identity } });
        })();
        mounting.set(key.tabId, work);
        return work;
      };
      socket.addEventListener("open", () =>
        send({
          type: "hello",
          protocol: 1,
          hostId: "simulator",
          label: "Simulator",
          platform: "node-test",
          runtimeIdentity: identity,
          capabilities: {
            cdp: true,
            clipboardText: true,
            uploads: false,
            downloads: false,
            recording: false,
          },
        }),
      );
      socket.addEventListener("error", () =>
        errors.reject(new SimulatorFixtureError({ cause: "Simulator uplink failed" })),
      );
      socket.addEventListener("message", (event) => {
        try {
          const message = decoder.accept(String(event.data)) as CompanionDown | undefined;
          if (!message) return;
          if (message.type === "welcome") welcome.resolve();
          if (message.type === "heartbeat")
            send({ type: "heartbeat", sentAt: clock.currentTimeMillisUnsafe() });
          if (message.type === "mount") void mount(message).catch(errors.reject);
          if (message.type === "browser" && message.command.type === "cdp")
            pages.get(message.command.tabId)?.relay.receive(message.command.message);
          if (message.type === "unmount") void pages.get(message.tabId)?.page.close();
        } catch (error) {
          errors.reject(error);
        }
      });
      void errors.promise.catch(() => {});
      yield* Effect.promise(() => Promise.race([welcome.promise, errors.promise]));
      const selected = yield* HttpClient.put(
        `${origin}/api/jones/preview-companion/threads/${scope.thread.threadId}`,
        {
          body: HttpBody.jsonUnsafe({ selection: { _tag: "companion", hostId: "simulator" } }),
        },
      ).pipe(Effect.provide(FetchHttpClient.layer));
      expect(selected.status).toBe(204);
      const broker = Context.get(services, Broker.PreviewAutomationBroker);
      const browser = Context.get(services, ServerBrowser.ServerBrowser);
      const opened = yield* Effect.raceFirst(
        broker.invoke<PreviewAutomationStatus>({
          scope,
          operation: "open",
          input: { reuseExistingTab: false, show: false },
        }),
        Effect.promise(() => errors.promise),
      );
      const tabId = opened.tabId!;
      yield* broker.invoke({
        scope,
        tabId,
        operation: "navigate",
        input: { url: `${origin}/simulator-page`, readiness: "load" },
      });
      const snapshot = yield* broker.invoke<PreviewAutomationSnapshot>({
        scope,
        tabId,
        operation: "snapshot",
        input: {},
      });
      expect(JSON.stringify(snapshot)).toContain("companion evidence");
      expect(snapshot.screenshot.mimeType).toBe("image/png");
      expect(Buffer.from(snapshot.screenshot.data, "base64").subarray(0, 8)).toEqual(
        Buffer.from([137, 80, 78, 71, 13, 10, 26, 10]),
      );
      for (const operation of ["recordingStart", "recordingStop", "upload"] as const) {
        const refusal = yield* broker
          .invoke({
            scope,
            tabId,
            operation,
            input:
              operation === "upload"
                ? {
                    paths: [NodePath.join(directory, "does-not-exist")],
                    locator: "input[type=file]",
                  }
                : {},
          })
          .pipe(
            Effect.flip,
            Effect.mapError((result) => new SimulatorFixtureError({ cause: result })),
          );
        expect(refusal).toMatchObject({
          _tag: "PreviewAutomationExecutionError",
          outcome: "not_started",
        });
        expect(refusal.message).toContain("unsupported on preview browser host Simulator");
      }
      const profileRefusal = yield* browser.clearProfile("default").pipe(Effect.flip);
      expect(profileRefusal).toMatchObject({
        _tag: "PreviewClearProfileError",
        cause: { _tag: "CompanionOperationUnsupported", operation: "clearProfile" },
      });
      expect(encoder).not.toHaveBeenCalled();
      expect(clear).not.toHaveBeenCalled();
      expect(upload).not.toHaveBeenCalled();
      const viewer = yield* browser.attachViewer({
        threadId: scope.thread.threadId,
        tabId,
        canOperate: true,
        maxWidth: 800,
        maxHeight: 600,
        quality: 70,
      });
      let output = yield* Queue.take(viewer.output);
      while (output._tag !== "frame") output = yield* Queue.take(viewer.output);
      expect(output.data.byteLength).toBeGreaterThan(100);
      yield* output.ack;
      yield* viewer.input({ type: "takeControl" });
      yield* viewer.input({ type: "text", text: "from viewer" });
      expect(
        yield* Effect.promise(() => pages.get(tabId)!.page.locator("input").inputValue()),
      ).toBe("from viewer");
      expect(fallback).not.toHaveBeenCalled();
      yield* Effect.promise(() => pages.get(tabId)!.page.locator("input").selectText());
      yield* viewer.input({ type: "key", action: "down", key: "c", code: "KeyC", modifiers: 2 });
      let copied = yield* Queue.take(viewer.output);
      while (copied._tag !== "clipboard") copied = yield* Queue.take(viewer.output);
      expect(copied.text).toBe("from viewer");
      yield* viewer.input({ type: "releaseControl" });
      const interrupted = yield* broker
        .invoke({ scope, tabId, operation: "evaluate", input: { expression: heldExpression } })
        .pipe(
          Effect.flip,
          Effect.mapError((result) => new SimulatorFixtureError({ cause: result })),
          Effect.forkScoped,
        );
      yield* Effect.promise(() => actionDispatched.promise);
      socket.close();
      let end = yield* Queue.take(viewer.output);
      while (end._tag !== "reconnect") end = yield* Queue.take(viewer.output);
      expect(
        (yield* Context.get(services, Registry.CompanionHostRegistry).status("simulator"))?.online,
      ).toBe(false);
      expect(yield* Fiber.join(interrupted)).toMatchObject({ outcome: "unknown" });
      const quarantined = yield* broker
        .invoke({ scope, tabId, operation: "press", input: { key: "Enter" } })
        .pipe(
          Effect.flip,
          Effect.mapError((result) => new SimulatorFixtureError({ cause: result })),
        );
      expect(quarantined).toMatchObject({ outcome: "not_started", unreconciled: true });
      const offline = yield* browser
        .attachViewer({
          threadId: scope.thread.threadId,
          tabId,
          canOperate: true,
          maxWidth: 800,
          maxHeight: 600,
          quality: 70,
        })
        .pipe(Effect.flip);
      expect(offline).toMatchObject({
        _tag: "ServerBrowserLaunchError",
        cause: { _tag: "CompanionHostUnavailable", state: "offline", outcome: "not_started" },
      });
      expect(fallback).not.toHaveBeenCalled();
    }).pipe(Effect.scoped),
  60_000,
);
