// @effect-diagnostics nodeBuiltinImport:off - Entry point for the inherited-pipe shutdown regression test.
import * as NodeRuntime from "@effect/platform-node/NodeRuntime";
import * as NodeServices from "@effect/platform-node/NodeServices";
import { PreviewAutomationRuntimeIdentity } from "@t3tools/contracts";
import * as HostProcess from "@t3tools/shared/HostProcess";
import * as Console from "effect/Console";
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Fiber from "effect/Fiber";
import * as Layer from "effect/Layer";
import * as Schema from "effect/Schema";
import * as Stream from "effect/Stream";

import * as ServerSettings from "../../../../serverSettings.ts";

import * as ServerConfig from "../../../../config.ts";
import * as DesktopTelemetryReceiver from "../../../../resourceTelemetry/DesktopTelemetryReceiver.ts";
import * as DesktopBrowserChannel from "../../../../preview/DesktopBrowserChannel.ts";

const key = { threadId: "thread-1", tabId: "tab-1" };
const encodeRuntimeEvidence = Schema.encodeEffect(
  Schema.fromJsonString(
    Schema.Struct({
      attached: Schema.Boolean,
      evidence: Schema.NullOr(
        Schema.Struct({
          runtimeIdentity: Schema.NullOr(PreviewAutomationRuntimeIdentity),
          attachmentGeneration: Schema.Number,
        }),
      ),
    }),
  ),
);

Effect.gen(function* () {
  const base = yield* ServerConfig.ServerConfig.pipe(
    Effect.provide(ServerConfig.layerTest(process.cwd(), { prefix: "t3-pipe-lifecycle-" })),
  );
  const context = yield* Layer.build(
    Layer.merge(DesktopBrowserChannel.layer, DesktopTelemetryReceiver.layer).pipe(
      Layer.provide(ServerSettings.layerTest()),
      Layer.provide(
        Layer.succeed(ServerConfig.ServerConfig, {
          ...base,
          desktopBrowserFd: 3,
          desktopBrowserControlFd: 4,
          desktopTelemetryFd: 5,
          desktopTelemetryControlFd: 6,
        }),
      ),
    ),
  );
  const browser = Context.get(context, DesktopBrowserChannel.DesktopBrowserChannel);
  if ((yield* HostProcess.Arguments).includes("--runtime-evidence")) {
    const sentinel = { threadId: "sentinel-thread", tabId: "sentinel-tab" };
    yield* Console.log("ready");
    const attached = yield* browser.awaitAttached(sentinel, "5 seconds");
    const evidence = yield* browser.runtimeEvidence(key);
    const encoded = yield* encodeRuntimeEvidence({ attached, evidence });
    yield* Console.log(`runtime-evidence:${encoded}`);
    return;
  }
  const telemetry = Context.get(context, DesktopTelemetryReceiver.DesktopTelemetryReceiver);
  const health = yield* telemetry.subscribeHealth;
  const healthy = yield* health.changes.pipe(
    Stream.filter((value) => value.status === "healthy"),
    Stream.runHead,
    Effect.forkScoped,
  );
  const detached = yield* browser.detached.pipe(Stream.runHead, Effect.forkScoped);
  yield* Console.log("ready");
  if (!(yield* browser.awaitAttached(key, "5 seconds"))) {
    return yield* Effect.die("The browser attach message was not received.");
  }
  yield* Fiber.join(healthy);
  yield* Console.log("attached");
  yield* Fiber.join(detached);
  const endpoint = yield* Effect.exit(Effect.scoped(browser.endpoint(key)));
  if (Exit.isSuccess(endpoint)) return yield* Effect.die("A detached tab received an endpoint.");
  yield* Console.log("verified");
}).pipe(Effect.scoped, Effect.provide(NodeServices.layer), NodeRuntime.runMain);
