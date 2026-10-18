import * as NodeServices from "@effect/platform-node/NodeServices";
import { expect, it } from "@effect/vitest";
import { EnvironmentId, ThreadId, type PreviewSessionSnapshot } from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";
import * as Layer from "effect/Layer";
import * as TestClock from "effect/testing/TestClock";
import * as ServerConfig from "../../config.ts";
import * as ServerEnvironment from "../../environment/ServerEnvironment.ts";
import * as DesktopChannel from "../../preview/DesktopBrowserChannel.ts";
import * as Registry from "./CompanionHostRegistry.ts";
import * as Placement from "./RenderPlacement.ts";
import * as Selection from "./RenderHostSelection.ts";
import * as Channel from "./CompanionBrowserChannel.ts";
import * as Registration from "./registration.ts";
import { runPlacement } from "./serverBrowserAdapter.ts";

const dependencies = Registration.servicesLayer.pipe(
  Layer.provide(DesktopChannel.layer),
  Layer.provide(
    Layer.succeed(ServerEnvironment.ServerEnvironment, {
      getEnvironmentId: Effect.succeed(EnvironmentId.make("test")),
      getDescriptor: Effect.die("unused"),
    }),
  ),
  Layer.provide(ServerConfig.layerTest(process.cwd(), { prefix: "preview-companion-placement-" })),
  Layer.provide(NodeServices.layer),
);
const snapshot: PreviewSessionSnapshot = {
  threadId: ThreadId.make("thread"),
  tabId: "tab",
  navStatus: { _tag: "Idle" },
  canGoBack: false,
  canGoForward: false,
  updatedAt: "2026-10-08T00:00:00.000Z",
};
it.effect(
  "fails closed for an unknown companion and retains the binding across selection changes",
  () =>
    Effect.gen(function* () {
      const placement = yield* Placement.RenderPlacement;
      const selection = yield* Selection.RenderHostSelection;
      yield* selection.setThread("thread", { _tag: "companion", hostId: "absent" });
      expect(yield* placement.place(snapshot).pipe(Effect.flip)).toMatchObject({
        _tag: "CompanionHostUnavailable",
        state: "unknown",
        outcome: "not_started",
      });
      yield* selection.setThread("thread", { _tag: "server" });
      expect(yield* placement.place(snapshot).pipe(Effect.flip)).toMatchObject({
        hostId: "absent",
      });
      yield* placement.release(snapshot);
      expect(yield* placement.place(snapshot)).toEqual({ _tag: "headless" });
    }).pipe(Effect.scoped, Effect.provide(dependencies)),
);

it.effect("unmounts after the 20-second attachment deadline without a local fallback", () =>
  Effect.gen(function* () {
    const placement = yield* Placement.RenderPlacement;
    const origin = {
      hostId: "mini",
      label: "Mini",
      capabilities: {
        cdp: true,
        clipboardText: true,
        uploads: false,
        downloads: false,
        recording: false,
      },
    } as const;
    const waiting = yield* placement
      .awaitCompanion(origin, snapshot)
      .pipe(Effect.flip, Effect.forkScoped);
    yield* TestClock.adjust("20 seconds");
    expect(yield* Fiber.join(waiting)).toMatchObject({
      _tag: "CompanionHostUnavailable",
      state: "timeout",
      outcome: "not_started",
    });
  }).pipe(
    Effect.scoped,
    Effect.provide(
      Placement.layer.pipe(
        Layer.provide(Layer.mock(Selection.RenderHostSelection, {})),
        Layer.provide(Layer.mock(Registry.CompanionHostRegistry, {})),
        Layer.provide(Layer.mock(DesktopChannel.DesktopBrowserChannel, { available: false })),
        Layer.provide(
          Layer.mock(Channel.CompanionBrowserChannel, {
            mount: () => Effect.void,
            awaitAttached: (_key, timeout) => Effect.sleep(timeout).pipe(Effect.as(false)),
            unmount: () => Effect.void,
          }),
        ),
      ),
    ),
  ),
);

it.live("preserves the typed not_started failure at the existing Promise boundary", () =>
  Effect.promise(async () => {
    const error = new Registry.CompanionHostUnavailable({
      hostId: "mini",
      label: "Mini",
      state: "offline",
    });
    await expect(runPlacement(Effect.fail(error))).rejects.toBe(error);
  }),
);
