import type { PreviewCompanionCapabilities, PreviewSessionSnapshot } from "@t3tools/contracts";
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import type { DesktopTabKey } from "../../preview/DesktopBrowserChannel.ts";
import * as DesktopBrowserChannel from "../../preview/DesktopBrowserChannel.ts";
import * as CompanionBrowserChannel from "./CompanionBrowserChannel.ts";
import * as CompanionHostRegistry from "./CompanionHostRegistry.ts";
import * as RenderHostSelection from "./RenderHostSelection.ts";

export type CompanionOrigin = {
  readonly hostId: string;
  readonly label: string;
  readonly capabilities: PreviewCompanionCapabilities;
};
export type Placement =
  | { readonly _tag: "local" | "headless" }
  | ({ readonly _tag: "companion" } & CompanionOrigin);
export class RenderPlacement extends Context.Service<
  RenderPlacement,
  {
    readonly channel: CompanionBrowserChannel.CompanionBrowserChannel["Service"];
    readonly place: (
      snapshot: PreviewSessionSnapshot,
    ) => Effect.Effect<Placement, CompanionHostRegistry.CompanionHostUnavailable>;
    readonly release: (key: DesktopTabKey) => Effect.Effect<void>;
    readonly awaitCompanion: (
      origin: CompanionOrigin,
      key: DesktopTabKey,
    ) => Effect.Effect<void, CompanionHostRegistry.CompanionHostUnavailable>;
  }
>()("t3/jones/previewCompanion/RenderPlacement") {}
const make = Effect.gen(function* () {
  const selection = yield* RenderHostSelection.RenderHostSelection;
  const registry = yield* CompanionHostRegistry.CompanionHostRegistry;
  const channel = yield* CompanionBrowserChannel.CompanionBrowserChannel;
  const desktop = yield* DesktopBrowserChannel.DesktopBrowserChannel;
  return RenderPlacement.of({
    channel,
    place: (snapshot) =>
      Effect.gen(function* () {
        const bound = yield* selection.bind(snapshot);
        if (bound._tag === "server") return { _tag: desktop.available ? "local" : "headless" };
        const status = yield* registry.status(bound.hostId);
        if (!status?.online)
          return yield* new CompanionHostRegistry.CompanionHostUnavailable({
            hostId: bound.hostId,
            label: status?.label ?? bound.hostId,
            state: status ? "offline" : "unknown",
          });
        return {
          _tag: "companion",
          hostId: bound.hostId,
          label: status.label,
          capabilities: status.capabilities,
        };
      }),
    awaitCompanion: (origin, key) =>
      Effect.gen(function* () {
        yield* channel.mount(origin.hostId, key);
        if (yield* channel.awaitAttached(key, "20 seconds")) return;
        yield* channel.unmount(origin.hostId, key);
        return yield* new CompanionHostRegistry.CompanionHostUnavailable({
          hostId: origin.hostId,
          label: origin.label,
          state: "timeout",
        });
      }),
    release: (key) =>
      Effect.gen(function* () {
        const binding = yield* selection.binding(key);
        if (binding?._tag === "companion") yield* channel.unmount(binding.hostId, key);
        yield* selection.release(key);
      }),
  });
});
export const layer = Layer.effect(RenderPlacement, make);
