import * as Layer from "effect/Layer";
import * as CompanionHostRegistry from "./CompanionHostRegistry.ts";
import * as CompanionBrowserChannel from "./CompanionBrowserChannel.ts";
import * as RenderHostSelection from "./RenderHostSelection.ts";
import * as RenderPlacement from "./RenderPlacement.ts";
import * as CompanionWs from "./ws.ts";

export const servicesLayer = RenderPlacement.layer.pipe(
  Layer.provideMerge(CompanionBrowserChannel.layer),
  Layer.provideMerge(CompanionHostRegistry.layer),
  Layer.provideMerge(RenderHostSelection.layer),
);
export const routeLayer = CompanionWs.routeLayer;
