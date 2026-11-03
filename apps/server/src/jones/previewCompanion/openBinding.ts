import type { PreviewSessionSnapshot } from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Option from "effect/Option";
import * as RenderHostSelection from "./RenderHostSelection.ts";

export const resolveOpenBinding = Effect.fn("PreviewCompanion.resolveOpenBinding")(function* () {
  const selection = yield* Effect.serviceOption(RenderHostSelection.RenderHostSelection);
  if (Option.isNone(selection)) return undefined;
  return (snapshot: PreviewSessionSnapshot): void => {
    if (snapshot.runtime !== "server") return;
    // bind is synchronous: freeze selection before Manager publishes opened to its async mirror.
    Effect.runSync(selection.value.bind(snapshot));
  };
});
