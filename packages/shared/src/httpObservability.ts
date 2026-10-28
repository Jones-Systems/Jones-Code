import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Headers from "effect/http/Headers";
import * as UrlParams from "effect/http/UrlParams";

export const layer = Layer.merge(
  Layer.effect(
    Headers.CurrentRedactedNames,
    // `expo-session` carries the `eas login` session secret on Expo API requests.
    Effect.map(Headers.CurrentRedactedNames, (names) => [...names, "dpop", "expo-session"]),
  ),
  // Remote clients authenticate the `/ws` upgrade with a reusable `wsTicket` query
  // parameter, which HTTP spans would otherwise record in `url.full` and `url.query`.
  Layer.effect(
    UrlParams.CurrentRedactedNames,
    Effect.map(UrlParams.CurrentRedactedNames, (names) => [...names, "wsTicket"]),
  ),
);
