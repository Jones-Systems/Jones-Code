import * as Effect from "effect/Effect";
import { RemoteEnvironmentAuthorization } from "../../authorization/service.ts";

export const unusedAuthorization = RemoteEnvironmentAuthorization.of({
  authorizeBearer: Effect.fn("Fixture.authorizeBearer")(() =>
    Effect.die("Unexpected bearer authorization in route fixture"),
  ),
  authorizeDpop: Effect.fn("Fixture.authorizeDpop")(() =>
    Effect.die("Unexpected DPoP authorization in route fixture"),
  ),
  authorizeDpopHttp: Effect.fn("Fixture.authorizeDpopHttp")(() =>
    Effect.die("Unexpected HTTP authorization in route fixture"),
  ),
  renewDpopBeforeExpiry: Effect.fn("Fixture.renewDpopBeforeExpiry")(() =>
    Effect.die("Unexpected renewal in non-DPoP route fixture"),
  ),
});
