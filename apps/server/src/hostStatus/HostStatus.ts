import type { HostStatusSnapshot } from "@t3tools/contracts";
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";

import { hostStatusConfigFromEnv, readHostStatus } from "./gateway.ts";

export class HostStatus extends Context.Service<
  HostStatus,
  { readonly snapshot: () => Effect.Effect<HostStatusSnapshot> }
>()("t3/hostStatus/HostStatus") {}

const make = Effect.sync(() =>
  HostStatus.of({
    // Configuration and collector credentials belong to the serving environment.
    snapshot: Effect.fn("hostStatus.snapshot")(() =>
      Effect.promise(() => readHostStatus(hostStatusConfigFromEnv(process.env))),
    ),
  }),
);

export const layer = Layer.effect(HostStatus, make);
