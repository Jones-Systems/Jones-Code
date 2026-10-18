import * as HostProcess from "@t3tools/shared/HostProcess";
import * as PtyAdapter from "@t3tools/shared/PtyAdapter";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import { placementCommand } from "./processPlacement.ts";

export const layer = Layer.effect(
  PtyAdapter.PtyAdapter,
  Effect.gen(function* () {
    const platform = yield* HostProcess.Platform;
    const underlying = yield* PtyAdapter.PtyAdapter;
    return PtyAdapter.PtyAdapter.of({
      spawn: (input) =>
        Effect.try({
          try: () =>
            placementCommand(input.shell, input.args ?? [], "workload", undefined, false, platform),
          catch: (cause) =>
            new PtyAdapter.PtySpawnError({
              adapter: "process-placement",
              shell: input.shell,
              cause,
            }),
        }).pipe(
          Effect.flatMap((command) =>
            underlying.spawn({ ...input, shell: command.command, args: [...command.args] }),
          ),
        ),
    });
  }),
);
