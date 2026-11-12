import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import type { SourceControlProviderKind } from "@t3tools/contracts";

import * as BuiltInDrivers from "../sourceControl/builtInDrivers.ts";
import type { PullRequestProviderApi } from "@t3tools/source-control-core/server/PullRequestProvider";
import type { SourceControlCiReader } from "@t3tools/source-control-core/server/driver";

export class PullRequestProviderRegistry extends Context.Service<
  PullRequestProviderRegistry,
  {
    readonly get: (kind: SourceControlProviderKind) => PullRequestProviderApi | null;
    readonly getCi?: (kind: SourceControlProviderKind) => SourceControlCiReader | null;
    readonly kinds: ReadonlyArray<SourceControlProviderKind>;
  }
>()("t3/pullRequest/PullRequestProviderRegistry") {}

export function fromProviders(
  providers: ReadonlyArray<PullRequestProviderApi>,
  ciReaders: ReadonlyMap<SourceControlProviderKind, SourceControlCiReader> = new Map(),
): PullRequestProviderRegistry["Service"] {
  const byKind = new Map(providers.map((provider) => [provider.kind, provider]));
  return {
    get: (kind) => byKind.get(kind) ?? null,
    getCi: (kind) => ciReaders.get(kind) ?? null,
    kinds: providers.map((provider) => provider.kind),
  };
}

/** @public */
export const make = Effect.gen(function* () {
  const drivers = yield* BuiltInDrivers.SourceControlDrivers;
  return fromProviders(
    drivers.flatMap(({ instance }) => instance.pullRequests ?? []),
    new Map(
      drivers.flatMap(({ driver, instance }) =>
        instance.ci === undefined ? [] : [[driver.kind, instance.ci] as const],
      ),
    ),
  );
});

export const layer = Layer.effect(PullRequestProviderRegistry, make).pipe(
  Layer.provide(BuiltInDrivers.layer),
);
