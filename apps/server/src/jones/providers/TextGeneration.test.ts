import { it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as PubSub from "effect/PubSub";
import * as Result from "effect/Result";
import * as Stream from "effect/Stream";
import { describe, expect } from "vite-plus/test";

import { ProviderInstanceId, TextGenerationError } from "@t3tools/contracts";
import { createModelSelection } from "@t3tools/shared/model";

import type { ProviderInstance } from "@t3tools/provider-core/server/driver";
import * as ProviderInstanceRegistry from "../../provider/ProviderInstanceRegistry.ts";
import * as TextGeneration from "../../textGeneration/TextGeneration.ts";
import * as SourceControlProviderRegistry from "../../sourceControl/SourceControlProviderRegistry.ts";
import * as Layer from "effect/Layer";

const makeStubTextGeneration = (
  overrides: Partial<TextGeneration.TextGeneration["Service"]>,
): TextGeneration.TextGeneration["Service"] =>
  TextGeneration.TextGeneration.of({
    evaluateRemoteRequest: () =>
      Effect.die("evaluateRemoteRequest stub not configured for this test"),
    generateCommitMessage: () =>
      Effect.die("generateCommitMessage stub not configured for this test"),
    generatePrContent: () => Effect.die("generatePrContent stub not configured for this test"),
    generateBranchName: () => Effect.die("generateBranchName stub not configured for this test"),
    generateThreadTitle: () => Effect.die("generateThreadTitle stub not configured for this test"),
    generateReviewGuide: () => Effect.die("generateReviewGuide stub not configured for this test"),
    ...overrides,
  });

const makeStubInstance = (
  instanceId: ProviderInstanceId,
  textGeneration: TextGeneration.TextGeneration["Service"],
): ProviderInstance =>
  ({
    instanceId,
    driverKind: instanceId as unknown as ProviderInstance["driverKind"],
    continuationIdentity: {
      driverKind: instanceId as unknown as ProviderInstance["driverKind"],
      continuationKey: `${instanceId}:test`,
    },
    displayName: undefined,
    enabled: true,
    snapshot: {} as ProviderInstance["snapshot"],
    orchestrationAdapter: {} as ProviderInstance["orchestrationAdapter"],
    textGeneration,
  }) satisfies ProviderInstance;

const makeStubRegistry = (
  instances: ReadonlyArray<ProviderInstance>,
): ProviderInstanceRegistry.ProviderInstanceRegistry["Service"] => {
  const byId = new Map(instances.map((instance) => [instance.instanceId, instance] as const));
  return {
    getInstance: (id) => Effect.succeed(byId.get(id)),
    listInstances: Effect.succeed(instances),
    listUnavailable: Effect.succeed([]),
    streamChanges: Stream.empty,
    subscribeChanges: Effect.flatMap(PubSub.unbounded<void>(), (pubsub) =>
      PubSub.subscribe(pubsub),
    ),
  };
};

describe("Jones remote evaluation routing", () => {
  it.effect.each(["pass", "hold", "missing", "unsupported"] as const)(
    "routes remote evaluation and preserves %s outcomes without link resolution",
    (outcome) =>
      Effect.gen(function* () {
        const instanceId = ProviderInstanceId.make("remote-evaluator");
        const input = {
          cwd: "/synthetic/workspace",
          message: "Fix the parser",
          context: "Parser repository",
          modelSelection: createModelSelection(instanceId, "synthetic-model"),
        };
        const instances =
          outcome === "missing"
            ? []
            : [
                makeStubInstance(
                  instanceId,
                  makeStubTextGeneration({
                    evaluateRemoteRequest: (received) => {
                      expect(received).toEqual(input);
                      if (outcome === "unsupported")
                        return Effect.fail(
                          new TextGenerationError({
                            operation: "evaluateRemoteRequest",
                            detail: "Unsupported synthetic evaluator.",
                          }),
                        );
                      return Effect.succeed(
                        outcome === "hold"
                          ? { verdict: "hold", category: "risky", reason: "Owner input needed." }
                          : { verdict: "pass", category: "none", reason: "Ordinary coding work." },
                      );
                    },
                  }),
                ),
              ];
        const generation = yield* TextGeneration.make.pipe(
          Effect.provideService(
            ProviderInstanceRegistry.ProviderInstanceRegistry,
            makeStubRegistry(instances),
          ),
          Effect.provide(
            Layer.mock(SourceControlProviderRegistry.SourceControlProviderRegistry)({
              resolveLink: () => Effect.die("Evaluator must not investigate linked content"),
            }),
          ),
        );
        const result = yield* generation.evaluateRemoteRequest(input).pipe(Effect.result);
        if (outcome === "missing" || outcome === "unsupported") {
          expect(Result.isFailure(result)).toBe(true);
          if (Result.isFailure(result)) {
            expect(result.failure.operation).toBe("evaluateRemoteRequest");
            expect(result.failure.detail).toContain(
              outcome === "missing" ? "No provider instance" : "Unsupported synthetic",
            );
          }
        } else {
          expect(Result.isSuccess(result)).toBe(true);
          if (Result.isSuccess(result)) expect(result.success.verdict).toBe(outcome);
        }
      }),
  );
});
