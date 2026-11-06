import { ProviderDriverKind, TextGenerationError } from "@t3tools/contracts";
import * as ProviderAdapter from "@t3tools/provider-core/server/ProviderAdapter";
import type { ProviderTextGeneration } from "@t3tools/provider-core/server/textGeneration";
import * as Effect from "effect/Effect";
import * as Option from "effect/Option";

import type * as OpenCodeRuntime from "../OpenCodeRuntime.ts";
import type { makeOpenCodeRuntimeProbe } from "../versionProbe.ts";

type OpenCodeRuntimeProbe = Effect.Success<
  ReturnType<typeof makeOpenCodeRuntimeProbe<OpenCodeRuntime.OpenCodeRuntimeError>>
>;

const DRIVER_KIND = ProviderDriverKind.make("opencode");

export function selectOpenCodeRuntimeAdapter(input: {
  readonly probe: OpenCodeRuntimeProbe;
  readonly v1: ProviderAdapter.ProviderAdapterV2["Service"];
  readonly v2: ProviderAdapter.ProviderAdapterV2["Service"];
}): ProviderAdapter.ProviderAdapterV2["Service"] {
  const hot = input.probe.lastSuccess.pipe(
    Effect.map((result) =>
      Option.isSome(result) && result.value.generation === "v2" ? input.v2 : input.v1,
    ),
  );
  return {
    instanceId: input.v1.instanceId,
    driver: DRIVER_KIND,
    getCapabilities: () => Effect.flatMap(hot, (adapter) => adapter.getCapabilities()),
    planSelectionTransition: (transition) =>
      Effect.flatMap(hot, (adapter) => adapter.planSelectionTransition(transition)),
    openSession: (session) =>
      input.probe.get.pipe(
        Effect.mapError(
          (cause) =>
            new ProviderAdapter.ProviderAdapterOpenSessionError({
              driver: DRIVER_KIND,
              providerSessionId: session.providerSessionId,
              cause,
            }),
        ),
        Effect.flatMap((result) =>
          (result.generation === "v2" ? input.v2 : input.v1).openSession(session),
        ),
      ),
  };
}

export function selectOpenCodeRuntimeTextGeneration(
  probe: OpenCodeRuntimeProbe,
  v1: ProviderTextGeneration,
  v2: ProviderTextGeneration,
): ProviderTextGeneration {
  const select = <A>(
    operation: keyof ProviderTextGeneration,
    paths: {
      readonly v1: Effect.Effect<A, TextGenerationError>;
      readonly v2: Effect.Effect<A, TextGenerationError>;
    },
  ) =>
    probe.get.pipe(
      Effect.mapError(
        (cause) =>
          new TextGenerationError({
            operation,
            detail: "Couldn't verify a fresh OpenCode protocol version.",
            cause,
          }),
      ),
      Effect.flatMap((result) => (result.generation === "v2" ? paths.v2 : paths.v1)),
    );
  return {
    generateCommitMessage: (input) =>
      select("generateCommitMessage", {
        v1: v1.generateCommitMessage(input),
        v2: v2.generateCommitMessage(input),
      }),
    generatePrContent: (input) =>
      select("generatePrContent", {
        v1: v1.generatePrContent(input),
        v2: v2.generatePrContent(input),
      }),
    generateBranchName: (input) =>
      select("generateBranchName", {
        v1: v1.generateBranchName(input),
        v2: v2.generateBranchName(input),
      }),
    generateThreadTitle: (input) =>
      select("generateThreadTitle", {
        v1: v1.generateThreadTitle(input),
        v2: v2.generateThreadTitle(input),
      }),
    evaluateRemoteRequest: (input) =>
      select("evaluateRemoteRequest", {
        v1: v1.evaluateRemoteRequest(input),
        v2: v2.evaluateRemoteRequest(input),
      }),
    generateReviewGuide: (input) =>
      select("generateReviewGuide", {
        v1: v1.generateReviewGuide(input),
        v2: v2.generateReviewGuide(input),
      }),
  };
}
