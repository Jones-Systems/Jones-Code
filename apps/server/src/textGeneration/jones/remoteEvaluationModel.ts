import { TextGenerationError, type ModelSelection } from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as ProviderInstanceRegistry from "../../provider/ProviderInstanceRegistry.ts";

export const resolveRemoteEvaluationModel = Effect.fn("resolveRemoteEvaluationModel")(function* (
  selection: ModelSelection,
) {
  const registry = yield* ProviderInstanceRegistry.ProviderInstanceRegistry;
  const instance = yield* registry.getInstance(selection.instanceId);
  const snapshot = instance ? yield* instance.snapshot.getSnapshot : undefined;
  if (
    !instance?.enabled ||
    !snapshot?.enabled ||
    snapshot.availability === "unavailable" ||
    snapshot.supportsRemoteRequestEvaluation !== true
  )
    return yield* new TextGenerationError({
      operation: "evaluateRemoteRequest",
      detail:
        "The selected provider instance does not support tool-free remote request evaluation.",
    });
  const model = snapshot.models.find((candidate) => candidate.slug === selection.model);
  if (!model)
    return yield* new TextGenerationError({
      operation: "evaluateRemoteRequest",
      detail: "The selected evaluator model is unavailable on this provider instance.",
    });
  const seen = new Set<string>();
  for (const option of selection.options ?? []) {
    const descriptor = model.capabilities?.optionDescriptors?.find(({ id }) => id === option.id);
    if (
      seen.has(option.id) ||
      !descriptor ||
      (descriptor.type === "boolean"
        ? typeof option.value !== "boolean"
        : !descriptor.options.some((choice) => choice.id === option.value))
    )
      return yield* new TextGenerationError({
        operation: "evaluateRemoteRequest",
        detail: "The selected evaluator options are unsupported by this exact model and instance.",
      });
    seen.add(option.id);
  }
  return instance;
});
