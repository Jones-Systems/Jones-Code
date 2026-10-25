import { expect, it } from "@effect/vitest";
import { ProviderInstanceId, type ModelSelection, type ServerProvider } from "@t3tools/contracts";
import type { ProviderInstance } from "@t3tools/provider-core/server/driver";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as ProviderInstanceRegistry from "../../provider/ProviderInstanceRegistry.ts";
import { resolveRemoteEvaluationModel } from "./remoteEvaluationModel.ts";
import * as TextGeneration from "../TextGeneration.ts";
import * as SourceControlProviderRegistry from "../../sourceControl/SourceControlProviderRegistry.ts";

const instanceId = ProviderInstanceId.make("second-account");
const selection: ModelSelection = {
  instanceId,
  model: "gpt-6.1-sol",
  options: [{ id: "reasoningEffort", value: "high" }],
};
const snapshot = {
  instanceId,
  enabled: true,
  supportsRemoteRequestEvaluation: true,
  models: [
    {
      slug: "gpt-6.1-sol",
      capabilities: {
        optionDescriptors: [
          {
            type: "select",
            id: "reasoningEffort",
            label: "Effort",
            options: [
              { id: "medium", label: "Medium" },
              { id: "high", label: "High" },
            ],
          },
        ],
      },
    },
  ],
} as unknown as ServerProvider;
const fixture = (overrides: Partial<ServerProvider> = {}) => {
  const instance = {
    instanceId,
    enabled: true,
    snapshot: { getSnapshot: Effect.succeed({ ...snapshot, ...overrides }) },
  } as unknown as ProviderInstance;
  const layer = Layer.succeed(ProviderInstanceRegistry.ProviderInstanceRegistry, {
    getInstance: (id) => Effect.succeed(id === instanceId ? instance : undefined),
  } as ProviderInstanceRegistry.ProviderInstanceRegistry["Service"]);
  return { instance, layer };
};

it.effect("returns the exact capable instance and preserves the full explicit selection", () =>
  Effect.gen(function* () {
    const { instance, layer } = fixture();
    const before = structuredClone(selection);
    expect(yield* resolveRemoteEvaluationModel(selection).pipe(Effect.provide(layer))).toBe(
      instance,
    );
    expect(selection).toEqual(before);
  }),
);

it.effect(
  "TextGeneration routes to the exact second account and forwards all model options unchanged",
  () =>
    Effect.gen(function* () {
      const second = fixture().instance;
      const calls: unknown[] = [];
      const registry = {
        getInstance: (id: ProviderInstanceId) =>
          Effect.succeed(
            id === instanceId
              ? ({
                  ...second,
                  textGeneration: {
                    ...second.textGeneration,
                    evaluateRemoteRequest: (input: unknown) =>
                      Effect.sync(() => {
                        calls.push(input);
                        return {
                          verdict: "pass" as const,
                          category: "none" as const,
                          reason: "Routine work",
                        };
                      }),
                  },
                } as ProviderInstance)
              : undefined,
          ),
      } as ProviderInstanceRegistry.ProviderInstanceRegistry["Service"];
      const generation = yield* TextGeneration.make.pipe(
        Effect.provideService(ProviderInstanceRegistry.ProviderInstanceRegistry, registry),
        Effect.provideService(
          SourceControlProviderRegistry.SourceControlProviderRegistry,
          {} as SourceControlProviderRegistry.SourceControlProviderRegistry["Service"],
        ),
      );
      const input = { cwd: "/synthetic", message: "Fix the parser", modelSelection: selection };
      yield* generation.evaluateRemoteRequest(input);
      expect(calls).toEqual([input]);
      expect(calls[0]).toBe(input);
      yield* generation
        .evaluateRemoteRequest({
          ...input,
          modelSelection: { ...selection, instanceId: ProviderInstanceId.make("first-account") },
        })
        .pipe(Effect.flip);
      expect(calls).toHaveLength(1);
    }),
);

it.effect.each([false, undefined])(
  "capability opt-in is mandatory (%s)",
  (supportsRemoteRequestEvaluation) =>
    resolveRemoteEvaluationModel(selection).pipe(
      Effect.provide(fixture({ supportsRemoteRequestEvaluation }).layer),
      Effect.flip,
      Effect.map((error) => {
        expect(error.detail).toContain("does not support");
      }),
    ),
);

it.effect(
  "rejects another account/model, disabled or unavailable instances, and invalid options",
  () =>
    Effect.gen(function* () {
      const invalid: ModelSelection[] = [
        { ...selection, instanceId: ProviderInstanceId.make("first-account") },
        { ...selection, model: "gpt-6.1" },
        { ...selection, options: [{ id: "reasoningEffort", value: "unknown" }] },
        { ...selection, options: [{ id: "fastMode", value: true }] },
        { ...selection, options: [...selection.options!, ...selection.options!] },
      ];
      for (const input of invalid)
        yield* resolveRemoteEvaluationModel(input).pipe(
          Effect.provide(fixture().layer),
          Effect.flip,
        );
      for (const overrides of [
        { enabled: false },
        { availability: "unavailable" as const },
        { models: [] },
      ])
        yield* resolveRemoteEvaluationModel(selection).pipe(
          Effect.provide(fixture(overrides).layer),
          Effect.flip,
        );
    }),
);
