import { CodexSettings, ProviderInstanceId } from "@t3tools/contracts";
import * as Fiber from "effect/Fiber";
import * as TestClock from "effect/testing/TestClock";
import * as CodexErrors from "effect-codex-app-server/errors";
import * as ChildProcessSpawner from "effect/process/ChildProcessSpawner";
import * as Schema from "effect/Schema";
import * as Effect from "effect/Effect";
import { assert, it } from "@effect/vitest";

import {
  applyPreferredCodexDefaultModel,
  mapCodexModelCapabilities,
  checkCodexProviderStatus,
} from "./CodexProvider.ts";

it("uses medium instead of low for newly discovered Codex models when supported", () => {
  const capabilities = mapCodexModelCapabilities({
    additionalSpeedTiers: [],
    defaultReasoningEffort: "low",
    description: "Future model",
    displayName: "Future model",
    hidden: false,
    id: "future-model",
    isDefault: false,
    model: "future-model",
    supportedReasoningEfforts: ["low", "medium", "high"].map((reasoningEffort) => ({
      reasoningEffort,
      description: reasoningEffort,
    })),
  });
  const reasoning = capabilities.optionDescriptors?.find(({ id }) => id === "reasoningEffort");
  assert.strictEqual(reasoning?.currentValue, "medium");
  assert.deepStrictEqual(
    reasoning?.type === "select" ? reasoning.options.filter(({ isDefault }) => isDefault) : [],
    [{ id: "medium", label: "Medium", isDefault: true }],
  );
});

it("retains a low catalog default when the model does not support medium", () => {
  const capabilities = mapCodexModelCapabilities({
    additionalSpeedTiers: [],
    defaultReasoningEffort: "low",
    description: "Limited model",
    displayName: "Limited model",
    hidden: false,
    id: "limited-model",
    isDefault: false,
    model: "limited-model",
    supportedReasoningEfforts: [{ reasoningEffort: "low", description: "Low" }],
  });
  assert.strictEqual(capabilities.optionDescriptors?.[0]?.currentValue, "low");
});

it("maps current Codex model capability fields", () => {
  const capabilities = mapCodexModelCapabilities({
    additionalSpeedTiers: [],
    defaultReasoningEffort: "super-high",
    description: "Test model",
    displayName: "GPT Test",
    hidden: false,
    id: "gpt-test",
    isDefault: true,
    model: "gpt-test",
    defaultServiceTier: "flex",
    serviceTiers: [
      {
        id: "priority",
        name: "Fast",
        description: "Lower latency responses.",
      },
      {
        id: "flex",
        name: "Flex",
        description: "Lower-cost asynchronous routing.",
      },
    ],
    supportedReasoningEfforts: [
      {
        description: "Maximum reasoning",
        reasoningEffort: "super-high",
      },
    ],
  });

  assert.deepStrictEqual(capabilities.optionDescriptors, [
    {
      id: "reasoningEffort",
      label: "Reasoning",
      type: "select",
      options: [{ id: "super-high", label: "super-high", isDefault: true }],
      currentValue: "super-high",
    },
    {
      id: "serviceTier",
      label: "Service Tier",
      type: "select",
      options: [
        { id: "default", label: "Standard" },
        {
          id: "priority",
          label: "Fast",
          description: "Lower latency responses.",
        },
        {
          id: "flex",
          label: "Flex",
          description: "Lower-cost asynchronous routing.",
          isDefault: true,
        },
      ],
      currentValue: "flex",
    },
  ]);
});

it("uses standard routing when the catalog has no default service tier", () => {
  const capabilities = mapCodexModelCapabilities({
    additionalSpeedTiers: ["fast"],
    defaultReasoningEffort: "medium",
    defaultServiceTier: null,
    description: "Test model",
    displayName: "GPT Test",
    hidden: false,
    id: "gpt-test",
    isDefault: true,
    model: "gpt-test",
    serviceTiers: [
      {
        id: "priority",
        name: "Fast",
        description: "1.5x speed, increased usage",
      },
      {
        id: "ultrafast",
        name: "Ultrafast",
        description: "The fastest available responses for latency-sensitive work.",
      },
    ],
    supportedReasoningEfforts: [],
  });

  assert.deepStrictEqual(capabilities.optionDescriptors, [
    {
      id: "serviceTier",
      label: "Service Tier",
      type: "select",
      options: [
        { id: "default", label: "Standard", isDefault: true },
        {
          id: "priority",
          label: "Fast",
          description: "1.5x speed, increased usage",
        },
        {
          id: "ultrafast",
          label: "Ultrafast",
          description: "Even faster, more expensive",
        },
      ],
      currentValue: "default",
    },
  ]);
});

it("marks the most preferred available model as default", () => {
  const models = applyPreferredCodexDefaultModel([
    { slug: "gpt-5.6-terra", name: "GPT-5.6-Terra", isCustom: false, capabilities: null },
    { slug: "gpt-5.4", name: "GPT-5.4", isCustom: false, isDefault: true, capabilities: null },
  ]);

  assert.deepStrictEqual(
    models.map((model) => ({ slug: model.slug, isDefault: model.isDefault })),
    [
      { slug: "gpt-5.6-terra", isDefault: true },
      { slug: "gpt-5.4", isDefault: undefined },
    ],
  );
});

it("prefers sol over terra when both are available", () => {
  const models = applyPreferredCodexDefaultModel([
    { slug: "gpt-5.6-terra", name: "GPT-5.6-Terra", isCustom: false, capabilities: null },
    { slug: "gpt-5.6-sol", name: "GPT-5.6-Sol", isCustom: false, capabilities: null },
  ]);

  assert.deepStrictEqual(models.find((model) => model.isDefault)?.slug, "gpt-5.6-sol");
});

it("ranks qualified Codex models while preserving their wire ids", () => {
  const models = applyPreferredCodexDefaultModel([
    {
      slug: "openai.gpt-5.6-luna",
      name: "Luna",
      isCustom: false,
      isDefault: true,
      capabilities: null,
    },
    { slug: "openai.gpt-5.6-sol", name: "Sol", isCustom: false, capabilities: null },
  ]);
  assert.deepStrictEqual(
    models.filter((model) => model.isDefault).map((model) => model.slug),
    ["openai.gpt-5.6-sol"],
  );
});

it("keeps Codex's own default when no preferred model is available", () => {
  const models = applyPreferredCodexDefaultModel([
    { slug: "gpt-5.5", name: "GPT-5.5", isCustom: false, capabilities: null },
    { slug: "gpt-5.4", name: "GPT-5.4", isCustom: false, isDefault: true, capabilities: null },
  ]);

  assert.deepStrictEqual(models.find((model) => model.isDefault)?.slug, "gpt-5.4");
});

it("ignores custom models that shadow a preferred slug", () => {
  const models = applyPreferredCodexDefaultModel([
    { slug: "gpt-5.6-sol", name: "gpt-5.6-sol", isCustom: true, capabilities: null },
    { slug: "gpt-5.4", name: "GPT-5.4", isCustom: false, isDefault: true, capabilities: null },
  ]);

  assert.deepStrictEqual(models.find((model) => model.isDefault)?.slug, "gpt-5.4");
});

it.effect(
  "publishes qualified full-map quota with receipt provenance and fresh model capabilities",
  () =>
    Effect.gen(function* () {
      const receipt = "2026-09-30T12:00:01.000Z";
      const map = {
        codex: { primary: { usedPercent: 40, windowDurationMins: 300, resetsAt: 1791000000 } },
        other: { secondary: { usedPercent: 10, windowDurationMins: 10080, resetsAt: 1792000000 } },
      };
      const settings = yield* Schema.decodeEffect(CodexSettings)({});
      const status = yield* checkCodexProviderStatus(
        settings,
        () =>
          Effect.succeed({
            account: {
              account: { type: "chatgpt", email: "private@example.com", planType: "pro" },
              requiresOpenaiAuth: true,
            },
            version: "1.0",
            skills: [],
            models: [
              {
                slug: "gpt-6.1-sol",
                name: "Sol",
                isCustom: false,
                capabilities: {
                  optionDescriptors: [
                    {
                      id: "reasoningEffort",
                      label: "Reasoning",
                      type: "select",
                      options: [{ id: "medium", label: "Medium" }],
                    },
                  ],
                },
              },
              { slug: "custom", name: "Custom", isCustom: true, capabilities: null },
            ],
            rateLimits: {
              snapshot: map.codex,
              rateLimitsByLimitId: map,
              quotaReceivedAt: receipt,
              resetCredits: null,
              ordinaryUsageAllowed: true,
              accountId: "private-quota-account-sentinel",
            },
          }),
        {},
        undefined,
        ProviderInstanceId.make("named-codex-instance"),
      );
      assert.strictEqual(status.qualifiedQuota?.instanceId, "named-codex-instance");
      assert.strictEqual(status.qualifiedQuota?.status, "qualified");
      assert.strictEqual(status.qualifiedQuota?.quotaReceivedAt, receipt);
      assert.deepStrictEqual(status.qualifiedQuota?.rateLimitsByLimitId, map);
      assert.deepStrictEqual(
        status.qualifiedQuota?.capabilityRefs.map((ref) => [ref.modelId, ref.reasoningEfforts]),
        [["gpt-6.1-sol", ["medium"]]],
      );
      const serializedQuota = yield* Schema.encodeEffect(Schema.fromJsonString(Schema.Unknown))(
        status.qualifiedQuota ?? null,
      );
      assert.strictEqual(serializedQuota.includes("private@example.com"), false);
      assert.strictEqual(serializedQuota.includes("private-quota-account-sentinel"), false);
      assert.strictEqual(serializedQuota.includes("ordinaryUsageAllowed"), false);
      assert.strictEqual(serializedQuota.includes("bindingGeneration"), false);
    }).pipe(
      Effect.provideService(
        ChildProcessSpawner.ChildProcessSpawner,
        ChildProcessSpawner.make(() => Effect.die("unexpected spawn")),
      ),
    ),
);

it.effect(
  "a failed full probe publishes failed quota rather than an eligible last-good value",
  () =>
    Effect.gen(function* () {
      const settings = yield* Schema.decodeEffect(CodexSettings)({});
      const status = yield* checkCodexProviderStatus(settings, () =>
        Effect.fail(
          new CodexErrors.CodexAppServerSpawnError({
            command: "fixture",
            cause: new Error("private fixture error"),
          }),
        ),
      );
      assert.strictEqual(status.qualifiedQuota?.status, "failed");
      assert.strictEqual(status.qualifiedQuota?.complete, false);
      assert.strictEqual(status.qualifiedQuota?.quotaReceivedAt, null);
      assert.strictEqual(status.qualifiedQuota?.rateLimitsByLimitId, null);
      const serializedQuota = yield* Schema.encodeEffect(Schema.fromJsonString(Schema.Unknown))(
        status.qualifiedQuota ?? null,
      );
      assert.strictEqual(serializedQuota.includes("private fixture error"), false);
    }).pipe(
      Effect.provideService(
        ChildProcessSpawner.ChildProcessSpawner,
        ChildProcessSpawner.make(() => Effect.die("unexpected spawn")),
      ),
    ),
);

it.effect(
  "preserves managedAuth as argument four and never qualifies its skipped native usage",
  () =>
    Effect.gen(function* () {
      const settings = yield* Schema.decodeEffect(CodexSettings)({});
      const managedAuth = {
        status: "authenticated" as const,
        type: "chatgpt",
        subscriptionSharing: true,
        label: "Managed",
        email: "private-managed-email",
      };
      const result = yield* checkCodexProviderStatus(
        settings,
        (input) => {
          assert.strictEqual(input.skipNativeUsage, true);
          return Effect.succeed({
            account: {
              account: {
                type: "chatgpt" as const,
                email: "native-sentinel",
                planType: "pro" as const,
              },
              requiresOpenaiAuth: true,
            },
            models: [],
            skills: [],
            version: "1.0",
            rateLimits: {
              ordinaryUsageAllowed: true,
              accountId: "private-native-account",
              quotaReceivedAt: "2026-09-30T12:00:01.000Z",
              resetCredits: null,
              snapshot: {
                primary: { usedPercent: 0, resetsAt: 1791000000, windowDurationMins: 300 },
              },
              rateLimitsByLimitId: {
                codex: {
                  primary: { usedPercent: 0, resetsAt: 1791000000, windowDurationMins: 300 },
                },
              },
            },
          });
        },
        {},
        managedAuth,
        ProviderInstanceId.make("managed-codex"),
      );
      assert.deepStrictEqual(result.auth, managedAuth);
      assert.strictEqual(result.usageLimits, undefined);
      assert.strictEqual(result.qualifiedQuota?.instanceId, "managed-codex");
      assert.strictEqual(result.qualifiedQuota?.status, "unsupported");
      assert.strictEqual(result.qualifiedQuota?.failureCode, "unsupported_account");
      assert.strictEqual(result.qualifiedQuota?.rateLimitsByLimitId, null);
      const json = yield* Schema.encodeEffect(Schema.fromJsonString(Schema.Unknown))(
        result.qualifiedQuota,
      );
      assert.strictEqual(json.includes("private-native-account"), false);
      assert.strictEqual(json.includes("private-managed-email"), false);
      assert.strictEqual(json.includes("native-sentinel"), false);
    }).pipe(
      Effect.provideService(
        ChildProcessSpawner.ChildProcessSpawner,
        ChildProcessSpawner.make(() => Effect.die("unexpected spawn")),
      ),
    ),
);

it.effect("a bounded whole-probe timeout cannot qualify a partial quota response", () =>
  Effect.gen(function* () {
    const settings = yield* Schema.decodeEffect(CodexSettings)({});
    const fiber = yield* checkCodexProviderStatus(settings, () => Effect.never).pipe(
      Effect.forkChild,
    );
    yield* TestClock.adjust("11 seconds");
    const result = yield* Fiber.join(fiber);
    assert.strictEqual(result.qualifiedQuota?.status, "failed");
    assert.strictEqual(result.qualifiedQuota?.failureCode, "probe_timeout");
    assert.strictEqual(result.qualifiedQuota?.quotaReceivedAt, null);
    assert.strictEqual(result.qualifiedQuota?.rateLimitsByLimitId, null);
  }).pipe(
    Effect.provideService(
      ChildProcessSpawner.ChildProcessSpawner,
      ChildProcessSpawner.make(() => Effect.die("unexpected spawn")),
    ),
  ),
);
