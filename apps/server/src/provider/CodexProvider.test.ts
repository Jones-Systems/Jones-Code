import { CodexSettings, ProviderInstanceId } from "@t3tools/contracts";
import { assert, it } from "@effect/vitest";
import * as Cause from "effect/Cause";
import * as DateTime from "effect/DateTime";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Fiber from "effect/Fiber";
import * as FileSystem from "effect/FileSystem";
import * as Layer from "effect/Layer";
import * as Path from "effect/Path";
import * as Queue from "effect/Queue";
import * as Schema from "effect/Schema";
import * as Scope from "effect/Scope";
import * as Sink from "effect/Sink";
import * as Stream from "effect/Stream";
import * as ChildProcessSpawner from "effect/process/ChildProcessSpawner";
import * as TestClock from "effect/testing/TestClock";
import * as CodexErrors from "effect-codex-app-server/errors";
import * as HostProcess from "@t3tools/shared/HostProcess";

import {
  applyPreferredCodexDefaultModel,
  codexDefaultReasoningEffortOverride,
  checkCodexProviderStatus,
  codexAutoProbeConfigArgs,
  codexCustomEndpoint,
  codexRoutesSpendNativeQuota,
  makeCodexSkillsProbe,
  mapCodexModelCapabilities,
  probeCodexAutoRouting,
} from "./CodexProvider.ts";

const layerProbePlatform = Layer.mergeAll(
  Path.layer,
  FileSystem.layerNoop({
    readFileString: () => Effect.die("Injected Codex probes must not read provider files."),
  }),
);

function makeFakeCodexAppServers(responses: Readonly<Record<string, unknown>> = {}) {
  const helpers: Array<{
    readonly codexHome: unknown;
    readonly args: ReadonlyArray<string>;
    readonly crash: Effect.Effect<void>;
    readonly methods: Array<string>;
    readonly requests: Array<unknown>;
    closed: boolean;
  }> = [];
  const spawner = ChildProcessSpawner.make((command) =>
    Effect.gen(function* () {
      const output = yield* Queue.unbounded<Uint8Array, Cause.Done>();
      const exited = yield* Deferred.make<ChildProcessSpawner.ExitCode>();
      const crash = Deferred.succeed(exited, ChildProcessSpawner.ExitCode(1)).pipe(
        Effect.andThen(Queue.end(output)),
        Effect.asVoid,
      );
      const helper = {
        codexHome: command._tag === "StandardCommand" ? command.options.env?.CODEX_HOME : undefined,
        args: command._tag === "StandardCommand" ? command.args.map(String) : [],
        crash,
        methods: [] as Array<string>,
        requests: [] as Array<unknown>,
        closed: false,
      };
      helpers.push(helper);
      yield* Effect.addFinalizer(() => Effect.sync(() => (helper.closed = true)));
      const respond = (id: unknown, result: unknown) =>
        Queue.offer(output, new TextEncoder().encode(`${JSON.stringify({ id, result })}\n`));
      return ChildProcessSpawner.makeHandle({
        pid: ChildProcessSpawner.ProcessId(900_000_000 + helpers.length),
        exitCode: Deferred.await(exited),
        isRunning: Effect.map(Deferred.isDone(exited), (done) => !done),
        kill: () => crash,
        unref: Effect.succeed(Effect.void),
        stdin: Sink.forEach((chunk: string | Uint8Array) =>
          Effect.forEach(
            (typeof chunk === "string" ? chunk : new TextDecoder().decode(chunk))
              .split("\n")
              .filter(Boolean),
            (line) => {
              const frame = JSON.parse(line) as {
                readonly id?: number;
                readonly method: string;
                readonly params?: { readonly cwds: ReadonlyArray<string> };
              };
              if (frame.id === undefined) return Effect.void;
              helper.methods.push(frame.method);
              if (frame.method === "initialize") {
                return respond(frame.id, {
                  codexHome: "/codex-home",
                  platformFamily: "unix",
                  platformOs: "linux",
                  userAgent: "codex/0.0.0",
                });
              }
              helper.requests.push(frame.params);
              if (Object.hasOwn(responses, frame.method))
                return respond(frame.id, responses[frame.method]);
              return respond(frame.id, {
                data: (frame.params?.cwds ?? []).map((cwd) => ({
                  cwd,
                  errors: [],
                  skills: [
                    {
                      name: `skill-in-${cwd}`,
                      path: `${cwd}/SKILL.md`,
                      enabled: true,
                      description: "",
                      scope: "repo",
                    },
                  ],
                })),
              });
            },
            { discard: true },
          ),
        ),
        stdout: Stream.fromQueue(output),
        stderr: Stream.empty,
        all: Stream.empty,
        getInputFd: () => Sink.drain,
        getOutputFd: () => Stream.empty,
      });
    }),
  );
  return { helpers, spawner };
}

it("allows route-preserving Codex launch flags and rejects flags that can activate integrations", () => {
  assert.deepStrictEqual(
    codexAutoProbeConfigArgs("--profile work -c 'model_provider=\"openai\"'"),
    [
      "--config",
      "features.connectors=false",
      "--config",
      "features.plugins=false",
      "--config",
      "features.apps=false",
      "--config",
      "features.codex_hooks=false",
      "--config",
      "features.hooks=false",
    ],
  );
  for (const args of [
    "--enable=plugins",
    "-c mcp_servers.foo.command=external",
    "--listen ws://0.0.0.0:9876",
    "--profile --config model=foo",
    "--",
  ])
    assert.strictEqual(codexAutoProbeConfigArgs(args), undefined);
});

it.effect("waits for a new Codex catalog and performs only contextual reads", () =>
  Effect.gen(function* () {
    const { helpers, spawner } = makeFakeCodexAppServers({
      "account/read": {
        account: { type: "chatgpt", email: "example@example.invalid", planType: "pro" },
        requiresOpenaiAuth: true,
      },
      "config/read": { config: { model_provider: "openai" }, origins: {} },
      "account/rateLimits/read": {
        accountId: " synthetic-codex ",
        ordinaryUsageAllowed: true,
        rateLimits: { limitId: "codex" },
      },
      "model/list": { data: [] },
    });
    const settings = yield* Schema.decodeEffect(CodexSettings)({
      binaryPath: "codex",
      launchArgs: "-c 'model_provider=\"openai\"'",
    });
    const input = {
      instanceId: ProviderInstanceId.make("synthetic-auto"),
      settings,
      environment: {},
      cwd: "/project",
    };
    const catalogWith = (fresh: boolean) =>
      FileSystem.layerNoop({
        readFileString: () =>
          DateTime.now.pipe(
            Effect.map((now) =>
              JSON.stringify({
                fetched_at: fresh ? DateTime.formatIso(now) : "1969-12-31T23:59:59.000Z",
              }),
            ),
          ),
      });
    const observe = (fresh: boolean) =>
      probeCodexAutoRouting(input).pipe(
        Effect.provideService(ChildProcessSpawner.ChildProcessSpawner, spawner),
        Effect.provide(Layer.mergeAll(catalogWith(fresh), Path.layer)),
      );
    const stale = yield* observe(false).pipe(Effect.forkChild);
    yield* TestClock.adjust("3 seconds");
    assert.strictEqual(yield* Fiber.join(stale), undefined);
    assert.strictEqual(helpers[0]!.methods.includes("model/list"), false);
    assert.strictEqual(helpers[0]!.closed, true);

    const current = yield* observe(true);
    assert.strictEqual(current?.accountIdentity, "synthetic-codex");
    assert.deepStrictEqual(helpers[1]!.methods, [
      "initialize",
      "account/read",
      "config/read",
      "config/read",
      "account/rateLimits/read",
      "model/list",
    ]);
    assert.deepStrictEqual(helpers[1]!.args, [
      "app-server",
      "-c",
      "thread_unload_delay_secs=0",
      "-c",
      'model_provider="openai"',
      ...codexAutoProbeConfigArgs("")!,
    ]);
  }).pipe(Effect.provideService(HostProcess.Platform, "linux")),
);

it.effect("reuses one Codex skills helper per launch until it idles out", () =>
  Effect.gen(function* () {
    const { helpers, spawner } = makeFakeCodexAppServers();
    const providerScope = yield* Scope.make();
    const probe = yield* makeCodexSkillsProbe().pipe(
      Effect.provideService(ChildProcessSpawner.ChildProcessSpawner, spawner),
      Scope.provide(providerScope),
    );
    const read = (cwd: string, homePath = "/home-a") =>
      probe({ binaryPath: "codex", homePath, cwd, environment: {} }).pipe(
        Effect.map((skills) => skills.map((skill) => skill.name)),
      );

    assert.deepStrictEqual(yield* read("/alpha"), ["skill-in-/alpha"]);
    assert.deepStrictEqual(yield* read("/beta"), ["skill-in-/beta"]);
    assert.strictEqual(helpers.length, 1);
    assert.deepStrictEqual(helpers[0]!.requests, [
      { cwds: ["/alpha"], forceReload: true },
      { cwds: ["/beta"], forceReload: true },
    ]);

    // Another home is another launch identity; the previous helper retires at once.
    assert.deepStrictEqual(yield* read("/alpha", "/home-b"), ["skill-in-/alpha"]);
    assert.strictEqual(helpers.length, 2);
    assert.strictEqual(helpers[0]!.closed, true);
    assert.strictEqual(helpers[1]!.codexHome, "/home-b");

    // A helper that exits is replaced within the same read.
    yield* helpers[1]!.crash;
    assert.deepStrictEqual(yield* read("/beta", "/home-b"), ["skill-in-/beta"]);
    assert.strictEqual(helpers.length, 3);

    yield* TestClock.adjust("29 seconds");
    assert.strictEqual(helpers[2]!.closed, false);
    yield* TestClock.adjust("1 second");
    assert.strictEqual(helpers[2]!.closed, true);
    assert.deepStrictEqual(yield* read("/alpha", "/home-b"), ["skill-in-/alpha"]);
    assert.strictEqual(helpers.length, 4);

    yield* Scope.close(providerScope, Exit.void);
    assert.strictEqual(helpers[3]!.closed, true);
  }).pipe(Effect.provideService(HostProcess.Platform, "linux")),
);

it.each(["openai.gpt-6.2-astra", "gpt-6-astra"])(
  "uses medium as the Jones default effort for %s",
  (model) => {
    const capabilities = mapCodexModelCapabilities({
      additionalSpeedTiers: [],
      defaultReasoningEffort: "high",
      description: "Astra",
      displayName: "Astra",
      hidden: false,
      id: model,
      isDefault: false,
      model,
      supportedReasoningEfforts: ["medium", "high", "xhigh", "max"].map((reasoningEffort) => ({
        reasoningEffort,
        description: reasoningEffort,
      })),
    });
    const reasoning = capabilities.optionDescriptors?.find(({ id }) => id === "reasoningEffort");
    assert.strictEqual(reasoning?.currentValue, "medium");
    assert.deepStrictEqual(
      reasoning?.type === "select" ? reasoning.options.map(({ id }) => id) : [],
      ["medium", "high", "xhigh", "max"],
    );
    assert.strictEqual(codexDefaultReasoningEffortOverride(model), "medium");
  },
);

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
    availableAccessPrograms: { cyber: ["standard", "daybreakBlue"] },
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
    {
      id: "cyberAccessProgram",
      label: "Cyber Access",
      type: "select",
      options: [
        { id: "standard", label: "Standard", isDefault: true },
        { id: "daybreakBlue", label: "Daybreak Blue" },
      ],
      currentValue: "standard",
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

it("prefers GPT-6.2 Astra and keeps GPT-6 Astra as the next catalog fallback", () => {
  const legacy = { slug: "gpt-6-astra", name: "GPT-6 Astra", isCustom: false, capabilities: null };
  const current = { ...legacy, slug: "openai.gpt-6.2-astra", name: "GPT-6.2 Astra" };
  const earlier = { ...legacy, slug: "gpt-5.6-sol", name: "GPT-5.6 Sol", isDefault: true };

  const models = applyPreferredCodexDefaultModel([earlier, legacy, current]);
  assert.deepStrictEqual(
    models.filter((model) => model.isDefault).map(({ slug }) => slug),
    [current.slug],
  );
  assert.deepStrictEqual(
    models.find(({ slug }) => slug === legacy.slug),
    legacy,
  );
  assert.strictEqual(
    applyPreferredCodexDefaultModel([earlier, legacy]).find((model) => model.isDefault)?.slug,
    legacy.slug,
  );
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

it("ignores custom models that shadow a preferred slug", () => {
  const models = applyPreferredCodexDefaultModel([
    { slug: "gpt-5.6-sol", name: "gpt-5.6-sol", isCustom: true, capabilities: null },
    { slug: "gpt-5.4", name: "GPT-5.4", isCustom: false, isDefault: true, capabilities: null },
  ]);

  assert.deepStrictEqual(models.find((model) => model.isDefault)?.slug, "gpt-5.4");
});

it("reports native quota only for an OpenAI login on a native route", () => {
  const chatgpt = { type: "chatgpt", email: "a@example.com", planType: "pro" } as const;
  const hub = {
    model_provider: "hub",
    model_providers: { hub: { base_url: "https://hub.example.com/v1" } },
  };
  const cases = [
    ["default route", chatgpt, true, {}, true],
    ["openai base url", chatgpt, true, { openai_base_url: "https://chatgpt.com/x" }, true],
    ["openai proxy url", chatgpt, true, { openai_base_url: "https://proxy.example.com" }, false],
    ["custom provider", chatgpt, true, hub, false],
    ["unknown provider id", chatgpt, true, { model_provider: "missing" }, false],
    ["unreadable config", chatgpt, true, null, false],
    ["login not required", chatgpt, false, {}, false],
    ["api key", { type: "apiKey" }, true, {}, false],
  ] as const;
  for (const [name, account, requiresOpenaiAuth, config, expected] of cases) {
    assert.strictEqual(
      codexRoutesSpendNativeQuota({ account, requiresOpenaiAuth }, config),
      expected,
      name,
    );
  }
  // The custom endpoint is what a usage-limit hub is matched against.
  assert.strictEqual(codexCustomEndpoint(hub), "https://hub.example.com");
  assert.strictEqual(
    codexCustomEndpoint({ openai_base_url: "http://127.0.0.1:8317/v1" }),
    "http://127.0.0.1:8317",
  );
  assert.strictEqual(codexCustomEndpoint({ openai_base_url: "https://chatgpt.com/x" }), undefined);
  assert.strictEqual(codexCustomEndpoint({}), undefined);
});

const compiledDecodeEffect1 = Schema.decodeEffect(CodexSettings);
const compiledEncodeEffect2 = Schema.encodeEffect(Schema.fromJsonString(Schema.Unknown));

it.effect(
  "publishes qualified full-map quota with receipt provenance and fresh model capabilities",
  () =>
    Effect.gen(function* () {
      const receipt = "2026-09-30T12:00:01.000Z";
      const map = {
        codex: { primary: { usedPercent: 40, windowDurationMins: 300, resetsAt: 1791000000 } },
        other: { secondary: { usedPercent: 10, windowDurationMins: 10080, resetsAt: 1792000000 } },
      };
      const settings = yield* compiledDecodeEffect1({});
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
      const serializedQuota = yield* compiledEncodeEffect2(status.qualifiedQuota ?? null);
      assert.strictEqual(serializedQuota.includes("private@example.com"), false);
      assert.strictEqual(serializedQuota.includes("private-quota-account-sentinel"), false);
      assert.strictEqual(serializedQuota.includes("ordinaryUsageAllowed"), false);
      assert.strictEqual(serializedQuota.includes("bindingGeneration"), false);
    }).pipe(
      Effect.provide(layerProbePlatform),
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
      const settings = yield* compiledDecodeEffect1({});
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
      const serializedQuota = yield* compiledEncodeEffect2(status.qualifiedQuota ?? null);
      assert.strictEqual(serializedQuota.includes("private fixture error"), false);
    }).pipe(
      Effect.provide(layerProbePlatform),
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
      const settings = yield* compiledDecodeEffect1({});
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
      const json = yield* compiledEncodeEffect2(result.qualifiedQuota);
      assert.strictEqual(json.includes("private-native-account"), false);
      assert.strictEqual(json.includes("private-managed-email"), false);
      assert.strictEqual(json.includes("native-sentinel"), false);
    }).pipe(
      Effect.provide(layerProbePlatform),
      Effect.provideService(
        ChildProcessSpawner.ChildProcessSpawner,
        ChildProcessSpawner.make(() => Effect.die("unexpected spawn")),
      ),
    ),
);

it.effect("a bounded whole-probe timeout cannot qualify a partial quota response", () =>
  Effect.gen(function* () {
    const settings = yield* compiledDecodeEffect1({});
    const fiber = yield* checkCodexProviderStatus(settings, () => Effect.never).pipe(
      Effect.forkChild,
    );
    // Codex has its own 20-second handshake budget.
    yield* TestClock.adjust("21 seconds");
    const result = yield* Fiber.join(fiber);
    assert.strictEqual(result.qualifiedQuota?.status, "failed");
    assert.strictEqual(result.qualifiedQuota?.failureCode, "probe_timeout");
    assert.strictEqual(result.qualifiedQuota?.quotaReceivedAt, null);
    assert.strictEqual(result.qualifiedQuota?.rateLimitsByLimitId, null);
  }).pipe(
    Effect.provide(layerProbePlatform),
    Effect.provideService(
      ChildProcessSpawner.ChildProcessSpawner,
      ChildProcessSpawner.make(() => Effect.die("unexpected spawn")),
    ),
  ),
);
