import { describe, expect, it } from "@effect/vitest";
import {
  MessageId,
  ProjectId,
  ProviderDriverKind,
  ProviderInstanceId,
  RunId,
  ThreadId,
  type ModelSelection,
  type ServerProvider,
} from "@t3tools/contracts";
import * as Effect from "effect/Effect";

import type { ProviderSwitchPlanV2 } from "../../orchestration-v2/ProviderSwitchService.ts";
import {
  readmitCodexFailoverTarget,
  selectCodexFailoverTarget,
  type CodexFailoverAdmissionInput,
  type CodexFailoverMetadata,
} from "./CodexFailoverAdmission.ts";
import { codexFailoverChain, type CodexFailoverChainStep } from "./CodexFailoverEligibility.ts";

const source = ProviderInstanceId.make("codex_A");
const target = ProviderInstanceId.make("codex_B");
const projectId = ProjectId.make("failover-project");
const now = Date.parse("2026-10-26T20:00:00.000Z");
const savedSelection: ModelSelection = {
  instanceId: source,
  model: "saved-model",
  options: [{ id: "effort", value: "high" }],
};
const continuationSelection: ModelSelection = {
  instanceId: source,
  model: "origin-model",
  options: [{ id: "effort", value: "max" }],
};

function provider(id: string, overrides: Partial<ServerProvider> = {}): ServerProvider {
  return {
    instanceId: ProviderInstanceId.make(id),
    driver: ProviderDriverKind.make("codex"),
    enabled: true,
    installed: true,
    version: "synthetic",
    status: "ready",
    auth: { status: "authenticated" },
    checkedAt: "2026-10-26T20:00:00.000Z",
    models: ["saved-model", "origin-model"].map((slug) => ({
      slug,
      name: slug,
      isCustom: false,
      capabilities: {
        optionDescriptors: [
          {
            id: "effort",
            label: "Effort",
            type: "select",
            options: ["high", "max"].map((id) => ({ id, label: id })),
          },
        ],
      },
    })),
    slashCommands: [],
    skills: [],
    ...overrides,
  };
}

function metadata(
  id: string,
  overrides: Partial<CodexFailoverMetadata> = {},
): CodexFailoverMetadata {
  return {
    instanceId: ProviderInstanceId.make(id),
    driver: "codex",
    continuationKey: "codex:shared-synthetic-home",
    enabled: true,
    ...overrides,
  };
}

type Input = CodexFailoverAdmissionInput<string, never> & {
  readonly visited: ReadonlySet<ProviderInstanceId>;
};
function input(ids: ReadonlyArray<string> = ["codex_A", "codex_B", "codex_C"]): Input {
  return {
    savedSelection,
    continuationSelection,
    providers: ids.map((id) => provider(id)),
    metadata: ids.map((id) => metadata(id)),
    settings: { projectSettingsOverrides: {}, providerModelPreferences: null },
    projectId,
    now,
    visited: new Set(),
    plan: () => Effect.succeed({ transition: { type: "restart_and_resume" } }),
  };
}

describe("Codex failover target admission", () => {
  it.effect(
    "chooses the first sorted admissible proposal and preserves both selections independently",
    () =>
      Effect.gen(function* () {
        const planned: Array<ModelSelection> = [];
        const args = input(["codex_C", "codex_B", "codex_A"]);
        const result = yield* selectCodexFailoverTarget({
          ...args,
          plan: (selection) => {
            planned.push(selection);
            return args.plan(selection);
          },
        });
        expect(result).toEqual({
          savedSelection: { ...savedSelection, instanceId: target },
          continuationSelection: { ...continuationSelection, instanceId: target },
        });
        expect(planned).toEqual([{ ...continuationSelection, instanceId: target }]);
        expect(args.savedSelection.instanceId).toBe(source);
        expect(args.continuationSelection.instanceId).toBe(source);
      }),
  );

  const rejectionCases: Array<{ name: string; change: (args: Input) => Input }> = [
    {
      name: "known unauthenticated account",
      change: (i) => ({
        ...i,
        providers: [
          provider("codex_A"),
          provider("codex_B", { auth: { status: "unauthenticated" } }),
        ],
      }),
    },
    {
      name: "known unavailable account",
      change: (i) => ({
        ...i,
        providers: [provider("codex_A"), provider("codex_B", { availability: "unavailable" })],
      }),
    },
    {
      name: "uninstalled account",
      change: (i) => ({
        ...i,
        providers: [provider("codex_A"), provider("codex_B", { installed: false })],
      }),
    },
    {
      name: "disabled live snapshot",
      change: (i) => ({
        ...i,
        providers: [provider("codex_A"), provider("codex_B", { enabled: false })],
      }),
    },
    {
      name: "disabled metadata",
      change: (i) => ({
        ...i,
        metadata: [metadata("codex_A"), metadata("codex_B", { enabled: false })],
      }),
    },
    { name: "missing live snapshot", change: (i) => ({ ...i, providers: [provider("codex_A")] }) },
    { name: "missing target metadata", change: (i) => ({ ...i, metadata: [metadata("codex_A")] }) },
    { name: "missing source metadata", change: (i) => ({ ...i, metadata: [metadata("codex_B")] }) },
    {
      name: "unknown source continuation identity",
      change: (i) => ({
        ...i,
        metadata: [metadata("codex_A", { continuationKey: null }), metadata("codex_B")],
      }),
    },
    {
      name: "incompatible continuation identity",
      change: (i) => ({
        ...i,
        metadata: [
          metadata("codex_A"),
          metadata("codex_B", { continuationKey: "codex:other-home" }),
        ],
      }),
    },
    {
      name: "different driver metadata",
      change: (i) => ({
        ...i,
        metadata: [metadata("codex_A"), metadata("codex_B", { driver: "claudeAgent" })],
      }),
    },
    {
      name: "different driver snapshot",
      change: (i) => ({
        ...i,
        providers: [
          provider("codex_A"),
          provider("codex_B", { driver: ProviderDriverKind.make("claudeAgent") }),
        ],
      }),
    },
    {
      name: "project disable",
      change: (i) => ({
        ...i,
        settings: {
          ...i.settings,
          projectSettingsOverrides: {
            [projectId]: { providerInstances: { [target]: { enabled: false } } },
          },
        },
      }),
    },
    {
      name: "missing saved model",
      change: (i) => ({
        ...i,
        providers: [
          provider("codex_A"),
          provider("codex_B", {
            models: provider("codex_B").models.filter(
              (model) => model.slug !== savedSelection.model,
            ),
          }),
        ],
      }),
    },
    {
      name: "missing continuation model",
      change: (i) => ({
        ...i,
        providers: [
          provider("codex_A"),
          provider("codex_B", {
            models: provider("codex_B").models.filter(
              (model) => model.slug !== continuationSelection.model,
            ),
          }),
        ],
      }),
    },
    {
      name: "invalid saved options",
      change: (i) => ({
        ...i,
        savedSelection: { ...i.savedSelection, options: [{ id: "effort", value: "invalid" }] },
      }),
    },
    {
      name: "invalid continuation options",
      change: (i) => ({
        ...i,
        continuationSelection: {
          ...i.continuationSelection,
          options: [{ id: "effort", value: "invalid" }],
        },
      }),
    },
    {
      name: "hidden saved model",
      change: (i) => ({
        ...i,
        settings: {
          ...i.settings,
          providerModelPreferences: {
            [target]: { hiddenModels: [savedSelection.model], modelOrder: [] },
          },
        },
      }),
    },
    {
      name: "hidden continuation model",
      change: (i) => ({
        ...i,
        settings: {
          ...i.settings,
          providerModelPreferences: {
            [target]: { hiddenModels: [continuationSelection.model], modelOrder: [] },
          },
        },
      }),
    },
    { name: "already visited instance", change: (i) => ({ ...i, visited: new Set([target]) }) },
  ];
  it.effect.each(rejectionCases)("skips $name before planning", ({ change }) =>
    Effect.gen(function* () {
      const planned: Array<ModelSelection> = [];
      const args = change(input(["codex_A", "codex_B"]));
      const result = yield* selectCodexFailoverTarget({
        ...args,
        plan: (selection) => {
          planned.push(selection);
          return args.plan(selection);
        },
      });
      expect(result).toBeNull();
      expect(planned).toEqual([]);
    }),
  );

  it.effect.each([
    { usedPercent: 100, resetsAt: "2026-10-26T20:00:00.001Z", rejected: true },
    { usedPercent: 100, resetsAt: "2026-10-26T20:00:00.000Z", rejected: false },
    { usedPercent: 100, resetsAt: "2026-10-26T19:00:00.000Z", rejected: false },
    { usedPercent: 100, resetsAt: undefined, rejected: false },
    { usedPercent: 99, resetsAt: "2026-10-26T21:00:00.000Z", rejected: false },
  ])(
    "requires an exhausted window with a future reset: $usedPercent / $resetsAt",
    ({ usedPercent, resetsAt, rejected }) =>
      Effect.gen(function* () {
        const args = input(["codex_A", "codex_B"]);
        const result = yield* selectCodexFailoverTarget({
          ...args,
          providers: [
            provider("codex_A"),
            provider("codex_B", {
              usageLimits: {
                checkedAt: "2026-10-26T20:00:00.000Z",
                windows: [
                  {
                    id: "weekly",
                    label: "Weekly",
                    kind: "weekly",
                    usedPercent,
                    ...(resetsAt === undefined ? {} : { resetsAt }),
                  },
                ],
              },
            }),
          ],
        });
        expect(result === null).toBe(rejected);
      }),
  );

  it.effect(
    "does not treat unknown auth, unavailable usage probes or warning status as known exhaustion",
    () =>
      Effect.gen(function* () {
        const args = input(["codex_A", "codex_B"]);
        const result = yield* selectCodexFailoverTarget({
          ...args,
          providers: [
            provider("codex_A"),
            provider("codex_B", {
              auth: { status: "unknown" },
              status: "warning",
              usageLimits: {
                checkedAt: "2026-10-26T20:00:00.000Z",
                windows: [],
                unavailable: { reason: "probeFailed" },
              },
            }),
          ],
        });
        expect(result?.savedSelection.instanceId).toBe(target);
      }),
  );

  const transitions: Array<ProviderSwitchPlanV2["transition"]> = [
    { type: "reuse" },
    { type: "switch_model_in_session" },
    { type: "create_with_handoff" },
    { type: "reject", reason: "incompatible" },
  ];
  it.effect.each(transitions)("skips a $type plan and keeps looking", (transition) =>
    Effect.gen(function* () {
      const planned: Array<ProviderInstanceId> = [];
      const result = yield* selectCodexFailoverTarget({
        ...input(),
        plan: (selection) => {
          planned.push(selection.instanceId);
          return Effect.succeed({
            transition:
              selection.instanceId === target ? transition : { type: "restart_and_resume" },
          });
        },
      });
      expect(result?.savedSelection.instanceId).toBe("codex_C");
      expect(planned).toEqual([target, "codex_C"]);
    }),
  );

  it.effect(
    "skips a typed plan refusal and exhausts when no restart-and-resume plan succeeds",
    () =>
      Effect.gen(function* () {
        const args = input();
        const result = yield* selectCodexFailoverTarget({
          ...args,
          plan: (selection) =>
            selection.instanceId === target ? Effect.fail("plan_refused") : args.plan(selection),
        });
        expect(result?.savedSelection.instanceId).toBe("codex_C");
        expect(
          yield* selectCodexFailoverTarget({ ...args, plan: () => Effect.fail("plan_refused") }),
        ).toBeNull();
      }),
  );

  it.effect(
    "re-reads a dynamic pool beyond eight accounts, tries newly added accounts once and skips disabled ones",
    () =>
      Effect.gen(function* () {
        const threadId = ThreadId.make("dynamic-chain-thread");
        const chainRootRunId = RunId.make("chain-root");
        const ids = Array.from(
          { length: 10 },
          (_, index) => `codex_${String(index).padStart(2, "0")}`,
        );
        const steps: Array<CodexFailoverChainStep> = [];
        const tried: Array<ProviderInstanceId> = [];
        for (let index = 0; index < ids.length; index += 1) {
          const instanceId = ProviderInstanceId.make(ids[index]!);
          tried.push(instanceId);
          const origin = {
            id: index === 0 ? chainRootRunId : RunId.make(`run-${index}`),
            userMessageId:
              index === 0 ? MessageId.make("original") : steps[index - 1]!.continueMessageId,
            providerInstanceId: instanceId,
          };
          const chain = codexFailoverChain({ threadId, origin, steps, usageResets: [], now });
          expect(chain.chainRootRunId).toBe(chainRootRunId);
          const result = yield* selectCodexFailoverTarget({
            ...input([...ids, "codex_disabled"]),
            savedSelection: { ...savedSelection, instanceId },
            continuationSelection: { ...continuationSelection, instanceId },
            visited: chain.visited,
            providers: [
              ...ids.map((id) => provider(id)),
              provider("codex_disabled", { enabled: false }),
            ],
          });
          if (index + 1 < ids.length) {
            expect(result?.savedSelection.instanceId).toBe(ids[index + 1]);
          } else {
            expect(result).toBeNull();
            const expanded = input([...ids, "codex_new"]);
            const added = yield* selectCodexFailoverTarget({
              ...expanded,
              savedSelection: { ...savedSelection, instanceId },
              continuationSelection: { ...continuationSelection, instanceId },
              visited: chain.visited,
            });
            expect(added?.savedSelection.instanceId).toBe("codex_new");
            expect(
              yield* selectCodexFailoverTarget({
                ...expanded,
                savedSelection: { ...savedSelection, instanceId },
                continuationSelection: { ...continuationSelection, instanceId },
                visited: new Set([...chain.visited, ProviderInstanceId.make("codex_new")]),
              }),
            ).toBeNull();
          }
          steps.push({
            threadId,
            chainRootRunId,
            originRunId: origin.id,
            continueMessageId: MessageId.make(`continue-${index}`),
            sourceInstanceId: instanceId,
          });
        }
        expect(new Set(tried).size).toBe(10);
      }),
  );
});

describe("Codex failover post-switch target re-admission", () => {
  it.effect("checks the exact switched target without requiring another proposal", () =>
    Effect.gen(function* () {
      const args = input(["codex_B"]);
      const targetSelections = {
        savedSelection: { ...savedSelection, instanceId: target },
        continuationSelection: { ...continuationSelection, instanceId: target },
      };
      expect(yield* readmitCodexFailoverTarget({ ...args, ...targetSelections })).toEqual(
        targetSelections,
      );
      expect(
        yield* readmitCodexFailoverTarget({
          ...args,
          ...targetSelections,
          providers: [provider("codex_B", { auth: { status: "unauthenticated" } })],
        }),
      ).toBeNull();
      expect(
        yield* readmitCodexFailoverTarget({ ...args, ...targetSelections, continuationSelection }),
      ).toBeNull();
      expect(
        yield* readmitCodexFailoverTarget({
          ...args,
          ...targetSelections,
          plan: () => Effect.succeed({ transition: { type: "reuse" } }),
        }),
      ).toBeNull();
    }),
  );
});
