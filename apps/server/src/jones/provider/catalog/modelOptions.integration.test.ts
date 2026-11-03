import * as NodeServices from "@effect/platform-node/NodeServices";
import { assert, describe, it } from "@effect/vitest";
import {
  EnvironmentId,
  NodeId,
  ProviderDriverKind,
  RunId,
  ThreadId,
  type OrchestrationV2Run,
  type OrchestrationV2ServerCommand,
  type ServerProvider,
} from "@t3tools/contracts";
import { createModelCapabilities } from "@t3tools/shared/model";
import { OpenCodeSettings } from "@t3tools/provider-opencode/settings";
import * as OpenCodeRuntime from "@t3tools/provider-opencode/server/OpenCodeRuntime";
import * as OpenCodeServerOwner from "@t3tools/provider-opencode/server/OpenCodeServerOwner";
import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";
import * as Layer from "effect/Layer";
import * as Queue from "effect/Queue";
import * as Schema from "effect/Schema";
import * as TestClock from "effect/testing/TestClock";

import {
  checkOpenCodeProviderStatus,
  makeOpenCode2ModelLoader,
  type OpenCode2Model,
} from "../../../../../../packages/provider-opencode/src/server/status.ts";

import type { McpInvocationScope } from "../../../mcp/McpInvocationContext.ts";
import { idleThreadProjection, liveThreadShell } from "../../../mcp/McpToolAccess.testkit.ts";
import * as OrchestratorMcpService from "../../../mcp/OrchestratorMcpService.ts";
import * as ProviderAdapterRegistry from "../../../orchestration-v2/ProviderAdapterRegistry.ts";
import * as ThreadLaunchService from "../../../orchestration-v2/ThreadLaunchService.ts";
import * as ThreadManagementService from "../../../orchestration-v2/ThreadManagementService.ts";
import * as RemoteDelegation from "../../../peer/RemoteDelegation.ts";
import * as ProjectService from "../../../project/ProjectService.ts";
import * as ProviderRegistry from "../../../provider/ProviderRegistry.ts";
import * as ScheduledTaskService from "../../../scheduledTasks/ScheduledTaskService.ts";
import * as SecretRequests from "../../../secrets/SecretRequests.ts";
import * as ServerSettings from "../../../serverSettings.ts";
import * as GitVcsDriver from "../../../vcs/GitVcsDriver.ts";
import { driverKind, instanceA, makeCatalogHarness, providerSnapshot } from "./catalog.testkit.ts";

const modelWithVariant = {
  slug: "openai/catalog-model",
  name: "Catalog model",
  isCustom: false,
  capabilities: createModelCapabilities({
    optionDescriptors: [
      {
        id: "variant",
        label: "Reasoning",
        type: "select",
        options: [{ id: "high", label: "High" }],
      },
    ],
  }),
} satisfies ServerProvider["models"][number];
const emptyOptions = {
  ...modelWithVariant,
  capabilities: createModelCapabilities({ optionDescriptors: [] }),
};
const previous = { ...providerSnapshot(), models: [modelWithVariant] };

describe("OpenCode completed option discovery (Audit 4.5 / R5)", () => {
  it("clears completed explicit empty options and retains absent, incomplete, error and Cursor metadata", () => {
    const cleared = ProviderRegistry.mergeProviderSnapshot(previous, {
      ...previous,
      models: [emptyOptions],
    });
    assert.deepStrictEqual(cleared.models[0]?.capabilities?.optionDescriptors, []);
    const incomplete = [
      { ...previous, models: [{ ...emptyOptions, capabilities: null }] },
      { ...previous, status: "error", models: [emptyOptions] },
      { ...previous, status: "warning", models: [emptyOptions] },
      { ...previous, installed: false, status: "warning", models: [emptyOptions] },
      { ...previous, enabled: false, status: "disabled", models: [emptyOptions] },
      { ...previous, driver: ProviderDriverKind.make("cursor"), models: [emptyOptions] },
    ] satisfies ReadonlyArray<ServerProvider>;
    for (const next of incomplete) {
      assert.deepStrictEqual(
        ProviderRegistry.mergeProviderSnapshot(previous, next).models[0]?.capabilities,
        modelWithVariant.capabilities,
      );
    }
  });

  it.effect(
    "real OpenCode discovery retains partial options and the public resolver refuses a completed removal without changing saved intent",
    () =>
      Effect.gen(function* () {
        const settings = Schema.decodeSync(OpenCodeSettings)({
          enabled: true,
          binaryPath: "synthetic-opencode",
        });
        const model: OpenCode2Model = {
          providerID: "openai",
          id: "catalog-model",
          name: "Catalog model",
          variants: [{ id: "high" }],
        };
        const reads = yield* Queue.unbounded<number>();
        const progress: {
          mode: "warm" | "error" | "timeout" | "complete";
          reads: number;
        } = { mode: "warm", reads: 0 };
        const load = yield* makeOpenCode2ModelLoader(
          Effect.suspend(() => {
            const ordinal = ++progress.reads;
            return Queue.offer(reads, ordinal).pipe(
              Effect.andThen(
                progress.mode === "error" && ordinal > 1
                  ? Effect.fail(
                      new OpenCodeRuntime.OpenCodeRuntimeError({
                        operation: "models",
                        detail: "Synthetic discovery error after partial progress",
                      }),
                    )
                  : progress.mode === "timeout" && ordinal > 1
                    ? Effect.never
                    : Effect.succeed([
                        progress.mode === "warm" ? model : { ...model, variants: [] },
                      ]),
              ),
            );
          }),
        );
        const producerDependencies = Layer.mergeAll(
          Layer.mock(OpenCodeRuntime.OpenCodeRuntime)({}),
          Layer.mock(OpenCodeServerOwner.OpenCodeServerOwner)({}),
        );
        const produce = Effect.fn("CatalogTest.produceOpenCodeStatus")(function* (
          mode: typeof progress.mode,
        ) {
          progress.mode = mode;
          progress.reads = 0;
          const reading = yield* checkOpenCodeProviderStatus(
            settings,
            "/synthetic-opencode-workspace",
            Effect.succeed({ generation: "v2", version: "2.0.28" }),
            load,
          ).pipe(Effect.provide(producerDependencies), Effect.forkChild);
          assert.equal(yield* Queue.take(reads), 1);
          yield* TestClock.adjust(250);
          assert.equal(yield* Queue.take(reads), 2);
          if (mode === "timeout") yield* TestClock.adjust(4_750);
          return { ...(yield* Fiber.join(reading)), instanceId: instanceA, driver: driverKind };
        });
        const warm = yield* produce("warm");
        assert.equal(warm.models[0]?.capabilities?.optionDescriptors?.[0]?.id, "variant");
        const harness = yield* makeCatalogHarness({
          configureSource: (source) => {
            source.provider = warm;
          },
        });
        const parentThreadId = ThreadId.make("thread:catalog-options-parent");
        const savedSelection = {
          instanceId: instanceA,
          model: modelWithVariant.slug,
          options: [{ id: "variant", value: "high" }],
        } as const;
        const parent = idleThreadProjection(liveThreadShell(parentThreadId));
        const parentProjection = {
          ...parent,
          thread: { ...parent.thread, modelSelection: savedSelection },
          runs: [
            {
              id: RunId.make("run:catalog-options-parent"),
              ordinal: 1,
              status: "running",
              rootNodeId: NodeId.make("node:catalog-options-parent"),
              providerInstanceId: instanceA,
              modelSelection: savedSelection,
            } as OrchestrationV2Run,
          ],
        };
        const scope: McpInvocationScope = {
          environmentId: EnvironmentId.make("environment:catalog-options"),
          requestNamespace: "catalog-options",
          capabilities: new Set(["orchestration"]),
          issuedAt: 1,
          client: undefined,
          thread: {
            threadId: parentThreadId,
            providerSessionId: "synthetic",
            providerInstanceId: instanceA,
          },
        };
        const commands: Array<OrchestrationV2ServerCommand> = [];
        const dependencies = Layer.mergeAll(
          Layer.succeed(ProviderRegistry.ProviderRegistry, harness.registry),
          Layer.mock(ProviderAdapterRegistry.ProviderAdapterRegistryV2)({
            list: () => Effect.succeed([instanceA]),
          }),
          Layer.mock(ThreadManagementService.ThreadManagementService)({
            getThreadRecords: () => Effect.succeed(parentProjection),
            getCommandReceipt: () => Effect.succeedNone,
            isProviderInstanceEnabledForProject: () => Effect.succeed(true),
            assertProviderInstanceEnabled: () => Effect.void,
            dispatch: (command) =>
              Effect.sync(() => {
                commands.push(command);
              }).pipe(
                Effect.andThen(
                  Effect.fail(
                    new ThreadManagementService.ThreadManagementProjectionLoadError({
                      projectId: parent.thread.projectId,
                      threadId: parentThreadId,
                      cause: new Error(
                        "Synthetic dispatch boundary reached after catalog validation.",
                      ),
                    }),
                  ),
                ),
              ),
          }),
          Layer.mock(ThreadLaunchService.ThreadLaunchService)({}),
          Layer.mock(GitVcsDriver.GitVcsDriver)({}),
          Layer.mock(ProjectService.ProjectService)({}),
          Layer.mock(ScheduledTaskService.ScheduledTaskService)({}),
          Layer.mock(SecretRequests.SecretRequests)({}),
          Layer.mock(RemoteDelegation.RemoteDelegation)({}),
          ServerSettings.layerTest(),
        );
        yield* Effect.gen(function* () {
          const service = yield* OrchestratorMcpService.OrchestratorMcpService;
          const request = {
            threads: [
              {
                prompt: "Catalog validation only.",
                target: {
                  providerInstanceId: instanceA,
                  model: modelWithVariant.slug,
                  options: [{ id: "variant", value: "high" }],
                },
              },
            ],
            clientRequestId: "catalog-options-before",
          } as const;
          const before = yield* service.createThreads(scope, request).pipe(Effect.flip);
          assert.equal(before.code, "orchestration_error");
          assert.equal(commands.length, 1);
          const admitted = commands[0];
          assert.equal(admitted?.type, "thread.create");
          if (admitted?.type === "thread.create")
            assert.deepStrictEqual(admitted.modelSelection, savedSelection);

          for (const incomplete of ["error", "timeout"] as const) {
            const partial = yield* produce(incomplete);
            assert.equal(partial.status, "ready");
            assert.isNull(partial.models[0]?.capabilities);
            harness.source().provider = partial;
            const retained = yield* harness.registry.refreshInstance(instanceA);
            assert.deepStrictEqual(
              retained[0]?.models[0]?.capabilities,
              warm.models[0]?.capabilities,
            );
          }
          harness.source().provider = yield* produce("complete");
          const providers = yield* harness.registry.refreshInstance(instanceA);
          assert.deepStrictEqual(providers[0]?.models[0]?.capabilities?.optionDescriptors, []);
          const after = yield* service
            .createThreads(scope, {
              ...request,
              clientRequestId: "catalog-options-after",
            })
            .pipe(Effect.flip);
          assert.equal(after.code, "invalid_request");
          assert.include(after.message, "Unknown option variant");
          assert.equal(commands.length, 1);
          assert.strictEqual(parentProjection.thread.modelSelection, savedSelection);
          assert.deepStrictEqual(savedSelection.options, [{ id: "variant", value: "high" }]);
        }).pipe(Effect.provide(OrchestratorMcpService.layer.pipe(Layer.provide(dependencies))));
      }).pipe(Effect.scoped, Effect.provide(NodeServices.layer)),
  );
});
