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
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";

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
import { instanceA, makeCatalogHarness, providerSnapshot } from "./catalog.testkit.ts";

const modelWithVariant = {
  slug: "openai/catalog-model",
  name: "Catalog model",
  isCustom: false,
  capabilities: createModelCapabilities({
    optionDescriptors: [{
      id: "variant",
      label: "Reasoning",
      type: "select",
      options: [{ id: "high", label: "High" }],
    }],
  }),
} satisfies ServerProvider["models"][number];
const emptyOptions = { ...modelWithVariant, capabilities: createModelCapabilities({ optionDescriptors: [] }) };
const previous = { ...providerSnapshot(), models: [modelWithVariant] };

describe("OpenCode completed option discovery (Audit 4.5 / R5)", () => {
  it("clears completed explicit empty options and retains absent, incomplete, error and Cursor metadata", () => {
    const cleared = ProviderRegistry.mergeProviderSnapshot(previous, { ...previous, models: [emptyOptions] });
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

  it.effect("the public catalog resolver refuses an explicit removed variant without changing saved intent", () =>
    Effect.gen(function* () {
      const harness = yield* makeCatalogHarness({
        configureSource: (source) => { source.provider = previous; },
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
        runs: [{
          id: RunId.make("run:catalog-options-parent"),
          ordinal: 1,
          status: "running",
          rootNodeId: NodeId.make("node:catalog-options-parent"),
          providerInstanceId: instanceA,
          modelSelection: savedSelection,
        } as OrchestrationV2Run],
      };
      const scope: McpInvocationScope = {
        environmentId: EnvironmentId.make("environment:catalog-options"),
        requestNamespace: "catalog-options",
        capabilities: new Set(["orchestration"]),
        issuedAt: 1,
        client: undefined,
        thread: { threadId: parentThreadId, providerSessionId: "synthetic", providerInstanceId: instanceA },
      };
      const commands: Array<OrchestrationV2ServerCommand> = [];
      const dependencies = Layer.mergeAll(
        Layer.succeed(ProviderRegistry.ProviderRegistry, harness.registry),
        Layer.mock(ProviderAdapterRegistry.ProviderAdapterRegistryV2)({ list: () => Effect.succeed([instanceA]) }),
        Layer.mock(ThreadManagementService.ThreadManagementService)({
          getThreadRecords: () => Effect.succeed(parentProjection),
          getCommandReceipt: () => Effect.succeedNone,
          isProviderInstanceEnabledForProject: () => Effect.succeed(true),
          assertProviderInstanceEnabled: () => Effect.void,
          dispatch: (command) => Effect.sync(() => { commands.push(command); }).pipe(
            Effect.andThen(Effect.fail(new ThreadManagementService.ThreadManagementProjectionLoadError({
              projectId: parent.thread.projectId,
              threadId: parentThreadId,
              cause: new Error("Synthetic dispatch boundary reached after catalog validation."),
            }))),
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
          threads: [{ prompt: "Catalog validation only.", target: {
            providerInstanceId: instanceA,
            model: modelWithVariant.slug,
            options: [{ id: "variant", value: "high" }],
          } }],
          clientRequestId: "catalog-options-before",
        } as const;
        const before = yield* service.createThreads(scope, request).pipe(Effect.flip);
        assert.equal(before.code, "orchestration_error");
        assert.equal(commands.length, 1);
        const admitted = commands[0];
        assert.equal(admitted?.type, "thread.create");
        if (admitted?.type === "thread.create") assert.deepStrictEqual(admitted.modelSelection, savedSelection);

        harness.source().provider = { ...previous, models: [emptyOptions] };
        const providers = yield* harness.registry.refreshInstance(instanceA);
        assert.deepStrictEqual(providers[0]?.models[0]?.capabilities?.optionDescriptors, []);
        const after = yield* service.createThreads(scope, {
          ...request, clientRequestId: "catalog-options-after",
        }).pipe(Effect.flip);
        assert.equal(after.code, "invalid_request");
        assert.include(after.message, "Unknown option variant");
        assert.equal(commands.length, 1);
        assert.strictEqual(parentProjection.thread.modelSelection, savedSelection);
        assert.deepStrictEqual(savedSelection.options, [{ id: "variant", value: "high" }]);
      }).pipe(Effect.provide(OrchestratorMcpService.layer.pipe(Layer.provide(dependencies))));
    }).pipe(Effect.scoped, Effect.provide(NodeServices.layer)),
  );
});
