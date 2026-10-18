import * as NodeServices from "@effect/platform-node/NodeServices";
import { expect, it } from "@effect/vitest";
import {
  DEFAULT_SERVER_SETTINGS,
  EnvironmentId,
  ProjectId,
  ProviderInstanceId,
  type ServerSettings,
  ThreadId,
} from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import { HttpServer } from "effect/http";
import * as NetAddress from "effect/net/NetAddress";
import { describe } from "vite-plus/test";

import * as ServerEnvironment from "../../../environment/ServerEnvironment.ts";
import type { McpCapability } from "../../../mcp/McpInvocationContext.ts";
import * as McpSessionRegistry from "../../../mcp/McpSessionRegistry.ts";
import {
  DECISION_SNAPSHOT_CAPABILITY,
  mcpGrantMatches,
  resolveAgentDecisionSnapshotAccess,
} from "./access.ts";

const projectId = ProjectId.make("project-decision-snapshot-access");
const otherProjectId = ProjectId.make("project-decision-snapshot-other");

const settingsWith = (patch: Partial<ServerSettings>): ServerSettings => ({
  ...DEFAULT_SERVER_SETTINGS,
  ...patch,
});

// Mirrors the hook in ProviderSessionManager: fixed thread tools plus each
// optional capability the resolved settings grant.
const grantFor = (decisionSnapshot: boolean) => {
  const grant = new Set<McpCapability>(["orchestration", "worktree", "pull-requests", "preview"]);
  if (decisionSnapshot) grant.add(DECISION_SNAPSHOT_CAPABILITY);
  return grant;
};

describe("agent decision snapshot access", () => {
  it("is off by default, including for settings written before the key existed", () => {
    expect(DEFAULT_SERVER_SETTINGS.enableAgentDecisionSnapshotAccess).toBe(false);
    expect(resolveAgentDecisionSnapshotAccess(DEFAULT_SERVER_SETTINGS, projectId, true)).toBe(
      false,
    );
    expect(resolveAgentDecisionSnapshotAccess(DEFAULT_SERVER_SETTINGS, projectId, false)).toBe(
      false,
    );
  });

  it("follows the environment opt-in and lets a project override it either way", () => {
    const globallyOn = settingsWith({ enableAgentDecisionSnapshotAccess: true });
    expect(resolveAgentDecisionSnapshotAccess(globallyOn, projectId, true)).toBe(true);

    const projectOff = settingsWith({
      enableAgentDecisionSnapshotAccess: true,
      projectSettingsOverrides: { [projectId]: { enableAgentDecisionSnapshotAccess: false } },
    });
    expect(resolveAgentDecisionSnapshotAccess(projectOff, projectId, true)).toBe(false);
    expect(resolveAgentDecisionSnapshotAccess(projectOff, otherProjectId, true)).toBe(true);

    const projectOn = settingsWith({
      projectSettingsOverrides: { [projectId]: { enableAgentDecisionSnapshotAccess: true } },
    });
    expect(resolveAgentDecisionSnapshotAccess(projectOn, projectId, true)).toBe(true);
    expect(resolveAgentDecisionSnapshotAccess(projectOn, otherProjectId, true)).toBe(false);
  });

  it("withholds access from an unresolved project whenever any project overrides it", () => {
    const projectOff = settingsWith({
      enableAgentDecisionSnapshotAccess: true,
      projectSettingsOverrides: { [projectId]: { enableAgentDecisionSnapshotAccess: false } },
    });
    expect(resolveAgentDecisionSnapshotAccess(projectOff, projectId, false)).toBe(false);
    const noOverride = settingsWith({ enableAgentDecisionSnapshotAccess: true });
    expect(resolveAgentDecisionSnapshotAccess(noOverride, projectId, false)).toBe(true);
  });

  it("compares the complete grant so any optional capability change rotates", () => {
    expect(mcpGrantMatches(grantFor(false), grantFor(false))).toBe(true);
    expect(mcpGrantMatches(grantFor(true), grantFor(true))).toBe(true);
    expect(mcpGrantMatches(grantFor(false), grantFor(true))).toBe(false);
    expect(mcpGrantMatches(grantFor(true), grantFor(false))).toBe(false);
    const withDevice = grantFor(false).add("device");
    expect(mcpGrantMatches(withDevice, grantFor(false))).toBe(false);
  });
});

const environmentId = EnvironmentId.make("environment-decision-snapshot-access");
const makeRegistry = McpSessionRegistry.__testing.make({ now: () => 1_000 }).pipe(
  Effect.provideService(
    HttpServer.HttpServer,
    HttpServer.HttpServer.of({
      address: NetAddress.inetAddressFromIpStringUnsafe("127.0.0.1", 43124),
      serve: (() => Effect.void) as HttpServer.HttpServer["Service"]["serve"],
    }),
  ),
  Effect.provideService(
    ServerEnvironment.ServerEnvironment,
    ServerEnvironment.ServerEnvironment.of({
      getEnvironmentId: Effect.succeed(environmentId),
      getDescriptor: Effect.die("unused"),
    }),
  ),
  Effect.provide(NodeServices.layer),
);

it.effect("an opt-in change invalidates the issued grant and the reissued token carries it", () =>
  Effect.gen(function* () {
    const registry = yield* makeRegistry;
    const request = {
      threadId: ThreadId.make("thread-decision-snapshot-access"),
      providerInstanceId: ProviderInstanceId.make("codex"),
    };
    const scopeOf = (credential: McpSessionRegistry.McpIssuedCredential) =>
      registry.resolve(credential.config.authorizationHeader.replace(/^Bearer\s+/, ""));

    const before = yield* registry.issue({ ...request, capabilities: grantFor(false) });
    const beforeScope = yield* scopeOf(before);
    expect(beforeScope?.capabilities.has(DECISION_SNAPSHOT_CAPABILITY)).toBe(false);
    expect(mcpGrantMatches(beforeScope!.capabilities, grantFor(false))).toBe(true);

    const optedIn = settingsWith({ enableAgentDecisionSnapshotAccess: true });
    const current = grantFor(resolveAgentDecisionSnapshotAccess(optedIn, projectId, true));
    expect(mcpGrantMatches(beforeScope!.capabilities, current)).toBe(false);

    yield* registry.revokeThread(request.threadId);
    const after = yield* registry.issue({ ...request, capabilities: current });
    expect(yield* scopeOf(before)).toBeUndefined();
    const afterScope = yield* scopeOf(after);
    expect(afterScope?.capabilities.has(DECISION_SNAPSHOT_CAPABILITY)).toBe(true);
    expect(mcpGrantMatches(afterScope!.capabilities, current)).toBe(true);
    expect(mcpGrantMatches(afterScope!.capabilities, grantFor(false))).toBe(false);
  }),
);
