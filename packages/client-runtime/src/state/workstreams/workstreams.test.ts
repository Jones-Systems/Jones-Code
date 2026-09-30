import { describe, expect, it } from "vite-plus/test";

import { WorkstreamMetadataCache } from "./cache.ts";
import {
  WORKSTREAM_CONTRACT_MANIFEST,
  WORKSTREAM_CONTRACT_VERSION,
  type LifecycleDeclaration,
  type MembershipEpisode,
  type NativeReferenceRecord,
  type WorkstreamReadContext,
  type WorkstreamRecord,
} from "./model.ts";
import { projectWorkstreamDetail, selectOrderedWorkstreams } from "./projection.ts";
import { applyWorkstreamUpdate, createWorkstreamState } from "./reducer.ts";

function context(
  registryVersion: number,
  overrides: Partial<WorkstreamReadContext> = {},
): WorkstreamReadContext {
  return {
    registryId: "owner-registry",
    ownerScopeId: "owner-wide",
    principalId: "owner-principal",
    authorizationRevision: 7,
    serverGeneration: 2,
    registryVersion,
    contractVersion: WORKSTREAM_CONTRACT_VERSION,
    contractManifest: WORKSTREAM_CONTRACT_MANIFEST,
    ...overrides,
  };
}

function workstream(
  workstreamId: string,
  sortOrder: number,
  overrides: Partial<WorkstreamRecord> = {},
): WorkstreamRecord {
  return {
    workstreamId,
    name: workstreamId,
    lifecycle: "active",
    progress: { state: "progressing" },
    delivery: "none-observed",
    freshness: "current",
    sortOrder,
    version: 1,
    createdAt: "2026-09-12T12:00:00Z",
    updatedAt: "2026-09-12T12:00:00Z",
    ...overrides,
  };
}

function reference(
  nativeReferenceId: string,
  accountId: string,
  overrides: Partial<NativeReferenceRecord> = {},
): NativeReferenceRecord {
  return {
    nativeReferenceId,
    provider: "t3",
    sourceInstanceId: "vps-t3",
    resourceKind: "thread",
    idKind: "internal",
    nativeId: nativeReferenceId,
    accountProvenance: {
      kind: "account",
      accountId,
      accountNamespace: `chatgpt:${accountId}`,
    },
    registrationState: "attested",
    t3Locator: {
      kind: "thread",
      environmentId: `environment-${accountId}`,
      threadId: `thread-${nativeReferenceId}`,
      projectId: `project-${nativeReferenceId}`,
    },
    linkedPullRequest: null,
    ...overrides,
  };
}

function episode(
  membershipId: string,
  workstreamId: string,
  nativeReferenceId: string,
  kind: MembershipEpisode["kind"],
  registryVersion: number,
  overrides: Partial<MembershipEpisode> = {},
): MembershipEpisode {
  return {
    membershipId,
    workstreamId,
    nativeReferenceId,
    kind,
    opened: {
      at: "2026-09-12T12:00:00Z",
      commandId: `open-${membershipId}`,
      registryVersion,
    },
    closed: null,
    ...overrides,
  };
}

function appliedState(
  state: ReturnType<typeof createWorkstreamState>,
  update: Parameters<typeof applyWorkstreamUpdate>[1],
) {
  const result = applyWorkstreamUpdate(state, update);
  expect(result.kind).toBe("updated");
  return result.state;
}

describe("Workstream projection", () => {
  it("groups references from different accounts under one owner-wide workstream", () => {
    const first = reference("first", "personal");
    const second = reference("second", "work");
    const state = appliedState(createWorkstreamState(context(0)), {
      context: context(3),
      workstreams: [workstream("shared", 1)],
      nativeReferences: [first, second],
      memberships: [
        episode("primary", "shared", first.nativeReferenceId, "primary", 2),
        episode("secondary", "shared", second.nativeReferenceId, "secondary", 3),
      ],
    });

    const projection = projectWorkstreamDetail(state, "shared");
    expect(projection?.activePrimary[0]?.reference?.accountProvenance).toEqual(
      first.accountProvenance,
    );
    expect(projection?.activeSecondary[0]?.reference?.accountProvenance).toEqual(
      second.accountProvenance,
    );
    expect(projection?.activePrimary[0]?.reference?.t3Locator).toBe(first.t3Locator);
  });

  it("retains ended primary episodes when a resource moves and later reattaches", () => {
    const closed = episode("old-primary", "first", "thread", "primary", 2, {
      closed: {
        at: "2026-09-12T13:00:00Z",
        commandId: "move-primary-command",
        registryVersion: 4,
        reason: "continued-elsewhere",
        otherReason: null,
      },
    });
    let state = appliedState(createWorkstreamState(context(0)), {
      context: context(4),
      workstreams: [workstream("first", 1), workstream("second", 2)],
      nativeReferences: [reference("thread", "personal")],
      memberships: [closed, episode("moved-primary", "second", "thread", "primary", 4)],
    });
    state = appliedState(state, {
      context: context(6),
      memberships: [episode("reattached-primary", "first", "thread", "primary", 6)],
    });
    state = appliedState(state, {
      context: context(6),
      memberships: [episode("old-primary", "first", "thread", "primary", 2)],
    });

    const first = projectWorkstreamDetail(state, "first");
    expect(first?.membershipHistory.map(({ episode: item }) => item.membershipId)).toEqual([
      "old-primary",
      "reattached-primary",
    ]);
    expect(first?.activePrimary.map(({ episode: item }) => item.membershipId)).toEqual([
      "reattached-primary",
    ]);
    expect(projectWorkstreamDetail(state, "second")?.activePrimary).toHaveLength(1);
  });

  it("orders workstreams and preserves lifecycle reopening declarations", () => {
    const lifecycleDeclarations: LifecycleDeclaration[] = [
      {
        lifecycleDeclarationId: "completed",
        workstreamId: "later",
        revision: 1,
        priorLifecycle: "active",
        newLifecycle: "completed",
        recordedAt: "2026-09-12T13:00:00Z",
        commandId: "complete-command",
        supersedesLifecycleDeclarationId: null,
        registryVersion: 2,
      },
      {
        lifecycleDeclarationId: "reopened",
        workstreamId: "later",
        revision: 2,
        priorLifecycle: "completed",
        newLifecycle: "active",
        recordedAt: "2026-09-12T14:00:00Z",
        commandId: "reopen-command",
        supersedesLifecycleDeclarationId: "completed",
        registryVersion: 3,
      },
    ];
    const state = appliedState(createWorkstreamState(context(0)), {
      context: context(3),
      workstreams: [workstream("later", 20), workstream("earlier", 10)],
      lifecycleDeclarations: lifecycleDeclarations.toReversed(),
      edges: [
        {
          edgeId: "continuation",
          fromWorkstreamId: "earlier",
          toWorkstreamId: "later",
          relation: "continues_as",
          opened: {
            at: "2026-09-12T14:00:00Z",
            commandId: "continue-command",
            registryVersion: 3,
          },
          closed: null,
        },
      ],
    });

    expect(selectOrderedWorkstreams(state).map((item) => item.workstreamId)).toEqual([
      "earlier",
      "later",
    ]);
    const later = projectWorkstreamDetail(state, "later");
    expect(later?.lifecycleHistory).toEqual(lifecycleDeclarations);
    expect(later?.relationships[0]?.relation).toBe("continues_as");
  });

  it("keeps coordination disposition and native settlement receipts separate", () => {
    const state = appliedState(createWorkstreamState(context(0)), {
      context: context(4),
      workstreams: [workstream("target", 1), workstream("other", 2)],
      receipts: [
        {
          commandId: "coordination-command",
          operation: "set_coordination_disposition",
          workstreamIds: ["target"],
          state: "committed",
          acceptedAt: "2026-09-12T13:00:00Z",
          registryVersion: 3,
          changed: true,
          coordinationDisposition: {
            membershipId: "target-membership",
            disposition: "completed",
          },
          nativeSettlement: null,
        },
        {
          commandId: "settlement-command",
          operation: "request_native_t3_settlement",
          workstreamIds: ["target"],
          state: "committed",
          acceptedAt: "2026-09-12T13:00:01Z",
          registryVersion: 3,
          changed: false,
          coordinationDisposition: null,
          nativeSettlement: {
            nativeReferenceId: "target-thread",
            nativeAction: "settle",
            outcome: "unsupported",
          },
        },
        {
          commandId: "other-command",
          operation: "set_coordination_disposition",
          workstreamIds: ["other"],
          state: "committed",
          acceptedAt: "2026-09-12T13:00:02Z",
          registryVersion: 4,
          changed: true,
          coordinationDisposition: {
            membershipId: "other-membership",
            disposition: "completed",
          },
          nativeSettlement: null,
        },
      ],
    });

    const detail = projectWorkstreamDetail(state, "target");
    expect(detail?.coordinationDispositionReceipts.map((item) => item.commandId)).toEqual([
      "coordination-command",
    ]);
    expect(detail?.nativeT3SettlementReceipts.map((item) => item.commandId)).toEqual([
      "settlement-command",
    ]);
    expect(detail?.nativeT3SettlementReceipts[0]?.nativeSettlement?.outcome).toBe("unsupported");
  });

  it("projects linked PR refresh freshness without inferring a new association", () => {
    const lastSuccess = {
      state: "open" as const,
      draft: false,
      observedAt: "2026-09-12T12:00:00Z",
      providerUpdatedAt: "2026-09-12T11:59:00Z",
    };
    let state = appliedState(createWorkstreamState(context(0)), {
      context: context(2),
      workstreams: [workstream("target", 1)],
      nativeReferences: [
        reference("pull-request", "personal", {
          provider: "github",
          resourceKind: "pull_request",
          idKind: "external",
          nativeId: "jones-systems/t3code#42",
          t3Locator: null,
          linkedPullRequest: {
            host: "github.com",
            repositoryOwner: "jones-systems",
            repositoryName: "t3code",
            number: 42,
            observationVersion: 1,
            attemptedAt: "2026-09-12T12:00:00Z",
            outcome: "observed",
            lastSuccess,
          },
        }),
      ],
      memberships: [episode("linked-pr", "target", "pull-request", "secondary", 2)],
    });
    state = appliedState(state, {
      context: context(3),
      nativeReferences: [
        {
          ...state.nativeReferences["pull-request"]!,
          linkedPullRequest: {
            ...state.nativeReferences["pull-request"]!.linkedPullRequest!,
            observationVersion: 2,
            attemptedAt: "2026-09-12T13:00:00Z",
            outcome: "rate_limited",
            lastSuccess,
          },
        },
      ],
    });

    const pullRequest = projectWorkstreamDetail(state, "target")?.activeSecondary[0]?.reference
      ?.linkedPullRequest;
    expect(pullRequest?.outcome).toBe("rate_limited");
    expect(pullRequest?.lastSuccess).toBe(lastSuccess);
    expect(Object.keys(state.nativeReferences)).toEqual(["pull-request"]);
  });
});

describe("Workstream reducer and cache", () => {
  it("ignores stale registry updates and requires a reset for generation changes", () => {
    const current = appliedState(createWorkstreamState(context(0)), {
      context: context(5),
      workstreams: [workstream("current", 1)],
    });
    const stale = applyWorkstreamUpdate(current, {
      context: context(4),
      workstreams: [workstream("stale", 1)],
    });
    expect(stale.kind).toBe("unchanged");
    expect(stale.state.workstreams.stale).toBeUndefined();

    const newGeneration = applyWorkstreamUpdate(current, {
      context: context(6, { serverGeneration: 3 }),
    });
    expect(newGeneration.kind).toBe("reset-required");
    if (newGeneration.kind === "reset-required") {
      expect(newGeneration.reason).toBe("server-generation-changed");
    }
  });

  it("partitions the bounded memory cache and hides it while disconnected", () => {
    const cache = new WorkstreamMetadataCache(2);
    const first = appliedState(createWorkstreamState(context(0)), {
      context: context(1),
      workstreams: [workstream("first", 1)],
    });
    cache.write(first);

    expect(cache.read(first.context, false)).toBeNull();
    const cached = cache.read(first.context, true);
    expect(cached?.workstreams.first?.name).toBe("first");
    expect(Object.keys(cached ?? {})).toEqual(["context", "workstreams"]);
    expect(cached).not.toHaveProperty("memberships");
    expect(cached).not.toHaveProperty("declarations");
    expect(cached).not.toHaveProperty("receipts");
    expect(cache.read(context(1, { authorizationRevision: 8 }), true)).toBeNull();
    expect(
      cache.transitionBinding(first.context, context(1, { authorizationRevision: 8 })),
    ).toBeNull();
    expect(cache.read(first.context, true)).toBeNull();

    const second = createWorkstreamState(
      context(0, { registryId: "second-registry", ownerScopeId: "second-owner" }),
    );
    const third = createWorkstreamState(
      context(0, { registryId: "third-registry", ownerScopeId: "third-owner" }),
    );
    cache.write(second);
    cache.write(third);
    expect(cache.read(first.context, true)).toBeNull();
  });
});
