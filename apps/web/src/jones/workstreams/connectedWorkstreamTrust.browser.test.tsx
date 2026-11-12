import { act, render as renderScreen, type Screen } from "../../../test/render";

import type { EnvironmentThreadShell } from "@t3tools/client-runtime/state/models";
import {
  buildConnectedEnvironmentTrust,
  buildWorkstreamAssignmentIndex,
  nativeWorkstreamThreadKey,
  type ConnectedEnvironmentTrustInput,
} from "@t3tools/client-runtime/state/workstreams";
import type {
  MembershipEpisode,
  NativeReference,
  TrustedT3PlacementEnvironment,
} from "@t3tools/contracts";
import { createRef } from "react";
import { describe, expect, it, vi } from "vite-plus/test";

import {
  WorkstreamNativeSidebar,
  type NativeThreadAssignmentMenu,
} from "../../components/workstreams/WorkstreamNativeSidebar";
import {
  data,
  now,
  placements,
  reference,
  thread,
} from "../../components/workstreams/nativeWorkstreamActions.fixtures";
import type { WorkstreamListView } from "../../state/workstreams";
import { groupNativeThreadsWithConnectedTrust } from "./connectedWorkstreamTrust";

const primaryTrust: TrustedT3PlacementEnvironment = {
  environmentId: "env:a",
  authorityNamespace: "authority",
  storeGeneration: 1,
};
const remoteThread = { ...thread, environmentId: "env:b", id: "remote-thread" };
const remoteReference: NativeReference = {
  ...reference,
  native_reference_id: "remote-reference",
  identity: { ...reference.identity, source_instance_id: "env:b", native_id: "remote-thread" },
  registration: {
    ...reference.registration,
    evidence: {
      ...reference.registration.evidence!,
      source_instance_id: "env:b",
      native_id: "remote-thread",
      authority_namespace: "remote-authority",
      store_generation: 3,
    },
  },
};
const episode = (id: string, workstreamId: string, referenceId: string): MembershipEpisode => ({
  membership_id: id,
  workstream_id: workstreamId,
  native_reference_id: referenceId,
  kind: "primary",
  opened: {
    at: "2026-09-30T11:00:00Z",
    actor: { principal_id: "owner" },
    command_id: "command-open-0001",
    registry_version: 1,
  },
  closed: null,
});
const assignmentIndex = buildWorkstreamAssignmentIndex({
  ownerId: "owner",
  registryVersion: 11,
  references: [reference, remoteReference],
  memberships: [
    episode("membership", "alpha", reference.native_reference_id),
    episode("remote-membership", "beta", remoteReference.native_reference_id),
  ],
  complete: true,
});
const live = (
  environmentId: string,
  authorityNamespace: string,
  storeGeneration: number,
): ConnectedEnvironmentTrustInput => ({
  environmentId,
  config: {
    environment: { environmentId },
    nativeStoreAuthority: { environmentId, authorityNamespace, storeGeneration },
  },
  live: true,
});
const nowIso = new Date(now).toISOString();
const threads = [
  { ...thread, title: "Primary conversation" },
  { ...remoteThread, title: "Remote conversation" },
] as unknown as readonly EnvironmentThreadShell[];
const primaryKey = nativeWorkstreamThreadKey("env:a", "thread");
const remoteKey = nativeWorkstreamThreadKey("env:b", "remote-thread");

const group = (environments: readonly ConnectedEnvironmentTrustInput[], withIndex = true) =>
  groupNativeThreadsWithConnectedTrust({
    workstreams: data.items,
    placements: [],
    threads,
    trustedNow: nowIso,
    ...(withIndex ? { assignmentIndex, verificationNow: nowIso } : {}),
    trust: buildConnectedEnvironmentTrust({
      environments,
      placementTrustedEnvironments: [primaryTrust],
    }),
  });

describe("Workstream grouping with connected-environment trust", () => {
  it("verifies a remote environment's thread from its live descriptor", () => {
    const grouping = group([live("env:b", "remote-authority", 3)]);
    expect(grouping.assuranceByKey?.get(remoteKey)).toBe("verified");
    expect(grouping.assuranceByKey?.get(primaryKey)).toBe("verified");
    expect(grouping.groups.map((entry) => [entry.workstream.workstreamId, entry.threads])).toEqual([
      ["alpha", [threads[0]]],
      ["beta", [threads[1]]],
    ]);
    expect(grouping.conflictingKeys.size).toBe(0);
  });

  it("keeps a remote thread recorded, still grouped, without a live descriptor", () => {
    for (const environments of [[], [{ environmentId: "env:b", config: null, live: false }]]) {
      const grouping = group(environments);
      expect(grouping.assuranceByKey?.get(remoteKey)).toBe("recorded");
      expect(grouping.groupedKeys.has(remoteKey)).toBe(true);
    }
  });

  it("shows every thread of an environment with disagreeing descriptors as a conflict", () => {
    const grouping = group([live("env:a", "authority", 2), live("env:b", "remote-authority", 3)]);
    expect(grouping.assuranceByKey?.get(primaryKey)).toBe("conflict");
    expect(grouping.conflictingKeys).toEqual(new Set([primaryKey]));
    expect(grouping.groupedKeys).toEqual(new Set([remoteKey]));
    expect(grouping.groups.map((entry) => entry.workstream.workstreamId)).toEqual(["beta"]);
    expect(grouping.ungrouped).toEqual([threads[0]]);
    expect(grouping.ordered).toEqual([threads[1], threads[0]]);
  });

  it("marks environment conflicts on the placement-only path without inventing assurance", () => {
    const grouping = group([live("env:a", "authority", 2)], false);
    expect(grouping.assuranceByKey).toBeUndefined();
    expect(grouping.conflictingKeys).toEqual(new Set([primaryKey]));
    expect(grouping.ungrouped).toEqual(threads);
  });
});

describe("Workstream sidebar with an environment trust conflict", () => {
  let root: Screen;
  let renderDisposables: AsyncDisposableStack;
  let container: HTMLDivElement;

  async function setupFixture1() {
    const stack = new AsyncDisposableStack();
    renderDisposables = stack;
    stack.defer(async () => {
      await act(async () => root.unmount());
      container.remove();
      localStorage.clear();
      vi.restoreAllMocks();
      vi.unstubAllGlobals();
    });
    try {
      vi.spyOn(Date, "now").mockReturnValue(now);
      container = document.createElement("div");
      document.body.append(container);
      root = renderDisposables.use(await renderScreen(null, { container, router: false }));

      return stack;
    } catch (error) {
      await stack.disposeAsync();
      throw error;
    }
  }

  it("warns and blocks every membership change for the conflicting environment", async () => {
    await using _fixture = await setupFixture1();
    const assignNativeThread = vi.fn(async () => undefined);
    const controller = {
      data,
      registrationContext: {
        protocol: "workstreams-registration-context/2.0.0",
        state: "ready",
        ...placements.context,
        grant_id: "grant",
        sources: ["env:a", "env:b"].map((source_instance_id) => ({
          provider: "t3",
          source_instance_id,
          authority_namespace: "authority",
          store_generation: 1,
          resource_kind: "thread",
          id_kind: "internal",
          account_provenance: { kind: "not_account_scoped" },
          native_protocol: "workstreams-t3-provider/1.0.0",
          build: {
            repository: "Jones-Systems/Jones-Code",
            sha: "a".repeat(40),
            tree: "b".repeat(40),
          },
        })),
      },
      assignNativeThread,
      placements: null,
      placementInventory: { coverage: "complete", identities: [], json: "[]", totalIdentities: 0 },
      assignmentIndex,
      stale: false,
      error: null,
      loading: false,
      refresh: vi.fn(),
      submit: vi.fn(),
      runBindingOperation: vi.fn(),
      loadDetail: vi.fn(),
      loadReference: vi.fn(),
    } as unknown as WorkstreamListView;
    const grouping = group([live("env:a", "authority", 2), live("env:b", "remote-authority", 3)]);
    const menuRef = createRef<
      ((item: EnvironmentThreadShell) => NativeThreadAssignmentMenu) | null
    >();
    await act(async () =>
      root.rerender(
        <WorkstreamNativeSidebar
          controller={controller}
          grouping={grouping}
          summaryGrouping={grouping}
          assignmentMenuRef={menuRef}
          renderThread={(item) => <li>{(item as unknown as { title: string }).title}</li>}
          canOperate={() => true}
          canReorder={() => true}
          reorder={vi.fn(async () => undefined)}
        />,
      ),
    );
    expect(container.querySelector('[role="alert"]')?.textContent).toContain(
      "conflicting Workstream assignments",
    );
    const blocked = menuRef.current!(threads[0]!);
    expect(blocked.items[0]).toMatchObject({
      label: "Conflicting assignments; changes blocked",
      disabled: true,
    });
    expect(blocked.items.every((item) => item.disabled === true)).toBe(true);
    await act(async () => blocked.onAction("assign-workstream:group:beta"));
    await act(async () => blocked.onAction("assign-workstream:remove"));
    expect(assignNativeThread).not.toHaveBeenCalled();
    const remote = menuRef.current!(threads[1]!);
    expect(remote.items.find((item) => item.id === "assign-workstream:group:alpha")?.disabled).toBe(
      false,
    );
  });
});
