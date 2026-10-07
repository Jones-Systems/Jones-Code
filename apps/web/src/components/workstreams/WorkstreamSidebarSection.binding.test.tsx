import { isValidElement, type ReactElement } from "react";
import { afterEach, describe, expect, it, vi } from "vite-plus/test";

import { visitElements } from "../../test/reactElementTree";
import { reactHookHarness as hooks } from "../../test/reactHookHarness";
import type { WorkstreamDetailView, WorkstreamListView } from "../../state/workstreams";

vi.mock("react", async (importOriginal) => {
  const actual = await importOriginal<typeof import("react")>();
  const { reactHookHarness } = await import("../../test/reactHookHarness");
  return {
    ...actual,
    useLayoutEffect: reactHookHarness.useEffect,
    useMemo: reactHookHarness.useMemo,
    useRef: reactHookHarness.useRef,
    useState: reactHookHarness.useState,
  };
});

vi.mock("react/compiler-runtime", async () => {
  const { reactHookHarness } = await import("../../test/reactHookHarness");
  return { c: reactHookHarness.useMemoCache };
});

vi.mock("../../hooks/useLocalStorage", () => ({
  useLocalStorage: (_key: string, initial: unknown) => hooks.useState(initial),
}));

vi.mock("../../jones/workstreamAppearance/useWorkstreamAppearance", () => ({
  useWorkstreamAppearance: () => ({ writable: false, colors: new Map(), save: vi.fn() }),
}));

import { WorkstreamSidebarSection } from "./WorkstreamSidebarSection";

const binding = {
  registryId: "registry",
  ownerId: "owner",
  principalId: "principal",
  authorizationRevision: 1,
  serverGeneration: 7,
  registryVersion: 11,
  permissions: ["workstreams:read" as const, "workstreams:write" as const],
  contractVersion: "workstreams/1.0.0" as const,
  contractManifest: "6d8da23d51c1bba024dddc4b8d4dd9a594d2f474affd73e4cc7de508b797f557" as const,
};

const data = {
  binding,
  items: [
    {
      workstreamId: "ws-a",
      name: "Alpha",
      lifecycle: "active" as const,
      progress: { state: "progressing" as const },
      delivery: "pr-open" as const,
      freshness: "current" as const,
      sortOrder: 0,
      version: 1,
      updatedAt: "2026-09-12T12:00:00Z",
    },
  ],
  nextCursor: null,
  source: "live" as const,
  stale: false,
};

const detailWithReference = {
  detail: {
    context: { owner_id: "owner", server_generation: 7, registry_version: 11 },
    workstream: {
      workstream_id: "ws-a",
      name: "Alpha detail",
      lifecycle: "active",
      version: 1,
    },
  },
  memberships: {
    context: {},
    items: [
      {
        membership_id: "membership-a",
        native_reference_id: "reference-a",
        kind: "primary",
        closed: null,
      },
    ],
    next_cursor: null,
  },
  declarations: { context: {}, items: [], next_cursor: null },
  edges: { context: {}, items: [], next_cursor: null },
  history: { context: {}, items: [], next_cursor: null },
  references: {
    context: {},
    items: [
      {
        native_reference_id: "reference-a",
        identity: {
          provider: "github",
          source_instance_id: "github-owner",
          resource_kind: "pull_request",
          id_kind: "external",
          native_id: "jones-systems/t3code#5",
          account_provenance: { kind: "not_account_scoped" },
        },
        pr_locator: {
          host: "github.com",
          repository_owner: "jones-systems",
          repository_name: "t3code",
          number: 5,
        },
      },
    ],
    next_cursor: null,
  },
} as unknown as WorkstreamDetailView;

const completedDetail = {
  ...detailWithReference,
  detail: {
    ...detailWithReference.detail,
    workstream: { ...detailWithReference.detail.workstream, lifecycle: "completed" },
  },
  history: {
    coverage: "complete",
    context: { owner_id: "owner", server_generation: 7, registry_version: 11 },
    next_cursor: null,
    items: [
      {
        event_id: "completion-event",
        command_id: "owner-completion-command",
        actor: { principal_id: "owner-principal" },
        operation: "update_workstream",
        occurred_at: "2026-09-20T12:00:00Z",
        registry_version: 11,
        changed: true,
        workstream_versions: [{ workstream_id: "ws-a", version: 1 }],
        native_reference_id: null,
        membership_ids: [],
        declaration_id: null,
        declaration_revision: null,
        edge_id: null,
        lifecycle_declaration: {
          lifecycle_declaration_id: "lifecycle-completed",
          workstream_id: "ws-a",
          revision: 1,
          prior_lifecycle: "active",
          new_lifecycle: "completed",
          recorded_at: "2026-09-20T12:00:00Z",
          actor: { principal_id: "owner-principal" },
          command_id: "owner-completion-command",
          supersedes_lifecycle_declaration_id: null,
          registry_version: 11,
        },
      },
    ],
  },
} as unknown as WorkstreamDetailView;

const writableData = data;
const readOnlyData = {
  ...data,
  binding: { ...binding, permissions: ["workstreams:read" as const] },
};

function containsText(node: unknown, text: string): boolean {
  if (typeof node === "string") return node.includes(text);
  if (Array.isArray(node)) return node.some((child) => containsText(child, text));
  if (!isValidElement<Record<string, unknown>>(node)) return false;
  return Object.values(node.props).some((value) => containsText(value, text));
}

function findElement(
  node: unknown,
  predicate: (element: ReactElement<Record<string, unknown>>) => boolean,
) {
  return visitElements(node, predicate) as ReactElement<Record<string, unknown>> | undefined;
}

describe("Workstream sidebar binding cancellation", () => {
  afterEach(() => hooks.reset());

  it("presents owner completion separately from member, PR, and settlement evidence", async () => {
    const controller = {
      placementInventory: {
        coverage: "complete" as const,
        identities: [],
        json: "[]",
        totalIdentities: 0,
      },
      placements: null,
      data: {
        ...writableData,
        items: [{ ...writableData.items[0]!, lifecycle: "completed" as const }],
      },
      error: null,
      loading: false,
      refresh: vi.fn(),
      submit: vi.fn(),
      runBindingOperation: vi.fn(),
      loadDetail: vi.fn(async () => completedDetail),
      loadReference: vi.fn(async () => ({
        context: completedDetail.detail.context,
        reference: completedDetail.references.items[0]!,
        latest_observation: null,
      })),
    } as WorkstreamListView;

    hooks.beginRender();
    const initial = WorkstreamSidebarSection({ controller });
    expect(containsText(initial, "completed — open to verify")).toBe(false);
    const alpha = visitElements(
      initial,
      (element) => element.type === "button" && containsText(element.props.children, "Alpha"),
    ) as ReactElement<{ onClick: () => void }> | undefined;
    alpha?.props.onClick();
    await Promise.resolve();
    await Promise.resolve();

    hooks.beginRender();
    const current = WorkstreamSidebarSection({ controller });
    expect(containsText(current, "completed — owner-declared")).toBe(false);
    expect(containsText(current, "owner-declared by owner-principal")).toBe(true);
    expect(containsText(current, "Revision ")).toBe(true);
    expect(containsText(current, "registry version")).toBe(true);
    expect(containsText(current, "owner-completion-command")).toBe(true);
    expect(
      containsText(
        current,
        "Terminal turns, member disposition, pull request status, and T3 thread settlement are separate evidence. None completes this workstream.",
      ),
    ).toBe(true);
    expect(containsText(current, "Record member completed")).toBe(true);
    expect(containsText(current, "Mark completed")).toBe(false);
    expect(containsText(current, "Owner statement")).toBe(true);
    expect(
      containsText(
        current,
        "Owner statements add context only; they do not change lifecycle or grant execution authority.",
      ),
    ).toBe(true);
  });

  it("distinguishes loaded but unverified completion from unopened completion", async () => {
    const unverifiedDetail = {
      ...completedDetail,
      history: { ...completedDetail.history, items: [] },
    } as WorkstreamDetailView;
    const controller = {
      placementInventory: {
        coverage: "complete" as const,
        identities: [],
        json: "[]",
        totalIdentities: 0,
      },
      placements: null,
      data: {
        ...writableData,
        items: [{ ...writableData.items[0]!, lifecycle: "completed" as const }],
      },
      error: null,
      loading: false,
      refresh: vi.fn(),
      submit: vi.fn(),
      runBindingOperation: vi.fn(),
      loadDetail: vi.fn(async () => unverifiedDetail),
      loadReference: vi.fn(async () => ({
        context: unverifiedDetail.detail.context,
        reference: unverifiedDetail.references.items[0]!,
        latest_observation: null,
      })),
    } as WorkstreamListView;

    hooks.beginRender();
    const initial = WorkstreamSidebarSection({ controller });
    expect(containsText(initial, "completed — open to verify")).toBe(false);
    const alpha = visitElements(
      initial,
      (element) => element.type === "button" && containsText(element.props.children, "Alpha"),
    ) as ReactElement<{ onClick: () => void }> | undefined;
    alpha?.props.onClick();
    await Promise.resolve();
    await Promise.resolve();
    hooks.beginRender();
    const current = WorkstreamSidebarSection({ controller });
    expect(containsText(current, "completed — unverified")).toBe(false);
    expect(containsText(current, "completed — open to verify")).toBe(false);
    expect(containsText(current, "missing declaration")).toBe(true);
  });

  it("gates native settlement by attestation and exposes both directions", async () => {
    const t3Reference = {
      ...detailWithReference.references.items[0]!,
      identity: {
        ...detailWithReference.references.items[0]!.identity,
        provider: "t3",
        resource_kind: "thread",
        native_id: "thread-a",
      },
      pr_locator: null,
      registration: { state: "attested", attestation_version: 2 },
    };
    const loadDetail = vi.fn(async () => ({
      ...detailWithReference,
      references: { ...detailWithReference.references, items: [t3Reference] },
    }));
    const submit = vi.fn(
      async (command: { readonly action: { readonly native_action?: "settle" | "unsettle" } }) => ({
        state: "pending",
        command,
      }),
    );
    const controller = {
      placementInventory: {
        coverage: "complete" as const,
        identities: [],
        json: "[]",
        totalIdentities: 0,
      },
      placements: null,
      data: writableData,
      error: null,
      loading: false,
      refresh: vi.fn(),
      submit,
      loadDetail,
      loadReference: vi.fn(),
    } as unknown as WorkstreamListView;

    hooks.beginRender();
    const initial = WorkstreamSidebarSection({ controller });
    const alpha = visitElements(
      initial,
      (element) => element.type === "button" && containsText(element.props.children, "Alpha"),
    ) as ReactElement<{ onClick: () => void }> | undefined;
    alpha?.props.onClick();
    await Promise.resolve();
    await Promise.resolve();

    hooks.beginRender();
    const current = WorkstreamSidebarSection({ controller });
    const settle = visitElements(
      current,
      (element) =>
        typeof element.props.onClick === "function" &&
        containsText(element, "Request T3 thread settlement"),
    ) as ReactElement<{ onClick: () => void }> | undefined;
    const restore = visitElements(
      current,
      (element) =>
        typeof element.props.onClick === "function" &&
        containsText(element, "Request T3 thread restore"),
    ) as ReactElement<{ onClick: () => void }> | undefined;
    expect(settle).toBeDefined();
    expect(restore).toBeDefined();
    settle?.props.onClick();
    restore?.props.onClick();
    await Promise.resolve();
    await Promise.resolve();
    await Promise.resolve();
    expect(submit.mock.calls.map(([command]) => command.action.native_action).toSorted()).toEqual([
      "settle",
      "unsettle",
    ]);

    hooks.reset();
    const unverifiedController = {
      ...controller,
      loadDetail: vi.fn(async () => ({
        ...detailWithReference,
        references: {
          ...detailWithReference.references,
          items: [{ ...t3Reference, registration: { state: "expired", attestation_version: 2 } }],
        },
      })),
    } as unknown as WorkstreamListView;
    hooks.beginRender();
    const unverifiedInitial = WorkstreamSidebarSection({ controller: unverifiedController });
    const unverifiedAlpha = visitElements(
      unverifiedInitial,
      (element) => element.type === "button" && containsText(element.props.children, "Alpha"),
    ) as ReactElement<{ onClick: () => void }> | undefined;
    unverifiedAlpha?.props.onClick();
    await Promise.resolve();
    await Promise.resolve();
    hooks.beginRender();
    const unverified = WorkstreamSidebarSection({ controller: unverifiedController });
    expect(containsText(unverified, "T3 settlement unavailable: reference is ")).toBe(true);
    expect(containsText(unverified, "expired")).toBe(true);
    expect(containsText(unverified, "Request T3 thread settlement")).toBe(false);
  });

  it("keeps read-only bindings observational", async () => {
    const controller = {
      placementInventory: {
        coverage: "complete" as const,
        identities: [],
        json: "[]",
        totalIdentities: 0,
      },
      placements: null,
      data: readOnlyData,
      error: null,
      loading: false,
      refresh: vi.fn(),
      submit: vi.fn(),
      runBindingOperation: vi.fn(),
      loadDetail: vi.fn(async () => detailWithReference),
      loadReference: vi.fn(async () => ({
        context: detailWithReference.detail.context,
        reference: detailWithReference.references.items[0]!,
        latest_observation: null,
      })),
    } as WorkstreamListView;

    hooks.beginRender();
    const initial = WorkstreamSidebarSection({ controller });
    expect(containsText(initial, "Actions for Alpha")).toBe(false);
    const alpha = visitElements(
      initial,
      (element) => element.type === "button" && containsText(element.props.children, "Alpha"),
    ) as ReactElement<{ onClick: () => void }> | undefined;
    alpha?.props.onClick();
    await Promise.resolve();
    await Promise.resolve();

    hooks.beginRender();
    const current = WorkstreamSidebarSection({ controller });
    const lifecycle = visitElements(
      current,
      (element) => element.props["aria-label"] === "Owner-declared workstream lifecycle",
    ) as ReactElement<{ disabled: boolean }> | undefined;
    expect(lifecycle?.props.disabled).toBe(true);
    for (const action of [
      "Move",
      "Link",
      "Remove",
      "Record member completed",
      "Refresh status",
      "Record statement",
      "Continues as",
      "Superseded by",
    ]) {
      expect(containsText(current, action)).toBe(false);
    }
    expect(controller.submit).not.toHaveBeenCalled();
  });

  it("removes an open rename control when write authority is revoked", () => {
    let controller = {
      placementInventory: {
        coverage: "complete" as const,
        identities: [],
        json: "[]",
        totalIdentities: 0,
      },
      placements: null,
      data: writableData,
      error: null,
      loading: false,
      refresh: vi.fn(),
      submit: vi.fn(),
      runBindingOperation: vi.fn(),
      loadDetail: vi.fn(),
      loadReference: vi.fn(),
    } as WorkstreamListView;

    hooks.beginRender();
    const initial = WorkstreamSidebarSection({ controller });
    const rename = visitElements(
      initial,
      (element) => typeof element.props.onClick === "function" && containsText(element, "Rename"),
    ) as ReactElement<{ onClick: () => void }> | undefined;
    rename?.props.onClick();
    hooks.beginRender();
    const editing = WorkstreamSidebarSection({ controller });
    expect(
      visitElements(editing, (element) => element.props["aria-label"] === "Workstream name"),
    ).toBeDefined();

    controller = {
      ...controller,
      data: { ...readOnlyData, binding: { ...readOnlyData.binding, authorizationRevision: 2 } },
    };
    hooks.beginRender();
    const readOnly = WorkstreamSidebarSection({ controller });
    expect(
      visitElements(readOnly, (element) => element.props["aria-label"] === "Workstream name"),
    ).toBeNull();
    expect(containsText(readOnly, "Actions for Alpha")).toBe(false);
  });

  it.each([
    ["registry", { registryId: "registry-replaced" }],
    [
      "contract",
      {
        contractManifest:
          "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa" as typeof binding.contractManifest,
      },
    ],
  ])("cannot commit deferred detail after a %s-only binding change", async (_name, change) => {
    let resolveDetail!: (value: WorkstreamDetailView) => void;
    const pendingDetail = new Promise<WorkstreamDetailView>((resolve) => {
      resolveDetail = resolve;
    });
    const loadDetail = vi.fn(
      (_workstreamId: string, _options?: { readonly signal?: AbortSignal }) => pendingDetail,
    );
    let controller: WorkstreamListView = {
      placementInventory: { coverage: "complete", identities: [], json: "[]", totalIdentities: 0 },
      placements: null,
      data,
      error: null,
      loading: false,
      refresh: vi.fn(),
      submit: vi.fn(),
      runBindingOperation: vi.fn(),
      loadDetail,
      loadReference: vi.fn(),
    };

    hooks.beginRender();
    const initial = WorkstreamSidebarSection({ controller });
    const alpha = visitElements(
      initial,
      (element) => element.type === "button" && containsText(element.props.children, "Alpha"),
    ) as ReactElement<{ onClick: () => void }> | undefined;
    alpha?.props.onClick();
    const signal = loadDetail.mock.calls[0]?.[1]?.signal;
    expect(signal?.aborted).toBe(false);

    controller = {
      ...controller,
      data: { ...data, binding: { ...binding, ...change } },
    };
    hooks.beginRender();
    WorkstreamSidebarSection({ controller });
    expect(signal?.aborted).toBe(true);

    resolveDetail(detailWithReference);
    await pendingDetail;
    await Promise.resolve();

    hooks.beginRender();
    const current = WorkstreamSidebarSection({ controller });
    expect(containsText(current, "Alpha detail")).toBe(false);
  });

  it("cannot commit detail from a replaced non-null authorization snapshot", async () => {
    let resolveDetail!: (value: WorkstreamDetailView) => void;
    const pendingDetail = new Promise<WorkstreamDetailView>((resolve) => {
      resolveDetail = resolve;
    });
    const loadDetail = vi.fn(
      (_workstreamId: string, _options?: { readonly signal?: AbortSignal }) => pendingDetail,
    );
    const loadReference = vi.fn();
    let controller: WorkstreamListView = {
      placementInventory: { coverage: "complete", identities: [], json: "[]", totalIdentities: 0 },
      placements: null,
      data,
      error: null,
      loading: false,
      refresh: vi.fn(),
      submit: vi.fn(),
      runBindingOperation: vi.fn(),
      loadDetail,
      loadReference,
    };

    hooks.beginRender();
    const initial = WorkstreamSidebarSection({ controller });
    const alpha = visitElements(
      initial,
      (element) => element.type === "button" && containsText(element.props.children, "Alpha"),
    ) as ReactElement<{ onClick: () => void }> | undefined;
    expect(alpha).toBeDefined();
    alpha?.props.onClick();
    const signal = loadDetail.mock.calls[0]?.[1]?.signal;
    expect(signal).toBeInstanceOf(AbortSignal);
    expect(signal?.aborted).toBe(false);

    controller = {
      ...controller,
      data: {
        ...data,
        binding: { ...binding, authorizationRevision: 2, registryVersion: 12 },
      },
    };
    hooks.beginRender();
    WorkstreamSidebarSection({ controller });
    expect(signal?.aborted).toBe(true);

    resolveDetail(detailWithReference);
    await pendingDetail;
    await Promise.resolve();

    hooks.beginRender();
    const current = WorkstreamSidebarSection({ controller });
    expect(containsText(current, "Alpha detail")).toBe(false);
    expect(loadReference).not.toHaveBeenCalled();
  });

  it("cannot commit a linked-PR status from a replaced authorization snapshot", async () => {
    let resolveStaleReference!: (value: unknown) => void;
    const staleReference = new Promise((resolve) => {
      resolveStaleReference = resolve;
    });
    const pendingCurrentReference = new Promise(() => undefined);
    const loadDetail = vi.fn(async () => detailWithReference);
    const loadReference = vi
      .fn()
      .mockImplementationOnce(() => staleReference)
      .mockImplementation(() => pendingCurrentReference);
    let controller = {
      placementInventory: {
        coverage: "complete" as const,
        identities: [],
        json: "[]",
        totalIdentities: 0,
      },
      placements: null,
      data,
      error: null,
      loading: false,
      refresh: vi.fn(),
      submit: vi.fn(),
      runBindingOperation: vi.fn(),
      loadDetail,
      loadReference,
    } as WorkstreamListView;

    hooks.beginRender();
    const initial = WorkstreamSidebarSection({ controller });
    const initialAlpha = visitElements(
      initial,
      (element) => element.type === "button" && containsText(element.props.children, "Alpha"),
    ) as ReactElement<{ onClick: () => void }> | undefined;
    initialAlpha?.props.onClick();
    await Promise.resolve();
    expect(loadReference).toHaveBeenCalledTimes(1);

    controller = {
      ...controller,
      data: {
        ...data,
        binding: { ...binding, authorizationRevision: 2, registryVersion: 12 },
      },
    };
    hooks.beginRender();
    WorkstreamSidebarSection({ controller });
    resolveStaleReference({
      latest_observation: { last_success: { state: "STALE STATUS" } },
    });
    await staleReference;
    await Promise.resolve();
    expect(JSON.stringify(hooks.snapshot())).not.toContain("STALE STATUS");

    hooks.beginRender();
    const afterStaleResolution = WorkstreamSidebarSection({ controller });
    expect(containsText(afterStaleResolution, "STALE STATUS")).toBe(false);

    hooks.beginRender();
    const rebound = WorkstreamSidebarSection({ controller });
    const reboundAlpha = visitElements(
      rebound,
      (element) => element.type === "button" && containsText(element.props.children, "Alpha"),
    ) as ReactElement<{ onClick: () => void }> | undefined;
    reboundAlpha?.props.onClick();
    await Promise.resolve();

    hooks.beginRender();
    const current = WorkstreamSidebarSection({ controller });
    expect(containsText(current, "STALE STATUS")).toBe(false);
    expect(containsText(current, "loading")).toBe(true);
    expect(loadReference).toHaveBeenCalledTimes(2);
  });

  it("cancels a pending manual PR refresh when the binding changes", async () => {
    let resolveReference!: (value: unknown) => void;
    const pendingReference = new Promise((resolve) => {
      resolveReference = resolve;
    });
    const loadDetail = vi.fn(async () => detailWithReference);
    const loadReference = vi
      .fn()
      .mockResolvedValueOnce({
        latest_observation: { observation_version: 1, last_success: { state: "OPEN" } },
      })
      .mockImplementationOnce(() => pendingReference);
    const submit = vi.fn();
    let controller = {
      placementInventory: {
        coverage: "complete" as const,
        identities: [],
        json: "[]",
        totalIdentities: 0,
      },
      placements: null,
      data,
      error: null,
      loading: false,
      refresh: vi.fn(),
      submit,
      runBindingOperation: vi.fn(),
      loadDetail,
      loadReference,
    } as WorkstreamListView;

    hooks.beginRender();
    const initial = WorkstreamSidebarSection({ controller });
    const alpha = visitElements(
      initial,
      (element) => element.type === "button" && containsText(element.props.children, "Alpha"),
    ) as ReactElement<{ onClick: () => void }> | undefined;
    alpha?.props.onClick();
    await Promise.resolve();
    await Promise.resolve();

    hooks.beginRender();
    const detailed = WorkstreamSidebarSection({ controller });
    const refreshStatus = visitElements(
      detailed,
      (element) =>
        element.props.children === "Refresh status" && typeof element.props.onClick === "function",
    ) as ReactElement<{ onClick: () => void }> | undefined;
    expect(refreshStatus).toBeDefined();
    const automaticSignal = loadReference.mock.calls[0]?.[1]?.signal;
    refreshStatus?.props.onClick();
    const signal = loadReference.mock.calls
      .map((call) => call[1]?.signal)
      .find((candidate) => candidate !== undefined && candidate !== automaticSignal);
    expect(signal).toBeInstanceOf(AbortSignal);
    expect(signal?.aborted).toBe(false);

    controller = {
      ...controller,
      data: {
        ...data,
        binding: { ...binding, authorizationRevision: 2, registryVersion: 12 },
      },
    };
    hooks.beginRender();
    WorkstreamSidebarSection({ controller });
    expect(signal?.aborted).toBe(true);

    resolveReference({
      latest_observation: { observation_version: 1, last_success: { state: "OPEN" } },
    });
    await pendingReference;
    await Promise.resolve();
    expect(submit).not.toHaveBeenCalled();
  });

  it("does not commit command UI after its binding is replaced", async () => {
    let resolveSubmit!: (value: unknown) => void;
    const pendingSubmit = new Promise((resolve) => {
      resolveSubmit = resolve;
    });
    const loadDetail = vi.fn(async () => detailWithReference);
    const controllerBase = {
      placementInventory: {
        coverage: "complete" as const,
        identities: [],
        json: "[]",
        totalIdentities: 0,
      },
      placements: null,
      data,
      error: null,
      loading: false,
      refresh: vi.fn(),
      submit: vi.fn(() => pendingSubmit),
      runBindingOperation: vi.fn(),
      loadDetail,
      loadReference: vi.fn(async () => ({ latest_observation: null })),
    } as unknown as WorkstreamListView;
    let controller = controllerBase;

    hooks.beginRender();
    const initial = WorkstreamSidebarSection({ controller });
    const alpha = visitElements(
      initial,
      (element) => element.type === "button" && containsText(element.props.children, "Alpha"),
    ) as ReactElement<{ onClick: () => void }> | undefined;
    alpha?.props.onClick();
    await Promise.resolve();
    await Promise.resolve();

    hooks.beginRender();
    const detailed = WorkstreamSidebarSection({ controller });
    const lifecycle = visitElements(
      detailed,
      (element) =>
        element.type === "select" &&
        element.props["aria-label"] === "Owner-declared workstream lifecycle",
    ) as ReactElement<{ onChange: (event: { target: { value: string } }) => void }> | undefined;
    expect(lifecycle).not.toBeNull();
    lifecycle?.props.onChange({ target: { value: "paused" } });
    await vi.waitFor(() => expect(controller.submit).toHaveBeenCalledTimes(1));

    controller = {
      ...controller,
      data: {
        ...data,
        binding: { ...binding, authorizationRevision: 2, registryVersion: 12 },
      },
    };
    hooks.beginRender();
    WorkstreamSidebarSection({ controller });

    resolveSubmit({
      command_id: "command-a",
      operation: "update_workstream",
      accepted_at: "2026-09-12T12:00:00Z",
      state: "committed",
      completed_at: "2026-09-12T12:00:01Z",
      registry_version: 12,
      changed: true,
      effects: {
        workstream_versions: [{ workstream_id: "ws-a", version: 2 }],
        native_reference_id: null,
        membership_ids: [],
        declaration_id: null,
        declaration_revision: null,
        edge_id: null,
        observation: null,
        registration: null,
        lifecycle_declaration: null,
        coordination_disposition: null,
        native_settlement: null,
      },
    });
    await pendingSubmit;
    await Promise.resolve();

    expect(controller.submit).toHaveBeenCalledTimes(1);
    expect(loadDetail).toHaveBeenCalledTimes(1);
    expect(JSON.stringify(hooks.snapshot())).not.toContain("command-a");
  });

  it("continues a multi-step reorder after rerendering at its accepted registry version", async () => {
    let resolveSecond!: (value: unknown) => void;
    const second = new Promise((resolve) => {
      resolveSecond = resolve;
    });
    const refresh = vi.fn();
    const stepReceipts = [
      {
        state: "committed",
        registry_version: 12,
        effects: {
          workstream_versions: [
            { workstream_id: "ws-c", version: 2 },
            { workstream_id: "ws-a", version: 5 },
          ],
        },
      },
      second,
      {
        state: "committed",
        registry_version: 14,
        effects: { workstream_versions: [{ workstream_id: "ws-b", version: 10 }] },
      },
    ];
    let controller!: WorkstreamListView;
    let rerenderedAtAcceptedVersion = false;
    const submitStep = vi.fn(async (_command: unknown) => {
      const value = stepReceipts.shift();
      const receipt = await Promise.resolve(value);
      if (!rerenderedAtAcceptedVersion) {
        rerenderedAtAcceptedVersion = true;
        controller = {
          ...controller,
          data: {
            ...controller.data!,
            binding: { ...controller.data!.binding, registryVersion: 12 },
          },
        };
        hooks.beginRender();
        WorkstreamSidebarSection({ controller });
      }
      return receipt;
    });
    const runBindingOperation = vi.fn(async (operation: (submit: typeof submitStep) => unknown) => {
      const value = await operation(submitStep);
      refresh();
      return value;
    });
    const items = [
      data.items[0],
      { ...data.items[0]!, workstreamId: "ws-b", name: "Beta", sortOrder: 1 },
      { ...data.items[0]!, workstreamId: "ws-c", name: "Gamma", sortOrder: 2 },
    ];
    controller = {
      placementInventory: { coverage: "complete", identities: [], json: "[]", totalIdentities: 0 },
      placements: null,
      data: { ...data, items },
      error: null,
      loading: false,
      refresh,
      submit: vi.fn(async () => {
        throw new Error("reorder used per-command submission");
      }),
      runBindingOperation,
      loadDetail: vi.fn(),
      loadReference: vi.fn(),
    } as unknown as WorkstreamListView;

    hooks.beginRender();
    const initial = WorkstreamSidebarSection({ controller });
    const gamma = findElement(
      initial,
      (element) => element.props["aria-label"] === "Drag Workstream Gamma to reorder",
    ) as ReactElement<{ onDragStart: (event: object) => void }> | undefined;
    gamma?.props.onDragStart({
      stopPropagation: vi.fn(),
      dataTransfer: { effectAllowed: "none", setData: vi.fn() },
    });

    hooks.beginRender();
    const dragging = WorkstreamSidebarSection({ controller });
    const alpha = findElement(
      dragging,
      (element) => element.type === "li" && containsText(element, "Alpha"),
    ) as ReactElement<{ onDrop: (event: object) => void }> | undefined;
    alpha?.props.onDrop({
      preventDefault: vi.fn(),
      stopPropagation: vi.fn(),
      clientY: 0,
      currentTarget: { getBoundingClientRect: () => ({ top: 0, height: 40 }) },
    });

    await vi.waitFor(() => expect(submitStep).toHaveBeenCalledTimes(2));
    expect(runBindingOperation).toHaveBeenCalledTimes(1);
    expect(controller.submit).not.toHaveBeenCalled();
    expect(refresh).not.toHaveBeenCalled();
    expect(submitStep.mock.calls[1]?.[0]).toMatchObject({
      expected_registry_version: 12,
      action: { workstream_id: "ws-a", expected_version: 5 },
    });

    resolveSecond({
      state: "committed",
      registry_version: 13,
      effects: {
        workstream_versions: [
          { workstream_id: "ws-a", version: 6 },
          { workstream_id: "ws-b", version: 9 },
        ],
      },
    });
    await vi.waitFor(() => expect(submitStep).toHaveBeenCalledTimes(3));
    expect(submitStep.mock.calls[2]?.[0]).toMatchObject({
      expected_registry_version: 13,
      action: { workstream_id: "ws-b", expected_version: 9 },
    });
    await vi.waitFor(() => expect(refresh).toHaveBeenCalledTimes(1));
  });
});
