import type {
  NativeReference,
  WorkstreamCommand,
  WorkstreamReceipt,
  WorkstreamsRegistrationContextResponse,
} from "@t3tools/contracts";
import { WorkstreamPrObservation } from "@t3tools/contracts";
import * as Schema from "effect/Schema";
import { afterEach, beforeEach, describe, expect, it, vi } from "vite-plus/test";
import {
  type WorkstreamActionSnapshot,
  type WorkstreamDetailView,
  type WorkstreamListView,
  WorkstreamActionError,
} from "../../state/workstreams";
import { moveNativeMembershipThreads, ThreadMovementError } from "./nativeWorkstreamActions";
import { data, now, placements, reference, thread } from "./nativeWorkstreamActions.fixtures";
import {
  canonicalWorkstreamPrId,
  parseWorkstreamPrUrl,
  prepareWorkstreamPr,
  refreshWorkstreamPr,
  threadReferenceState,
  workstreamPrObservationLabel,
} from "./workstreamReferenceActions";

const registrationContext: WorkstreamsRegistrationContextResponse = {
  protocol: "workstreams-registration-context/1.0.0",
  state: "ready",
  owner_id: "owner",
  principal_id: "principal",
  grant_id: "grant",
  authorization_revision: 1,
  server_generation: 7,
  registry_version: 11,
  sources: [
    {
      provider: "t3",
      source_instance_id: "env:a",
      authority_namespace: "authority",
      store_generation: 1,
      resource_kind: "thread",
      id_kind: "internal",
      account_provenance: { kind: "not_account_scoped" },
      native_protocol: "workstreams-t3-provider/1.0.0",
      build: { repository: "Jones-Systems/Jones-Code", sha: "a".repeat(40), tree: "b".repeat(40) },
    },
    {
      provider: "github",
      source_instance_id: "github",
      authority_namespace: "github-authority",
      store_generation: 1,
      resource_kind: "pull_request",
      id_kind: "external",
      account_provenance: { kind: "not_account_scoped" },
    },
  ],
};

function fixture(initial: readonly NativeReference[] = []) {
  let registryVersion = 11;
  let refs = [...initial];
  const operations: WorkstreamCommand[] = [];
  let snapshots = 0;
  let nextId = 0;
  const snapshot = (): WorkstreamActionSnapshot => ({
    data: {
      ...data,
      binding: { ...data.binding, registryVersion },
      items: data.items.map((item) => ({ ...item, version: registryVersion })),
    },
    references: {
      context: { owner_id: "owner", server_generation: 7, registry_version: registryVersion },
      items: refs,
      next_cursor: null,
    },
    placements: {
      ...placements,
      context: { ...placements.context, registry_version: registryVersion },
      items: [],
    },
    registrationContext: { ...registrationContext, registry_version: registryVersion },
  });
  const submit = vi.fn(async (command: WorkstreamCommand): Promise<WorkstreamReceipt> => {
    operations.push(command);
    expect(command.expected_registry_version).toBe(registryVersion);
    registryVersion += 1;
    let referenceId: string | null = null;
    if (command.action.operation === "register_reference") {
      referenceId = `reference-${operations.length}`;
      refs.push({
        ...reference,
        native_reference_id: referenceId,
        identity: command.action.identity,
        pr_locator: command.action.pr_locator,
        registration: {
          state: "quarantined",
          attestation_version: 0,
          attested_at: null,
          expires_at: null,
          evidence: null,
        },
      });
    }
    if (command.action.operation === "verify_reference") {
      const action = command.action;
      refs = refs.map((value) =>
        value.native_reference_id !== action.native_reference_id
          ? value
          : {
              ...value,
              registration: {
                ...reference.registration,
                attestation_version: action.expected_attestation_version + 1,
                evidence: {
                  ...reference.registration.evidence!,
                  provider: value.identity.provider,
                  source_instance_id: value.identity.source_instance_id,
                  native_id: value.identity.native_id,
                  authority_namespace:
                    value.identity.provider === "github" ? "github-authority" : "authority",
                },
              },
            },
      );
    }
    return {
      state: "committed",
      registry_version: registryVersion,
      effects: { native_reference_id: referenceId, workstream_versions: [] },
    } as unknown as WorkstreamReceipt;
  });
  const controller: WorkstreamListView = {
    data,
    placements: { ...placements, items: [] },
    references: snapshot().references,
    registrationContext,
    loading: false,
    error: null,
    placementInventory: {
      coverage: "complete",
      identities: [thread, { ...thread, id: "two" }, { ...thread, id: "three" }].map((item) => ({
        source_instance_id: item.environmentId,
        native_thread_id: item.id,
      })),
      json: "[]",
      totalIdentities: 3,
    },
    refresh: vi.fn(),
    retry: vi.fn(async () => {}),
    observeCommand: vi.fn(),
    submit,
    runBindingOperation: async (operation) => operation(submit),
    loadActionSnapshot: vi.fn(async () => {
      snapshots += 1;
      return snapshot();
    }),
    loadReference: vi.fn(async (id) => ({
      context: snapshot().references.context,
      reference: refs.find((value) => value.native_reference_id === id)!,
      latest_observation: null,
    })),
    loadDetail: vi.fn(
      async (id) =>
        ({
          detail: {
            context: snapshot().references.context,
            workstream: { workstream_id: id, version: registryVersion },
          },
          memberships: { items: [] },
        }) as unknown as WorkstreamDetailView,
    ),
  };
  return {
    controller,
    submit,
    operations,
    snapshot,
    commandId: async () => `command-${++nextId}`,
    snapshots: () => snapshots,
  };
}

beforeEach(() => {
  vi.spyOn(Date, "now").mockReturnValue(now);
});
afterEach(() => {
  vi.restoreAllMocks();
});

describe("selected native reference preparation", () => {
  it("registers quarantined identity, verifies, reloads and assigns with fresh versions", async () => {
    const f = fixture();
    await moveNativeMembershipThreads({
      ...f,
      threads: [thread],
      destination: "beta",
      now,
      intent: { prepareReferences: true },
    });
    expect(f.operations.map((command) => command.action.operation)).toEqual([
      "register_reference",
      "verify_reference",
      "attach_primary",
    ]);
    expect(f.operations[0]?.action).toMatchObject({
      identity: {
        provider: "t3",
        source_instance_id: "env:a",
        native_id: "thread",
        resource_kind: "thread",
        id_kind: "internal",
      },
      pr_locator: null,
    });
    expect(f.operations[1]?.action).toMatchObject({
      native_reference_id: "reference-1",
      expected_attestation_version: 0,
    });
    expect(f.operations[2]).toMatchObject({
      expected_registry_version: 13,
      action: { expected_version: 13, native_reference_id: "reference-1" },
    });
    expect(f.controller.loadReference).toHaveBeenCalledWith("reference-1", {});
    expect(f.snapshots()).toBe(3);
  });
  it("reuses a unique quarantined reference and never registers to disambiguate", async () => {
    const quarantined = {
      ...reference,
      registration: {
        state: "quarantined" as const,
        attestation_version: 0,
        attested_at: null,
        expires_at: null,
        evidence: null,
      },
    };
    const f = fixture([quarantined]);
    await moveNativeMembershipThreads({
      ...f,
      threads: [thread],
      destination: "beta",
      now,
      intent: { prepareReferences: true },
    });
    expect(f.operations.map((command) => command.action.operation)).toEqual([
      "verify_reference",
      "attach_primary",
    ]);
    const ambiguous = fixture([reference, quarantined]);
    await expect(
      moveNativeMembershipThreads({
        ...ambiguous,
        threads: [thread],
        destination: "beta",
        now,
        intent: { prepareReferences: true },
      }),
    ).rejects.toMatchObject({ reason: "ambiguous" });
    expect(ambiguous.operations).toEqual([]);
  });
  it("keeps drag verified-only and requires explicit stale re-verification", async () => {
    const expired = {
      ...reference,
      registration: { ...reference.registration, expires_at: new Date(now - 1).toISOString() },
    };
    const f = fixture([expired]);
    expect(threadReferenceState(f.snapshot(), thread, now)).toBe("reverify");
    await expect(
      moveNativeMembershipThreads({ ...f, threads: [thread], destination: "beta", now }),
    ).rejects.toMatchObject({ reason: "stale" });
    expect(f.operations).toEqual([]);
    await moveNativeMembershipThreads({
      ...f,
      threads: [thread],
      destination: "beta",
      now,
      intent: { prepareReferences: true },
    });
    expect(f.operations.map((command) => command.action.operation)).toEqual([
      "verify_reference",
      "attach_primary",
    ]);
  });
  it("preflights every selected descriptor before any registration", async () => {
    const f = fixture();
    const other = { ...thread, environmentId: "env:b" };
    const controller = {
      ...f.controller,
      placementInventory: {
        ...f.controller.placementInventory,
        identities: [
          ...f.controller.placementInventory.identities,
          { source_instance_id: "env:b", native_thread_id: "thread" },
        ],
      },
    };
    await expect(
      moveNativeMembershipThreads({
        ...f,
        controller,
        threads: [thread, other],
        destination: "beta",
        now,
        intent: { prepareReferences: true },
      }),
    ).rejects.toMatchObject({ reason: "activation" });
    expect(f.operations).toEqual([]);
  });
  it("stops after a changed authorization instead of using pre-verification props", async () => {
    const f = fixture();
    const load = f.controller.loadActionSnapshot;
    vi.mocked(load).mockImplementation(async () => {
      const snapshot = f.snapshot();
      if (f.operations.length)
        return {
          ...snapshot,
          data: {
            ...snapshot.data,
            binding: { ...snapshot.data.binding, authorizationRevision: 2 },
          },
          registrationContext: { ...snapshot.registrationContext!, authorization_revision: 2 },
          placements: {
            ...snapshot.placements!,
            context: { ...snapshot.placements!.context, authorization_revision: 2 },
          },
        };
      return snapshot;
    });
    await expect(
      moveNativeMembershipThreads({
        ...f,
        threads: [thread],
        destination: "beta",
        now,
        intent: { prepareReferences: true },
      }),
    ).rejects.toMatchObject({ phase: "reload", commandId: "command-1", completedKeys: [] });
    expect(f.operations.map((command) => command.action.operation)).toEqual(["register_reference"]);
  });
  it("reports assigned, prepared, stopped phase and unprocessed threads without resuming", async () => {
    const f = fixture();
    const submit = f.submit;
    const original = submit.getMockImplementation()!;
    submit.mockImplementation(async (command) =>
      submit.mock.calls.length === 5
        ? ({ state: "unresolved" } as WorkstreamReceipt)
        : original(command),
    );
    const failure = await moveNativeMembershipThreads({
      ...f,
      threads: [
        thread,
        { ...thread, id: "two", title: "Second thread" },
        { ...thread, id: "three" },
      ],
      destination: "beta",
      now,
      intent: { prepareReferences: true },
    }).catch((cause: unknown) => cause);
    expect(failure).toBeInstanceOf(ThreadMovementError);
    expect(failure).toMatchObject({
      completedKeys: [JSON.stringify(["env:a", "thread"])],
      preparedKeys: [JSON.stringify(["env:a", "thread"]), JSON.stringify(["env:a", "two"])],
      stoppedTitle: "Second thread",
      phase: "verify",
      commandId: "command-5",
      unprocessedKeys: [JSON.stringify(["env:a", "three"])],
    });
    expect(String(failure)).toContain("Assigned 1. References prepared but not assigned 1");
    expect(submit).toHaveBeenCalledTimes(5);
  });
});

describe("GitHub references", () => {
  it("stops a cancelled refresh after its deferred observation without allocating or submitting a command", async () => {
    const f = fixture([reference]);
    const abort = new AbortController();
    let referenceStarted!: () => void;
    const started = new Promise<void>((resolve) => {
      referenceStarted = resolve;
    });
    let finishReference!: (value: Awaited<ReturnType<WorkstreamListView["loadReference"]>>) => void;
    const pendingReference = new Promise<Awaited<ReturnType<WorkstreamListView["loadReference"]>>>(
      (resolve) => {
        finishReference = resolve;
      },
    );
    const commandId = vi.fn(f.commandId);
    const controller = {
      ...f.controller,
      loadReference: vi.fn(async () => {
        referenceStarted();
        return pendingReference;
      }),
    };
    const result = refreshWorkstreamPr({
      controller,
      commandId,
      workstreamId: "beta",
      membershipId: "membership",
      referenceId: "reference",
      signal: abort.signal,
    });
    await started;
    abort.abort();
    finishReference({
      context: f.snapshot().references.context,
      reference,
      latest_observation: null,
    });
    await expect(result).rejects.toMatchObject({ name: "AbortError" });
    expect(commandId).not.toHaveBeenCalled();
    expect(f.submit).not.toHaveBeenCalled();
    expect(controller.loadActionSnapshot).toHaveBeenCalledOnce();
  });
  it("normalizes the full URL into a schema-validated canonical identity", () => {
    const locator = parseWorkstreamPrUrl("https://github.com/Jones-Systems/Jones-Code/pull/42");
    expect(canonicalWorkstreamPrId(locator)).toBe("jones-systems/jones-code#42");
    for (const url of [
      "#42",
      "https://gitlab.com/a/b/pull/42",
      "https://github.com/a/b/pull/0",
      "https://github.com/a/b/pull/42?x=1",
      "https://user:secret@github.com/a/b/pull/42",
    ])
      expect(() => parseWorkstreamPrUrl(url)).toThrow(WorkstreamActionError);
  });
  it("registers a separate PR identity then explicitly verifies and links it while T3 is absent", async () => {
    const f = fixture();
    vi.mocked(f.controller.loadActionSnapshot).mockImplementation(async () => {
      const value = f.snapshot();
      return {
        ...value,
        placements: null,
        registrationContext: {
          ...value.registrationContext!,
          sources: value.registrationContext!.sources.filter(
            (source) => source.provider === "github",
          ),
        },
      };
    });
    const input = {
      ...f,
      url: "https://github.com/Jones-Systems/Jones-Code/pull/42",
      workstreamId: "beta",
    };
    await prepareWorkstreamPr({ ...input, verify: false });
    expect(f.operations.map((command) => command.action.operation)).toEqual(["register_reference"]);
    await prepareWorkstreamPr({
      ...input,
      controller: { ...f.controller, data: f.snapshot().data },
      verify: true,
    });
    expect(f.operations.map((command) => command.action.operation)).toEqual([
      "register_reference",
      "verify_reference",
      "link_secondary",
    ]);
    expect(f.operations[0]?.action).toMatchObject({
      identity: {
        provider: "github",
        resource_kind: "pull_request",
        id_kind: "external",
        native_id: "jones-systems/jones-code#42",
      },
      pr_locator: { repository_owner: "jones-systems", repository_name: "jones-code", number: 42 },
    });
  });
  it("rejects the same PR identity with a conflicting immutable locator", async () => {
    const locator = parseWorkstreamPrUrl("https://github.com/a/b/pull/42");
    const f = fixture([
      {
        ...reference,
        identity: {
          ...reference.identity,
          provider: "github",
          source_instance_id: "github",
          resource_kind: "pull_request",
          id_kind: "external",
          native_id: canonicalWorkstreamPrId(locator),
        },
        pr_locator: { ...locator, number: 43 },
      },
    ]);
    await expect(
      prepareWorkstreamPr({
        ...f,
        workstreamId: "beta",
        url: "https://github.com/a/b/pull/42",
        verify: false,
      }),
    ).rejects.toMatchObject({ reason: "ambiguous" });
    expect(f.operations).toEqual([]);
  });
  it("retains last success and describes observation failure without inventing PR state", () => {
    const observation = Schema.decodeSync(WorkstreamPrObservation)({
      native_reference_id: "reference",
      observation_version: 2,
      attempted_at: "2026-09-30T12:00:00Z",
      outcome: "inaccessible",
      retry_after_seconds: null,
      last_success: {
        state: "merged",
        draft: false,
        observed_at: "2026-09-30T11:00:00Z",
        provider_updated_at: null,
      },
      command_id: "failed-refresh-001",
    });
    expect(workstreamPrObservationLabel(observation)).toBe("merged · refresh inaccessible");
    expect(
      workstreamPrObservationLabel(
        Schema.decodeSync(WorkstreamPrObservation)({
          ...observation,
          outcome: "unavailable",
          last_success: null,
        }),
      ),
    ).toBe("No successful observation · refresh unavailable");
  });
});
