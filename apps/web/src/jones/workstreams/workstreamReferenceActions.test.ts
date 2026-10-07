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
  type WorkstreamReferenceController,
  WorkstreamActionError,
} from "./workstreamActionSnapshot";
import type { WorkstreamDetailView } from "../../state/workstreams";
import { data, now, placements, reference } from "../../components/workstreams/nativeWorkstreamActions.fixtures";
import {
  canonicalWorkstreamPrId,
  parseWorkstreamPrUrl,
  prepareWorkstreamPr,
  createSnapshotSequence,
  refreshWorkstreamPr,
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
  const controller: WorkstreamReferenceController = {
    data,
    registrationContext,
    loading: false,
    retry: vi.fn(async () => {}),
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

describe("GitHub references", () => {
  it("stops a cancelled refresh after its deferred observation without allocating or submitting a command", async () => {
    const f = fixture([reference]);
    const abort = new AbortController();
    let referenceStarted!: () => void;
    const started = new Promise<void>((resolve) => {
      referenceStarted = resolve;
    });
    let finishReference!: (value: Awaited<ReturnType<WorkstreamReferenceController["loadReference"]>>) => void;
    const pendingReference = new Promise<Awaited<ReturnType<WorkstreamReferenceController["loadReference"]>>>(
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
      "https://github.com/a/b/pull/42#discussion",
      "http://github.com/a/b/pull/42",
      "https://github.com:444/a/b/pull/42",
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
  it("refuses mismatched owner, source, generation and duplicate context before registration", async () => {
    for (const patch of [
      { owner_id: "other-owner" },
      { server_generation: 8 },
      { sources: [...registrationContext.sources, registrationContext.sources[1]!] },
      { sources: [] },
    ]) {
      const f = fixture();
      vi.mocked(f.controller.loadActionSnapshot).mockImplementation(async () => ({
        ...f.snapshot(),
        registrationContext: { ...registrationContext, ...patch },
      }));
      await expect(prepareWorkstreamPr({ ...f, workstreamId: "beta", url: "https://github.com/a/b/pull/42", verify: false })).rejects.toBeInstanceOf(WorkstreamActionError);
      expect(f.submit).not.toHaveBeenCalled();
    }
  });
  it("rejects a reference registered under another GitHub source without adding a duplicate", async () => {
    const locator = parseWorkstreamPrUrl("https://github.com/a/b/pull/42");
    const f = fixture([{ ...reference, identity: { ...reference.identity, provider: "github", source_instance_id: "other-github", native_id: canonicalWorkstreamPrId(locator), resource_kind: "pull_request", id_kind: "external" }, pr_locator: locator }]);
    await expect(prepareWorkstreamPr({ ...f, workstreamId: "beta", url: "https://github.com/a/b/pull/42", verify: false })).rejects.toMatchObject({ reason: "ambiguous" });
    expect(f.submit).not.toHaveBeenCalled();
  });
  it("rejects rejected and unresolved receipts instead of granting a new registry version", async () => {
    const f = fixture();
    const sequence = createSnapshotSequence(f.controller);
    await sequence.load();
    for (const state of ["rejected", "unresolved", "pending"] as const) {
      expect(() => sequence.accept({ state, registry_version: 99 } as WorkstreamReceipt)).toThrow(WorkstreamActionError);
    }
  });
  it("accepts version changes only from committed receipts", async () => {
    const f = fixture();
    vi.mocked(f.controller.loadActionSnapshot).mockImplementation(async () => {
      const snapshot = f.snapshot();
      return { ...snapshot, data: { ...snapshot.data, binding: { ...snapshot.data.binding, registryVersion: 99 } }, references: { ...snapshot.references, context: { ...snapshot.references.context, registry_version: 99 } }, placements: null, registrationContext: { ...registrationContext, registry_version: 99 } };
    });
    await expect(prepareWorkstreamPr({ ...f, workstreamId: "beta", url: "https://github.com/a/b/pull/42", verify: false })).rejects.toMatchObject({ reason: "stale" });
    expect(f.submit).not.toHaveBeenCalled();
  });
  it("stops after an unknown registration receipt without verification or linking", async () => {
    const f = fixture();
    f.submit.mockResolvedValue({ state: "unresolved" } as WorkstreamReceipt);
    await expect(prepareWorkstreamPr({ ...f, workstreamId: "beta", url: "https://github.com/a/b/pull/42", verify: true })).rejects.toMatchObject({ reason: "unknown" });
    expect(f.submit).toHaveBeenCalledOnce();
    expect(f.submit.mock.calls[0]?.[0].action.operation).toBe("register_reference");
    expect(f.controller.loadDetail).not.toHaveBeenCalled();
  });
  it("does not register or link again when the reference is already a secondary member", async () => {
    const locator = parseWorkstreamPrUrl("https://github.com/a/b/pull/42");
    const pr: NativeReference = { ...reference, identity: { ...reference.identity, provider: "github", source_instance_id: "github", native_id: canonicalWorkstreamPrId(locator), resource_kind: "pull_request", id_kind: "external" }, pr_locator: locator };
    const f = fixture([pr]);
    vi.mocked(f.controller.loadDetail).mockImplementation(async () => ({
      detail: { context: f.snapshot().references.context, workstream: { workstream_id: "beta", version: f.snapshot().data.binding.registryVersion } },
      memberships: { items: [{ native_reference_id: pr.native_reference_id, closed: null, kind: "secondary" }] },
    }) as unknown as WorkstreamDetailView);
    await prepareWorkstreamPr({ ...f, workstreamId: "beta", url: "https://github.com/a/b/pull/42", verify: true });
    expect(f.operations.map((command) => command.action.operation)).toEqual(["verify_reference"]);
  });
  it("refreshes against current membership and observation versions and reads back the new receipt version", async () => {
    const f = fixture([reference]);
    const observed = Schema.decodeSync(WorkstreamPrObservation)({
      native_reference_id: "reference", observation_version: 2,
      attempted_at: "2026-09-30T12:00:00Z", outcome: "observed", retry_after_seconds: null,
      last_success: { state: "open", draft: false, observed_at: "2026-09-30T12:00:00Z", provider_updated_at: null },
      command_id: "refresh-command-001",
    });
    const fresh = { ...observed, observation_version: 3 };
    vi.mocked(f.controller.loadReference).mockImplementation(async () => ({
      context: f.snapshot().references.context, reference,
      latest_observation: f.operations.length ? fresh : observed,
    }));
    expect(await refreshWorkstreamPr({ ...f, workstreamId: "beta", membershipId: "membership", referenceId: "reference" })).toEqual(fresh);
    expect(f.operations).toHaveLength(1);
    expect(f.operations[0]).toMatchObject({
      expected_server_generation: 7, expected_registry_version: 11,
      action: { operation: "refresh_linked_pr", workstream_id: "beta", expected_version: 11, membership_id: "membership", expected_observation_version: 2 },
    });
    expect(f.controller.loadReference).toHaveBeenCalledTimes(2);
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
