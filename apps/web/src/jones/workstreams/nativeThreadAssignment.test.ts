import type {
  NativeReference,
  WorkstreamCommand,
  WorkstreamReceipt,
  WorkstreamsRegistrationContextAcceptedResponse,
} from "@t3tools/contracts";
import { beforeEach, afterEach, describe, expect, it, vi } from "vite-plus/test";
import {
  data,
  now,
  placements,
  primary,
  reference,
  thread,
} from "../../components/workstreams/nativeWorkstreamActions.fixtures";
import { assignNativeThread, type NativeThreadAssignmentSnapshot } from "./nativeThreadAssignment";

const registrationContext: WorkstreamsRegistrationContextAcceptedResponse = {
  protocol: "workstreams-registration-context/2.0.0",
  state: "ready",
  ...placements.context,
  grant_id: "grant",
  sources: [
    {
      provider: "t3",
      source_instance_id: thread.environmentId,
      authority_namespace: "authority",
      store_generation: 1,
      resource_kind: "thread",
      id_kind: "internal",
      account_provenance: { kind: "not_account_scoped" },
      native_protocol: "workstreams-t3-provider/1.0.0",
      build: { repository: "Jones-Systems/Jones-Code", sha: "a".repeat(40), tree: "b".repeat(40) },
    },
  ],
};
const quarantined: NativeReference = {
  ...reference,
  registration: {
    state: "quarantined",
    attestation_version: 0,
    attested_at: null,
    expires_at: null,
    evidence: null,
  },
};

function fixture(initial: NativeReference | null = null, placed = false, initialVersion = 11) {
  let version = initialVersion;
  let current = initial;
  let nextId = 0;
  const operations: WorkstreamCommand[] = [];
  const submit = vi.fn(async (command: WorkstreamCommand): Promise<WorkstreamReceipt> => {
    expect(command.expected_registry_version).toBe(version);
    operations.push(command);
    const reused = command.action.operation === "register_reference" && current !== null;
    if (!reused) version += 1;
    if (command.action.operation === "register_reference") current ??= quarantined;
    if (command.action.operation === "verify_reference") {
      expect(command.action.expected_attestation_version).toBe(
        current?.registration.attestation_version,
      );
      current = {
        ...reference,
        registration: {
          ...reference.registration,
          attestation_version: command.action.expected_attestation_version + 1,
        },
      };
    }
    return {
      command_id: command.command_id,
      owner_id: "owner",
      actor: { principal_id: "principal" },
      operation: command.action.operation,
      request_sha256: "c".repeat(64),
      server_generation: 7,
      accepted_at: new Date(now).toISOString(),
      completed_at: new Date(now).toISOString(),
      state: "committed",
      registry_version: version,
      changed: !reused,
      effects: {
        native_reference_id: current?.native_reference_id ?? null,
        registration: current?.registration ?? null,
        workstream_versions: [],
        membership_ids: [],
        declaration_id: null,
        declaration_revision: null,
        edge_id: null,
        observation: null,
        lifecycle_declaration: null,
        coordination_disposition: null,
        native_settlement: null,
      },
    };
  });
  const loadSnapshot = vi.fn(async (id?: string): Promise<NativeThreadAssignmentSnapshot> => {
    const context = { owner_id: "owner", server_generation: 7, registry_version: version };
    return {
      data: { ...data, binding: { ...data.binding, registryVersion: version } },
      registrationContext: { ...registrationContext, registry_version: version },
      placements: {
        ...placements,
        context: { ...placements.context, registry_version: version },
        items: placed ? [primary] : [],
      },
      reference:
        current && (id || placed)
          ? { context, reference: current, latest_observation: null }
          : null,
      destinationMemberships: { context, items: [], next_cursor: null },
    };
  });
  const run = (
    options: { signal?: AbortSignal; selected?: typeof thread; destination?: string | null } = {},
  ) =>
    assignNativeThread({
      data,
      thread: options.selected ?? thread,
      destination: options.destination === undefined ? "beta" : options.destination,
      loadSnapshot,
      runBindingOperation: async (operation) => operation(submit),
      commandId: async () => `command-${++nextId}`,
      ...(options.signal ? { signal: options.signal } : {}),
    });
  return { run, loadSnapshot, submit, operations };
}

beforeEach(() => vi.spyOn(Date, "now").mockReturnValue(now));
afterEach(() => vi.restoreAllMocks());

describe("chosen native thread assignment", () => {
  it("registers, verifies and attaches exactly one identity at each committed version", async () => {
    const f = fixture();
    await f.run();
    expect(f.operations.map((command) => command.action.operation)).toEqual([
      "register_reference",
      "verify_reference",
      "attach_primary",
    ]);
    expect(f.operations.map((command) => command.expected_registry_version)).toEqual([11, 12, 13]);
    expect(new Set(f.operations.map((command) => command.command_id)).size).toBe(3);
    expect(f.operations[0]?.action).toEqual({
      operation: "register_reference",
      identity: reference.identity,
      pr_locator: null,
    });
    expect(f.loadSnapshot.mock.calls).toEqual([[undefined], ["reference"], ["reference"]]);
  });

  it("uses the initial fresh registry version in every command precondition", async () => {
    const f = fixture(null, false, 12);
    await f.run();
    expect(f.operations.map((command) => command.expected_registry_version)).toEqual([12, 13, 14]);
    expect(f.operations.map((command) => command.action.operation)).toEqual([
      "register_reference",
      "verify_reference",
      "attach_primary",
    ]);
  });

  it.each([
    { ownerId: "other-owner" },
    { principalId: "other-principal" },
    { authorizationRevision: 2 },
    { serverGeneration: 8 },
    { registryId: "other-registry" },
    { permissions: ["workstreams:read"] as const },
  ])("rejects initial authority drift %j before any command", async (changed) => {
    const f = fixture(null, false, 12);
    const load = f.loadSnapshot.getMockImplementation()!;
    f.loadSnapshot.mockImplementation(async (id) => {
      const value = await load(id);
      return { ...value, data: { ...value.data, binding: { ...value.data.binding, ...changed } } };
    });
    await expect(f.run()).rejects.toMatchObject({ reason: "stale" });
    expect(f.submit).not.toHaveBeenCalled();
  });

  it("uses the register lookup receipt for an existing unplaced reference without re-verifying current evidence", async () => {
    const f = fixture(reference);
    await f.run();
    expect(f.operations.map((command) => command.action.operation)).toEqual([
      "register_reference",
      "attach_primary",
    ]);
    expect(f.operations.map((command) => command.expected_registry_version)).toEqual([11, 11]);
  });

  it("moves an existing primary directly and removes it without registering again", async () => {
    const f = fixture(reference, true);
    await f.run();
    expect(f.operations.map((command) => command.action.operation)).toEqual(["move_primary"]);
    const remove = fixture(reference, true);
    const load = remove.loadSnapshot.getMockImplementation()!;
    remove.loadSnapshot.mockImplementation(async (id) => ({
      ...(await load(id)),
      destinationMemberships: null,
    }));
    await remove.run({ destination: null });
    expect(remove.operations.map((command) => command.action.operation)).toEqual([
      "remove_membership",
    ]);
  });

  it("selects reattachment from closed destination history", async () => {
    const f = fixture(reference);
    const load = f.loadSnapshot.getMockImplementation()!;
    f.loadSnapshot.mockImplementation(async (id) => {
      const value = await load(id);
      return {
        ...value,
        destinationMemberships: {
          ...value.destinationMemberships!,
          items: [
            {
              membership_id: "closed",
              native_reference_id: reference.native_reference_id,
              workstream_id: "beta",
              kind: "primary",
              opened: {
                at: new Date(now).toISOString(),
                actor: { principal_id: "principal" },
                command_id: "opened",
                registry_version: 1,
              },
              closed: {
                at: new Date(now).toISOString(),
                actor: { principal_id: "principal" },
                command_id: "closed",
                registry_version: 2,
                reason: "other",
                other_reason: "Synthetic prior removal",
              },
            },
          ],
        },
      };
    });
    await f.run();
    expect(f.operations.at(-1)?.action.operation).toBe("reattach_primary");
  });

  it.each(["pending", "unresolved"] as const)(
    "stops at a %s receipt without submitting a successor",
    async (state) => {
      const f = fixture();
      f.submit.mockImplementationOnce(async (command) => ({
        command_id: command.command_id,
        owner_id: "owner",
        actor: { principal_id: "principal" },
        operation: command.action.operation,
        request_sha256: "c".repeat(64),
        server_generation: 7,
        accepted_at: new Date(now).toISOString(),
        state,
        retry_after_seconds: 1,
      }));
      await expect(f.run()).rejects.toMatchObject({ reason: "unknown" });
      expect(f.submit).toHaveBeenCalledOnce();
      expect(f.loadSnapshot).toHaveBeenCalledOnce();
    },
  );

  it("refuses an unrelated committed receipt without following it with verification", async () => {
    const f = fixture();
    const submit = f.submit.getMockImplementation()!;
    f.submit.mockImplementationOnce(async (command) => ({
      ...(await submit(command)),
      command_id: "different-command",
    }));
    await expect(f.run()).rejects.toMatchObject({ reason: "unknown" });
    expect(f.submit).toHaveBeenCalledOnce();
  });

  it("does not accept an authority-generation change even when the fresh trust snapshot agrees", async () => {
    const f = fixture(reference);
    const load = f.loadSnapshot.getMockImplementation()!;
    f.loadSnapshot.mockImplementation(async (id) => {
      const value = await load(id);
      if (!id) return value;
      return {
        ...value,
        registrationContext: {
          ...value.registrationContext,
          sources: value.registrationContext.sources.map((source) => ({
            ...source,
            store_generation: 2,
          })),
        },
        placements: {
          ...value.placements,
          trustedEnvironments: value.placements.trustedEnvironments.map((entry) => ({
            ...entry,
            storeGeneration: 2,
          })),
        },
      };
    });
    await expect(f.run()).rejects.toMatchObject({ reason: "stale" });
    expect(f.submit).toHaveBeenCalledOnce();
  });

  it("does not retry a lost command response", async () => {
    const f = fixture();
    f.submit.mockRejectedValueOnce(new Error("response lost"));
    await expect(f.run()).rejects.toThrow("response lost");
    expect(f.submit).toHaveBeenCalledOnce();
  });

  it("refuses the same thread ID from a different environment before registration", async () => {
    const f = fixture();
    await expect(f.run({ selected: { ...thread, environmentId: "remote" } })).rejects.toMatchObject(
      { reason: "activation" },
    );
    expect(f.submit).not.toHaveBeenCalled();
  });

  it.each(["version", "authority", "identity", "expiry"] as const)(
    "stops on changed %s after registration",
    async (mismatch) => {
      const f = fixture(reference);
      const load = f.loadSnapshot.getMockImplementation()!;
      f.loadSnapshot.mockImplementation(async (id) => {
        const value = await load(id);
        if (!id) return value;
        if (mismatch === "version")
          return {
            ...value,
            data: { ...value.data, binding: { ...value.data.binding, registryVersion: 999 } },
          };
        if (mismatch === "authority")
          return {
            ...value,
            registrationContext: { ...value.registrationContext, authorization_revision: 999 },
          };
        const detail = value.reference!;
        if (mismatch === "identity")
          return {
            ...value,
            reference: {
              ...detail,
              reference: {
                ...detail.reference,
                identity: { ...detail.reference.identity, source_instance_id: "remote" },
              },
            },
          };
        return {
          ...value,
          reference: {
            ...detail,
            reference: {
              ...detail.reference,
              registration: {
                ...detail.reference.registration,
                expires_at: new Date(now - 1).toISOString(),
              },
            },
          },
        };
      });
      await expect(f.run()).rejects.toThrow();
      expect(f.operations.some((command) => command.action.operation === "attach_primary")).toBe(
        false,
      );
      expect(
        f.operations.filter((command) => command.action.operation === "verify_reference"),
      ).toHaveLength(mismatch === "expiry" ? 1 : 0);
    },
  );

  it("stops after cancellation while a committed registration response is returning", async () => {
    const f = fixture();
    const abort = new AbortController();
    const submit = f.submit.getMockImplementation()!;
    f.submit.mockImplementationOnce(async (command) => {
      const value = await submit(command);
      abort.abort();
      return value;
    });
    await expect(f.run({ signal: abort.signal })).rejects.toThrow();
    expect(f.submit).toHaveBeenCalledOnce();
    expect(f.loadSnapshot).toHaveBeenCalledOnce();
  });
});
