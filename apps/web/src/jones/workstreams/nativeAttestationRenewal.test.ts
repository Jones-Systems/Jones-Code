import { EnvironmentHttpConflictError } from "@t3tools/contracts";
import type {
  NativeReference,
  WorkstreamCommand,
  WorkstreamReceipt,
  WorkstreamReferencePage,
} from "@t3tools/contracts";
import { afterEach, beforeEach, describe, expect, it, vi } from "vite-plus/test";
import {
  data,
  now,
  placements,
  primary,
  reference,
  thread,
} from "../../components/workstreams/nativeWorkstreamActions.fixtures";
import {
  runNativeAttestationRenewal,
  type NativeAttestationRenewalInput,
  type RenewalSnapshot,
} from "./nativeAttestationRenewal";

const source = {
  provider: "t3" as const,
  source_instance_id: thread.environmentId,
  authority_namespace: "authority",
  store_generation: 1,
  resource_kind: "thread" as const,
  id_kind: "internal" as const,
  account_provenance: { kind: "not_account_scoped" as const },
  native_protocol: "workstreams-t3-provider/1.0.0" as const,
  build: {
    repository: "Jones-Systems/Jones-Code" as const,
    sha: "a".repeat(40),
    tree: "b".repeat(40),
  },
};
const aborters: AbortController[] = [];
beforeEach(() => {
  vi.useFakeTimers();
  vi.setSystemTime(now);
});
afterEach(async () => {
  for (const controller of aborters.splice(0)) controller.abort();
  await vi.advanceTimersByTimeAsync(0);
  vi.useRealTimers();
});

function fixture(expired = false) {
  let version = 11;
  let native: NativeReference = {
    ...reference,
    registration: {
      ...reference.registration,
      attested_at: new Date(now - 1_000).toISOString(),
      expires_at: new Date(now + (expired ? -1 : 300_000)).toISOString(),
    },
  };
  const records = new Map<string, string>();
  const storage = {
    getItem: (key: string) => records.get(key) ?? null,
    setItem: (key: string, value: string) => {
      records.set(key, value);
    },
  };
  const context = () => ({ ...placements.context, registry_version: version });
  const member = {
    membership_id: "membership",
    workstream_id: "alpha",
    native_reference_id: "reference",
    kind: "primary" as const,
    opened: {
      at: new Date(now).toISOString(),
      actor: { principal_id: "principal" },
      command_id: "original-attach",
      registry_version: 1,
    },
    closed: null,
  };
  const snapshot = (): RenewalSnapshot => ({
    data: { ...data, binding: { ...data.binding, registryVersion: version } },
    context: {
      protocol: "workstreams-registration-context/2.0.0",
      state: "ready",
      ...context(),
      grant_id: "grant",
      sources: [source],
    },
    placements: {
      ...placements,
      context: context(),
      items:
        native.registration.state === "attested" &&
        Date.parse(native.registration.expires_at!) > Date.now()
          ? [
              {
                ...primary,
                attestation_version: native.registration.attestation_version,
                attested_at: native.registration.attested_at!,
                expires_at: native.registration.expires_at!,
              },
            ]
          : [],
    },
  });
  const commit = (command: WorkstreamCommand): WorkstreamReceipt => {
    expect(command.action.operation).toBe("verify_reference");
    if (command.action.operation !== "verify_reference") throw new Error("unexpected operation");
    expect(command.action.native_reference_id).toBe("reference");
    expect(command.action.expected_attestation_version).toBe(
      native.registration.attestation_version,
    );
    expect(command.expected_registry_version).toBe(version);
    version++;
    native = {
      ...native,
      registration: {
        ...native.registration,
        state: "attested",
        attestation_version: native.registration.attestation_version + 1,
        attested_at: new Date(Date.now()).toISOString(),
        expires_at: new Date(Date.now() + 300_000).toISOString(),
      },
    };
    return {
      command_id: command.command_id,
      owner_id: "owner",
      actor: { principal_id: "principal" },
      operation: "verify_reference",
      request_sha256: "c".repeat(64),
      server_generation: 7,
      accepted_at: new Date(Date.now()).toISOString(),
      completed_at: new Date(Date.now()).toISOString(),
      state: "committed",
      registry_version: version,
      changed: true,
      effects: {
        native_reference_id: "reference",
        registration: native.registration,
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
  };
  let sequence = 0;
  const ports = {
    identities: [{ source_instance_id: thread.environmentId, native_thread_id: thread.id }],
    storage,
    load: vi.fn(async () => snapshot()),
    references: vi.fn(async (): Promise<WorkstreamReferencePage> => ({
      context: context(),
      items: [native],
      next_cursor: null,
    })),
    reference: vi.fn(async () => ({
      context: context(),
      reference: native,
      latest_observation: null,
    })),
    memberships: vi.fn(async (id: string) => ({
      context: context(),
      items: id === "alpha" ? [member] : [],
      next_cursor: null,
    })),
    submit: vi.fn(async (command: WorkstreamCommand) => commit(command)),
    observe: vi.fn<(id: string) => Promise<WorkstreamReceipt>>(),
    publish: vi.fn<(value: RenewalSnapshot) => void>(),
    unavailable: vi.fn(),
    commandId: () => `renew-${++sequence}`,
  };
  const start = (overrides: Partial<NativeAttestationRenewalInput> = {}) => {
    const controller = new AbortController();
    aborters.push(controller);
    const outcome = runNativeAttestationRenewal({
      ...ports,
      initial: snapshot(),
      signal: controller.signal,
      ...overrides,
    }).catch((cause: unknown) => {
      if (!controller.signal.aborted) throw cause;
    });
    return { controller, outcome };
  };
  return {
    ports,
    start,
    snapshot,
    records,
    commit,
    changeReference: (update: (value: NativeReference) => NativeReference) => {
      native = update(native);
    },
  };
}

describe("native attestation renewal", () => {
  it("renews the same reference and membership through two original expiry boundaries", async () => {
    const f = fixture();
    f.start();
    await vi.advanceTimersByTimeAsync(600_000);
    expect(f.ports.submit).toHaveBeenCalledTimes(2);
    expect(f.ports.publish).toHaveBeenCalledTimes(2);
    expect(f.ports.publish.mock.lastCall?.[0].placements.items[0]).toMatchObject({
      native_reference_id: "reference",
      membership_id: "membership",
      workstream_id: "alpha",
      attestation_version: 4,
    });
    expect(
      Date.parse(f.ports.publish.mock.lastCall![0].placements.items[0]!.expires_at),
    ).toBeGreaterThan(Date.now());
    expect(f.ports.references).not.toHaveBeenCalled();
  });
  it("discovers an expired cold-load reference only through matching inventory and open membership", async () => {
    const f = fixture(true);
    f.start();
    await vi.advanceTimersByTimeAsync(0);
    expect(f.ports.references).toHaveBeenCalledTimes(1);
    expect(f.ports.submit).toHaveBeenCalledTimes(1);
    expect(f.ports.publish.mock.lastCall?.[0].placements.items[0]?.membership_id).toBe(
      "membership",
    );
  });
  it("does not turn unassigned or remote metadata into a command", async () => {
    const f = fixture(true);
    f.ports.memberships.mockResolvedValue({
      context: placements.context,
      items: [],
      next_cursor: null,
    });
    await f.start().outcome;
    expect(f.ports.submit).not.toHaveBeenCalled();
    await f.start({ identities: [{ source_instance_id: "remote", native_thread_id: thread.id }] })
      .outcome;
    expect(f.ports.submit).not.toHaveBeenCalled();
  });
  it("refuses read-only and changed authority before submitting", async () => {
    const f = fixture();
    const initial = f.snapshot();
    await expect(
      f.start({
        initial: {
          ...initial,
          data: {
            ...initial.data,
            binding: { ...initial.data.binding, permissions: ["workstreams:read"] },
          },
        },
      }).outcome,
    ).rejects.toThrow();
    f.ports.load.mockResolvedValue({
      ...initial,
      context: { ...initial.context, authorization_revision: 2 },
    });
    f.start();
    await vi.advanceTimersByTimeAsync(240_000);
    expect(f.ports.submit).not.toHaveBeenCalled();
    expect(f.ports.unavailable).toHaveBeenCalledOnce();
  });
  it("uses another device's fresh proof without another POST", async () => {
    const f = fixture();
    f.start();
    f.changeReference((value) => ({
      ...value,
      registration: {
        ...value.registration,
        attestation_version: 3,
        expires_at: new Date(now + 600_000).toISOString(),
      },
    }));
    await vi.advanceTimersByTimeAsync(240_000);
    expect(f.ports.submit).not.toHaveBeenCalled();
    expect(f.ports.publish.mock.lastCall?.[0].placements.items[0]?.attestation_version).toBe(3);
  });
  it("deduplicates concurrent callbacks and cancels the next deadline", async () => {
    const f = fixture();
    const first = f.start();
    const second = f.start();
    await vi.advanceTimersByTimeAsync(240_000);
    expect(f.ports.submit).toHaveBeenCalledOnce();
    first.controller.abort();
    second.controller.abort();
    await vi.advanceTimersByTimeAsync(600_000);
    expect(f.ports.submit).toHaveBeenCalledOnce();
  });
  it("persists before POST and reconciles a lost response by GET after reload, never replaying", async () => {
    const f = fixture();
    let committed: WorkstreamReceipt;
    f.ports.submit.mockImplementation(async (command) => {
      expect([...f.records.values()].join("")).toContain(command.command_id);
      committed = f.commit(command);
      throw new Error("lost response");
    });
    const first = f.start();
    await vi.advanceTimersByTimeAsync(240_000);
    await first.outcome;
    f.ports.observe.mockImplementation(async () => committed);
    f.start();
    await vi.advanceTimersByTimeAsync(0);
    expect(f.ports.submit).toHaveBeenCalledOnce();
    expect(f.ports.observe).toHaveBeenCalledExactlyOnceWith("renew-1");
    expect(f.ports.publish).toHaveBeenCalledOnce();
  });
  it("leaves unresolved GET receipts pending across reconnect without POST", async () => {
    const f = fixture();
    f.ports.submit.mockRejectedValue(new Error("unknown"));
    await (async () => {
      const run = f.start();
      await vi.advanceTimersByTimeAsync(240_000);
      await run.outcome;
    })();
    f.ports.observe.mockRejectedValue(new Error("still unknown"));
    await f.start().outcome;
    expect(f.ports.submit).toHaveBeenCalledOnce();
    expect(f.ports.observe).toHaveBeenCalledOnce();
  });
  it("does not accept committed failed verification or retry the same observed proof", async () => {
    const f = fixture();
    f.ports.submit.mockImplementation(async (command) => {
      const receipt = f.commit(command);
      f.changeReference((value) => ({
        ...value,
        registration: {
          ...value.registration,
          state: "verification-failed",
          attestation_version: 2,
        },
      }));
      return receipt;
    });
    const run = f.start();
    await vi.advanceTimersByTimeAsync(240_000);
    await run.outcome;
    expect(f.ports.publish).not.toHaveBeenCalled();
    expect(f.ports.unavailable).toHaveBeenCalledOnce();
    await f.start().outcome;
    expect(f.ports.submit).toHaveBeenCalledOnce();
  });
  it("settles a pending verification with bounded GETs and no resubmission", async () => {
    const f = fixture();
    let committed: WorkstreamReceipt;
    f.ports.submit.mockImplementation(async (command) => {
      committed = f.commit(command);
      return { ...committed, state: "pending", retry_after_seconds: 1 };
    });
    f.ports.observe.mockImplementation(async () => committed);
    f.start();
    await vi.advanceTimersByTimeAsync(241_000);
    expect(f.ports.submit).toHaveBeenCalledOnce();
    expect(f.ports.observe).toHaveBeenCalledExactlyOnceWith("renew-1");
    expect(f.ports.publish).toHaveBeenCalledOnce();
  });
  it("reads back a conflicting other-device proof without replaying the command", async () => {
    const f = fixture();
    f.ports.submit.mockImplementation(async (command) => {
      f.commit(command);
      throw new EnvironmentHttpConflictError({ message: "registry_version_conflict" });
    });
    const run = f.start();
    await vi.advanceTimersByTimeAsync(240_000);
    await run.outcome;
    expect(f.ports.submit).toHaveBeenCalledOnce();
    expect(f.ports.publish).toHaveBeenCalledOnce();
    expect(f.ports.publish.mock.lastCall?.[0].placements.items[0]?.attestation_version).toBe(3);
  });
  it("cannot submit after cancellation of an in-flight preflight", async () => {
    const f = fixture();
    let finish: ((value: RenewalSnapshot) => void) | undefined;
    f.ports.load.mockImplementation(
      () =>
        new Promise((resolve) => {
          finish = resolve;
        }),
    );
    const run = f.start();
    await vi.advanceTimersByTimeAsync(240_000);
    run.controller.abort();
    finish?.(f.snapshot());
    await run.outcome;
    expect(f.ports.submit).not.toHaveBeenCalled();
    expect(f.ports.publish).not.toHaveBeenCalled();
  });
  it("bounds cold-load reference discovery to ten pages and exposes incomplete coverage", async () => {
    const f = fixture(true);
    let page = 0;
    f.ports.references.mockImplementation(async () => ({
      context: placements.context,
      items: [],
      next_cursor: `page-${++page}`,
    }));
    await f.start().outcome;
    expect(f.ports.references).toHaveBeenCalledTimes(10);
    expect(f.ports.unavailable).toHaveBeenCalledOnce();
    expect(f.ports.submit).not.toHaveBeenCalled();
  });
  it("stops before POST if durable intent cannot be retained", async () => {
    const f = fixture();
    const run = f.start({
      storage: {
        getItem: () => null,
        setItem: () => {
          throw new Error("storage full");
        },
      },
    });
    await vi.advanceTimersByTimeAsync(240_000);
    await run.outcome;
    expect(f.ports.submit).not.toHaveBeenCalled();
  });
});
