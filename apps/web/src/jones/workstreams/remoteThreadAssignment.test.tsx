import { act } from "../testing/nodeRenderer";
import { RegistryContext } from "@effect/atom-react";
import type { SupervisorConnectionState } from "@t3tools/client-runtime/connection";
import { RemoteEnvironmentAuthFetchError } from "@t3tools/client-runtime/rpc";
import { nativeWorkstreamThreadKey } from "@t3tools/client-runtime/state/workstreams";
import {
  EnvironmentHttpConflictError,
  EnvironmentScopeRequiredError,
  EnvironmentRequestInvalidError,
  EnvironmentInternalError,
  EnvironmentHttpUnauthorizedError,
  t3PlacementInventoryJson,
  type NativeReference,
  type T3PlacementIdentity,
  type T3WorkstreamListResult,
  type WorkstreamCommand,
  type WorkstreamReceipt,
  type WorkstreamsRegistrationContextAcceptedResponse,
} from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import { Atom, AtomRegistry, AsyncResult } from "effect/reactivity";
import { useLayoutEffect } from "react";
import { create, type ReactTestRenderer } from "react-test-renderer";
import { afterEach, beforeEach, describe, expect, it, vi } from "vite-plus/test";

import { workstreamFailureMessage } from "./workstreamActionSnapshot";
import { data } from "../../components/workstreams/nativeWorkstreamActions.fixtures";
import { useWorkstreams, type WorkstreamListView } from "../../state/workstreams";
import {
  routeWorkstreamsHttp,
  syntheticWorkstreams,
  type SyntheticAssignment,
  type WorkstreamsHttpCall,
} from "./workstreamsHttp.fixtures";

const http = vi.hoisted(() => ({
  sessionRefresh: vi.fn<(id: string) => Promise<void>>(),
  run: vi.fn<(effect: unknown, options?: { readonly signal?: AbortSignal }) => Promise<unknown>>(),
  environment:
    vi.fn<
      (
        environmentId: string,
        method: "GET" | "POST",
        url: unknown,
        run: (client: unknown, headers: unknown) => unknown,
        signal?: AbortSignal,
      ) => Promise<unknown>
    >(),
}));
vi.mock("../../state/session", () => ({
  refreshEnvironmentSession: http.sessionRefresh,
  readPreparedConnection: () => ({}),
}));
vi.mock("../../lib/runtime", () => ({ runPrimaryHttp: http.run }));
vi.mock("./environmentWorkstreamsRequest", () => ({
  requestEnvironmentWorkstreams: http.environment,
}));
vi.mock("../../connection/catalog", () => ({
  environmentCatalog: { stateAtom: () => connection },
}));
vi.mock("../../state/primaryEnvironment", async () => {
  const { Atom } = await import("effect/reactivity");
  const { EnvironmentId } = await import("@t3tools/contracts");
  return { primaryEnvironmentIdAtom: Atom.make(EnvironmentId.make("env:a")) };
});

const connected: SupervisorConnectionState = {
  desired: true,
  network: "online",
  phase: "connected",
  stage: null,
  attempt: 1,
  generation: 1,
  lastFailure: null,
  retryAt: null,
};
const connection = Atom.make(AsyncResult.success(connected));
const realSetTimeout = globalThis.setTimeout;
const start = Date.parse("2026-09-30T12:00:00Z");

const local = { environmentId: "env:a", id: "local" };
const primarySource = {
  source_instance_id: local.environmentId,
  authority_namespace: "authority",
  store_generation: 1,
};
const remote = { environmentId: "env:b", id: "remote" };
const threads = [local, remote];
const sha256Hex = async (text: string): Promise<string> =>
  Array.from(
    new Uint8Array(await crypto.subtle.digest("SHA-256", new TextEncoder().encode(text))),
    (value) => value.toString(16).padStart(2, "0"),
  ).join("");
const keyOf = (thread: { readonly environmentId: string; readonly id: string }) =>
  nativeWorkstreamThreadKey(thread.environmentId, thread.id);

let registry: ReturnType<typeof AtomRegistry.make>;
let renderer: ReactTestRenderer | undefined;
let latest: WorkstreamListView;
let staleRenders: boolean[];

function Mounted() {
  const view = useWorkstreams(true, threads);
  useLayoutEffect(() => {
    latest = view;
    staleRenders.push(view.stale === true);
  });
  return null;
}

async function settle(check: () => void, attempts = 3_000): Promise<void> {
  for (let attempt = 0; ; attempt += 1) {
    await act(async () => {
      await new Promise((resolve) => realSetTimeout(resolve, 0));
    });
    try {
      check();
      return;
    } catch (cause) {
      if (attempt >= attempts) throw cause;
    }
  }
}

/** Records the route and request a call builds; the effect it returns is never run. */
const captureCall = (run: (client: unknown, headers: unknown) => unknown): WorkstreamsHttpCall => {
  let captured: WorkstreamsHttpCall | undefined;
  run(
    new Proxy(
      {},
      {
        get: (_target, route) => (request: WorkstreamsHttpCall["request"]) => {
          captured = { route: String(route), request };
          return Effect.void;
        },
      },
    ),
    {},
  );
  if (captured === undefined) throw new Error("The request did not call a Workstreams route.");
  return captured;
};

interface EnvironmentCall extends WorkstreamsHttpCall {
  readonly environmentId: string;
}

/** The synthetic control plane's membership ID for an assignment at a list position. */
const membershipIdOf = (assignment: SyntheticAssignment, position: number) =>
  assignment.membershipId ?? `membership-${String(position).padStart(5, "0")}`;

/**
 * The thread's own environment: it shares the primary's control plane (one registry version,
 * stored memberships visible to the primary index) but answers with its own principal, its own
 * registration source and its own native-store trust.
 */
function remoteEnvironment(shared: ReturnType<typeof syntheticWorkstreams>) {
  const state = {
    calls: [] as EnvironmentCall[],
    reference: null as NativeReference | null,
    availabilityHeader: undefined as string | undefined,
    /** Return a value to answer or throw to fail a call before the default handler runs. */
    intercept: null as ((call: EnvironmentCall) => unknown) | null,
    binding: {
      ...data.binding,
      principalId: "principal-b",
      authorizationRevision: 2,
    } as T3WorkstreamListResult["binding"],
    sources: [
      {
        provider: "t3",
        source_instance_id: remote.environmentId,
        authority_namespace: "authority-b",
        store_generation: 3,
        resource_kind: "thread",
        id_kind: "internal",
        account_provenance: { kind: "not_account_scoped" },
        native_protocol: "workstreams-t3-provider/1.0.0",
        build: {
          repository: "Jones-Systems/Jones-Code",
          sha: "a".repeat(40),
          tree: "b".repeat(40),
        },
      },
    ] as WorkstreamsRegistrationContextAcceptedResponse["sources"],
  };
  const readContext = () => ({
    owner_id: state.binding.ownerId,
    server_generation: state.binding.serverGeneration,
    registry_version: shared.state.version,
  });
  const placementContext = () => ({
    ...readContext(),
    principal_id: state.binding.principalId,
    authorization_revision: state.binding.authorizationRevision,
  });
  const identity = {
    provider: "t3" as const,
    source_instance_id: remote.environmentId,
    native_id: remote.id,
    resource_kind: "thread" as const,
    id_kind: "internal" as const,
    account_provenance: { kind: "not_account_scoped" as const },
  };
  const receipt = (command: WorkstreamCommand): WorkstreamReceipt =>
    ({
      command_id: command.command_id,
      owner_id: state.binding.ownerId,
      actor: { principal_id: state.binding.principalId },
      operation: command.action.operation,
      request_sha256: "c".repeat(64),
      server_generation: state.binding.serverGeneration,
      accepted_at: vi.getMockedSystemTime()!.toISOString(),
      completed_at: vi.getMockedSystemTime()!.toISOString(),
      state: "committed",
      registry_version: shared.state.version,
      changed: true,
      effects: { native_reference_id: state.reference?.native_reference_id ?? null },
    }) as unknown as WorkstreamReceipt;
  const apply = (command: WorkstreamCommand) => {
    expect(command.expected_registry_version).toBe(shared.state.version);
    const action = command.action;
    shared.state.version += 1;
    if (action.operation === "register_reference") {
      state.reference = {
        native_reference_id: "reference-b",
        owner_id: state.binding.ownerId,
        identity,
        pr_locator: null,
        registration: {
          state: "quarantined",
          attestation_version: 0,
          attested_at: null,
          expires_at: null,
          evidence: null,
        },
        created_at: "2026-09-30T11:00:00Z",
        created_by: { principal_id: state.binding.principalId },
        created_registry_version: shared.state.version,
      };
    } else if (action.operation === "verify_reference" && state.reference) {
      state.reference = {
        ...state.reference,
        registration: {
          state: "attested",
          attestation_version: action.expected_attestation_version + 1,
          attested_at: "2026-09-30T11:59:00Z",
          expires_at: "2026-09-30T12:04:00Z",
          evidence: {
            provider: "t3",
            source_instance_id: remote.environmentId,
            native_id: remote.id,
            authority_namespace: "authority-b",
            store_generation: 3,
            evidence_sha256: "a".repeat(64),
          },
        },
      };
    } else if (action.operation === "attach_primary") {
      // The control plane is shared, so the primary's index reads the new membership.
      shared.state.assignments.push({
        environmentId: remote.environmentId,
        threadId: remote.id,
        workstreamId: action.workstream_id,
      });
    } else if (action.operation === "remove_membership") {
      const index = shared.state.assignments.findIndex(
        (assignment, position) => membershipIdOf(assignment, position) === action.membership_id,
      );
      expect(index).toBeGreaterThanOrEqual(0);
      shared.state.assignments.splice(index, 1);
    }
    return receipt(command);
  };
  /** Proof from this environment's own store: only a current attestation proves membership. */
  const placementItems = (identities: readonly T3PlacementIdentity[]) => {
    const reference = state.reference;
    const evidence = reference?.registration.evidence;
    if (
      !reference ||
      !evidence ||
      reference.registration.state !== "attested" ||
      Date.parse(reference.registration.expires_at ?? "") <= vi.getMockedSystemTime()!.getTime() ||
      !identities.some(
        (identity) =>
          identity.source_instance_id === remote.environmentId &&
          identity.native_thread_id === remote.id,
      )
    )
      return [];
    return shared.state.assignments.flatMap((assignment, position) =>
      assignment.environmentId === remote.environmentId && assignment.threadId === remote.id
        ? [
            {
              native_reference_id: reference.native_reference_id,
              membership_id: membershipIdOf(assignment, position),
              workstream_id: assignment.workstreamId,
              kind: assignment.kind ?? "primary",
              source_instance_id: remote.environmentId,
              native_thread_id: remote.id,
              attestation_version: reference.registration.attestation_version,
              attested_at: reference.registration.attested_at!,
              expires_at: reference.registration.expires_at!,
              evidence_sha256: evidence.evidence_sha256,
              authority_namespace: evidence.authority_namespace,
              store_generation: evidence.store_generation,
              source_binding_version: 1,
            },
          ]
        : [],
    );
  };
  const handle = async (call: EnvironmentCall): Promise<unknown> => {
    state.calls.push(call);
    if (state.intercept) {
      const intercepted = await state.intercept(call);
      if (intercepted !== undefined) return intercepted;
    }
    switch (call.route) {
      case "list":
        return {
          ...data,
          binding: { ...state.binding, registryVersion: shared.state.version },
        };
      case "registrationContext":
        return {
          protocol: "workstreams-registration-context/2.0.0",
          state: "ready",
          ...placementContext(),
          grant_id: "grant-b",
          sources: state.sources,
        };
      case "threadPlacements": {
        const identities = call.request.payload?.identities as readonly T3PlacementIdentity[];
        return {
          page: {
            inventory_sha256: await sha256Hex(t3PlacementInventoryJson(identities)),
            context: placementContext(),
            items: placementItems(identities),
            next_cursor: null,
          },
          trustedEnvironments: [
            {
              environmentId: remote.environmentId,
              authorityNamespace: "authority-b",
              storeGeneration: 3,
            },
          ],
          readiness: "ready",
        };
      }
      case "reference":
        return { context: readContext(), reference: state.reference, latest_observation: null };
      case "memberships":
        return { context: readContext(), items: [], next_cursor: null };
      case "submit":
        return apply(call.request.payload?.command as WorkstreamCommand);
      case "command":
        throw new Error("No command was lost.");
      default:
        throw new Error(`Unexpected remote Workstreams route ${call.route}`);
    }
  };
  http.environment.mockImplementation(async (environmentId, _method, _url, run, signal) => {
    signal?.throwIfAborted();
    const call = { ...captureCall(run), environmentId };
    const value = await handle(call);
    return call.request.responseMode === "decoded-and-response"
      ? [value, { headers: { "x-jones-workstreams-t3-availability": state.availabilityHeader } }]
      : value;
  });
  return { state, apply };
}

async function mountWith(shared: ReturnType<typeof syntheticWorkstreams>) {
  http.run.mockImplementation(routeWorkstreamsHttp(shared.handle));
  await act(async () => {
    renderer = create(
      <RegistryContext.Provider value={registry}>
        <Mounted />
      </RegistryContext.Provider>,
    );
  });
  await settle(() => {
    expect(latest.loading).toBe(false);
    expect(latest.data).not.toBeNull();
    expect(latest.assignmentIndex).not.toBeNull();
  });
}

async function assignRemote(destination: string | null = "beta"): Promise<unknown> {
  let nextId = 0;
  let failure: unknown;
  await act(async () => {
    await latest.assignNativeThread!(remote, destination, {
      commandId: async () => `remote-command-${++nextId}`,
    }).catch((cause: unknown) => {
      failure = cause;
    });
  });
  return failure;
}

const routes = (calls: readonly { readonly route: string }[]) => calls.map((call) => call.route);
const primarySubmits = (shared: ReturnType<typeof syntheticWorkstreams>) =>
  shared.state.calls.filter((call) => call.route === "submit" || call.route === "command");

beforeEach(() => {
  vi.useFakeTimers({ toFake: ["setTimeout", "clearTimeout", "Date"], now: start });
  vi.stubGlobal("window", globalThis);
  http.sessionRefresh.mockReset().mockResolvedValue(undefined);
  http.run.mockReset();
  http.environment.mockReset();
  registry = AtomRegistry.make();
  registry.set(connection, AsyncResult.success(connected));
  staleRenders = [];
});

afterEach(async () => {
  await act(() => renderer?.unmount());
  renderer = undefined;
  registry.dispose();
  vi.useRealTimers();
  vi.unstubAllGlobals();
  vi.restoreAllMocks();
});

describe("assigning a thread from another environment", () => {
  const primaryCommand = (id: string): WorkstreamCommand => ({
    command_id: id,
    expected_server_generation: data.binding.serverGeneration,
    expected_registry_version: latest.data!.binding.registryVersion,
    action: {
      operation: "update_workstream",
      workstream_id: "alpha",
      expected_version: 3,
      name: "renamed",
      lifecycle: "active",
      progress: { state: "unknown" },
      sort_order: 0,
    },
  });
  const primarySubmit = async (id: string) => {
    let failure: unknown;
    await act(async () => {
      await latest.submit(primaryCommand(id)).catch((cause: unknown) => {
        failure = cause;
      });
    });
    return failure;
  };
  it.each([
    [new EnvironmentHttpConflictError({ message: "workstream_command_not_current" }), "stale"],
    [
      new EnvironmentScopeRequiredError({
        code: "insufficient_scope",
        traceId: "test",
        requiredScope: "orchestration:operate",
      }),
      "denied",
    ],
    [
      new EnvironmentRequestInvalidError({
        code: "invalid_request",
        reason: "invalid_command",
        traceId: "test",
      }),
      "denied",
    ],
  ])("releases only a primary known pre-effect rejection", async (failure, reason) => {
    const shared = syntheticWorkstreams({
      now: () => vi.getMockedSystemTime()!.getTime(),
      assignments: [],
    });
    await mountWith(shared);
    shared.state.intercept = (call) => {
      if (call.route === "submit") throw failure;
    };
    expect(await primarySubmit("primary-no-effect")).toMatchObject({ reason });
    await act(async () => latest.refresh());
    await settle(() => expect(latest.loading).toBe(false));
    expect(latest.data).not.toBeNull();
    shared.state.intercept = (call) =>
      call.route === "submit"
        ? { command_id: "primary-next", state: "committed", registry_version: shared.state.version }
        : undefined;
    expect(await primarySubmit("primary-next")).toBeUndefined();
    expect(shared.state.calls.filter((call) => call.route === "command")).toEqual([]);
    expect(shared.state.calls.filter((call) => call.route === "submit")).toHaveLength(2);
  });
  it.each([
    new Error("lost response"),
    new EnvironmentInternalError({
      code: "internal_error",
      reason: "internal_error",
      traceId: "test",
    }),
    new EnvironmentHttpConflictError({ message: "idempotency-conflict" }),
  ])(
    "keeps primary ambiguous submissions fenced and reconciles before metadata",
    async (failure) => {
      const shared = syntheticWorkstreams({
        now: () => vi.getMockedSystemTime()!.getTime(),
        assignments: [],
      });
      await mountWith(shared);
      shared.state.intercept = (call) => {
        if (call.route === "submit") throw failure;
      };
      expect(await primarySubmit("primary-lost")).toMatchObject({ reason: "unknown" });
      await settle(() => expect(latest.loading).toBe(false));
      expect(await primarySubmit("primary-blocked")).toMatchObject({ reason: "unknown" });
      shared.state.intercept = (call) => {
        if (call.route === "command") throw new Error("receipt unavailable");
      };
      await act(async () => {
        await expect(latest.retry!()).rejects.toMatchObject({ reason: "unknown" });
      });
      const receiptsBeforeRecovery = shared.state.calls.filter(
        (call) => call.route === "command",
      ).length;
      shared.state.intercept = (call) => {
        if (call.route === "list") throw new Error("metadata offline");
        if (call.route === "command")
          return {
            command_id: "primary-lost",
            state: "committed",
            registry_version: shared.state.version,
          };
      };
      await act(async () => {
        await latest.retry!().catch(() => undefined);
      });
      expect(
        shared.state.calls
          .filter((call) => call.route === "command")
          .slice(receiptsBeforeRecovery)
          .map((call) => call.request.params?.commandId),
      ).toEqual(["primary-lost"]);
      expect(shared.state.calls.filter((call) => call.route === "submit")).toHaveLength(1);
      shared.state.intercept = (call) =>
        call.route === "submit"
          ? {
              command_id: "primary-next",
              state: "committed",
              registry_version: shared.state.version,
            }
          : undefined;
      await act(async () => latest.refresh());
      await settle(() => expect(latest.loading).toBe(false));
      expect(await primarySubmit("primary-next")).toBeUndefined();
    },
  );
  it.each([false, true])(
    "classifies a loading error by actual submission phase (%s)",
    async (submitted) => {
      const shared = syntheticWorkstreams({
        now: () => vi.getMockedSystemTime()!.getTime(),
        assignments: [],
      });
      await mountWith(shared);
      shared.state.intercept = (call) =>
        call.route === "submit"
          ? {
              command_id: "primary-phase",
              state: "committed",
              registry_version: shared.state.version,
            }
          : undefined;
      let failure: unknown;
      await act(async () => {
        await latest
          .runBindingOperation(async (submit) => {
            if (submitted) await submit(primaryCommand("primary-phase"));
            throw new DOMException("read aborted", "AbortError");
          })
          .catch((cause: unknown) => {
            failure = cause;
          });
      });
      expect(workstreamFailureMessage(failure)).toContain(
        submitted ? "effect is unknown" : "Refresh before trying again",
      );
      expect(shared.state.calls.filter((call) => call.route === "submit")).toHaveLength(
        submitted ? 1 : 0,
      );
    },
  );

  it.each([
    { header: undefined, present: false, state: "unavailable", reason: "source unavailable" },
    { header: "malformed", present: false, state: "unavailable", reason: "source unavailable" },
    { header: "ready", present: false, state: "unavailable", reason: "source unavailable" },
    { header: undefined, present: true, state: "unknown", reason: null },
    { header: "malformed", present: true, state: "unknown", reason: null },
    { header: "ready", present: true, state: "ready", reason: null },
    ...[false, true].map((present) => ({
      header: "unavailable;reason=generation_mismatch",
      present,
      state: "unavailable",
      reason: "source generation differs",
    })),
  ])("retains primary body eligibility with optional diagnostics: %j", async (testCase) => {
    const shared = syntheticWorkstreams({
      now: () => vi.getMockedSystemTime()!.getTime(),
      assignments: [],
    });
    const environment = remoteEnvironment(shared);
    const context = {
      ...shared.registration(),
      sources: testCase.present ? [{ ...environment.state.sources[0]!, ...primarySource }] : [],
    };
    shared.state.intercept = (call) => (call.route === "registrationContext" ? context : undefined);
    await mountWith(shared);
    http.run.mockImplementation(
      routeWorkstreamsHttp(
        shared.handle,
        testCase.header === undefined
          ? {}
          : { "x-jones-workstreams-t3-availability": testCase.header },
      ),
    );
    await act(async () => latest.refresh());
    await settle(() => expect(latest.loading).toBe(false));
    expect(latest.registrationContext).toEqual(context);
    expect(latest.assignmentAvailability!(local)).toEqual({
      state: testCase.state,
      reason: testCase.reason,
    });
    expect(latest.stale).toBe(false);
    expect(primarySubmits(shared)).toEqual([]);
    expect(latest.assignmentAvailability!(remote)).toEqual({ state: "unknown", reason: null });
    expect(await assignRemote()).toBeUndefined();
    expect(latest.stale).toBe(false);
  });

  it("a remote scope denial before submission retains the primary snapshot", async () => {
    const shared = syntheticWorkstreams({
      now: () => vi.getMockedSystemTime()!.getTime(),
      assignments: [],
    });
    await mountWith(shared);
    const environment = remoteEnvironment(shared);
    environment.state.intercept = () => {
      throw new EnvironmentScopeRequiredError({
        code: "insufficient_scope",
        traceId: "test",
        requiredScope: "orchestration:operate",
      });
    };
    const snapshot = latest.data;
    expect(await assignRemote()).toMatchObject({ reason: "denied" });
    expect(latest.data).toBe(snapshot);
    expect(latest.stale).toBe(false);
    expect(environment.state.calls.filter((call) => call.route === "submit")).toEqual([]);
  });

  it("explicit Refresh rechecks relevant prepared sessions without waiting on metadata", async () => {
    const shared = syntheticWorkstreams({
      now: () => vi.getMockedSystemTime()!.getTime(),
      assignments: [],
    });
    await mountWith(shared);
    shared.state.intercept = (call) => {
      if (call.route === "list") throw new Error("metadata offline");
    };
    await act(async () => latest.refresh());
    await settle(() =>
      expect(http.sessionRefresh.mock.calls.map(([id]) => id).sort()).toEqual(["env:a", "env:b"]),
    );
  });

  it("retains a sanitized remote refusal until explicit Refresh", async () => {
    const shared = syntheticWorkstreams({
      now: () => vi.getMockedSystemTime()!.getTime(),
      assignments: [],
    });
    await mountWith(shared);
    const environment = remoteEnvironment(shared);
    const sources = environment.state.sources;
    environment.state.sources = [];
    environment.state.availabilityHeader = "unavailable;reason=source_mismatch";
    expect(latest.assignmentAvailability!(remote)).toEqual({ state: "unknown", reason: null });
    expect(await assignRemote()).toMatchObject({
      message: expect.stringContaining("this environment is not enrolled"),
    });
    expect(latest.assignmentAvailability!(remote)).toMatchObject({
      state: "unavailable",
      reason: expect.stringContaining("this environment is not enrolled"),
    });
    environment.state.sources = sources;
    environment.state.availabilityHeader = "ready";
    const readCount = environment.state.calls.length;
    expect(await assignRemote()).toBeDefined();
    expect(environment.state.calls).toHaveLength(readCount);
    await act(async () => latest.refresh());
    await settle(() => expect(latest.loading).toBe(false));
    expect(latest.assignmentAvailability!(remote)).toEqual({ state: "unknown", reason: null });
    expect(await assignRemote()).toBeUndefined();
  });

  it.each([
    [new EnvironmentHttpConflictError({ message: "workstream_command_not_current" }), "stale"],
    [
      new EnvironmentRequestInvalidError({
        code: "invalid_request",
        reason: "invalid_command",
        traceId: "test",
      }),
      "denied",
    ],
    [
      new EnvironmentScopeRequiredError({
        code: "insufficient_scope",
        traceId: "test",
        requiredScope: "orchestration:operate",
      }),
      "denied",
    ],
  ])("clears a remote known pre-effect rejection without resubmitting", async (failure, reason) => {
    const shared = syntheticWorkstreams({
      now: () => vi.getMockedSystemTime()!.getTime(),
      assignments: [],
    });
    await mountWith(shared);
    const environment = remoteEnvironment(shared);
    environment.state.intercept = (call) => {
      if (call.route === "submit") throw failure;
      return undefined;
    };
    expect(await assignRemote()).toMatchObject({ reason });
    await settle(() => expect(latest.loading).toBe(false));
    expect(latest.data).not.toBeNull();
    expect(environment.state.calls.filter((call) => call.route === "submit")).toHaveLength(1);
    environment.state.intercept = null;
    expect(await assignRemote()).toBeUndefined();
    expect(environment.state.calls.filter((call) => call.route === "command")).toHaveLength(0);
  });

  it("reconciles a remote lost response before failing metadata reads", async () => {
    const shared = syntheticWorkstreams({
      now: () => vi.getMockedSystemTime()!.getTime(),
      assignments: [],
    });
    await mountWith(shared);
    const environment = remoteEnvironment(shared);
    let accepted: WorkstreamReceipt | undefined;
    environment.state.intercept = (call) => {
      if (call.route === "submit") {
        accepted = environment.apply(call.request.payload?.command as WorkstreamCommand);
        throw new Error("lost response");
      }
      if (call.route === "command") return accepted;
      return undefined;
    };
    expect(await assignRemote()).toBeDefined();
    await settle(() => expect(latest.loading).toBe(false));
    shared.state.intercept = (call) => {
      if (call.route === "list") throw new Error("metadata offline");
      return undefined;
    };
    await act(async () => {
      await latest.retry!().catch(() => undefined);
    });
    expect(
      environment.state.calls
        .filter((call) => call.route === "command")
        .map((call) => call.request.params?.commandId),
    ).toEqual([accepted!.command_id]);
    expect(environment.state.calls.filter((call) => call.route === "submit")).toHaveLength(1);
    shared.state.intercept = null;
    await act(async () => latest.refresh());
    await settle(() => expect(latest.loading).toBe(false));
    environment.state.intercept = null;
    expect(await assignRemote()).toBeUndefined();
  });

  it("registers, verifies and attaches through the thread's own server", async () => {
    const shared = syntheticWorkstreams({
      now: () => vi.getMockedSystemTime()!.getTime(),
      assignments: [],
    });
    await mountWith(shared);
    const environment = remoteEnvironment(shared);
    const primaryCallsBefore = shared.state.calls.length;

    expect(await assignRemote()).toBeUndefined();

    expect(new Set(environment.state.calls.map((call) => call.environmentId))).toEqual(
      new Set(["env:b"]),
    );
    expect(
      environment.state.calls
        .filter((call) => call.route === "submit")
        .map(
          (call) =>
            (call.request.payload?.command as WorkstreamCommand | undefined)?.action.operation,
        ),
    ).toEqual(["register_reference", "verify_reference", "attach_primary"]);
    expect(routes(environment.state.calls)).toEqual(
      expect.arrayContaining(["list", "registrationContext", "threadPlacements", "reference"]),
    );
    // Nothing about the remote thread is read from or written to the primary server.
    expect(primarySubmits(shared)).toEqual([]);
    expect(
      shared.state.calls
        .slice(primaryCallsBefore)
        .filter((call) => call.route === "threadPlacements")
        .flatMap((call) => call.request.payload?.identities as readonly T3PlacementIdentity[])
        .some((identity) => identity.source_instance_id === remote.environmentId),
    ).toBe(false);

    // The receipt is not placement proof: the refreshed primary index shows the membership.
    await settle(() =>
      expect(latest.assignmentIndex?.entries.get(keyOf(remote))?.primary?.workstreamId).toBe(
        "beta",
      ),
    );
    expect(latest.data?.binding.registryVersion).toBe(shared.state.version);
  });

  it.each([
    ["another owner", { ownerId: "other-owner" }],
    ["another registry", { registryId: "https://other.example" }],
    ["another control-plane generation", { serverGeneration: 8 }],
  ])(
    "refuses before submitting when the environment's registry belongs to %s",
    async (_label, override) => {
      const shared = syntheticWorkstreams({
        now: () => vi.getMockedSystemTime()!.getTime(),
        assignments: [],
      });
      await mountWith(shared);
      const environment = remoteEnvironment(shared);
      environment.state.binding = { ...environment.state.binding, ...override };

      const failure = await assignRemote();

      expect(failure).toMatchObject({ refusal: "owner-mismatch", reason: "activation" });
      expect((failure as Error).message).toContain("different Workstreams owner registry");
      expect(routes(environment.state.calls)).toEqual(["list"]);
      expect(primarySubmits(shared)).toEqual([]);
    },
  );

  it.each([
    [
      "not configured",
      new EnvironmentHttpConflictError({ message: "workstreams_not_configured" }),
      "not-configured",
    ],
    [
      "unauthenticated",
      new EnvironmentHttpUnauthorizedError({ message: "session expired" }),
      "unauthenticated",
    ],
    [
      "unreachable",
      new RemoteEnvironmentAuthFetchError({ message: "fetch failed", cause: null }),
      "unavailable",
    ],
  ])("refuses before submitting when the environment is %s", async (_label, error, refusal) => {
    const shared = syntheticWorkstreams({
      now: () => vi.getMockedSystemTime()!.getTime(),
      assignments: [],
    });
    await mountWith(shared);
    const environment = remoteEnvironment(shared);
    environment.state.intercept = () => {
      throw error;
    };

    const failure = await assignRemote();

    expect(failure).toMatchObject({ refusal, reason: "activation" });
    expect(routes(environment.state.calls)).toEqual(["list"]);
    expect(primarySubmits(shared)).toEqual([]);
    // A remote environment's failure says nothing about primary access.
    expect(latest.data).not.toBeNull();
    expect(latest.stale).toBe(false);
  });

  it("refuses before submitting when the environment cannot register its own thread", async () => {
    const shared = syntheticWorkstreams({
      now: () => vi.getMockedSystemTime()!.getTime(),
      assignments: [],
    });
    await mountWith(shared);
    const environment = remoteEnvironment(shared);
    environment.state.sources = [];

    const failure = await assignRemote();

    expect(failure).toMatchObject({ refusal: "source-unavailable" });
    expect(routes(environment.state.calls)).toEqual(["list", "registrationContext"]);
    expect(primarySubmits(shared)).toEqual([]);
  });

  it("refuses before submitting when the environment's access is read-only", async () => {
    const shared = syntheticWorkstreams({
      now: () => vi.getMockedSystemTime()!.getTime(),
      assignments: [],
    });
    await mountWith(shared);
    const environment = remoteEnvironment(shared);
    environment.state.binding = { ...environment.state.binding, permissions: ["workstreams:read"] };

    const failure = await assignRemote();

    expect(failure).toMatchObject({ refusal: "read-only" });
    expect(routes(environment.state.calls)).toEqual(["list"]);
  });

  it("refuses without contacting the environment while the primary snapshot is stale", async () => {
    const shared = syntheticWorkstreams({
      now: () => vi.getMockedSystemTime()!.getTime(),
      assignments: [],
    });
    await mountWith(shared);
    const environment = remoteEnvironment(shared);
    shared.state.intercept = (call) => {
      if (call.route === "list") throw new Error("Connection interrupted");
      return undefined;
    };
    await act(async () => latest.refresh());
    await settle(() => expect(latest.stale).toBe(true));

    expect(await assignRemote()).toMatchObject({ reason: "stale" });
    expect(environment.state.calls).toEqual([]);
  });

  it("refuses without contacting the environment when stored primaries conflict", async () => {
    const shared = syntheticWorkstreams({
      now: () => vi.getMockedSystemTime()!.getTime(),
      assignments: [
        { environmentId: "env:b", threadId: remote.id, workstreamId: "alpha" },
        { environmentId: "env:b", threadId: remote.id, workstreamId: "beta" },
      ],
    });
    await mountWith(shared);
    const environment = remoteEnvironment(shared);

    expect(await assignRemote()).toMatchObject({ reason: "ambiguous" });
    expect(environment.state.calls).toEqual([]);
  });

  it("reconciles a lost submit response only through the environment that accepted it", async () => {
    const shared = syntheticWorkstreams({
      now: () => vi.getMockedSystemTime()!.getTime(),
      assignments: [],
    });
    await mountWith(shared);
    const environment = remoteEnvironment(shared);
    let lost: WorkstreamCommand | undefined;
    environment.state.intercept = (call) => {
      if (call.route === "submit" && lost === undefined) {
        lost = call.request.payload?.command as WorkstreamCommand;
        environment.apply(lost);
        throw new RemoteEnvironmentAuthFetchError({ message: "response lost", cause: null });
      }
      return undefined;
    };

    const rendersBeforeLoss = staleRenders.length;
    expect(await assignRemote()).toBeDefined();
    expect(lost?.action.operation).toBe("register_reference");
    // The effect may have reached the shared registry: the primary reloads, staying read-only
    // until that reload succeeds.
    expect(staleRenders.slice(rendersBeforeLoss)).toContain(true);
    await settle(() => {
      expect(latest.loading).toBe(false);
      expect(latest.stale).toBe(false);
    });
    const callsAfterLoss = environment.state.calls.length;

    // The effect is unknown, so another assignment waits for reconciliation instead of guessing.
    expect(await assignRemote()).toMatchObject({ reason: "unknown" });
    expect(environment.state.calls.length).toBe(callsAfterLoss);

    const versionBeforeRetry = shared.state.version;
    environment.state.intercept = (call) => {
      if (call.route === "command") {
        expect(call.request.params?.commandId).toBe(lost!.command_id);
        return {
          command_id: lost!.command_id,
          owner_id: "owner",
          actor: { principal_id: "principal-b" },
          operation: lost!.action.operation,
          request_sha256: "c".repeat(64),
          server_generation: 7,
          accepted_at: vi.getMockedSystemTime()!.toISOString(),
          completed_at: vi.getMockedSystemTime()!.toISOString(),
          state: "committed",
          registry_version: versionBeforeRetry,
          changed: true,
          effects: { native_reference_id: "reference-b" },
        } as unknown as WorkstreamReceipt;
      }
      return undefined;
    };
    await act(async () => latest.retry!());

    expect(
      environment.state.calls.slice(callsAfterLoss).map((call) => [call.environmentId, call.route]),
    ).toEqual([["env:b", "command"]]);
    expect(
      environment.state.calls.filter((call) => call.route === "submit").map((call) => call.route),
    ).toEqual(["submit"]);
    expect(primarySubmits(shared)).toEqual([]);

    // Reconciled, the next attempt proceeds on the same environment.
    environment.state.intercept = null;
    expect(await assignRemote()).toBeUndefined();
    expect(primarySubmits(shared)).toEqual([]);
  });

  it("holds primary writes until an unresolved remote command is reconciled", async () => {
    const shared = syntheticWorkstreams({
      now: () => vi.getMockedSystemTime()!.getTime(),
      assignments: [],
    });
    await mountWith(shared);
    const environment = remoteEnvironment(shared);
    let lost: WorkstreamCommand | undefined;
    environment.state.intercept = (call) => {
      if (call.route === "submit" && lost === undefined) {
        lost = call.request.payload?.command as WorkstreamCommand;
        environment.apply(lost);
        throw new RemoteEnvironmentAuthFetchError({ message: "response lost", cause: null });
      }
      return undefined;
    };
    expect(await assignRemote()).toBeDefined();
    // An ordinary primary reload succeeds, but it does not resolve the remote command.
    await settle(() => {
      expect(latest.loading).toBe(false);
      expect(latest.stale).toBe(false);
      expect(latest.data?.binding.registryVersion).toBe(shared.state.version);
    });

    const primaryPosts = () => shared.state.calls.filter((call) => call.route === "submit").length;
    const update = (commandId: string): WorkstreamCommand => ({
      command_id: commandId,
      expected_server_generation: data.binding.serverGeneration,
      expected_registry_version: latest.data!.binding.registryVersion,
      action: {
        operation: "update_workstream",
        workstream_id: "alpha",
        expected_version: 3,
        name: "renamed",
        lifecycle: "active",
        progress: { state: "unknown" },
        sort_order: 0,
      },
    });
    // The F-REV-1 path: the primary can register its own thread, so only the remote-command
    // hold stops this assignment from submitting before Retry.
    shared.state.intercept = (call) =>
      call.route === "registrationContext"
        ? {
            ...shared.registration(),
            sources: [{ ...environment.state.sources[0]!, ...primarySource }],
          }
        : undefined;
    let assignmentFailure: unknown;
    await act(async () => {
      await latest.assignNativeThread!(local, "beta", {
        commandId: async () => "primary-assign-1",
      }).catch((cause: unknown) => {
        assignmentFailure = cause;
      });
    });
    // Restore the primary's ordinary context for the plain update.
    shared.state.intercept = null;
    let updateFailure: unknown;
    await act(async () => {
      await latest.submit(update("primary-update-1")).catch((cause: unknown) => {
        updateFailure = cause;
      });
    });
    expect(
      shared.state.calls
        .filter((call) => call.route === "submit")
        .map(
          (call) =>
            (call.request.payload?.command as WorkstreamCommand | undefined)?.action.operation,
        ),
    ).toEqual([]);
    expect(updateFailure).toMatchObject({ reason: "unknown" });
    expect(assignmentFailure).toMatchObject({ reason: "unknown" });

    const remoteCallsBeforeRetry = environment.state.calls.length;
    environment.state.intercept = (call) =>
      call.route === "command"
        ? ({
            command_id: call.request.params?.commandId,
            owner_id: "owner",
            actor: { principal_id: "principal-b" },
            operation: lost!.action.operation,
            request_sha256: "c".repeat(64),
            server_generation: 7,
            accepted_at: vi.getMockedSystemTime()!.toISOString(),
            completed_at: vi.getMockedSystemTime()!.toISOString(),
            state: "committed",
            registry_version: shared.state.version,
            changed: true,
            effects: { native_reference_id: "reference-b" },
          } as unknown as WorkstreamReceipt)
        : undefined;
    await act(async () => latest.retry!());
    expect(
      environment.state.calls
        .slice(remoteCallsBeforeRetry)
        .map((call) => [call.environmentId, call.route, call.request.params?.commandId]),
    ).toEqual([["env:b", "command", lost!.command_id]]);
    await settle(() => expect(latest.loading).toBe(false));

    shared.state.intercept = (call) => {
      if (call.route !== "submit") return undefined;
      const command = call.request.payload?.command as WorkstreamCommand;
      shared.state.version += 1;
      return {
        command_id: command.command_id,
        owner_id: "owner",
        actor: { principal_id: "principal" },
        operation: command.action.operation,
        request_sha256: "c".repeat(64),
        server_generation: 7,
        accepted_at: vi.getMockedSystemTime()!.toISOString(),
        completed_at: vi.getMockedSystemTime()!.toISOString(),
        state: "committed",
        registry_version: shared.state.version,
        changed: true,
        effects: {},
      } as unknown as WorkstreamReceipt;
    };
    let receipt: WorkstreamReceipt | undefined;
    await act(async () => {
      receipt = await latest.submit(update("primary-update-2"));
    });
    expect(receipt?.state).toBe("committed");
    expect(primaryPosts()).toBe(1);
    // The original remote POST was sent once and never again.
    expect(environment.state.calls.filter((call) => call.route === "submit")).toHaveLength(1);
  });

  it("keeps the primary read-only when the caller cancels after a remote submit", async () => {
    const shared = syntheticWorkstreams({
      now: () => vi.getMockedSystemTime()!.getTime(),
      assignments: [],
    });
    await mountWith(shared);
    const environment = remoteEnvironment(shared);
    const controller = new AbortController();
    let sent: WorkstreamCommand | undefined;
    environment.state.intercept = (call) => {
      if (call.route === "submit" && sent === undefined) {
        sent = call.request.payload?.command as WorkstreamCommand;
        environment.apply(sent);
        controller.abort(new DOMException("Cancelled by the caller.", "AbortError"));
        throw controller.signal.reason;
      }
      return undefined;
    };

    const rendersBeforeCancel = staleRenders.length;
    let cancelled: unknown;
    await act(async () => {
      await latest.assignNativeThread!(remote, "beta", {
        commandId: async () => "remote-cancelled-1",
        signal: controller.signal,
      }).catch((cause: unknown) => {
        cancelled = cause;
      });
    });
    expect(cancelled).toBeDefined();
    expect(sent?.action.operation).toBe("register_reference");
    // Cancellation does not undo a sent command: the primary is read-only until it reloads.
    expect(staleRenders.slice(rendersBeforeCancel)).toContain(true);
    await settle(() => expect(latest.loading).toBe(false));

    // The ledger entry survives the cancellation, so writes wait for reconciliation.
    const callsAfterCancel = environment.state.calls.length;
    expect(await assignRemote()).toMatchObject({ reason: "unknown" });
    let updateFailure: unknown;
    await act(async () => {
      await latest
        .submit({
          command_id: "primary-update-1",
          expected_server_generation: data.binding.serverGeneration,
          expected_registry_version: latest.data!.binding.registryVersion,
          action: {
            operation: "update_workstream",
            workstream_id: "alpha",
            expected_version: 3,
            name: "renamed",
            lifecycle: "active",
            progress: { state: "unknown" },
            sort_order: 0,
          },
        })
        .catch((cause: unknown) => {
          updateFailure = cause;
        });
    });
    expect(updateFailure).toMatchObject({ reason: "unknown" });
    expect(environment.state.calls.length).toBe(callsAfterCancel);
    expect(primarySubmits(shared)).toEqual([]);

    environment.state.intercept = (call) =>
      call.route === "command"
        ? ({
            command_id: call.request.params?.commandId,
            owner_id: "owner",
            actor: { principal_id: "principal-b" },
            operation: sent!.action.operation,
            request_sha256: "c".repeat(64),
            server_generation: 7,
            accepted_at: vi.getMockedSystemTime()!.toISOString(),
            completed_at: vi.getMockedSystemTime()!.toISOString(),
            state: "committed",
            registry_version: shared.state.version,
            changed: true,
            effects: { native_reference_id: "reference-b" },
          } as unknown as WorkstreamReceipt)
        : undefined;
    await act(async () => latest.retry!());
    expect(
      environment.state.calls
        .slice(callsAfterCancel)
        .map((call) => [call.environmentId, call.route, call.request.params?.commandId]),
    ).toEqual([["env:b", "command", sent!.command_id]]);
    // The cancelled POST was sent once and never again.
    expect(environment.state.calls.filter((call) => call.route === "submit")).toHaveLength(1);
  });

  describe("when operations overlap before their submits", () => {
    const deferred = <A,>() => {
      let resolve!: (value: A) => void;
      const promise = new Promise<A>((done) => {
        resolve = done;
      });
      return { promise, resolve };
    };
    const committed = (command: WorkstreamCommand, registryVersion: number): WorkstreamReceipt =>
      ({
        command_id: command.command_id,
        owner_id: "owner",
        actor: { principal_id: "principal" },
        operation: command.action.operation,
        request_sha256: "c".repeat(64),
        server_generation: 7,
        accepted_at: vi.getMockedSystemTime()!.toISOString(),
        completed_at: vi.getMockedSystemTime()!.toISOString(),
        state: "committed",
        registry_version: registryVersion,
        changed: true,
        effects: { native_reference_id: "reference-00000" },
      }) as unknown as WorkstreamReceipt;
    const remoteSubmits = (environment: ReturnType<typeof remoteEnvironment>) =>
      environment.state.calls.filter((call) => call.route === "submit");
    /** Starts an assignment that waits at its first command ID until the gate opens. */
    const startGated = (
      thread: { readonly environmentId: string; readonly id: string },
      prefix: string,
    ) => {
      const gate = deferred<void>();
      const outcome: { reached: boolean; failure?: unknown; done: boolean } = {
        reached: false,
        done: false,
      };
      let nextId = 0;
      void latest.assignNativeThread!(thread, "beta", {
        commandId: async () => {
          outcome.reached = true;
          await gate.promise;
          return `${prefix}-${++nextId}`;
        },
      })
        .catch((cause: unknown) => {
          outcome.failure = cause;
        })
        .finally(() => {
          outcome.done = true;
        });
      return { gate, outcome };
    };
    const primaryCanRegister = (
      shared: ReturnType<typeof syntheticWorkstreams>,
      environment: ReturnType<typeof remoteEnvironment>,
    ) =>
      ({
        ...shared.registration(),
        sources: [{ ...environment.state.sources[0]!, ...primarySource }],
      }) as unknown;

    it("refuses a gated primary submit while a remote command is unknown", async () => {
      const shared = syntheticWorkstreams({
        now: () => vi.getMockedSystemTime()!.getTime(),
        assignments: [],
      });
      await mountWith(shared);
      const environment = remoteEnvironment(shared);
      shared.state.intercept = (call) =>
        call.route === "registrationContext" ? primaryCanRegister(shared, environment) : undefined;
      const primary = startGated(local, "primary");
      await settle(() => expect(primary.outcome.reached).toBe(true));

      // The remote command is sent and its response held, so its effect is unknown.
      const held = deferred<WorkstreamReceipt>();
      let sent: WorkstreamCommand | undefined;
      environment.state.intercept = (call) => {
        if (call.route !== "submit" || sent !== undefined) return undefined;
        sent = call.request.payload!.command as WorkstreamCommand;
        return held.promise;
      };
      const remoteAssignment = startGated(remote, "remote");
      remoteAssignment.gate.resolve();
      await settle(() => expect(sent).toBeDefined());

      primary.gate.resolve();
      await settle(() => expect(primary.outcome.done).toBe(true));
      expect(primarySubmits(shared)).toEqual([]);
      expect(primary.outcome.failure).toMatchObject({ reason: "unknown" });

      held.resolve(environment.apply(sent!) as WorkstreamReceipt);
      await settle(() => expect(remoteAssignment.outcome.done).toBe(true));
      expect(remoteSubmits(environment)).toHaveLength(1);
      expect(primarySubmits(shared)).toEqual([]);
    });

    it("refuses a gated remote submit while a primary command is unknown", async () => {
      const shared = syntheticWorkstreams({
        now: () => vi.getMockedSystemTime()!.getTime(),
        assignments: [],
      });
      await mountWith(shared);
      const environment = remoteEnvironment(shared);
      const remoteAssignment = startGated(remote, "remote");
      await settle(() => expect(remoteAssignment.outcome.reached).toBe(true));

      // The primary command is sent and its response held, so its effect is unknown.
      const held = deferred<WorkstreamReceipt>();
      let sent: WorkstreamCommand | undefined;
      shared.state.intercept = (call) => {
        if (call.route === "registrationContext") return primaryCanRegister(shared, environment);
        if (call.route !== "submit" || sent !== undefined) return undefined;
        sent = call.request.payload!.command as WorkstreamCommand;
        return held.promise;
      };
      const primary = startGated(local, "primary");
      primary.gate.resolve();
      await settle(() => expect(sent).toBeDefined());

      remoteAssignment.gate.resolve();
      await settle(() => expect(remoteAssignment.outcome.done).toBe(true));
      expect(remoteSubmits(environment)).toEqual([]);
      expect(remoteAssignment.outcome.failure).toMatchObject({ reason: "unknown" });

      shared.state.version += 1;
      held.resolve(committed(sent!, shared.state.version));
      await settle(() => expect(primary.outcome.done).toBe(true));
      expect(primarySubmits(shared)).toHaveLength(1);
      expect(remoteSubmits(environment)).toEqual([]);
    });

    it("refuses a second gated remote submit while the first remote command is unknown", async () => {
      const shared = syntheticWorkstreams({
        now: () => vi.getMockedSystemTime()!.getTime(),
        assignments: [],
      });
      await mountWith(shared);
      const environment = remoteEnvironment(shared);
      const held = deferred<WorkstreamReceipt>();
      let sent: WorkstreamCommand | undefined;
      environment.state.intercept = (call) => {
        if (call.route !== "submit" || sent !== undefined) return undefined;
        sent = call.request.payload!.command as WorkstreamCommand;
        return held.promise;
      };
      const first = startGated(remote, "remote-a");
      const second = startGated(remote, "remote-b");
      await settle(() => {
        expect(first.outcome.reached).toBe(true);
        expect(second.outcome.reached).toBe(true);
      });

      first.gate.resolve();
      await settle(() => expect(sent).toBeDefined());
      second.gate.resolve();
      await settle(() => expect(second.outcome.done).toBe(true));
      expect(second.outcome.failure).toMatchObject({ reason: "unknown" });
      expect(remoteSubmits(environment)).toHaveLength(1);

      held.resolve(environment.apply(sent!) as WorkstreamReceipt);
      await settle(() => expect(first.outcome.done).toBe(true));
      // The first operation continues with its own commands; the second never sent one.
      expect(sent?.command_id).toBe("remote-a-1");
      expect(
        remoteSubmits(environment).map(
          (call) => (call.request.payload!.command as WorkstreamCommand).command_id,
        ),
      ).not.toContain("remote-b-1");
      expect(primarySubmits(shared)).toEqual([]);
    });
  });

  const expiredAssignment = (environmentId: string, threadId: string): SyntheticAssignment => ({
    environmentId,
    threadId,
    workstreamId: "alpha",
    attestedAt: "2026-09-30T11:50:00Z",
    expiresAt: "2026-09-30T11:55:00Z",
  });
  const recordedReference = (
    environmentId: string,
    threadId: string,
    evidence: { readonly authority_namespace: string; readonly store_generation: number },
    attestation = { attested_at: "2026-09-30T11:50:00Z", expires_at: "2026-09-30T11:55:00Z" },
  ): NativeReference => ({
    native_reference_id: "reference-00000",
    owner_id: "owner",
    identity: {
      provider: "t3",
      source_instance_id: environmentId,
      native_id: threadId,
      resource_kind: "thread",
      id_kind: "internal",
      account_provenance: { kind: "not_account_scoped" },
    },
    pr_locator: null,
    registration: {
      state: "attested",
      attestation_version: 2,
      ...attestation,
      evidence: {
        provider: "t3",
        source_instance_id: environmentId,
        native_id: threadId,
        ...evidence,
        evidence_sha256: "a".repeat(64),
      },
    },
    created_at: "2026-09-30T11:00:00Z",
    created_by: { principal_id: "owner" },
    created_registry_version: 1,
  });
  const submitted = (calls: readonly WorkstreamsHttpCall[]) =>
    calls
      .filter((call) => call.route === "submit")
      .map((call) => {
        const action = (call.request.payload!.command as WorkstreamCommand).action;
        return [
          action.operation,
          "membership_id" in action
            ? action.membership_id
            : "native_reference_id" in action
              ? action.native_reference_id
              : null,
        ];
      });

  it("removes another environment's expired-proof primary through its recorded reference", async () => {
    const shared = syntheticWorkstreams({
      now: () => vi.getMockedSystemTime()!.getTime(),
      assignments: [expiredAssignment(remote.environmentId, remote.id)],
    });
    await mountWith(shared);
    expect(latest.assignmentIndex?.entries.get(keyOf(remote))?.primary?.membershipId).toBe(
      "membership-00000",
    );
    const environment = remoteEnvironment(shared);
    environment.state.reference = recordedReference(remote.environmentId, remote.id, {
      authority_namespace: "authority-b",
      store_generation: 3,
    });

    expect(await assignRemote(null)).toBeUndefined();

    // Fresh proof first, then exactly one removal of the recorded membership.
    expect(submitted(environment.state.calls)).toEqual([
      ["verify_reference", "reference-00000"],
      ["remove_membership", "membership-00000"],
    ]);
    expect(primarySubmits(shared)).toEqual([]);
    await settle(() => expect(latest.assignmentIndex?.entries.get(keyOf(remote))).toBeUndefined());
  });

  it("removes a primary-environment expired-proof primary through its recorded reference", async () => {
    const shared = syntheticWorkstreams({
      now: () => vi.getMockedSystemTime()!.getTime(),
      assignments: [expiredAssignment(local.environmentId, local.id)],
    });
    await mountWith(shared);
    const environment = remoteEnvironment(shared);
    const evidence = { authority_namespace: "authority", store_generation: 1 };
    let reference = recordedReference(local.environmentId, local.id, evidence);
    shared.state.intercept = (call) => {
      switch (call.route) {
        case "registrationContext":
          return {
            ...shared.registration(),
            sources: [{ ...environment.state.sources[0]!, ...primarySource }],
          };
        case "reference":
          return {
            context: {
              owner_id: "owner",
              server_generation: 7,
              registry_version: shared.state.version,
            },
            reference,
            latest_observation: null,
          };
        case "submit": {
          const command = call.request.payload?.command as WorkstreamCommand;
          shared.state.version += 1;
          if (command.action.operation === "verify_reference") {
            const attestation = {
              attested_at: "2026-09-30T11:59:00Z",
              expires_at: "2026-09-30T12:04:00Z",
            };
            shared.state.assignments[0] = {
              ...shared.state.assignments[0]!,
              attestedAt: attestation.attested_at,
              expiresAt: attestation.expires_at,
            };
            reference = recordedReference(local.environmentId, local.id, evidence, attestation);
          } else if (command.action.operation === "remove_membership")
            shared.state.assignments.splice(0, 1);
          return {
            command_id: command.command_id,
            owner_id: "owner",
            actor: { principal_id: "principal" },
            operation: command.action.operation,
            request_sha256: "c".repeat(64),
            server_generation: 7,
            accepted_at: vi.getMockedSystemTime()!.toISOString(),
            completed_at: vi.getMockedSystemTime()!.toISOString(),
            state: "committed",
            registry_version: shared.state.version,
            changed: true,
            effects: { native_reference_id: "reference-00000" },
          } as unknown as WorkstreamReceipt;
        }
        default:
          return undefined;
      }
    };

    let nextId = 0;
    let failure: unknown;
    await act(async () => {
      await latest.assignNativeThread!(local, null, {
        commandId: async () => `local-command-${++nextId}`,
      }).catch((cause: unknown) => {
        failure = cause;
      });
    });

    expect(failure).toBeUndefined();
    expect(submitted(shared.state.calls)).toEqual([
      ["verify_reference", "reference-00000"],
      ["remove_membership", "membership-00000"],
    ]);
    expect(http.environment).not.toHaveBeenCalled();
  });

  it("keeps primary-environment assignment on the primary server", async () => {
    const shared = syntheticWorkstreams({
      now: () => vi.getMockedSystemTime()!.getTime(),
      assignments: [],
    });
    await mountWith(shared);
    remoteEnvironment(shared);

    let failure: unknown;
    await act(async () => {
      await latest.assignNativeThread!(local, "beta", {
        commandId: async () => "local-command",
      }).catch((cause: unknown) => {
        failure = cause;
      });
    });

    // The synthetic primary names no T3 source, so the primary path refuses as before.
    expect(failure).toMatchObject({ reason: "activation" });
    expect(http.environment).not.toHaveBeenCalled();
  });
});
