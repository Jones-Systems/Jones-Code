// @effect-diagnostics globalDate:off - Fixed timestamps and injected clocks exercise receipt, lease and review behavior.
import { describe, expect, it, vi } from "vite-plus/test";
import { it as effectIt } from "@effect/vitest";
import * as Cause from "effect/Cause";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as SubscriptionRef from "effect/SubscriptionRef";
import { Atom, AtomRegistry } from "effect/reactivity";
import { EnvironmentId } from "@t3tools/contracts";
import type {
  JonesInstallPlan,
  JonesInstallPlanGrant,
  JonesInstallPlanHostOperation,
  JonesInstallPlanRequest,
  JonesInstallPlanResult,
  JonesInstallPlanState,
} from "@t3tools/contracts/jones/installPlan";
import type {
  JonesOperationReceipt,
  JonesOperationStartInput,
} from "@t3tools/contracts/jones/jonesOperations";
import type { FleetHostStatus } from "@t3tools/contracts/jones/fleet-updates";
import {
  AVAILABLE_CONNECTION_STATE,
  PrimaryConnectionTarget,
  type PreparedConnection,
} from "../../connection/model.ts";
import { EnvironmentRegistry } from "../../connection/registry.ts";
import { EnvironmentSupervisor } from "../../connection/supervisor.ts";
import { layerRemoteHttpClient } from "../../rpc/http.ts";
import type * as RpcSession from "../../rpc/session.ts";
import { runAtomCommand } from "../../state/runtime.ts";
import { createJonesUpdateAtoms, JonesOperationNotSentError } from "../updates/jonesUpdates.ts";
import { matchPlanEnrollment, startPlan } from "./model.ts";
import { advanceInstallPlan, matchesForwardRoute, type InstallPlanDriver } from "./runner.ts";

const env = EnvironmentId.make("host");
const otherEnv = EnvironmentId.make("other-host");
const laptop = EnvironmentId.make("laptop");
const planId = "11111111-1111-4111-8111-111111111111";
const opId = "22222222-2222-4222-8222-222222222222";
const enrollmentId = "33333333-3333-4333-8333-333333333333";
const previousDirectiveId = "44444444-4444-4444-8444-444444444444";
const otherPlanId = "55555555-5555-4555-8555-555555555555";
const otherOpId = "66666666-6666-4666-8666-666666666666";
const source = "a".repeat(40),
  tree = "b".repeat(40),
  installed = "c".repeat(40);
const now = "2026-10-27T05:00:00.000Z";
const leaseUntil = "2026-10-27T05:02:00.000Z";
const request: JonesOperationStartInput = {
  operationId: opId,
  environmentId: env,
  expectedInstalledSource: installed,
  action: { kind: "download-exact", targetSource: source },
  correlation: { planId, deviceKey: env },
};
function fixture(recoveryHistory?: 1, boundAdmission?: 1) {
  let plan = startPlan({
    planId,
    origin: "update-button",
    target: {
      repository: "Jones-Systems/Jones-Code",
      sourceSha: source,
      sourceTree: tree,
      version: "new",
    },
    from: { version: "old" },
    laptopEnvironmentId: laptop,
    select: [{ environmentId: env, label: "Host", route: "operations" }],
    now,
  });
  plan = {
    ...plan,
    laptop: {
      ...plan.laptop,
      gate: {
        openedAt: now,
        basis: "plan-install",
        transactionId: "tx",
        activeGeneration: "gen",
        activeSource: source,
        activeTree: tree,
      },
    },
  };
  const state = (): JonesInstallPlanState => ({
    schema: 1,
    current: plan,
    last: null,
    laptop: { observedAt: now, activationBlocked: false },
    view: {
      complete: false,
      ...(recoveryHistory === undefined ? {} : { recoveryHistory }),
      ...(boundAdmission === undefined ? {} : { boundAdmission }),
    },
  });
  const grant: JonesInstallPlanGrant = {
    planId,
    environmentId: env,
    operationId: opId,
    step: "download",
    request: { kind: "operation-start", input: request },
    leaseUntil,
  };
  const receipt: JonesOperationReceipt = {
    ...request,
    protocol: 1,
    phase: "accepted",
    acceptedAt: now,
    updatedAt: now,
  };
  const desktop = vi.fn<InstallPlanDriver["desktop"]>(
    async (r): Promise<JonesInstallPlanResult> => ({
      state: state(),
      ...(r.type === "claim" ? { grant } : {}),
    }),
  );
  const observe = vi.fn<InstallPlanDriver["observe"]>(async () => ({
    canOperate: true,
    fleetStatus: null,
    observation: {
      freshness: "fresh",
      state: {
        source: "jones-actions",
        channel: "jones-main",
        environmentId: env,
        phase: "no-new",
        currentVersion: "old",
        installedSource: { repository: "Jones-Systems/Jones-Code", sha: installed, tree },
        capability: {
          check: true,
          download: true,
          install: true,
          exactSource: true,
          operations: 1,
        },
      },
    },
  }));
  const operation = vi.fn<InstallPlanDriver["operation"]>(async (_env, r) =>
    r.action === "operationRead" ? { receipts: [{ operationId: opId, receipt }] } : receipt,
  );
  const host = vi.fn<InstallPlanDriver["host"]>(async () => {
    throw new Error("unused");
  });
  let observed: Awaited<ReturnType<InstallPlanDriver["observe"]>>;
  const canDispatch = vi.fn(() => true);
  const canReadReceipt = vi.fn(() => true);
  const authority = vi.fn<InstallPlanDriver["authority"]>((device) => ({
    canDispatch: canDispatch(),
    canReadReceipt: canReadReceipt(),
    enrollment: matchPlanEnrollment(device, observed.observation.state, observed.fleetStatus),
    fleetStatus: observed.fleetStatus,
    ...(observed.observation.state?.capability.boundAdmission === 1 ? { boundAdmission: 1 } : {}),
  }));
  return {
    driver: {
      claimant: "window:a",
      now: () => Date.parse(now),
      cancelled: () => false,
      desktop,
      observe: async (id: EnvironmentId) => (observed = await observe(id)),
      authority,
      operation,
      host,
    },
    plan: () => plan,
    state,
    setPlan: (p: JonesInstallPlan) => {
      plan = p;
    },
    grant,
    receipt,
    desktop,
    operation,
    observe,
    canDispatch,
    canReadReceipt,
    authority,
    host,
  };
}

function enrolled(): FleetHostStatus & { enrollment: NonNullable<FleetHostStatus["enrollment"]> } {
  return {
    environmentId: env,
    operationProtocol: 1,
    planDirectiveProtocol: 1,
    enrollment: { environmentId: env, enrollmentId, enabled: true, continueRunningThreads: false },
    operation: null,
    update: null,
  };
}

function reservation(claimant = "window:a"): JonesInstallPlanHostOperation {
  return {
    operationId: opId,
    step: "download",
    request: { kind: "operation-start", input: request },
    lease: { claimant, reservedAt: now, leaseUntil },
  };
}

function withReservation(plan: JonesInstallPlan, operation = reservation()): JonesInstallPlan {
  return { ...plan, devices: [{ ...plan.devices[0]!, operations: [operation] }] };
}

function nextPlan(plan: JonesInstallPlan): JonesInstallPlan {
  return {
    ...plan,
    planId: otherPlanId,
    revision: 0,
    target: { ...plan.target, version: "newer", sourceSha: "e".repeat(40) },
    laptop: { environmentId: plan.laptop.environmentId, operations: [] },
    devices: [{ ...plan.devices[0]!, environmentId: otherEnv, label: "Other", operations: [] }],
  };
}

const readInput = { environmentId: env, operationIds: [opId] };
const emptyRead = { receipts: [{ operationId: opId, receipt: null }] };

const operationTransport = (fetchFn: typeof fetch, failures: unknown[] = []) =>
  Effect.gen(function* () {
    const target = new PrimaryConnectionTarget({
      environmentId: env,
      label: "Synthetic primary",
      httpBaseUrl: "http://updates.example.test",
      wsBaseUrl: "ws://updates.example.test/ws",
    });
    const supervisor = EnvironmentSupervisor.of({
      target,
      state: yield* SubscriptionRef.make(AVAILABLE_CONNECTION_STATE),
      session: yield* SubscriptionRef.make(Option.none<RpcSession.RpcSession>()),
      prepared: yield* SubscriptionRef.make<Option.Option<PreparedConnection>>(
        Option.some({
          environmentId: env,
          label: target.label,
          target,
          httpBaseUrl: target.httpBaseUrl,
          socketUrl: target.wsBaseUrl,
          httpAuthorization: null,
        }),
      ),
      connect: Effect.void,
      disconnect: Effect.void,
      retryNow: Effect.void,
    });
    const run: EnvironmentRegistry["Service"]["run"] = (_id, effect) =>
      Effect.provideService(effect, EnvironmentSupervisor, supervisor);
    const environmentRegistry = EnvironmentRegistry.of({ run } as EnvironmentRegistry["Service"]);
    const registry = AtomRegistry.make();
    yield* Effect.addFinalizer(() => Effect.sync(() => registry.dispose()));
    const atoms = createJonesUpdateAtoms(
      Atom.runtime(
        Layer.mergeAll(
          Layer.succeed(EnvironmentRegistry, environmentRegistry),
          layerRemoteHttpClient(fetchFn),
        ),
      ),
    );
    const operation: InstallPlanDriver["operation"] = async (environmentId, input) => {
      const result = await runAtomCommand(
        registry,
        atoms.operation,
        { environmentId, input },
        { reportFailure: false, reportDefect: false },
      );
      if (result._tag === "Success") {
        if ("operationId" in result.value || "receipts" in result.value) return result.value;
        throw new Error("Unexpected operation result in the test transport.");
      }
      const error = Option.getOrElse(Cause.findErrorOption(result.cause), () => result.cause);
      failures.push(error);
      throw error;
    };
    return operation;
  });

describe("bounded install plan runner", () => {
  it("blocks an unbound forward request when live status contradicts the cached directive", () => {
    const f = fixture();
    const plan = f.plan();
    const directive = {
      directiveId: previousDirectiveId,
      targetSource: source,
      targetTree: tree,
      mode: "manual-review",
      issuedAt: now,
    } as const;
    const device = {
      ...plan.devices[0]!,
      route: "follower-operations",
      enrollmentId,
      operations: [
        {
          operationId: directive.directiveId,
          step: "directive",
          lease: reservation().lease,
          request: {
            kind: "plan-directive",
            input: {
              directiveId: directive.directiveId,
              environmentId: env,
              enrollmentId,
              repository: plan.target.repository,
              targetSource: source,
              targetTree: tree,
              expectedDirectiveId: null,
            },
          },
          directive: { kind: "applied", observedAt: now, directive },
        },
      ],
    } as const;
    const fleet = {
      ...enrolled(),
      follower: {
        protocol: 1,
        leaderSource: source,
        state: "waiting-for-leader",
        planDirective: directive,
      },
    } as const;
    const live = { source, tree, directiveId: directive.directiveId };
    expect(matchesForwardRoute(plan, device, request, fleet)).toBe(true);
    expect(matchesForwardRoute(plan, device, request, fleet, live)).toBe(true);
    expect(
      matchesForwardRoute(plan, device, request, fleet, { ...live, directiveId: otherOpId }),
    ).toBe(false);
  });

  effectIt.effect.each([
    "cancelled",
    "closed",
    "lease expired",
    "reservation changed",
    "route changed",
    "identity changed",
    "route changed during the native read",
  ] as const)("vetoes a V1 start queued behind a read when %s", (change) =>
    Effect.gen(function* () {
      const f = fixture(1);
      const entered = Promise.withResolvers<void>();
      const unblock = Promise.withResolvers<void>();
      const queued = Promise.withResolvers<void>();
      const nativeRead = Promise.withResolvers<void>();
      const resumeNative = Promise.withResolvers<void>();
      const failures: unknown[] = [];
      const calls: string[] = [];
      const command = yield* operationTransport(async (url) => {
        const path = new URL(String(url)).pathname;
        calls.push(path);
        if (path.endsWith("/read")) {
          entered.resolve();
          await unblock.promise;
          return Response.json(emptyRead);
        }
        return Response.json(f.receipt);
      }, failures);
      const inFlight: Promise<unknown>[] = [];
      yield* Effect.addFinalizer(() =>
        Effect.promise(async () => {
          unblock.resolve();
          resumeNative.resolve();
          await Promise.allSettled(inFlight);
        }),
      );
      const blockingRead = command(env, { action: "operationRead", input: readInput });
      inFlight.push(blockingRead);
      expect(
        yield* Effect.promise(() =>
          Promise.race([entered.promise.then(() => true), blockingRead.then(() => false)]),
        ),
      ).toBe(true);
      let checkInProgress = false;
      f.desktop.mockImplementation(async (query) => {
        if (query.type === "claim") {
          f.setPlan(withReservation(f.plan()));
          return { state: f.state(), grant: f.grant };
        }
        if (
          query.type === "read" &&
          checkInProgress &&
          change === "route changed during the native read"
        ) {
          const proof = f.state();
          nativeRead.resolve();
          await resumeNative.promise;
          return { state: proof };
        }
        return { state: f.state() };
      });
      f.operation.mockImplementation((id, input) => {
        expect(input).toMatchObject({ action: "operationStart", input: request });
        const dispatched = command(id, input);
        checkInProgress = true;
        queued.resolve();
        return dispatched;
      });
      let cancelled = false;
      let currentTime = Date.parse(now);
      const pass = advanceInstallPlan({
        ...f.driver,
        cancelled: () => cancelled,
        now: () => currentTime,
      });
      inFlight.push(pass);
      expect(
        yield* Effect.promise(() =>
          Promise.race([queued.promise.then(() => true), pass.then(() => false)]),
        ),
      ).toBe(true);
      expect(calls).toEqual(["/api/jones-updates/operations/read"]);
      const changeRoute = (enrollment: "route-changed" | "identity-changed") =>
        f.authority.mockReturnValue({
          canDispatch: true,
          canReadReceipt: true,
          enrollment,
          fleetStatus:
            enrollment === "route-changed"
              ? enrolled()
              : { ...enrolled(), environmentId: otherEnv },
        });
      if (change === "cancelled") cancelled = true;
      else if (change === "closed") f.setPlan(nextPlan(f.plan()));
      else if (change === "lease expired") currentTime = Date.parse(leaseUntil);
      else if (change === "reservation changed")
        f.setPlan(
          withReservation(f.plan(), {
            ...reservation(),
            request: {
              kind: "operation-start",
              input: { ...request, expectedInstalledSource: tree },
            },
          }),
        );
      else if (change === "route changed") changeRoute("route-changed");
      else if (change === "identity changed") changeRoute("identity-changed");
      unblock.resolve();
      if (change === "route changed during the native read") {
        expect(
          yield* Effect.promise(() =>
            Promise.race([nativeRead.promise.then(() => true), pass.then(() => false)]),
          ),
        ).toBe(true);
        changeRoute("route-changed");
        resumeNative.resolve();
      }
      yield* Effect.promise(() => pass);
      expect(calls).toEqual(["/api/jones-updates/operations/read"]);
      expect(failures).toEqual([expect.any(JonesOperationNotSentError)]);
      expect(f.desktop.mock.calls.some(([query]) => query.type === "report")).toBe(false);
      if (change !== "closed") {
        expect(f.plan().devices[0]?.operations).toHaveLength(1);
        expect(f.plan().devices[0]?.operations[0]?.receipt).toBeUndefined();
      }
    }).pipe(Effect.scoped),
  );

  effectIt.effect(
    "fences the exact bound follower request after a same-host enrollment change",
    () =>
      Effect.gen(function* () {
        const f = fixture(1, 1);
        const input: JonesOperationStartInput = {
          ...request,
          admission: {
            kind: "follower",
            enrollmentId,
            directiveId: previousDirectiveId,
            targetSource: source,
            targetTree: tree,
          },
        };
        const original = {
          ...reservation(),
          request: { kind: "operation-start" as const, input },
          lease: { ...reservation().lease, leaseUntil: now },
        };
        const plan = f.plan();
        f.setPlan(
          withReservation(
            {
              ...plan,
              devices: [{ ...plan.devices[0]!, route: "follower-operations", enrollmentId }],
            },
            original,
          ),
        );
        const observed = yield* Effect.promise(() => f.observe(env));
        const update = observed.observation.state!;
        const fleet = enrolled();
        f.observe.mockResolvedValue({
          ...observed,
          observation: {
            ...observed.observation,
            state: { ...update, capability: { ...update.capability, boundAdmission: 1 } },
          },
          fleetStatus: { ...fleet, enrollment: { ...fleet.enrollment, enrollmentId: otherOpId } },
        });
        const grant: JonesInstallPlanGrant = {
          ...f.grant,
          step: "fence",
          request: { kind: "operation-fence", input },
        };
        f.desktop.mockImplementation(async (query) => {
          if (query.type !== "claim") return { state: f.state() };
          expect(query).toMatchObject({ step: "fence", operationId: opId });
          expect(query.boundAdmission).toBeUndefined();
          f.setPlan(
            withReservation(f.plan(), { ...original, fence: { lease: reservation().lease } }),
          );
          return { state: f.state(), grant };
        });
        const fenced = { ...input, protocol: 1, phase: "fenced", updatedAt: now } as const;
        const fetch = vi.fn<typeof globalThis.fetch>(async () => Response.json(fenced));
        f.operation.mockImplementation(yield* operationTransport(fetch));
        yield* Effect.promise(() => advanceInstallPlan(f.driver));
        expect(fetch).toHaveBeenCalledTimes(1);
        const [url, init] = fetch.mock.calls[0]!;
        expect(new URL(String(url)).pathname).toBe("/api/jones-updates/operations/v2/fence");
        expect(JSON.parse(String(init?.body))).toEqual(input);
        expect(f.desktop.mock.calls.at(-1)?.[0]).toMatchObject({
          evidence: { kind: "receipt", receipt: fenced },
        });
        expect(f.host).not.toHaveBeenCalled();
      }).pipe(Effect.scoped),
  );

  effectIt.effect.each([
    { name: "both sides", host: true, native: true, status: 200 },
    { name: "host only", host: true, native: false, status: 200 },
    { name: "native only", host: false, native: true, status: 200 },
    { name: "both sides and actual V2 route returns 404", host: true, native: true, status: 404 },
  ])("uses the durable intent for transport with $name", ({ host, native, status }) =>
    Effect.gen(function* () {
      const f = fixture(1, native ? 1 : undefined);
      const observed = yield* Effect.promise(() => f.observe(env));
      const update = observed.observation.state!;
      f.observe.mockResolvedValue({
        ...observed,
        observation: {
          ...observed.observation,
          state: {
            ...update,
            capability: { ...update.capability, ...(host ? { boundAdmission: 1 } : {}) },
          },
        },
      });
      const bound = host && native;
      const input: JonesOperationStartInput = bound
        ? { ...request, admission: { kind: "ordinary" } }
        : request;
      const operation = { ...reservation(), request: { kind: "operation-start" as const, input } };
      const receipt = { ...f.receipt, ...input };
      f.desktop.mockImplementation(async (query) => {
        if (query.type !== "claim") return { state: f.state() };
        expect(query.boundAdmission).toBe(bound ? 1 : undefined);
        f.setPlan(withReservation(f.plan(), operation));
        return { state: f.state(), grant: { ...f.grant, request: operation.request } };
      });
      const calls: Array<{ path: string; body: unknown }> = [];
      const command = yield* operationTransport(async (url, init) => {
        const path = new URL(String(url)).pathname;
        calls.push({ path, body: JSON.parse(String(init?.body)) });
        if (path.endsWith("/read")) return Response.json(emptyRead);
        return status === 404 ? new Response(null, { status }) : Response.json(receipt);
      });
      f.operation.mockImplementation(command);
      yield* Effect.promise(() => advanceInstallPlan(f.driver));
      expect(calls).toEqual([
        { path: `/api/jones-updates/operations/${bound ? "v2/" : ""}start`, body: input },
        ...(status === 404
          ? [{ path: "/api/jones-updates/operations/read", body: readInput }]
          : []),
      ]);
      const expected =
        status === 404 ? ["read", "claim", "read"] : ["read", "claim", "read", "report"];
      expect(f.desktop.mock.calls.map(([query]) => query.type)).toEqual(expected);
      expect(f.plan().devices[0]?.operations).toEqual([operation]);
    }).pipe(Effect.scoped),
  );

  it("claims before download, reports exact acceptance and never installs", async () => {
    const f = fixture();
    await advanceInstallPlan(f.driver);
    expect(f.desktop.mock.calls.map(([r]) => r.type)).toEqual(["read", "claim", "report"]);
    expect(f.desktop.mock.calls[1]?.[0]).toMatchObject({ step: "download", explicit: false });
    expect(f.operation.mock.calls[0]?.[1]).toEqual({
      action: "operationStart",
      input: request,
      beforeSend: expect.any(Function),
    });
    expect(f.desktop.mock.invocationCallOrder[1]).toBeLessThan(
      f.operation.mock.invocationCallOrder[0]!,
    );
  });

  it.each([
    {
      name: "refused",
      refusal: { code: "unsupported", message: "Read refused." },
    },
    {
      name: "read-only",
      readOnly: { code: "invalid", message: "Read-only ledger." },
    },
  ] as const)("does not act on a $name initial read even with a current plan", async (test) => {
    const f = fixture(1);
    f.desktop.mockResolvedValueOnce(
      test.name === "refused"
        ? { state: f.state(), refused: test.refusal }
        : { state: { ...f.state(), readOnly: test.readOnly } },
    );
    await advanceInstallPlan(f.driver);
    expect(f.desktop.mock.calls).toEqual([[{ type: "read" }]]);
    expect(f.observe).not.toHaveBeenCalled();
    expect(f.operation).not.toHaveBeenCalled();
    expect(f.host).not.toHaveBeenCalled();
  });

  it("a close before a delayed claim leaves the archived request unsent as plan B becomes current", async () => {
    const f = fixture(1);
    const a = f.plan();
    const b = nextPlan(a);
    let state = f.state();
    const claimArrived = Promise.withResolvers<JonesInstallPlanRequest>();
    const evaluateClaim = Promise.withResolvers<void>();
    f.desktop.mockImplementation(async (request) => {
      if (request.type !== "claim") return { state };
      claimArrived.resolve(request);
      await evaluateClaim.promise;
      return request.planId === state.current?.planId
        ? { state, grant: f.grant }
        : { state, refused: { code: "plan-mismatch", message: "The plan is closed." } };
    });
    const pass = advanceInstallPlan(f.driver);
    const claim = await Promise.race([claimArrived.promise, pass]);
    state = {
      ...state,
      current: b,
      last: { ...a, dismissedAt: now, cancelled: true, revision: a.revision + 1 },
    };
    evaluateClaim.resolve();
    await pass;
    expect(claim).toMatchObject({
      type: "claim",
      planId,
      environmentId: env,
      step: "download",
      explicit: false,
    });
    expect(f.desktop.mock.calls.map(([r]) => r.type)).toEqual(["read", "claim"]);
    expect(f.operation).not.toHaveBeenCalled();
    expect(f.host).not.toHaveBeenCalled();
  });

  it.each([false, true])(
    "reports an in-flight grant to its original plan after close (disposed: %s)",
    async (disposeBeforeReceipt) => {
      const f = fixture(1);
      const a = f.plan();
      const b = nextPlan(a);
      let state = f.state();
      const granted = Promise.withResolvers<void>();
      const deliverGrant = Promise.withResolvers<void>();
      const sent = Promise.withResolvers<void>();
      const deliverReceipt = Promise.withResolvers<JonesOperationReceipt>();
      let disposed = false;
      f.operation.mockImplementation(async (_environmentId, command) => {
        if (command.action !== "operationStart" || !(await command.beforeSend?.()))
          throw new Error("fixture did not qualify queued dispatch");
        sent.resolve();
        return deliverReceipt.promise;
      });
      f.desktop.mockImplementation(async (r) => {
        if (r.type === "claim" && r.planId === state.current?.planId && r.environmentId === env) {
          state = { ...state, current: withReservation(a) };
          const authorized = state;
          granted.resolve();
          await deliverGrant.promise;
          return { state: authorized, grant: f.grant };
        }
        if (
          r.type === "report" &&
          r.planId === state.last?.planId &&
          r.environmentId === env &&
          r.operationId === state.last.devices[0]?.operations[0]?.operationId &&
          r.evidence.kind === "receipt"
        ) {
          const archived = state.last;
          state = {
            ...state,
            last: withReservation(
              { ...archived, revision: archived.revision + 1 },
              { ...reservation(), receipt: r.evidence.receipt },
            ),
          };
        }
        return { state };
      });
      const pass = advanceInstallPlan({ ...f.driver, cancelled: () => disposed });
      await Promise.race([granted.promise, pass]);
      deliverGrant.resolve();
      await Promise.race([sent.promise, pass]);
      const closed = state.current!;
      state = {
        ...state,
        current: b,
        last: { ...closed, dismissedAt: now, cancelled: true, revision: closed.revision + 1 },
      };
      disposed = disposeBeforeReceipt;
      if (disposed) {
        const currentAuthority = f.authority.getMockImplementation()!;
        f.canReadReceipt.mockReturnValue(false);
        f.authority.mockImplementation((device) => ({
          ...currentAuthority(device),
          canReportReceipt: true,
        }));
      }
      deliverReceipt.resolve(f.receipt);
      await pass;

      expect(f.operation.mock.calls).toEqual([
        [env, { action: "operationStart", input: request, beforeSend: expect.any(Function) }],
      ]);
      expect(f.desktop.mock.calls.map(([r]) => r.type)).toEqual([
        "read",
        "claim",
        "read",
        "report",
      ]);
      expect(f.desktop.mock.calls.at(-1)?.[0]).toEqual({
        type: "report",
        planId,
        environmentId: env,
        operationId: opId,
        evidence: { kind: "receipt", receipt: f.receipt },
      });
      expect(state.current).toBe(b);
      expect(state.last).toMatchObject({
        planId,
        devices: [{ environmentId: env, operations: [{ operationId: opId, receipt: f.receipt }] }],
      });
    },
  );

  it("runs a disjoint active device without reading, replaying, or fencing archived reservations", async () => {
    const f = fixture(1);
    const a = f.plan();
    const b = nextPlan(a);
    const archive = {
      ...withReservation(a),
      dismissedAt: now,
      cancelled: true as const,
      revision: a.revision + 1,
    };
    const state: JonesInstallPlanState = {
      ...f.state(),
      documentRevision: 2,
      current: null,
      last: archive,
      recoveryHistory: [
        {
          planId,
          revision: archive.revision,
          targetVersion: a.target.version,
          closedAt: now,
          pendingLaptop: false,
          pendingDevices: [{ environmentId: env, label: "Host" }],
        },
      ],
    };
    f.desktop.mockResolvedValue({ state, historyPlan: archive });
    await advanceInstallPlan(f.driver);
    expect(f.desktop.mock.calls).toEqual([[{ type: "read" }]]);
    expect(f.observe).not.toHaveBeenCalled();
    expect(f.operation).not.toHaveBeenCalled();

    const active = {
      ...b,
      laptop: {
        ...b.laptop,
        gate: { ...a.laptop.gate!, transactionId: "tx-b", activeSource: b.target.sourceSha },
      },
    };
    const bInput: JonesOperationStartInput = {
      ...request,
      environmentId: otherEnv,
      operationId: otherOpId,
      action: { kind: "download-exact", targetSource: b.target.sourceSha },
      correlation: { planId: otherPlanId, deviceKey: otherEnv },
    };
    f.desktop.mockImplementation(async (r) => ({
      state: { ...state, current: active },
      ...(r.type === "claim"
        ? {
            grant: {
              ...f.grant,
              planId: otherPlanId,
              environmentId: otherEnv,
              operationId: otherOpId,
              request: { kind: "operation-start", input: bInput },
            },
          }
        : {}),
    }));
    const observed = await f.observe(otherEnv);
    f.observe.mockClear();
    f.observe.mockResolvedValue({
      ...observed,
      observation: {
        ...observed.observation,
        state: { ...observed.observation.state!, environmentId: otherEnv },
      },
    });
    f.operation.mockResolvedValue({ ...f.receipt, ...bInput });
    f.desktop.mockClear();
    await advanceInstallPlan(f.driver);
    expect(f.desktop.mock.calls.map(([r]) => r.type)).toEqual(["read", "claim", "report"]);
    expect(f.observe.mock.calls).toEqual([[otherEnv]]);
    expect(f.operation.mock.calls).toEqual([
      [otherEnv, { action: "operationStart", input: bInput, beforeSend: expect.any(Function) }],
    ]);
    expect(f.host).not.toHaveBeenCalled();
  });

  it.each(["removed", "changed", "paused"] as const)(
    "keeps %s devices or gates inert",
    async (kind) => {
      const f = fixture(),
        p = f.plan();
      f.setPlan(
        kind === "removed"
          ? { ...p, devices: [{ ...p.devices[0]!, membership: "removed" }] }
          : {
              ...p,
              laptop: {
                ...p.laptop,
                gateClosed: { at: now, reason: kind === "paused" ? "blocked" : "changed" },
                ...(kind === "paused" ? { gatePause: true as const } : {}),
              },
            },
      );
      await advanceInstallPlan(f.driver);
      expect(f.operation).not.toHaveBeenCalled();
      expect(f.desktop.mock.calls.map(([r]) => r.type)).toEqual(["read"]);
    },
  );

  it.each(["new download", "download replay", "fence"] as const)(
    "uses no lease or host mutation for a read-only peer's %s",
    async (step) => {
      const f = fixture(1);
      if (step !== "new download") {
        const plan = f.plan();
        f.setPlan({
          ...plan,
          devices: [
            {
              ...plan.devices[0]!,
              operations: [
                {
                  operationId: opId,
                  step: "download",
                  request: { kind: "operation-start", input: request },
                  lease: {
                    claimant: f.driver.claimant,
                    reservedAt: now,
                    leaseUntil: step === "fence" ? now : leaseUntil,
                  },
                },
              ],
            },
          ],
        });
      }
      const observed = await f.observe(env);
      f.observe.mockResolvedValueOnce({ ...observed, canOperate: false });
      await advanceInstallPlan(f.driver);
      f.canDispatch.mockReturnValue(false);
      await advanceInstallPlan(f.driver);
      expect(f.desktop.mock.calls.map(([r]) => r.type)).toEqual(["read", "read"]);
      expect(f.operation).not.toHaveBeenCalled();
      expect(f.host).not.toHaveBeenCalled();
    },
  );

  it("does not dispatch when authority changes during a native claim", async () => {
    const f = fixture();
    const claim = f.desktop.getMockImplementation()!;
    f.desktop.mockImplementation(async (request) => {
      const result = await claim(request);
      if (request.type === "claim") f.canDispatch.mockReturnValue(false);
      return result;
    });
    await advanceInstallPlan(f.driver);
    expect(f.desktop.mock.calls.map(([r]) => r.type)).toEqual(["read", "claim"]);
    expect(f.operation).not.toHaveBeenCalled();
  });

  it.each(["new download", "reclaim"] as const)(
    "requires positive per-pass enrollment for %s even when newer evidence matches",
    async (step) => {
      const f = fixture(1);
      const p = f.plan();
      const following = {
        ...p,
        devices: [{ ...p.devices[0]!, route: "follower-operations" as const, enrollmentId }],
      };
      f.setPlan(step === "reclaim" ? withReservation(following) : following);
      const { fleetStatus: _fleet, ...unknown } = await f.observe(env);
      f.authority.mockReturnValue({
        canDispatch: true,
        canReadReceipt: true,
        enrollment: "match",
        fleetStatus: enrolled(),
      });
      for (const bad of [
        unknown,
        { ...unknown, fleetStatus: null },
        {
          ...unknown,
          fleetStatus: {
            ...enrolled(),
            enrollment: { ...enrolled().enrollment, enrollmentId: otherOpId },
          },
        },
        {
          ...unknown,
          fleetStatus: {
            ...enrolled(),
            enrollment: { ...enrolled().enrollment, environmentId: otherEnv },
          },
        },
      ]) {
        f.observe.mockResolvedValueOnce(bad);
        f.operation.mockResolvedValueOnce({ receipts: [{ operationId: opId, receipt: null }] });
        await advanceInstallPlan(f.driver);
      }
      expect(f.desktop.mock.calls.map(([r]) => r.type)).toEqual(["read", "read", "read", "read"]);
      expect(f.operation.mock.calls).toEqual(
        step === "new download"
          ? []
          : Array.from({ length: 3 }, () => [
              env,
              { action: "operationRead", input: { environmentId: env, operationIds: [opId] } },
            ]),
      );
      expect(f.host).not.toHaveBeenCalled();
    },
  );

  it.each(["unknown", "route-changed"] as const)(
    "reads the original reservation if route becomes %s while reclaim is pending",
    async (enrollment) => {
      const f = fixture(1);
      f.setPlan(withReservation(f.plan()));
      const admitted = f.desktop.getMockImplementation()!;
      f.desktop.mockImplementation(async (r) => {
        if (r.type === "claim")
          f.authority.mockReturnValue({
            canDispatch: false,
            canReadReceipt: true,
            enrollment,
            fleetStatus: undefined,
          });
        return admitted(r);
      });
      await advanceInstallPlan(f.driver);
      expect(f.desktop.mock.calls.map(([r]) => r.type)).toEqual(["read", "claim", "report"]);
      expect(f.operation.mock.calls).toEqual([
        [env, { action: "operationRead", input: { environmentId: env, operationIds: [opId] } }],
      ]);
      expect(f.desktop.mock.calls.at(-1)?.[0]).toMatchObject({ evidence: { ledgerRead: true } });
      expect(f.host).not.toHaveBeenCalled();
    },
  );

  it("fences expired exact operations with a missing read or proved same-host change", async () => {
    const f = fixture(1);
    const { fleetStatus: _fleet, ...unknown } = await f.observe(env);
    f.observe.mockResolvedValue(unknown);
    f.setPlan(
      withReservation(f.plan(), {
        ...reservation(),
        lease: { ...reservation().lease, leaseUntil: now },
      }),
    );
    f.desktop.mockResolvedValue({
      state: f.state(),
      grant: { ...f.grant, step: "fence", request: { kind: "operation-fence", input: request } },
    });
    const fenced: JonesOperationReceipt = {
      ...request,
      protocol: 1,
      phase: "fenced",
      updatedAt: now,
    };
    f.operation.mockResolvedValueOnce(fenced);
    await advanceInstallPlan(f.driver);
    expect(f.desktop.mock.calls[1]?.[0]).toMatchObject({
      step: "fence",
      operationId: opId,
      explicit: false,
    });
    const exactFence = [
      env,
      { action: "operationFence", input: request, beforeSend: expect.any(Function) },
    ];
    expect(f.operation.mock.calls).toEqual([exactFence]);
    expect(f.host).not.toHaveBeenCalled();

    f.desktop.mockClear();
    f.operation.mockClear();
    f.observe.mockResolvedValueOnce({ ...unknown, fleetStatus: enrolled() });
    f.operation.mockResolvedValueOnce(fenced);
    await advanceInstallPlan(f.driver);
    expect(f.operation.mock.calls).toEqual([exactFence]);

    f.desktop.mockClear();
    f.operation.mockClear();
    f.observe.mockResolvedValueOnce({
      ...unknown,
      fleetStatus: { ...enrolled(), environmentId: otherEnv },
    });
    await advanceInstallPlan(f.driver);
    expect(f.desktop.mock.calls).toEqual([[{ type: "read" }]]);
    expect(f.operation).not.toHaveBeenCalled();
  });

  it("reauthorizes an exact receipt read and report independently of enrollment", async () => {
    const f = fixture(1);
    const { fleetStatus: _fleet, ...unknown } = await f.observe(env);
    f.observe.mockResolvedValue({ ...unknown, canOperate: false });
    f.setPlan(withReservation(f.plan()));
    f.canReadReceipt.mockReturnValue(false);
    await advanceInstallPlan(f.driver);
    expect(f.operation).not.toHaveBeenCalled();
    f.canReadReceipt.mockReturnValue(true);
    f.operation.mockImplementationOnce(async () => {
      f.canReadReceipt.mockReturnValue(false);
      return { receipts: [{ operationId: opId, receipt: f.receipt }] };
    });
    await advanceInstallPlan(f.driver);
    expect(f.operation.mock.calls.map(([, r]) => r.action)).toEqual(["operationRead"]);
    expect(f.desktop.mock.calls.map(([r]) => r.type)).toEqual(["read", "read"]);
  });

  it("resends only its own identical live reservation and fences an abandoned one", async () => {
    const f = fixture(1, 1);
    const observed = await f.observe(env);
    const state = observed.observation.state!;
    f.observe.mockResolvedValue({
      ...observed,
      observation: {
        ...observed.observation,
        state: { ...state, capability: { ...state.capability, boundAdmission: 1 } },
      },
    });
    f.setPlan(withReservation(f.plan()));
    await advanceInstallPlan(f.driver);
    expect(f.desktop.mock.calls[1]?.[0]).toMatchObject({ step: "download", operationId: opId });
    expect(f.desktop.mock.calls[1]?.[0]).not.toHaveProperty("boundAdmission");
    expect(f.operation.mock.calls[0]?.[1]).toEqual({
      action: "operationStart",
      input: request,
      beforeSend: expect.any(Function),
    });
    f.operation.mockClear();
    f.driver.now = () => Date.parse(leaseUntil) + 1;
    f.desktop.mockImplementation(async (r) => ({
      state: {
        schema: 1,
        current: f.plan(),
        last: null,
        laptop: { observedAt: now, activationBlocked: false },
        view: { complete: false },
      },
      ...(r.type === "claim"
        ? {
            grant: {
              ...f.grant,
              step: "fence",
              leaseUntil: "2026-10-27T05:04:00.000Z",
              request: { kind: "operation-fence", input: request },
            },
          }
        : {}),
    }));
    await advanceInstallPlan(f.driver);
    expect(f.operation.mock.calls[0]?.[1]).toEqual({
      action: "operationFence",
      input: request,
      beforeSend: expect.any(Function),
    });
  });

  it("does not mint a new lease if the original is fenced before the delayed reclaim", async () => {
    const f = fixture(1);
    const reserved = withReservation(f.plan());
    f.setPlan(reserved);
    const waiting = Promise.withResolvers<void>();
    const evaluateClaim = Promise.withResolvers<void>();
    let replacementLeases = 0;
    f.desktop.mockImplementation(async (r) => {
      if (r.type !== "claim") return { state: f.state() };
      waiting.resolve();
      await evaluateClaim.promise;
      if (r.operationId === opId)
        return {
          state: f.state(),
          refused: { code: "outstanding-operation", message: "The original is fenced." },
        };
      // An unbound request after fencing is indistinguishable from a fresh schedule.
      replacementLeases++;
      return {
        state: f.state(),
        grant: {
          ...f.grant,
          operationId: otherOpId,
          request: { kind: "operation-start", input: { ...request, operationId: otherOpId } },
        },
      };
    });
    const pass = advanceInstallPlan(f.driver);
    await Promise.race([waiting.promise, pass]);
    f.setPlan(
      withReservation(
        { ...reserved, revision: reserved.revision + 1 },
        {
          ...reservation(),
          receipt: { ...request, protocol: 1, phase: "fenced", updatedAt: leaseUntil },
        },
      ),
    );
    evaluateClaim.resolve();
    await pass;
    expect(f.desktop.mock.calls[1]?.[0]).toMatchObject({ step: "download", operationId: opId });
    expect(replacementLeases).toBe(0);
    expect(f.operation).not.toHaveBeenCalled();
  });

  it("reads an old native download reservation without inferring absence and fences on expiry", async () => {
    const f = fixture();
    f.setPlan(withReservation(f.plan()));
    f.operation.mockResolvedValueOnce({ receipts: [{ operationId: opId, receipt: null }] });
    await advanceInstallPlan(f.driver);
    expect(f.desktop.mock.calls).toEqual([[{ type: "read" }]]);
    expect(f.operation.mock.calls).toEqual([
      [env, { action: "operationRead", input: { environmentId: env, operationIds: [opId] } }],
    ]);

    f.driver.now = () => Date.parse(leaseUntil) + 1;
    const fenced: JonesOperationReceipt = {
      ...request,
      protocol: 1,
      phase: "fenced",
      updatedAt: leaseUntil,
    };
    f.operation.mockResolvedValueOnce(fenced);
    f.desktop.mockImplementation(async (r) => ({
      state: f.state(),
      ...(r.type === "claim"
        ? {
            grant: {
              ...f.grant,
              step: "fence",
              leaseUntil: "2026-10-27T05:04:00.000Z",
              request: { kind: "operation-fence", input: request },
            },
          }
        : {}),
    }));
    await advanceInstallPlan(f.driver);
    expect(f.operation.mock.calls.at(-1)).toEqual([
      env,
      { action: "operationFence", input: request, beforeSend: expect.any(Function) },
    ]);
    expect(f.desktop.mock.calls.map(([r]) => r.type)).toEqual(["read", "read", "claim", "report"]);
    expect(f.desktop.mock.calls.at(-1)?.[0]).toEqual({
      type: "report",
      planId,
      environmentId: env,
      operationId: opId,
      evidence: { kind: "receipt", receipt: fenced },
    });
  });

  it.each(["another operation", "start payload", "different body"] as const)(
    "rejects a fence grant for %s",
    async (kind) => {
      const f = fixture(1);
      f.setPlan(withReservation(f.plan()));
      f.driver.now = () => Date.parse(leaseUntil);
      const { fleetStatus: _fleet, ...unknown } = await f.observe(env);
      f.observe.mockResolvedValue(unknown);
      f.desktop.mockImplementation(async (r) => ({
        state: f.state(),
        ...(r.type === "claim"
          ? {
              grant: {
                ...f.grant,
                step: "fence",
                leaseUntil: "2026-10-27T05:04:00.000Z",
                ...(kind === "another operation"
                  ? {
                      operationId: otherOpId,
                      request: {
                        kind: "operation-fence",
                        input: { ...request, operationId: otherOpId },
                      },
                    }
                  : kind === "different body"
                    ? {
                        request: {
                          kind: "operation-fence",
                          input: { ...request, expectedInstalledSource: "d".repeat(40) },
                        },
                      }
                    : {}),
              },
            }
          : {}),
      }));
      await advanceInstallPlan(f.driver);
      expect(f.desktop.mock.calls[1]?.[0]).toMatchObject({ step: "fence", operationId: opId });
      expect(f.operation).not.toHaveBeenCalled();
    },
  );

  it("reads after lost acceptance and preserves null receipts without another action", async () => {
    const f = fixture(),
      p = f.plan();
    f.setPlan({
      ...p,
      devices: [
        {
          ...p.devices[0]!,
          operations: [
            {
              operationId: opId,
              step: "download",
              request: { kind: "operation-start", input: request },
              lease: { claimant: "other", reservedAt: now, leaseUntil },
              receipt: f.receipt,
            },
          ],
        },
      ],
    });
    f.operation.mockResolvedValue({ receipts: [{ operationId: opId, receipt: null }] });
    await advanceInstallPlan(f.driver);
    expect(f.operation.mock.calls.map(([, r]) => r.action)).toEqual(["operationRead"]);
    expect(f.desktop.mock.calls.map(([r]) => r.type)).toEqual(["read"]);
    f.operation.mockResolvedValue({
      receipts: [{ operationId: opId, receipt: { ...f.receipt, environmentId: laptop } }],
    });
    await advanceInstallPlan(f.driver);
    expect(f.desktop.mock.calls.filter(([r]) => r.type === "report")).toHaveLength(0);
    const observed = await f.observe(env);
    f.observe.mockResolvedValue({ ...observed, canOperate: false });
    f.canDispatch.mockReturnValue(false);
    f.operation.mockResolvedValue({ receipts: [{ operationId: opId, receipt: f.receipt }] });
    await advanceInstallPlan(f.driver);
    expect(f.operation.mock.calls.at(-1)?.[1]).toMatchObject({
      action: "operationRead",
      input: { environmentId: env, operationIds: [opId] },
    });
    expect(f.desktop.mock.calls.at(-1)?.[0]).toMatchObject({
      type: "report",
      operationId: opId,
      evidence: { kind: "receipt", receipt: f.receipt, ledgerRead: true },
    });
    expect(f.desktop.mock.calls.some(([r]) => r.type === "claim")).toBe(false);
  });

  it.each([false, true])(
    "reads a bound reservation with no current bound support (expired: %s)",
    async (expired) => {
      const f = fixture(1, 1);
      const input: JonesOperationStartInput = { ...request, admission: { kind: "ordinary" } };
      f.setPlan(
        withReservation(f.plan(), {
          ...reservation(),
          request: { kind: "operation-start", input },
          lease: { ...reservation().lease, leaseUntil: expired ? now : leaseUntil },
        }),
      );
      const receipt = { ...f.receipt, ...input };
      f.canDispatch.mockReturnValue(false);
      f.operation.mockResolvedValue({ receipts: [{ operationId: opId, receipt }] });
      await advanceInstallPlan(f.driver);
      expect(f.operation.mock.calls).toEqual([
        [env, { action: "operationRead", input: readInput }],
      ]);
      expect(f.desktop.mock.calls.map(([query]) => query.type)).toEqual(["read", "report"]);
      expect(f.desktop.mock.calls.at(-1)?.[0]).toMatchObject({
        evidence: { kind: "receipt", receipt, ledgerRead: true },
      });
    },
  );

  it("requires positive exact ledger proof before replacing a synthetic unsupported reply", async () => {
    const f = fixture(1);
    f.setPlan(withReservation(f.plan()));
    const provisional: JonesOperationReceipt = {
      ...request,
      protocol: 1,
      phase: "refused",
      code: "unsupported",
      updatedAt: leaseUntil,
    };
    f.operation.mockResolvedValueOnce(provisional).mockResolvedValueOnce(emptyRead);
    await advanceInstallPlan(f.driver);
    expect(f.operation.mock.calls.map(([, query]) => query.action)).toEqual([
      "operationStart",
      "operationRead",
    ]);
    expect(f.desktop.mock.calls.map(([query]) => query.type)).toEqual(["read", "claim"]);
    expect(f.plan().devices[0]?.operations).toEqual([reservation()]);

    f.setPlan(withReservation(f.plan(), { ...reservation(), receipt: provisional }));
    const completed: JonesOperationReceipt = {
      ...f.receipt,
      phase: "completed",
      staged: { stagedHandle: "stage", sourceSha: source, sourceTree: tree, artifactId: 1 },
    };
    f.operation.mockResolvedValueOnce({ receipts: [{ operationId: opId, receipt: completed }] });
    await advanceInstallPlan(f.driver);
    expect(f.operation.mock.calls.map(([, query]) => query.action)).toEqual([
      "operationStart",
      "operationRead",
      "operationRead",
    ]);
    expect(f.desktop.mock.calls.at(-1)?.[0]).toMatchObject({
      evidence: { kind: "receipt", receipt: completed, ledgerRead: true },
    });
    expect(f.desktop.mock.calls.filter(([query]) => query.type === "claim")).toHaveLength(1);
  });

  it("leaves the request unresolved when the ledger read returns the conflicting body", async () => {
    const f = fixture(1);
    f.setPlan(withReservation(f.plan()));
    const conflict: JonesOperationReceipt = {
      ...request,
      protocol: 1,
      phase: "refused",
      code: "operation-conflict",
      updatedAt: now,
    };
    const retained: JonesOperationReceipt = { ...f.receipt, expectedInstalledSource: tree };
    f.operation.mockResolvedValueOnce(conflict).mockResolvedValueOnce({
      receipts: [{ operationId: opId, receipt: retained }],
    });

    await advanceInstallPlan(f.driver);

    expect(f.operation.mock.calls.map(([, query]) => query.action)).toEqual([
      "operationStart",
      "operationRead",
    ]);
    expect(f.desktop.mock.calls.map(([query]) => query.type)).toEqual(["read", "claim"]);
    expect(f.plan().devices[0]?.operations).toEqual([reservation()]);
  });

  it.each([
    {
      name: "receipt-only support",
      capability: { check: false, download: false, install: false, operationReceipts: 1 },
    },
    {
      name: "download disabled",
      capability: { check: true, download: false, install: true, operations: 1, exactSource: true },
    },
    {
      name: "install disabled",
      capability: { check: true, download: true, install: false, operations: 1, exactSource: true },
    },
  ] as const)(
    "reads exact receipts and never claims a lease with $name",
    async ({ capability }) => {
      const f = fixture();
      const plan = f.plan();
      const observed = await f.observe(env);
      const before = observed.observation.state!;
      const limited = {
        ...observed,
        observation: { ...observed.observation, state: { ...before, capability } },
      };
      const reservation = {
        operationId: opId,
        step: "download" as const,
        request: { kind: "operation-start" as const, input: request },
        lease: { claimant: f.driver.claimant, reservedAt: now, leaseUntil: now },
      };
      const setOperation = (operation = reservation) => {
        f.setPlan({ ...plan, devices: [{ ...plan.devices[0]!, operations: [operation] }] });
      };

      f.observe.mockResolvedValue(limited);
      for (const expiresAt of [now, leaseUntil]) {
        setOperation({ ...reservation, lease: { ...reservation.lease, leaseUntil: expiresAt } });
        f.operation.mockResolvedValueOnce({ receipts: [{ operationId: opId, receipt: null }] });
        await advanceInstallPlan(f.driver);
        expect(f.operation).toHaveBeenLastCalledWith(env, {
          action: "operationRead",
          input: { environmentId: env, operationIds: [opId] },
        });
      }
      expect(f.operation).toHaveBeenCalledTimes(2);
      expect(f.desktop.mock.calls.map(([r]) => r.type)).toEqual(["read", "read"]);

      f.operation.mockClear();
      const accepted = { ...reservation, receipt: f.receipt };
      setOperation(accepted);
      f.observe.mockResolvedValueOnce({
        ...limited,
        observation: {
          ...limited.observation,
          state: { ...before, capability, environmentId: laptop },
        },
      });
      await advanceInstallPlan(f.driver);
      f.observe.mockResolvedValueOnce({
        ...limited,
        observation: { ...limited.observation, freshness: "stale" },
      });
      await advanceInstallPlan(f.driver);
      expect(f.operation).not.toHaveBeenCalled();

      f.observe.mockResolvedValueOnce({ ...limited, canOperate: false });
      f.canDispatch.mockReturnValue(false);
      await advanceInstallPlan(f.driver);
      expect(f.desktop.mock.calls.at(-1)?.[0]).toEqual({
        type: "report",
        planId,
        environmentId: env,
        operationId: opId,
        evidence: { kind: "receipt", receipt: f.receipt, ledgerRead: true },
      });

      f.setPlan(plan);
      f.canDispatch.mockReturnValue(true);
      await advanceInstallPlan(f.driver);
      expect(f.operation).toHaveBeenCalledTimes(1);
      expect(f.desktop.mock.calls.some(([r]) => r.type === "claim")).toBe(false);
      expect(f.host).not.toHaveBeenCalled();
    },
  );

  it("does not borrow another window's lease or perform explicit install/Stop", async () => {
    const f = fixture(),
      p = f.plan();
    for (const step of ["download", "install"] as const) {
      f.setPlan({
        ...p,
        devices: [
          {
            ...p.devices[0]!,
            operations: [
              {
                operationId: opId,
                step,
                request: { kind: "operation-start", input: request },
                lease: {
                  claimant: step === "install" ? "window:a" : "window:other",
                  reservedAt: now,
                  leaseUntil,
                },
              },
            ],
          },
        ],
      });
      await advanceInstallPlan(f.driver);
    }
    expect(f.operation).not.toHaveBeenCalled();
  });

  const directiveReplies = [
    "reply",
    "reply after disposal",
    "lost reply",
    "first-send conflict",
    "no recorded host effect",
  ] as const;
  it.each(directiveReplies)("reconciles the directive after %s", async (result) => {
    const f = fixture(),
      p = f.plan();
    f.setPlan({
      ...p,
      devices: [{ ...p.devices[0]!, route: "follower-operations", enrollmentId }],
    });
    const observed = await f.observe(env);
    const fleet = {
      environmentId: env,
      operationProtocol: 1 as const,
      planDirectiveProtocol: 1 as const,
      enrollment: {
        environmentId: env,
        enrollmentId,
        enabled: true,
        continueRunningThreads: false,
      },
      operation: null,
      update: observed.observation.state,
      follower: {
        protocol: 1 as const,
        leaderSource: installed,
        state: "waiting-for-leader" as const,
        planDirective: {
          directiveId: previousDirectiveId,
          targetSource: installed,
          targetTree: tree,
          mode: "manual-review" as const,
          issuedAt: now,
        },
      },
    };
    f.observe.mockResolvedValue({ ...observed, fleetStatus: fleet });
    const directive = {
      directiveId: opId,
      environmentId: env,
      enrollmentId,
      repository: "Jones-Systems/Jones-Code" as const,
      targetSource: source,
      targetTree: tree,
      expectedDirectiveId: previousDirectiveId,
    };
    const state = (): JonesInstallPlanState => ({
      schema: 1,
      current: f.plan(),
      last: null,
      laptop: { observedAt: now, activationBlocked: false },
      view: { complete: false },
    });
    const grant: JonesInstallPlanGrant = {
      ...f.grant,
      step: "directive",
      request: { kind: "plan-directive", input: directive },
    };
    f.desktop.mockResolvedValue({ state: state(), grant });
    const applied = {
      ...fleet,
      follower: {
        ...fleet.follower,
        planDirective: {
          directiveId: opId,
          targetSource: source,
          targetTree: tree,
          mode: "manual-review" as const,
          issuedAt: now,
        },
      },
    };
    if (result === "reply") {
      f.observe.mockResolvedValueOnce({ ...observed, canOperate: false, fleetStatus: fleet });
      await advanceInstallPlan(f.driver);
      expect(f.desktop.mock.calls.map(([request]) => request.type)).toEqual(["read"]);
      expect(f.host).not.toHaveBeenCalled();
      f.desktop.mockClear();
    }
    let disposed = false;
    const replied = result === "reply" || result === "reply after disposal";
    if (!replied)
      f.host.mockRejectedValueOnce({
        _tag: "FleetHostError",
        reason: result === "first-send conflict" ? "conflict" : "storage",
        message: result === "first-send conflict" ? "Host busy." : "sync failed",
      });
    else
      f.host.mockImplementationOnce(async () => {
        disposed = result === "reply after disposal";
        return applied;
      });

    await advanceInstallPlan({ ...f.driver, cancelled: () => disposed });
    expect(f.host.mock.calls[0]?.[1]).toEqual({ action: "planDirective", input: directive });
    if (!replied) {
      expect(f.desktop.mock.calls.map(([input]) => input.type)).toEqual(["read", "claim"]);
      const pending = f.plan();
      f.setPlan({
        ...pending,
        devices: [
          {
            ...pending.devices[0]!,
            operations: [
              {
                operationId: opId,
                step: "directive",
                request: { kind: "plan-directive", input: directive },
                lease: { claimant: f.driver.claimant, reservedAt: now, leaseUntil },
              },
            ],
          },
        ],
      });
      const resumed = { ...f.driver, claimant: "window:b", now: () => Date.parse(leaseUntil) };
      const update = observed.observation.state!;
      const limited = {
        ...observed,
        observation: {
          ...observed.observation,
          state: { ...update, capability: { ...update.capability, download: false } },
        },
      };
      if (result === "lost reply") {
        f.desktop.mockResolvedValue({ state: state() });
        const { fleetStatus: _unavailable, ...withoutFleet } = limited;
        f.observe.mockResolvedValueOnce(withoutFleet);
        f.desktop.mockClear();
        await advanceInstallPlan(resumed);
        expect(f.desktop.mock.calls.map(([r]) => r.type)).toEqual(["read"]);
        expect(f.host).toHaveBeenCalledTimes(1);

        f.observe.mockResolvedValue({ ...limited, canOperate: false, fleetStatus: applied });
        f.canDispatch.mockReturnValue(false);
        await advanceInstallPlan(resumed);
        expect(f.host).toHaveBeenCalledTimes(1);
      } else {
        const changed = {
          ...applied,
          follower: {
            ...applied.follower,
            planDirective: { ...applied.follower.planDirective, directiveId: enrollmentId },
          },
        };
        f.observe.mockResolvedValue({ ...observed, fleetStatus: changed });
        await advanceInstallPlan(resumed);
        expect(f.host).toHaveBeenCalledTimes(1);
        f.observe.mockResolvedValue({ ...observed, canOperate: false, fleetStatus: fleet });
        f.desktop.mockClear();
        await advanceInstallPlan(resumed);
        expect(f.host).toHaveBeenCalledTimes(1);
        expect(f.desktop.mock.calls.map(([r]) => r.type)).toEqual(["read"]);
        f.observe.mockResolvedValue({ ...limited, fleetStatus: fleet });
        f.desktop.mockClear();
        await advanceInstallPlan(resumed);
        expect(f.host).toHaveBeenCalledTimes(1);
        expect(f.desktop.mock.calls.map(([r]) => r.type)).toEqual(["read"]);
        f.observe.mockResolvedValue({ ...observed, fleetStatus: fleet });
        f.desktop.mockResolvedValue({
          state: state(),
          grant: {
            ...grant,
            leaseUntil: new Date(Date.parse(leaseUntil) + 120_000).toISOString(),
          },
        });
        f.host.mockRejectedValueOnce({
          _tag: "FleetHostError",
          reason: "conflict",
          message: "Host busy.",
        });
        await advanceInstallPlan(resumed);
        expect(f.desktop.mock.calls.at(-1)?.[0]).toMatchObject({
          type: "claim",
          operationId: opId,
          claimant: "window:b",
          expectedDirectiveId: previousDirectiveId,
        });
        expect(f.host.mock.calls[1]?.[1]).toEqual({ action: "planDirective", input: directive });
        f.host.mockResolvedValueOnce(applied);
        await advanceInstallPlan(resumed);
        expect(f.host.mock.calls[2]?.[1]).toEqual({ action: "planDirective", input: directive });
      }
    }
    expect(f.operation).not.toHaveBeenCalled();
    expect(f.desktop.mock.calls.at(-1)?.[0]).toMatchObject({
      type: "report",
      planId,
      environmentId: env,
      operationId: opId,
      evidence: { kind: "directive" },
    });
  });
});
