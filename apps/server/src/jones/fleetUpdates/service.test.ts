import { describe, expect, it } from "@effect/vitest";
import * as NodeServices from "@effect/platform-node/NodeServices";
import { vi } from "vite-plus/test";
import * as Deferred from "effect/Deferred";
import * as Fiber from "effect/Fiber";
import * as Clock from "effect/Clock";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Path from "effect/Path";
import * as Schema from "effect/Schema";
import * as HttpApi from "effect/http-api/HttpApi";
import * as HttpApiBuilder from "effect/http-api/HttpApiBuilder";
import * as HttpRouter from "effect/http/HttpRouter";
import * as HttpServer from "effect/http/HttpServer";
import * as TestClock from "effect/testing/TestClock";
import {
  AuthOrchestrationOperateScope,
  AuthOrchestrationReadScope,
  AuthSessionId,
  EnvironmentHttpApi,
  EnvironmentId,
  type JonesUpdateState,
} from "@t3tools/contracts";
import { FLEET_UPDATES_HTTP_BASE } from "@t3tools/contracts/jones/fleet-updates";
import type {
  FleetEnrollment,
  FleetHostOperation,
  FleetStageInput,
} from "@t3tools/contracts/jones/fleet-updates";
import * as EnvironmentAuth from "../../auth/EnvironmentAuth.ts";
import * as AuthHttp from "../../auth/http.ts";
import {
  AUTOMATIC_ACTIVATION_SPACING_MS,
  QUIET_INPUT_IDLE_MS,
  type HostWorkSnapshot,
} from "../updates/followerPolicy.ts";
import { fleetUpdatesHttpApiLayer } from "./http.ts";
import type { FleetQuietSignals } from "./quietSignals.ts";
import {
  FleetUpdates,
  FOLLOWER_STAGE_RETRY_MS,
  makeFleetUpdates,
  type FleetHostUpdater,
} from "./service.ts";
import { createFleetHostStore, FleetFollowerRecord, read, type FleetHostStore } from "./store.ts";

const environmentId = EnvironmentId.make("fleet-host");
const enrollment: FleetEnrollment = {
  environmentId,
  enrollmentId: "11111111-1111-4111-8111-111111111111",
  enabled: true,
  continueRunningThreads: false,
};
const input: FleetStageInput = {
  operationId: "22222222-2222-4222-8222-222222222222",
  enrollmentId: enrollment.enrollmentId,
  environmentId,
  expectedInstalledSource: "a".repeat(40),
  targetSource: "b".repeat(40),
};
const installedSource: NonNullable<JonesUpdateState["installedSource"]> = {
  repository: "Jones-Systems/Jones-Code",
  sha: input.expectedInstalledSource,
  tree: "e".repeat(40),
};
const sourceBindingFailures = ["changed", "unknown", "unavailable"] as const;
const failedSourceState = (
  update: JonesUpdateState,
  failure: (typeof sourceBindingFailures)[number],
): JonesUpdateState | null => {
  if (failure === "unavailable") return null;
  const { installedSource: _installedSource, ...withoutSource } = update;
  return failure === "unknown"
    ? withoutSource
    : { ...update, installedSource: { ...installedSource, sha: "d".repeat(40) } };
};

/** A host where nobody has typed for the whole quiet window and nothing is running. */
const quietHost = (): FleetQuietSignals & {
  work: Effect.Effect<HostWorkSnapshot | "unavailable">;
  set: (patch: Partial<{ lastInputAt: number; runningTerminals: number }>) => void;
  setWork: (work: HostWorkSnapshot | "unavailable") => void;
} => {
  let lastInputAt = -QUIET_INPUT_IDLE_MS;
  let runningTerminals = 0;
  let work: HostWorkSnapshot | "unavailable" = {
    activeRuns: 0,
    unheldQueuedRuns: 0,
    processEffects: 0,
    backgroundTasks: 0,
    pendingRequests: [],
  };
  return {
    lastInputAt: Effect.sync(() => lastInputAt),
    runningTerminals: Effect.sync(() => runningTerminals),
    work: Effect.sync(() => work),
    set: (patch) => {
      lastInputAt = patch.lastInputAt ?? lastInputAt;
      runningTerminals = patch.runningTerminals ?? runningTerminals;
    },
    setWork: (next) => {
      work = next;
    },
  };
};

const fixture = (
  options: {
    readonly relation?: Parameters<typeof makeFleetUpdates>[0]["relation"];
    readonly store?: FleetHostStore;
  } = {},
) =>
  Effect.gen(function* () {
    let savedEnrollment: FleetEnrollment | null = null;
    let follower: FleetFollowerRecord | null = null;
    const operations = new Map<string, FleetHostOperation>();
    const store: FleetHostStore = options.store ?? {
      readEnrollment: async () => savedEnrollment,
      editEnrollment: async (change) => (savedEnrollment = change(savedEnrollment)),
      readOperation: async (id) => operations.get(id) ?? null,
      editOperation: async (id, change) => {
        const next = change(operations.get(id) ?? null);
        operations.set(id, next);
        return next;
      },
      readFollower: async () => follower,
      readConfirmedFollower: async () => follower,
      withConfirmedOperationRoute: (admit) => admit(savedEnrollment, follower),
      editFollower: async (change) => (follower = change(follower)),
      transactDirective: async (change) => {
        const { record, result } = change(follower, savedEnrollment);
        follower = record;
        return result;
      },
    };
    const quiet = quietHost();
    let update: JonesUpdateState = {
      source: "jones-actions",
      channel: "jones-main",
      phase: "available",
      capability: { check: true, download: true, install: true },
      currentVersion: "old-version",
      environmentId,
      installedSource,
    };
    let native: Effect.Success<ReturnType<FleetHostUpdater["reconcileOperation"]>> = {
      state: "absent",
      operationId: input.operationId,
    };
    const install = vi.fn<FleetHostUpdater["installForOperation"]>((request) =>
      Effect.sync(() => {
        expect(operations.get(request.operationId)).toMatchObject({
          phase: "dispatching",
          input: {
            operationId: request.operationId,
            environmentId: request.environmentId,
            expectedInstalledSource: request.expectedInstalledSource,
            targetSource: request.targetSource,
          },
          currentVersion: request.currentVersion,
          stagedHandle: request.stagedHandle,
        });
        native = { state: "pending", operationId: request.operationId, binding: request };
        return { ...update, phase: "installing" };
      }),
    );
    const retireStaged = vi.fn<FleetHostUpdater["retireStagedOperation"]>(() =>
      Effect.succeed({ retired: true }),
    );
    const refreshPolicy = vi.fn(() => undefined);
    const updates: FleetHostUpdater = {
      fleetOperationsSupported: true,
      state: () => Effect.succeed(update),
      stageExact: ({ targetSource }) =>
        Effect.sync(
          () =>
            (update = {
              ...update,
              phase: "staged",
              stagedHandle: "staged-handle",
              provenance: {
                repository: "Jones-Systems/Jones-Code",
                sourceSha: targetSource,
                sourceTree: "c".repeat(40),
                workflow: "main",
                runId: 1,
                runAttempt: 1,
                artifactId: 2,
                artifactDigest: "digest",
                platform: "linux",
                architecture: "x64",
              },
            }),
        ),
      retireStagedOperation: retireStaged,
      installForOperation: install,
      reconcileOperation: () => Effect.sync(() => native),
      refreshPolicy: Effect.sync(refreshPolicy),
    };
    let nextOperation = 0;
    const create = () =>
      makeFleetUpdates({
        environmentId,
        serviceMode: true,
        store,
        updates,
        quiet,
        ...(options.relation === undefined ? {} : { relation: options.relation }),
        operationId: () => `33333333-3333-4333-8333-${String(++nextOperation).padStart(12, "0")}`,
      });
    return {
      service: yield* create(),
      create,
      refreshPolicy,
      install,
      operations,
      store,
      updates,
      quiet,
      retireStaged,
      follower: () => follower,
      setFollower: (next: FleetFollowerRecord | null) => {
        follower = next;
      },
      setUpdate: (next: JonesUpdateState) => {
        update = next;
      },
      snapshot: () => update,
      setInstalledSource: (installedSource: JonesUpdateState["installedSource"]) => {
        const { installedSource: _previous, ...rest } = update;
        update = installedSource === undefined ? rest : { ...rest, installedSource };
      },
      setNative: (value: typeof native) => {
        native = value;
      },
    };
  });

describe("fleet host coordination", () => {
  it.effect("rejects staging when the installed source SHA changed or is unavailable", () =>
    Effect.gen(function* () {
      for (const sha of ["d".repeat(40), undefined]) {
        const f = yield* fixture();
        yield* f.service.enroll({ enrollment, expectedEnrollmentId: null });
        const stage = vi.spyOn(f.updates, "stageExact");
        f.setInstalledSource(
          sha === undefined ? undefined : { ...f.snapshot().installedSource!, sha },
        );
        expect((yield* f.service.stage(input).pipe(Effect.flip)).reason).toBe("binding");
        expect(stage).not.toHaveBeenCalled();
        expect(f.operations.has(input.operationId)).toBe(false);
        expect(f.install).not.toHaveBeenCalled();
      }
    }),
  );

  it.effect("requires explicit enrollment and rejects changed immutable operation bindings", () =>
    Effect.gen(function* () {
      const f = yield* fixture();
      expect((yield* f.service.stage(input).pipe(Effect.flip)).message).toContain("not accepted");
      yield* f.service.enroll({ enrollment, expectedEnrollmentId: null });
      const staged = yield* f.service.stage(input);
      expect(staged.operation).toMatchObject({ phase: "staged", input });
      expect(staged.update?.installedSource).toEqual(installedSource);
      expect(
        (yield* f.service.stage({ ...input, targetSource: "d".repeat(40) }).pipe(Effect.flip))
          .message,
      ).toContain("another update");
      expect(f.install).not.toHaveBeenCalled();
    }),
  );

  it.effect.each(sourceBindingFailures)(
    "refuses staging when installed source is %s without reserving or staging an operation",
    (failure) =>
      Effect.gen(function* () {
        const f = yield* fixture();
        yield* f.service.enroll({ enrollment, expectedEnrollmentId: null });
        vi.spyOn(f.updates, "state").mockReturnValue(
          Effect.succeed(failedSourceState(f.snapshot(), failure)),
        );
        const stage = vi.spyOn(f.updates, "stageExact");
        expect((yield* f.service.stage(input).pipe(Effect.flip)).reason).toBe("binding");
        expect(f.operations.size).toBe(0);
        expect(stage).not.toHaveBeenCalled();
        expect(f.install).not.toHaveBeenCalled();
      }),
  );

  it.effect(
    "persists dispatch before IPC and reconciles duplicates across coordinator restart",
    () =>
      Effect.gen(function* () {
        const f = yield* fixture();
        yield* f.service.enroll({ enrollment, expectedEnrollmentId: null });
        yield* f.service.stage(input);
        const activate = {
          environmentId,
          enrollmentId: enrollment.enrollmentId,
          operationId: input.operationId,
        };
        const result = yield* f.service.activate(activate);
        expect(result.operation?.phase).toBe("pending");
        const restarted = yield* f.create();
        yield* restarted.activate(activate);
        expect(f.install).toHaveBeenCalledTimes(1);
      }),
  );

  it.effect("replays authoritative absence but stops on a mismatched native receipt", () =>
    Effect.gen(function* () {
      for (const changed of [false, true]) {
        const f = yield* fixture();
        yield* f.service.enroll({ enrollment, expectedEnrollmentId: null });
        yield* f.service.stage(input);
        f.operations.set(input.operationId, {
          ...f.operations.get(input.operationId)!,
          phase: "dispatching",
        });
        if (changed)
          f.setNative({
            state: "pending",
            operationId: input.operationId,
            binding: {
              environmentId,
              currentVersion: "wrong",
              expectedInstalledSource: input.expectedInstalledSource,
              targetSource: input.targetSource,
              stagedHandle: "staged-handle",
            },
          });
        const result = yield* f.service.activate({
          environmentId,
          enrollmentId: enrollment.enrollmentId,
          operationId: input.operationId,
        });
        expect(result.operation?.phase).toBe(changed ? "reconciling" : "pending");
        expect(f.install).toHaveBeenCalledTimes(changed ? 0 : 1);
      }
    }),
  );
});

describe("fleet failure and retirement reconciliation", () => {
  it.effect("recovers an unrecorded stage only while its installed source SHA still matches", () =>
    Effect.gen(function* () {
      for (const sha of [input.expectedInstalledSource, "d".repeat(40), undefined]) {
        const f = yield* fixture();
        yield* f.service.enroll({ enrollment, expectedEnrollmentId: null });
        yield* f.service.stage(input);
        const { stagedHandle, ...unrecorded } = f.operations.get(input.operationId)!;
        f.operations.set(input.operationId, { ...unrecorded, phase: "staging" });
        f.setInstalledSource(
          sha === undefined ? undefined : { ...f.snapshot().installedSource!, sha },
        );
        const retire = vi.spyOn(f.updates, "retireStagedOperation");
        if (sha === input.expectedInstalledSource) {
          expect((yield* f.service.retire(input)).operation?.phase).toBe("superseded");
          expect(retire).toHaveBeenCalledTimes(1);
          expect(retire).toHaveBeenCalledWith({
            ...input,
            currentVersion: unrecorded.currentVersion,
            stagedHandle,
          });
        } else {
          expect((yield* f.service.retire(input).pipe(Effect.flip)).reason).toBe("binding");
          expect(retire).not.toHaveBeenCalled();
          expect(f.operations.get(input.operationId)?.phase).toBe("staging");
        }
        expect(f.install).not.toHaveBeenCalled();
      }
    }),
  );

  it.effect("does not retry a refused install after installed source identity is lost", () =>
    Effect.gen(function* () {
      for (const sha of ["d".repeat(40), undefined]) {
        const f = yield* fixture();
        yield* f.service.enroll({ enrollment, expectedEnrollmentId: null });
        yield* f.service.stage(input);
        f.install.mockImplementation(() =>
          Effect.sync(() => {
            f.setInstalledSource(
              sha === undefined ? undefined : { ...f.snapshot().installedSource!, sha },
            );
            return {
              ...f.snapshot(),
              phase: "error",
              message: "Installed source changed during preparation.",
            };
          }),
        );
        expect((yield* f.service.activate(input)).operation).toMatchObject({
          phase: "blocked",
          reason: "Installed source changed during preparation.",
        });
        expect((yield* f.service.activate(input)).operation?.phase).toBe("blocked");
        expect(f.install).toHaveBeenCalledTimes(1);
      }
    }),
  );

  it.effect(
    "observes a reservation gap through pending to committed without repeating installation",
    () =>
      Effect.gen(function* () {
        const f = yield* fixture();
        yield* f.service.enroll({ enrollment, expectedEnrollmentId: null });
        yield* f.service.stage(input);
        const selected = f.operations.get(input.operationId)!;
        f.operations.set(input.operationId, { ...selected, phase: "dispatching" });
        const binding = {
          environmentId,
          currentVersion: selected.currentVersion,
          expectedInstalledSource: input.expectedInstalledSource,
          targetSource: input.targetSource,
          stagedHandle: selected.stagedHandle!,
        };
        f.setNative({
          state: "blocked",
          operationId: input.operationId,
          binding,
          reason: "reservation precedes native state",
        });
        expect((yield* f.service.status(input.operationId)).operation?.phase).toBe("reconciling");
        f.setNative({ state: "absent", operationId: input.operationId });
        expect((yield* f.service.activate(input)).operation?.phase).toBe("reconciling");
        f.setNative({
          state: "pending",
          operationId: input.operationId,
          binding,
          updateId: input.operationId,
        });
        expect((yield* f.service.status(input.operationId)).operation?.phase).toBe("pending");
        f.setNative({
          state: "committed",
          operationId: input.operationId,
          binding,
          updateId: input.operationId,
        });
        expect((yield* f.service.status(input.operationId)).operation?.phase).toBe("committed");
        expect(f.install).not.toHaveBeenCalled();
      }),
  );

  it.effect("ends a reconciling operation the launcher proved rejected without reinstalling", () =>
    Effect.gen(function* () {
      const f = yield* fixture();
      yield* f.service.enroll({ enrollment, expectedEnrollmentId: null });
      yield* f.service.stage(input);
      const selected = f.operations.get(input.operationId)!;
      f.operations.set(input.operationId, { ...selected, phase: "dispatching" });
      const binding = {
        environmentId,
        currentVersion: selected.currentVersion,
        expectedInstalledSource: input.expectedInstalledSource,
        targetSource: input.targetSource,
        stagedHandle: selected.stagedHandle!,
      };
      f.setNative({
        state: "blocked",
        operationId: input.operationId,
        binding,
        reason: "reservation precedes native state",
      });
      expect((yield* f.service.status(input.operationId)).operation?.phase).toBe("reconciling");
      // A rejection bound to another installation proves nothing about this one.
      f.setNative({
        state: "rejected",
        operationId: input.operationId,
        binding: { ...binding, targetSource: "0".repeat(40) },
      });
      expect((yield* f.service.status(input.operationId)).operation?.phase).toBe("reconciling");
      f.setNative({
        state: "rejected",
        operationId: input.operationId,
        binding,
        reason: "The launcher finished this request without accepting it.",
      });
      const ended = (yield* f.service.status(input.operationId)).operation;
      expect(ended?.phase).toBe("blocked");
      expect(ended?.reason).toBe("The launcher finished this request without accepting it.");
      expect((yield* f.service.activate(input)).operation?.phase).toBe("blocked");
      expect(f.install).not.toHaveBeenCalled();
    }),
  );

  it.effect("retains a preacceptance refusal reason and safely retries only native absence", () =>
    Effect.gen(function* () {
      const f = yield* fixture();
      yield* f.service.enroll({ enrollment, expectedEnrollmentId: null });
      yield* f.service.stage(input);
      f.install.mockImplementation(() =>
        Effect.succeed({
          ...f.snapshot(),
          phase: "error",
          message: "Insufficient capacity for the retained backup.",
        }),
      );
      const first = yield* f.service.activate(input);
      expect(first.operation).toMatchObject({
        phase: "install-blocked",
        reason: "Insufficient capacity for the retained backup.",
      });
      expect((yield* f.service.status(input.operationId)).operation?.reason).toContain("capacity");
      // The retry is another automatic activation, so it waits out the spacing first.
      const early = yield* f.service.activate(input);
      expect(early.operation).toMatchObject({
        phase: "install-blocked",
        reason: "The previous automatic update was too recent.",
      });
      expect(f.install).toHaveBeenCalledTimes(1);
      yield* TestClock.adjust(AUTOMATIC_ACTIVATION_SPACING_MS);
      yield* f.service.activate(input);
      expect(f.install).toHaveBeenCalledTimes(2);
    }),
  );

  it.effect.each(sourceBindingFailures)(
    "makes preacceptance refusal terminal when installed source becomes %s",
    (failure) =>
      Effect.gen(function* () {
        const f = yield* fixture();
        yield* f.service.enroll({ enrollment, expectedEnrollmentId: null });
        yield* f.service.stage(input);
        f.install.mockImplementation(() =>
          Effect.sync(() => {
            vi.spyOn(f.updates, "state").mockReturnValue(
              Effect.succeed(failedSourceState(f.snapshot(), failure)),
            );
            return { ...f.snapshot(), phase: "error", message: "Preparation refused." };
          }),
        );
        const result = yield* f.service.activate(input);
        expect(result.operation).toMatchObject({
          phase: "blocked",
          reason: "Preparation refused.",
        });
        expect((yield* f.service.activate(input)).operation?.phase).toBe("blocked");
        expect(f.install).toHaveBeenCalledTimes(1);
      }),
  );

  it.effect.each(["staging-receipt", "missing-receipt"] as const)(
    "recovers and retires the exact structured-source slot after a crash with %s",
    (receipt) =>
      Effect.gen(function* () {
        const f = yield* fixture();
        yield* f.service.enroll({ enrollment, expectedEnrollmentId: null });
        if (receipt === "staging-receipt")
          f.operations.set(input.operationId, {
            input,
            currentVersion: f.snapshot().currentVersion!,
            continueRunningThreads: enrollment.continueRunningThreads,
            phase: "staging",
          });
        yield* f.updates.stageExact({
          targetSource: input.targetSource,
          operationId: input.operationId,
        });
        const retire = vi.spyOn(f.updates, "retireStagedOperation");
        const restarted = yield* f.create();
        expect((yield* restarted.retire(input)).operation?.phase).toBe("superseded");
        expect(retire).toHaveBeenCalledExactlyOnceWith({
          ...input,
          currentVersion: "old-version",
          stagedHandle: "staged-handle",
        });
        expect((yield* restarted.stage(input)).operation?.phase).toBe("superseded");
        expect(f.install).not.toHaveBeenCalled();
      }),
  );

  it.effect.each(sourceBindingFailures)(
    "preserves the recovered native slot when installed source is %s",
    (failure) =>
      Effect.gen(function* () {
        const f = yield* fixture();
        yield* f.service.enroll({ enrollment, expectedEnrollmentId: null });
        yield* f.updates.stageExact({
          targetSource: input.targetSource,
          operationId: input.operationId,
        });
        vi.spyOn(f.updates, "state").mockReturnValue(
          Effect.succeed(failedSourceState(f.snapshot(), failure)),
        );
        const retire = vi.spyOn(f.updates, "retireStagedOperation");
        const restarted = yield* f.create();
        expect((yield* restarted.retire(input).pipe(Effect.flip)).reason).toBe(
          failure === "unavailable" ? "bootstrap-required" : "binding",
        );
        expect(f.operations.size).toBe(0);
        expect(retire).not.toHaveBeenCalled();
        expect(f.install).not.toHaveBeenCalled();
      }),
  );

  it.effect("retires only an unaccepted operation and tombstones a delayed old stage request", () =>
    Effect.gen(function* () {
      const f = yield* fixture();
      yield* f.service.enroll({ enrollment, expectedEnrollmentId: null });
      yield* f.service.stage(input);
      expect((yield* f.service.retire(input)).operation?.phase).toBe("superseded");
      expect((yield* f.service.stage(input)).operation?.phase).toBe("superseded");
      const next = {
        ...input,
        operationId: "33333333-3333-4333-8333-333333333333",
        targetSource: "c".repeat(40),
      };
      expect((yield* f.service.stage(next)).operation?.phase).toBe("staged");
      expect(f.install).not.toHaveBeenCalled();
    }),
  );
});

it.effect(
  "keeps status readable without interpreting its own reservation-before-state interval",
  () =>
    Effect.gen(function* () {
      const f = yield* fixture();
      yield* f.service.enroll({ enrollment, expectedEnrollmentId: null });
      yield* f.service.stage(input);
      const started = yield* Deferred.make<void>();
      const waiting = yield* Deferred.make<void>();
      const selected = f.operations.get(input.operationId)!;
      const binding = {
        environmentId,
        currentVersion: selected.currentVersion,
        expectedInstalledSource: input.expectedInstalledSource,
        targetSource: input.targetSource,
        stagedHandle: selected.stagedHandle!,
      };
      f.install.mockImplementation(() =>
        Effect.gen(function* () {
          f.setNative({
            state: "blocked",
            operationId: input.operationId,
            binding,
            reason: "reservation before pending state",
          });
          yield* Deferred.succeed(started, undefined);
          yield* Deferred.await(waiting);
          f.setNative({
            state: "pending",
            operationId: input.operationId,
            binding,
            updateId: input.operationId,
          });
          return { ...f.snapshot(), phase: "installing" as const };
        }),
      );
      const installing = yield* f.service.activate(input).pipe(Effect.forkScoped);
      yield* Effect.gen(function* () {
        yield* Deferred.await(started);
        expect((yield* f.service.status(input.operationId)).operation?.phase).toBe("dispatching");
      }).pipe(Effect.ensuring(Deferred.succeed(waiting, undefined)));
      expect((yield* Fiber.join(installing)).operation?.phase).toBe("pending");
    }).pipe(Effect.scoped),
);

const isoAt = (millis: number) => DateTime.formatIso(DateTime.makeUnsafe(millis));
const idleWork: HostWorkSnapshot = {
  activeRuns: 0,
  unheldQueuedRuns: 0,
  processEffects: 0,
  backgroundTasks: 0,
  pendingRequests: [],
};

describe("automatic activation quiet window", () => {
  it.effect(
    "holds a laptop campaign until input is idle and nothing a restart cuts is running",
    () =>
      Effect.gen(function* () {
        const f = yield* fixture();
        yield* f.service.enroll({ enrollment, expectedEnrollmentId: null });
        yield* f.service.stage(input);
        f.quiet.set({ lastInputAt: 0 });
        expect((yield* f.service.activate(input)).operation).toMatchObject({
          phase: "install-blocked",
          reason: expect.stringContaining("30 minutes left"),
        });
        yield* TestClock.adjust(QUIET_INPUT_IDLE_MS);
        f.quiet.set({ runningTerminals: 1 });
        expect((yield* f.service.activate(input)).operation?.reason).toBe(
          "Waiting for terminal commands (1) to finish.",
        );
        f.quiet.set({ runningTerminals: 0 });
        f.quiet.setWork({
          ...idleWork,
          activeRuns: 2,
          pendingRequests: [
            { kind: "user_input", capability: "message" },
            { kind: "command_execution_approval", capability: "live" },
          ],
        });
        expect((yield* f.service.activate(input)).operation?.reason).toBe(
          "Waiting for running turns or delegated tasks (2), approvals waiting in a live provider session (1) to finish.",
        );
        expect(f.install).not.toHaveBeenCalled();
        expect(f.follower()?.lastAutomaticActivationAt ?? null).toBeNull();
        // A question answerable by message survives the restart, so it does not hold the update.
        f.quiet.setWork({
          ...idleWork,
          pendingRequests: [{ kind: "user_input", capability: "message" }],
        });
        expect((yield* f.service.activate(input)).operation?.phase).toBe("pending");
        expect(f.install).toHaveBeenCalledTimes(1);
        expect(f.follower()?.lastAutomaticActivationAt).toBe(isoAt(QUIET_INPUT_IDLE_MS));
      }),
  );

  it.effect("rechecks admission just before the handoff and keeps a withdrawn stage ready", () =>
    Effect.gen(function* () {
      const f = yield* fixture();
      yield* f.service.enroll({ enrollment, expectedEnrollmentId: null });
      yield* f.service.stage(input);
      const admissions: Array<string | undefined> = [];
      f.install.mockImplementation((request) =>
        Effect.gen(function* () {
          admissions.push(yield* Effect.promise(() => request.admit!()));
          f.quiet.set({ lastInputAt: yield* Clock.currentTimeMillis });
          const reason = yield* Effect.promise(() => request.admit!());
          admissions.push(reason);
          return {
            ...f.snapshot(),
            phase: "staged" as const,
            message: `The automatic update was withdrawn before the launcher accepted it: ${reason}`,
          };
        }),
      );
      const withdrawn = yield* f.service.activate(input);
      expect(admissions[0]).toBeUndefined();
      expect(admissions[1]).toContain("30 minutes");
      expect(withdrawn.operation).toMatchObject({
        phase: "install-blocked",
        reason: expect.stringContaining("withdrawn"),
      });
    }),
  );
});

describe("following host", () => {
  const leaderInput = {
    enrollmentId: enrollment.enrollmentId,
    environmentId,
    repository: "Jones-Systems/Jones-Code" as const,
    committedSource: input.targetSource,
    committedTree: "c".repeat(40),
  };
  const pendingInstall = (f: Effect.Success<ReturnType<typeof fixture>>) =>
    f.install.mockImplementation((request) =>
      Effect.sync(() => {
        f.setNative({ state: "pending", operationId: request.operationId, binding: request });
        return { ...f.snapshot(), phase: "installing" as const };
      }),
    );

  it.effect("waits for the laptop, prepares its build at once and activates it when quiet", () =>
    Effect.gen(function* () {
      const f = yield* fixture();
      pendingInstall(f);
      yield* f.service.enroll({ enrollment, expectedEnrollmentId: null });
      expect((yield* f.service.status()).follower).toMatchObject({
        protocol: 1,
        leaderSource: null,
        state: "waiting-for-leader",
      });
      yield* f.service.follow;
      expect(f.snapshot().stagedHandle).toBeUndefined();
      f.quiet.set({ lastInputAt: 0 });
      const acknowledged = yield* f.service.leader(leaderInput);
      expect(acknowledged.follower).toMatchObject({
        leaderSource: input.targetSource,
        leaderPublishedAt: isoAt(0),
      });
      expect((yield* f.service.status()).follower?.leaderPublishedAt).toBe(isoAt(0));
      yield* f.service.leader(leaderInput);
      expect(f.follower()?.leader).toEqual({
        enrollmentId: enrollment.enrollmentId,
        committedSource: input.targetSource,
        committedTree: leaderInput.committedTree,
        publishedAt: isoAt(0),
      });
      yield* f.service.follow;
      expect(f.snapshot().provenance?.sourceSha).toBe(input.targetSource);
      expect((yield* f.service.status()).follower).toMatchObject({
        state: "waiting-for-quiet",
        leaderSource: input.targetSource,
        notBefore: isoAt(QUIET_INPUT_IDLE_MS),
        quiet: { requiredInputIdleMs: QUIET_INPUT_IDLE_MS, blockers: [] },
      });
      expect(f.install).not.toHaveBeenCalled();
      yield* TestClock.adjust(QUIET_INPUT_IDLE_MS);
      yield* f.service.follow;
      expect(f.install).toHaveBeenCalledTimes(1);
      expect(f.install.mock.calls[0]![0]).toMatchObject({
        targetSource: input.targetSource,
        expectedInstalledSource: input.expectedInstalledSource,
        stagedHandle: "staged-handle",
      });
      expect((yield* f.service.status()).follower?.state).toBe("activating");
      yield* f.service.follow;
      expect(f.install).toHaveBeenCalledTimes(1);
    }),
  );

  it.effect("retires another unaccepted stage before preparing the laptop's build", () =>
    Effect.gen(function* () {
      const f = yield* fixture();
      yield* f.service.enroll({ enrollment, expectedEnrollmentId: null });
      yield* f.service.stage(input);
      f.retireStaged.mockImplementation(() =>
        Effect.sync(() => {
          const { stagedHandle: _handle, provenance: _provenance, ...rest } = f.snapshot();
          f.setUpdate({ ...rest, phase: "no-new" });
          return { retired: true };
        }),
      );
      f.quiet.set({ lastInputAt: 0 });
      yield* f.service.leader({ ...leaderInput, committedSource: "c".repeat(40) });
      yield* f.service.follow;
      expect(f.retireStaged).toHaveBeenCalledTimes(1);
      expect(f.retireStaged.mock.calls[0]![0]).toMatchObject({
        targetSource: input.targetSource,
        stagedHandle: "staged-handle",
        expectedInstalledSource: input.expectedInstalledSource,
      });
      expect(f.snapshot().provenance?.sourceSha).toBe("c".repeat(40));
      expect((yield* f.service.status()).follower?.state).toBe("waiting-for-quiet");
    }),
  );

  it.effect("stays idle on the laptop's build and backs off a failed preparation", () =>
    Effect.gen(function* () {
      const f = yield* fixture();
      yield* f.service.enroll({ enrollment, expectedEnrollmentId: null });
      yield* f.service.leader({ ...leaderInput, committedSource: input.expectedInstalledSource });
      yield* f.service.follow;
      expect((yield* f.service.status()).follower?.state).toBe("current");
      const stageExact = vi
        .spyOn(f.updates, "stageExact")
        .mockReturnValue(
          Effect.succeed({ ...f.snapshot(), phase: "blocked", message: "Not qualified yet." }),
        );
      yield* f.service.leader(leaderInput);
      yield* f.service.follow;
      expect((yield* f.service.status()).follower).toMatchObject({
        state: "prepare-blocked",
        reason: "Not qualified yet.",
      });
      yield* f.service.follow;
      expect((yield* f.service.status()).follower).toMatchObject({
        state: "prepare-blocked",
        notBefore: isoAt(FOLLOWER_STAGE_RETRY_MS),
      });
      expect(stageExact).toHaveBeenCalledTimes(1);
      yield* TestClock.adjust(FOLLOWER_STAGE_RETRY_MS);
      yield* f.service.follow;
      expect(stageExact).toHaveBeenCalledTimes(2);
    }),
  );

  it.effect("accepts a laptop's source only from the enrolled laptop", () =>
    Effect.gen(function* () {
      const f = yield* fixture();
      yield* f.service.enroll({ enrollment, expectedEnrollmentId: null });
      const refused = yield* f.service
        .leader({ ...leaderInput, enrollmentId: "44444444-4444-4444-8444-444444444444" })
        .pipe(Effect.flip);
      expect(refused.reason).toBe("not-enrolled");
      expect(f.follower()).toBeNull();
    }),
  );

  it.effect("treats a laptop report recorded without its source tree as no report", () =>
    Effect.gen(function* () {
      const f = yield* fixture();
      const stageExact = vi.spyOn(f.updates, "stageExact");
      yield* f.service.enroll({ enrollment, expectedEnrollmentId: null });
      f.quiet.set({ lastInputAt: 0 });
      f.setFollower({
        schema: 1,
        leader: {
          enrollmentId: enrollment.enrollmentId,
          committedSource: input.targetSource,
          publishedAt: isoAt(0),
        },
        operation: null,
        lastAutomaticActivationAt: null,
      });
      yield* f.service.follow;
      expect((yield* f.service.status()).follower).toMatchObject({
        leaderSource: null,
        state: "waiting-for-leader",
      });
      expect(stageExact).not.toHaveBeenCalled();
      // The laptop's next report carries its tree and replaces the record.
      yield* f.service.leader(leaderInput);
      expect(f.follower()?.leader?.committedTree).toBe(leaderInput.committedTree);
      yield* f.service.follow;
      expect(stageExact).toHaveBeenCalledTimes(1);
    }),
  );

  it.effect("holds a host already installed ahead of the laptop instead of going back", () =>
    Effect.gen(function* () {
      const relations: Array<{ base: string; head: string }> = [];
      const f = yield* fixture({
        relation: (pair) =>
          Effect.sync(() => {
            relations.push(pair);
            return {
              repository: "Jones-Systems/Jones-Code" as const,
              ...pair,
              relation: "ahead" as const,
              observedAt: isoAt(0),
            };
          }),
      });
      const stageExact = vi.spyOn(f.updates, "stageExact");
      yield* f.service.enroll({ enrollment, expectedEnrollmentId: null });
      yield* f.service.leader(leaderInput);
      yield* f.service.follow;
      expect(relations).toEqual([
        { base: input.targetSource, head: input.expectedInstalledSource },
      ]);
      expect((yield* f.service.status()).follower).toMatchObject({
        state: "waiting-for-leader",
        reason: expect.stringContaining("newer build than the last one the laptop reported"),
      });
      expect(stageExact).not.toHaveBeenCalled();
      expect(f.install).not.toHaveBeenCalled();
    }),
  );
});

it.effect("backs off a refused retirement and retries it under the same identity", () =>
  Effect.gen(function* () {
    const f = yield* fixture();
    yield* f.service.enroll({ enrollment, expectedEnrollmentId: null });
    yield* f.service.stage(input);
    f.retireStaged.mockReturnValue(
      Effect.succeed({ retired: false, reason: "An update operation is still active." }),
    );
    yield* f.service.leader({
      enrollmentId: enrollment.enrollmentId,
      environmentId,
      repository: "Jones-Systems/Jones-Code",
      committedSource: "c".repeat(40),
      committedTree: "d".repeat(40),
    });
    yield* f.service.follow;
    yield* f.service.follow;
    expect(f.retireStaged).toHaveBeenCalledTimes(1);
    expect((yield* f.service.status()).follower).toMatchObject({
      state: "prepare-blocked",
      notBefore: DateTime.formatIso(DateTime.makeUnsafe(FOLLOWER_STAGE_RETRY_MS)),
    });
    yield* TestClock.adjust(FOLLOWER_STAGE_RETRY_MS);
    yield* f.service.follow;
    expect(f.retireStaged).toHaveBeenCalledTimes(2);
    expect(f.retireStaged.mock.calls[1]![0].operationId).toBe(
      f.retireStaged.mock.calls[0]![0].operationId,
    );
    expect(f.snapshot().provenance?.sourceSha).toBe(input.targetSource);
  }),
);

describe("plan-managed follower targets", () => {
  const directive = {
    directiveId: "55555555-5555-4555-8555-555555555555",
    enrollmentId: enrollment.enrollmentId,
    environmentId,
    repository: "Jones-Systems/Jones-Code" as const,
    targetSource: input.targetSource,
    targetTree: "c".repeat(40),
    expectedDirectiveId: null,
  };
  const replacementEnrollment = {
    ...enrollment,
    enrollmentId: "99999999-9999-4999-8999-999999999999",
  };
  it.effect(
    "fences a delayed setup before it arrives, keeps current policy, and retains the exact fence after restart",
    () =>
      Effect.gen(function* () {
        const f = yield* fixture();
        yield* f.service.enroll({ enrollment, expectedEnrollmentId: null });
        yield* f.service.planDirective(directive);
        const captured = {
          ...directive,
          directiveId: "66666666-6666-4666-8666-666666666666",
          targetTree: "d".repeat(40),
          expectedDirectiveId: directive.directiveId,
        };
        const policy = f.follower()!.directive;
        const installed = f.snapshot().installedSource;
        expect((yield* f.service.status()).fencePlanDirective).toBe(1);
        expect((yield* f.service.releaseDirective(captured).pipe(Effect.flip)).reason).toBe(
          "conflict",
        );
        yield* f.service.fenceDirective(captured); // Lose the first reply.
        const restarted = yield* f.create();
        expect((yield* restarted.fenceDirective(captured)).directiveFence).toEqual({
          protocol: 1,
          input: captured,
          outcome: "fenced",
        });
        expect(
          (yield* restarted
            .fenceDirective({ ...captured, expectedDirectiveId: null })
            .pipe(Effect.flip)).reason,
        ).toBe("conflict");
        expect((yield* restarted.planDirective(captured).pipe(Effect.flip)).reason).toBe(
          "conflict",
        );
        expect((yield* restarted.releaseDirective(captured).pipe(Effect.flip)).reason).toBe(
          "conflict",
        );
        expect(f.follower()?.directive).toEqual(policy);
        expect(f.snapshot().installedSource).toEqual(installed);
        expect(f.install).not.toHaveBeenCalled();
        const later = { ...captured, directiveId: "77777777-7777-4777-8777-777777777777" };
        expect((yield* restarted.planDirective(later)).follower?.planDirective?.directiveId).toBe(
          later.directiveId,
        );
      }),
  );

  it.effect(
    "publishes only confirmed setup status and refreshes policy after an exact apply retry",
    () =>
      Effect.gen(function* () {
        const f = yield* fixture();
        yield* f.service.enroll({ enrollment, expectedEnrollmentId: null });
        yield* f.service.planDirective(directive);
        const retained = f.follower();
        const readConfirmedFollower = vi
          .spyOn(f.store, "readConfirmedFollower")
          .mockRejectedValueOnce(new Error("Directory sync refused."));
        expect((yield* f.service.status().pipe(Effect.flip)).reason).toBe("storage");
        expect(readConfirmedFollower).toHaveBeenCalledOnce();
        f.refreshPolicy.mockClear();
        const replay = yield* f.service.planDirective(directive);
        expect(replay.follower?.planDirective?.directiveId).toBe(directive.directiveId);
        expect(f.refreshPolicy).toHaveBeenCalledOnce();
        expect(f.follower()).toBe(retained);
      }),
  );

  it.effect("durably denies Round 1 replay after fencing and restoring an old enrollment", () =>
    Effect.scoped(
      Effect.gen(function* () {
        const fs = yield* FileSystem.FileSystem;
        const path = yield* Path.Path;
        const home = yield* fs.makeTempDirectoryScoped({ prefix: "jones-fence-rollback-" });
        const store = createFleetHostStore(home);
        const f = yield* fixture({ store });
        yield* f.service.enroll({ enrollment, expectedEnrollmentId: null });
        yield* f.service.planDirective(directive);
        const delayed = {
          ...directive,
          directiveId: "66666666-6666-4666-8666-666666666666",
          expectedDirectiveId: directive.directiveId,
        };
        yield* f.service.enroll({
          enrollment: replacementEnrollment,
          expectedEnrollmentId: enrollment.enrollmentId,
        });
        yield* f.service.leader({
          environmentId,
          enrollmentId: replacementEnrollment.enrollmentId,
          repository: directive.repository,
          committedSource: directive.targetSource,
          committedTree: directive.targetTree,
        });
        const policy = (yield* f.service.status()).follower;
        f.refreshPolicy.mockClear();

        // Round 1 (5cd471f61e44) reads the same journal with no fencedDirectives field in its schema.
        const { fencedDirectives: _unsupported, ...roundOneFields } = FleetFollowerRecord.fields;
        const isRoundOneFollower = Schema.is(Schema.Struct(roundOneFields));
        const journal = path.join(home, "runtime", "jones-fleet", "follower.json");
        const before = yield* Effect.promise(() => read(journal, isRoundOneFollower));
        expect(before).not.toBeNull();
        expect(before?.directive?.directiveId).toBe(delayed.expectedDirectiveId);
        expect(before?.releasedDirectives?.includes(delayed.directiveId) ?? false).toBe(false);
        const fenced = yield* f.service.fenceDirective(delayed);
        const evidence = { protocol: 1, input: delayed, outcome: "fenced" };
        expect(fenced.directiveFence).toEqual(evidence);
        expect(fenced.follower).toEqual(policy);
        expect(fenced.enrollment).toEqual(replacementEnrollment);
        expect(f.refreshPolicy).not.toHaveBeenCalled();
        expect(f.install).not.toHaveBeenCalled();
        const bytes = yield* fs.readFileString(journal);
        const after = yield* Effect.promise(() => read(journal, isRoundOneFollower));
        expect(after).toMatchObject({ ...before, releasedDirectives: [delayed.directiveId] });

        const saved = yield* fs.stat(journal);
        const restarted = yield* fixture({ store: createFleetHostStore(home) });
        expect((yield* restarted.service.fenceDirective(delayed)).directiveFence).toEqual(evidence);
        expect(
          (yield* restarted.service
            .fenceDirective({ ...delayed, targetTree: "d".repeat(40) })
            .pipe(Effect.flip)).reason,
        ).toBe("conflict");
        expect(Option.getOrThrow((yield* fs.stat(journal)).ino)).toBe(Option.getOrThrow(saved.ino));
        yield* restarted.service.enroll({
          enrollment,
          expectedEnrollmentId: replacementEnrollment.enrollmentId,
        });
        expect((yield* restarted.service.status()).follower?.planDirective?.directiveId).toBe(
          delayed.expectedDirectiveId,
        );
        expect((yield* restarted.service.planDirective(delayed).pipe(Effect.flip)).reason).toBe(
          "conflict",
        );

        // A delayed request that passed the initial read is still refused by the Round 1 CAS.
        yield* Effect.promise(() =>
          expect(
            store.editFollower((old) => {
              if (
                (old?.directive?.directiveId ?? null) !== delayed.expectedDirectiveId ||
                old?.releasedDirectives?.includes(delayed.directiveId)
              )
                throw new Error("The update-plan directive changed before it could be saved.");
              return old!;
            }),
          ).rejects.toThrow("The update-plan directive changed before it could be saved."),
        );
        expect(yield* fs.readFileString(journal)).toBe(bytes);
      }),
    ).pipe(Effect.provide(NodeServices.layer)),
  );

  it.effect("upgrades an exact fence-only record and refuses an ambiguous legacy release", () =>
    Effect.gen(function* () {
      const f = yield* fixture();
      yield* f.service.enroll({ enrollment, expectedEnrollmentId: null });
      yield* f.service.planDirective(directive);
      const delayed = {
        ...directive,
        directiveId: "66666666-6666-4666-8666-666666666666",
        expectedDirectiveId: directive.directiveId,
      };
      yield* f.service.fenceDirective(delayed);
      const { releasedDirectives: _pairedDenial, ...record } = f.follower()!;
      const { directiveId, bindingHash } = record.fencedDirectives![0]!;
      const fenceOnly = { ...record, fencedDirectives: [{ directiveId, bindingHash }] };
      const ambiguous = { ...fenceOnly, releasedDirectives: [directiveId] };
      f.setFollower(ambiguous);
      expect((yield* f.service.fenceDirective(delayed).pipe(Effect.flip)).reason).toBe("conflict");
      expect(f.follower()).toBe(ambiguous);

      f.setFollower(fenceOnly);
      expect((yield* f.service.fenceDirective(delayed)).directiveFence?.outcome).toBe("fenced");
      expect(f.follower()?.releasedDirectives).toEqual([directiveId]);
      expect(f.follower()?.directive).toEqual(record.directive);
      expect((yield* f.service.fenceDirective(delayed)).directiveFence?.outcome).toBe("fenced");
    }),
  );

  it.effect("uses exact applied and superseded evidence over a contradictory old fence", () =>
    Effect.gen(function* () {
      const f = yield* fixture();
      yield* f.service.enroll({ enrollment, expectedEnrollmentId: null });
      yield* f.service.fenceDirective(directive);
      const { directiveId, bindingHash } = f.follower()!.fencedDirectives![0]!;
      f.setFollower(null);
      yield* f.service.planDirective(directive);
      // The preceding writer could apply this request while a fence-only record existed.
      const active = { ...f.follower()!, fencedDirectives: [{ directiveId, bindingHash }] };
      f.setFollower(active);
      expect((yield* f.service.fenceDirective(directive)).directiveFence).toMatchObject({
        outcome: "applied",
        directive: active.directive,
      });
      expect(f.follower()).toBe(active);
      const successor = {
        ...directive,
        directiveId: "66666666-6666-4666-8666-666666666666",
        expectedDirectiveId: directive.directiveId,
      };
      yield* f.service.planDirective(successor);
      const superseded = f.follower();
      expect((yield* f.service.fenceDirective(directive)).directiveFence?.outcome).toBe(
        "superseded",
      );
      expect(f.follower()).toBe(superseded);
    }),
  );

  it.effect("returns exact applied or superseded setup evidence after a lost send reply", () =>
    Effect.gen(function* () {
      const f = yield* fixture();
      yield* f.service.enroll({ enrollment, expectedEnrollmentId: null });
      yield* f.service.planDirective(directive); // Lose the reply after the host applied the grant.
      const restarted = yield* f.create();
      const before = f.follower();
      const applied = yield* restarted.fenceDirective(directive);
      expect(applied.directiveFence).toMatchObject({
        protocol: 1,
        input: directive,
        outcome: "applied",
        directive: { directiveId: directive.directiveId, targetTree: directive.targetTree },
      });
      expect(f.follower()).toBe(before);
      const successor = {
        ...directive,
        directiveId: "66666666-6666-4666-8666-666666666666",
        expectedDirectiveId: directive.directiveId,
      };
      yield* restarted.planDirective(successor);
      const after = f.follower();
      expect((yield* restarted.fenceDirective(directive)).directiveFence).toEqual({
        protocol: 1,
        input: directive,
        outcome: "superseded",
      });
      expect(
        (yield* restarted
          .fenceDirective({ ...directive, targetTree: "d".repeat(40) })
          .pipe(Effect.flip)).reason,
      ).toBe("conflict");
      expect(
        (yield* restarted
          .planDirective({ ...directive, expectedDirectiveId: successor.directiveId })
          .pipe(Effect.flip)).reason,
      ).toBe("conflict");
      expect(f.follower()).toBe(after);
      expect(f.snapshot().installedSource).toEqual(installedSource);
      expect(f.install).not.toHaveBeenCalled();
    }),
  );

  it.effect("recovers exact applied and superseded setup across same-host re-enrollment", () =>
    Effect.gen(function* () {
      const f = yield* fixture();
      yield* f.service.enroll({ enrollment, expectedEnrollmentId: null });
      yield* f.service.planDirective(directive);
      const successor = {
        ...directive,
        directiveId: "66666666-6666-4666-8666-666666666666",
        expectedDirectiveId: directive.directiveId,
      };
      yield* f.service.planDirective(successor);
      yield* f.service.enroll({
        enrollment: replacementEnrollment,
        expectedEnrollmentId: enrollment.enrollmentId,
      });
      const before = f.follower();
      const recovered = yield* f.service.fenceDirective(successor);
      expect(recovered.directiveFence).toEqual({
        protocol: 1,
        input: successor,
        outcome: "applied",
        directive: before?.directive,
      });
      expect(recovered.follower?.planDirective).toBeUndefined();
      expect((yield* f.service.fenceDirective(directive)).directiveFence).toEqual({
        protocol: 1,
        input: directive,
        outcome: "superseded",
      });
      expect(
        (yield* f.service
          .fenceDirective({ ...successor, expectedDirectiveId: null })
          .pipe(Effect.flip)).reason,
      ).toBe("conflict");
      expect((yield* f.service.planDirective(successor).pipe(Effect.flip)).reason).toBe(
        "not-enrolled",
      );
      expect((yield* f.service.releaseDirective(successor).pipe(Effect.flip)).reason).toBe(
        "not-enrolled",
      );
      expect(f.follower()).toBe(before);

      const active = {
        ...directive,
        directiveId: "77777777-7777-4777-8777-777777777777",
        enrollmentId: replacementEnrollment.enrollmentId,
      };
      yield* f.service.planDirective(active);
      const replaced = f.follower();
      // The successor now has only a release ID. It no longer proves the exact old request.
      expect(replaced?.releasedDirectives).toEqual([successor.directiveId]);
      expect((yield* f.service.fenceDirective(successor).pipe(Effect.flip)).reason).toBe(
        "conflict",
      );
      // The remaining predecessor history has lost its copy of the issued directive.
      expect((yield* f.service.fenceDirective(directive).pipe(Effect.flip)).reason).toBe(
        "conflict",
      );
      expect(f.follower()).toBe(replaced);

      const unseen = {
        ...directive,
        directiveId: "88888888-8888-4888-8888-888888888888",
        expectedDirectiveId: successor.directiveId,
      };
      expect((yield* f.service.fenceDirective(unseen)).directiveFence).toEqual({
        protocol: 1,
        input: unseen,
        outcome: "fenced",
      });
      expect((yield* f.service.status()).follower?.planDirective?.directiveId).toBe(
        active.directiveId,
      );
      expect(f.follower()).toMatchObject({
        ...replaced,
        releasedDirectives: [successor.directiveId, unseen.directiveId],
      });
    }),
  );

  it.effect("requires a qualified service and the exact connected environment to fence", () =>
    Effect.gen(function* () {
      const f = yield* fixture();
      yield* f.service.enroll({ enrollment, expectedEnrollmentId: null });
      for (const [serviceMode, fleetOperationsSupported] of [
        [false, true],
        [true, false],
      ] as const) {
        const unqualified = yield* makeFleetUpdates({
          environmentId,
          serviceMode,
          store: f.store,
          updates: { ...f.updates, fleetOperationsSupported },
          quiet: f.quiet,
        });
        const status = yield* unqualified.status();
        expect(status.fencePlanDirective).toBeUndefined();
        expect(status.fenceHistoricalPlanDirective).toBeUndefined();
        expect((yield* unqualified.fenceDirective(directive).pipe(Effect.flip)).reason).toBe(
          "binding",
        );
      }

      const otherEnvironment = EnvironmentId.make("different-fleet-host");
      expect(
        (yield* f.service
          .fenceDirective({ ...directive, environmentId: otherEnvironment })
          .pipe(Effect.flip)).reason,
      ).toBe("binding");
      expect(f.follower()).toBeNull();
    }),
  );

  it.effect(
    "fences an unseen historical setup through the authenticated route on a disabled host",
    () =>
      Effect.scoped(
        Effect.gen(function* () {
          const fs = yield* FileSystem.FileSystem;
          const home = yield* fs.makeTempDirectoryScoped({ prefix: "jones-fence-http-" });
          const store = createFleetHostStore(home);
          const f = yield* fixture({ store });
          const api = HttpApi.make("environment").add(EnvironmentHttpApi.groups.jonesFleetUpdates);
          const auth = Layer.mock(EnvironmentAuth.EnvironmentAuth)({
            authenticateHttpRequest: (request) => {
              if (request.headers.authorization === undefined)
                return Effect.fail(new EnvironmentAuth.ServerAuthMissingCredentialError());
              return Effect.succeed({
                sessionId: AuthSessionId.make("fleet-fixture-session"),
                subject: "fleet-fixture",
                method: "bearer-access-token",
                scopes: [
                  AuthOrchestrationReadScope,
                  ...(request.headers.authorization === "Bearer fixture-operator"
                    ? [AuthOrchestrationOperateScope]
                    : []),
                ],
              });
            },
          });
          const routes = HttpApiBuilder.layer(api).pipe(
            Layer.provide(fleetUpdatesHttpApiLayer),
            Layer.provide(Layer.succeed(FleetUpdates, f.service)),
            Layer.provide(AuthHttp.layerAuthenticatedAuth),
            Layer.provide(auth),
            Layer.provide(HttpServer.layerServices),
          );
          const app = yield* Effect.acquireRelease(
            Effect.sync(() => HttpRouter.toWebHandler(routes, { disableLogger: true })),
            (app) => Effect.promise(() => app.dispose()),
          );
          const operator = "Bearer fixture-operator";
          const fence = (authorization?: string, payload: unknown = directive) =>
            Effect.promise(() =>
              app.handler(
                new Request(`http://fixture${FLEET_UPDATES_HTTP_BASE}/plan-directive/fence`, {
                  method: "POST",
                  headers: {
                    "content-type": "application/json",
                    ...(authorization === undefined ? {} : { authorization }),
                  },
                  body: JSON.stringify(payload),
                }),
              ),
            );

          // With no enrollment file, the host cannot bind the request to a persisted enrollment.
          const unenrolled = yield* fence(operator);
          expect(unenrolled.status).toBe(409);
          expect(yield* Effect.promise(() => unenrolled.json())).toMatchObject({
            reason: "not-enrolled",
          });
          expect(yield* Effect.promise(store.readFollower)).toBeNull();

          const disabled = { ...replacementEnrollment, enabled: false };
          yield* Effect.promise(() =>
            store.editEnrollment(() => ({
              ...disabled,
              environmentId: EnvironmentId.make("different-fleet-host"),
            })),
          );
          const foreign = yield* fence(operator);
          expect(foreign.status).toBe(409);
          expect(yield* Effect.promise(() => foreign.json())).toMatchObject({
            reason: "not-enrolled",
          });
          expect(yield* Effect.promise(store.readFollower)).toBeNull();

          // A disabled enrollment still identifies this host even without a follower journal.
          yield* f.service.enroll({
            enrollment: disabled,
            expectedEnrollmentId: disabled.enrollmentId,
          });
          expect((yield* fence()).status).toBe(401);
          expect((yield* fence("Bearer fixture-reader")).status).toBe(403);
          expect(
            (yield* fence(operator, { ...directive, repository: "fixture/other" })).status,
          ).toBe(400);
          expect(yield* Effect.promise(store.readFollower)).toBeNull();

          const response = yield* fence(operator);
          expect(response.status).toBe(200);
          expect(response.headers.get("cache-control")).toBe("no-store");
          expect(yield* Effect.promise(() => response.json())).toEqual({
            ...(yield* f.service.status()),
            fencePlanDirective: 1,
            fenceHistoricalPlanDirective: 1,
            enrollment: disabled,
            directiveFence: { protocol: 1, input: directive, outcome: "fenced" },
          });
          expect(yield* Effect.promise(store.readFollower)).toEqual({
            schema: 1,
            leader: null,
            operation: null,
            lastAutomaticActivationAt: null,
            fencedDirectives: [
              {
                directiveId: directive.directiveId,
                bindingHash: expect.any(String),
                rollbackDenied: true,
              },
            ],
            releasedDirectives: [directive.directiveId],
          });
          const disabledRequest = { ...directive, enrollmentId: disabled.enrollmentId };
          expect((yield* f.service.planDirective(disabledRequest).pipe(Effect.flip)).reason).toBe(
            "not-enrolled",
          );
          expect(
            (yield* f.service.releaseDirective(disabledRequest).pipe(Effect.flip)).reason,
          ).toBe("not-enrolled");
        }),
      ).pipe(Effect.provide(NodeServices.layer)),
  );

  it.effect("replaces an inactive enrollment's directive without restoring it on release", () =>
    Effect.gen(function* () {
      const f = yield* fixture();
      yield* f.service.enroll({ enrollment, expectedEnrollmentId: null });
      const predecessor = { ...directive, directiveId: "44444444-4444-4444-8444-444444444444" };
      yield* f.service.planDirective(predecessor);
      const retained = f.follower()!.directiveHistory![0];
      const first = { ...directive, expectedDirectiveId: predecessor.directiveId };
      yield* f.service.planDirective(first);
      yield* f.service.follow;
      expect((yield* f.service.status()).follower?.planDirective?.directiveId).toBe(
        first.directiveId,
      );

      const replacement = { ...enrollment, enrollmentId: "66666666-6666-4666-8666-666666666666" };
      yield* f.service.enroll({
        enrollment: replacement,
        expectedEnrollmentId: enrollment.enrollmentId,
      });
      const oldPolicy = f.follower();
      expect(oldPolicy?.directive?.directiveId).toBe(first.directiveId);
      yield* f.service.follow;
      expect((yield* f.service.status()).follower?.planDirective).toBeUndefined();
      expect((yield* f.service.planDirective(first).pipe(Effect.flip)).reason).toBe("not-enrolled");
      expect(f.follower()).toBe(oldPolicy);

      const second = {
        ...directive,
        directiveId: "77777777-7777-4777-8777-777777777777",
        enrollmentId: replacement.enrollmentId,
      };
      expect((yield* f.service.planDirective(second)).releasableDirectiveId).toBe(
        second.directiveId,
      );
      expect(f.follower()?.directiveHistory).toEqual([
        retained,
        { directiveId: second.directiveId, prior: null },
      ]);
      expect(f.follower()?.releasedDirectives).toEqual([first.directiveId]);
      expect((yield* f.service.releaseDirective(second)).follower?.planDirective).toBeUndefined();
      expect(f.follower()?.directive).toBeUndefined();
      expect(f.follower()?.directiveHistory).toEqual([retained]);
      expect(
        (yield* f.service
          .planDirective({ ...second, directiveId: first.directiveId })
          .pipe(Effect.flip)).reason,
      ).toBe("conflict");
    }),
  );

  it.effect("rechecks enrollment at the release boundary", () =>
    Effect.gen(function* () {
      const f = yield* fixture();
      yield* f.service.enroll({ enrollment, expectedEnrollmentId: null });
      yield* f.service.planDirective(directive);
      const before = f.follower();
      vi.spyOn(f.updates, "state").mockReturnValueOnce(
        Effect.promise(() =>
          f.store.editEnrollment(() => ({
            ...enrollment,
            enrollmentId: "66666666-6666-4666-8666-666666666666",
          })),
        ).pipe(Effect.as(f.snapshot())),
      );
      expect((yield* f.service.releaseDirective(directive).pipe(Effect.flip)).reason).toBe(
        "not-enrolled",
      );
      expect(f.follower()).toBe(before);
    }),
  );

  it.effect("shares release capacity between fences and applied history", () =>
    Effect.gen(function* () {
      const f = yield* fixture();
      yield* f.service.enroll({ enrollment, expectedEnrollmentId: null });
      yield* f.service.planDirective(directive);
      const releasedDirectives = Array.from(
        { length: 126 },
        (_, index) => `22222222-2222-4222-8222-${String(index).padStart(12, "0")}`,
      );
      f.setFollower({ ...f.follower()!, releasedDirectives });
      const delayed = {
        ...directive,
        directiveId: "66666666-6666-4666-8666-666666666666",
        expectedDirectiveId: directive.directiveId,
      };
      expect((yield* f.service.fenceDirective(delayed)).directiveFence?.outcome).toBe("fenced");
      const full = f.follower();
      const extra = { ...delayed, directiveId: "77777777-7777-4777-8777-777777777777" };
      expect((yield* f.service.planDirective(extra).pipe(Effect.flip)).reason).toBe("storage");
      expect((yield* f.service.fenceDirective(extra).pipe(Effect.flip)).reason).toBe("storage");
      expect(f.follower()).toBe(full);
      expect((yield* f.service.releaseDirective(directive)).releasedDirectiveId).toBe(
        directive.directiveId,
      );
      expect(f.follower()?.releasedDirectives).toEqual([
        ...releasedDirectives,
        delayed.directiveId,
        directive.directiveId,
      ]);
      expect(f.follower()?.directiveHistory).toEqual([]);
      expect((yield* f.service.fenceDirective(delayed)).directiveFence?.outcome).toBe("fenced");
    }),
  );

  it.effect("restores the exact prior directive and prevents a late replay", () =>
    Effect.gen(function* () {
      const f = yield* fixture();
      yield* f.service.enroll({ enrollment, expectedEnrollmentId: null });
      yield* f.service.planDirective(directive);
      const successor = {
        ...directive,
        directiveId: "66666666-6666-4666-8666-666666666666",
        expectedDirectiveId: directive.directiveId,
      };
      yield* f.service.planDirective(successor);
      expect((yield* f.service.releaseDirective(directive).pipe(Effect.flip)).reason).toBe(
        "conflict",
      );
      const result = yield* f.service.releaseDirective(successor);
      expect(result.releasedDirectiveId).toBe(successor.directiveId);
      expect(result.follower?.planDirective?.directiveId).toBe(directive.directiveId);
      expect((yield* f.service.releaseDirective(successor)).releasedDirectiveId).toBe(
        successor.directiveId,
      );
      expect((yield* f.service.planDirective(successor).pipe(Effect.flip)).reason).toBe("conflict");
      expect(
        (yield* f.service.releaseDirective(directive)).follower?.planDirective,
      ).toBeUndefined();
    }),
  );

  it.effect("replays a release after restart without rewriting the follower journal", () =>
    Effect.scoped(
      Effect.gen(function* () {
        const fs = yield* FileSystem.FileSystem;
        const path = yield* Path.Path;
        const home = yield* fs.makeTempDirectoryScoped({ prefix: "jones-release-replay-" });
        const f = yield* fixture({ store: createFleetHostStore(home) });
        yield* f.service.enroll({ enrollment, expectedEnrollmentId: null });
        yield* f.service.planDirective(directive);
        yield* f.service.releaseDirective(directive);

        const journal = path.join(home, "runtime", "jones-fleet", "follower.json");
        const before = yield* fs.stat(journal);
        const restarted = yield* fixture({ store: createFleetHostStore(home) });
        const status = yield* restarted.service.status();
        expect(status.releasedDirectiveId).toBeUndefined();
        expect(status.releasableDirectiveId).toBeUndefined();
        const replay = yield* restarted.service.releaseDirective(directive);
        expect(replay.releasedDirectiveId).toBe(directive.directiveId);
        expect(replay.follower?.planDirective).toBeUndefined();
        const after = yield* fs.stat(journal);
        expect(Option.getOrThrow(after.ino)).toBe(Option.getOrThrow(before.ino));
        expect(Option.getOrThrow(after.mtime).getTime()).toBe(
          Option.getOrThrow(before.mtime).getTime(),
        );
      }),
    ).pipe(Effect.provide(NodeServices.layer)),
  );

  it.effect("preserves a legacy directive with unknown predecessor history", () =>
    Effect.gen(function* () {
      const f = yield* fixture();
      yield* f.service.enroll({ enrollment, expectedEnrollmentId: null });
      yield* f.service.planDirective(directive);
      const { directiveHistory: _unrecorded, ...legacy } = f.follower()!;
      f.setFollower(legacy);
      expect((yield* f.service.status()).releasableDirectiveId).toBeUndefined();
      expect((yield* f.service.releaseDirective(directive).pipe(Effect.flip)).reason).toBe(
        "conflict",
      );
      expect(f.follower()).toEqual(legacy);
    }),
  );

  it.effect(
    "persists an idempotent directive without changing enrollment and excludes both campaign paths",
    () =>
      Effect.gen(function* () {
        const f = yield* fixture();
        yield* f.service.enroll({ enrollment, expectedEnrollmentId: null });
        const stage = vi.spyOn(f.updates, "stageExact");
        const first = yield* f.service.planDirective(directive);
        expect(first.planDirectiveProtocol).toBe(1);
        expect(first.enrollment).toEqual(enrollment);
        expect(first.follower?.planDirective).toMatchObject({
          directiveId: directive.directiveId,
          mode: "manual-review",
        });
        expect((yield* f.service.planDirective(directive)).follower?.planDirective).toEqual(
          first.follower?.planDirective,
        );
        expect(
          (yield* f.service
            .planDirective({ ...directive, targetSource: "d".repeat(40) })
            .pipe(Effect.flip)).reason,
        ).toBe("conflict");
        expect(
          (yield* f.service
            .planDirective({
              ...directive,
              directiveId: "66666666-6666-4666-8666-666666666666",
              expectedDirectiveId: null,
            })
            .pipe(Effect.flip)).reason,
        ).toBe("conflict");
        expect((yield* f.service.stage(input).pipe(Effect.flip)).reason).toBe("conflict");
        f.operations.set(input.operationId, {
          input,
          currentVersion: "old-version",
          continueRunningThreads: false,
          phase: "staged",
          stagedHandle: "stage",
        });
        expect(
          (yield* f.service
            .activate({
              environmentId,
              enrollmentId: enrollment.enrollmentId,
              operationId: input.operationId,
            })
            .pipe(Effect.flip)).reason,
        ).toBe("conflict");
        yield* f.service.follow;
        expect(stage).not.toHaveBeenCalled();
        expect(f.install).not.toHaveBeenCalled();
      }),
  );
  it.effect(
    "survives restart and legacy leader publication, then resumes on a later different source",
    () =>
      Effect.gen(function* () {
        const f = yield* fixture();
        yield* f.service.enroll({ enrollment, expectedEnrollmentId: null });
        yield* f.service.leader({
          environmentId,
          enrollmentId: enrollment.enrollmentId,
          repository: "Jones-Systems/Jones-Code",
          committedSource: "d".repeat(40),
          committedTree: "e".repeat(40),
        });
        yield* f.service.planDirective(directive);
        const savedDirective = f.follower()?.directive;
        const stage = vi.spyOn(f.updates, "stageExact");
        const restarted = yield* f.create();
        yield* restarted.follow;
        expect(stage).not.toHaveBeenCalled();
        expect(f.install).not.toHaveBeenCalled();
        expect(f.operations.size).toBe(0);
        yield* restarted.leader({
          environmentId,
          enrollmentId: enrollment.enrollmentId,
          repository: "Jones-Systems/Jones-Code",
          committedSource: directive.targetSource,
          committedTree: directive.targetTree,
        });
        yield* restarted.follow;
        expect(stage).not.toHaveBeenCalled();
        expect(f.install).not.toHaveBeenCalled();
        expect(f.retireStaged).not.toHaveBeenCalled();
        expect(f.operations.size).toBe(0);
        expect(f.follower()?.directive).toEqual(savedDirective);
        expect((yield* restarted.status()).follower).toMatchObject({
          state: "waiting-for-leader",
          planDirective: { directiveId: directive.directiveId },
        });
        yield* restarted.leader({
          environmentId,
          enrollmentId: enrollment.enrollmentId,
          repository: "Jones-Systems/Jones-Code",
          committedSource: "f".repeat(40),
          committedTree: "e".repeat(40),
        });
        yield* restarted.follow;
        const laterOperationId = "33333333-3333-4333-8333-000000000001";
        expect(stage).toHaveBeenCalledTimes(1);
        expect(stage).toHaveBeenCalledWith({
          operationId: laterOperationId,
          targetSource: "f".repeat(40),
        });
        expect(f.install).toHaveBeenCalledTimes(1);
        expect(f.install).toHaveBeenCalledWith(
          expect.objectContaining({
            operationId: laterOperationId,
            environmentId,
            currentVersion: "old-version",
            stagedHandle: "staged-handle",
            expectedInstalledSource: input.expectedInstalledSource,
            targetSource: "f".repeat(40),
          }),
        );
        expect(f.operations.get(laterOperationId)).toMatchObject({
          phase: "pending",
          input: { ...input, operationId: laterOperationId, targetSource: "f".repeat(40) },
        });
        expect(f.operations.has(input.operationId)).toBe(false);
        expect(f.follower()?.operation?.operationId).toBe(laterOperationId);
        expect(f.follower()?.directive).toEqual(savedDirective);
        yield* restarted.follow;
        expect(stage).toHaveBeenCalledTimes(1);
        expect(f.install).toHaveBeenCalledTimes(1);
      }),
  );
  it.effect("refuses a directive while native acceptance is uncertain", () =>
    Effect.gen(function* () {
      const f = yield* fixture();
      yield* f.service.enroll({ enrollment, expectedEnrollmentId: null });
      f.setFollower({
        schema: 1,
        leader: null,
        operation: {
          operationId: input.operationId,
          targetSource: input.targetSource,
          expectedInstalledSource: input.expectedInstalledSource,
        },
        lastAutomaticActivationAt: null,
      });
      f.operations.set(input.operationId, {
        input,
        currentVersion: "old-version",
        continueRunningThreads: false,
        phase: "reconciling",
      });
      expect((yield* f.service.planDirective(directive).pipe(Effect.flip)).reason).toBe("conflict");
      expect(f.follower()?.directive).toBeUndefined();
    }),
  );
});
