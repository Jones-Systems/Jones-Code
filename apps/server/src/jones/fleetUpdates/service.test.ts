import { describe, expect, it } from "@effect/vitest";
import { vi } from "vite-plus/test";
import * as Deferred from "effect/Deferred";
import * as Fiber from "effect/Fiber";
import * as Clock from "effect/Clock";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as TestClock from "effect/testing/TestClock";
import { EnvironmentId, type JonesUpdateState } from "@t3tools/contracts";
import type {
  FleetEnrollment,
  FleetHostOperation,
  FleetStageInput,
} from "@t3tools/contracts/jones/fleet-updates";
import {
  AUTOMATIC_ACTIVATION_SPACING_MS,
  QUIET_INPUT_IDLE_MS,
  type HostWorkSnapshot,
} from "../updates/followerPolicy.ts";
import type { FleetQuietSignals } from "./quietSignals.ts";
import { FOLLOWER_STAGE_RETRY_MS, makeFleetUpdates, type FleetHostUpdater } from "./service.ts";
import type { FleetFollowerRecord, FleetHostStore } from "./store.ts";

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

const fixture = () =>
  Effect.gen(function* () {
    let savedEnrollment: FleetEnrollment | null = null;
    let follower: FleetFollowerRecord | null = null;
    const operations = new Map<string, FleetHostOperation>();
    const store: FleetHostStore = {
      readEnrollment: async () => savedEnrollment,
      editEnrollment: async (change) => (savedEnrollment = change(savedEnrollment)),
      readOperation: async (id) => operations.get(id) ?? null,
      editOperation: async (id, change) => {
        const next = change(operations.get(id) ?? null);
        operations.set(id, next);
        return next;
      },
      readFollower: async () => follower,
      editFollower: async (change) => (follower = change(follower)),
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
        expect(operations.get(input.operationId)?.phase).toBe("dispatching");
        native = { state: "pending", operationId: request.operationId, binding: request };
        return { ...update, phase: "installing" };
      }),
    );
    const retireStaged = vi.fn<FleetHostUpdater["retireStagedOperation"]>(() =>
      Effect.succeed({ retired: true }),
    );
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
      refreshPolicy: Effect.succeed(undefined),
    };
    let nextOperation = 0;
    const create = () =>
      makeFleetUpdates({
        environmentId,
        serviceMode: true,
        store,
        updates,
        quiet,
        operationId: () => `33333333-3333-4333-8333-${String(++nextOperation).padStart(12, "0")}`,
      });
    return {
      service: yield* create(),
      create,
      install,
      operations,
      updates,
      quiet,
      retireStaged,
      follower: () => follower,
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
    committedSource: input.targetSource,
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
      yield* f.service.leader(leaderInput);
      yield* f.service.leader(leaderInput);
      expect(f.follower()?.leader).toEqual({
        enrollmentId: enrollment.enrollmentId,
        committedSource: input.targetSource,
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
      committedSource: "c".repeat(40),
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
