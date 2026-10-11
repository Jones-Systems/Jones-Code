import { describe, expect, it } from "@effect/vitest";
import { vi } from "vite-plus/test";
import * as Deferred from "effect/Deferred";
import * as Fiber from "effect/Fiber";
import * as Effect from "effect/Effect";
import { EnvironmentId, type JonesUpdateState } from "@t3tools/contracts";
import type {
  FleetEnrollment,
  FleetHostOperation,
  FleetStageInput,
} from "@t3tools/contracts/jones/fleet-updates";
import { makeFleetUpdates, type FleetHostUpdater } from "./service.ts";
import type { FleetHostStore } from "./store.ts";

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
const fixture = () =>
  Effect.gen(function* () {
    let savedEnrollment: FleetEnrollment | null = null;
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
    };
    let update: JonesUpdateState = {
      source: "jones-actions",
      channel: "jones-main",
      phase: "available",
      capability: { check: true, download: true, install: true },
      currentVersion: "old-version",
      environmentId,
      installedSource: input.expectedInstalledSource,
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
      retireStagedOperation: () => Effect.succeed({ retired: true }),
      installForOperation: install,
      reconcileOperation: () => Effect.sync(() => native),
    };
    const create = () => makeFleetUpdates({ environmentId, serviceMode: true, store, updates });
    return {
      service: yield* create(),
      create,
      install,
      operations,
      updates,
      snapshot: () => update,
      setNative: (value: typeof native) => {
        native = value;
      },
    };
  });

describe("fleet host coordination", () => {
  it.effect("requires explicit enrollment and rejects changed immutable operation bindings", () =>
    Effect.gen(function* () {
      const f = yield* fixture();
      expect((yield* f.service.stage(input).pipe(Effect.flip)).message).toContain("not accepted");
      yield* f.service.enroll({ enrollment, expectedEnrollmentId: null });
      yield* f.service.stage(input);
      expect(
        (yield* f.service.stage({ ...input, targetSource: "d".repeat(40) }).pipe(Effect.flip))
          .message,
      ).toContain("another update");
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
      yield* f.service.activate(input);
      expect(f.install).toHaveBeenCalledTimes(2);
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
