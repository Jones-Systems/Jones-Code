import { expect, it } from "@effect/vitest";
import { ServerSelfUpdateError, ThreadId } from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import { withRunningThreadContinuation } from "../../cloud/selfUpdate.ts";
import { ServiceLauncherClientError } from "../../cloud/serviceLauncherClient.ts";

const operationId = "12345678-1234-4234-8234-123456789abc";
const otherId = "22345678-1234-4234-8234-123456789abc";
const running = [ThreadId.make("running-native")];

it.effect.each(["send", "disconnect", "timeout"] as const)(
  "holds every Install after an uncertain %s handoff until the launcher proves it rejected",
  (operation) =>
    Effect.gen(function* () {
      let attempts = 0;
      const cleared: Array<ReadonlyArray<ThreadId>> = [];
      const wrapped = yield* withRunningThreadContinuation({
        mode: "web",
        selfUpdate: {
          update: () => Effect.die("unexpected legacy update"),
          commitDesktopUpdate: () => Effect.never,
          installQualified: () =>
            Effect.sync(() => {
              attempts += 1;
            }).pipe(
              Effect.andThen(
                Effect.fail(
                  new ServerSelfUpdateError({
                    reason: "uncertain handoff",
                    cause: new ServiceLauncherClientError({ operation }),
                  }),
                ),
              ),
            ),
        },
        prepare: Effect.succeed(running),
        clear: (threadIds) =>
          Effect.sync(() => {
            cleared.push(threadIds);
          }),
      });
      const install = wrapped.installQualified!;
      const request = {
        operationId,
        stagedHandle: "fixed-candidate",
        continueRunningThreads: true,
        // The faked installQualified never reaches staged verification.
        verifyStaged: () => Promise.reject(new Error("unexpected staged verification")),
      };
      yield* install(request).pipe(Effect.flip);
      expect(attempts).toBe(1);
      // Neither the same operation nor another may start while the outcome is unknown.
      for (const id of [operationId, otherId])
        expect(
          (yield* install({ ...request, operationId: id }).pipe(Effect.flip)).reason,
        ).toContain("needs reconciliation");
      // A rejection proven for another operation releases nothing.
      yield* wrapped.releaseRejectedHandoff(otherId);
      expect((yield* install(request).pipe(Effect.flip)).reason).toContain("needs reconciliation");
      expect(attempts).toBe(1);
      expect(cleared).toEqual([]);
      // The launcher proved this operation rejected: its markers clear once and Install reopens.
      yield* wrapped.releaseRejectedHandoff(operationId);
      yield* wrapped.releaseRejectedHandoff(operationId);
      expect(cleared).toEqual([running]);
      yield* install({ ...request, operationId: otherId }).pipe(Effect.flip);
      expect(attempts).toBe(2);
    }),
);

it.effect("a proven rejection never releases an accepted handoff", () =>
  Effect.gen(function* () {
    const cleared: Array<ReadonlyArray<ThreadId>> = [];
    const wrapped = yield* withRunningThreadContinuation({
      mode: "web",
      selfUpdate: {
        update: () => Effect.die("unexpected legacy update"),
        commitDesktopUpdate: () => Effect.never,
        installQualified: (_request, onAccepted) =>
          (onAccepted?.() ?? Effect.void).pipe(
            Effect.andThen(
              Effect.fail(new ServerSelfUpdateError({ reason: "failed after acceptance" })),
            ),
          ),
      },
      prepare: Effect.succeed(running),
      clear: (threadIds) =>
        Effect.sync(() => {
          cleared.push(threadIds);
        }),
    });
    const request = {
      operationId,
      stagedHandle: "fixed-candidate",
      continueRunningThreads: true,
      verifyStaged: () => Promise.reject(new Error("unexpected staged verification")),
    };
    yield* wrapped.installQualified!(request).pipe(Effect.flip);
    const before = cleared.length;
    yield* wrapped.releaseRejectedHandoff(operationId);
    expect(cleared.length).toBe(before);
    expect((yield* wrapped.installQualified!(request).pipe(Effect.flip)).reason).toContain(
      "needs reconciliation",
    );
  }),
);
