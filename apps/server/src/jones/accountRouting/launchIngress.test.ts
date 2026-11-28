import { it, expect } from "@effect/vitest";
import { ProjectId, ProviderInstanceId, type ModelSelection } from "@t3tools/contracts";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";
import * as Ref from "effect/Ref";
import * as Result from "effect/Result";

import { resolveLaunchSelection } from "./launchIngress.ts";
import { AccountAutoRoutingError } from "./selectAutoAccount.ts";

const representative = {
  instanceId: ProviderInstanceId.make("representative"),
  model: "test-model",
  options: [{ id: "effort", value: "max" }],
} satisfies ModelSelection;
const winner = { ...representative, instanceId: ProviderInstanceId.make("winner") };
const input = {
  accountRouting: { mode: "auto" },
  projectId: ProjectId.make("test-project"),
  modelSelection: representative,
  workspaceStrategy: { type: "root" },
  runtimeMode: "full-access",
  interactionMode: "default",
} satisfies Parameters<typeof resolveLaunchSelection>[0];
const quotaRefusal = new AccountAutoRoutingError({
  code: "no_available_allowance",
  message: "Synthetic quota refusal",
});
const unexpectedReceipt = Effect.die("unexpected receipt lookup");
const unexpectedSelect = () => Effect.die("unexpected quota observation");

it.effect("preserves a manual selection without reading a receipt or observing quota", () =>
  Effect.gen(function* () {
    const { accountRouting: _routing, ...manual } = input;
    const chosen = yield* resolveLaunchSelection(manual, unexpectedReceipt, unexpectedSelect);
    expect(chosen).toBe(representative);
  }),
);

it.effect("substitutes the selected account before launch with exact options and cwd", () =>
  Effect.gen(function* () {
    const chosen = yield* resolveLaunchSelection(
      {
        ...input,
        workspaceStrategy: { type: "existing_worktree", worktreePath: "/test/worktree" },
      },
      Effect.succeed(false),
      (request) => {
        expect(request.cwd).toBe("/test/worktree");
        expect(request.modelSelection).toBe(representative);
        expect(request.runtimeMode).toBe("full-access");
        expect(request.interactionMode).toBe("default");
        return Effect.succeed(winner);
      },
    );
    expect(chosen).toBe(winner);
  }),
);

it.effect("refuses reuse and unmaterialized workspaces before observing quota", () =>
  Effect.gen(function* () {
    const reuse = yield* resolveLaunchSelection(
      { ...input, reuseExistingThread: true },
      unexpectedReceipt,
      unexpectedSelect,
    ).pipe(Effect.result);
    expect(Result.isFailure(reuse) && reuse.failure.code).toBe("reuse_existing_thread");
    const allocation = yield* resolveLaunchSelection(
      { ...input, workspaceStrategy: { type: "worktree" } },
      Effect.succeed(false),
      unexpectedSelect,
    ).pipe(Effect.result);
    expect(Result.isFailure(allocation) && allocation.failure.code).toBe("unsupported_workspace");
  }),
);

it.effect(
  "defers a same-command replay to the immutable receipt, even if the workspace changed",
  () =>
    Effect.gen(function* () {
      const replay = yield* resolveLaunchSelection(
        { ...input, workspaceStrategy: { type: "auto" } },
        Effect.succeed(true),
        unexpectedSelect,
      );
      // The original request is forwarded so ThreadLaunchService can recover the durable winner.
      expect(replay).toBe(representative);
    }),
);

it.effect("rechecks the receipt when a simultaneous winner lands during a quota refusal", () =>
  Effect.gen(function* () {
    const accepted = yield* Ref.make(false);
    const sampling = yield* Deferred.make<void>();
    const finish = yield* Deferred.make<void>();
    const racing = yield* resolveLaunchSelection(input, Ref.get(accepted), () =>
      Deferred.succeed(sampling, undefined).pipe(
        Effect.andThen(Deferred.await(finish)),
        Effect.andThen(Effect.fail(quotaRefusal)),
      ),
    ).pipe(Effect.forkScoped);
    yield* Deferred.await(sampling);
    yield* Ref.set(accepted, true);
    yield* Deferred.succeed(finish, undefined);
    expect(yield* Fiber.join(racing)).toBe(representative);
  }),
);

it.effect("never falls back to the representative on an unaccepted quota refusal", () =>
  Effect.gen(function* () {
    const launch = yield* Ref.make(false);
    const failure = yield* resolveLaunchSelection(
      input,
      Effect.succeed(false),
      () => quotaRefusal,
    ).pipe(Effect.andThen(Ref.set(launch, true)), Effect.result);
    expect(Result.isFailure(failure) && failure.failure).toBe(quotaRefusal);
    expect(yield* Ref.get(launch)).toBe(false);
  }),
);
