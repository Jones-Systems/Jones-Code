import { EnvironmentSupervisor } from "@t3tools/client-runtime/connection";
import { fetchWorkQueueMetadata } from "@t3tools/client-runtime/work-queue-metadata";
import * as ManagedRelay from "@t3tools/client-runtime/relay";
import { createEnvironmentCommand } from "@t3tools/client-runtime/state/runtime";
import type { EnvironmentId } from "@t3tools/contracts";
import * as Cause from "effect/Cause";
import * as Effect from "effect/Effect";
import * as Option from "effect/Option";
import * as SubscriptionRef from "effect/SubscriptionRef";
import { useCallback } from "react";
import { connectionAtomRuntime } from "../../connection/runtime";
import { useAtomCommand } from "../../state/use-atom-command";

const query = createEnvironmentCommand(connectionAtomRuntime, {
  label: "web-work-queue:metadata",
  execute: (signal: AbortSignal) => {
    const aborted = Effect.callback<never>((resume) => {
      const abort = () => resume(Effect.interrupt);
      if (signal.aborted) abort();
      else signal.addEventListener("abort", abort, { once: true });
      return Effect.sync(() => signal.removeEventListener("abort", abort));
    });
    return Effect.raceFirst(
      Effect.gen(function* () {
        if (signal.aborted) return yield* Effect.interrupt;
        const supervisor = yield* EnvironmentSupervisor.EnvironmentSupervisor;
        const prepared = yield* SubscriptionRef.get(supervisor.prepared);
        if (Option.isNone(prepared))
          return yield* Effect.fail({ _tag: "WorkQueueMetadataUnavailableError" });
        const signer = yield* Effect.serviceOption(
          ManagedRelay.ManagedRelay.ManagedRelayDpopSigner,
        );
        return yield* fetchWorkQueueMetadata({ prepared: prepared.value, signer });
      }),
      aborted,
    );
  },
});

export function useWorkQueueMetadata(environmentId: EnvironmentId) {
  const run = useAtomCommand(query, { reportFailure: false, reportDefect: false });
  return useCallback(
    async (signal: AbortSignal) => {
      const result = await run({ environmentId, input: signal });
      if (result._tag === "Failure") throw Cause.squash(result.cause);
      return result.value;
    },
    [environmentId, run],
  );
}
