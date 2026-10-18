import { useAtomValue } from "@effect/atom-react";
import { EnvironmentSupervisor } from "@t3tools/client-runtime/connection";
import {
  createEnvironmentCommand,
  createEnvironmentQueryAtomFamily,
} from "@t3tools/client-runtime/state/runtime";
import {
  fetchCompanionHosts,
  fetchCompanionThread,
  isCompanionEndpointUnsupported,
  setCompanionDefault,
  setCompanionThread,
} from "@t3tools/client-runtime/jones/preview-companion/http";
import type {
  DesktopCompanionState,
  EnvironmentId,
  PreviewCompanionThreadSelectionResponse,
  PreviewRenderHostSelection,
  ScopedThreadRef,
} from "@t3tools/contracts";
import * as Data from "effect/Data";
import * as Effect from "effect/Effect";
import * as Option from "effect/Option";
import * as SubscriptionRef from "effect/SubscriptionRef";
import { AsyncResult, Atom } from "effect/reactivity";
import { connectionAtomRuntime } from "../../connection/runtime";
import { appAtomRegistry } from "../../rpc/atomRegistry";

export const companionStateAtom = Atom.make<DesktopCompanionState | null>(null).pipe(
  Atom.keepAlive,
);

class CompanionEnvironmentUnavailable extends Data.TaggedError("CompanionEnvironmentUnavailable") {}

const preparedConnection = Effect.gen(function* () {
  const supervisor = yield* EnvironmentSupervisor.EnvironmentSupervisor;
  const prepared = yield* SubscriptionRef.get(supervisor.prepared);
  if (Option.isNone(prepared)) return yield* Effect.fail(new CompanionEnvironmentUnavailable());
  return prepared.value;
});

export type CompanionQuery<A> =
  | { readonly status: "ready"; readonly value: A }
  | { readonly status: "unsupported" | "unavailable" };
const result = <A, E, R>(effect: Effect.Effect<A, E, R>) =>
  effect.pipe(
    Effect.map((value): CompanionQuery<A> => ({ status: "ready", value })),
    Effect.catch((cause) =>
      Effect.succeed<CompanionQuery<A>>({
        status: isCompanionEndpointUnsupported(cause) ? "unsupported" : "unavailable",
      }),
    ),
  );

export const hostsQuery = createEnvironmentQueryAtomFamily(connectionAtomRuntime, {
  label: "companion:hosts",
  staleTimeMs: 2_000,
  refreshIntervalMs: 5_000,
  idleTtlMs: 0,
  execute: (_input: Record<string, never>) =>
    result(preparedConnection.pipe(Effect.flatMap(fetchCompanionHosts))),
});
export const bindingsQuery = createEnvironmentQueryAtomFamily(connectionAtomRuntime, {
  label: "companion:bindings",
  staleTimeMs: 1_000,
  refreshIntervalMs: 2_000,
  idleTtlMs: 0,
  execute: (input: { readonly threadId: ScopedThreadRef["threadId"] }) =>
    result(
      preparedConnection.pipe(
        Effect.flatMap((prepared) => fetchCompanionThread(prepared, input.threadId)),
      ),
    ),
});
export const setDefaultCommand = createEnvironmentCommand(connectionAtomRuntime, {
  label: "companion:set-default",
  execute: (selection: PreviewRenderHostSelection) =>
    preparedConnection.pipe(Effect.flatMap((prepared) => setCompanionDefault(prepared, selection))),
});
export const setThreadCommand = createEnvironmentCommand(connectionAtomRuntime, {
  label: "companion:set-thread",
  execute: (input: {
    readonly threadId: ScopedThreadRef["threadId"];
    readonly selection: PreviewRenderHostSelection | null;
  }) =>
    preparedConnection.pipe(
      Effect.flatMap((prepared) => setCompanionThread(prepared, input.threadId, input.selection)),
    ),
});

export function readBindingResult(
  threadRef: ScopedThreadRef,
): CompanionQuery<PreviewCompanionThreadSelectionResponse> | null {
  const value = appAtomRegistry.get(
    bindingsQuery({
      environmentId: threadRef.environmentId,
      input: { threadId: threadRef.threadId },
    }),
  );
  return AsyncResult.isSuccess(value) ? value.value : null;
}

export function useCompanionBindings(threadRef: ScopedThreadRef) {
  const value = useAtomValue(
    bindingsQuery({
      environmentId: threadRef.environmentId,
      input: { threadId: threadRef.threadId },
    }),
  );
  return AsyncResult.isSuccess(value) ? value.value : null;
}

export function refreshCompanionBindings(threadRef: ScopedThreadRef): void {
  appAtomRegistry.refresh(
    bindingsQuery({
      environmentId: threadRef.environmentId,
      input: { threadId: threadRef.threadId },
    }),
  );
}
