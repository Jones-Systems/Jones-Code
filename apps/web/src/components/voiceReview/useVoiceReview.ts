import { EnvironmentSupervisor } from "@t3tools/client-runtime/connection";
import * as ManagedRelay from "@t3tools/client-runtime/relay";
import { createEnvironmentCommand } from "@t3tools/client-runtime/state/runtime";
import {
  fetchVoiceReviewDraft,
  fetchVoiceReviewDrafts,
  mutateVoiceReviewDraft,
} from "@t3tools/client-runtime/voice-review";
import type {
  EnvironmentId,
  VoiceReviewAction,
  VoiceReviewMutationPayload,
} from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Option from "effect/Option";
import * as SubscriptionRef from "effect/SubscriptionRef";
import * as Cause from "effect/Cause";
import { useCallback, useMemo } from "react";
import { connectionAtomRuntime } from "../../connection/runtime";
import { useAtomCommand } from "../../state/use-atom-command";

const requestContext = Effect.gen(function* () {
  const supervisor = yield* EnvironmentSupervisor.EnvironmentSupervisor;
  const prepared = yield* SubscriptionRef.get(supervisor.prepared);
  if (Option.isNone(prepared)) return yield* Effect.fail({ _tag: "VoiceReviewUnavailableError" });
  const signer = yield* Effect.serviceOption(ManagedRelay.ManagedRelay.ManagedRelayDpopSigner);
  return { prepared: prepared.value, signer };
});

const list = createEnvironmentCommand(connectionAtomRuntime, {
  label: "web-voice-review:list",
  execute: (scope: "pending" | "recent") =>
    Effect.gen(function* () {
      return yield* fetchVoiceReviewDrafts({ ...(yield* requestContext), scope, limit: 50 });
    }),
});
const get = createEnvironmentCommand(connectionAtomRuntime, {
  label: "web-voice-review:get",
  execute: (id: string) =>
    Effect.gen(function* () {
      return yield* fetchVoiceReviewDraft({ ...(yield* requestContext), id });
    }),
});
const mutate = createEnvironmentCommand(connectionAtomRuntime, {
  label: "web-voice-review:mutate",
  execute: (input: {
    id: string;
    action: VoiceReviewAction;
    payload: VoiceReviewMutationPayload;
  }) =>
    Effect.gen(function* () {
      return yield* mutateVoiceReviewDraft({ ...(yield* requestContext), ...input });
    }),
});

export function useVoiceReview(environmentId: EnvironmentId) {
  const runList = useAtomCommand(list, { reportFailure: false, reportDefect: false });
  const runGet = useAtomCommand(get, { reportFailure: false, reportDefect: false });
  const runMutate = useAtomCommand(mutate, { reportFailure: false, reportDefect: false });
  const fetchList = useCallback(
    async (scope: "pending" | "recent") => {
      const result = await runList({ environmentId, input: scope });
      if (result._tag === "Failure") throw Cause.squash(result.cause);
      return result.value;
    },
    [environmentId, runList],
  );
  const transport = useMemo(
    () => ({
      get: async (id: string) => {
        const result = await runGet({ environmentId, input: id });
        if (result._tag === "Failure") throw Cause.squash(result.cause);
        return result.value;
      },
      mutate: async (
        id: string,
        action: VoiceReviewAction,
        payload: VoiceReviewMutationPayload,
      ) => {
        const result = await runMutate({ environmentId, input: { id, action, payload } });
        if (result._tag === "Failure") throw Cause.squash(result.cause);
        return result.value;
      },
    }),
    [environmentId, runGet, runMutate],
  );
  return { fetchList, transport };
}
