import { RegistryContext, useAtomValue } from "@effect/atom-react";
import {
  endpointKey,
  mirrorLinkedMutation,
  sameEndpoint,
  supportsLinkedThreads,
  unionLinkedThreads,
} from "@t3tools/client-runtime/jones/linked-threads";
import type { EnvironmentId } from "@t3tools/contracts";
import type {
  LinkedThreadLink,
  LinkedThreadsListResult,
  LinkThreadsInput,
  ThreadEndpointRef,
} from "@t3tools/contracts/jones/linkedThreads";
import { AsyncResult, Atom } from "effect/reactivity";
import * as Cause from "effect/Cause";
import * as Option from "effect/Option";
import { useContext, useEffect } from "react";
import { useEnvironments } from "../../state/environments";
import { useQuietThreadShells } from "../../state/entities";
import { useAtomCommand } from "../../state/use-atom-command";
import { useStableCallback } from "../../hooks/useStableCallback";
import { toastManager } from "../../components/ui/toast";
import { linkedThreads } from "./state";

const queryResponses = Atom.family((key: string) => {
  const { ids, subject }: { ids: EnvironmentId[]; subject: ThreadEndpointRef } = JSON.parse(key);
  return Atom.make((get) =>
    ids.map((environmentId) => ({
      environmentId,
      result: get(linkedThreads.list({ environmentId, input: { thread: subject } })),
    })),
  ).pipe(Atom.setIdleTTL(300_000));
});
const cachedResults = Atom.family((_key: string) =>
  Atom.make<ReadonlyMap<EnvironmentId, LinkedThreadsListResult>>(new Map()).pipe(
    Atom.setIdleTTL(300_000),
  ),
);
const refreshTimes = Atom.family((_key: string) => Atom.make(0).pipe(Atom.setIdleTTL(300_000)));
const previousRuns = Atom.family((_key: string) =>
  Atom.make<{ active: boolean; settledKey: string } | null>(null).pipe(Atom.setIdleTTL(300_000)),
);
const unlinkPermissions = Atom.family((idsKey: string) => {
  const ids: EnvironmentId[] = JSON.parse(idsKey);
  return Atom.make(
    (get) => new Map(ids.map((id) => [id, get(linkedThreads.unlink.permissionAtom(id))])),
  ).pipe(Atom.setIdleTTL(300_000));
});

export function useLinkedThreads(
  subject: ThreadEndpointRef,
  runActive: boolean,
  settledKey: string,
) {
  const registry = useContext(RegistryContext);
  const { environments } = useEnvironments();
  const shells = useQuietThreadShells();
  const connected = environments.filter((env) => env.connection.phase === "connected");
  const capable = connected.filter((env) =>
    supportsLinkedThreads(env.serverConfig?.environment.capabilities),
  );
  const idsKey = JSON.stringify(capable.map((env) => env.environmentId).sort());
  const subjectKey = endpointKey(subject);
  const responses = useAtomValue(
    queryResponses(
      JSON.stringify({ ids: capable.map((env) => env.environmentId).sort(), subject }),
    ),
  );
  const cached = useAtomValue(cachedResults(subjectKey));
  useAtomValue(previousRuns(subjectKey));
  const refresh = useStableCallback((target: ThreadEndpointRef = subject) => {
    const atom = linkedThreads.refreshAtom(target);
    registry.set(atom, registry.get(atom) + 1);
    registry.set(refreshTimes(endpointKey(target)), Date.now());
  });
  // oxlint-disable-next-line t3code/react-effect-budget -- Refresh linked-thread reads when the selected subject gains new linked environment identities.
  useEffect(() => {
    if (idsKey === "[]") return;
    refresh({ environmentId: subject.environmentId, threadId: subject.threadId });
  }, [subject.environmentId, subject.threadId, idsKey, refresh]);
  // oxlint-disable-next-line t3code/react-effect-budget -- Observe server run completion transitions and refresh linked threads without losing the prior run watermark.
  useEffect(() => {
    const atom = previousRuns(subjectKey);
    const previous = registry.get(atom);
    if (previous && !runActive && (previous.active || previous.settledKey !== settledKey))
      refresh();
    registry.set(atom, { active: runActive, settledKey });
  }, [subjectKey, runActive, settledKey, registry, refresh]);
  // oxlint-disable-next-line t3code/react-effect-budget -- Retain successful asynchronous linked-thread responses in the subject-scoped registry cache.
  useEffect(() => {
    const atom = cachedResults(subjectKey);
    const previous = registry.get(atom);
    const results = new Map(previous);
    for (const { environmentId, result } of responses) {
      if (AsyncResult.isSuccess(result)) results.set(environmentId, result.value);
    }
    if ([...results].some(([id, value]) => previous.get(id) !== value)) registry.set(atom, results);
  }, [responses, subjectKey, registry]);
  const results = new Map(cached);
  for (const { environmentId, result } of responses) {
    const value = Option.getOrNull(AsyncResult.value(result));
    if (value) results.set(environmentId, value);
  }
  const view = unionLinkedThreads(subject, [...results.values()]);
  const errors = responses
    .filter(({ result }) => AsyncResult.isFailure(result))
    .map(({ environmentId }) => environmentId);
  const answering = new Set(
    responses
      .filter(({ result }) => AsyncResult.isSuccess(result))
      .map(({ environmentId }) => environmentId),
  );
  const canLink = capable.some((env) => env.environmentId === subject.environmentId);
  const linkAllowed = useAtomValue(linkedThreads.link.permissionAtom(subject.environmentId));
  const acceptAllowed = useAtomValue(linkedThreads.accept.permissionAtom(subject.environmentId));
  const canUnlinkByEnvironment = useAtomValue(unlinkPermissions(idsKey));
  const linkCommand = useAtomCommand(linkedThreads.link, { reportFailure: false });
  const unlinkCommand = useAtomCommand(linkedThreads.unlink, { reportFailure: false });
  const acceptCommand = useAtomCommand(linkedThreads.accept, { reportFailure: false });
  const label = (id: EnvironmentId) =>
    environments.find((env) => env.environmentId === id)?.label ?? id;
  const report = async (
    operation: () => Promise<{
      steps: readonly { environmentId: EnvironmentId; state: string; detail?: string }[];
    }>,
    outcome: "recorded" | "removed" | "accepted" = "recorded",
  ) => {
    return await (async () => {
      try {
        const result = await operation();
        const receiverOnly =
          outcome === "accepted" && result.steps.some((step) => step.state === "receiver_only");
        toastManager.add({
          type: result.steps.every(
            (step) => (step.state === "recorded" && !step.detail) || step.state === "receiver_only",
          )
            ? "success"
            : "warning",
          title: receiverOnly
            ? "Accepted here · origin unchanged"
            : outcome === "accepted"
              ? "Handoff accepted"
              : outcome === "removed"
                ? "Thread unlinked"
                : "Threads linked",
          description: result.steps
            .map(
              (step) =>
                `${label(step.environmentId)}: ${step.state === "recorded" ? outcome : step.state === "receiver_only" ? "origin unchanged" : step.state}${step.detail ? ` (${step.detail})` : ""}`,
            )
            .join(" · "),
        });
        return true;
      } catch (error) {
        toastManager.add({
          type: "error",
          title: "Could not update linked threads",
          description: error instanceof Error ? error.message : String(error),
        });
        return false;
      }
    })().finally(refresh);
  };
  const available = (id: EnvironmentId) => capable.some((env) => env.environmentId === id);
  const mutate = (kind: "unlink" | "accept", link: LinkedThreadLink) =>
    report(
      async () => {
        const local =
          kind === "accept"
            ? subject.environmentId
            : [subject.environmentId, link.from.environmentId, link.to.environmentId].find(
                (id) =>
                  (id === link.from.environmentId || id === link.to.environmentId) &&
                  available(id) &&
                  canUnlinkByEnvironment.get(id),
              );
        if (local === undefined) throw new Error("This connection cannot change this link");
        const remote =
          local === link.from.environmentId ? link.to.environmentId : link.from.environmentId;
        if (
          !available(local) ||
          (kind === "accept" ? !acceptAllowed : !canUnlinkByEnvironment.get(local))
        )
          throw new Error("This connection cannot change this link");
        if (kind === "accept" && link.from.environmentId !== link.to.environmentId) {
          if (!sameEndpoint(link.to, subject))
            throw new Error("Only the receiver can accept this handoff");
          const result = await acceptCommand({
            environmentId: link.to.environmentId,
            input: { from: link.from, to: link.to, linkId: link.linkId },
          });
          if (result._tag === "Failure") throw new Error(Cause.pretty(result.cause));
          return {
            result: result.value,
            steps: [
              { environmentId: link.to.environmentId, state: "recorded" },
              {
                environmentId: link.from.environmentId,
                state: "receiver_only",
                detail:
                  "origin copy unchanged; the receiving agent’s t3_thread_link accept updates it through its environment link",
              },
            ],
          };
        }
        return mirrorLinkedMutation({
          local,
          remote,
          remoteCapable: available(remote),
          execute: async (environmentId, reverse) => {
            const result =
              kind === "unlink"
                ? await unlinkCommand({
                    environmentId,
                    input: { linkId: link.linkId, ...(reverse ? { reverse } : {}) },
                  })
                : await acceptCommand({
                    environmentId,
                    input: {
                      from: link.from,
                      to: link.to,
                      linkId: link.linkId,
                      ...(reverse ? { reverse } : {}),
                    },
                  });
            if (result._tag === "Failure") throw new Error(Cause.pretty(result.cause));
            return result.value;
          },
        });
      },
      kind === "unlink" ? "removed" : "accepted",
    );
  return {
    ...view,
    environments,
    shells,
    answering,
    errors,
    loading: responses.some(({ result }) => result.waiting || result._tag === "Initial"),
    incomplete: view.truncated || errors.length > 0,
    offline: environments.filter((env) => env.connection.phase !== "connected"),
    visible: capable.length > 0 || view.relations.length > 0,
    canLink: canLink && linkAllowed,
    canCopy: canLink,
    canAccept: (link: LinkedThreadLink) =>
      canLink && acceptAllowed && sameEndpoint(link.to, subject) && link.acceptance === "offered",
    canUnlink: (link: LinkedThreadLink) =>
      [link.from.environmentId, link.to.environmentId].some(
        (id) => available(id) && canUnlinkByEnvironment.get(id),
      ),
    onExpand: () => {
      if (Date.now() - registry.get(refreshTimes(subjectKey)) >= 30_000) refresh();
    },
    unlink: (link: LinkedThreadLink) => mutate("unlink", link),
    accept: (link: LinkedThreadLink) => mutate("accept", link),
    link: (input: LinkThreadsInput) =>
      report(async () => {
        if (!canLink || !linkAllowed) throw new Error("This connection cannot link threads");
        const other = sameEndpoint(input.from, subject) ? input.to : input.from;
        return mirrorLinkedMutation({
          local: subject.environmentId,
          remote: other.environmentId,
          remoteCapable: available(other.environmentId),
          execute: async (environmentId, reverse) => {
            const foreign = environmentId === input.from.environmentId ? input.to : input.from;
            const shell = shells.find(
              (s) => s.environmentId === foreign.environmentId && s.id === foreign.threadId,
            );
            const result = await linkCommand({
              environmentId,
              input: {
                ...input,
                remote: {
                  environmentLabel: label(foreign.environmentId).slice(0, 256),
                  ...(shell?.title ? { title: shell.title.slice(0, 512) } : {}),
                },
                ...(reverse ? { reverse } : {}),
              },
            });
            if (result._tag === "Failure") throw new Error(Cause.pretty(result.cause));
            return result.value;
          },
        });
      }),
  };
}
