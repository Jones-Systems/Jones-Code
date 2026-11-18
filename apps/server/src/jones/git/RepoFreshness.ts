import type { ProjectId, ProjectSettingsOverrides, ServerSettings } from "@t3tools/contracts";
import * as Clock from "effect/Clock";
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";

import * as ProjectStore from "../../orchestration-v2/ProjectStore.ts";
import { forkParked } from "../../serverActivation.ts";
import * as ServerSettingsService from "../../serverSettings.ts";
import * as GitVcsDriver from "../../vcs/GitVcsDriver.ts";
import type { RepoFreshnessRemote, VerifiedRepoDefault } from "./repoFreshnessGit.ts";

type Cadence = NonNullable<ProjectSettingsOverrides["repoFreshnessCadenceSeconds"]>;
type Verification = VerifiedRepoDefault & { readonly checkedAtMs: number };
type Attempt =
  | { readonly status: "succeeded"; readonly verification: Verification }
  | { readonly status: "failed"; readonly checkedAtMs: number };
type Evidence = { readonly latestVerified: Verification | null; readonly latestAttempt: Attempt };

export interface RepoFreshnessStatus {
  readonly projectId: ProjectId;
  readonly cadenceSeconds: Cadence;
  readonly target:
    | { readonly status: "resolved"; readonly remoteName: string }
    | {
        readonly status: "unavailable";
        readonly reason:
          | "project-missing"
          | "project-state-unavailable"
          | "remote-unresolved"
          | "target-changed";
      };
  readonly latestVerified: Verification | null;
  readonly latestAttempt: Attempt | null;
  readonly freshness: "unknown" | "fresh" | "stale" | "cadence-off";
}

interface RefreshResult {
  readonly status: RepoFreshnessStatus;
  readonly checkout: {
    readonly status: "skipped";
    readonly reason: "checkout-update-deferred";
  };
}

export class RepoFreshness extends Context.Service<
  RepoFreshness,
  {
    readonly status: (projectId: ProjectId) => Effect.Effect<RepoFreshnessStatus>;
    readonly refresh: (projectId: ProjectId) => Effect.Effect<RefreshResult>;
  }
>()("t3/jones/git/RepoFreshness") {}

const cadenceFor = (settings: ServerSettings, id: ProjectId): Cadence =>
  settings.projectSettingsOverrides[id]?.repoFreshnessCadenceSeconds ?? 0;

const cadenceMs = (seconds: Cadence) => seconds * 1_000;
const attemptTime = (attempt: Attempt) =>
  attempt.status === "succeeded" ? attempt.verification.checkedAtMs : attempt.checkedAtMs;

const TICK_MS = 5_000;
const MAX_EVIDENCE = 2_048;
const staggerMs = (projectId: ProjectId) => {
  let hash = 0;
  for (const character of projectId) hash = Math.imul(31, hash) + character.charCodeAt(0);
  return (Math.abs(hash) % 6) * TICK_MS;
};

const make = Effect.gen(function* () {
  const git = yield* GitVcsDriver.GitVcsDriver;
  const projects = yield* ProjectStore.ProjectStoreV2;
  const settings = yield* ServerSettingsService.ServerSettingsService;
  const scope = yield* Effect.scope;
  const evidence = new Map<string, { observed?: Evidence }>();
  const flights = new Map<string, Fiber.Fiber<Evidence>>();
  const due = new Map<ProjectId, { cadence: Cadence; at: number }>();
  let scheduledChecks = 0;

  const resolve = Effect.fn("RepoFreshness.resolve")(
    function* (projectId: ProjectId) {
      const [current, project] = yield* Effect.all([
        settings.getSettings,
        projects.getShell(projectId),
      ]);
      const cadence = cadenceFor(current, projectId);
      if (Option.isNone(project)) return { cadence, reason: "project-missing" } as const;
      const remote = yield* git.resolveRepoFreshnessRemote(project.value.workspaceRoot);
      return remote === null
        ? ({ cadence, reason: "remote-unresolved" } as const)
        : { cadence, remote };
    },
    Effect.orElseSucceed(() => ({ cadence: 0, reason: "project-state-unavailable" }) as const),
  );

  const snapshot = Effect.fn("RepoFreshness.snapshot")(function* (
    projectId: ProjectId,
    cadenceSeconds: Cadence,
    target: RepoFreshnessStatus["target"],
    observed?: Evidence,
  ) {
    const latestVerified = observed?.latestVerified ?? null;
    const now = yield* Clock.currentTimeMillis;
    return {
      projectId,
      cadenceSeconds,
      target,
      latestVerified,
      latestAttempt: observed?.latestAttempt ?? null,
      freshness:
        latestVerified === null
          ? "unknown"
          : cadenceSeconds === 0
            ? "cadence-off"
            : now - latestVerified.checkedAtMs >= 2.5 * cadenceMs(cadenceSeconds)
              ? "stale"
              : "fresh",
    } satisfies RepoFreshnessStatus;
  });

  const startCheck = Effect.fn("RepoFreshness.startCheck")(function* (
    remote: RepoFreshnessRemote,
    scheduled: boolean,
  ) {
    const pending = flights.get(remote.identity);
    if (pending) return pending;
    const entry = evidence.get(remote.identity) ?? {};
    evidence.delete(remote.identity);
    evidence.set(remote.identity, entry);
    const oldest = evidence.keys().next().value;
    if (evidence.size > MAX_EVIDENCE && oldest !== undefined) evidence.delete(oldest);
    if (scheduled) scheduledChecks++;
    const check = Effect.gen(function* () {
      const selected = yield* remote.check.pipe(Effect.option);
      const checkedAtMs = yield* Clock.currentTimeMillis;
      const observed: Evidence = Option.match(selected, {
        onNone: () => ({
          latestVerified: entry.observed?.latestVerified ?? null,
          latestAttempt: { status: "failed", checkedAtMs },
        }),
        onSome: (value) => {
          const verification = { ...value, checkedAtMs };
          return {
            latestVerified: verification,
            latestAttempt: { status: "succeeded", verification },
          };
        },
      });
      entry.observed = observed;
      return observed;
    }).pipe(
      Effect.ensuring(
        Effect.sync(() => {
          flights.delete(remote.identity);
          if (scheduled) scheduledChecks--;
        }),
      ),
    );
    const fiber = yield* Effect.forkIn(check, scope, {
      startImmediately: false,
      uninterruptible: false,
    });
    flights.set(remote.identity, fiber);
    return fiber;
  }, Effect.uninterruptible);

  const status = Effect.fn("RepoFreshness.status")(function* (projectId: ProjectId) {
    const binding = yield* resolve(projectId);
    return "remote" in binding
      ? yield* snapshot(
          projectId,
          binding.cadence,
          { status: "resolved", remoteName: binding.remote.remoteName },
          evidence.get(binding.remote.identity)?.observed,
        )
      : yield* snapshot(projectId, binding.cadence, {
          status: "unavailable",
          reason: binding.reason,
        });
  });

  const refreshStatus = Effect.fn("RepoFreshness.refreshStatus")(function* (projectId: ProjectId) {
    const binding = yield* resolve(projectId);
    if (!("remote" in binding)) {
      return yield* snapshot(projectId, binding.cadence, {
        status: "unavailable",
        reason: binding.reason,
      });
    }
    const checked = yield* Fiber.join(yield* startCheck(binding.remote, false));
    const current = yield* resolve(projectId);
    if (!("remote" in current) || current.remote.identity !== binding.remote.identity) {
      return yield* snapshot(projectId, current.cadence, {
        status: "unavailable",
        reason: "target-changed",
      });
    }
    return yield* snapshot(
      projectId,
      current.cadence,
      { status: "resolved", remoteName: current.remote.remoteName },
      checked,
    );
  });

  const refresh = Effect.fn("RepoFreshness.refresh")(function* (projectId: ProjectId) {
    return {
      status: yield* refreshStatus(projectId),
      checkout: { status: "skipped", reason: "checkout-update-deferred" },
    } satisfies RefreshResult;
  });

  const tick = Effect.fn("RepoFreshness.tick")(function* () {
    const current = yield* settings.getSettings;
    if (
      !Object.values(current.projectSettingsOverrides).some(
        (entry) => entry.repoFreshnessCadenceSeconds,
      )
    ) {
      due.clear();
      return;
    }
    const registered = yield* projects.listShells();
    const now = yield* Clock.currentTimeMillis;
    const enabled = new Set<ProjectId>();
    let next: (typeof registered)[number] | undefined;
    let nextAt = Infinity;
    for (const project of registered) {
      const cadence = cadenceFor(current, project.id);
      if (cadence === 0) continue;
      enabled.add(project.id);
      const previous = due.get(project.id);
      const at = previous?.cadence === cadence ? previous.at : now + staggerMs(project.id);
      due.set(project.id, { cadence, at });
      if (at <= now && at < nextAt) {
        next = project;
        nextAt = at;
      }
    }
    for (const id of due.keys()) if (!enabled.has(id)) due.delete(id);
    if (next === undefined || scheduledChecks >= 2) return;

    const cadence = cadenceFor(current, next.id);
    due.set(next.id, { cadence, at: now + cadenceMs(cadence) });
    const remote = yield* git.resolveRepoFreshnessRemote(next.workspaceRoot);
    if (remote === null) return;
    const prior = evidence.get(remote.identity)?.observed;
    if (prior !== undefined) {
      // A manual check or an alias at a shorter cadence can already have serviced this target.
      const at = attemptTime(prior.latestAttempt) + cadenceMs(cadence);
      if (at > now) {
        due.set(next.id, { cadence, at });
        return;
      }
    }
    yield* startCheck(remote, true);
  });

  yield* forkParked(
    Effect.forever(tick().pipe(Effect.ignore, Effect.andThen(Effect.sleep(TICK_MS)))),
  );
  return RepoFreshness.of({ status, refresh });
});

export const layer = Layer.effect(RepoFreshness, make);
