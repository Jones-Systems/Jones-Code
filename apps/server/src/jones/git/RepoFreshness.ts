import type {
  ProjectId,
  ProjectSettingsOverrides,
  ServerSettings,
  ServerSettingsError,
} from "@t3tools/contracts";
import { resolveProjectSettings } from "@t3tools/shared/projectSettings";
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
import {
  skipRepoFreshnessCheckout,
  type RepoFreshnessCheckout,
  type RepoFreshnessCheckoutSkipReason,
} from "./repoFreshnessCheckout.ts";
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

type ProjectBinding =
  | {
      readonly cadence: Cadence;
      readonly remote: RepoFreshnessRemote;
      readonly workspaceRoot: string;
    }
  | {
      readonly cadence: Cadence;
      readonly reason: Extract<RepoFreshnessStatus["target"], { status: "unavailable" }>["reason"];
    };

interface RefreshResult {
  readonly status: RepoFreshnessStatus;
  readonly checkout: RepoFreshnessCheckout;
}

export class RepoFreshness extends Context.Service<
  RepoFreshness,
  {
    readonly status: (projectId: ProjectId) => Effect.Effect<RepoFreshnessStatus>;
    /** Fetch-only verification for launch. */
    readonly checkForLaunch: (projectId: ProjectId) => Effect.Effect<RepoFreshnessStatus>;
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
  const scheduledProjects = new Set<ProjectId>();

  const resolve = Effect.fn("RepoFreshness.resolve")(
    function* (
      projectId: ProjectId,
    ): Effect.fn.Return<ProjectBinding, ProjectStore.ProjectStoreV2Error | ServerSettingsError> {
      const [current, project] = yield* Effect.all([
        settings.getSettings,
        projects.getShell(projectId),
      ]);
      const cadence = cadenceFor(current, projectId);
      if (Option.isNone(project)) return { cadence, reason: "project-missing" } as const;
      const remote = yield* git.resolveRepoFreshnessRemote(project.value.workspaceRoot);
      return remote === null
        ? ({ cadence, reason: "remote-unresolved" } as const)
        : {
            cadence,
            remote,
            workspaceRoot: project.value.workspaceRoot,
          };
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

  const startCheck = Effect.fn("RepoFreshness.startCheck")(function* (remote: RepoFreshnessRemote) {
    const pending = flights.get(remote.identity);
    if (pending) return pending;
    const entry = evidence.get(remote.identity) ?? {};
    evidence.delete(remote.identity);
    evidence.set(remote.identity, entry);
    const oldest = evidence.keys().next().value;
    if (evidence.size > MAX_EVIDENCE && oldest !== undefined) evidence.delete(oldest);
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

  const checkProject = Effect.fn("RepoFreshness.checkProject")(function* (
    projectId: ProjectId,
    scheduledRoot?: string,
  ) {
    const binding = yield* resolve(projectId);
    if (!("remote" in binding)) {
      return {
        status: yield* snapshot(projectId, binding.cadence, {
          status: "unavailable",
          reason: binding.reason,
        }),
        reason: "target-unavailable",
      } as const;
    }
    const target = { status: "resolved", remoteName: binding.remote.remoteName } as const;
    if (
      scheduledRoot !== undefined &&
      (scheduledRoot !== binding.workspaceRoot || !binding.cadence)
    ) {
      return {
        status: yield* snapshot(projectId, binding.cadence, target),
        reason: binding.cadence ? "target-changed" : "cadence-disabled",
      } as const;
    }
    const identity = binding.remote.identity;
    const prior = evidence.get(identity)?.observed;
    const pending = flights.get(identity);
    const now = yield* Clock.currentTimeMillis;
    const dueAt =
      prior === undefined ? 0 : attemptTime(prior.latestAttempt) + cadenceMs(binding.cadence);
    // Only the remote evidence is shared. Each due project evaluates its own checkout and opt-in.
    const checked =
      scheduledRoot !== undefined && pending === undefined && prior !== undefined && dueAt > now
        ? prior
        : yield* Fiber.join(yield* startCheck(binding.remote));
    if (scheduledRoot !== undefined) {
      due.set(projectId, {
        cadence: binding.cadence,
        at: attemptTime(checked.latestAttempt) + cadenceMs(binding.cadence),
      });
    }
    const current = yield* resolve(projectId);
    if (!("remote" in current) || current.remote.identity !== identity) {
      return {
        status: yield* snapshot(projectId, current.cadence, {
          status: "unavailable",
          reason: "target-changed",
        }),
        reason: "target-changed",
      } as const;
    }
    const status = yield* snapshot(projectId, current.cadence, target, checked);
    if (checked.latestAttempt.status === "failed")
      return { status, reason: "verification-failed" } as const;
    return { status, binding, current, checked, verified: checked.latestAttempt.verification };
  });

  const refreshProject = Effect.fn("RepoFreshness.refreshProject")(function* (
    projectId: ProjectId,
    scheduledRoot?: string,
  ): Effect.fn.Return<RefreshResult> {
    const outcome = yield* checkProject(projectId, scheduledRoot);
    const { status } = outcome;
    if ("reason" in outcome) return { status, checkout: skipRepoFreshnessCheckout(outcome.reason) };
    const { binding, current, checked, verified } = outcome;
    if (
      current.workspaceRoot !== binding.workspaceRoot ||
      current.remote.checkoutIdentity !== binding.remote.checkoutIdentity
    )
      return { status, checkout: skipRepoFreshnessCheckout("target-changed") };

    const authorize = Effect.gen(function* () {
      const project = yield* projects.getShell(projectId);
      if (
        Option.isNone(project) ||
        project.value.workspaceRoot !== binding.workspaceRoot ||
        evidence.get(binding.remote.identity)?.observed !== checked
      )
        return "target-changed";
      const policy = yield* settings.getSettings;
      if (scheduledRoot !== undefined && !cadenceFor(policy, projectId)) return "cadence-disabled";
      return resolveProjectSettings(policy, projectId).settings.defaultAutoPull
        ? null
        : "auto-pull-disabled";
    }).pipe(
      Effect.orElseSucceed(() => "target-unavailable" as const),
    ) satisfies Effect.Effect<RepoFreshnessCheckoutSkipReason | null>;
    const permission = yield* authorize;
    if (permission !== null) return { status, checkout: skipRepoFreshnessCheckout(permission) };
    return {
      status,
      checkout: yield* binding.remote.updateCheckout(verified, authorize),
    };
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
    const ready: Array<{ project: (typeof registered)[number]; cadence: Cadence; at: number }> = [];
    for (const project of registered) {
      const cadence = cadenceFor(current, project.id);
      if (cadence === 0) continue;
      enabled.add(project.id);
      const previous = due.get(project.id);
      const at = previous?.cadence === cadence ? previous.at : now + staggerMs(project.id);
      due.set(project.id, { cadence, at });
      if (at <= now && !scheduledProjects.has(project.id)) ready.push({ project, cadence, at });
    }
    for (const id of due.keys()) if (!enabled.has(id)) due.delete(id);
    ready.sort((a, b) => a.at - b.at);
    for (const {
      project: { id, workspaceRoot },
      cadence,
    } of ready) {
      if (scheduledProjects.size >= 2) break;
      due.set(id, { cadence, at: now + cadenceMs(cadence) });
      scheduledProjects.add(id);
      yield* Effect.forkIn(
        refreshProject(id, workspaceRoot).pipe(
          Effect.provideService(GitVcsDriver.BackgroundGitWork, true),
          Effect.ensuring(
            Effect.sync(() => {
              scheduledProjects.delete(id);
            }),
          ),
        ),
        scope,
      );
    }
  });

  yield* forkParked(
    Effect.forever(tick().pipe(Effect.ignore, Effect.andThen(Effect.sleep(TICK_MS)))),
  );
  return RepoFreshness.of({
    status,
    checkForLaunch: Effect.fn("RepoFreshness.checkForLaunch")(function* (projectId: ProjectId) {
      return (yield* checkProject(projectId)).status;
    }),
    refresh: Effect.fnUntraced(function* (projectId: ProjectId) {
      return yield* refreshProject(projectId);
    }),
  });
});

export const layer = Layer.effect(RepoFreshness, make);
