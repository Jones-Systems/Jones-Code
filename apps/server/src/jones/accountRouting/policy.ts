import type { ProviderInstanceId } from "@t3tools/contracts";

const ACCOUNT_EVIDENCE_MAX_AGE_MS = 120_000;
const HOUR_MS = 3_600_000;
const DAY_MS = 24 * HOUR_MS;

export interface AllowanceWindow {
  readonly usedPercent: number;
  readonly resetsAt: number | null;
  readonly durationMs: number;
}

export type AccountAllowance =
  | { readonly status: "unknown" }
  | { readonly status: "blocked"; readonly resetsAt: number | null }
  | {
      readonly status: "eligible";
      readonly weekly: ReadonlyArray<AllowanceWindow>;
      readonly session?: AllowanceWindow;
    };

export function currentObservation(observedAt: number, now: number): boolean {
  return observedAt <= now && now - observedAt <= ACCOUNT_EVIDENCE_MAX_AGE_MS;
}

function hasCurrentReset(window: AllowanceWindow, now: number): boolean {
  return window.resetsAt !== null && window.resetsAt > now;
}

export function validPercent(value: unknown): value is number {
  return typeof value === "number" && Number.isFinite(value) && value >= 0 && value <= 100;
}

export function blockingReset(resets: ReadonlyArray<number | null>): number | null {
  const dated = resets.filter((reset): reset is number => reset !== null);
  return dated.length > 0 && dated.length === resets.length ? Math.max(...dated) : null;
}

export function claudeAllowance(input: {
  readonly session: AllowanceWindow | undefined;
  readonly weekly: ReadonlyArray<AllowanceWindow>;
  readonly hasOverallWeek: boolean;
  readonly now: number;
}): AccountAllowance {
  const { session, weekly, hasOverallWeek, now } = input;
  const blockers = [
    ...(session && session.usedPercent > 70 ? [session] : []),
    ...weekly.filter((window) => window.usedPercent >= 90),
  ].filter((window) => window.resetsAt === null || window.resetsAt > now);
  if (blockers.length > 0) {
    return {
      status: "blocked",
      resetsAt: blockingReset(blockers.map((window) => window.resetsAt)),
    };
  }
  if (
    !session ||
    !hasOverallWeek ||
    !hasCurrentReset(session, now) ||
    !weekly.every((window) => hasCurrentReset(window, now))
  ) {
    return { status: "unknown" };
  }
  return { status: "eligible", session, weekly };
}

function pace(window: AllowanceWindow, now: number) {
  const remainingMs = (window.resetsAt ?? now) - now;
  const fraction = Math.max(0, Math.min(1, 1 - remainingMs / window.durationMs));
  const ahead = window.usedPercent - 100 * fraction ** 0.75;
  return {
    ahead,
    surplusPerDay: remainingMs > 0 ? Math.max(0, -ahead) / (remainingMs / DAY_MS) : 0,
  };
}

export function chooseAccount(
  driver: "codex" | "claudeAgent",
  members: ReadonlyArray<{
    readonly instanceId: ProviderInstanceId;
    readonly allowance: AccountAllowance;
  }>,
  now: number,
): { readonly instanceId: ProviderInstanceId } | { readonly nextReset: number | null } {
  const ordered = members.toSorted((a, b) =>
    a.instanceId < b.instanceId ? -1 : a.instanceId > b.instanceId ? 1 : 0,
  );
  const eligible = ordered
    .flatMap(({ instanceId, allowance }) => {
      if (allowance.status !== "eligible") return [];
      const weeklyPace = allowance.weekly.map((window) => ({ ...pace(window, now), window }));
      return [
        {
          instanceId,
          session: allowance.session,
          weeklyPace,
          score:
            weeklyPace.length > 0 ? Math.min(...weeklyPace.map((entry) => entry.surplusPerDay)) : 0,
        },
      ];
    })
    .sort((a, b) => b.score - a.score);
  const normal = eligible[0];
  if (normal) {
    if (driver !== "claudeAgent" || !normal.session) return { instanceId: normal.instanceId };
    const normalReset = normal.session.resetsAt;
    const behind = normal.weeklyPace.filter((entry) => entry.ahead < 0);
    if (
      normalReset !== null &&
      behind.length > 0 &&
      behind.every(
        ({ window }) => window.resetsAt !== null && window.resetsAt - now >= 12 * HOUR_MS,
      )
    ) {
      const detour = eligible
        .slice(1)
        .find(
          ({ session, weeklyPace }) =>
            session !== undefined &&
            session.usedPercent <= 60 &&
            session.resetsAt !== null &&
            session.resetsAt > now &&
            session.resetsAt <= now + HOUR_MS &&
            session.resetsAt < normalReset &&
            weeklyPace.every((entry) => entry.ahead <= 15),
        );
      if (detour) return { instanceId: detour.instanceId };
    }
    return { instanceId: normal.instanceId };
  }
  const unknown = ordered.find(({ allowance }) => allowance.status === "unknown");
  if (unknown) return { instanceId: unknown.instanceId };
  const resets = ordered.flatMap(({ allowance }) =>
    allowance.status === "blocked" && allowance.resetsAt !== null ? [allowance.resetsAt] : [],
  );
  return { nextReset: resets.length > 0 ? Math.min(...resets) : null };
}
