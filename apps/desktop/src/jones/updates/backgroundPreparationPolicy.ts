import type { DesktopUpdateState } from "@t3tools/contracts";
import * as DateTime from "effect/DateTime";

export type JonesDesktopPreparationState = NonNullable<DesktopUpdateState["jonesPreparation"]>;

/** A newer build must stay the latest this long before it is downloaded or replaces a stage. */
export const BACKGROUND_SETTLE_MS = 10 * 60_000;
/** A failed preparation waits this long before the same source is tried again. */
export const BACKGROUND_FAILURE_BACKOFF_MS = 15 * 60_000;
/** Replaced downloads whose payload could not be removed; at this count replacement pauses. */
export const BACKGROUND_MAX_UNRECLAIMED = 2;

export interface PreparationBuild {
  readonly source: string;
  readonly version: string;
}

/**
 * Why the controller cannot be driven. `install` covers a claimed or running install, which pins
 * its stage; `blocked` is retained native work needing reconciliation; `unavailable` means this
 * app cannot install, so preparing for it would only fill the disk.
 */
export type PreparationHold =
  | { readonly kind: "install" }
  | { readonly kind: "blocked"; readonly message?: string }
  | { readonly kind: "unavailable"; readonly message?: string };

export interface PreparationInput {
  readonly enabled: boolean;
  readonly now: number;
  readonly hold: PreparationHold | undefined;
  /** The controller's current action, including a manual one. */
  readonly busy: string | null;
  readonly staged: PreparationBuild | undefined;
  readonly latest: PreparationBuild | undefined;
  /** When `latest` was first seen as the newest build. */
  readonly latestSince: number | undefined;
  readonly declinedSource: string | undefined;
  readonly retryAt: number | undefined;
  /** Replaced downloads whose payload removal is not yet proved. */
  readonly unreclaimed: number;
  readonly settleMs?: number;
  readonly maxUnreclaimed?: number;
}

export type PreparationDecision =
  | { readonly action: "none"; readonly status: JonesDesktopPreparationState }
  | {
      readonly action: "download";
      readonly target: PreparationBuild;
      readonly status: JonesDesktopPreparationState;
    }
  | {
      readonly action: "replace";
      readonly staged: PreparationBuild;
      readonly target: PreparationBuild;
      readonly status: JonesDesktopPreparationState;
    };

const iso = (time: number) => DateTime.formatIso(DateTime.makeUnsafe(time));

/** One decision per tick: one slot, latest wins, never a queue of versions. */
export function decideBackgroundPreparation(input: PreparationInput): PreparationDecision {
  const none = (status: Omit<JonesDesktopPreparationState, "enabled">): PreparationDecision => ({
    action: "none",
    status: { enabled: input.enabled, ...status },
  });
  const { staged, latest } = input;
  const newer = latest !== undefined && latest.source !== staged?.source ? latest : undefined;
  const resting = (): PreparationDecision =>
    staged === undefined
      ? none({ phase: "watching" })
      : none({ phase: "ready", targetVersion: staged.version });

  if (!input.enabled) return none({ phase: "off" });
  if (input.hold?.kind === "unavailable")
    return none({
      phase: "unavailable",
      ...(input.hold.message === undefined ? {} : { reason: input.hold.message }),
    });
  if (input.hold?.kind === "blocked")
    return none({
      phase: "blocked",
      reason:
        input.hold.message ??
        "Retained update work needs reconciliation, so background preparation is paused.",
    });
  if (input.hold?.kind === "install")
    return none({
      phase: "pinned",
      ...(newer === undefined ? {} : { targetVersion: newer.version }),
    });
  if (input.busy === "download")
    return none({
      phase: "preparing",
      ...(latest === undefined ? {} : { targetVersion: latest.version }),
    });
  if (input.busy !== null || newer === undefined) return resting();
  if (newer.source === input.declinedSource)
    return staged === undefined
      ? none({
          phase: "watching",
          reason: `You discarded ${newer.version}; a newer build will be prepared.`,
        })
      : resting();
  if (input.retryAt !== undefined && input.retryAt > input.now)
    return none({ phase: "backoff", targetVersion: newer.version, nextAt: iso(input.retryAt) });
  const readyAt = (input.latestSince ?? input.now) + (input.settleMs ?? BACKGROUND_SETTLE_MS);
  if (readyAt > input.now)
    return none({ phase: "settling", targetVersion: newer.version, nextAt: iso(readyAt) });
  if (staged === undefined)
    return {
      action: "download",
      target: newer,
      status: { enabled: true, phase: "preparing", targetVersion: newer.version },
    };
  const maxUnreclaimed = input.maxUnreclaimed ?? BACKGROUND_MAX_UNRECLAIMED;
  if (input.unreclaimed >= maxUnreclaimed)
    return none({
      phase: "blocked",
      targetVersion: newer.version,
      reason: `${input.unreclaimed} replaced downloads could not be removed, so replacing the download is paused. ${staged.version} can still be installed.`,
    });
  return {
    action: "replace",
    staged,
    target: newer,
    status: {
      enabled: true,
      phase: "preparing",
      targetVersion: newer.version,
      replacingVersion: staged.version,
    },
  };
}
