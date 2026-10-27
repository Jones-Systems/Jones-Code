import type { JonesUpdateState } from "@t3tools/contracts/jones/jonesUpdates";
import type { JonesUpdateObservation } from "./jonesUpdates.ts";
import {
  jonesCandidatePresentation,
  jonesUpdatePresentation,
  resolveJonesExactSourceAction,
} from "./jonesUpdates.ts";

const checkText = {
  available: "A qualified package is available.",
  "target-already-running": "The requested source is already running.",
  "qualification-pending": "Package qualification is still pending.",
  "qualification-failed": "Package qualification failed.",
  "no-eligible-package-in-window": "No eligible package was found in this search window.",
  "incomplete-discovery": "Discovery was incomplete. Check again for more evidence.",
  failure: "The update check failed. Check the connection or authentication.",
} as const;

export interface JonesTargetStatusOptions {
  readonly canOperate?: boolean;
  readonly targetSource?: string;
  readonly planId?: string;
  /** Client-observed receipt time; a package check timestamp is not a connection last-seen time. */
  readonly lastObservedAt?: string;
  /** Current negotiated authority overrides cached state, including an explicit downgrade. */
  readonly capability?: JonesUpdateState["capability"] | null;
}

export function jonesTargetStatus(
  observation: JonesUpdateObservation,
  options: JonesTargetStatusOptions = {},
) {
  const state = observation.state;
  const capability = "capability" in options ? options.capability : state?.capability;
  const operation = state?.operation;
  const held = operation?.phase === "held";
  const unknown = operation?.code === "launcher-unconfirmed";
  const fresh = observation.freshness === "fresh" && state !== null;
  const allowed =
    fresh &&
    options.canOperate === true &&
    capability !== undefined &&
    capability !== null &&
    !held &&
    !unknown;
  const reasons: string[] = [];
  if (!fresh) reasons.push(observation.message ?? "Waiting for fresh host status.");
  if (!options.canOperate) reasons.push("This session cannot change updates.");
  if (capability === undefined || capability === null)
    reasons.push("Update capability is unavailable.");
  if (held || unknown) reasons.push("Read the exact operation outcome before any new work.");
  const pending =
    operation !== undefined &&
    ["accepted", "running", "stop-requested", "adopting", "handed-off", "held"].includes(
      operation.phase,
    );
  let download = false;
  let install = false;
  if (allowed && state !== null && !pending) {
    const effective = { ...state, capability: capability! };
    if (options.targetSource !== undefined && state.environmentId !== undefined) {
      const decision = resolveJonesExactSourceAction({
        environmentId: state.environmentId,
        observation: { ...observation, state: effective },
        canOperate: true,
        targetSource: options.targetSource,
      });
      if (decision.status === "ready") {
        download = decision.step.kind === "download" || decision.step.kind === "download-exact";
        install = decision.step.kind === "install";
      } else reasons.push(decision.message);
    } else if (!jonesUpdatePresentation(effective).busy) {
      download =
        capability!.download &&
        state.stagedHandle === undefined &&
        state.phase === "available" &&
        state.provenance !== undefined;
      install =
        capability!.install &&
        state.stagedHandle !== undefined &&
        state.currentVersion !== undefined;
    }
  }
  const stop =
    allowed &&
    capability?.operations === 1 &&
    operation !== undefined &&
    operation.action.kind !== "install" &&
    ["accepted", "running"].includes(operation.phase);
  const classification = state?.checkClassification;
  const headline = held
    ? `Stop not confirmed · ${operation.code ?? "held"}`
    : unknown
      ? "Not confirmed"
      : operation?.phase === "stop-requested"
        ? "Stop requested…"
        : operation?.phase === "adopting"
          ? "Checking download · can't stop now"
          : operation?.phase === "handed-off"
            ? "Installing · restarting"
            : state === null
              ? observation.freshness === "unsupported"
                ? "Updates unsupported"
                : "Waiting for update status"
              : jonesUpdatePresentation(state).message;
  return {
    headline,
    ...(operation?.message === undefined && observation.message === undefined
      ? {}
      : { detail: operation?.message ?? observation.message }),
    tone:
      held || unknown
        ? ("warning" as const)
        : !fresh
          ? ("muted" as const)
          : state?.phase === "error" || state?.phase === "blocked"
            ? ("error" as const)
            : ("neutral" as const),
    freshness: {
      state: observation.freshness,
      ...(options.lastObservedAt === undefined ? {} : { lastObservedAt: options.lastObservedAt }),
    },
    ...(operation === undefined
      ? {}
      : {
          operation: {
            operationId: operation.operationId,
            phase: operation.phase,
            held,
            unknown,
            planCorrelated:
              options.planId !== undefined && operation.correlation?.planId === options.planId,
          },
        }),
    ...(classification === undefined
      ? {}
      : {
          check: {
            kind: classification.kind,
            text: checkText[classification.kind],
            bounded: classification.discovery?.bounded ?? false,
          },
        }),
    candidate: state === null ? undefined : jonesCandidatePresentation(state),
    actions: {
      ...(download ? { download: true as const } : {}),
      ...(install ? { install: true as const } : {}),
      ...(stop ? { stop: true as const } : {}),
      reasons,
    },
  };
}
