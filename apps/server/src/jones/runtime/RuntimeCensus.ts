import { type ProjectId, ProviderRuntimeBinding } from "@t3tools/contracts";
import * as Clock from "effect/Clock";
import * as Context from "effect/Context";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";
import * as ProjectionStore from "../../orchestration-v2/ProjectionStore.ts";
import * as ProviderSessionManager from "../../orchestration-v2/ProviderSessionManager.ts";
import * as RuntimeObservation from "../provider/observations/ProviderThreadRuntimeObservation.ts";

export class RuntimeCensusReadError extends Schema.TaggedError<RuntimeCensusReadError>()(
  "RuntimeCensusReadError",
  {
    reason: Schema.Literals(["census_unavailable", "census_changed"]),
    cause: Schema.optional(Schema.Defect()),
  },
) {
  override get message(): string {
    return "The application runtime census could not be read coherently.";
  }
}

interface ApplicationOperatingCounts {
  readonly total: number;
  readonly operating: number;
  readonly foregroundWaitingApproval: number;
  readonly foregroundWaitingInput: number;
  readonly foregroundWaitingPlan: number;
  readonly foregroundUnknown: number;
  readonly backgroundOperating: number;
  readonly backgroundUnknown: number;
  readonly backgroundSampledAt: string;
  readonly observedAt: string;
  readonly snapshotSequence: number;
  readonly countScope: "application";
  readonly applicationCensusCoverage: "complete";
  readonly nativeBackgroundCoverage: "unknown";
}

export class RuntimeCensus extends Context.Service<
  RuntimeCensus,
  {
    readonly readOperatingCounts: (
      projectId?: ProjectId,
    ) => Effect.Effect<ApplicationOperatingCounts, RuntimeCensusReadError>;
  }
>()("t3/jones/runtime/RuntimeCensus") {}

const make = Effect.gen(function* () {
  const projections = yield* ProjectionStore.ProjectionStoreV2;
  const sessionsOption = yield* Effect.serviceOption(
    ProviderSessionManager.ProviderSessionManagerV2,
  );
  const equivalentBinding = Schema.toEquivalence(ProviderRuntimeBinding);
  const readOperatingCounts = Effect.fn("RuntimeCensus.readOperatingCounts")(function* (
    projectId?: ProjectId,
  ) {
    const getCandidates = projections.getOperatingCountsCandidates;
    if (Option.isNone(sessionsOption) || getCandidates === undefined)
      return yield* new RuntimeCensusReadError({ reason: "census_unavailable" });
    const sessions = sessionsOption.value;
    const input = projectId === undefined ? undefined : { projectId };
    const snapshot = yield* getCandidates(input);
    const backgroundSampledAt = DateTime.formatIso(yield* DateTime.now);
    const counts = {
      total: snapshot.threads.length,
      operating: 0,
      foregroundWaitingApproval: 0,
      foregroundWaitingInput: 0,
      foregroundWaitingPlan: 0,
      foregroundUnknown: 0,
      backgroundOperating: 0,
      backgroundUnknown: 0,
    };
    const revalidations: Array<Effect.Effect<boolean>> = [];
    for (const thread of snapshot.threads) {
      const sampledAt = yield* Clock.currentTimeMillis;
      const attachment =
        sessions.readCurrentThreadRuntimeAttachment === undefined
          ? null
          : yield* sessions.readCurrentThreadRuntimeAttachment(thread.id);
      let native: RuntimeObservation.ProviderRuntimeObservation = {
        status: "unknown",
        reason: "runtime_binding_unavailable",
      };
      if (attachment?.status === "stopped") {
        revalidations.push(attachment.isCurrent);
        native = { status: "unknown", reason: "runtime_not_resident" };
      } else if (attachment?.status === "attached") {
        revalidations.push(attachment.isCurrent);
        native =
          sessions.observeThreadActivity === undefined
            ? { status: "unknown", reason: "native_activity_unsupported" }
            : yield* sessions.observeThreadActivity(thread.id);
        if (native.status !== "unknown" && !equivalentBinding(native.binding, attachment.binding))
          native = { status: "unknown", reason: "runtime_binding_changed" };
      }
      const activity = RuntimeObservation.providerThreadActivityObservation(
        thread,
        native,
        sampledAt,
      );
      if (activity.foreground === "waiting_approval") counts.foregroundWaitingApproval++;
      if (activity.foreground === "waiting_input") counts.foregroundWaitingInput++;
      if (activity.foreground === "waiting_plan") counts.foregroundWaitingPlan++;
      if (activity.background !== null) counts.backgroundOperating++;
      if (activity.backgroundStatus === "unknown") counts.backgroundUnknown++;
      if (activity.foreground === "working" || activity.background !== null) counts.operating++;
      if (
        RuntimeObservation.providerThreadForegroundActivity(thread) === "working" &&
        activity.foreground !== "working" &&
        activity.backgroundStatus === "unknown"
      )
        counts.foregroundUnknown++;
    }
    if (!(yield* Effect.all(revalidations).pipe(Effect.map((current) => current.every(Boolean)))))
      return yield* new RuntimeCensusReadError({ reason: "census_changed" });
    const after = yield* getCandidates(input);
    if (after.snapshotSequence !== snapshot.snapshotSequence)
      return yield* new RuntimeCensusReadError({ reason: "census_changed" });
    return {
      ...counts,
      backgroundSampledAt,
      snapshotSequence: snapshot.snapshotSequence,
      observedAt: DateTime.formatIso(yield* DateTime.now),
      countScope: "application",
      applicationCensusCoverage: "complete",
      nativeBackgroundCoverage: "unknown",
    } as const;
  });
  return RuntimeCensus.of({
    readOperatingCounts: (projectId) =>
      readOperatingCounts(projectId).pipe(
        Effect.mapError((cause) =>
          Schema.is(RuntimeCensusReadError)(cause)
            ? cause
            : new RuntimeCensusReadError({ reason: "census_unavailable", cause }),
        ),
      ),
  });
});

export const layer = Layer.effect(RuntimeCensus, make);
