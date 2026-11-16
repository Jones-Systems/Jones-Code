import type {
  JonesInstallPlanRequest,
  JonesInstallPlanResult,
} from "@t3tools/contracts/jones/installPlan";
import type {
  JonesUpdateBuildList,
  JonesUpdateProvenance,
} from "@t3tools/contracts/jones/jonesUpdates";
import type {
  FleetDesktopRequest,
  FleetDesktopState,
} from "@t3tools/contracts/jones/fleet-updates";
import type {
  DesktopUpdateActionResult,
  DesktopUpdateChannel,
  DesktopUpdateCheckResult,
  DesktopUpdateState,
} from "@t3tools/contracts";
import type {
  JonesDesktopDownloadSelection,
  JonesDesktopDownloadResult,
} from "../jones/updates/JonesDesktopUpdates.ts";
import type { DesktopUpdateSetChannelError } from "./DesktopUpdates.ts";
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Option from "effect/Option";
import * as PubSub from "effect/PubSub";
import * as Ref from "effect/Ref";
import * as Scope from "effect/Scope";
import * as Semaphore from "effect/Semaphore";
import * as Stream from "effect/Stream";

export interface DesktopPreparedUpdateInstallResult extends DesktopUpdateActionResult {
  readonly failed: boolean;
}

export class DesktopUpdates extends Context.Service<
  DesktopUpdates,
  {
    readonly getState: Effect.Effect<DesktopUpdateState>;
    readonly isActionActive: Effect.Effect<boolean>;
    readonly isInstallActive: Effect.Effect<boolean>;
    readonly subscribe: Effect.Effect<
      {
        readonly latest: DesktopUpdateState;
        readonly changes: Stream.Stream<DesktopUpdateState>;
      },
      never,
      Scope.Scope
    >;
    readonly emitState: Effect.Effect<void>;
    readonly disabledReason: Effect.Effect<Option.Option<string>>;
    readonly configure: Effect.Effect<void, never, Scope.Scope>;
    /** `minimumVersion` is a track switch's checked release; nothing older is offered after. */
    readonly setChannel: (
      channel: DesktopUpdateChannel,
      options?: { readonly minimumVersion?: string | undefined },
    ) => Effect.Effect<DesktopUpdateState, DesktopUpdateSetChannelError>;
    /** `allowDowngrade` is for a guarded track switch, which checked that older release. */
    readonly check: (
      reason: string,
      options?: { readonly allowDowngrade?: boolean },
    ) => Effect.Effect<DesktopUpdateCheckResult>;
    readonly download: Effect.Effect<DesktopUpdateActionResult>;
    readonly listJonesBuilds?: Effect.Effect<JonesUpdateBuildList>;
    readonly selectJonesBuild?: (build: JonesUpdateProvenance) => Effect.Effect<boolean>;
    readonly downloadSelected?: (
      selection: JonesDesktopDownloadSelection,
    ) => Effect.Effect<JonesDesktopDownloadResult & { readonly state: DesktopUpdateState }>;
    readonly install: Effect.Effect<DesktopUpdateActionResult>;
    readonly discardStaged?: (handle: string) => Effect.Effect<DesktopUpdateActionResult>;
    readonly setBackgroundPreparation?: (enabled: boolean) => Effect.Effect<DesktopUpdateState>;
    readonly installStaged?: (
      handle: string,
      campaignId?: string,
    ) => Effect.Effect<DesktopUpdateActionResult>;
    readonly installPlan?: (
      request: JonesInstallPlanRequest,
    ) => Effect.Effect<JonesInstallPlanResult>;
    readonly fleetUpdates?: (request: FleetDesktopRequest) => Effect.Effect<FleetDesktopState>;
    readonly installPrepared: (
      expectedVersion: string,
      stagedHandle?: string,
    ) => Effect.Effect<DesktopPreparedUpdateInstallResult>;
  }
  // @effect-diagnostics-next-line deterministicKeys:off - Preserve the production service key across extraction.
>()("@t3tools/desktop/updates/DesktopUpdates") {}

export const makeDesktopUpdateStateCell = Effect.fn("DesktopUpdates.stateCell")(function* (
  initial: DesktopUpdateState,
  send: (state: DesktopUpdateState) => Effect.Effect<void>,
) {
  const updateStateRef = yield* Ref.make(initial);
  const stateChanges = yield* PubSub.sliding<DesktopUpdateState>(16);
  // Serialize writes and publishes against subscription so snapshots cannot overlap the first change.
  const stateMutex = yield* Semaphore.make(1);

  const emitState = Ref.get(updateStateRef).pipe(Effect.flatMap((state) => send(state)));

  const setState = (state: DesktopUpdateState): Effect.Effect<void> =>
    stateMutex
      .withPermits(1)(
        Ref.set(updateStateRef, state).pipe(Effect.andThen(PubSub.publish(stateChanges, state))),
      )
      .pipe(Effect.andThen(emitState));

  const updateState = (
    f: (state: DesktopUpdateState) => DesktopUpdateState,
  ): Effect.Effect<DesktopUpdateState> =>
    Ref.get(updateStateRef).pipe(
      Effect.flatMap((state) => {
        const nextState = f(state);
        return setState(nextState).pipe(Effect.as(nextState));
      }),
    );

  return { updateStateRef, stateChanges, stateMutex, emitState, setState, updateState };
});
