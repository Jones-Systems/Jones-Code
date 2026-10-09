import type {
  DesktopRuntimeInfo,
  DesktopUpdateChannel,
  DesktopUpdateState,
} from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Option from "effect/Option";
import * as Stream from "effect/Stream";
import type { DesktopUpdates } from "../../updates/DesktopUpdates.ts";
import { createInitialDesktopUpdateState } from "../../updates/updateMachine.ts";

export const MANUAL_UPDATE_REASON = "Jones Preview Companion uses manual updates.";

/** No native updater, artifact staging, feed, or activation capability enters this service. */
export function makeCompanionProductUpdates(input: {
  readonly version: string;
  readonly runtimeInfo: DesktopRuntimeInfo;
  readonly channel: DesktopUpdateChannel;
  readonly emit: (state: DesktopUpdateState) => Effect.Effect<void>;
}): DesktopUpdates["Service"] {
  const state = {
    ...createInitialDesktopUpdateState(input.version, input.runtimeInfo, input.channel),
    message: MANUAL_UPDATE_REASON,
  };
  const refused = { accepted: false, completed: false, state };
  const emitState = input.emit(state);
  return {
    getState: Effect.succeed(state),
    isActionActive: Effect.succeed(false),
    isInstallActive: Effect.succeed(false),
    subscribe: Effect.succeed({ latest: state, changes: Stream.never }),
    emitState,
    disabledReason: Effect.succeed(Option.some(MANUAL_UPDATE_REASON)),
    configure: emitState,
    setChannel: () => Effect.succeed(state),
    check: () => Effect.succeed({ checked: false, state }),
    download: Effect.succeed(refused),
    install: Effect.succeed(refused),
    installPrepared: () => Effect.succeed({ ...refused, failed: false }),
  };
}
