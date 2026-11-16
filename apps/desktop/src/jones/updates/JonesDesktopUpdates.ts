import {
  JonesDesktopUpdateController as ControllerCore,
  type JonesDesktopUpdateOptions,
} from "./JonesDesktopUpdatesCore.ts";
import { prepareJonesStage } from "./stagingJobs.ts";
import { stageJonesMacApp } from "./jonesMacStaging.ts";

export * from "./JonesDesktopUpdatesCore.ts";

/** Production and legacy callers retain the original in-process staging defaults. */
export class JonesDesktopUpdateController extends ControllerCore {
  constructor(options: JonesDesktopUpdateOptions) {
    super(options, { prepareJonesStage, stageJonesMacApp });
  }
}
