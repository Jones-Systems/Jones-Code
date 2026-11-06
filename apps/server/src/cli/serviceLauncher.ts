import * as Effect from "effect/Effect";
import { Command } from "effect/cli";

import { launcherFailureMessage } from "../jones/cloud/qualifiedCommitAttempt.ts";
import { main as runServiceLauncher } from "../serviceLauncher.ts";

/** The launcher must stop its child before exiting; `runMain` would interrupt that shutdown. */
export const serviceLauncherCommand = Command.make("__service-launcher").pipe(
  Command.unlisted,
  Command.withHandler(() =>
    Effect.sync(() => {
      runServiceLauncher().catch((cause: unknown) => {
        process.stderr.write(`[service-launcher] ${launcherFailureMessage(cause)}\n`);
        process.exitCode = 1;
      });
    }),
  ),
);
