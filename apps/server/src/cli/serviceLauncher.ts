import * as Effect from "effect/Effect";
import { Command } from "effect/cli";

import { launcherFailureMessage } from "../jones/cloud/qualifiedCommitAttempt.ts";
import { main as runServiceLauncher } from "../serviceLauncher.ts";

/**
 * The launcher owns SIGTERM handling and the process lifetime: it must finish stopping its child
 * before the process exits, so it runs detached from the CLI's fiber rather than under `runMain`,
 * whose signal handler would interrupt the fiber and exit while the child is still being
 * terminated.
 */
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
