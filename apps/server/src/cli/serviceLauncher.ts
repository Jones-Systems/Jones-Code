import { launcherFailureMessage } from "../jones/cloud/qualifiedCommitAttempt.ts";
import { main as runServiceLauncher } from "../serviceLauncher.ts";

export async function runServiceLauncherCli(): Promise<void> {
  try {
    await runServiceLauncher();
  } catch (cause) {
    process.stderr.write(`[service-launcher] ${launcherFailureMessage(cause)}\n`);
    process.exitCode = 1;
  }
}
