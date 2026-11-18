import { launcherFailureMessage } from "../jones/cloud/qualifiedCommitAttempt.ts";
import { type LauncherOptions, main as runServiceLauncher } from "../serviceLauncher.ts";

/** The flags a Windows task passes, parsed here so the launcher starts without the full CLI. */
export function parseServiceLauncherArgs(args: ReadonlyArray<string>): LauncherOptions | undefined {
  let baseDir: string | undefined;
  let logFile: string | undefined;
  let serviceUnit: string | undefined;
  for (let index = 0; index < args.length; index += 2) {
    const flag = args[index];
    const value = args[index + 1];
    if (value === undefined) return undefined;
    if (flag === "--base-dir") baseDir = value;
    else if (flag === "--log-file") logFile = value;
    else if (flag === "--service-unit") serviceUnit = value;
    else return undefined;
  }
  return { baseDir, logFile, serviceUnit };
}

export async function runServiceLauncherCli(options?: LauncherOptions): Promise<void> {
  try {
    await runServiceLauncher(options);
  } catch (cause) {
    process.stderr.write(`[service-launcher] ${launcherFailureMessage(cause)}\n`);
    process.exitCode = 1;
  }
}
