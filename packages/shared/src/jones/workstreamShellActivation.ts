import { WORKSTREAM_SHELL_ENV_NAMES } from "@t3tools/contracts/jones/workstreamShellActivation";
export { WORKSTREAM_SHELL_ENV_NAMES };

export function installMissingWorkstreamShellActivation(
  environment: NodeJS.ProcessEnv,
  shellEnvironment: Readonly<Record<string, string | undefined>>,
): void {
  for (const name of WORKSTREAM_SHELL_ENV_NAMES) {
    // Empty inherited values deliberately disable activation; recover absent values only.
    if (environment[name] === undefined && shellEnvironment[name])
      environment[name] = shellEnvironment[name];
  }
}
export const captureWorkstreamShellActivation = (environment: NodeJS.ProcessEnv) =>
  Object.fromEntries(
    WORKSTREAM_SHELL_ENV_NAMES.flatMap((name) =>
      environment[name] === undefined ? [] : [[name, environment[name]]],
    ),
  );
