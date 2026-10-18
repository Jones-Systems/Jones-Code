export const WORKSTREAM_SHELL_ENV_NAMES = [
  "T3_WORKSTREAM_CONTROL_PLANE_URL",
  "T3_WORKSTREAM_OWNER_ID",
  "T3_WORKSTREAM_PRINCIPAL_ID",
  "T3_WORKSTREAM_KEY_ID",
  "T3_WORKSTREAM_SIGNING_SECRET",
  "T3_WORKSTREAM_AUTHORIZATION_REVISION",
] as const;

export function installMissingWorkstreamShellActivation(
  environment: NodeJS.ProcessEnv,
  shellEnvironment: Readonly<Record<string, string>>,
): void {
  for (const name of WORKSTREAM_SHELL_ENV_NAMES) {
    // An inherited empty value keeps activation disabled; only recover absent values.
    if (environment[name] === undefined && shellEnvironment[name]) {
      environment[name] = shellEnvironment[name];
    }
  }
}
