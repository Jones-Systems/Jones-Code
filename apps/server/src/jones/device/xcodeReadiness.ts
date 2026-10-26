// @effect-diagnostics preferSchemaOverJson:off - Fixed messages are embedded as string literals in standalone SSH JavaScript.
import type { DevicePlatformAvailability } from "@t3tools/contracts";
import { isCommandAvailable } from "@t3tools/shared/shell";
import * as HostProcess from "@t3tools/shared/HostProcess";
import * as Duration from "effect/Duration";
import * as Effect from "effect/Effect";

import * as ProcessRunner from "../../processRunner.ts";

const messages = {
  required:
    "Xcode setup is incomplete. Open Xcode on the device host and finish installing its required components, then refresh devices.",
  missing: "Xcode command line tools were not found. Install and select Xcode on the device host.",
  timeout: "The Xcode setup check timed out. Check Xcode on the device host, then refresh devices.",
  unavailable: "Could not verify Xcode setup. Check the selected Xcode on the device host, then refresh devices.",
};

// simctl can invoke first-launch installation itself and wait indefinitely for authorization.
export const checkXcodeSetup = Effect.gen(function* () {
  if (!(yield* isCommandAvailable("xcodebuild"))) return messages.missing;
  const runner = yield* ProcessRunner.ProcessRunner;
  const environment = yield* HostProcess.Environment;
  return yield* runner
    .run({
      command: "xcodebuild",
      args: ["-checkFirstLaunchStatus"],
      env: environment,
      timeout: Duration.seconds(5),
      timeoutBehavior: "timedOutResult",
    })
    .pipe(
      Effect.map((result) =>
        result.timedOut ? messages.timeout : result.code === 0 ? null : messages.required,
      ),
      Effect.orElseSucceed(() => messages.unavailable),
    );
});

export const availableHubPlatform = (platforms: ReadonlyArray<DevicePlatformAvailability>) => {
  const available = platforms.filter((platform) => platform.available);
  return available.length === 1 ? available[0]!.platform : undefined;
};

export const unavailablePlatformReason = (platforms: ReadonlyArray<DevicePlatformAvailability>) =>
  platforms.map((platform) => platform.reason).filter(Boolean).join("\n") ||
  "No device platforms are available on this host.";

// The SSH host executes this beside its existing bounded spawnSync runner.
export const remoteXcodeReadinessSource = `
const checkXcodeSetup = () => {
  const result = run('xcodebuild', ['-checkFirstLaunchStatus'], { timeout: 5000 });
  if (result.error?.code === 'ENOENT') return ${JSON.stringify(messages.missing)};
  if (result.error?.code === 'ETIMEDOUT') return ${JSON.stringify(messages.timeout)};
  if (result.error) return ${JSON.stringify(messages.unavailable)};
  return result.status === 0 ? null : ${JSON.stringify(messages.required)};
};
`;
