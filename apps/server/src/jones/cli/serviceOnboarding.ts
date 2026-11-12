import * as BootService from "../../cloud/bootService.ts";
import { compareExactServiceVersions } from "../../cloud/serviceProtocol.ts";
import type { ServiceReconcileResult } from "../../cli/service.ts";
import * as Console from "effect/Console";
import * as Effect from "effect/Effect";
import * as Terminal from "effect/Terminal";
import * as HostProcess from "@t3tools/shared/HostProcess";
import { Prompt } from "effect/cli";

export type ServiceOnboardingOutcome = "ready" | "pending" | "skipped";
export function makeServiceOnboarding<E, R>(input: {
  readonly cliVersion: string;
  readonly reconcileService: () => Effect.Effect<ServiceReconcileResult, E, R>;
  readonly formatServiceChange: (
    result: Extract<ServiceReconcileResult, { readonly changed: true }>,
  ) => string;
}) {
  const offerServiceDuringOnboarding = Effect.gen(function* () {
    const service = yield* BootService.BootService;
    const status = yield* service.status;
    const { supported, installed, current } = status;
    if (!supported) {
      return "skipped" as const;
    }
    if (installed && current) {
      yield* Console.log("T3 Code is already set up to run in the background on this machine.");
      return "ready" as const;
    }
    for (const problem of status.problems ?? []) {
      yield* Console.warn(`[${problem}] ${BootService.formatBootServiceProblem(problem)}`);
    }
    if (
      installed &&
      status.installedVersion !== undefined &&
      compareExactServiceVersions(status.installedVersion, input.cliVersion) > 0
    ) {
      yield* Console.log(
        `A newer t3@${status.installedVersion} background service is installed. Leaving it unchanged.`,
      );
      return "skipped" as const;
    }
    const platform = yield* HostProcess.Platform;
    const wanted = yield* Prompt.run(
      Prompt.Confirm({
        message: installed
          ? "The installed T3 Code service needs an update or repair. Update it now?"
          : platform === "darwin"
            ? "Run T3 Code in the background whenever you log in to this Mac? " +
              "It stays reachable through T3 Connect while you are logged in."
            : "Run T3 Code in the background whenever this machine boots? " +
              "It stays reachable through T3 Connect even after you log out.",
        initial: true,
      }),
    );
    if (!wanted) {
      return "skipped" as const;
    }
    const result = yield* input.reconcileService();
    if (result.changed) {
      yield* Console.log(
        result.plan.handedOff === true
          ? input.formatServiceChange(result)
          : `Background service ${result.previouslyInstalled ? "updated" : "installed"}. Logs: ${result.plan.logPath}`,
      );
    }
    return result.changed && result.plan.handedOff === true
      ? ("pending" as const)
      : ("ready" as const);
  });

  return { offerServiceDuringOnboarding };
}

const reportServiceSetupFailure = (error: BootService.BootServiceError) =>
  Console.warn(`Background setup did not finish: ${error.message}`).pipe(
    Effect.as("skipped" as const),
  );

export const recoverServiceOnboardingOffer = <R>(
  offer: Effect.Effect<
    ServiceOnboardingOutcome,
    BootService.BootServiceError | Terminal.QuitError,
    R
  >,
) =>
  offer.pipe(
    Effect.catchTags({
      QuitError: () => Effect.succeed("skipped" as const),
      BootServiceUnsupportedError: (error) =>
        Console.log(`Skipping background setup: ${error.message}`).pipe(
          Effect.as("skipped" as const),
        ),
      BootServiceCommandError: reportServiceSetupFailure,
      BootServiceInstallError: reportServiceSetupFailure,
      BootServicePrerequisiteError: reportServiceSetupFailure,
      BootServiceBootstrapRequiredError: (error) =>
        Console.warn(error.message).pipe(Effect.as("skipped" as const)),
      BootServiceUpdatePendingError: reportServiceSetupFailure,
      BootServiceDowngradeRefusedError: reportServiceSetupFailure,
    }),
  );
