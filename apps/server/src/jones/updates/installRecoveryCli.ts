import * as NodeOS from "node:os";

import { EnvironmentId } from "@t3tools/contracts";
import {
  JONES_INSTALL_PLAN_CLI_RESPONSE_BYTES,
  JonesInstallPlanCliResponse,
  confirmsInstallPlanCancellation,
  findInstallPlanResultPlan,
  type JonesInstallPlanCliRequest,
} from "@t3tools/contracts/jones/installPlan";
import { JonesOperationId } from "@t3tools/contracts/jones/jonesOperations";
import { resolveDesktopAppControlAddress } from "@t3tools/shared/desktopAppControl";
import * as HostProcess from "@t3tools/shared/HostProcess";
import * as Config from "effect/Config";
import * as Console from "effect/Console";
import * as SecureRandom from "effect/crypto/SecureRandom";
import * as Effect from "effect/Effect";
import * as Option from "effect/Option";
import * as Path from "effect/Path";
import * as Schema from "effect/Schema";
import { Argument, Command, Flag } from "effect/cli";

import { sendDesktopAppControlRequest } from "../../cli/app.ts";
import { projectLocationFlags } from "../../cli/config.ts";
import { resolveBaseDir } from "../../os-jank.ts";
import { InstallRecoveryError, runHostRecovery } from "./installRecoveryClient.ts";

const common = { ...projectLocationFlags, json: Flag.Boolean("json") };
const environmentId = Flag.String("environment-id").pipe(Flag.withSchema(EnvironmentId));
const operationId = Argument.String("operation-id").pipe(Argument.withSchema(JonesOperationId));
const confirmed = Flag.Boolean("confirm").pipe(
  Flag.withDescription("Confirm this exact recovery action."),
);
const operationFlags = { ...common, environmentId, operationId };
const print = (json: boolean, value: unknown) =>
  Console.log(JSON.stringify(value, undefined, json ? undefined : 2));
const requireConfirmation = (confirm: boolean) =>
  confirm
    ? Effect.void
    : Effect.fail(
        new InstallRecoveryError({ detail: "Review the exact target, then pass --confirm." }),
      );

const status = Command.make("status", common).pipe(
  Command.withDescription(
    "Read installed build, updater capabilities, and recent progress on this host.",
  ),
  Command.withHandler((flags) =>
    runHostRecovery(flags, { type: "status" }).pipe(
      Effect.flatMap((result) => print(flags.json, result)),
    ),
  ),
);

const operationCommands = [
  { name: "operation", description: "Read the retained receipt for one update attempt." },
  {
    name: "refresh-outcome",
    description: "Read the native launcher outcome and reconcile the exact operation.",
  },
  {
    name: "inspect-cleanup",
    description: "Check whether native cleanup can be proved for one operation.",
  },
] as const;

const operations = operationCommands.map(({ name, description }) =>
  Command.make(name, operationFlags).pipe(
    Command.withDescription(description),
    Command.withHandler((flags) =>
      runHostRecovery(flags, {
        type: name,
        environmentId: flags.environmentId,
        operationId: flags.operationId,
      }).pipe(Effect.flatMap((result) => print(flags.json, result))),
    ),
  ),
);

const stop = Command.make("stop", {
  ...operationFlags,
  stopId: Flag.String("stop-id").pipe(Flag.withSchema(JonesOperationId)),
  confirm: confirmed,
}).pipe(
  Command.withDescription(
    "Request a stop for this attempt. Reuse the same stop ID after a lost reply.",
  ),
  Command.withHandler((flags) =>
    Effect.gen(function* () {
      yield* requireConfirmation(flags.confirm);
      const result = yield* runHostRecovery(flags, {
        type: "stop",
        environmentId: flags.environmentId,
        operationId: flags.operationId,
        stopId: flags.stopId,
      });
      yield* print(flags.json, result);
      const recorded = "receipt" in result ? result.receipt?.stop : undefined;
      if (
        recorded?.stopId !== flags.stopId ||
        recorded.result === "too-late" ||
        recorded.result === "not-stoppable-phase"
      ) {
        return yield* new InstallRecoveryError({
          detail:
            "The host did not accept this stop request. Review the returned operation receipt.",
        });
      }
    }),
  ),
);

const recoverCleanup = Command.make("recover-cleanup", {
  ...operationFlags,
  jobId: Flag.String("job-id").pipe(Flag.withSchema(JonesOperationId)),
  confirm: confirmed,
}).pipe(
  Command.withDescription(
    "Verify absence of the preparation's native effects and release the exact cleanup hold.",
  ),
  Command.withHandler((flags) =>
    Effect.gen(function* () {
      yield* requireConfirmation(flags.confirm);
      const result = yield* runHostRecovery(flags, {
        type: "recover-cleanup",
        environmentId: flags.environmentId,
        operationId: flags.operationId,
        jobId: flags.jobId,
      });
      yield* print(flags.json, result);
    }),
  ),
);

const isPlanResponse = Schema.is(JonesInstallPlanCliResponse);

const runPlan = Effect.fn("InstallRecovery.runPlan")(function* (
  flags: { readonly baseDir: Option.Option<string>; readonly json: boolean },
  action: JonesInstallPlanCliRequest["action"],
) {
  const platform = yield* HostProcess.Platform;
  const userId = yield* HostProcess.UserId;
  if (platform !== "darwin" || userId === undefined) {
    return yield* new InstallRecoveryError({
      detail: "Run the plan command as the owner of the desktop app on the Laptop.",
    });
  }
  const path = yield* Path.Path;
  const configuredBaseDir = yield* Config.String("T3CODE_HOME").pipe(Config.option);
  const baseDir = yield* resolveBaseDir(
    Option.getOrUndefined(flags.baseDir) ?? Option.getOrUndefined(configuredBaseDir),
  );
  const address = resolveDesktopAppControlAddress({
    stateDir: path.join(baseDir, "userdata"),
    platform,
    tempDir: NodeOS.tmpdir(),
    userId,
    joinPath: path.join,
  });
  const random = yield* SecureRandom.SecureRandom;
  const request: JonesInstallPlanCliRequest = {
    version: 1,
    requestId: yield* random.uuidV4(),
    type: "jones-install-plan",
    action,
  };
  const response = yield* Effect.tryPromise({
    try: () =>
      sendDesktopAppControlRequest({
        address,
        userId,
        request,
        isResponse: isPlanResponse,
        maxResponseBytes: JONES_INSTALL_PLAN_CLI_RESPONSE_BYTES,
      }),
    catch: () =>
      new InstallRecoveryError({
        detail:
          "The selected desktop returned no usable plan response. Check the desktop version and base directory. Read plan status before repeating an action.",
      }),
  });
  if (!response.ok) {
    return yield* new InstallRecoveryError({ detail: response.message });
  }
  const output =
    response.projection === undefined
      ? response.result
      : { ...response.result, projection: response.projection };
  if (response.result.refused !== undefined) {
    yield* print(flags.json, output);
    return yield* new InstallRecoveryError({ detail: response.result.refused.message });
  }
  if (
    action.type === "read" &&
    action.planId !== undefined &&
    findInstallPlanResultPlan(response.result, action.planId) === undefined
  )
    return yield* new InstallRecoveryError({
      detail: "The desktop did not return the requested plan ID.",
    });
  if (action.type === "cancel-plan" && !confirmsInstallPlanCancellation(response.result, action)) {
    return yield* new InstallRecoveryError({
      detail: "Cancellation is unconfirmed. Read plan status again.",
    });
  }
  yield* print(flags.json, output);
});

const planStatus = Command.make("status", {
  ...common,
  planId: Flag.String("plan-id").pipe(Flag.withSchema(JonesOperationId), Flag.optional),
}).pipe(
  Command.withDescription(
    "Read the Laptop's current and last plans, or inspect a retained plan by ID.",
  ),
  Command.withHandler((flags) =>
    runPlan(flags, {
      type: "read",
      ...Option.match(flags.planId, { onNone: () => ({}), onSome: (planId) => ({ planId }) }),
    }),
  ),
);

const planCancel = Command.make("cancel", {
  ...common,
  environmentId,
  planId: Argument.String("plan-id").pipe(Argument.withSchema(JonesOperationId)),
  revision: Flag.Int("revision").pipe(
    Flag.withSchema(Schema.Int.check(Schema.isGreaterThanOrEqualTo(0))),
  ),
  confirm: confirmed,
}).pipe(
  Command.withDescription(
    "Cancel the reviewed plan revision; preserve completed installs and unresolved receipts.",
  ),
  Command.withHandler((flags) =>
    Effect.gen(function* () {
      yield* requireConfirmation(flags.confirm);
      yield* runPlan(flags, {
        type: "cancel-plan",
        confirmed: true,
        environmentId: flags.environmentId,
        planId: flags.planId,
        expectedRevision: flags.revision,
      });
    }),
  ),
);

export const installRecoveryCommand = Command.make("updates").pipe(
  Command.withDescription("Inspect Jones Code updates and act on exact recovery evidence."),
  Command.withSubcommands([
    status,
    ...operations,
    stop,
    recoverCleanup,
    Command.make("plan").pipe(
      Command.withDescription("Inspect the native Laptop rollout or cancel an exact revision."),
      Command.withSubcommands([planStatus, planCancel]),
    ),
  ]),
);
