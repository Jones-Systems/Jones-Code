import * as Console from "effect/Console";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";
import { Command, Flag } from "effect/cli";
import { FetchHttpClient } from "effect/http";

import * as ProcessRunner from "../../processRunner.ts";
import * as HostService from "./HostService.ts";
import * as HostServiceConfig from "./HostServiceConfig.ts";

const encodeStatusJson = Schema.encodeEffect(Schema.fromJsonString(Schema.Unknown, { space: 2 }));
const encodeUnknownJson = Schema.encodeEffect(Schema.fromJsonString(Schema.Unknown));

const baseFlags = {
  baseDir: Flag.String("base-dir").pipe(
    Flag.withDescription("Explicit absolute Jones service home."),
  ),
};
const dryRun = Flag.Boolean("dry-run").pipe(Flag.withDefault(false));
const artifactFlags = {
  artifactDir: Flag.String("artifact-dir"),
  expectSourceCommit: Flag.String("expect-source-commit"),
};

const stage = Command.make("stage-runtime", { ...baseFlags, ...artifactFlags }).pipe(
  Command.withDescription("Stage only a verified local Jones runtime artifact."),
  Command.withHandler((input) =>
    Effect.gen(function* () {
      const service = yield* HostService.HostService;
      const result = yield* service.stageRuntime(input);
      yield* Console.log(`Verified Jones runtime staged at ${result.versionDir}`);
    }),
  ),
);
const setup = Command.make("setup", {
  ...baseFlags,
  port: Flag.Int("port"),
  artifactDir: artifactFlags.artifactDir.pipe(Flag.optional),
  expectSourceCommit: artifactFlags.expectSourceCommit.pipe(Flag.optional),
  tailscaleServePort: Flag.Int("tailscale-serve-port").pipe(Flag.optional),
  allowLingerEnable: Flag.Boolean("allow-linger-enable").pipe(Flag.withDefault(false)),
  dryRun,
}).pipe(
  Command.withDescription("Plan and reconcile the Jones user service with a stable loopback port."),
  Command.withHandler((flags) =>
    Effect.gen(function* () {
      const service = yield* HostService.HostService;
      const input = {
        baseDir: flags.baseDir,
        port: flags.port,
        dryRun: flags.dryRun,
        allowLingerEnable: flags.allowLingerEnable,
        ...(Option.isSome(flags.artifactDir) ? { artifactDir: flags.artifactDir.value } : {}),
        ...(Option.isSome(flags.expectSourceCommit)
          ? { expectSourceCommit: flags.expectSourceCommit.value }
          : {}),
        ...(Option.isSome(flags.tailscaleServePort)
          ? { tailscaleServePort: flags.tailscaleServePort.value }
          : {}),
      };
      const plan = yield* service.plan(input);
      yield* Console.log(
        ["Jones host effect plan:", ...plan.effects.map((effect) => `  ${effect}`)].join("\n"),
      );
      const result = yield* service.setup(input);
      yield* Console.log(
        [result.state, ...result.warnings, `Next: ${result.plan.nextStep}`].join("\n"),
      );
    }),
  ),
);
const adopt = Command.make("adopt", {
  ...baseFlags,
  activeArtifactDir: Flag.String("active-artifact-dir"),
  activeSourceCommit: Flag.String("active-source-commit"),
  launcherArtifactDir: Flag.String("launcher-artifact-dir"),
  launcherSourceCommit: Flag.String("launcher-source-commit"),
  supersedeExecstart: Flag.Boolean("supersede-execstart").pipe(Flag.withDefault(false)),
  legacyDirectServe: Flag.Boolean("legacy-direct-serve").pipe(Flag.withDefault(false)),
  acceptUnattestedChildCapability: Flag.Boolean("accept-unattested-child-capability").pipe(
    Flag.withDefault(false),
  ),
  serviceUnit: Flag.Literals("service-unit", ["jones-code.service", "t3code.service"]).pipe(
    Flag.optional,
  ),
  serviceUnitSha256: Flag.String("service-unit-sha256").pipe(Flag.optional),
  taskOperationId: Flag.String("task-operation-id").pipe(Flag.optional),
  taskHandoffSha256: Flag.String("task-handoff-sha256").pipe(Flag.optional),
  taskDropin: Flag.String("task-dropin").pipe(Flag.optional),
  preserveDropin: Flag.String("preserve-dropin").pipe(Flag.atLeast(0)),
  dryRun,
}).pipe(
  Command.withDescription(
    "Enroll exact Actions artifacts and adopt a qualified launcher while retaining the active server version.",
  ),
  Command.withHandler((flags) =>
    Effect.gen(function* () {
      const input = {
        baseDir: flags.baseDir,
        activeArtifactDir: flags.activeArtifactDir,
        activeSourceCommit: flags.activeSourceCommit,
        launcherArtifactDir: flags.launcherArtifactDir,
        launcherSourceCommit: flags.launcherSourceCommit,
        dryRun: flags.dryRun,
        supersedeExecstart: flags.supersedeExecstart,
        legacyDirectServe: flags.legacyDirectServe,
        acceptUnattestedChildCapability: flags.acceptUnattestedChildCapability,
        preserveDropin: flags.preserveDropin,
        ...(Option.isSome(flags.serviceUnit) ? { serviceUnit: flags.serviceUnit.value } : {}),
        ...(Option.isSome(flags.serviceUnitSha256)
          ? { serviceUnitSha256: flags.serviceUnitSha256.value }
          : {}),
        ...(Option.isSome(flags.taskOperationId)
          ? { taskOperationId: flags.taskOperationId.value }
          : {}),
        ...(Option.isSome(flags.taskHandoffSha256)
          ? { taskHandoffSha256: flags.taskHandoffSha256.value }
          : {}),
        ...(Option.isSome(flags.taskDropin) ? { taskDropin: flags.taskDropin.value } : {}),
      };
      const service = yield* HostService.HostService;
      const plan = yield* service.planAdoption(input);
      yield* Console.log(
        ["Jones host adoption effect plan:", ...plan.effects.map((effect) => `  ${effect}`)].join(
          "\n",
        ),
      );
      if (plan.recovery !== undefined)
        yield* Console.log(["Recovery requires separate review:", ...plan.recovery].join("\n"));
      const result = yield* service.adopt(input);
      yield* Console.log(
        `${result.state}: launcher ${result.plan.launcherVersion}; active server ${result.plan.activeVersion}`,
      );
    }),
  ),
);
const status = Command.make("status", {
  ...baseFlags,
  json: Flag.Boolean("json").pipe(Flag.withDefault(false)),
}).pipe(
  Command.withDescription("Read Jones service, provenance, runtime and Serve ownership status."),
  Command.withHandler((input) =>
    Effect.gen(function* () {
      const service = yield* HostService.HostService;
      const result = yield* service.status(input);
      const statusJson = input.json ? yield* encodeStatusJson(result) : undefined;
      const sleepJson =
        result.mac === undefined ? undefined : yield* encodeUnknownJson(result.mac.sleep);
      yield* Console.log(
        input.json
          ? statusJson
          : [
              `Jones host: ${result.baseDir}`,
              `Runtime: ${result.activeVersion ?? "missing"} · provenance ${result.provenance}`,
              `Jones service: ${result.service === "unknown" ? "unknown" : result.service.installed ? (result.service.current ? "current" : "needs reconciliation") : "not installed"}`,
              ...result.serviceProblems.map((problem) => `  ${problem}`),
              `Config: ${typeof result.config === "string" ? result.config : `loopback ${result.config.port}, Serve ${result.config.tailscaleServePort ?? "not claimed"}`}`,
              `Live runtime: ${typeof result.runtime === "string" ? result.runtime : `pid ${result.runtime.pid}, port ${result.runtime.port}, serviceManaged ${result.runtime.serviceManaged === true}, environmentId ${result.runtime.environmentId ?? "unknown"}`}`,
              `Serve mapping: ${typeof result.serve === "string" ? result.serve : `${result.serve._tag}${"reason" in result.serve ? ` (${result.serve.reason})` : ""}`}`,
              `Serve descriptor environment match: ${result.serveEndpoint.environmentMatch} (host side only; remote reach unverified)`,
              `Foreign upstream unit: ${result.foreignUpstreamUnit.present} · not managed`,
              ...(result.mac === undefined
                ? []
                : [
                    `Mac: ${result.mac.consoleUser}; loaded ${result.mac.loaded}; running ${result.mac.running}; auto-login ${result.mac.autoLogin}; sleep ${sleepJson}`,
                  ]),
            ].join("\n"),
      );
    }),
  ),
);
const routeRemove = Command.make("route-remove", { ...baseFlags, dryRun }).pipe(
  Command.withDescription("Remove only the exact Serve mapping recorded in Jones host config."),
  Command.withHandler((input) =>
    Effect.gen(function* () {
      const service = yield* HostService.HostService;
      const result = yield* service.routeRemove(input);
      yield* Console.log(`Jones route: ${result.state} · observed ${result.mapping._tag}`);
    }),
  ),
);

export const jonesCommand = Command.make("jones").pipe(
  Command.withSubcommands([
    Command.make("host").pipe(Command.withSubcommands([stage, setup, adopt, status, routeRemove])),
  ]),
);

export const layer = HostService.layer.pipe(
  Layer.provide(HostServiceConfig.layer),
  Layer.provide(ProcessRunner.layer),
  Layer.provide(FetchHttpClient.layer),
);
