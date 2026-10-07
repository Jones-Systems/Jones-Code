import * as Console from "effect/Console";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";
import { Command, Flag } from "effect/unstable/cli";
import { FetchHttpClient } from "effect/unstable/http";

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
    Command.make("host").pipe(Command.withSubcommands([stage, setup, status, routeRemove])),
  ]),
);

export const layer = HostService.layer.pipe(
  Layer.provide(HostServiceConfig.layer),
  Layer.provide(ProcessRunner.layer),
  Layer.provide(FetchHttpClient.layer),
);
