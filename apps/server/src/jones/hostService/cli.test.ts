import * as NodeServices from "@effect/platform-node/NodeServices";
import { expect, it } from "@effect/vitest";
import * as Console from "effect/Console";
import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";
import { Command } from "effect/unstable/cli";

import * as HostService from "./HostService.ts";
import { jonesCommand } from "./cli.ts";

const baseDir = "/task-owned/jones-host";
const decodeUnknownJson = Schema.decodeEffect(Schema.fromJsonString(Schema.Unknown));
const plan: HostService.HostPlan = {
  baseDir,
  config: { schema: 1, port: 4321, tailscaleServePort: 8443 },
  effects: ["Write Jones config.", "Reconcile jones-code.service."],
  nextStep: `t3 pair --base-dir '${baseDir}' --tailscale --tailscale-serve-port 8443`,
};

function testService() {
  const calls: Array<{ readonly method: string; readonly input: unknown }> = [];
  const printed: string[] = [];
  const record = <A>(method: string, input: unknown, result: A) =>
    Effect.sync(() => {
      calls.push({ method, input });
      return result;
    });
  const service = HostService.HostService.of({
    plan: (input) => record("plan", input, plan),
    setup: (input) =>
      Effect.gen(function* () {
        expect(printed[0]).toContain("Jones host effect plan:");
        return yield* record("setup", input, {
          state: input.dryRun ? ("dry-run" as const) : ("installed" as const),
          plan,
          warnings: [],
        });
      }),
    stageRuntime: (input) =>
      record("stage-runtime", input, {
        versionDir: "/task-owned/runtime/1.0.0",
        entryPath: "/task-owned/runtime/1.0.0/t3",
        sentinelPath: "/task-owned/runtime/1.0.0/.install-complete",
      }),
    routeRemove: (input) =>
      record("route-remove", input, {
        state: input.dryRun ? ("dry-run" as const) : ("removed" as const),
        mapping: { _tag: "absent" as const },
      }),
    status: (input) =>
      record("status", input, {
        baseDir,
        provenance: "missing" as const,
        config: "missing" as const,
        service: "unknown" as const,
        serviceProblems: [],
        runtime: "missing" as const,
        serve: "not-configured" as const,
        serveEndpoint: { environmentMatch: "unknown" as const, scope: "host-side-only" as const },
        foreignUpstreamUnit: { present: false, managed: false as const },
      }),
  });
  return {
    calls,
    printed,
    run: (args: ReadonlyArray<string>) =>
      Effect.gen(function* () {
        const console = yield* Console.Console;
        return yield* Command.runWith(jonesCommand, { version: "1.0.0" })(args).pipe(
          Effect.provideService(HostService.HostService, service),
          Effect.provideService(Console.Console, {
            ...console,
            log: (...args: ReadonlyArray<unknown>) => {
              printed.push(args.map(String).join(" "));
            },
          }),
          Effect.provide(NodeServices.layer),
        );
      }),
  };
}

it.effect("setup prints the effect plan before dispatch and exposes bounded flags", () =>
  Effect.gen(function* () {
    const t = testService();
    yield* t.run([
      "host",
      "setup",
      "--base-dir",
      baseDir,
      "--port",
      "4321",
      "--tailscale-serve-port",
      "8443",
      "--allow-linger-enable",
      "--dry-run",
      "--artifact-dir",
      "/approved/artifact",
      "--expect-source-commit",
      "a".repeat(40),
    ]);
    expect(t.calls.map((call) => call.method)).toEqual(["plan", "setup"]);
    expect(t.calls[1]?.input).toEqual({
      baseDir,
      port: 4321,
      tailscaleServePort: 8443,
      allowLingerEnable: true,
      dryRun: true,
      artifactDir: "/approved/artifact",
      expectSourceCommit: "a".repeat(40),
    });
    expect(t.printed.at(-1)).toContain("Next: t3 pair");
    expect(t.printed.at(-1)).toContain("dry-run");
  }),
);

it.effect.each(["setup", "status", "stage-runtime", "route-remove"] as const)(
  "%s requires explicit --base-dir before dispatch",
  (command) =>
    Effect.gen(function* () {
      const t = testService();
      yield* t.run(["host", command]).pipe(Effect.flip);
      expect(t.calls).toEqual([]);
    }),
);

it.effect("stage-runtime calls the local verified runtime service", () =>
  Effect.gen(function* () {
    const t = testService();
    yield* t.run([
      "host",
      "stage-runtime",
      "--base-dir",
      baseDir,
      "--artifact-dir",
      "/approved/artifact",
      "--expect-source-commit",
      "a".repeat(40),
    ]);
    expect(t.calls).toEqual([
      {
        method: "stage-runtime",
        input: { baseDir, artifactDir: "/approved/artifact", expectSourceCommit: "a".repeat(40) },
      },
    ]);
  }),
);

it.effect("status --json returns the domain's observed status", () =>
  Effect.gen(function* () {
    const t = testService();
    yield* t.run(["host", "status", "--base-dir", baseDir, "--json"]);
    expect(t.calls.map((call) => call.method)).toEqual(["status"]);
    const observed = yield* decodeUnknownJson(t.printed.at(-1)!);
    expect(observed).toMatchObject({
      provenance: "missing",
      service: "unknown",
      foreignUpstreamUnit: { managed: false },
    });
  }),
);

it.effect("route-remove forwards dry-run without exposing a route override", () =>
  Effect.gen(function* () {
    const t = testService();
    yield* t.run(["host", "route-remove", "--base-dir", baseDir, "--dry-run"]);
    expect(t.calls).toEqual([{ method: "route-remove", input: { baseDir, dryRun: true } }]);
    expect(t.printed.at(-1)).toContain("dry-run");
  }),
);

it.effect("there is no Jones host pair command", () =>
  Effect.gen(function* () {
    const t = testService();
    yield* t.run(["host", "pair", "--base-dir", baseDir]).pipe(Effect.flip);
    expect(t.calls).toEqual([]);
  }),
);
