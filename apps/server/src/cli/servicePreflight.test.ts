import * as NodeServices from "@effect/platform-node/NodeServices";
import { expect, it } from "@effect/vitest";
import * as Console from "effect/Console";
import * as Effect from "effect/Effect";
import { Command } from "effect/cli";
import { beforeEach, vi } from "vite-plus/test";

import { servicePreflightCommand } from "./servicePreflight.ts";

const candidate = vi.hoisted(() => ({
  upstream: [
    [1, "Initial"],
    [96, "NormalizeGitRemotes"],
  ] as const,
  jones: [
    [1, "WorktreeOwnershipLeases", "first-effect"],
    [156, "RemoteSendReceipts", "last-effect"],
  ] as const,
  readMigrationPlan: vi.fn(),
  readJonesStartupGateProtocol: vi.fn<() => 1>(),
  runServicePreflight: vi.fn(),
}));

vi.mock("../persistence/Migrations.ts", () => ({ migrationManifest: candidate.upstream }));
vi.mock("../jones/persistence/JonesMigrations.ts", () => ({
  jonesMigrationEntries: candidate.jones,
}));
vi.mock("../jones/updates/migrationPlan.ts", () => ({
  readMigrationPlan: candidate.readMigrationPlan,
}));
vi.mock("../jones/updates/trialStartup.ts", () => ({
  readJonesStartupGateProtocol: candidate.readJonesStartupGateProtocol,
}));
vi.mock("../jones/cloud/qualifiedRuntime.ts", () => ({ QUALIFIED_RECOVERY_PROTOCOL: 1 }));
vi.mock("../cloud/servicePreflight.ts", () => ({
  runServicePreflight: candidate.runServicePreflight,
}));

const databasePath = "/candidate-fixture/state.sqlite";
const plan = {
  status: "ready",
  migrationPlan: { pendingUpstream: [85, 96], pendingJones: [156] },
} as const;

beforeEach(() => {
  vi.clearAllMocks();
  candidate.readMigrationPlan.mockReturnValue(plan);
  candidate.readJonesStartupGateProtocol.mockReturnValue(1);
});

const runPreflight = (launcherProtocol: number) =>
  Effect.gen(function* () {
    const output: string[] = [];
    yield* Command.runWith(servicePreflightCommand, { version: "fixture-version" })([
      "--database-path",
      databasePath,
      "--launcher-protocol",
      String(launcherProtocol),
    ]).pipe(
      Effect.provide(NodeServices.layer),
      Effect.provideService(Console.Console, {
        ...globalThis.console,
        log: (...args: ReadonlyArray<unknown>) => {
          output.push(args.join(" "));
        },
      }),
    );
    return output;
  });

it.effect("protocol 3 passes the candidate plan without qualified claims", () =>
  Effect.gen(function* () {
    const result = {
      status: "ready",
      version: "fixture-version",
      launcherProtocol: 3,
      migrationPlan: plan.migrationPlan,
    };
    candidate.runServicePreflight.mockReturnValue(result);
    expect(yield* runPreflight(3)).toEqual([JSON.stringify(result)]);
    expect(candidate.readMigrationPlan).toHaveBeenCalledTimes(1);
    expect(candidate.readMigrationPlan).toHaveBeenCalledWith({
      databasePath,
      upstream: candidate.upstream,
      jones: [
        [1, "WorktreeOwnershipLeases"],
        [156, "RemoteSendReceipts"],
      ],
    });
    expect(candidate.readMigrationPlan.mock.calls[0]?.[0].upstream).toBe(candidate.upstream);
    expect(candidate.runServicePreflight).toHaveBeenCalledTimes(1);
    expect(candidate.runServicePreflight).toHaveBeenCalledWith({
      databasePath,
      launcherProtocol: 3,
      migrationPlanResult: plan,
    });
    expect(candidate.runServicePreflight.mock.calls[0]?.[0].migrationPlanResult).toBe(plan);
    expect(candidate.readJonesStartupGateProtocol).not.toHaveBeenCalled();
  }),
);

it.effect("protocol 4 reads the startup gate and supplies recovery capability", () =>
  Effect.gen(function* () {
    const result = {
      status: "ready",
      version: "fixture-version",
      launcherProtocol: 4,
      startupGateProtocol: 1,
      qualifiedRecoveryProtocol: 1,
      migrationPlan: plan.migrationPlan,
    };
    candidate.runServicePreflight.mockReturnValue(result);
    expect(yield* runPreflight(4)).toEqual([JSON.stringify(result)]);
    expect(candidate.readJonesStartupGateProtocol).toHaveBeenCalledTimes(1);
    expect(candidate.readJonesStartupGateProtocol).toHaveBeenCalledWith();
    expect(candidate.readMigrationPlan).toHaveBeenCalledTimes(1);
    expect(candidate.readMigrationPlan).toHaveBeenCalledWith({
      databasePath,
      upstream: candidate.upstream,
      jones: [
        [1, "WorktreeOwnershipLeases"],
        [156, "RemoteSendReceipts"],
      ],
    });
    expect(candidate.runServicePreflight).toHaveBeenCalledTimes(1);
    expect(candidate.runServicePreflight).toHaveBeenCalledWith({
      databasePath,
      launcherProtocol: 4,
      startupGateProtocol: 1,
      qualifiedRecoveryProtocol: 1,
      migrationPlanResult: plan,
    });
  }),
);

it.effect("forwards blocked ledger inspection and prints the service's refusal", () =>
  Effect.gen(function* () {
    const blocked = { status: "blocked", reason: "Jones migration history has a missing prefix." };
    const result = { ...blocked, version: "fixture-version" };
    candidate.readMigrationPlan.mockReturnValue(blocked);
    candidate.runServicePreflight.mockReturnValue(result);
    expect(yield* runPreflight(4)).toEqual([JSON.stringify(result)]);
    expect(candidate.runServicePreflight).toHaveBeenCalledTimes(1);
    expect(candidate.runServicePreflight).toHaveBeenCalledWith({
      databasePath,
      launcherProtocol: 4,
      startupGateProtocol: 1,
      qualifiedRecoveryProtocol: 1,
      migrationPlanResult: blocked,
    });
    expect(candidate.runServicePreflight.mock.calls[0]?.[0].migrationPlanResult).toBe(blocked);
  }),
);
