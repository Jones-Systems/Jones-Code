// @effect-diagnostics nodeBuiltinImport:off -- Host observations are capabilities, captured once by each retirement owner.
import type { SqlError } from "effect/sql/SqlError";
import * as NodeOS from "node:os";
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import type * as SqlClient from "effect/sql/SqlClient";
import { processProbe, nativeProcessIO, type Probe, type ProcessProbeIO } from "./ConsumerProbe.ts";
import { captureLinuxProcess, verifyLinuxInfrastructure } from "./LinuxInfrastructureEvidence.ts";
import { nativePlatform } from "./Filesystem.ts";
import { sharedNativeServices, nativeWorkloads } from "./NativeConsumers.ts";

export function nativeProcesses(
  target: string,
  platform = nativePlatform,
  io: ProcessProbeIO = nativeProcessIO,
) {
  return processProbe(target, platform, {
    capture: captureLinuxProcess,
    verifyInfrastructure: verifyLinuxInfrastructure,
    ...io,
  });
}

export class NativeObservations extends Context.Reference<{
  readonly services: (target: string) => Promise<Probe>;
  readonly processes: (target: string) => Promise<Probe>;
  readonly workloads: (sql: SqlClient.SqlClient, target: string) => Effect.Effect<Probe, SqlError>;
  readonly temporaryRoots: readonly string[];
}>("jones/workspaceRetirement/NativeObservations", {
  defaultValue: () => ({
    services: sharedNativeServices,
    processes: nativeProcesses,
    workloads: nativeWorkloads,
    temporaryRoots: [NodeOS.tmpdir(), "/tmp", "/private/tmp", "/var/tmp", "/private/var/tmp"],
  }),
}) {}
