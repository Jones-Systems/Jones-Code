// @effect-diagnostics nodeBuiltinImport:off -- Host observations are capabilities, captured once by each retirement owner.
import * as NodeOS from "node:os";
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import type * as SqlClient from "effect/sql/SqlClient";
import { processProbe, type Probe } from "./ConsumerProbe.ts";
import { nativeServices, nativeWorkloads } from "./NativeConsumers.ts";

export class NativeObservations extends Context.Reference<{
  readonly services: (target: string) => Promise<Probe>;
  readonly processes: (target: string) => Promise<Probe>;
  readonly workloads: (
    sql: SqlClient.SqlClient["Service"],
    target: string,
  ) => Effect.Effect<Probe, unknown>;
  readonly temporaryRoots: readonly string[];
}>("jones/workspaceRetirement/NativeObservations", {
  defaultValue: () => ({
    services: nativeServices,
    processes: processProbe,
    workloads: nativeWorkloads,
    temporaryRoots: [NodeOS.tmpdir(), "/tmp", "/private/tmp", "/var/tmp", "/private/var/tmp"],
  }),
}) {}
