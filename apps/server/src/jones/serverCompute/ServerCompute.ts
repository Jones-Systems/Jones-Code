// @effect-diagnostics nodeBuiltinImport:off -- Node supervisor is a scoped server capability; no native package loads here.
import * as NodePath from "node:path";
import { AgentScope } from "@t3tools/shared/AgentScope";
import * as HostProcess from "@t3tools/shared/HostProcess";
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import type * as Scope from "effect/Scope";
import * as ServerConfig from "../../config.ts";
import type {
  LogicalFinderHandle,
  WorkspaceJobs,
  WorkspaceKey,
} from "../serverParallelism/computeContract.ts";
import type { ComputeObservation } from "../serverParallelism/observations.ts";
import { ComputeFailure, computeFailure, isComputeFailure } from "./failure.ts";
import { GenerationJournal } from "./journal.ts";
import { ProcessSlot } from "./processSlot.ts";
import { ComputeSupervisor } from "./supervisor.ts";
import { resolveWorkerCommand } from "./workerCommand.ts";
export { ComputeFailure } from "./failure.ts";

export class ServerCompute extends Context.Service<
  ServerCompute,
  {
    readonly acquireLogical: (
      key: WorkspaceKey,
    ) => Effect.Effect<LogicalFinderHandle, ComputeFailure, Scope.Scope>;
    readonly run: <K extends keyof WorkspaceJobs>(
      handle: LogicalFinderHandle,
      operation: K,
      input: WorkspaceJobs[K]["input"],
    ) => Effect.Effect<WorkspaceJobs[K]["output"], ComputeFailure>;
  }
>()("t3/jones/serverCompute/ServerCompute") {}

/** Deliberate optional callback seam; root supplies instrumentation through the single shared layer. */
export const ComputeObserver = Context.Reference<((event: ComputeObservation) => void) | undefined>(
  "t3/jones/ServerCompute/observer",
  { defaultValue: () => undefined },
);
const failure = (cause: unknown) =>
  isComputeFailure(cause) ? cause : computeFailure("unavailable", cause);
const make = Effect.gen(function* () {
  const config = yield* ServerConfig.ServerConfig;
  const agentScope = yield* AgentScope;
  const observe = yield* ComputeObserver;
  const platform = yield* HostProcess.Platform;
  const command = yield* Effect.tryPromise({
    try: () => resolveWorkerCommand(platform),
    catch: failure,
  });
  // Existing placement/scope policy only. No provider category, core cap or shared scope setting changes.
  const commands = yield* Effect.forEach(
    Array.from({ length: config.serverParallelism.computeWorkerCount }, (_, slot) => slot),
    (slot) =>
      agentScope
        .wrap({
          command: command.file,
          args: command.args,
          env: command.env,
          name: `server-compute-${slot}`,
        })
        .pipe(Effect.map((wrapped) => ({ ...command, file: wrapped.command, args: wrapped.args }))),
  );
  const supervisor = yield* Effect.acquireRelease(
    Effect.tryPromise({
      try: async () => {
        const journal = await GenerationJournal.acquire(
          NodePath.join(config.stateDir, "server-compute"),
          config.serverParallelism.computeWorkerCount,
          platform,
        ).catch((cause) => {
          throw computeFailure("retirement-unknown", cause);
        });
        return new ComputeSupervisor({
          count: config.serverParallelism.computeWorkerCount,
          build: command.build,
          journal,
          ...(observe === undefined ? {} : { observe }),
          start: (boot, register, onExit) =>
            ProcessSlot.start(commands[boot.slotId]!, boot, register, onExit, observe),
        });
      },
      catch: failure,
    }),
    (pool) => Effect.tryPromise({ try: () => pool.shutdown(), catch: failure }).pipe(Effect.orDie),
  );
  return ServerCompute.of({
    acquireLogical: Effect.fn("ServerCompute.acquireLogical")(function* (key: WorkspaceKey) {
      const handle = yield* Effect.acquireRelease(
        Effect.try({ try: () => supervisor.register(key), catch: failure }),
        (handle) => Effect.sync(() => supervisor.release(handle.handleId)),
      );
      yield* Effect.tryPromise({
        try: (signal) => supervisor.ensure(handle, signal),
        catch: failure,
      }).pipe(Effect.onError(() => Effect.sync(() => supervisor.release(handle.handleId))));
      return handle;
    }),
    run: Effect.fn("ServerCompute.run")(
      <K extends keyof WorkspaceJobs>(
        handle: LogicalFinderHandle,
        operation: K,
        input: WorkspaceJobs[K]["input"],
      ) =>
        Effect.tryPromise({
          try: (signal) => supervisor.run(handle, operation, input, signal),
          catch: failure,
        }),
    ),
  });
});
/** Root must provide this same layer object to every workspace consumer. */
export const layer = Layer.effect(ServerCompute, make);
