import * as NodeServices from "@effect/platform-node/NodeServices";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Queue from "effect/Queue";
import * as Sink from "effect/Sink";
import * as Stream from "effect/Stream";
import { ChildProcess, ChildProcessSpawner } from "effect/process";

import * as ServerConfig from "../../config.ts";
import { makeGitVcsDriverCore } from "../../vcs/GitVcsDriverCore.ts";

export const dependencies = ServerConfig.layerTest(process.cwd(), {
  prefix: "background-git-admission-test-",
}).pipe(Layer.provideMerge(NodeServices.layer));

export const makeFixture = Effect.fn("GitAdmissionFixture.make")(function* (
  labels: readonly string[],
) {
  const starts = yield* Queue.unbounded<string>();
  const gates = new Map(
    yield* Effect.forEach(labels, (label) =>
      Deferred.make<void>().pipe(Effect.map((gate) => [label, gate] as const)),
    ),
  );
  let activeBackground = 0;
  let peakBackground = 0;
  const started: string[] = [];
  const releaseAll = Effect.forEach(gates.values(), (gate) => Deferred.succeed(gate, undefined), {
    discard: true,
  });
  yield* Effect.addFinalizer(() => releaseAll);
  const spawner = ChildProcessSpawner.make((command) =>
    Effect.gen(function* () {
      if (!ChildProcess.isStandardCommand(command)) {
        return yield* Effect.die("expected a standard Git command");
      }
      const label = command.args[0]!;
      const gate = gates.get(label);
      if (gate === undefined) return yield* Effect.die(`unexpected Git command: ${label}`);
      started.push(label);
      const background = label.startsWith("background-");
      if (background) {
        activeBackground++;
        peakBackground = Math.max(peakBackground, activeBackground);
      }
      yield* Queue.offer(starts, label);
      return ChildProcessSpawner.makeHandle({
        pid: ChildProcessSpawner.ProcessId(1),
        exitCode: Deferred.await(gate).pipe(
          Effect.as(ChildProcessSpawner.ExitCode(0)),
          Effect.ensuring(
            Effect.sync(() => {
              if (background) activeBackground--;
            }),
          ),
        ),
        isRunning: Effect.succeed(true),
        kill: () => Deferred.succeed(gate, undefined).pipe(Effect.asVoid),
        unref: Effect.succeed(Effect.void),
        stdin: Sink.drain,
        stdout: Stream.empty,
        stderr: Stream.empty,
        all: Stream.empty,
        getInputFd: () => Sink.drain,
        getOutputFd: () => Stream.empty,
      });
    }),
  );
  const driver = yield* makeGitVcsDriverCore().pipe(
    Effect.provideService(ChildProcessSpawner.ChildProcessSpawner, spawner),
  );
  return {
    starts,
    started,
    peakBackground: () => peakBackground,
    release: (label: string) => Deferred.succeed(gates.get(label)!, undefined),
    releaseAll,
    run: (label: string, timeoutMs?: number | null, operation = "test.backgroundGitAdmission") =>
      driver.execute({
        cwd: "/workspace/background-git-admission",
        operation,
        args: [label],
        ...(timeoutMs === undefined ? {} : { timeoutMs }),
      }),
  };
});
