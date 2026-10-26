import {
  CommandId,
  EnvironmentId,
  MessageId,
  ORCHESTRATION_V2_WS_METHODS,
  ProviderDriverKind,
  ProviderInstanceId,
  ProviderSessionId,
  ProviderThreadId,
  RunId,
  ThreadId,
  type OrchestrationV2Command,
} from "@t3tools/contracts";
import { describe, expect, it } from "@effect/vitest";
import * as Crypto from "effect/Crypto";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Queue from "effect/Queue";
import * as SubscriptionRef from "effect/SubscriptionRef";
import { Atom, AtomRegistry } from "effect/reactivity";
import { vi } from "vite-plus/test";

import * as EnvironmentRegistry from "../../connection/registry.ts";
import * as EnvironmentSupervisor from "../../connection/supervisor.ts";
import type { RpcSession } from "../../rpc/session.ts";
import { createThreadEnvironmentAtoms } from "../../state/threadCommands.ts";

vi.mock("../../state/commandPermissions.ts", () => ({
  createCommandPermissions: () => ({
    authorize: () => Effect.void,
    requiredScopes: () => [],
    permissionAtom: () => Atom.make(true),
  }),
}));

describe("stop observation isolation evidence", () => {
  it.effect("records same-thread Send and interrupt waiting while another thread progresses", () =>
    Effect.gen(function* () {
      const environmentId = EnvironmentId.make("synthetic");
      const threadId = ThreadId.make("held");
      const otherThreadId = ThreadId.make("independent");
      const entered = yield* Deferred.make<void>();
      const release = yield* Deferred.make<void>();
      const requests = yield* Queue.unbounded<{
        command: OrchestrationV2Command;
        reply: Deferred.Deferred<{ sequence: number }>;
      }>();
      const supervisor = EnvironmentSupervisor.EnvironmentSupervisor.of({
        target: { environmentId },
        session: yield* SubscriptionRef.make(
          Option.some({
            client: {
              [ORCHESTRATION_V2_WS_METHODS.observeCurrentThreadRuntimeStop]: () =>
                Deferred.succeed(entered, undefined).pipe(
                  Effect.andThen(Deferred.await(release)),
                  Effect.as(null),
                ),
              [ORCHESTRATION_V2_WS_METHODS.dispatchCommand]: (command: OrchestrationV2Command) =>
                Effect.gen(function* () {
                  const reply = yield* Deferred.make<{ sequence: number }>();
                  yield* Queue.offer(requests, { command, reply });
                  return yield* Deferred.await(reply);
                }),
            },
          } as unknown as RpcSession),
        ),
      } as EnvironmentSupervisor.EnvironmentSupervisor["Service"]);
      const runtime = Atom.runtime(
        Layer.mergeAll(
          Layer.succeed(EnvironmentRegistry.EnvironmentRegistry, {
            run: (_id, effect) =>
              Effect.provideService(
                effect,
                EnvironmentSupervisor.EnvironmentSupervisor,
                supervisor,
              ),
          } as EnvironmentRegistry.EnvironmentRegistry["Service"]),
          Layer.succeed(
            Crypto.Crypto,
            Crypto.make({
              randomBytes: (size) => new Uint8Array(size),
              digest: (_algorithm, data) => Effect.succeed(data),
            }),
          ),
        ),
      );
      const commands = createThreadEnvironmentAtoms(runtime, () => Atom.make(null));
      const registry = AtomRegistry.make();
      yield* Effect.addFinalizer(() => Effect.sync(() => registry.dispose()));
      const observation = commands.observeCurrentThreadRuntimeStop.run(registry, {
        environmentId,
        input: {
          commandId: CommandId.make("observe"),
          threadId,
          target: {
            evidenceRevision: 1,
            binding: {
              threadId,
              providerThreadId: ProviderThreadId.make("provider-thread"),
              providerSessionId: ProviderSessionId.make("provider-session"),
              providerInstanceId: ProviderInstanceId.make("codex"),
              driver: ProviderDriverKind.make("codex"),
              nativeThreadId: "native",
              runtimeGeneration: "generation",
            },
          },
        },
      });
      yield* Deferred.await(entered);
      const send = commands.startTurn.run(registry, {
        environmentId,
        input: {
          threadId,
          commandId: CommandId.make("send"),
          message: {
            messageId: MessageId.make("message"),
            role: "user",
            text: "Continue",
            attachments: [],
          },
          runtimeMode: "full-access",
          interactionMode: "default",
          dispatchMode: "start",
        },
      });
      const interrupt = commands.interruptTurn.run(registry, {
        environmentId,
        input: {
          threadId,
          commandId: CommandId.make("interrupt"),
          runId: RunId.make("run"),
        },
      });
      const independent = commands.interruptTurn.run(registry, {
        environmentId,
        input: {
          threadId: otherThreadId,
          commandId: CommandId.make("independent"),
          runId: RunId.make("other-run"),
        },
      });
      const other = yield* Queue.take(requests);
      expect(other.command).toMatchObject({ type: "run.interrupt", threadId: otherThreadId });
      yield* Deferred.succeed(other.reply, { sequence: 1 });
      expect((yield* Effect.promise(() => independent))._tag).toBe("Success");
      expect(yield* Queue.poll(requests)).toEqual(Option.none());
      yield* Deferred.succeed(release, undefined);
      expect((yield* Effect.promise(() => observation))._tag).toBe("Success");
      const sent = yield* Queue.take(requests);
      expect(sent.command.type).toBe("message.dispatch");
      expect(yield* Queue.poll(requests)).toEqual(Option.none());
      yield* Deferred.succeed(sent.reply, { sequence: 2 });
      expect((yield* Effect.promise(() => send))._tag).toBe("Success");
      const stopped = yield* Queue.take(requests);
      expect(stopped.command.type).toBe("run.interrupt");
      yield* Deferred.succeed(stopped.reply, { sequence: 3 });
      expect((yield* Effect.promise(() => interrupt))._tag).toBe("Success");
    }),
  );
});
