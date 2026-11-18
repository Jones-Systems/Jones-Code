import { expect, it } from "@effect/vitest";
import { EnvironmentId, ProjectId, WS_METHODS } from "@t3tools/contracts";
import type { RepoFreshnessStatus } from "@t3tools/contracts/jones/repoFreshness";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Stream from "effect/Stream";
import * as SubscriptionRef from "effect/SubscriptionRef";
import { Atom, AtomRegistry } from "effect/reactivity";

import type { ConnectionCatalogEntry } from "../connection/catalog.ts";
import {
  AVAILABLE_CONNECTION_STATE,
  PrimaryConnectionTarget,
  type NetworkStatus,
  type PreparedConnection,
  type SupervisorConnectionState,
} from "../connection/model.ts";
import * as EnvironmentRegistry from "../connection/registry.ts";
import * as EnvironmentSupervisor from "../connection/supervisor.ts";
import type { RpcSession } from "../rpc/session.ts";
import { createRepoFreshnessAtoms, repoFreshnessTarget } from "./repoFreshness.ts";

const selected = new PrimaryConnectionTarget({
  environmentId: EnvironmentId.make("remote-owner"),
  label: "Selected remote",
  httpBaseUrl: "https://remote.example.test",
  wsBaseUrl: "wss://remote.example.test",
});
const projectId = ProjectId.make("same-project");
const status: RepoFreshnessStatus = {
  projectId,
  target: { status: "resolved", remoteName: "origin" },
  cadenceSeconds: 0,
  latestVerified: null,
  latestAttempt: null,
  freshness: "unknown",
  sampledAtMs: 123,
};

it.effect.each([false, true])(
  "uses the selected session and checks its advertised capability %s",
  (supported) =>
    Effect.gen(function* () {
      const requests: ProjectId[] = [];
      const session = {
        initialConfig: Effect.succeed({
          environment: { capabilities: supported ? { repoFreshness: true } : {} },
        }),
        client: {
          [WS_METHODS.vcsGetRepoFreshness]: (input: { projectId: ProjectId }) =>
            Effect.sync(() => {
              requests.push(input.projectId);
              return status;
            }),
        },
      } as unknown as RpcSession;
      const supervisor = EnvironmentSupervisor.EnvironmentSupervisor.of({
        target: selected,
        state: yield* SubscriptionRef.make<SupervisorConnectionState>({
          ...AVAILABLE_CONNECTION_STATE,
          phase: "connected",
          desired: true,
          network: "online",
          attempt: 1,
          generation: 1,
        }),
        session: yield* SubscriptionRef.make(Option.some(session)),
        prepared: yield* SubscriptionRef.make(Option.none<PreparedConnection>()),
        connect: Effect.void,
        disconnect: Effect.void,
        retryNow: Effect.void,
      });
      const run: EnvironmentRegistry.EnvironmentRegistry["Service"]["run"] = (id, effect) => {
        expect(id).toBe(selected.environmentId);
        return Effect.provideService(
          effect,
          EnvironmentSupervisor.EnvironmentSupervisor,
          supervisor,
        );
      };
      const followStream: EnvironmentRegistry.EnvironmentRegistry["Service"]["followStream"] = (
        id,
        stream,
      ) => {
        expect(id).toBe(selected.environmentId);
        return Stream.provideService(
          stream,
          EnvironmentSupervisor.EnvironmentSupervisor,
          supervisor,
        );
      };
      const runtime = Atom.runtime(
        Layer.mock(EnvironmentRegistry.EnvironmentRegistry)({
          entries: yield* SubscriptionRef.make<ReadonlyMap<EnvironmentId, ConnectionCatalogEntry>>(
            new Map(),
          ),
          networkStatus: yield* SubscriptionRef.make<NetworkStatus>("online"),
          run,
          followStream,
        }),
      );
      const atom = createRepoFreshnessAtoms(runtime).status(
        repoFreshnessTarget({ environmentId: selected.environmentId, projectId }),
      );
      const registry = AtomRegistry.make();
      yield* Effect.addFinalizer(() => Effect.sync(() => registry.dispose()));
      const answer = AtomRegistry.getResult(registry, atom, { suspendOnWaiting: true });
      if (supported) {
        expect(yield* answer).toEqual(status);
        expect(requests).toEqual([projectId]);
      } else {
        expect(yield* answer.pipe(Effect.flip)).toMatchObject({
          _tag: "RepoFreshnessUnavailableError",
        });
        expect(requests).toEqual([]);
      }
    }),
);
