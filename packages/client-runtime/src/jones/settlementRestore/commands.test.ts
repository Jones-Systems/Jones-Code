import { beforeEach, vi } from "vite-plus/test";
import { describe, expect, it } from "@effect/vitest";
import {
  CommandId,
  EnvironmentId,
  ORCHESTRATION_V2_WS_METHODS,
  SETTLEMENT_RESTORE_CAPABILITY,
  ThreadId,
  type SettlementRestoreInspectQuery,
} from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Option from "effect/Option";

import { RelayConnectionTarget, type PreparedConnection } from "../../connection/model.ts";
import * as EnvironmentSupervisor from "../../connection/supervisor.ts";
import type { ManagedRelayDpopSigner } from "../../relay/managedRelay.ts";
import { layerRemoteHttpClient } from "../../rpc/http.ts";

const mocks = vi.hoisted(() => ({ config: vi.fn(), request: vi.fn() }));
vi.mock(
  import("../../rpc/client.ts"),
  () => ({ getInitialServerConfig: mocks.config, request: mocks.request }) as never,
);
import { dispatchSettlementRestore, inspectSettlementRestores } from "./commands.ts";

const env = EnvironmentId.make("restore-env");
const prepared: PreparedConnection = {
  environmentId: env,
  label: "Synthetic restore environment",
  target: new RelayConnectionTarget({ environmentId: env, label: "Synthetic" }),
  httpBaseUrl: "https://restore.example.test",
  socketUrl: "wss://restore.example.test/ws",
  httpAuthorization: { _tag: "Bearer", token: "synthetic-token" },
};
const signer = Option.none<ManagedRelayDpopSigner["Service"]>();
const query: SettlementRestoreInspectQuery = {
  threadId: ThreadId.make("thread"),
  settleCommandId: CommandId.make("settle-1"),
};
const row = {
  threadId: query.threadId,
  status: "none",
  requestedSettleCommandId: query.settleCommandId,
  historicalOutcome: "unavailable",
  record: null,
};

beforeEach(() => {
  mocks.request.mockReset().mockReturnValue(Effect.succeed({ sequence: 3 }));
  mocks.config.mockReset().mockReturnValue(
    Effect.succeed({
      environment: { capabilities: { settlementRestore: SETTLEMENT_RESTORE_CAPABILITY } },
    }),
  );
});

// The RPC client is mocked, so dispatch never uses the supervisor.
const withSupervisor = Effect.provideService(
  EnvironmentSupervisor.EnvironmentSupervisor,
  {} as EnvironmentSupervisor.EnvironmentSupervisor["Service"],
);

describe("restore dispatch", () => {
  const command = {
    commandId: CommandId.make("restore-1"),
    threadId: query.threadId,
    settleCommandId: CommandId.make("settle-1"),
  };

  it.effect("sends the restore command with the caller's IDs", () =>
    Effect.gen(function* () {
      yield* dispatchSettlementRestore(command);
      expect(mocks.request).toHaveBeenCalledExactlyOnceWith(
        ORCHESTRATION_V2_WS_METHODS.dispatchCommand,
        { ...command, type: "thread.settlement.restore" },
      );
    }).pipe(withSupervisor),
  );

  it.effect("never sends it to an older or differently versioned server", () =>
    Effect.gen(function* () {
      for (const capabilities of [{}, { settlementRestore: { schemaVersion: "other" } }]) {
        mocks.config.mockReturnValue(Effect.succeed({ environment: { capabilities } }));
        const exit = yield* Effect.exit(dispatchSettlementRestore(command));
        expect(exit).toMatchObject({ _tag: "Failure" });
      }
      expect(mocks.request).not.toHaveBeenCalled();
    }).pipe(withSupervisor),
  );
});

describe("restore inspect", () => {
  it.effect("posts the exact query and returns the server's answer", () =>
    Effect.gen(function* () {
      const calls: Array<{ url: string; body: unknown }> = [];
      const items = yield* inspectSettlementRestores({ prepared, signer, items: [query] }).pipe(
        Effect.provide(
          layerRemoteHttpClient(async (url, init) => {
            calls.push({
              url: String(url),
              body: JSON.parse(
                init?.body instanceof Uint8Array
                  ? new TextDecoder().decode(init.body)
                  : String(init?.body),
              ),
            });
            return Response.json({ environmentId: env, items: [row] });
          }),
        ),
      );
      expect(items).toEqual([row]);
      expect(calls).toEqual([
        {
          url: "https://restore.example.test/api/jones/settlement-restores/inspect",
          body: { items: [query] },
        },
      ]);
    }),
  );

  it.effect("rejects an answer for another environment, thread or settle", () =>
    Effect.gen(function* () {
      for (const response of [
        { environmentId: "other-env", items: [row] },
        { environmentId: env, items: [{ ...row, threadId: "other-thread" }] },
        { environmentId: env, items: [{ ...row, requestedSettleCommandId: "settle-2" }] },
        { environmentId: env, items: [] },
      ]) {
        const exit = yield* Effect.exit(
          inspectSettlementRestores({ prepared, signer, items: [query] }).pipe(
            Effect.provide(layerRemoteHttpClient(async () => Response.json(response))),
          ),
        );
        expect(exit).toMatchObject({ _tag: "Failure" });
      }
    }),
  );
});
