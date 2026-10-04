import * as NodeSqliteClient from "@t3tools/shared/nodeSqliteClient";
import * as Layer from "effect/Layer";
import * as SqlClient from "effect/unstable/sql/SqlClient";
import { AuthSessionId, NativeCreationHistoricalBinding } from "@t3tools/contracts";
import {
  NativeCreationAuthority,
  NativeCreationAuthorityError,
} from "./orchestration-v2/NativeCreationAuthority.ts";
import {
  NativePreparationBinding,
  nativePreparationCommand,
  nativeCreationCanonicalJson,
  nativeCreationSha256,
} from "./orchestration-v2/NativeCreationPreparation.ts";
import * as NativeCreationRepositoryLayer from "./persistence/Layers/NativeCreationRepository.ts";
import { runMigrations } from "./persistence/Migrations.ts";
import { makeNativeBootstrapDispatcher } from "./ws.ts";
import * as Schema from "effect/Schema";
import { describe, expect, it } from "vite-plus/test";
import { it as effectIt } from "@effect/vitest";
import {
  NATIVE_BOOTSTRAP_MAX_SUBMISSION_BYTES,
  ORCHESTRATION_WS_METHODS,
  WsOrchestrationDispatchBootstrapRpc,
} from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Option from "effect/Option";
import * as Queue from "effect/Queue";
import * as RpcGroup from "effect/unstable/rpc/RpcGroup";
import * as RpcServer from "effect/unstable/rpc/RpcServer";
import * as RpcSerialization from "effect/unstable/rpc/RpcSerialization";
import type { FromClientEncoded, FromServerEncoded } from "effect/unstable/rpc/RpcMessage";
import { nativeBootstrapCommandIds, nativeBootstrapRpcSerialization } from "./ws.ts";

const encodeWire = Schema.encodeUnknownSync(Schema.fromJsonString(Schema.Unknown));
const validSubmission = {
  schema: "t3.native-bootstrap-submission/v1",
  preparationBase64: "e30=",
  creationGuard: {
    schema: "t3.native-creation-guard/v1",
    grantId: "synthetic-grant",
    grantRevision: 1,
  },
};
const request = (
  tag: string = ORCHESTRATION_WS_METHODS.dispatchBootstrap,
  payload: unknown = validSubmission,
) => ({ _tag: "Request", id: "1", tag, payload, headers: [] });
const oversize = (text: string) => `${" ".repeat(NATIVE_BOOTSTRAP_MAX_SUBMISSION_BYTES)}${text}`;

describe("native guarded received-message bound", () => {
  for (const binary of [false, true]) {
    it(`rejects original ${binary ? "binary" : "text"} whitespace bytes before returning requests`, () => {
      const parser = nativeBootstrapRpcSerialization.makeUnsafe();
      const text = oversize(encodeWire(request()));
      expect(() => parser.decode(binary ? new TextEncoder().encode(text) : text)).toThrow(
        RpcSerialization.MaxBufferSizeExceeded,
      );
    });
  }
  it("counts envelope headers and Unicode bytes", () => {
    const parser = nativeBootstrapRpcSerialization.makeUnsafe();
    const input = {
      ...request(),
      headers: [["synthetic", "é".repeat(NATIVE_BOOTSTRAP_MAX_SUBMISSION_BYTES / 2)]],
    };
    expect(() => parser.decode(encodeWire(input))).toThrow(RpcSerialization.MaxBufferSizeExceeded);
  });
  it("rejects all siblings in an oversized bootstrap batch with zero dispatches", () => {
    let effects = 0;
    const parser = nativeBootstrapRpcSerialization.makeUnsafe();
    expect(() => {
      for (const _request of parser.decode(
        oversize(encodeWire([request("server.getConfig"), request()])),
      ))
        effects++;
    }).toThrow(RpcSerialization.MaxBufferSizeExceeded);
    expect(effects).toBe(0);
  });
  it("keeps ordinary and legacy RPC serialization unchanged", () => {
    for (const tag of ["server.getConfig", ORCHESTRATION_WS_METHODS.dispatchCommand]) {
      const text = oversize(encodeWire(request(tag)));
      expect(nativeBootstrapRpcSerialization.makeUnsafe().decode(text)).toEqual(
        RpcSerialization.json.makeUnsafe().decode(text),
      );
    }
  });
  it("counts the exact Uint8Array view rather than its backing buffer", () => {
    const bytes = new TextEncoder().encode(encodeWire(request()));
    const backing = new Uint8Array(NATIVE_BOOTSTRAP_MAX_SUBMISSION_BYTES + bytes.length);
    backing.set(bytes, NATIVE_BOOTSTRAP_MAX_SUBMISSION_BYTES);
    expect(
      nativeBootstrapRpcSerialization
        .makeUnsafe()
        .decode(backing.subarray(NATIVE_BOOTSTRAP_MAX_SUBMISSION_BYTES)),
    ).toEqual([request()]);
  });
  it("returns the native decoder result for an in-bound batch", () => {
    const text = `  ${encodeWire([request(), request("server.getConfig")])}  `;
    expect(nativeBootstrapRpcSerialization.makeUnsafe().decode(text)).toEqual(
      RpcSerialization.json.makeUnsafe().decode(text),
    );
  });
  it("reserves a closed inventory containing all core, setup, and cleanup commands", () => {
    const ids = nativeBootstrapCommandIds("synthetic-original");
    expect(ids).toHaveLength(14);
    expect(new Set(ids).size).toBe(ids.length);
    expect(ids).toContain("synthetic-original:bootstrap-thread-delete");
    expect(ids).toContain("synthetic-original:worktree-setup-cancelled");
  });
});

const group = RpcGroup.make(WsOrchestrationDispatchBootstrapRpc);
const nativeWire = (payload: unknown) =>
  Effect.gen(function* () {
    const requests = yield* Queue.unbounded<FromClientEncoded>();
    const responses = yield* Queue.unbounded<FromServerEncoded>();
    const disconnects = yield* Queue.unbounded<number>();
    let effects = 0;
    const protocol = RpcServer.Protocol.of({
      run: (receive) =>
        Queue.take(requests).pipe(
          Effect.flatMap((message) => receive(1, message)),
          Effect.forever,
        ),
      disconnects,
      send: (_clientId, message) => Queue.offer(responses, message).pipe(Effect.asVoid),
      end: () => Effect.void,
      clientIds: Effect.succeed(new Set([1])),
      initialMessage: Effect.succeed(Option.none()),
      supportsAck: false,
      supportsTransferables: false,
      supportsSpanPropagation: false,
      supportsNotifications: false,
      codecFor: nativeBootstrapRpcSerialization.codecFor,
    });
    yield* RpcServer.make(group).pipe(
      Effect.provideService(RpcServer.Protocol, protocol),
      Effect.provide(
        group.toLayer({
          [ORCHESTRATION_WS_METHODS.dispatchBootstrap]: () =>
            Effect.sync(() => {
              effects++;
              return { sequence: 7 };
            }),
        }),
      ),
      Effect.forkScoped,
    );
    const decoded = nativeBootstrapRpcSerialization
      .makeUnsafe()
      .decode(encodeWire(request(ORCHESTRATION_WS_METHODS.dispatchBootstrap, payload)));
    for (const message of decoded) yield* Queue.offer(requests, message as FromClientEncoded);
    const response = yield* Queue.take(responses);
    return { response, effects };
  });
effectIt.effect("actual native RPC codecs accept the guarded outer schema", () =>
  nativeWire(request().payload).pipe(
    Effect.tap(({ response, effects }) =>
      Effect.sync(() => {
        expect(effects).toBe(1);
        expect(response._tag).toBe("Exit");
      }),
    ),
    Effect.scoped,
  ),
);
effectIt.effect(
  "actual native RPC codecs reject an excess outer field before handler effects",
  () =>
    nativeWire({ ...validSubmission, unexpected: true }).pipe(
      Effect.tap(({ effects }) =>
        Effect.sync(() => {
          expect(effects).toBe(0);
        }),
      ),
      Effect.scoped,
    ),
);

const nativeDatabase = Layer.effectDiscard(runMigrations()).pipe(
  Layer.provideMerge(NodeSqliteClient.layer({ filename: ":memory:" })),
);
const producerFixture = (text = "Synthetic immutable text") => {
  const binding = Schema.decodeUnknownSync(NativePreparationBinding)({
    backend_instance: "synthetic-backend",
    environment_id: "synthetic-env",
    project_id: "synthetic-project",
    project_cwd: "/synthetic/project",
    account_ref: "synthetic-account",
    runtime_mode: "full-access",
    interaction_mode: "default",
    base_branch: "main",
    start_from_origin: false,
    run_setup_script: false,
    provider_model_selection: { instanceId: "codex", model: "synthetic-model" },
  });
  const command = nativePreparationCommand(
    "synthetic-operation",
    binding,
    text,
    "Synthetic thread",
    "2026-10-02T12:00:00Z",
  );
  const preparation = nativeCreationCanonicalJson({
    schema: "voice.t3-bootstrap-preparation/v1",
    operation_id: "synthetic-operation",
    preparation_id: command.commandId.replace("voice-command-", "voice-bootstrap-"),
    binding,
    command,
    binding_digest: nativeCreationSha256(nativeCreationCanonicalJson(binding)),
    prompt_digest: nativeCreationSha256(text),
    command_digest: nativeCreationSha256(nativeCreationCanonicalJson(command)),
  });
  const historical = Schema.decodeUnknownSync(NativeCreationHistoricalBinding)({
    backendInstance: binding.backend_instance,
    environmentId: binding.environment_id,
    projectId: binding.project_id,
    projectCwd: binding.project_cwd,
    accountRef: binding.account_ref,
    accountBindingId: "synthetic-account-binding",
    accountBindingRevision: 1,
    providerModelSelection: binding.provider_model_selection,
    runtimeMode: binding.runtime_mode,
    interactionMode: binding.interaction_mode,
    baseBranch: binding.base_branch,
    startFromOrigin: false,
    runSetupScript: false,
    requestedBranch: command.bootstrap.prepareWorktree.branch,
  });
  return {
    historical,
    command,
    submission: {
      schema: "t3.native-bootstrap-submission/v1",
      preparationBase64: Buffer.from(preparation).toString("base64"),
      creationGuard: {
        schema: "t3.native-creation-guard/v1",
        grantId: "synthetic-grant",
        grantRevision: 1,
      },
    },
  };
};
const preparedFixture = producerFixture();
effectIt.effect(
  "real SQL claim and all command identities commit before native normalizer and dispatch",
  () =>
    Effect.gen(function* () {
      const repository = yield* NativeCreationRepositoryLayer.make;
      const sql = yield* SqlClient.SqlClient;
      let counter = 0;
      let normalizations = 0;
      let dispatches = 0;
      const dispatcher = makeNativeBootstrapDispatcher({
        actorSessionId: AuthSessionId.make("synthetic-session"),
        worktreesDir: "/synthetic/worktrees",
        bootId: "synthetic-boot",
        repository,
        authority: NativeCreationAuthority.of({
          isAutomationEnrolled: () => Effect.succeed(true),
          authorize: () => Effect.succeed(preparedFixture.historical),
        }),
        newId: Effect.sync(() => `synthetic-id-${counter++}`),
        now: Effect.succeed("2026-10-02T12:00:01Z"),
        normalize: (command) =>
          Effect.gen(function* () {
            const identities =
              yield* sql`SELECT command_id FROM native_creation_reserved_command_identities`;
            const intents = yield* sql`SELECT claim_id FROM native_creation_intents`;
            expect(identities).toHaveLength(14);
            expect(intents).toHaveLength(1);
            normalizations++;
            return {
              ...command,
              createdAt: "2026-10-02T12:00:01Z",
              bootstrap: {
                ...command.bootstrap!,
                createThread: {
                  ...command.bootstrap!.createThread!,
                  createdAt: "2026-10-02T12:00:01Z",
                },
              },
            };
          }).pipe(Effect.orDie),
        dispatch: (_command, creation) =>
          Effect.gen(function* () {
            const history = yield* repository.readHistoryByClaim(creation.claimId);
            expect(history.normalizedCommandDigest).not.toBeNull();
            expect(history.normalizedCommandDigest).not.toBe(history.intent.commandDigest);
            dispatches++;
            return { sequence: 7 };
          }).pipe(Effect.orDie),
      });
      expect(yield* dispatcher(preparedFixture.submission)).toEqual({ sequence: 7 });
      const duplicate = yield* dispatcher(preparedFixture.submission).pipe(Effect.result);
      expect(duplicate._tag).toBe("Failure");
      if (duplicate._tag === "Failure")
        expect(duplicate.failure.creationRejectionCode).toBe("unresolved_claim");
      expect(normalizations).toBe(1);
      expect(dispatches).toBe(1);
    }).pipe(Effect.provide(nativeDatabase)),
);

effectIt.effect(
  "authority revocation between claim and normalization preserves claimed history with zero bootstrap effects",
  () =>
    Effect.gen(function* () {
      const repository = yield* NativeCreationRepositoryLayer.make;
      let counter = 0;
      let effects = 0;
      const dispatcher = makeNativeBootstrapDispatcher({
        actorSessionId: AuthSessionId.make("synthetic-session"),
        worktreesDir: "/synthetic/worktrees",
        bootId: "synthetic-boot",
        repository,
        authority: NativeCreationAuthority.of({
          isAutomationEnrolled: () => Effect.succeed(true),
          authorize: (input) =>
            input.stage === "claim"
              ? Effect.succeed(preparedFixture.historical)
              : Effect.fail(
                  new NativeCreationAuthorityError({
                    code: "stale_grant",
                    message: "Synthetic revoked grant",
                  }),
                ),
        }),
        newId: Effect.sync(() => `synthetic-id-${counter++}`),
        now: Effect.succeed("2026-10-02T12:00:01Z"),
        normalize: (command) =>
          Effect.sync(() => {
            effects++;
            return command;
          }),
        dispatch: () =>
          Effect.sync(() => {
            effects++;
            return { sequence: 1 };
          }),
      });
      const rejected = yield* dispatcher(preparedFixture.submission).pipe(Effect.result);
      expect(rejected._tag).toBe("Failure");
      if (rejected._tag === "Failure")
        expect(rejected.failure.creationRejectionCode).toBe("stale_grant");
      expect(effects).toBe(0);
      const history = yield* repository.readHistory(preparedFixture.command.commandId);
      expect(Option.isSome(history)).toBe(true);
      if (Option.isSome(history)) {
        expect(history.value.normalizedCommandDigest).toBeNull();
        expect(history.value.effects).toHaveLength(0);
      }
    }).pipe(Effect.provide(nativeDatabase)),
);

effectIt.effect(
  "unavailable native authority rejects before durable claims, normalization or dispatch",
  () =>
    Effect.gen(function* () {
      const repository = yield* NativeCreationRepositoryLayer.make;
      const sql = yield* SqlClient.SqlClient;
      let effects = 0;
      const dispatcher = makeNativeBootstrapDispatcher({
        actorSessionId: AuthSessionId.make("synthetic-session"),
        worktreesDir: "/synthetic/worktrees",
        bootId: "synthetic-boot",
        repository,
        authority: NativeCreationAuthority.of({
          isAutomationEnrolled: () => Effect.succeed(true),
          authorize: () =>
            Effect.fail(
              new NativeCreationAuthorityError({
                code: "unsupported_authority",
                message: "Synthetic unavailable authority",
              }),
            ),
        }),
        newId: Effect.succeed("synthetic-claim"),
        now: Effect.succeed("2026-10-02T12:00:01Z"),
        normalize: (command) =>
          Effect.sync(() => {
            effects++;
            return command;
          }),
        dispatch: () =>
          Effect.sync(() => {
            effects++;
            return { sequence: 1 };
          }),
      });
      const rejected = yield* dispatcher(preparedFixture.submission).pipe(Effect.result);
      expect(rejected._tag).toBe("Failure");
      expect(effects).toBe(0);
      expect(yield* sql`SELECT claim_id FROM native_creation_intents`).toHaveLength(0);
    }).pipe(Effect.provide(nativeDatabase)),
);
