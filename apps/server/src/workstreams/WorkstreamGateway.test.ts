import * as NodeCrypto from "node:crypto";

import { expect, it } from "@effect/vitest";
import { WorkstreamDetail, WorkstreamReferenceDetail } from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";

import {
  WORKSTREAM_CONTRACT_HEADER_VERSION,
  WORKSTREAM_CONTRACT_MANIFEST_SHA256,
  type WorkstreamCommand,
  type WorkstreamTransport,
  WorkstreamTransportError,
  make,
} from "./WorkstreamGateway.ts";
import { makeSyntheticWorkstreamTransport } from "./SyntheticWorkstreamTransport.ts";

const binding = {
  registryId: "fixture-registry",
  ownerId: "owner-fixture",
  principalId: "principal-fixture",
  authorizationRevision: 1,
} as const;

const updateCommand = (overrides: Partial<WorkstreamCommand> = {}): WorkstreamCommand => ({
  command_id: "command-fixture-0001",
  expected_server_generation: 7,
  expected_registry_version: 11,
  action: {
    operation: "update_workstream",
    workstream_id: "ws-core-v1",
    expected_version: 3,
    name: "Cross-system Workstreams",
    lifecycle: "active",
    progress: { state: "progressing" },
    sort_order: 10,
  },
  ...overrides,
});

it.effect("bounds metadata reads and falls back only to its metadata cache", () =>
  Effect.gen(function* () {
    const fixture = makeSyntheticWorkstreamTransport();
    let offline = false;
    let authOffline = false;
    let now = 1_000;
    const transport: WorkstreamTransport = {
      ...fixture,
      getCapabilities: (input) =>
        authOffline
          ? Effect.fail(
              new WorkstreamTransportError({
                operation: "capabilities",
                effect: "no-effect",
                detail: "authorization unavailable",
              }),
            )
          : fixture.getCapabilities(input),
      listWorkstreams: (input) =>
        offline
          ? Effect.fail(
              new WorkstreamTransportError({
                operation: "list",
                effect: "no-effect",
                detail: "offline",
              }),
            )
          : fixture.listWorkstreams(input),
    };
    const gateway = yield* make(transport, { binding, now: () => now, cacheMaxAgeMs: 50 });

    const live = yield* gateway.readMetadata({ limit: 1 });
    expect(live.source).toBe("live");
    expect(live.stale).toBe(false);
    expect(live.items).toHaveLength(1);
    expect(Object.keys(live.items[0] ?? {})).not.toContain("created_by");

    offline = true;
    now = 1_020;
    const freshCache = yield* gateway.readMetadata({ limit: 1 });
    expect(freshCache.source).toBe("cache");
    expect(freshCache.stale).toBe(false);

    now = 1_051;
    const staleCache = yield* gateway.readMetadata({ limit: 1 });
    expect(staleCache.source).toBe("cache");
    expect(staleCache.stale).toBe(true);

    authOffline = true;
    const unauthorizedCache = yield* gateway.readMetadata({ limit: 1 }).pipe(Effect.flip);
    expect(unauthorizedCache.reason).toBe("offline");
    authOffline = false;

    const invalid = yield* gateway.readMetadata({ limit: 101 }).pipe(Effect.flip);
    expect(invalid.reason).toBe("invalid-response");
  }),
);

it.effect("rejects a receipt attributed to another principal", () =>
  Effect.gen(function* () {
    const fixture = makeSyntheticWorkstreamTransport();
    const gateway = yield* make(
      {
        ...fixture,
        submitCommand: (input) =>
          fixture.submitCommand(input).pipe(
            Effect.map((receipt) => ({
              ...receipt,
              actor: { principal_id: "different-principal" },
            })),
          ),
      },
      { binding },
    );
    const error = yield* gateway.submit(updateCommand()).pipe(Effect.flip);
    expect(error.reason).toBe("invalid-response");
  }),
);

it.effect("returns schema-valid typed detail and reference fixtures", () =>
  Effect.gen(function* () {
    const transport = makeSyntheticWorkstreamTransport();
    const contract = {
      contractVersion: WORKSTREAM_CONTRACT_HEADER_VERSION,
      contractManifest: WORKSTREAM_CONTRACT_MANIFEST_SHA256,
    } as const;
    const rawDetail = yield* transport.getWorkstream({ ...contract, workstreamId: "ws-core-v1" });
    const rawReference = yield* transport.getReference({
      ...contract,
      nativeReferenceId: "reference-fixture",
    });
    yield* Schema.decodeUnknownEffect(WorkstreamDetail)(rawDetail);
    yield* Schema.decodeUnknownEffect(WorkstreamReferenceDetail)(rawReference);

    const gateway = yield* make(transport, { binding });
    const detail = yield* gateway.readDetail("ws-core-v1");
    const reference = yield* gateway.readReference("reference-fixture");

    expect(detail.workstream.workstream_id).toBe("ws-core-v1");
    expect(reference.reference.native_reference_id).toBe("reference-fixture");
    expect(reference.reference.registration.state).toBe("verification-pending");
  }),
);

it.effect("refuses mutations while offline or against stale generation and revision", () =>
  Effect.gen(function* () {
    const offlineGateway = yield* make(makeSyntheticWorkstreamTransport({ offline: true }), {
      binding,
    });
    const offline = yield* offlineGateway.submit(updateCommand()).pipe(Effect.flip);
    expect(offline.reason).toBe("offline");

    const gateway = yield* make(makeSyntheticWorkstreamTransport(), { binding });
    const stale = yield* gateway
      .submit(updateCommand({ expected_registry_version: 10 }))
      .pipe(Effect.flip);
    expect(stale.reason).toBe("version-conflict");
  }),
);

it.effect("binds the accepted contract and exact idempotency bytes", () =>
  Effect.gen(function* () {
    const fixture = makeSyntheticWorkstreamTransport();
    const submissions: Array<Parameters<WorkstreamTransport["submitCommand"]>[0]> = [];
    const transport: WorkstreamTransport = {
      ...fixture,
      submitCommand: (input) => {
        submissions.push(input);
        return fixture.submitCommand(input);
      },
    };
    const gateway = yield* make(transport, { binding });
    const command = updateCommand();

    const first = yield* gateway.submit(command);
    const replay = yield* gateway.submit(command);
    expect(replay).toEqual(first);
    expect(submissions).toHaveLength(1);
    expect(submissions[0]?.idempotencyKey).toBe(command.command_id);
    expect(submissions[0]?.contractVersion).toBe(WORKSTREAM_CONTRACT_HEADER_VERSION);
    expect(submissions[0]?.contractManifest).toBe(WORKSTREAM_CONTRACT_MANIFEST_SHA256);

    const conflict = yield* gateway
      .submit(
        updateCommand({
          action: {
            operation: "update_workstream",
            workstream_id: "ws-core-v1",
            expected_version: 3,
            name: "Different bytes",
            lifecycle: "active",
            progress: { state: "progressing" },
            sort_order: 10,
          },
        }),
      )
      .pipe(Effect.flip);
    expect(conflict.reason).toBe("idempotency-conflict");
    expect(submissions).toHaveLength(1);

    const mismatchGateway = yield* make(
      makeSyntheticWorkstreamTransport({ manifestSha256: "0".repeat(64) }),
      { binding },
    );
    const mismatch = yield* mismatchGateway.submit(command).pipe(Effect.flip);
    expect(mismatch.reason).toBe("contract-mismatch");
  }),
);

it.effect("bounds failed-command digests and evicts the oldest command without false replay", () =>
  Effect.gen(function* () {
    const fixture = makeSyntheticWorkstreamTransport();
    const submitted: string[] = [];
    const transport: WorkstreamTransport = {
      ...fixture,
      submitCommand: (input) => {
        submitted.push(input.command.command_id);
        return Effect.fail(
          new WorkstreamTransportError({
            operation: "submit_command",
            effect: "no-effect",
            detail: "fixture_offline",
          }),
        );
      },
    };
    const gateway = yield* make(transport, { binding, cacheCapacity: 2 });

    for (const commandId of [
      "command-capacity-0001",
      "command-capacity-0002",
      "command-capacity-0003",
    ]) {
      const failure = yield* gateway
        .submit(updateCommand({ command_id: commandId }))
        .pipe(Effect.flip);
      expect(failure.reason).toBe("offline");
    }

    const evictedRetry = yield* gateway
      .submit(
        updateCommand({
          command_id: "command-capacity-0001",
          action: {
            operation: "update_workstream",
            workstream_id: "ws-core-v1",
            expected_version: 3,
            name: "Changed after bounded eviction",
            lifecycle: "active",
            progress: { state: "progressing" },
            sort_order: 10,
          },
        }),
      )
      .pipe(Effect.flip);

    expect(evictedRetry.reason).toBe("offline");
    expect(submitted).toEqual([
      "command-capacity-0001",
      "command-capacity-0002",
      "command-capacity-0003",
      "command-capacity-0001",
    ]);
  }),
);

it.effect("keeps coordination disposition and native T3 settlement as distinct receipts", () =>
  Effect.gen(function* () {
    const gateway = yield* make(makeSyntheticWorkstreamTransport(), { binding });
    const coordination = yield* gateway.submit({
      command_id: "command-coordinate-0001",
      expected_server_generation: 7,
      expected_registry_version: 11,
      action: {
        operation: "set_coordination_disposition",
        workstream_id: "ws-core-v1",
        expected_version: 3,
        membership_id: "membership-1",
        disposition: "completed",
        other_disposition: null,
      },
    });
    const settlement = yield* gateway.submit({
      command_id: "command-settlement-0001",
      expected_server_generation: 7,
      expected_registry_version: 11,
      action: {
        operation: "request_native_t3_settlement",
        native_reference_id: "reference-t3-1",
        expected_attestation_version: 1,
        native_action: "settle",
      },
    });

    expect(coordination.state).toBe("committed");
    expect(settlement.state).toBe("committed");
    if (coordination.state !== "committed" || settlement.state !== "committed") return;
    expect(coordination.effects.coordination_disposition).toEqual({
      membership_id: "membership-1",
      disposition: "completed",
    });
    expect(coordination.effects.native_settlement).toBeNull();
    expect(settlement.effects.coordination_disposition).toBeNull();
    expect(settlement.effects.native_settlement).toEqual({
      native_reference_id: "reference-t3-1",
      native_action: "settle",
      outcome: "committed",
    });
  }),
);

it.effect("reconciles pending and unresolved commands only through the exact GET route", () =>
  Effect.gen(function* () {
    const fixture = makeSyntheticWorkstreamTransport();
    const command = updateCommand({ command_id: "command-pending-0001" });
    const body = "fixture-request-body";
    const terminal = yield* fixture.submitCommand({
      body,
      command,
      idempotencyKey: command.command_id,
      contractVersion: WORKSTREAM_CONTRACT_HEADER_VERSION,
      contractManifest: WORKSTREAM_CONTRACT_MANIFEST_SHA256,
    });
    if (terminal.state !== "committed") return;
    let polls = 0;
    let submissions = 0;
    const transport: WorkstreamTransport = {
      ...fixture,
      submitCommand: (input) => {
        submissions += 1;
        return Effect.succeed({
          command_id: input.command.command_id,
          owner_id: binding.ownerId,
          actor: { principal_id: binding.principalId },
          operation: input.command.action.operation,
          request_sha256: NodeCrypto.createHash("sha256").update(input.body).digest("hex"),
          server_generation: 7,
          accepted_at: terminal.accepted_at,
          state: "pending",
          retry_after_seconds: 1,
        });
      },
      getCommand: () => {
        polls += 1;
        return Effect.succeed(
          polls === 1
            ? {
                command_id: command.command_id,
                owner_id: binding.ownerId,
                actor: { principal_id: binding.principalId },
                operation: command.action.operation,
                request_sha256: terminal.request_sha256,
                server_generation: 7,
                accepted_at: terminal.accepted_at,
                state: "unresolved" as const,
                retry_after_seconds: 1,
              }
            : terminal,
        );
      },
    };
    const gateway = yield* make(transport, { binding });
    expect((yield* gateway.submit(command)).state).toBe("pending");
    expect((yield* gateway.pollCommand(command.command_id)).state).toBe("unresolved");
    expect((yield* gateway.pollCommand(command.command_id)).state).toBe("committed");
    expect(submissions).toBe(1);
    expect(polls).toBe(2);
  }),
);
