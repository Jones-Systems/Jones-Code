// @effect-diagnostics-next-line nodeBuiltinImport:off -- Synchronous contract hashing and opaque request identities retain their existing wire encoding.
import * as NodeCrypto from "node:crypto";

import { expect, it } from "@effect/vitest";
import {
  WorkstreamDetail,
  WorkstreamReferenceDetail,
  WORKSTREAM_COUNTS_MANIFEST_SHA256,
  type WorkstreamCountsCapabilities,
  type WorkstreamRegistryCounts,
} from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";

import {
  WORKSTREAM_CONTRACT_HEADER_VERSION,
  WORKSTREAM_CONTRACT_MANIFEST_SHA256,
  type WorkstreamCommand,
  type WorkstreamReceiptRead,
  type WorkstreamTransport,
  WorkstreamTransportError,
  WorkstreamVerifiedPreDispatchRefusal,
  make,
} from "./WorkstreamGateway.ts";
import { makeSyntheticWorkstreamTransport } from "./SyntheticWorkstreamTransport.ts";
import { makeControlPlaneWorkstreamTransport } from "./ControlPlaneWorkstreamTransport.ts";

const decodeWorkstreamDetail = Schema.decodeUnknownEffect(WorkstreamDetail);
const decodeReferenceDetail = Schema.decodeUnknownEffect(WorkstreamReferenceDetail);

const binding = {
  registryId: "fixture-registry",
  ownerId: "owner-fixture",
  principalId: "principal-fixture",
  authorizationRevision: 1,
} as const;

it.effect("recognizes only a verified pre-dispatch refusal after capabilities succeeded", () =>
  Effect.gen(function* () {
    const fixture = makeSyntheticWorkstreamTransport();
    const calls: string[] = [];
    const refusal = new WorkstreamVerifiedPreDispatchRefusal({});
    const gateway = yield* make(
      {
        ...fixture,
        getCapabilities: (input) =>
          fixture
            .getCapabilities(input)
            .pipe(Effect.tap(() => Effect.sync(() => calls.push("ready")))),
        // Qualified internal fixture only; producer proof verification belongs to the adapter.
        submitCommand: () => {
          calls.push("refused-before-dispatch");
          return Effect.fail(refusal);
        },
      },
      { binding },
    );
    const error = yield* gateway.submit(updateCommand()).pipe(Effect.flip);
    expect(error.reason).toBe("authorization-not-current");
    expect(error.cause).toBe(refusal);
    expect(calls).toEqual(["ready", "refused-before-dispatch"]);
  }),
);

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

it.effect("recovers its own lost commit after version advance with identical retained bytes", () =>
  Effect.gen(function* () {
    const fixture = makeSyntheticWorkstreamTransport();
    const bodies: string[] = [];
    const gateway = yield* make(
      {
        ...fixture,
        resubmitCommand: (input) =>
          Effect.gen(function* () {
            bodies.push(input.body);
            const receipt = yield* fixture.submitCommand(input).pipe(Effect.orDie);
            if (bodies.length === 1)
              return yield* new WorkstreamTransportError({
                operation: "resubmit_command",
                effect: "unknown-effect",
                detail: "reply lost after commit",
              });
            return { kind: "receipt" as const, receipt };
          }),
      },
      { binding },
    );
    const command = updateCommand();
    const body = `${JSON.stringify(command, null, 2)}\n`;
    expect((yield* gateway.resubmit(command.command_id, body).pipe(Effect.flip)).reason).toBe(
      "unknown-effect",
    );
    expect((yield* gateway.readSession()).registryVersion).toBe(12);
    // This legacy fixture has no authenticated receipt lookup; absence of that seam stays unknown.
    expect((yield* gateway.submit(command).pipe(Effect.flip)).reason).toBe("unknown-effect");
    const replay = yield* gateway.resubmit(command.command_id, body);
    expect(replay.kind).toBe("receipt");
    if (replay.kind !== "receipt") throw new Error("Expected replay receipt");
    expect(replay.receipt.state).toBe("committed");
    expect(replay.receipt.request_sha256).toBe(
      NodeCrypto.createHash("sha256").update(body).digest("hex"),
    );
    expect(bodies).toEqual([body, body]);
    expect((yield* gateway.readSession()).registryVersion).toBe(12);
  }),
);

it.effect("reads its witness before the receipt even when a commit interleaves", () =>
  Effect.gen(function* () {
    const fixture = makeSyntheticWorkstreamTransport();
    const calls: string[] = [];
    const command = updateCommand();
    const body = JSON.stringify(command);
    const gateway = yield* make(
      {
        ...fixture,
        getCapabilities: (input) =>
          fixture
            .getCapabilities(input)
            .pipe(Effect.tap(() => Effect.sync(() => calls.push("witness")))),
        readCommandReceipt: () =>
          Effect.gen(function* () {
            calls.push("receipt");
            if (calls.length > 2) return { kind: "absent" as const };
            const receipt = yield* fixture
              .submitCommand({
                body,
                command,
                idempotencyKey: command.command_id,
                contractVersion: WORKSTREAM_CONTRACT_HEADER_VERSION,
                contractManifest: WORKSTREAM_CONTRACT_MANIFEST_SHA256,
              })
              .pipe(Effect.orDie);
            return { kind: "receipt" as const, receipt };
          }),
      },
      { binding },
    );
    const result = yield* gateway.readReceipt(command.command_id);
    expect(result).toMatchObject({
      witness_version: 11,
      kind: "receipt",
      receipt: { state: "committed" },
    });
    expect(yield* gateway.readReceipt("another-command")).toEqual({
      witness_version: 12,
      kind: "absent",
    });
    expect(calls).toEqual(["witness", "receipt", "witness", "receipt"]);
  }),
);

it.effect(
  "reuses a locally refused absent index with new bytes without poisoning legacy guards",
  () =>
    Effect.gen(function* () {
      const fixture = makeSyntheticWorkstreamTransport();
      const bodies: string[] = [];
      const gateway = yield* make(
        {
          ...fixture,
          readCommandReceipt: () => Effect.succeed<WorkstreamReceiptRead>({ kind: "absent" }),
          resubmitCommand: (input) => {
            bodies.push(input.body);
            return fixture.submitCommand(input).pipe(
              Effect.orDie,
              Effect.map((receipt) => ({ kind: "receipt" as const, receipt })),
            );
          },
        },
        { binding },
      );
      const refused = updateCommand({ expected_registry_version: 10 });
      expect((yield* gateway.submit(refused).pipe(Effect.flip)).reason).toBe("version-conflict");
      expect(yield* gateway.readReceipt(refused.command_id)).toEqual({
        witness_version: 11,
        kind: "absent",
      });
      const body = JSON.stringify(updateCommand());
      expect(yield* gateway.resubmit(refused.command_id, body)).toMatchObject({
        kind: "receipt",
        receipt: { state: "committed" },
      });
      expect(bodies).toEqual([body]);
    }),
);

it.effect(
  "leaves recovery unknown when CP refuses an old generation or current authorization fails",
  () =>
    Effect.gen(function* () {
      const fixture = makeSyntheticWorkstreamTransport();
      let submissions = 0;
      let permitted = true;
      const gateway = yield* make(
        {
          ...fixture,
          getCapabilities: (input) =>
            fixture.getCapabilities(input).pipe(
              Effect.map((value) => ({
                ...value,
                permissions: permitted ? value.permissions : [],
                context: { ...value.context, server_generation: 8 },
              })),
            ),
          resubmitCommand: () => {
            submissions += 1;
            return Effect.fail(
              new WorkstreamTransportError({
                operation: "resubmit_command",
                effect: "no-effect",
                detail: "generation_conflict",
              }),
            );
          },
        },
        { binding },
      );
      const command = updateCommand();
      const body = JSON.stringify(command);
      expect((yield* gateway.resubmit(command.command_id, body).pipe(Effect.flip)).reason).toBe(
        "unknown-effect",
      );
      expect(submissions).toBe(1);
      permitted = false;
      expect((yield* gateway.resubmit(command.command_id, body).pipe(Effect.flip)).reason).toBe(
        "unknown-effect",
      );
      expect(submissions).toBe(1);
    }),
);

it.effect(
  "exposes recovery idempotency conflicts and rejects mismatched retained command IDs",
  () =>
    Effect.gen(function* () {
      let submissions = 0;
      const gateway = yield* make(
        {
          ...makeSyntheticWorkstreamTransport(),
          resubmitCommand: () => {
            submissions += 1;
            return Effect.succeed({ kind: "idempotency_conflict" as const });
          },
        },
        { binding },
      );
      const command = updateCommand();
      expect(yield* gateway.resubmit(command.command_id, JSON.stringify(command))).toEqual({
        kind: "idempotency_conflict",
      });
      const mismatched = yield* gateway
        .resubmit("different-command", JSON.stringify(command))
        .pipe(Effect.flip);
      expect(mismatched.reason).toBe("unknown-effect");
      expect(submissions).toBe(1);
    }),
);

it.effect("never converts an unverified receipt failure into absence", () =>
  Effect.gen(function* () {
    const gateway = yield* make(
      {
        ...makeSyntheticWorkstreamTransport(),
        readCommandReceipt: () =>
          Effect.fail(
            new WorkstreamTransportError({
              operation: "read_command_receipt",
              effect: "no-effect",
              detail: "not_found",
            }),
          ),
      },
      { binding },
    );
    expect((yield* gateway.readReceipt("unknown-command").pipe(Effect.flip)).reason).toBe(
      "unknown-effect",
    );
  }),
);

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

it.effect("reports disabled activation as not configured rather than offline", () =>
  Effect.gen(function* () {
    const disabled = makeControlPlaneWorkstreamTransport({ state: "disabled" });
    const gateway = yield* make(disabled.transport, { binding: disabled.binding });
    expect((yield* gateway.readMetadata().pipe(Effect.flip)).reason).toBe("not-configured");
    expect((yield* gateway.readSession().pipe(Effect.flip)).reason).toBe("not-configured");

    const fixture = makeSyntheticWorkstreamTransport();
    const unreachable = yield* make(
      {
        ...fixture,
        getCapabilities: () =>
          Effect.fail(
            new WorkstreamTransportError({
              operation: "capabilities",
              effect: "no-effect",
              detail: "http_error",
            }),
          ),
      },
      { binding },
    );
    expect((yield* unreachable.readMetadata().pipe(Effect.flip)).reason).toBe("offline");
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
    yield* decodeWorkstreamDetail(rawDetail);
    yield* decodeReferenceDetail(rawReference);

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

    const gateway = yield* make(
      {
        ...makeSyntheticWorkstreamTransport(),
        readCommandReceipt: () => Effect.succeed<WorkstreamReceiptRead>({ kind: "absent" }),
      },
      { binding },
    );
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
      // Pin the advertised revision so the replay below exercises the gateway's own receipt
      // cache under one unchanged binding; the synthetic registry advances on every commit.
      getCapabilities: (input) =>
        fixture.getCapabilities(input).pipe(
          Effect.map((value) => ({
            ...value,
            context: { ...value.context, registry_version: 11 },
          })),
        ),
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
    expect(mismatch.reason).toBe("authorization-not-current");
    expect(mismatch.cause).toMatchObject({ reason: "contract-mismatch" });
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
    // The committed disposition advanced the synthetic registry version.
    const settlement = yield* gateway.submit({
      command_id: "command-settlement-0001",
      expected_server_generation: 7,
      expected_registry_version: 12,
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
    // A separate instance supplies the terminal receipt, so the gateway's registry stays at 11.
    const terminal = yield* makeSyntheticWorkstreamTransport().submitCommand({
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

const countsCapability = {
  version: 1,
  path: "/workstreams/v1/counts",
  active_definition: "latest_owner_lifecycle_declaration_active",
} as const;
const registryCounts: WorkstreamRegistryCounts = {
  context: { owner_id: binding.ownerId, server_generation: 7, registry_version: 11 },
  principal_id: binding.principalId,
  authority_effect: "none",
  observed_at: "2026-10-18T00:00:00Z",
  total: 9,
  active: 3,
  unknown_lifecycle: 1,
};
const countsTransport = (): WorkstreamTransport => {
  const fixture = makeSyntheticWorkstreamTransport();
  return {
    ...fixture,
    getCountsCapabilities: () =>
      fixture
        .getCapabilities({
          contractVersion: WORKSTREAM_CONTRACT_HEADER_VERSION,
          contractManifest: WORKSTREAM_CONTRACT_MANIFEST_SHA256,
        })
        .pipe(
          Effect.map((capabilities) => ({
            ...capabilities,
            manifest_sha256: WORKSTREAM_COUNTS_MANIFEST_SHA256,
            registry_counts: countsCapability,
          })),
        ),
    getRegistryCounts: () => Effect.succeed(registryCounts),
  };
};

it.effect("returns exact counts only after revalidating the session, without paging", () =>
  Effect.gen(function* () {
    const transport = countsTransport();
    const calls: string[] = [];
    const gateway = yield* make(
      {
        ...transport,
        getCapabilities: (input) =>
          Effect.suspend(() => {
            calls.push("session");
            return transport.getCapabilities(input);
          }),
        getCountsCapabilities: () =>
          Effect.suspend(() => {
            calls.push("negotiate");
            return transport.getCountsCapabilities!();
          }),
        getRegistryCounts: () =>
          Effect.sync(() => {
            calls.push("counts");
            return registryCounts;
          }),
        listWorkstreams: () => Effect.die("counts must never page"),
      },
      { binding },
    );
    expect(yield* gateway.readRegistryCounts()).toEqual({
      counts: registryCounts,
      binding: yield* gateway.readSession(),
    });
    expect(calls).toEqual(["session", "negotiate", "counts", "session", "session"]);
  }),
);

it.effect.each([
  undefined,
  { ...countsCapability, version: 2 },
  { ...countsCapability, path: "/unsupported" },
  { ...countsCapability, active_definition: "running_executors" },
])("rejects unsupported counts capability %j", (capability) =>
  Effect.gen(function* () {
    const transport = countsTransport();
    let countReads = 0;
    const gateway = yield* make(
      {
        ...transport,
        getCountsCapabilities: () =>
          transport.getCountsCapabilities!().pipe(
            Effect.map((caps) => {
              const { registry_counts: _capability, ...legacy } = caps;
              return (
                capability === undefined ? legacy : { ...legacy, registry_counts: capability }
              ) as WorkstreamCountsCapabilities;
            }),
          ),
        getRegistryCounts: () =>
          Effect.sync(() => {
            countReads++;
            return registryCounts;
          }),
        listWorkstreams: () => Effect.die("no pagination fallback"),
      },
      { binding },
    );
    expect((yield* gateway.readRegistryCounts().pipe(Effect.flip)).reason).toBe(
      "contract-mismatch",
    );
    expect(countReads).toBe(0);
  }),
);

it.effect("rejects absent ports, legacy manifests and denied counts permissions", () =>
  Effect.gen(function* () {
    const legacy = yield* make(makeSyntheticWorkstreamTransport(), { binding });
    expect((yield* legacy.readRegistryCounts().pipe(Effect.flip)).reason).toBe("contract-mismatch");
    for (const overrides of [
      { manifest_sha256: WORKSTREAM_CONTRACT_MANIFEST_SHA256 },
      { permissions: [] },
    ]) {
      const transport = countsTransport();
      const gateway = yield* make(
        {
          ...transport,
          getCountsCapabilities: () =>
            transport.getCountsCapabilities!().pipe(
              Effect.map((caps) => ({ ...caps, ...overrides })),
            ),
          getRegistryCounts: () => Effect.die("unnegotiated counts"),
        },
        { binding },
      );
      expect((yield* gateway.readRegistryCounts().pipe(Effect.flip)).reason).toBe(
        "permissions" in overrides ? "permission-denied" : "contract-mismatch",
      );
    }
  }),
);

it.effect.each(["owner_id", "server_generation", "registry_version", "principal_id"] as const)(
  "rejects a counts response with changed %s",
  (field) =>
    Effect.gen(function* () {
      const changed =
        field === "principal_id"
          ? { ...registryCounts, principal_id: "another-principal" }
          : {
              ...registryCounts,
              context: {
                ...registryCounts.context,
                [field]: field === "owner_id" ? "another-owner" : 99,
              },
            };
      const gateway = yield* make(
        { ...countsTransport(), getRegistryCounts: () => Effect.succeed(changed) },
        { binding },
      );
      expect((yield* gateway.readRegistryCounts().pipe(Effect.flip)).reason).toBe("stale");
    }),
);
it.effect.each([
  "registryId",
  "ownerId",
  "principalId",
  "authorizationRevision",
  "serverGeneration",
  "registryVersion",
] as const)("rejects post-read %s drift", (field) =>
  Effect.gen(function* () {
    const transport = countsTransport();
    const mutableBinding: {
      registryId: string;
      ownerId: string;
      principalId: string;
      authorizationRevision: number;
    } = { ...binding };
    let read = false;
    const gateway = yield* make(
      {
        ...transport,
        getCapabilities: (input) =>
          transport.getCapabilities(input).pipe(
            Effect.map((caps) => ({
              ...caps,
              context: {
                ...caps.context,
                ...(read && field === "serverGeneration" ? { server_generation: 8 } : {}),
                ...(read && field === "registryVersion" ? { registry_version: 12 } : {}),
              },
            })),
          ),
        getRegistryCounts: () =>
          Effect.sync(() => {
            read = true;
            if (field === "authorizationRevision") mutableBinding.authorizationRevision++;
            if (field === "registryId") mutableBinding.registryId = "changed-registry";
            if (field === "ownerId") mutableBinding.ownerId = "changed-owner";
            if (field === "principalId") mutableBinding.principalId = "changed-principal";
            return registryCounts;
          }),
      },
      { binding: mutableBinding },
    );
    expect((yield* gateway.readRegistryCounts().pipe(Effect.flip)).reason).toBe(
      field === "ownerId" ? "permission-denied" : "stale",
    );
  }),
);

it.effect("rejects excess fields, impossible totals and failed revalidation", () =>
  Effect.gen(function* () {
    for (const counts of [
      { ...registryCounts, total: 3 },
      { ...registryCounts, content: "forbidden" },
    ]) {
      const gateway = yield* make(
        { ...countsTransport(), getRegistryCounts: () => Effect.succeed(counts) },
        { binding },
      );
      expect((yield* gateway.readRegistryCounts().pipe(Effect.flip)).reason).toBe(
        "invalid-response",
      );
    }
    const transport = countsTransport();
    let read = false;
    const gateway = yield* make(
      {
        ...transport,
        getCapabilities: (input) =>
          read
            ? Effect.fail(
                new WorkstreamTransportError({
                  operation: "capabilities",
                  effect: "no-effect",
                  detail: "offline",
                }),
              )
            : transport.getCapabilities(input),
        getRegistryCounts: () =>
          Effect.sync(() => {
            read = true;
            return registryCounts;
          }),
      },
      { binding },
    );
    expect((yield* gateway.readRegistryCounts().pipe(Effect.flip)).reason).toBe("offline");
  }),
);

it.effect.each(["owner_id", "server_generation", "registry_version"] as const)(
  "rejects negotiation context drift in %s before reading counts",
  (field) =>
    Effect.gen(function* () {
      const transport = countsTransport();
      const gateway = yield* make(
        {
          ...transport,
          getCountsCapabilities: () =>
            transport.getCountsCapabilities!().pipe(
              Effect.map((caps) => ({
                ...caps,
                context: { ...caps.context, [field]: field === "owner_id" ? "other-owner" : 99 },
              })),
            ),
          getRegistryCounts: () => Effect.die("changed context must not read counts"),
        },
        { binding },
      );
      expect((yield* gateway.readRegistryCounts().pipe(Effect.flip)).reason).toBe("stale");
    }),
);

it.effect("rejects an authorization revision change during initial authorization", () =>
  Effect.gen(function* () {
    const transport = countsTransport();
    const mutableBinding = {
      ...binding,
      authorizationRevision: Number(binding.authorizationRevision),
    };
    const gateway = yield* make(
      {
        ...transport,
        getCapabilities: (input) =>
          transport.getCapabilities(input).pipe(
            Effect.map((caps) => {
              mutableBinding.authorizationRevision++;
              return caps;
            }),
          ),
      },
      { binding: mutableBinding },
    );
    expect((yield* gateway.readRegistryCounts().pipe(Effect.flip)).reason).toBe("stale");
  }),
);

it.effect.each([
  "retained",
  "owner",
  "principal",
  "generation",
  "command",
  "digest",
  "operation",
  "read-only",
  "write-only",
  "revoked-during-lookup",
  "read-revoked-during-lookup",
  "absent",
  "unknown",
  "unavailable",
] as const)("stale POST replay fences %s evidence without another dispatch", (variant) =>
  Effect.gen(function* () {
    const fixture = makeSyntheticWorkstreamTransport();
    const calls: string[] = [];
    let permissions: readonly ("workstreams:read" | "workstreams:write")[] = [
      "workstreams:read",
      "workstreams:write",
    ];
    let replay = false;
    const gateway = yield* make(
      {
        ...fixture,
        getCapabilities: (input) =>
          fixture.getCapabilities(input).pipe(
            Effect.map((value) => {
              if (replay) calls.push("witness");
              return { ...value, permissions };
            }),
          ),
        submitCommand: (input) => {
          calls.push("dispatch");
          return fixture.submitCommand(input);
        },
        readCommandReceipt: (input) =>
          Effect.gen(function* () {
            calls.push("lookup");
            if (variant === "absent") return { kind: "absent" as const };
            if (variant === "unknown" || variant === "unavailable")
              return yield* new WorkstreamTransportError({
                operation: "receipt",
                effect: "no-effect",
                detail: variant,
              });
            const receipt = yield* fixture.getCommand(input);
            if (variant === "revoked-during-lookup") permissions = ["workstreams:read"];
            if (variant === "read-revoked-during-lookup") permissions = ["workstreams:write"];
            return {
              kind: "receipt" as const,
              receipt: {
                ...receipt,
                ...(variant === "owner" ? { owner_id: "other-owner" } : {}),
                ...(variant === "principal" ? { actor: { principal_id: "other-principal" } } : {}),
                ...(variant === "generation" ? { server_generation: 8 } : {}),
                ...(variant === "command" ? { command_id: "other-command-0001" } : {}),
                ...(variant === "digest" ? { request_sha256: "0".repeat(64) } : {}),
                ...(variant === "operation" ? { operation: "create_workstream" as const } : {}),
              },
            };
          }),
      },
      { binding },
    );
    const command = updateCommand();
    const retained = yield* gateway.submit(command);
    calls.length = 0;
    replay = true;
    if (variant === "read-only") permissions = ["workstreams:read"];
    if (variant === "write-only") permissions = ["workstreams:write"];
    const result = yield* gateway.submit(command).pipe(Effect.result);
    if (variant === "retained") {
      expect(result._tag).toBe("Success");
      if (result._tag === "Success") expect(result.success).toEqual(retained);
      expect(calls).toEqual(["witness", "witness", "lookup", "witness"]);
    } else {
      expect(result._tag).toBe("Failure");
      if (result._tag === "Failure")
        expect(result.failure.reason).toBe(
          variant === "read-only" || variant === "revoked-during-lookup"
            ? "authorization-not-current"
            : variant === "digest" || variant === "operation"
              ? "idempotency-conflict"
              : variant === "absent"
                ? "version-conflict"
                : "unknown-effect",
        );
    }
    expect(calls).not.toContain("dispatch");
    if (variant === "write-only" || variant === "read-only") expect(calls).not.toContain("lookup");
    if (variant === "absent") expect(calls).toEqual(["witness", "witness", "lookup", "witness"]);
  }),
);
