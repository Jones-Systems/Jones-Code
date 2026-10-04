import { assert, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as DateTime from "effect/DateTime";
import * as Deferred from "effect/Deferred";
import * as Fiber from "effect/Fiber";
import * as Schema from "effect/Schema";
import {
  CommandId,
  ThreadId,
  WorkstreamsNativeSettlementResponse,
  WorkstreamsNativeAttestationResponse,
  WorkstreamsNativeContextResponse,
  type WorkstreamsNativeSettlementRequest,
} from "@t3tools/contracts";
import { NativeStoreAuthorityPersistenceError } from "../../environment/nativeStoreAuthorityPersistence.ts";
import {
  OrchestratorCommandRejectedError,
  NativeWorkstreamSettlementAuthorityError,
} from "../../orchestration-v2/Orchestrator.ts";
import {
  makeWorkstreamsNativeProvider,
  nativeProviderResultEvidence,
  nativeProviderSettlementFromFacts,
} from "./service.ts";
import { NATIVE_PROVIDER_SCOPES, type NativeProviderEnrollmentBinding } from "./enrollment.ts";
import {
  makeProviderFixture,
  nativeSettlementCommand,
  binding,
  request,
  requestBytesSha256,
  attestationRequest,
  now,
  canonicalCommittedResult,
  nativeTestPrincipalLayer,
} from "./testFixtures.ts";

it.layer(nativeTestPrincipalLayer)("native provider correlation", (it) => {
  it.effect("result association digest matches the shared canonical registry fixture", () =>
    Effect.gen(function* () {
      const result = {
        native_outcome: "committed",
        native_evidence: yield* nativeProviderResultEvidence(request, "committed"),
      };
      assert.deepEqual(result, canonicalCommittedResult);
    }),
  );

  it.effect(
    "context and five-minute attestation are closed and come from the enrolled native tuple",
    () =>
      Effect.gen(function* () {
        const fixture = makeProviderFixture();
        const provider = fixture.provider();
        const context = yield* provider.context(binding);
        assert.strictEqual(context.state, "ready");
        yield* Schema.decodeUnknownEffect(WorkstreamsNativeContextResponse)(context);
        if (context.state === "ready") assert.deepEqual(context.context.build, binding.build);
        const result = yield* provider.attest(binding, attestationRequest);
        yield* Schema.decodeUnknownEffect(WorkstreamsNativeAttestationResponse)(result);
        assert.strictEqual(result.state, "attested");
        if (result.state === "attested") {
          assert.strictEqual(
            DateTime.toEpochMillis(DateTime.makeUnsafe(result.expires_at)) -
              DateTime.toEpochMillis(DateTime.makeUnsafe(result.attested_at)),
            300_000,
          );
          assert.strictEqual(result.attestation.native_id, request.identity.native_id);
        }
        assert.strictEqual(fixture.calls.length, 0);
      }),
  );

  it.effect("dispatches native settlement once and keeps unavailable exact observation held", () =>
    Effect.gen(function* () {
      for (const action of ["settle", "unsettle"] as const) {
        const fixture = makeProviderFixture();
        const provider = fixture.provider();
        const input = { ...request, native_action: action };
        const first = yield* provider.settle(binding, input, requestBytesSha256);
        yield* Schema.decodeUnknownEffect(WorkstreamsNativeSettlementResponse)(first);
        assert.strictEqual(first.state, "unknown");
        if (first.state === "unknown") assert.strictEqual(first.reason, "authority_unavailable");
        const attributed = yield* fixture.observeExpected(input);
        assert.strictEqual(attributed.state, "terminal");
        if (attributed.state === "terminal")
          assert.strictEqual(attributed.result.native_outcome, "committed");
        assert.strictEqual(fixture.calls.length, 1);
        assert.notStrictEqual(fixture.calls[0]!.commandId, input.command_id);
        assert.notStrictEqual(fixture.attempts.get(fixture.key(input))!.dispatchStartedAt, null);
        assert.deepEqual(yield* provider.settle(binding, input, requestBytesSha256), first);
        assert.deepEqual(yield* provider.lookup(binding, input, requestBytesSha256), first);
        assert.strictEqual(fixture.calls.length, 1);
      }
    }),
  );

  it.effect(
    "accepts a settlement followed by native companion events through the receipt's final sequence",
    () =>
      Effect.gen(function* () {
        const fixture = makeProviderFixture();
        const provider = makeWorkstreamsNativeProvider({
          ...fixture.ports,
          orchestrator: {
            dispatchNativeWorkstreamSettlement: (input) =>
              Effect.sync(() => {
                const command = nativeSettlementCommand(input);
                fixture.commit(command.commandId, "settle", true);
                return { sequence: 3, storedEvents: [] };
              }),
          },
        });
        yield* provider.settle(binding, request, requestBytesSha256);
        const result = yield* fixture.observeExpected();
        assert.strictEqual(result.state, "terminal");
        if (result.state === "terminal" && result.result.native_outcome === "committed") {
          assert.strictEqual(result.settlement_event!.sequence, 2);
          assert.strictEqual(result.native_receipt!.resultSequence, 3);
        }
      }),
  );

  it.effect("rejects changed immutable association, request bytes, enrollment or reservation", () =>
    Effect.gen(function* () {
      const fixture = makeProviderFixture();
      const provider = fixture.provider();
      yield* provider.settle(binding, request, requestBytesSha256);
      for (const changed of [
        { ...request, reservation_sha256: "f".repeat(64) },
        { ...request, request_sha256: "f".repeat(64) },
        { ...request, native_reference_id: "other-reference" },
        { ...request, native_action: "unsettle" as const },
        { ...request, identity: { ...request.identity, native_id: "other-thread" } },
      ]) {
        const result = yield* provider.settle(binding, changed, requestBytesSha256);
        assert.deepEqual(result, {
          protocol: binding.protocol,
          state: "rejected",
          reason: "idempotency_conflict",
        });
      }
      assert.strictEqual(
        (yield* provider.settle(binding, request, "f".repeat(64))).state,
        "rejected",
      );
      assert.strictEqual(
        (yield* provider.lookup(binding, request, "f".repeat(64))).state,
        "rejected",
      );
      assert.strictEqual(
        (yield* provider.lookup(
          { ...binding, registry_origin: "https://other.invalid" },
          request,
          requestBytesSha256,
        )).state,
        "rejected",
      );
      assert.strictEqual(fixture.calls.length, 1);
    }),
  );

  it.effect(
    "rejects wrong scope, caller, source, build and authority generation before reservation",
    () =>
      Effect.gen(function* () {
        const fixture = makeProviderFixture();
        const provider = fixture.provider();
        const cases: ReadonlyArray<{
          readonly enrolled: NativeProviderEnrollmentBinding;
          readonly input: WorkstreamsNativeSettlementRequest;
          readonly reason: Extract<
            WorkstreamsNativeSettlementResponse,
            { state: "rejected" }
          >["reason"];
        }> = [
          {
            enrolled: { ...binding, scopes: [NATIVE_PROVIDER_SCOPES.context] },
            input: request,
            reason: "forbidden",
          },
          {
            enrolled: binding,
            input: { ...request, owner_id: "other-owner" },
            reason: "caller_mismatch",
          },
          {
            enrolled: binding,
            input: { ...request, principal_id: "other-principal" },
            reason: "caller_mismatch",
          },
          {
            enrolled: binding,
            input: {
              ...request,
              identity: { ...request.identity, source_instance_id: "other-source" },
            },
            reason: "source_mismatch",
          },
          {
            enrolled: { ...binding, build: { ...binding.build, sha: "f".repeat(40) } },
            input: request,
            reason: "contract_mismatch",
          },
          {
            enrolled: binding,
            input: { ...request, expected_store_generation: request.expected_store_generation + 1 },
            reason: "generation_conflict",
          },
        ];
        for (const entry of cases)
          assert.deepEqual(
            yield* provider.settle(entry.enrolled, entry.input, requestBytesSha256),
            {
              protocol: binding.protocol,
              state: "rejected",
              reason: entry.reason,
            },
          );
        assert.strictEqual(fixture.calls.length, 0);
        assert.strictEqual(fixture.attempts.size, 0);
      }),
  );

  it.effect("fenced authority and deleted thread do not attest or dispatch", () =>
    Effect.gen(function* () {
      const fixture = makeProviderFixture();
      const fenced = makeWorkstreamsNativeProvider({
        ...fixture.ports,
        authority: {
          readCurrent: Effect.fail(new NativeStoreAuthorityPersistenceError("fenced", "fixture")),
        },
      });
      assert.deepEqual(yield* fenced.attest(binding, attestationRequest), {
        protocol: binding.protocol,
        state: "rejected",
        reason: "authority_fenced",
      });
      assert.deepEqual(yield* fenced.settle(binding, request, requestBytesSha256), {
        protocol: binding.protocol,
        state: "rejected",
        reason: "authority_fenced",
      });
      const deleted = makeWorkstreamsNativeProvider({
        ...fixture.ports,
        threadExists: () => Effect.succeed(false),
      });
      assert.deepEqual(yield* deleted.attest(binding, attestationRequest), {
        protocol: binding.protocol,
        state: "rejected",
        reason: "thread_not_found",
      });
      assert.deepEqual(yield* deleted.settle(binding, request, requestBytesSha256), {
        protocol: binding.protocol,
        state: "rejected",
        reason: "thread_not_found",
      });
      assert.strictEqual(fixture.calls.length, 0);
    }),
  );

  it.effect("authority change across targeted existence prevents attestation and invocation", () =>
    Effect.gen(function* () {
      const fixture = makeProviderFixture();
      let reads = 0;
      const provider = makeWorkstreamsNativeProvider({
        ...fixture.ports,
        authority: {
          readCurrent: Effect.sync(() => ({
            environmentId: binding.source_instance_id,
            authorityNamespace: binding.authority_namespace,
            storeGeneration: binding.store_generation + (reads++ % 2),
          })),
        },
      });
      assert.deepEqual(yield* provider.attest(binding, attestationRequest), {
        protocol: binding.protocol,
        state: "unknown",
        reason: "authority_changed",
      });
      const result = yield* provider.settle(binding, request, requestBytesSha256);
      assert.strictEqual(result.state, "unknown");
      if (result.state === "unknown") assert.strictEqual(result.reason, "authority_changed");
      assert.strictEqual(fixture.attempts.size, 0);
      assert.strictEqual(fixture.calls.length, 0);
    }),
  );

  it.effect(
    "lost response and missing native receipt remain observation-only after durable dispatch-start",
    () =>
      Effect.gen(function* () {
        const fixture = makeProviderFixture();
        const dispatched = yield* Deferred.make<void>();
        let invocations = 0;
        const provider = makeWorkstreamsNativeProvider({
          ...fixture.ports,
          orchestrator: {
            dispatchNativeWorkstreamSettlement: () =>
              Effect.gen(function* () {
                invocations += 1;
                yield* Deferred.succeed(dispatched, undefined);
                return yield* Effect.never;
              }),
          },
        });
        const pending = yield* Effect.forkChild(
          provider.settle(binding, request, requestBytesSha256),
        );
        yield* Deferred.await(dispatched);
        yield* Fiber.interrupt(pending);
        const missing = yield* provider.lookup(binding, request, requestBytesSha256);
        assert.strictEqual(missing.state, "unknown");
        if (missing.state === "unknown") assert.strictEqual(missing.reason, "receipt_missing");
        yield* provider.settle(binding, request, requestBytesSha256);
        assert.strictEqual(invocations, 1);
        const attempt = fixture.attempts.get(fixture.key(request))!;
        fixture.commit(attempt.nativeCommandId);
        const recovered = yield* provider.lookup(binding, request, requestBytesSha256);
        assert.strictEqual(recovered.state, "unknown");
        if (recovered.state === "unknown")
          assert.strictEqual(recovered.reason, "authority_unavailable");
        assert.strictEqual((yield* fixture.observeExpected()).state, "terminal");
        assert.strictEqual(invocations, 1);
      }),
  );

  it.effect("lookup of a missing or unstarted attempt never reserves or dispatches", () =>
    Effect.gen(function* () {
      const fixture = makeProviderFixture();
      const provider = fixture.provider();
      const missing = yield* provider.lookup(binding, request, requestBytesSha256);
      assert.strictEqual(missing.state, "unknown");
      if (missing.state === "unknown") assert.strictEqual(missing.reason, "attempt_not_found");
      assert.strictEqual(fixture.attempts.size, 0);
      yield* provider.settle(binding, request, requestBytesSha256);
      const attempt = fixture.attempts.get(fixture.key(request))!;
      fixture.attempts.set(fixture.key(request), { ...attempt, dispatchStartedAt: null });
      const unstarted = yield* provider.lookup(binding, request, requestBytesSha256);
      assert.strictEqual(unstarted.state, "unknown");
      assert.strictEqual(fixture.calls.length, 1);
    }),
  );

  it.effect("concurrent duplicate attempts share one durable dispatch claim", () =>
    Effect.gen(function* () {
      const fixture = makeProviderFixture();
      const started = yield* Deferred.make<void>();
      const finish = yield* Deferred.make<void>();
      let calls = 0;
      const provider = makeWorkstreamsNativeProvider({
        ...fixture.ports,
        orchestrator: {
          dispatchNativeWorkstreamSettlement: (input) =>
            Effect.gen(function* () {
              const command = nativeSettlementCommand(input);
              calls += 1;
              yield* Deferred.succeed(started, undefined);
              yield* Deferred.await(finish);
              fixture.commit(command.commandId);
              return { sequence: 2, storedEvents: [] };
            }),
        },
      });
      const first = yield* Effect.forkChild(provider.settle(binding, request, requestBytesSha256));
      yield* Deferred.await(started);
      const duplicate = yield* provider.settle(binding, request, requestBytesSha256);
      assert.strictEqual(duplicate.state, "unknown");
      assert.strictEqual(calls, 1);
      yield* Deferred.succeed(finish, undefined);
      assert.strictEqual((yield* Fiber.join(first)).state, "unknown");
      assert.strictEqual(
        (yield* provider.lookup(binding, request, requestBytesSha256)).state,
        "unknown",
      );
      assert.strictEqual((yield* fixture.observeExpected()).state, "terminal");
      assert.strictEqual(calls, 1);
    }),
  );

  it.effect("contradictory receipt or event attribution stays unknown", () =>
    Effect.gen(function* () {
      for (const fault of [
        "command",
        "thread",
        "sequence",
        "action",
        "missing_event",
        "rejected_with_events",
        "overflow",
        "legacy_event",
        "missing_identity",
        "identity_type",
        "identity_digest",
        "duplicate_settlement",
        "event_gap",
        "accepted_at",
      ] as const) {
        const fixture = makeProviderFixture();
        const provider = fixture.provider();
        yield* provider.settle(binding, request, requestBytesSha256);
        const commandId = fixture.calls[0]!.commandId;
        const receipt = fixture.receipts.get(commandId)!;
        const event = fixture.events.get(commandId)![0]!;
        if (fault === "command")
          fixture.receipts.set(commandId, {
            ...receipt,
            commandId: CommandId.make("other-command"),
          });
        if (fault === "thread")
          fixture.events.set(commandId, [{ ...event, aggregateId: ThreadId.make("other-thread") }]);
        if (fault === "sequence")
          fixture.receipts.set(commandId, { ...receipt, resultSequence: 999 });
        if (fault === "action")
          fixture.events.set(commandId, [{ ...event, type: "thread.unsettled" }]);
        if (fault === "missing_event") fixture.events.set(commandId, []);
        if (fault === "rejected_with_events")
          fixture.receipts.set(commandId, { ...receipt, status: "rejected" });
        if (fault === "overflow")
          fixture.events.set(
            commandId,
            Array.from({ length: 257 }, () => event),
          );
        if (fault === "legacy_event")
          fixture.events.set(commandId, [{ ...event, applicationEventVersion: 1 }]);
        if (fault === "missing_identity") fixture.identities.delete(commandId);
        if (fault === "identity_type")
          fixture.identities.set(commandId, {
            ...fixture.identities.get(commandId)!,
            commandType: "thread.unsettle",
          });
        if (fault === "identity_digest")
          fixture.identities.set(commandId, {
            ...fixture.identities.get(commandId)!,
            bindingDigest: "unproved",
          });
        if (fault === "duplicate_settlement") {
          fixture.receipts.set(commandId, { ...receipt, resultSequence: 3 });
          fixture.events.set(commandId, [
            event,
            { ...event, eventId: "second-settlement", sequence: 3 },
          ]);
        }
        if (fault === "event_gap") {
          fixture.receipts.set(commandId, { ...receipt, resultSequence: 4 });
          fixture.events.set(commandId, [
            event,
            { ...event, eventId: "companion-gap", type: "thread.unpinned", sequence: 4 },
          ]);
        }
        if (fault === "accepted_at")
          fixture.receipts.set(commandId, {
            ...receipt,
            acceptedAt: DateTime.add(receipt.acceptedAt, { seconds: 1 }),
          });
        const result = yield* provider.lookup(binding, request, requestBytesSha256);
        assert.strictEqual(result.state, "unknown");
        if (result.state === "unknown") assert.strictEqual(result.reason, "evidence_conflict");
        assert.strictEqual(fixture.calls.length, 1);
      }
    }),
  );

  it.effect("native rejected receipt maps to denied without exposing native error text", () =>
    Effect.gen(function* () {
      const fixture = makeProviderFixture();
      const provider = makeWorkstreamsNativeProvider({
        ...fixture.ports,
        orchestrator: {
          dispatchNativeWorkstreamSettlement: (input) =>
            Effect.gen(function* () {
              const command = nativeSettlementCommand(input);
              fixture.recordIdentity(command.commandId);
              fixture.receipts.set(command.commandId, {
                commandId: command.commandId,
                commandType: command.type,
                threadId: ThreadId.make(request.identity.native_id),
                acceptedAt: DateTime.makeUnsafe(now),
                resultSequence: 1,
                status: "rejected",
                error: "private native error fixture",
              });
              return yield* new OrchestratorCommandRejectedError({
                commandId: command.commandId,
                commandType: command.type,
                cause: "Synthetic settlement denial.",
              });
            }),
        },
      });
      const held = yield* provider.settle(binding, request, requestBytesSha256);
      assert.strictEqual(held.state, "unknown");
      const result = yield* fixture.observeExpected();
      yield* Schema.decodeUnknownEffect(WorkstreamsNativeSettlementResponse)(result);
      assert.strictEqual(result.state, "terminal");
      if (result.state === "terminal") {
        assert.strictEqual(result.result.native_outcome, "denied");
        assert.strictEqual(result.settlement_event, null);
      }
      const encoded = yield* Schema.encodeEffect(
        Schema.fromJsonString(WorkstreamsNativeSettlementResponse),
      )(result);
      assert.strictEqual(encoded.includes("private native error"), false);
      assert.strictEqual(fixture.events.size, 0);
    }),
  );

  it.effect("authority changes after invocation prevent a terminal success claim", () =>
    Effect.gen(function* () {
      const fixture = makeProviderFixture();
      let reads = 0;
      const provider = makeWorkstreamsNativeProvider({
        ...fixture.ports,
        authority: {
          readCurrent: Effect.sync(() => ({
            environmentId: binding.source_instance_id,
            authorityNamespace: binding.authority_namespace,
            storeGeneration: binding.store_generation + (reads++ >= 4 ? 1 : 0),
          })),
        },
      });
      const result = yield* provider.settle(binding, request, requestBytesSha256);
      assert.strictEqual(result.state, "unknown");
      if (result.state === "unknown") assert.strictEqual(result.reason, "authority_changed");
      assert.strictEqual(fixture.calls.length, 1);
    }),
  );

  it.effect("missing V2 birth evidence cannot attest or reserve native settlement", () =>
    Effect.gen(function* () {
      const fixture = makeProviderFixture();
      const provider = makeWorkstreamsNativeProvider({
        ...fixture.ports,
        eventSink: { ...fixture.ports.eventSink, getThreadIncarnation: () => Effect.succeed(null) },
      });
      assert.deepEqual(yield* provider.attest(binding, attestationRequest), {
        protocol: binding.protocol,
        state: "unknown",
        reason: "observation_unavailable",
      });
      const settlement = yield* provider.settle(binding, request, requestBytesSha256);
      assert.strictEqual(settlement.state, "unknown");
      assert.strictEqual(fixture.attempts.size, 0);
      assert.strictEqual(fixture.calls.length, 0);
    }),
  );

  it.effect(
    "overflow sentinel and legacy lineage cannot claim settlement even with a correlated receipt",
    () =>
      Effect.gen(function* () {
        for (const fault of ["overflow_flag", "legacy_lineage", "missing_birth"] as const) {
          const fixture = makeProviderFixture();
          const provider = fixture.provider();
          yield* provider.settle(binding, request, requestBytesSha256);
          const observing = makeWorkstreamsNativeProvider({
            ...fixture.ports,
            eventSink: {
              ...fixture.ports.eventSink,
              readNativeCommandFacts: (input) =>
                fixture.ports.eventSink
                  .readNativeCommandFacts(input)
                  .pipe(
                    Effect.map((facts) => ({
                      ...facts,
                      ...(fault === "overflow_flag" ? { eventMetadataOverflow: true } : {}),
                      ...(fault === "legacy_lineage"
                        ? { creationProvenance: "legacy_import" as const }
                        : {}),
                      ...(fault === "missing_birth" ? { incarnation: null } : {}),
                    })),
                  ),
            },
          });
          const result = yield* observing.lookup(binding, request, requestBytesSha256);
          assert.strictEqual(result.state, "unknown");
          if (result.state === "unknown") assert.strictEqual(result.reason, "evidence_conflict");
          assert.strictEqual(fixture.calls.length, 1);
        }
      }),
  );

  it.effect(
    "native authority unavailable cannot fall back to ordinary dispatch or retry a started attempt",
    () =>
      Effect.gen(function* () {
        const fixture = makeProviderFixture();
        let calls = 0;
        const provider = makeWorkstreamsNativeProvider({
          ...fixture.ports,
          orchestrator: {
            dispatchNativeWorkstreamSettlement: (input) =>
              Effect.gen(function* () {
                calls += 1;
                assert.deepEqual(input.enrollment, binding);
                assert.deepEqual(input.request, request);
                assert.strictEqual(input.attempt, fixture.attempts.get(fixture.key(request)));
                assert.notStrictEqual(input.attempt.dispatchStartedAt, null);
                return yield* new NativeWorkstreamSettlementAuthorityError({
                  commandId: input.attempt.nativeCommandId,
                  code: "authority_unavailable",
                });
              }),
          },
        });
        const result = yield* provider.settle(binding, request, requestBytesSha256);
        assert.strictEqual(result.state, "unknown");
        if (result.state === "unknown") assert.strictEqual(result.reason, "authority_unavailable");
        assert.strictEqual(fixture.receipts.size, 0);
        assert.strictEqual(fixture.events.size, 0);
        yield* provider.settle(binding, request, requestBytesSha256);
        yield* provider.lookup(binding, request, requestBytesSha256);
        assert.strictEqual(calls, 1);
      }),
  );

  it.effect(
    "explicit expected identity compares both opaque digests without treating valid shape as correlation",
    () =>
      Effect.gen(function* () {
        const fixture = makeProviderFixture();
        yield* fixture.provider().settle(binding, request, requestBytesSha256);
        const attempt = fixture.attempts.get(fixture.key(request))!;
        const facts = yield* fixture.ports.eventSink.readNativeCommandFacts({
          threadId: ThreadId.make(request.identity.native_id),
          commandId: CommandId.make(attempt.nativeCommandId),
        });
        assert.notStrictEqual(facts.identity, null);
        const identity = facts.identity!;
        for (const expected of [
          null,
          { ...identity, normalizedCommandDigest: "c".repeat(64) },
          { ...identity, bindingDigest: "c".repeat(64) },
        ]) {
          const result = yield* nativeProviderSettlementFromFacts(attempt, facts, expected);
          assert.strictEqual(result.state, "unknown");
          if (result.state === "unknown")
            assert.strictEqual(
              result.reason,
              expected === null ? "authority_unavailable" : "evidence_conflict",
            );
        }
        assert.strictEqual(
          (yield* nativeProviderSettlementFromFacts(attempt, facts, identity)).state,
          "terminal",
        );
        assert.strictEqual(fixture.calls.length, 1);
      }),
  );

  it.effect(
    "explicit expected identity accepts 256 contiguous V2 events and rejects the 257th sentinel",
    () =>
      Effect.gen(function* () {
        const fixture = makeProviderFixture();
        yield* fixture.provider().settle(binding, request, requestBytesSha256);
        const attempt = fixture.attempts.get(fixture.key(request))!;
        const facts = yield* fixture.ports.eventSink.readNativeCommandFacts({
          threadId: ThreadId.make(request.identity.native_id),
          commandId: CommandId.make(attempt.nativeCommandId),
        });
        const event = facts.eventMetadata[0]!;
        const eventMetadata = Array.from({ length: 256 }, (_, index) => ({
          ...event,
          eventId: `synthetic-bounded-${index}`,
          sequence: index + 2,
          type: index === 0 ? "thread.settled" : "thread.unpinned",
        }));
        const bounded = {
          ...facts,
          eventMetadata,
          snapshotSequence: 257,
          receipt: { ...facts.receipt!, resultSequence: 257 },
        };
        const result = yield* nativeProviderSettlementFromFacts(attempt, bounded, facts.identity);
        assert.strictEqual(result.state, "terminal");
        if (result.state === "terminal") {
          assert.strictEqual(result.native_receipt?.resultSequence, 257);
          assert.strictEqual(result.settlement_event?.sequence, 2);
        }
        const overflow = yield* nativeProviderSettlementFromFacts(
          attempt,
          {
            ...bounded,
            eventMetadata: [
              ...eventMetadata,
              { ...event, eventId: "synthetic-overflow", sequence: 258 },
            ],
            eventMetadataOverflow: true,
          },
          facts.identity,
        );
        assert.strictEqual(overflow.state, "unknown");
        if (overflow.state === "unknown") assert.strictEqual(overflow.reason, "evidence_conflict");
      }),
  );
});
