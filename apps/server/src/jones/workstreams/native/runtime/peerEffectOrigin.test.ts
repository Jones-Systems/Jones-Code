import * as Schema from "effect/Schema";
import * as SqlClient from "effect/sql/SqlClient";
import * as Forward from "@t3tools/contracts/jones/workstreamsForwarding";
import * as Db from "../store/sql.ts";
import * as Effects from "../store/effectStore.ts";
import * as Registry from "../store/registryStore.ts";
import type { ReceiptRecord } from "../store/receiptStore.ts";
import type { ExternalIntent } from "../domain/state.ts";
import { createWorkstreamDomainState } from "../domain/state.ts";
import { completeNativeExternalIntent } from "../domain/decider.ts";
import { nativeSettlementResult } from "../domain/externalIntents.ts";
import { expect, it } from "@effect/vitest";
import { canonicalWorkstreamsSnapshotJson as canonical } from "@t3tools/contracts/jones/workstreamsSnapshot";
import { vi } from "vite-plus/test";
import * as Effect from "effect/Effect";
import * as Deferred from "effect/Deferred";
import * as Fiber from "effect/Fiber";
import * as Option from "effect/Option";
import * as Mcp from "../../../../mcp/McpInvocationContext.ts";
import * as Origin from "./peerEffectOrigin.ts";
import * as Fixture from "./peerEffectRecovery.testkit.ts";

vi.mock("../../../../../package.json", () => ({
  default: {
    version: "0.0.0-preview.20261002.100",
    jonesSource: {
      repository: "Jones-Systems/Jones-Code",
      sha: "a".repeat(40),
      tree: "b".repeat(40),
    },
  },
}));
it.effect(
  "mints an invocation-scoped origin only from the encrypted reservation, receipt and exact parent",
  () =>
    Fixture.withFixture((f) =>
      Effect.gen(function* () {
        const origins = yield* Origin.PeerEffectOrigins;
        let escaped: Origin.ValidatedPeerReservation | undefined;
        yield* origins.withOrigin(f.command.command_id, (proof) =>
          Effect.gen(function* () {
            escaped = proof;
            expect(proof.envelope.request_body).toBe(f.receipt.request_body);
            expect(proof.intent).toEqual(f.intent);
            expect(proof.parent.identity.forward_id).not.toBe(proof.command_id);
            expect(Object.isFrozen(proof.envelope.recovery)).toBe(true);
            expect(Option.isNone(yield* Effect.serviceOption(Mcp.McpInvocationContext))).toBe(true);
            yield* f.sql.withTransaction(Origin.checkOriginSnapshot(proof));
            expect((yield* Origin.checkOrigin({ ...proof }).pipe(Effect.result))._tag).toBe(
              "Failure",
            );
          }),
        );
        expect(escaped).toBeDefined();
        if (escaped)
          expect((yield* Origin.checkOrigin(escaped).pipe(Effect.result))._tag).toBe("Failure");
      }),
    ),
);
it.effect("refuses ambient preparation before any secret lookup", () =>
  Fixture.withFixture((f) =>
    Effect.gen(function* () {
      const origins = yield* Origin.PeerEffectOrigins;
      let keyCalls = 0;
      let used = false;
      f.setSecretHook(() =>
        Effect.sync(() => {
          keyCalls++;
        }),
      );
      const error = yield* f.sql
        .withTransaction(
          origins.withOrigin(f.command.command_id, () =>
            Effect.sync(() => {
              used = true;
            }),
          ),
        )
        .pipe(Effect.flip);
      expect(error).toMatchObject({ code: "preparation_in_transaction" });
      expect(keyCalls).toBe(0);
      expect(used).toBe(false);
    }),
  ),
);
it.effect("a blocked actual key acquisition holds no selected SQLite writer", () =>
  Fixture.withFixture((f) =>
    Effect.gen(function* () {
      const origins = yield* Origin.PeerEffectOrigins;
      const entered = yield* Deferred.make<void>();
      const release = yield* Deferred.make<void>();
      let writerSeen = false;
      f.setSecretHook(() =>
        Effect.gen(function* () {
          writerSeen ||= Option.isSome(yield* Effect.serviceOption(f.sql.transactionService));
          yield* Deferred.succeed(entered, undefined);
          yield* Deferred.await(release);
        }),
      );
      const work = yield* origins
        .withOrigin(f.command.command_id, Origin.checkOriginSnapshot)
        .pipe(Effect.forkChild);
      yield* Deferred.await(entered);
      yield* Effect.gen(function* () {
        yield* f.sql.withTransaction(
          f.sql`INSERT INTO writer_probe(value) VALUES ('independent commit')`,
        );
        expect((yield* f.sql`SELECT value FROM writer_probe`)[0]?.value).toBe("independent commit");
        yield* Deferred.succeed(release, undefined);
        yield* Fiber.join(work);
        expect(writerSeen).toBe(false);
      }).pipe(Effect.ensuring(Deferred.succeed(release, undefined)));
    }),
  ),
);
it.effect("missing historical keys do not become absence or a dispatch permission", () =>
  Fixture.withFixture((f) =>
    Effect.gen(function* () {
      const origins = yield* Origin.PeerEffectOrigins;
      f.keys.clear();
      let used = false;
      const failure = yield* origins
        .withOrigin(f.command.command_id, () =>
          Effect.sync(() => {
            used = true;
          }),
        )
        .pipe(Effect.result);
      expect(failure._tag).toBe("Failure");
      expect(used).toBe(false);
      expect(
        (yield* f.sql`SELECT state FROM jones_workstreams_external_reservations`)[0]?.state,
      ).toBe("active");
      expect((yield* f.sql`SELECT state FROM jones_workstreams_forward_receipts`)[0]?.state).toBe(
        "accepted",
      );
    }),
  ),
);
it.effect(
  "exact snapshots fence parent mutation without mistaking the proof itself for a row lock",
  () =>
    Fixture.withFixture((f) =>
      Effect.gen(function* () {
        const origins = yield* Origin.PeerEffectOrigins;
        yield* origins.withOrigin(f.command.command_id, (proof) =>
          Effect.gen(function* () {
            yield* f.sql`UPDATE jones_workstreams_forward_receipts SET state='completed',result_envelope=request_envelope`;
            expect((yield* Origin.checkOriginSnapshot(proof).pipe(Effect.result))._tag).toBe(
              "Failure",
            );
            // Lifetime/current-owner proof remains valid; it is not completion or dispatch authority.
            yield* Origin.checkOrigin(proof);
          }),
        );
      }),
    ),
);
it.effect("a parent completed while its command is pending cannot mint a recovery proof", () =>
  Fixture.withFixture((f) =>
    Effect.gen(function* () {
      const origins = yield* Origin.PeerEffectOrigins;
      yield* f.sql`UPDATE jones_workstreams_forward_receipts SET state='completed',result_envelope=request_envelope`;
      let used = false;
      expect(
        (yield* origins
          .withOrigin(f.command.command_id, () =>
            Effect.sync(() => {
              used = true;
            }),
          )
          .pipe(Effect.result))._tag,
      ).toBe("Failure");
      expect(used).toBe(false);
    }),
  ),
);
it.effect("a command ID without all three authoritative records supplies no recovery origin", () =>
  Fixture.withFixture(() =>
    Effect.gen(function* () {
      const origins = yield* Origin.PeerEffectOrigins;
      const failure = yield* origins
        .withOrigin("command:missing-000000", () => Effect.void)
        .pipe(Effect.flip);
      expect(failure.code).toBe("not_found");
    }),
  ),
);

it.effect("preserves the callback failure while invalidating the origin lifetime", () =>
  Fixture.withFixture((f) =>
    Effect.gen(function* () {
      const origins = yield* Origin.PeerEffectOrigins;
      const marker = { code: "provider_refused_after_observation" };
      const error = yield* origins
        .withOrigin(f.command.command_id, () => Effect.fail(marker))
        .pipe(Effect.flip);
      expect(error).toBe(marker);
    }),
  ),
);

it.effect.each(["wire-bytes", "identity-digest", "source-environment"] as const)(
  "refuses authenticated parent ciphertext/metadata substitution: %s",
  (kind) =>
    Fixture.withFixture((f) =>
      Effect.gen(function* () {
        const origins = yield* Origin.PeerEffectOrigins;
        // Simulate corrupt retained storage in this isolated synthetic database. Ordinary writes
        // remain protected by the immutable trigger; no production migration or trigger is changed.
        yield* f.sql`DROP TRIGGER jones_workstreams_forward_receipts_immutable`;
        if (kind === "wire-bytes") {
          const envelope = yield* f.db.sealJson(
            {
              ...Fixture.scope,
              purpose: "forward_request",
              record_id: canonical([
                f.identity.source_authority.environment_id,
                f.identity.forward_id,
              ]),
              revision: 1,
            },
            f.wire + " ",
          );
          yield* f.sql`UPDATE jones_workstreams_forward_receipts SET request_envelope=${envelope}`;
        } else if (kind === "identity-digest") {
          yield* f.sql`UPDATE jones_workstreams_forward_receipts SET identity_sha256=${"f".repeat(64)}`;
        } else {
          yield* f.sql`UPDATE jones_workstreams_forward_receipts SET source_environment_id='source:replacement'`;
        }
        let used = false;
        expect(
          (yield* origins
            .withOrigin(f.command.command_id, () =>
              Effect.sync(() => {
                used = true;
              }),
            )
            .pipe(Effect.result))._tag,
        ).toBe("Failure");
        expect(used).toBe(false);
        expect(
          (yield* f.sql`SELECT state FROM jones_workstreams_external_reservations`)[0]?.state,
        ).toBe("active");
      }),
    ),
);

const decodeForwardResult = Schema.decodeUnknownEffect(Forward.WorkstreamsForwardResult);
const completeSyntheticParent = Effect.fn("PeerOriginTest.completeSyntheticParent")(function* (f: {
  readonly intent: ExternalIntent;
  readonly receipt: ReceiptRecord;
  readonly identity: Forward.WorkstreamsForwardIdentity;
  readonly pin: { readonly pin_id: string; readonly revision: number };
}) {
  const db = yield* Db.NativeSql;
  const sql = yield* SqlClient.SqlClient;
  const effects = yield* Effects.EffectStore;
  const registry = yield* Registry.RegistryStore;
  const context = (yield* registry.context(Fixture.scope))!;
  const result = nativeSettlementResult(f.intent, "committed");
  const state = createWorkstreamDomainState(Fixture.scope.owner_id);
  state.registry_version = context.registry_version;
  state.receipts.set(f.intent.command.command_id, f.receipt.receipt);
  state.external_intents.set(f.intent.command.command_id, f.intent);
  const completed = completeNativeExternalIntent(
    state,
    f.intent,
    { next: () => "event:synthetic-peer-complete" },
    result,
    f.intent.accepted_at,
    true,
  );
  if (completed.receipt.state !== "committed")
    return yield* Effect.die("Expected committed fixture");
  const forward = yield* decodeForwardResult({
    contract: Forward.WORKSTREAMS_NATIVE_FORWARDING_CONTRACT,
    kind: "command",
    identity: f.identity,
    result: completed.receipt,
    home: {
      registry: {
        ...f.identity.expected_home,
        canonical_origin: f.identity.canonical_origin,
        registry_version: completed.receipt.registry_version,
      },
      principal_id: f.identity.principal_id,
      authorization_revision: 1,
      source_binding_version: f.identity.source_binding_version,
      incoming_peer_pin_id: f.pin.pin_id,
      incoming_peer_pin_revision: f.pin.revision,
    },
  });
  const aad = {
    ...Fixture.scope,
    purpose: "forward_result",
    record_id: canonical([f.identity.source_authority.environment_id, f.identity.forward_id]),
    revision: 1,
  };
  const ciphertext = yield* db.sealJson(aad, canonical(forward));
  // Synthetic completion exercises the authoritative domain/stores and same-transaction parent.
  // The not-yet-mounted PEER provider/completion owner is not substituted by this helper.
  yield* db.transaction(
    Fixture.scope,
    Effect.gen(function* () {
      yield* effects.saveResult(Fixture.scope, f.intent.command.command_id, result);
      if (completed.receipt.state === "committed" && completed.receipt.changed)
        yield* registry.advance(Fixture.scope, {
          server_generation: 1,
          registry_version: context.registry_version,
        });
      yield* effects.completeRetainedNativeSettlement(
        Fixture.scope,
        { ...f.receipt, receipt: completed.receipt },
        { server_generation: 1, registry_version: context.registry_version },
        completed.state.audit.map((event) => ({
          event_id: event.event_id,
          command_id: event.command_id,
          registry_version: event.registry_version,
          occurred_at: event.occurred_at,
          workstream_ids: event.workstream_versions.map((v) => v.workstream_id),
          value: event,
        })),
      );
      yield* sql`UPDATE jones_workstreams_forward_receipts SET state='completed',result_envelope=${ciphertext}`;
    }),
  );
  return { forward, aad };
});
it.effect("released recovery retains the exact terminal command and parent result", () =>
  Fixture.withFixture((f) =>
    Effect.gen(function* () {
      const saved = yield* completeSyntheticParent(f);
      const origins = yield* Origin.PeerEffectOrigins;
      yield* origins.withOrigin(f.command.command_id, (proof) =>
        Effect.gen(function* () {
          expect(proof.parent_result).toEqual(saved.forward);
          expect(proof.receipt.state).toBe("committed");
          yield* Origin.checkOriginSnapshot(proof);
        }),
      );
    }),
  ),
);
it.effect(
  "a released parent's authenticated result must match its authoritative command receipt",
  () =>
    Fixture.withFixture((f) =>
      Effect.gen(function* () {
        const saved = yield* completeSyntheticParent(f);
        const origins = yield* Origin.PeerEffectOrigins;
        const wrong = yield* f.db.sealJson(
          saved.aad,
          canonical({
            ...saved.forward,
            home: { ...saved.forward.home, incoming_peer_pin_id: "pin:replacement-home" },
          }),
        );
        yield* f.sql`DROP TRIGGER jones_workstreams_forward_receipts_immutable`;
        yield* f.sql`UPDATE jones_workstreams_forward_receipts SET result_envelope=${wrong}`;
        let used = false;
        expect(
          (yield* origins
            .withOrigin(f.command.command_id, () =>
              Effect.sync(() => {
                used = true;
              }),
            )
            .pipe(Effect.result))._tag,
        ).toBe("Failure");
        expect(used).toBe(false);
      }),
    ),
);
