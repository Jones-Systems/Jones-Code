// @effect-diagnostics nodeBuiltinImport:off -- Synthetic signatures and fixture bytes never use real keys.
import * as NodeCrypto from "node:crypto";
import { expect, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as SqlClient from "effect/sql/SqlClient";
import * as Schema from "effect/Schema";
import * as TestClock from "effect/testing/TestClock";
import * as Wire from "@t3tools/contracts";
import { WORKSTREAMS_LEGACY_RECEIPTS_CONTRACT } from "@t3tools/contracts/jones/workstreamsLegacyReceipts";
import {
  WORKSTREAMS_LEGACY_RECONCILIATION_CONTRACT as contract,
  LegacyReservation,
  LegacyResult,
  type LegacyReconciliationRequest,
} from "../../../../../../../packages/contracts/src/jones/workstreamsLegacyReconciliation.ts";
import vectors from "./fixtures/v1/vectors.json" with { type: "json" };
import migration from "../../../persistence/Migrations/125_JonesWorkstreamsNative.ts";
import { testLayer } from "../store/sql.testkit.ts";
import * as Archive from "../store/archiveStore.ts";
import * as Receipts from "../store/receiptStore.ts";
import {
  readLegacyLedger,
  LEGACY_RECONCILIATION_CLASS,
} from "../store/legacyReconciliationStore.ts";
import * as Registry from "../store/registryStore.ts";
import { LegacyStoredObservation } from "../../../../../../../packages/contracts/src/jones/workstreamsLegacyReconciliation.ts";
import { nativeSettlementResult } from "../domain/externalIntents.ts";
import { canonical, digest } from "./canonical.ts";
import {
  parseSnapshot,
  encodeSnapshot,
  encryptSnapshot,
  type SnapshotRow,
} from "./snapshotReader.ts";
import { plan } from "./plan.ts";
import { apply } from "./apply.ts";
import { readback } from "./readback.ts";
import { activate } from "./activate.ts";
import { prepareReverse } from "./reverseExport.ts";
import { reconcileLegacyOpen, LegacySourceLookup } from "./reconcileLegacyOpen.ts";

const original = parseSnapshot(vectors.full_snapshot.canonical_jsonl);
const registry = original.header.registry.registry_instance_id,
  owner = "synthetic.owner";
const principal = "20000000-0000-4000-8000-000000000001",
  key = Buffer.alloc(32, 7);
const scope = { registry_instance_id: registry, owner_id: owner };
const keys = () =>
  new Map([
    ["workstreams-content-key-synthetic-transition-key", key],
    [`workstreams-index-key-${registry}`, Buffer.alloc(32, 8)],
  ]);
const { privateKey, publicKey } = NodeCrypto.generateKeyPairSync("ed25519");
const at = "2026-10-25T12:00:01.000Z",
  accepted = "2026-10-25T12:00:00.000Z";
type Kind = LegacyReservation["kind"];
const parseReservation = Schema.decodeUnknownSync(LegacyReservation);
const decodeResult = Schema.decodeUnknownEffect(LegacyResult);
const decodeStoredObservation = Schema.decodeUnknownEffect(LegacyStoredObservation);
const parseResult = Schema.decodeUnknownSync(LegacyResult);
const isFreshCommand = Schema.is(Wire.WorkstreamCommand);
function scenario(
  kinds: readonly Kind[],
  retained = false,
  outcome: "committed" | "denied" | "unsupported" | "failed" = "committed",
) {
  const rows: SnapshotRow[] = original.rows.filter(
    (r) =>
      ["workstream_registry_identity", "workstream_registry_context"].includes(r.class) &&
      (r.row.owner_id === undefined || r.row.owner_id === owner),
  );
  const reservations: LegacyReservation[] = [],
    results: LegacyResult[] = [];
  const grant = {
    owner_id: owner,
    principal_id: principal,
    grant_id: "synthetic.grant",
    grant_version: 1,
    permissions: ["workstreams:read", "workstreams:write"],
    state: "active",
  };
  rows.push({ class: "workstream_owner_grants", key: [owner, principal, 1], row: grant });
  for (const [i, kind] of kinds.entries()) {
    const id = "command:" + i,
      refId = "reference:" + i,
      provider = kind === "refresh_linked_pr" ? "github" : "t3",
      source = "synthetic.environment";
    const identity = {
      provider,
      source_instance_id: source,
      resource_kind: provider === "t3" ? "thread" : "pull_request",
      id_kind: "internal",
      native_id: "synthetic.thread." + i,
      account_provenance: { kind: "not_account_scoped" },
    };
    const binding = {
      provider,
      source_instance_id: source,
      authority_namespace: "synthetic.authority",
      store_generation: 2,
      state: "active",
    };
    const evidence = {
      provider,
      source_instance_id: source,
      authority_namespace: binding.authority_namespace,
      native_id: identity.native_id,
      store_generation: 2,
      evidence_sha256: "a".repeat(64),
    };
    const registration = {
      state: "attested",
      attestation_version: 1,
      attested_at: accepted,
      expires_at: "2026-10-25T12:05:00.000Z",
      evidence,
    };
    const reference = {
      ...JSON.parse(
        String(original.rows.find((r) => r.class === "workstream_native_references")!.row.payload),
      ),
      native_reference_id: refId,
      identity,
      pr_locator:
        provider === "github"
          ? {
              host: "github.com",
              repository_owner: "synthetic",
              repository_name: "project",
              number: 1,
            }
          : null,
      registration:
        kind === "verify_reference"
          ? {
              state: "quarantined",
              attestation_version: 0,
              attested_at: null,
              expires_at: null,
              evidence: null,
            }
          : registration,
    };
    const refSource = original.rows.find((r) => r.class === "workstream_native_references")!;
    const refRow: Record<string, unknown> = {
      ...refSource.row,
      native_reference_id: refId,
      attestation_version: reference.registration.attestation_version,
      registration_state: reference.registration.state,
      payload: JSON.stringify(reference),
    };
    for (const k of Object.keys(refRow).filter((k) => k.startsWith("routing_"))) refRow[k] = null;
    if (kind === "native_t3_settlement")
      Object.assign(refRow, {
        routing_provider: provider,
        routing_source_instance_id: source,
        routing_resource_kind: identity.resource_kind,
        routing_id_kind: "internal",
        routing_native_id: identity.native_id,
        routing_account_kind: "not_account_scoped",
        routing_authority_namespace: binding.authority_namespace,
        routing_store_generation: 2,
        routing_attested_at: accepted,
        routing_expires_at: registration.expires_at,
        routing_evidence_sha256: evidence.evidence_sha256,
      });
    rows.push({ class: "workstream_native_references", key: [owner, refId], row: refRow });
    if (!rows.some((x) => x.class === "workstream_source_bindings" && x.row.provider === provider))
      rows.push({
        class: "workstream_source_bindings",
        key: [owner, provider, source, 1],
        row: { owner_id: owner, ...binding, binding_version: 1 },
      });
    let membership = null;
    if (kind === "refresh_linked_pr") {
      const work = original.rows.find((r) => r.class === "workstreams")!;
      if (!rows.some((r) => r.class === "workstreams")) rows.push(work);
      membership = {
        membership_id: "membership:pr",
        workstream_id: "workstream:0",
        native_reference_id: refId,
        kind: "secondary",
        opened: {
          at: accepted,
          actor: { principal_id: principal },
          command_id: "command:terminal",
          registry_version: 7,
        },
        closed: null,
      };
      rows.push({
        class: "workstream_memberships",
        key: [owner, membership.membership_id],
        row: {
          owner_id: owner,
          membership_id: membership.membership_id,
          workstream_id: membership.workstream_id,
          native_reference_id: refId,
          kind: "secondary",
          closed_at: null,
          value: membership,
        },
      });
    }
    const action =
      kind === "refresh_linked_pr"
        ? {
            operation: kind,
            workstream_id: "workstream:0",
            membership_id: "membership:pr",
            expected_version: 1,
            expected_observation_version: 0,
          }
        : kind === "native_t3_settlement"
          ? {
              operation: "request_native_t3_settlement",
              native_reference_id: refId,
              expected_attestation_version: 1,
              native_action: "settle",
            }
          : { operation: kind, native_reference_id: refId, expected_attestation_version: 0 };
    const r = parseReservation({
      kind,
      command: {
        command_id: id,
        expected_server_generation: 7,
        expected_registry_version: 7,
        action,
      },
      owner_id: owner,
      actor: { principal_id: principal },
      authorization: {
        owner_id: owner,
        actor: { principal_id: principal },
        grant,
        server_generation: 7,
      },
      request_sha256: "a".repeat(64),
      accepted_at: accepted,
      admitted_registry_version: 7,
      reference,
      source_binding: binding,
      membership,
      previous_observation: null,
    });
    const result = parseResult(
      kind === "native_t3_settlement"
        ? nativeSettlementResult(r, outcome)
        : kind === "verify_reference"
          ? { registration }
          : {
              observation: {
                native_reference_id: refId,
                observation_version: 1,
                attempted_at: at,
                outcome: "observed",
                retry_after_seconds: null,
                last_success: {
                  state: "open",
                  draft: false,
                  observed_at: at,
                  provider_updated_at: null,
                },
                command_id: id,
              },
            },
    );
    const receipt = {
      state: "pending",
      command_id: id,
      owner_id: owner,
      actor: r.actor,
      operation: action.operation,
      request_sha256: r.request_sha256,
      server_generation: 7,
      accepted_at: accepted,
      retry_after_seconds: 1,
    };
    const stored = original.rows.find((x) => x.class === "workstream_command_receipts")!.row;
    rows.push({
      class: "workstream_command_receipts",
      key: [owner, id],
      row: { ...stored, command_id: id, state: "pending", payload: JSON.stringify(receipt) },
    });
    const reservationStored = original.rows.find(
      (x) => x.class === "workstream_external_reservations",
    )!.row;
    rows.push({
      class: "workstream_external_reservations",
      key: [owner, id],
      row: {
        ...reservationStored,
        command_id: id,
        native_reference_id: refId,
        effect: kind,
        state: "active",
        payload: JSON.stringify(r),
      },
    });
    if (retained)
      rows.push({
        class: "workstream_external_results",
        key: [owner, id],
        row: {
          ...original.rows.find((x) => x.class === "workstream_external_results")!.row,
          command_id: id,
          payload: JSON.stringify(result),
        },
      });
    reservations.push(r);
    results.push(result);
  }
  const text = encodeSnapshot(
    {
      ...original.header,
      owners: [{ owner_id: owner, server_generation: 7, registry_version: 7, audit_high_water: 0 }],
      open_effects: kinds.length,
    },
    rows,
  );
  return { text, reservations, results };
}
const setup = (fixture: ReturnType<typeof scenario>) =>
  Effect.gen(function* () {
    const sql = yield* SqlClient.SqlClient;
    yield* TestClock.setTime(Date.parse(at));
    yield* migration;
    yield* sql`INSERT INTO jones_workstreams_keys(key_id,registry_instance_id,scope,state,created_at) VALUES ('synthetic-transition-key',${registry},'home','active',${accepted})`;
    const header = parseSnapshot(fixture.text).header;
    const p = yield* plan({
      import_run_id: "70000000-0000-4000-8000-000000000001",
      encrypted: encryptSnapshot(fixture.text, "synthetic-transition-key", key),
      stream_key_id: "synthetic-transition-key",
      permit_key_id: "operator",
      permit_public_key: publicKey,
      permit: {
        key_id: "operator",
        header_sha256: digest(canonical(header)),
        signature: NodeCrypto.sign(
          null,
          Buffer.from(canonical(["workstreams-snapshot-export-permit-v1", header])),
          privateKey,
        ).toString("base64"),
      },
      home_environment_id: "synthetic.home",
      active_key_id: "synthetic-transition-key",
      mappings: fixture.reservations
        .map((r) => ({
          principal_id: principal,
          environment_id: "synthetic.environment",
          source_instance_id: r.reference.identity.source_instance_id,
          evidence_sha256: "a".repeat(64),
        }))
        .filter((_, i) => i === 0),
    });
    yield* apply(p);
    yield* readback(p);
    return p;
  });
const request = (r: LegacyReservation): LegacyReconciliationRequest => ({
  contract,
  command_id: r.command.command_id,
  request_sha256: r.request_sha256,
  reservation_sha256: digest(canonical(r)),
  expected_server_generation: 7,
  home_environment_id: "synthetic.home",
});
const sourcePort = (
  f: ReturnType<typeof scenario>,
  change?: (e: Record<string, unknown>) => void,
) => ({
  lookup: ({
    binding,
    reservation,
  }: {
    binding: import("../../../../../../../packages/contracts/src/jones/workstreamsLegacyReconciliation.ts").LegacyReconciliationBinding;
    reservation: LegacyReservation;
  }) => {
    const i = f.reservations.findIndex(
      (r) => r.command.command_id === reservation.command.command_id,
    );
    const action = reservation.command.action;
    const evidence = {
      binding,
      result: f.results[i]!,
      completed_at: at,
      settlement_request:
        action.operation === "request_native_t3_settlement"
          ? {
              owner_id: owner,
              principal_id: principal,
              command_id: reservation.command.command_id,
              native_reference_id: reservation.reference.native_reference_id,
              native_action: action.native_action,
              request_sha256: reservation.request_sha256,
              reservation_sha256: digest(canonical(reservation)),
              identity: reservation.reference.identity,
              expected_authority_namespace: reservation.source_binding.authority_namespace,
              expected_store_generation: reservation.source_binding.store_generation,
            }
          : null,
      native_receipt:
        reservation.kind === "native_t3_settlement"
          ? {
              commandId: "native:receipt",
              aggregateKind: "thread",
              aggregateId: reservation.reference.identity.native_id,
              status: f.results[i]!.native_outcome === "committed" ? "accepted" : "rejected",
              resultSequence: 10,
            }
          : null,
      settlement_event:
        reservation.kind === "native_t3_settlement" && f.results[i]!.native_outcome === "committed"
          ? {
              commandId: "native:receipt",
              aggregateKind: "thread",
              aggregateId: reservation.reference.identity.native_id,
              sequence: 10,
              type: "thread.settled",
            }
          : null,
    };
    change?.(evidence);
    return Effect.succeed(Schema.decodeUnknownSync(importSource)(evidence));
  },
});
import { LegacySourceTerminal as importSource } from "../../../../../../../packages/contracts/src/jones/workstreamsLegacyReconciliation.ts";

it.effect.each(["verify_reference", "native_t3_settlement", "refresh_linked_pr"] as const)(
  "reconciles %s with authenticated outcome, unknown bytes and unsupported original ID",
  (kind) =>
    Effect.gen(function* () {
      const f = scenario([kind]),
        p = yield* setup(f),
        r = f.reservations[0]!;
      expect(isFreshCommand(r.command)).toBe(false);
      const result = yield* reconcileLegacyOpen(p, owner, principal, request(r)).pipe(
        Effect.provideService(LegacySourceLookup, sourcePort(f)),
      );
      expect(result.result.kind).toBe("reconciled");
      const ledger = yield* readLegacyLedger(scope, p.import_run_id);
      expect(ledger.deltas).toHaveLength(1);
      expect(ledger.deltas[0]!.binding.reservation_sha256).toBe(digest(canonical(r)));
      const before = yield* readback(p);
      expect(before.reconciliation.count).toBe(1);
      expect((yield* readback(p)).reconciliation).toEqual(before.reconciliation);
      const archive = yield* Archive.ArchiveStore,
        receipts = yield* Receipts.ReceiptStore;
      expect((yield* archive.evidence(scope, r.command.command_id))!.state).toBe("pending");
      expect(
        (yield* receipts.lookupLegacy(scope, principal, {
          contract: WORKSTREAMS_LEGACY_RECEIPTS_CONTRACT,
          command_id: r.command.command_id,
          expected_server_generation: 7,
        })).result.kind,
      ).toBe("unknown");
      expect(
        Exit.isFailure(yield* Effect.exit(receipts.lookup(scope, r.command.command_id, 7))),
      ).toBe(true);
      const replay = yield* reconcileLegacyOpen(p, owner, principal, request(r));
      expect(replay).toEqual(result);
      expect((yield* readLegacyLedger(scope, p.import_run_id)).deltas).toHaveLength(1);
      const activation = {
        activation_id: "activation:legacy",
        plan_sha256: p.plan_sha256,
        home_environment_id: p.home_environment_id,
        generations: [{ owner_id: owner, server_generation: 7 }],
        reconciliation: {
          delta_root: before.reconciliation.delta_root,
          registry_versions: before.reconciliation.registry_versions,
        },
      };
      expect(
        Exit.isFailure(
          yield* Effect.exit(
            activate(p, {
              ...activation,
              reconciliation: { ...activation.reconciliation, delta_root: "b".repeat(64) },
            }),
          ),
        ),
      ).toBe(true);
      yield* activate(p, activation);
      expect((yield* activate(p, activation)).replayed).toBe(true);
      expect(yield* reconcileLegacyOpen(p, owner, principal, request(r))).toEqual(result);
      const sql = yield* SqlClient.SqlClient;
      yield* sql`UPDATE jones_workstreams_registries SET state='fenced'`;
      const input = {
        snapshot_id: "80000000-0000-4000-8000-000000000001",
        exported_at: "2026-10-25T13:00:00.000Z",
        revision: "f0731f1d6e6255b7bc2ca7118a458c42e5f56969",
        fence_epoch: 3,
        stream_key_id: "synthetic-transition-key",
        representable_principals: [principal],
      };
      const reverse = yield* prepareReverse(p, input);
      expect((yield* prepareReverse(p, input)).canonical_jsonl).toBe(reverse.canonical_jsonl);
      expect(
        reverse.snapshot.rows.filter((x) => x.class === "workstream_external_results"),
      ).toHaveLength(1);
      expect(
        reverse.snapshot.rows.find((x) => x.class === "workstream_command_receipts")!.row.state,
      ).toBe("committed");
      expect(
        reverse.snapshot.rows.find((x) => x.class === "workstream_external_reservations")!.row
          .state,
      ).toBe("released");
      expect(reverse.next_generations).toEqual([{ owner_id: owner, server_generation: 8 }]);
    }).pipe(Effect.provide(testLayer({ keys: keys() }))),
);

it.effect.each(["verify_reference", "native_t3_settlement", "refresh_linked_pr"] as const)(
  "uses only a genuine retained terminal result for %s",
  (kind) =>
    Effect.gen(function* () {
      const f = scenario([kind], true),
        p = yield* setup(f);
      const result = yield* reconcileLegacyOpen(p, owner, principal, request(f.reservations[0]!));
      expect(result.result.kind).toBe("reconciled");
      if (result.result.kind === "reconciled")
        expect(result.result.proof.provenance.kind).toBe("retained_terminal_result");
    }).pipe(Effect.provide(testLayer({ keys: keys() }))),
);

it.effect.each([
  "source_unconfigured",
  "attempt_not_found",
  "source_refused",
  "source_unavailable",
] as const)("keeps settlement unknown with zero dispatch when %s", (reason) =>
  Effect.gen(function* () {
    const f = scenario(["native_t3_settlement"]),
      p = yield* setup(f);
    let lookups = 0;
    const run = reconcileLegacyOpen(p, owner, principal, request(f.reservations[0]!));
    const result =
      reason === "source_unconfigured"
        ? yield* run
        : yield* run.pipe(
            Effect.provideService(LegacySourceLookup, {
              lookup: () => {
                lookups++;
                return Effect.succeed({ kind: "unknown" as const, reason });
              },
            }),
          );
    expect(result.result).toEqual({ kind: "unknown", reason });
    expect(lookups).toBe(reason === "source_unconfigured" ? 0 : 1);
    expect((yield* readLegacyLedger(scope, p.import_run_id)).deltas).toHaveLength(0);
    expect((yield* readback(p)).reconciliation.count).toBe(0);
  }).pipe(Effect.provide(testLayer({ keys: keys() }))),
);

it.effect(
  "settlement uses current CAS after another proven effect and never changes membership",
  () =>
    Effect.gen(function* () {
      const f = scenario(["verify_reference", "native_t3_settlement"]),
        p = yield* setup(f),
        sql = yield* SqlClient.SqlClient;
      const before = yield* sql`SELECT * FROM jones_workstreams_memberships`;
      for (const r of f.reservations)
        expect(
          (yield* reconcileLegacyOpen(p, owner, principal, request(r)).pipe(
            Effect.provideService(LegacySourceLookup, sourcePort(f)),
          )).result.kind,
        ).toBe("reconciled");
      const ledger = yield* readLegacyLedger(scope, p.import_run_id);
      expect(
        ledger.deltas.map((x) => [x.before_registry_version, x.after_registry_version]),
      ).toEqual([
        [7, 8],
        [8, 9],
      ]);
      expect(yield* sql`SELECT * FROM jones_workstreams_memberships`).toEqual(before);
    }).pipe(Effect.provide(testLayer({ keys: keys() }))),
);

it.effect("partial coverage cannot activate; stale verification is refused", () =>
  Effect.gen(function* () {
    const f = scenario(["native_t3_settlement", "verify_reference"]),
      p = yield* setup(f);
    yield* reconcileLegacyOpen(p, owner, principal, request(f.reservations[0]!)).pipe(
      Effect.provideService(LegacySourceLookup, sourcePort(f)),
    );
    expect(
      Exit.isFailure(
        yield* Effect.exit(
          reconcileLegacyOpen(p, owner, principal, request(f.reservations[1]!)).pipe(
            Effect.provideService(LegacySourceLookup, sourcePort(f)),
          ),
        ),
      ),
    ).toBe(true);
    const rb = yield* readback(p);
    expect(
      Exit.isFailure(
        yield* Effect.exit(
          activate(p, {
            activation_id: "activation:partial",
            plan_sha256: p.plan_sha256,
            home_environment_id: p.home_environment_id,
            generations: [{ owner_id: owner, server_generation: 7 }],
            reconciliation: {
              delta_root: rb.reconciliation.delta_root,
              registry_versions: rb.reconciliation.registry_versions,
            },
          }),
        ),
      ),
    ).toBe(true);
  }).pipe(Effect.provide(testLayer({ keys: keys() }))),
);

it.effect("a late SQL refusal rolls back result, receipt, history, context and ledger", () =>
  Effect.gen(function* () {
    const f = scenario(["native_t3_settlement"]),
      p = yield* setup(f),
      sql = yield* SqlClient.SqlClient;
    yield* sql.unsafe(
      "CREATE TRIGGER synthetic_rollback BEFORE INSERT ON jones_workstreams_legacy_archive WHEN NEW.class='" +
        LEGACY_RECONCILIATION_CLASS +
        "' BEGIN SELECT RAISE(ABORT,'synthetic rollback'); END",
    );
    expect(
      Exit.isFailure(
        yield* Effect.exit(
          reconcileLegacyOpen(p, owner, principal, request(f.reservations[0]!)).pipe(
            Effect.provideService(LegacySourceLookup, sourcePort(f)),
          ),
        ),
      ),
    ).toBe(true);
    expect((yield* readback(p)).reconciliation.count).toBe(0);
    expect(yield* sql`SELECT state FROM jones_workstreams_receipts`).toEqual([
      { state: "pending" },
    ]);
    expect(yield* sql`SELECT command_id FROM jones_workstreams_external_results`).toEqual([]);
    expect(yield* sql`SELECT event_id FROM jones_workstreams_history`).toEqual([]);
  }).pipe(Effect.provide(testLayer({ keys: keys() }))),
);

it.effect.each([
  "namespace",
  "source_generation",
  "principal",
  "event",
  "request",
  "witness",
  "home",
] as const)("refuses %s mismatches without terminal writes", (which) =>
  Effect.gen(function* () {
    const f = scenario(["native_t3_settlement"]),
      p = yield* setup(f),
      r = f.reservations[0]!;
    const q = {
      ...request(r),
      ...(which === "witness" ? { original_byte_witness: "fabricated" } : {}),
      ...(which === "home" ? { home_environment_id: "other.home" } : {}),
    };
    const port = sourcePort(f, (e) => {
      const b = e.binding as Record<string, unknown>;
      if (which === "namespace") b.authority_namespace = "other";
      if (which === "source_generation") b.store_generation = 9;
      if (which === "principal") b.principal_id = "other.principal";
      if (which === "request") b.request_sha256 = "b".repeat(64);
      if (which === "event") (e.settlement_event as Record<string, unknown>).commandId = "other";
    });
    expect(
      Exit.isFailure(
        yield* Effect.exit(
          reconcileLegacyOpen(p, owner, principal, q).pipe(
            Effect.provideService(LegacySourceLookup, port),
          ),
        ),
      ),
    ).toBe(true);
    expect((yield* readback(p)).reconciliation.count).toBe(0);
  }).pipe(Effect.provide(testLayer({ keys: keys() }))),
);

it.effect.each(["denied", "unsupported", "failed"] as const)(
  "records proven settlement %s once without a registry bump",
  (outcome) =>
    Effect.gen(function* () {
      const f = scenario(["native_t3_settlement"], false, outcome),
        p = yield* setup(f),
        sql = yield* SqlClient.SqlClient;
      const first = yield* reconcileLegacyOpen(
        p,
        owner,
        principal,
        request(f.reservations[0]!),
      ).pipe(Effect.provideService(LegacySourceLookup, sourcePort(f)));
      expect(first.result.kind).toBe("reconciled");
      if (first.result.kind === "reconciled") {
        expect(first.result.proof.receipt.changed).toBe(false);
        expect(first.result.proof.after_registry_version).toBe(7);
      }
      expect(yield* reconcileLegacyOpen(p, owner, principal, request(f.reservations[0]!))).toEqual(
        first,
      );
      expect(
        (yield* sql<{ n: number }>`SELECT count(*) AS n FROM jones_workstreams_history`)[0]!.n,
      ).toBe(1);
    }).pipe(Effect.provide(testLayer({ keys: keys() }))),
);

it.effect(
  "refuses unexplained changes after reconciliation rather than updating baseline digests",
  () =>
    Effect.gen(function* () {
      const f = scenario(["native_t3_settlement"]),
        p = yield* setup(f),
        sql = yield* SqlClient.SqlClient;
      yield* reconcileLegacyOpen(p, owner, principal, request(f.reservations[0]!)).pipe(
        Effect.provideService(LegacySourceLookup, sourcePort(f)),
      );
      const pins = p.snapshot.classes;
      yield* sql`UPDATE jones_workstreams_registries SET registry_version=registry_version+1`;
      expect(Exit.isFailure(yield* Effect.exit(readback(p)))).toBe(true);
      expect(p.snapshot.classes).toBe(pins);
    }).pipe(Effect.provide(testLayer({ keys: keys() }))),
);

it.effect("rejects stale PR fences after a proven unrelated settlement", () =>
  Effect.gen(function* () {
    const f = scenario(["native_t3_settlement", "refresh_linked_pr"]),
      p = yield* setup(f);
    yield* reconcileLegacyOpen(p, owner, principal, request(f.reservations[0]!)).pipe(
      Effect.provideService(LegacySourceLookup, sourcePort(f)),
    );
    expect(
      Exit.isFailure(
        yield* Effect.exit(
          reconcileLegacyOpen(p, owner, principal, request(f.reservations[1]!)).pipe(
            Effect.provideService(LegacySourceLookup, sourcePort(f)),
          ),
        ),
      ),
    ).toBe(true);
    expect((yield* readLegacyLedger(scope, p.import_run_id)).deltas).toHaveLength(1);
  }).pipe(Effect.provide(testLayer({ keys: keys() }))),
);

it.effect("binds duplicates to the original principal and exact reconciliation request", () =>
  Effect.gen(function* () {
    const f = scenario(["native_t3_settlement"]),
      p = yield* setup(f),
      q = request(f.reservations[0]!);
    yield* reconcileLegacyOpen(p, owner, principal, q).pipe(
      Effect.provideService(LegacySourceLookup, sourcePort(f)),
    );
    expect(
      Exit.isFailure(yield* Effect.exit(reconcileLegacyOpen(p, owner, "other.principal", q))),
    ).toBe(true);
    expect(
      Exit.isFailure(
        yield* Effect.exit(
          reconcileLegacyOpen(p, owner, principal, { ...q, reservation_sha256: "b".repeat(64) }),
        ),
      ),
    ).toBe(true);
    expect((yield* readLegacyLedger(scope, p.import_run_id)).deltas).toHaveLength(1);
  }).pipe(Effect.provide(testLayer({ keys: keys() }))),
);

it.effect("refuses a source result whose association hash conflicts with its outcome", () =>
  Effect.gen(function* () {
    const f = scenario(["native_t3_settlement"]),
      p = yield* setup(f);
    const port = sourcePort(
      f,
      (e) =>
        ((
          e.result as { native_evidence: { result_sha256: string } }
        ).native_evidence.result_sha256 = "b".repeat(64)),
    );
    expect(
      Exit.isFailure(
        yield* Effect.exit(
          reconcileLegacyOpen(p, owner, principal, request(f.reservations[0]!)).pipe(
            Effect.provideService(LegacySourceLookup, port),
          ),
        ),
      ),
    ).toBe(true);
    expect((yield* readback(p)).reconciliation.count).toBe(0);
  }).pipe(Effect.provide(testLayer({ keys: keys() }))),
);

it.effect.each(["verify_reference", "refresh_linked_pr"] as const)(
  "keeps %s fenced on absent, refused, unavailable or conflicting source evidence",
  (kind) =>
    Effect.gen(function* () {
      const f = scenario([kind]),
        p = yield* setup(f);
      expect(
        (yield* reconcileLegacyOpen(p, owner, principal, request(f.reservations[0]!))).result,
      ).toEqual({ kind: "unknown", reason: "source_unconfigured" });
      for (const reason of ["source_refused", "source_unavailable"]) {
        const reply = yield* reconcileLegacyOpen(
          p,
          owner,
          principal,
          request(f.reservations[0]!),
        ).pipe(
          Effect.provideService(LegacySourceLookup, {
            lookup: () => Effect.succeed({ kind: "unknown" as const, reason }),
          }),
        );
        expect(reply.result).toEqual({ kind: "unknown", reason });
      }
      const conflict = yield* Effect.exit(
        reconcileLegacyOpen(p, owner, principal, request(f.reservations[0]!)).pipe(
          Effect.provideService(
            LegacySourceLookup,
            sourcePort(f, (e) => {
              (e.binding as Record<string, unknown>).authority_namespace = "conflicting.authority";
            }),
          ),
        ),
      );
      expect(Exit.isFailure(conflict)).toBe(true);
      expect((yield* readback(p)).reconciliation.count).toBe(0);
    }).pipe(Effect.provide(testLayer({ keys: keys() }))),
);

it.effect.each(["verify_reference", "refresh_linked_pr"] as const)(
  "records a genuine negative %s observation without manufacturing success",
  (kind) =>
    Effect.gen(function* () {
      const f = scenario([kind]),
        p = yield* setup(f);
      if (kind === "verify_reference")
        f.results[0] = {
          registration: {
            state: "verification-failed",
            attestation_version: 1,
            attested_at: null,
            expires_at: null,
            evidence: null,
          },
        };
      else
        f.results[0] = {
          observation: {
            native_reference_id: "reference:0",
            observation_version: 1,
            attempted_at: at,
            outcome: "inaccessible",
            retry_after_seconds: null,
            last_success: null,
            command_id: "command:0",
          },
        };
      const result = yield* reconcileLegacyOpen(
        p,
        owner,
        principal,
        request(f.reservations[0]!),
      ).pipe(Effect.provideService(LegacySourceLookup, sourcePort(f)));
      expect(result.result.kind).toBe("reconciled");
      if (result.result.kind === "reconciled")
        expect(result.result.proof.receipt.effects.native_settlement).toBe(null);
    }).pipe(Effect.provide(testLayer({ keys: keys() }))),
);

it.effect.each(["generation", "key", "current_receipt"] as const)(
  "preserves the import fence after %s drift",
  (which) =>
    Effect.gen(function* () {
      const f = scenario(["native_t3_settlement"]),
        p = yield* setup(f),
        sql = yield* SqlClient.SqlClient,
        q = request(f.reservations[0]!);
      const attempt = which === "generation" ? { ...q, expected_server_generation: 8 } : q;
      if (which === "key")
        yield* sql`UPDATE jones_workstreams_registries SET active_key_id='other-key',registry_version=registry_version+1`;
      if (which === "current_receipt")
        yield* sql`UPDATE jones_workstreams_receipts SET state='unresolved'`;
      expect(
        Exit.isFailure(
          yield* Effect.exit(
            reconcileLegacyOpen(p, owner, principal, attempt).pipe(
              Effect.provideService(LegacySourceLookup, sourcePort(f)),
            ),
          ),
        ),
      ).toBe(true);
      expect((yield* readLegacyLedger(scope, p.import_run_id)).deltas).toHaveLength(0);
    }).pipe(Effect.provide(testLayer({ keys: keys() }))),
);

it.effect.each(["not_modified", "rate_limited"] as const)(
  "retains the admitted legacy PR cache for a proven %s result",
  (outcome) =>
    Effect.gen(function* () {
      const f = scenario(["refresh_linked_pr"]),
        r = f.reservations[0]!;
      const previous = {
        native_reference_id: r.reference.native_reference_id,
        observation_version: 1,
        attempted_at: accepted,
        outcome: "observed",
        retry_after_seconds: null,
        last_success: {
          state: "open",
          draft: false,
          observed_at: accepted,
          provider_updated_at: null,
        },
        command_id: "command:previous",
        observation_cache: {
          schema_version: 1,
          owner_id: owner,
          native_reference_id: r.reference.native_reference_id,
          source_instance_id: r.reference.identity.source_instance_id,
          provider: r.reference.identity.provider,
          locator: r.reference.pr_locator,
          server_generation: 7,
          authority_namespace: r.source_binding.authority_namespace,
          store_generation: r.source_binding.store_generation,
          observation_version: 1,
          etag: '"synthetic-cache"',
          representation: {
            state: "open",
            draft: false,
            observed_at: accepted,
            provider_updated_at: null,
          },
        },
      };
      const reservation = parseReservation({
        ...r,
        command: { ...r.command, action: { ...r.command.action, expected_observation_version: 1 } },
        previous_observation: previous,
      });
      f.reservations[0] = reservation;
      const parsed = parseSnapshot(f.text);
      const rows = parsed.rows.map((x) =>
        x.class === "workstream_external_reservations"
          ? { ...x, row: { ...x.row, payload: JSON.stringify(reservation) } }
          : x,
      );
      rows.push({
        class: "workstream_pr_observations",
        key: [owner, r.reference.native_reference_id, 1],
        row: {
          owner_id: owner,
          native_reference_id: r.reference.native_reference_id,
          observation_version: 1,
          attempted_at: accepted,
          value: previous,
        },
      });
      f.text = encodeSnapshot(parsed.header, rows);
      f.results[0] = yield* decodeResult({
        observation: {
          native_reference_id: previous.native_reference_id,
          observation_version: 2,
          attempted_at: at,
          outcome,
          retry_after_seconds: outcome === "rate_limited" ? 30 : null,
          command_id: r.command.command_id,
          last_success: {
            ...previous.last_success,
            observed_at: outcome === "not_modified" ? at : accepted,
          },
        },
      });
      const p = yield* setup(f);
      const result = yield* reconcileLegacyOpen(p, owner, principal, request(reservation)).pipe(
        Effect.provideService(LegacySourceLookup, sourcePort(f)),
      );
      expect(result.result.kind).toBe("reconciled");
      const registryStore = yield* Registry.RegistryStore;
      const stored = (yield* registryStore.list(scope, "pr_observations")).find(
        (x) => x.metadata.observation_version === 2,
      )!.value;
      const observation = yield* decodeStoredObservation(stored);
      expect(observation.observation_cache?.etag).toBe(previous.observation_cache.etag);
      expect((yield* readback(p)).reconciliation.count).toBe(1);
    }).pipe(Effect.provide(testLayer({ keys: keys() }))),
);

it.effect(
  "refuses even a digest-matching original-byte witness while its verifier is unconfigured",
  () =>
    Effect.gen(function* () {
      const f = scenario(["native_t3_settlement"]),
        witness = '{ "synthetic": true }';
      const sha = NodeCrypto.createHash("sha256").update(witness).digest("hex");
      const r = parseReservation({ ...f.reservations[0]!, request_sha256: sha });
      f.reservations[0] = r;
      const parsed = parseSnapshot(f.text);
      f.text = encodeSnapshot(
        parsed.header,
        parsed.rows.map((x) => {
          if (x.class === "workstream_command_receipts")
            return {
              ...x,
              row: {
                ...x.row,
                payload: JSON.stringify({
                  ...JSON.parse(String(x.row.payload)),
                  request_sha256: sha,
                }),
              },
            };
          if (x.class === "workstream_external_reservations")
            return { ...x, row: { ...x.row, payload: JSON.stringify(r) } };
          return x;
        }),
      );
      const p = yield* setup(f);
      const result = yield* Effect.exit(
        reconcileLegacyOpen(p, owner, principal, { ...request(r), original_byte_witness: witness }),
      );
      expect(Exit.isFailure(result)).toBe(true);
      expect(JSON.stringify(result)).toContain("original_witness_unconfigured");
      expect((yield* readback(p)).reconciliation.count).toBe(0);
    }).pipe(Effect.provide(testLayer({ keys: keys() }))),
);
