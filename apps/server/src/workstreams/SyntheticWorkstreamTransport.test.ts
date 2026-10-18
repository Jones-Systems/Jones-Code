// @effect-diagnostics-next-line nodeBuiltinImport:off -- Synchronous contract hashing mirrors the gateway's inventory digest.
import * as NodeCrypto from "node:crypto";

import { expect, it } from "@effect/vitest";
import {
  T3PlacementPage,
  WORKSTREAMS_T3_PROVIDER_ATTESTATION_TTL_MS,
  WorkstreamMembershipPage,
  WorkstreamReceipt,
  WorkstreamReferencePage,
  t3PlacementInventoryJson,
  type T3PlacementIdentity,
} from "@t3tools/contracts";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";

import {
  WORKSTREAM_CONTRACT_HEADER_VERSION,
  WORKSTREAM_CONTRACT_MANIFEST_SHA256,
  type WorkstreamCommand,
  type WorkstreamGatewayError,
  make,
} from "./WorkstreamGateway.ts";
import {
  makeSyntheticWorkstreamTransport,
  type SyntheticT3Assignment,
} from "./SyntheticWorkstreamTransport.ts";

const decodeReceipt = Schema.decodeUnknownEffect(WorkstreamReceipt);
const decodeReferencePage = Schema.decodeUnknownEffect(WorkstreamReferencePage);
const decodeMembershipPage = Schema.decodeUnknownEffect(WorkstreamMembershipPage);
const decodePlacementPage = Schema.decodeUnknownEffect(T3PlacementPage);
const start = Date.parse("2026-10-17T12:00:00Z");
const iso = (ms: number) => DateTime.formatIso(DateTime.makeUnsafe(ms));
const binding = {
  registryId: "https://workstream-registry.invalid",
  ownerId: "owner-fixture",
  principalId: "principal-fixture",
  authorizationRevision: 1,
} as const;
const contract = {
  contractVersion: WORKSTREAM_CONTRACT_HEADER_VERSION,
  contractManifest: WORKSTREAM_CONTRACT_MANIFEST_SHA256,
} as const;
const trust = {
  readTrustSnapshot: () => ({
    trustedEnvironments: [
      {
        environmentId: "environment-synthetic",
        authorityNamespace: "synthetic-authority",
        storeGeneration: 1,
      },
    ],
    readiness: "ready" as const,
  }),
};
const thread = (id: string): T3PlacementIdentity => ({
  source_instance_id: "environment-synthetic",
  native_thread_id: id,
});
const assignment = (
  id: string,
  workstreamId: string | null,
  extra: Partial<SyntheticT3Assignment> = {},
): SyntheticT3Assignment => ({
  sourceInstanceId: "environment-synthetic",
  nativeThreadId: id,
  workstreamId,
  ...extra,
});
const t3Identity = (id: string) => ({
  provider: "t3",
  source_instance_id: "environment-synthetic",
  resource_kind: "thread",
  id_kind: "internal" as const,
  native_id: id,
  account_provenance: { kind: "not_account_scoped" as const },
});
const inventoryDigest = (identities: readonly T3PlacementIdentity[]) =>
  NodeCrypto.createHash("sha256").update(t3PlacementInventoryJson(identities)).digest("hex");

const setup = (assignments: readonly SyntheticT3Assignment[] = []) =>
  Effect.gen(function* () {
    const clock = { now: start };
    const transport = makeSyntheticWorkstreamTransport({ now: () => clock.now, assignments });
    const gateway = yield* make(transport, {
      binding,
      now: () => clock.now,
      placementTrustProvider: trust,
    });
    let sequence = 0;
    const send = (action: WorkstreamCommand["action"]) =>
      Effect.gen(function* () {
        const session = yield* gateway.readSession();
        const receipt = yield* gateway.submit({
          command_id: `command-synthetic-${String(++sequence).padStart(4, "0")}`,
          expected_server_generation: session.serverGeneration,
          expected_registry_version: session.registryVersion,
          action,
        });
        yield* decodeReceipt(receipt);
        return receipt;
      });
    const committed = (action: WorkstreamCommand["action"]) =>
      send(action).pipe(
        Effect.map((receipt) => {
          if (receipt.state !== "committed")
            throw new Error(`Expected commit, got ${receipt.state}`);
          return receipt;
        }),
      );
    const rejection = (action: WorkstreamCommand["action"]) =>
      send(action).pipe(
        Effect.map((receipt) =>
          receipt.state === "rejected" ? receipt.error.code : receipt.state,
        ),
      );
    const allPages = <
      A extends { readonly items: readonly unknown[]; readonly next_cursor: string | null },
    >(
      read: (cursor?: string) => Effect.Effect<A, WorkstreamGatewayError>,
    ) =>
      Effect.gen(function* () {
        const pages: A[] = [];
        let cursor: string | undefined;
        do {
          const next = yield* read(cursor);
          pages.push(next);
          cursor = next.next_cursor ?? undefined;
        } while (cursor !== undefined);
        return pages;
      });
    return { clock, transport, gateway, send, committed, rejection, allPages };
  });

it.effect("pages references and memberships at one read context with version-bound cursors", () =>
  Effect.gen(function* () {
    const seeds = Array.from({ length: 250 }, (_, index) =>
      assignment(`thread-${index}`, index % 2 === 0 ? "ws-core-v1" : "ws-follow-on"),
    );
    const { gateway, committed, allPages } = yield* setup(seeds);
    const session = yield* gateway.readSession();
    expect(session.registryVersion).toBe(11);

    const referencePages = yield* allPages((cursor) =>
      gateway.readReferences({ limit: 100, ...(cursor === undefined ? {} : { cursor }) }),
    );
    expect(referencePages.map((value) => value.items.length)).toEqual([100, 100, 51]);
    for (const value of referencePages) {
      yield* decodeReferencePage(value);
      expect(value.context).toEqual({
        owner_id: "owner-fixture",
        server_generation: 7,
        registry_version: 11,
      });
    }
    const referenceIds = referencePages.flatMap((value) =>
      value.items.map((item) => item.native_reference_id),
    );
    expect(new Set(referenceIds).size).toBe(251);
    expect(referenceIds).toEqual(referenceIds.toSorted());
    const t3References = referencePages
      .flatMap((value) => value.items)
      .filter((item) => item.identity.provider === "t3");
    expect(t3References).toHaveLength(250);
    expect(
      t3References.every(
        (item) =>
          item.pr_locator === null &&
          item.identity.resource_kind === "thread" &&
          item.identity.id_kind === "internal" &&
          item.identity.account_provenance.kind === "not_account_scoped" &&
          item.registration.state === "attested" &&
          item.registration.attestation_version >= 1 &&
          Date.parse(item.registration.expires_at!) - Date.parse(item.registration.attested_at!) ===
            WORKSTREAMS_T3_PROVIDER_ATTESTATION_TTL_MS,
      ),
    ).toBe(true);

    const membershipPages = yield* allPages((cursor) =>
      gateway.readMemberships("ws-core-v1", {
        limit: 100,
        ...(cursor === undefined ? {} : { cursor }),
      }),
    );
    expect(membershipPages.map((value) => value.items.length)).toEqual([100, 25]);
    for (const value of membershipPages) {
      yield* decodeMembershipPage(value);
      expect(value.context.registry_version).toBe(11);
    }
    const episodes = membershipPages.flatMap((value) => value.items);
    expect(
      episodes.every(
        (item) =>
          item.workstream_id === "ws-core-v1" &&
          item.kind === "primary" &&
          item.closed === null &&
          referenceIds.includes(item.native_reference_id),
      ),
    ).toBe(true);

    const firstPage = referencePages[0]!;
    yield* committed({
      operation: "register_reference",
      identity: t3Identity("thread-new"),
      pr_locator: null,
    });
    const stale = yield* gateway
      .readReferences({ limit: 100, cursor: firstPage.next_cursor! })
      .pipe(Effect.flip);
    expect(stale.reason).toBe("cursor-stale");
    expect((yield* gateway.readReferences({ limit: 100 })).context.registry_version).toBe(12);
  }),
);

it.effect("serves current proofs only for requested identities with open memberships", () =>
  Effect.gen(function* () {
    const { clock, transport, gateway, allPages } = yield* setup([
      assignment("thread-a", "ws-core-v1"),
      assignment("thread-b", "ws-follow-on", { attestedAt: start - 299_000 }),
      assignment("thread-c", null),
      assignment("thread-d", "ws-core-v1"),
      assignment("thread-d", "ws-follow-on", { kind: "secondary" }),
      assignment("thread-e", "ws-core-v1", { attestedAt: null }),
      assignment("thread-unrequested", "ws-core-v1"),
    ]);
    const identities = ["thread-e", "thread-d", "thread-c", "thread-b", "thread-a", "thread-x"].map(
      thread,
    );
    const references = (yield* allPages((cursor) =>
      gateway.readReferences({ limit: 100, ...(cursor === undefined ? {} : { cursor }) }),
    )).flatMap((value) => value.items);
    const memberships = [
      ...(yield* gateway.readMemberships("ws-core-v1", { limit: 100 })).items,
      ...(yield* gateway.readMemberships("ws-follow-on", { limit: 100 })).items,
    ];
    const pending = references.find((item) => item.identity.native_id === "thread-e")!;
    expect(pending.registration.state).toBe("verification-pending");
    expect(
      memberships.some((item) => item.native_reference_id === pending.native_reference_id),
    ).toBe(true);

    const raw = yield* transport.listThreadPlacements!({ identities, limit: 100 });
    yield* decodePlacementPage(raw);
    expect(raw.inventory_sha256).toBe(inventoryDigest(identities));
    expect(raw.context).toEqual({
      owner_id: "owner-fixture",
      principal_id: "principal-fixture",
      authorization_revision: 1,
      server_generation: 7,
      registry_version: 11,
    });

    const loaded = yield* gateway.readThreadPlacements({ identities });
    expect(loaded.readiness).toBe("ready");
    expect(loaded.page.inventory_sha256).toBe(inventoryDigest(identities));
    expect(loaded.page.items.map((item) => [item.native_thread_id, item.kind])).toEqual([
      ["thread-a", "primary"],
      ["thread-b", "primary"],
      ["thread-d", "primary"],
      ["thread-d", "secondary"],
    ]);
    for (const item of loaded.page.items) {
      const episode = memberships.find((value) => value.membership_id === item.membership_id)!;
      const reference = references.find(
        (value) => value.native_reference_id === item.native_reference_id,
      )!;
      expect(episode.native_reference_id).toBe(item.native_reference_id);
      expect(episode.workstream_id).toBe(item.workstream_id);
      expect(episode.kind).toBe(item.kind);
      expect(reference.identity.native_id).toBe(item.native_thread_id);
      expect(reference.registration.expires_at).toBe(item.expires_at);
      expect(reference.registration.evidence?.authority_namespace).toBe(item.authority_namespace);
      expect(item.authority_namespace).toBe("synthetic-authority");
    }

    // thread-b's attestation reaches its fixed lifetime and stops proving placement.
    clock.now = start + 1_000;
    const later = yield* transport.listThreadPlacements!({ identities, limit: 100 });
    expect(later.items.map((item) => item.native_thread_id)).toEqual([
      "thread-a",
      "thread-d",
      "thread-d",
    ]);
    const reloaded = yield* gateway.readThreadPlacements({ identities });
    expect(reloaded.page.items.map((item) => item.native_thread_id)).toEqual([
      "thread-a",
      "thread-d",
      "thread-d",
    ]);
  }),
);

it.effect("pages a 1,000-identity proof batch through gateway order and identity checks", () =>
  Effect.gen(function* () {
    const seeds = Array.from({ length: 450 }, (_, index) =>
      assignment(`thread-${index}`, "ws-core-v1"),
    );
    const { transport, gateway } = yield* setup(seeds);
    const identities = Array.from({ length: 1_000 }, (_, index) => thread(`thread-${index}`));
    const first = yield* transport.listThreadPlacements!({ identities, limit: 100 });
    expect(first.items).toHaveLength(100);
    expect(first.next_cursor).not.toBeNull();
    const loaded = yield* gateway.readThreadPlacements({ identities });
    expect(loaded.page.items).toHaveLength(450);
    expect(new Set(loaded.page.items.map((item) => item.membership_id)).size).toBe(450);
  }),
);

it.effect("register, verify, and attach update index-visible state and the registry version", () =>
  Effect.gen(function* () {
    const { clock, gateway, committed } = yield* setup();
    const registered = yield* committed({
      operation: "register_reference",
      identity: t3Identity("thread-assign"),
      pr_locator: null,
    });
    expect(registered.registry_version).toBe(12);
    expect(registered.changed).toBe(true);
    const referenceId = registered.effects.native_reference_id!;
    const pending = yield* gateway.readReference(referenceId);
    expect(pending.context.registry_version).toBe(12);
    expect(pending.reference.registration).toMatchObject({
      state: "verification-pending",
      attestation_version: 0,
    });
    expect(pending.reference.created_registry_version).toBe(12);
    expect(
      (yield* gateway.readReferences({ limit: 100 })).items.map((item) => item.native_reference_id),
    ).toContain(referenceId);

    clock.now = start + 5_000;
    const verified = yield* committed({
      operation: "verify_reference",
      native_reference_id: referenceId,
      expected_attestation_version: 0,
    });
    expect(verified.registry_version).toBe(13);
    expect(verified.effects.registration).toMatchObject({
      state: "attested",
      attestation_version: 1,
      attested_at: iso(start + 5_000),
      expires_at: iso(start + 5_000 + WORKSTREAMS_T3_PROVIDER_ATTESTATION_TTL_MS),
    });
    expect((yield* gateway.readReference(referenceId)).reference.registration.state).toBe(
      "attested",
    );

    const attached = yield* committed({
      operation: "attach_primary",
      workstream_id: "ws-core-v1",
      expected_version: 3,
      native_reference_id: referenceId,
    });
    expect(attached.registry_version).toBe(14);
    expect(attached.effects.workstream_versions).toEqual([
      { workstream_id: "ws-core-v1", version: 4 },
    ]);
    const membershipId = attached.effects.membership_ids[0]!;
    const memberships = yield* gateway.readMemberships("ws-core-v1", { limit: 100 });
    expect(memberships.context.registry_version).toBe(14);
    expect(memberships.items).toEqual([
      expect.objectContaining({
        membership_id: membershipId,
        native_reference_id: referenceId,
        kind: "primary",
        opened: expect.objectContaining({ registry_version: 14 }),
        closed: null,
      }),
    ]);
    expect((yield* gateway.readDetail("ws-core-v1")).workstream.version).toBe(4);
    const proof = yield* gateway.readThreadPlacements({ identities: [thread("thread-assign")] });
    expect(proof.page.context.registry_version).toBe(14);
    expect(proof.page.items).toEqual([
      expect.objectContaining({
        membership_id: membershipId,
        native_reference_id: referenceId,
        workstream_id: "ws-core-v1",
        kind: "primary",
      }),
    ]);
    expect((yield* gateway.readSession()).registryVersion).toBe(14);
  }),
);

it.effect("applies each command ID at most once and rejects stale or conflicting bytes", () =>
  Effect.gen(function* () {
    const transport = makeSyntheticWorkstreamTransport({ now: () => start });
    const registryVersion = () =>
      transport
        .getCapabilities(contract)
        .pipe(Effect.map((value) => value.context.registry_version));
    const referenceCount = () =>
      transport
        .listReferences({ ...contract, limit: 100 })
        .pipe(Effect.map((value) => value.items.length));
    const command: WorkstreamCommand = {
      command_id: "command-idempotent-0001",
      expected_server_generation: 7,
      expected_registry_version: 11,
      action: {
        operation: "register_reference",
        identity: t3Identity("thread-once"),
        pr_locator: null,
      },
    };
    const submit = (value: WorkstreamCommand, body = JSON.stringify(value)) =>
      transport.submitCommand({
        ...contract,
        body,
        command: value,
        idempotencyKey: value.command_id,
      });

    const first = yield* submit(command);
    expect(first.state).toBe("committed");
    expect(yield* registryVersion()).toBe(12);
    expect(yield* referenceCount()).toBe(2);

    const replay = yield* submit(command);
    expect(replay).toEqual(first);
    expect(yield* transport.getCommand({ ...contract, commandId: command.command_id })).toEqual(
      first,
    );
    expect(yield* registryVersion()).toBe(12);
    expect(yield* referenceCount()).toBe(2);

    const conflict = yield* submit(command, `${JSON.stringify(command)} `);
    expect(conflict.state === "rejected" && conflict.error.code).toBe("idempotency_conflict");
    expect(yield* transport.getCommand({ ...contract, commandId: command.command_id })).toEqual(
      first,
    );

    const stale = yield* submit({ ...command, command_id: "command-idempotent-0002" });
    expect(stale.state === "rejected" && stale.error.code).toBe("version_conflict");
    expect(yield* submit({ ...command, command_id: "command-idempotent-0002" })).toEqual(stale);
    expect(yield* registryVersion()).toBe(12);

    const duplicate = yield* submit({
      ...command,
      command_id: "command-idempotent-0003",
      expected_registry_version: 12,
    });
    expect(duplicate.state).toBe("committed");
    if (duplicate.state !== "committed" || first.state !== "committed") return;
    expect(duplicate.changed).toBe(false);
    expect(duplicate.registry_version).toBe(12);
    expect(duplicate.effects.native_reference_id).toBe(first.effects.native_reference_id);
    expect(yield* referenceCount()).toBe(2);
  }),
);

it.effect("requires a current attestation and one open primary for membership changes", () =>
  Effect.gen(function* () {
    const { clock, gateway, committed, rejection } = yield* setup();
    const referenceId = (yield* committed({
      operation: "register_reference",
      identity: t3Identity("thread-move"),
      pr_locator: null,
    })).effects.native_reference_id!;
    const attach = (workstreamId: string, expectedVersion: number) => ({
      operation: "attach_primary" as const,
      workstream_id: workstreamId,
      expected_version: expectedVersion,
      native_reference_id: referenceId,
    });
    expect(yield* rejection(attach("ws-core-v1", 3))).toBe("registration_unverified");

    yield* committed({
      operation: "verify_reference",
      native_reference_id: referenceId,
      expected_attestation_version: 0,
    });
    clock.now = start + WORKSTREAMS_T3_PROVIDER_ATTESTATION_TTL_MS;
    expect(yield* rejection(attach("ws-core-v1", 3))).toBe("registration_unverified");
    expect(
      yield* rejection({
        operation: "verify_reference",
        native_reference_id: referenceId,
        expected_attestation_version: 0,
      }),
    ).toBe("version_conflict");
    yield* committed({
      operation: "verify_reference",
      native_reference_id: referenceId,
      expected_attestation_version: 1,
    });
    expect(yield* rejection(attach("ws-core-v1", 2))).toBe("version_conflict");
    const membershipId = (yield* committed(attach("ws-core-v1", 3))).effects.membership_ids[0]!;
    expect(yield* rejection(attach("ws-follow-on", 1))).toBe("membership_conflict");

    const moved = yield* committed({
      operation: "move_primary",
      source_workstream_id: "ws-core-v1",
      expected_source_version: 4,
      source_membership_id: membershipId,
      destination_workstream_id: "ws-follow-on",
      expected_destination_version: 1,
    });
    const movedId = moved.effects.membership_ids[1]!;
    expect(moved.effects.membership_ids[0]).toBe(membershipId);
    const source = yield* gateway.readMemberships("ws-core-v1", { limit: 100 });
    expect(source.items[0]?.closed).toMatchObject({
      reason: "continued-elsewhere",
      registry_version: moved.registry_version,
    });
    const proof = yield* gateway.readThreadPlacements({ identities: [thread("thread-move")] });
    expect(proof.page.items.map((item) => [item.membership_id, item.workstream_id])).toEqual([
      [movedId, "ws-follow-on"],
    ]);

    yield* committed({
      operation: "remove_membership",
      workstream_id: "ws-follow-on",
      expected_version: 2,
      membership_id: movedId,
    });
    expect(
      (yield* gateway.readThreadPlacements({ identities: [thread("thread-move")] })).page.items,
    ).toEqual([]);
    expect(
      (yield* gateway.readMemberships("ws-follow-on", { limit: 100 })).items[0]?.closed,
    ).toMatchObject({ reason: "removed" });
  }),
);
