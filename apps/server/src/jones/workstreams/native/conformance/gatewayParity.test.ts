import * as Deferred from "effect/Deferred";
import * as Fiber from "effect/Fiber";
// @effect-diagnostics nodeBuiltinImport:off -- Read only frozen catalogue bytes in this checkout.
import * as NodeFS from "node:fs";
import * as NodeCrypto from "node:crypto";
import { caseCoverage, semanticCatalogue } from "./caseCoverage.ts";
import { expect, it } from "@effect/vitest";
import { vi } from "vite-plus/test";

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

import * as SqlClient from "effect/sql/SqlClient";
import {
  Effect,
  Wire,
  Store,
  TestClock,
  fresh,
  seed,
  app,
  harness,
  scope,
  registry,
  revokeGrant,
  canonicalJson,
  GitHub,
  withGitHub,
  harnessFor,
} from "./testkit.ts";

const run = <A, E, R>(body: Effect.Effect<A, E, R>) => {
  const f = fresh();
  return Effect.gen(function* () {
    const prepared = yield* seed(f);
    return yield* body.pipe(app, Effect.provide(prepared.services));
  }).pipe(Effect.scoped, Effect.provide(f.layer));
};
const rejected = (receipt: Wire.WorkstreamReceipt, code: string) => {
  expect(receipt.state).toBe("rejected");
  if (receipt.state === "rejected") expect(receipt.error.code).toBe(code);
};

it.effect("ingress metadata lifecycle progress ordering appearance and independent identity", () =>
  run(
    Effect.gen(function* () {
      const h = yield* harness,
        a = yield* h.create("Same synthetic name"),
        b = yield* h.create("Same synthetic name", 10);
      expect(a).not.toBe(b);
      const before = yield* h.gateway.readDetail(a);
      expect(before.workstream.progress).toEqual({ state: "unknown" });
      const progress: Wire.WorkstreamProgress[] = [
        { state: "progressing" },
        { state: "waiting", condition: "Synthetic dependency" },
        { state: "blocked", impediment: "Synthetic blocker" },
        { state: "stale" },
        { state: "unknown" },
        { state: "blocked", impediment: "Synthetic blocker" },
      ];
      for (const [index, lifecycle] of (
        ["completed", "active", "paused", "planned", "deferred", "abandoned"] as const
      ).entries()) {
        const detail = yield* h.gateway.readDetail(a);
        yield* h.committed({
          operation: "update_workstream",
          workstream_id: a,
          expected_version: detail.workstream.version,
          name: "Renamed synthetic",
          lifecycle,
          progress: progress[index]!,
          sort_order: 0,
        });
      }
      const detail = yield* h.gateway.readDetail(a);
      expect(detail.workstream.delivery).toBe(before.workstream.delivery);
      expect(detail.workstream.freshness).toBe(before.workstream.freshness);
      expect(detail.workstream).toMatchObject({
        name: "Renamed synthetic",
        lifecycle: "abandoned",
        sort_order: 0,
        progress: { state: "blocked", impediment: "Synthetic blocker" },
      });
      const lifecycle = yield* (yield* Store.RegistryStore).listPlain(
        scope,
        "lifecycle_declarations",
      );
      expect(lifecycle.filter((x) => x.workstream_id === a)).toHaveLength(7);
      expect(
        lifecycle.map((row) => JSON.parse(String(row.value))).find((row) => row.revision === 3),
      ).toMatchObject({ new_lifecycle: "active", revision: 3 });
      expect((yield* h.gateway.readMetadata()).items.map((x) => x.workstreamId)).toEqual([a, b]);
      const appearance = {
        command_id: "appearance-ingress-00000001",
        workstream_id: a,
        expected_server_generation: 1,
        expected_version: 0,
        border_color: "#2255AA",
      };
      const saved = yield* h.gateway.saveAppearance(appearance);
      expect(saved).toMatchObject({ workstream_id: a, version: 1, border_color: "#2255AA" });
      expect(yield* h.gateway.saveAppearance(appearance)).toEqual(saved);
      expect(
        (yield* h.gateway
          .saveAppearance({ ...appearance, command_id: "appearance-ingress-00000002" })
          .pipe(Effect.result))._tag,
      ).toBe("Failure");
      expect(yield* h.gateway.readAppearance([a])).toMatchObject({
        supported: true,
        page: { items: [saved] },
      });
      const counts = yield* h.gateway.readRegistryCounts();
      expect(counts).toMatchObject({
        counts: { total: 2, active: 1, unknown_lifecycle: 0, authority_effect: "none" },
        binding: { ownerId: registry.owner_id, serverGeneration: 1 },
      });
    }),
  ),
);

it.effect(
  "ingress primary secondary move rollback reattach and removal never settle source",
  () => {
    const f = fresh();
    return Effect.gen(function* () {
      const prepared = yield* seed(f);
      yield* Effect.gen(function* () {
        const h = yield* harness,
          a = yield* h.create("a"),
          b = yield* h.create("b"),
          c = yield* h.create("c"),
          ref = yield* h.register();
        yield* h.verify(ref);
        const p = (yield* h.attach(a, ref)).effects.membership_ids[0]!;
        const s = (yield* h.attach(c, ref, true)).effects.membership_ids[0]!;
        rejected(
          yield* h.submit({
            operation: "attach_primary",
            workstream_id: b,
            expected_version: 1,
            native_reference_id: ref,
          }),
          "membership_conflict",
        );
        yield* h.committed({
          operation: "update_workstream",
          workstream_id: b,
          expected_version: 1,
          name: "b updated",
          lifecycle: "active",
          progress: { state: "unknown" },
          sort_order: 0,
        });
        const snapshot = yield* h.gateway.readMemberships(a, { limit: 100 });
        rejected(
          yield* h.submit({
            operation: "move_primary",
            source_workstream_id: a,
            expected_source_version: yield* h.version(a),
            source_membership_id: p,
            destination_workstream_id: b,
            expected_destination_version: 1,
          }),
          "version_conflict",
        );
        expect((yield* h.gateway.readMemberships(a, { limit: 100 })).items).toEqual(snapshot.items);
        expect((yield* h.gateway.readMemberships(b, { limit: 100 })).items).toHaveLength(0);
        expect(yield* h.version(a)).toBe(2);
        expect(yield* h.version(b)).toBe(2);
        const moved = yield* h.committed({
          operation: "move_primary",
          source_workstream_id: a,
          expected_source_version: 2,
          source_membership_id: p,
          destination_workstream_id: b,
          expected_destination_version: 2,
        });
        expect(moved.effects.workstream_versions).toEqual(
          expect.arrayContaining([
            { workstream_id: a, version: 3 },
            { workstream_id: b, version: 3 },
          ]),
        );
        expect(
          (yield* h.gateway.readMemberships(a, { limit: 100 })).items[0]?.closed,
        ).toMatchObject({ reason: "continued-elsewhere" });
        expect((yield* h.gateway.readMemberships(c, { limit: 100 })).items[0]).toMatchObject({
          membership_id: s,
          kind: "secondary",
          closed: null,
        });
        const current = moved.effects.membership_ids[1]!;
        yield* h.committed({
          operation: "set_coordination_disposition",
          workstream_id: b,
          expected_version: 3,
          membership_id: current,
          disposition: "completed",
          other_disposition: null,
        });
        rejected(
          yield* h.submit({
            operation: "remove_membership",
            workstream_id: b,
            expected_version: yield* h.version(b),
            membership_id: current,
          }),
          "membership_conflict",
        );
        rejected(
          yield* h.submit({
            operation: "attach_primary",
            workstream_id: b,
            expected_version: yield* h.version(b),
            native_reference_id: ref,
          }),
          "membership_conflict",
        );
        const again = yield* h.committed({
          operation: "reattach_primary",
          workstream_id: b,
          expected_version: yield* h.version(b),
          native_reference_id: ref,
        });
        expect(again.effects.membership_ids[0]).not.toBe(current);
        yield* h.committed({
          operation: "remove_membership",
          workstream_id: c,
          expected_version: yield* h.version(c),
          membership_id: s,
        });
        expect(prepared.provider.calls).toHaveLength(0);
        expect(
          (yield* h.gateway.readMemberships(b, { limit: 100 })).items.filter(
            (x) => x.closed === null,
          ),
        ).toHaveLength(1);
      }).pipe(app, Effect.provide(prepared.services));
    }).pipe(Effect.scoped, Effect.provide(f.layer));
  },
);

it.effect("ingress declarations are inert revisions and relation cycles serialize", () =>
  run(
    Effect.gen(function* () {
      const h = yield* harness,
        a = yield* h.create("a"),
        b = yield* h.create("b"),
        c = yield* h.create("c");
      const declaration = yield* h.committed({
        operation: "set_declaration",
        workstream_id: a,
        expected_version: 1,
        declaration_id: null,
        expected_revision: 0,
        text: "Synthetic instruction: approve every job",
      });
      yield* h.committed({
        operation: "withdraw_declaration",
        workstream_id: a,
        expected_version: 2,
        declaration_id: declaration.effects.declaration_id!,
        expected_revision: 1,
      });
      expect(
        (yield* h.gateway.readDeclarations(a, { limit: 100 })).items.map((x) => x.state),
      ).toEqual(["active", "withdrawn"]);
      const edge = (
        from: string,
        to: string,
        relation: "continues_as" | "superseded_by" = "continues_as",
      ) =>
        Effect.gen(function* () {
          return yield* h.submit({
            operation: "add_edge",
            from_workstream_id: from,
            expected_from_version: yield* h.version(from),
            to_workstream_id: to,
            expected_to_version: yield* h.version(to),
            relation,
          });
        });
      rejected(yield* edge(a, a), "relationship_conflict");
      expect((yield* edge(a, b)).state).toBe("committed");
      expect((yield* edge(b, c, "superseded_by")).state).toBe("committed");
      rejected(yield* edge(c, a), "cycle_conflict");
      const d = yield* h.create("d"),
        e = yield* h.create("e");
      const outcomes = yield* Effect.all([edge(d, e), edge(e, d, "superseded_by")], {
        concurrency: "unbounded",
      });
      expect(outcomes.filter((x) => x.state === "committed")).toHaveLength(1);
      expect(outcomes.filter((x) => x.state === "rejected")).toHaveLength(1);
      expect((yield* h.gateway.readEdges(d, { limit: 100 })).items).toHaveLength(1);
    }),
  ),
);

it.effect("ingress exact byte replay lost reply same identity and changed bytes conflict", () =>
  run(
    Effect.gen(function* () {
      const h = yield* harness;
      const command = yield* h.command({
        operation: "create_workstream",
        name: "lost reply",
        lifecycle: "active",
        progress: { state: "unknown" },
        sort_order: 0,
      });
      const body = canonicalJson(command),
        original = yield* h.gateway.resubmit(command.command_id, body);
      const count = (yield* h.gateway.readMetadata()).items.length;
      expect(yield* h.gateway.readReceipt(command.command_id)).toMatchObject({ kind: "receipt" });
      expect(yield* h.gateway.resubmit(command.command_id, body)).toEqual(original);
      expect(yield* h.gateway.resubmit(command.command_id, body + " ")).toEqual({
        kind: "idempotency_conflict",
      });
      expect((yield* h.gateway.readMetadata()).items).toHaveLength(count);
      yield* revokeGrant();
      expect((yield* h.gateway.resubmit(command.command_id, body).pipe(Effect.result))._tag).toBe(
        "Failure",
      );
    }),
  ),
);

it.effect("ingress quarantined reference refuses membership without provider work", () =>
  run(
    Effect.gen(function* () {
      const h = yield* harness,
        a = yield* h.create(),
        ref = yield* h.register();
      expect((yield* h.gateway.readReference(ref)).reference.registration.state).toBe(
        "quarantined",
      );
      for (const operation of ["attach_primary", "link_secondary"] as const)
        rejected(
          yield* h.submit({
            operation,
            workstream_id: a,
            expected_version: 1,
            native_reference_id: ref,
          }),
          "registration_unverified",
        );
      expect((yield* h.gateway.readMemberships(a, { limit: 100 })).items).toHaveLength(0);
      yield* h.verify(ref);
      expect((yield* h.gateway.readReference(ref)).reference.registration).toMatchObject({
        state: "attested",
        attestation_version: 1,
        evidence: { native_id: "thread-synthetic" },
      });
      yield* TestClock.adjust(300_001);
      rejected(
        yield* h.submit({
          operation: "attach_primary",
          workstream_id: a,
          expected_version: 1,
          native_reference_id: ref,
        }),
        "registration_unverified",
      );
    }),
  ),
);

it.effect("ingress history complete pages 1 7 100 share versions high water and cursor scope", () =>
  run(
    Effect.gen(function* () {
      const h = yield* harness,
        id = yield* h.create();
      yield* h.committed({
        operation: "update_workstream",
        workstream_id: id,
        expected_version: 1,
        name: "version two",
        lifecycle: "active",
        progress: { state: "unknown" },
        sort_order: 0,
      });
      for (let n = 0; n < 12; n++)
        rejected(
          yield* h.submit({
            operation: "update_workstream",
            workstream_id: id,
            expected_version: 1,
            name: "stale",
            lifecycle: "active",
            progress: { state: "unknown" },
            sort_order: 0,
          }),
          "version_conflict",
        );
      const readAll = (limit: number) =>
        Effect.gen(function* () {
          const items: (typeof Wire.WorkstreamAuditEvent.Type)[] = [];
          let cursor: string | undefined;
          do {
            const page = yield* h.gateway.readHistory(undefined, {
              limit,
              ...(cursor ? { cursor } : {}),
            });
            expect(page.context.registry_version).toBe(
              (yield* h.gateway.readDetail(id)).context.registry_version,
            );
            items.push(...page.items);
            cursor = page.next_cursor ?? undefined;
          } while (cursor);
          return items;
        });
      const all = yield* readAll(100);
      expect(all).toHaveLength(14);
      expect(yield* readAll(1)).toEqual(all);
      expect(yield* readAll(7)).toEqual(all);
      expect(
        new Set(
          all.filter((x) => x.operation === "update_workstream").map((x) => x.registry_version),
        ).size,
      ).toBe(1);
      const first = yield* h.gateway.readHistory(undefined, { limit: 7 });
      expect(first.next_cursor).not.toBeNull();
      rejected(
        yield* h.submit({
          operation: "update_workstream",
          workstream_id: id,
          expected_version: 1,
          name: "later rejected",
          lifecycle: "active",
          progress: { state: "unknown" },
          sort_order: 0,
        }),
        "version_conflict",
      );
      const tail = yield* h.gateway.readHistory(undefined, {
        limit: 7,
        cursor: first.next_cursor!,
      });
      expect([...first.items, ...tail.items]).toEqual(all);
      expect(tail.next_cursor).toBeNull();
      const other = yield* h.create("other");
      expect(
        (yield* h.gateway
          .readHistory(other, { limit: 7, cursor: first.next_cursor! })
          .pipe(Effect.result))._tag,
      ).toBe("Failure");
      expect(
        (yield* h.gateway
          .readMetadata({ limit: 7, cursor: first.next_cursor! })
          .pipe(Effect.result))._tag,
      ).toBe("Failure");
      const stale = yield* h.gateway.readHistory(undefined, { limit: 1 });
      yield* h.committed({
        operation: "update_workstream",
        workstream_id: id,
        expected_version: 2,
        name: "updated",
        lifecycle: "active",
        progress: { state: "unknown" },
        sort_order: 1,
      });
      expect(
        (yield* h.gateway
          .readHistory(undefined, { limit: 1, cursor: stale.next_cursor! })
          .pipe(Effect.flip)).reason,
      ).toBe("cursor-stale");
      const sql = yield* SqlClient.SqlClient;
      expect((yield* sql`SELECT count(*) AS n FROM jones_workstreams_history`)[0]?.n).toBe(17);
    }),
  ),
);

it.effect(
  "ingress current owner grant read and write ceilings refuse without registry or provider changes",
  () =>
    run(
      Effect.gen(function* () {
        const h = yield* harness,
          store = yield* Store.RegistryStore;
        const command = yield* h.command({
          operation: "create_workstream",
          name: "must not exist",
          lifecycle: "active",
          progress: { state: "unknown" },
          sort_order: 0,
        });
        const before = yield* store.context(scope);
        yield* revokeGrant(["workstreams:read"], "active");
        expect((yield* h.gateway.submit(command).pipe(Effect.result))._tag).toBe("Failure");
        expect((yield* h.gateway.readMetadata()).items).toHaveLength(0);
        const sql = yield* SqlClient.SqlClient;
        expect(
          (yield* sql.unsafe<{ n: number }>(
            "SELECT count(*) AS n FROM jones_workstreams_receipts",
          ))[0]?.n,
        ).toBe(0);
        yield* revokeGrant([], "active");
        expect((yield* h.gateway.readMetadata().pipe(Effect.result))._tag).toBe("Failure");
        expect((yield* store.context(scope))?.registry_version).toBeGreaterThanOrEqual(
          before!.registry_version,
        );
      }),
    ),
);

it.effect(
  "ingress account scoped IDs remain distinct and PR locator mismatch refuses before verification",
  () =>
    run(
      Effect.gen(function* () {
        const h = yield* harness;
        const register = (account: string, native = "jones/example#7") =>
          h.submit({
            operation: "register_reference",
            identity: {
              provider: "github",
              source_instance_id: "github-synthetic",
              resource_kind: "pull_request",
              id_kind: "external",
              native_id: native,
              account_provenance: {
                kind: "account",
                account_id: account,
                account_namespace: "namespace-" + account,
              },
            },
            pr_locator: {
              host: "github.com",
              repository_owner: "jones",
              repository_name: "example",
              number: 7,
            },
          });
        const a = yield* register("one"),
          b = yield* register("two");
        expect(a.state).toBe("committed");
        expect(b.state).toBe("committed");
        if (a.state === "committed" && b.state === "committed")
          expect(a.effects.native_reference_id).not.toBe(b.effects.native_reference_id);
        rejected(yield* register("one", "jones/example#8"), "identity_conflict");
        expect((yield* h.gateway.readReferences({ limit: 100 })).items).toHaveLength(2);
      }),
    ),
);

it("accounts for each exact frozen semantic name once without converting exclusions or gaps into passes", () => {
  const raw = NodeFS.readFileSync(
    new URL(
      "../../../../../../../packages/contracts/src/workstreams-fixtures/semantic-cases.json.fixture",
      import.meta.url,
    ),
    "utf8",
  );
  expect(NodeCrypto.createHash("sha256").update(raw).digest("hex")).toBe(semanticCatalogue.sha256);
  const corpus = JSON.parse(raw) as { cases: { name: string }[] };
  expect(caseCoverage.map((row) => ({ ordinal: row.ordinal, id: row.id }))).toEqual(
    corpus.cases.map((row, ordinal) => ({ ordinal, id: row.name })),
  );
  expect(new Set(caseCoverage.map((row) => row.id)).size).toBe(corpus.cases.length);
  expect(caseCoverage.filter((row) => row.status === "excluded").map((row) => row.ordinal)).toEqual(
    [35, 42, 43, 44, 45],
  );
  expect(caseCoverage.find((row) => row.ordinal === 31)).toMatchObject({
    status: "existing-proof",
    file: "../crypto/envelope.test.ts",
  });
  for (const row of caseCoverage) {
    if (row.status === "missing" || row.status === "excluded") {
      expect(row.file).toBeNull();
      expect(row.test).toBeNull();
      expect(row.detail.length).toBeGreaterThan(20);
    } else {
      expect(row.file).not.toBeNull();
      expect(row.test).not.toBeNull();
      expect(NodeFS.readFileSync(new URL(row.file!, import.meta.url), "utf8")).toContain(row.test!);
    }
  }
});

it.effect(
  "ingress real GitHub observer preserves inaccessible snapshots bounds rates refuses redirects and never infers completion",
  () =>
    run(
      Effect.gen(function* () {
        const requests: GitHub.GitHubPrHttpRequest[] = [];
        const statuses = [200, 304, 200, 404, 429, 302, 200, 200];
        yield* withGitHub(
          {
            get: (input) =>
              Effect.sync(() => {
                requests.push(input);
                const status = statuses.shift();
                if (status === undefined) throw new Error("Unexpected extra GitHub request");
                const merged = requests.length >= 7;
                return {
                  status,
                  headers:
                    status === 429
                      ? { "retry-after": "999999" }
                      : status === 302
                        ? { location: "https://example.invalid/private" }
                        : { etag: '"synthetic-etag"' },
                  body: new TextEncoder().encode(
                    JSON.stringify({
                      number: 7,
                      state: merged ? "closed" : "open",
                      draft: false,
                      merged,
                      merged_at: merged ? "2026-01-01T00:00:00.000Z" : null,
                      updated_at:
                        requests.length === 8 ? "2026-02-30T00:00:00Z" : "2026-01-01T00:00:00.000Z",
                      base: { repo: { full_name: "jones/example" } },
                    }),
                  ),
                };
              }),
          },
          (gateway) =>
            Effect.gen(function* () {
              const h = yield* harnessFor(gateway),
                a = yield* h.create("Explicit PR organization");
              const ref = (yield* h.committed({
                operation: "register_reference",
                identity: {
                  provider: "github",
                  source_instance_id: "github-synthetic",
                  resource_kind: "pull_request",
                  id_kind: "external",
                  native_id: "jones/example#7",
                  account_provenance: { kind: "not_account_scoped" },
                },
                pr_locator: {
                  host: "github.com",
                  repository_owner: "jones",
                  repository_name: "example",
                  number: 7,
                },
              })).effects.native_reference_id!;
              yield* h.verify(ref);
              const verified = (yield* gateway.readReference(ref)).reference.registration;
              expect(verified).toMatchObject({ state: "attested", attestation_version: 1 });
              const closed = (yield* h.attach(a, ref)).effects.membership_ids[0]!;
              yield* h.committed({
                operation: "remove_membership",
                workstream_id: a,
                expected_version: yield* h.version(a),
                membership_id: closed,
              });
              const unlinked = yield* h.submit({
                operation: "refresh_linked_pr",
                workstream_id: a,
                expected_version: yield* h.version(a),
                membership_id: closed,
                expected_observation_version: 0,
              });
              rejected(unlinked, "membership_conflict");
              expect(requests).toHaveLength(1);
              const membership = (yield* h.committed({
                operation: "reattach_primary",
                workstream_id: a,
                expected_version: yield* h.version(a),
                native_reference_id: ref,
              })).effects.membership_ids[0]!;
              let observationVersion = 0;
              const refresh = () =>
                Effect.gen(function* () {
                  const receipt = yield* h.committed({
                    operation: "refresh_linked_pr",
                    workstream_id: a,
                    expected_version: yield* h.version(a),
                    membership_id: membership,
                    expected_observation_version: observationVersion++,
                  });
                  return receipt.effects.observation!;
                });
              expect(yield* refresh()).toMatchObject({
                outcome: "invalid_response",
                last_success: null,
              });
              const observed = yield* refresh();
              expect(observed).toMatchObject({
                outcome: "observed",
                last_success: { state: "open" },
              });
              expect(yield* refresh()).toMatchObject({
                outcome: "inaccessible",
                last_success: observed.last_success,
              });
              expect(yield* refresh()).toMatchObject({
                outcome: "rate_limited",
                retry_after_seconds: 3600,
                last_success: observed.last_success,
              });
              expect(yield* refresh()).toMatchObject({
                outcome: "invalid_response",
                last_success: observed.last_success,
              });
              expect(yield* refresh()).toMatchObject({
                outcome: "observed",
                last_success: { state: "merged" },
              });
              expect((yield* gateway.readDetail(a)).workstream.lifecycle).toBe("active");
              expect(
                (yield* gateway.readMemberships(a, { limit: 100 })).items.find(
                  (item) => item.membership_id === membership,
                ),
              ).toMatchObject({ membership_id: membership, closed: null });
              expect(yield* refresh()).toMatchObject({
                outcome: "invalid_response",
                last_success: { state: "merged" },
              });
              expect(requests).toHaveLength(8);
              expect(requests.every((request) => request.redirect === "error")).toBe(true);
            }),
        );
      }),
    ),
);

it.effect(
  "ingress membership removal during real PR refresh fences completion without an observation",
  () =>
    run(
      Effect.gen(function* () {
        const started = yield* Deferred.make<void>(),
          release = yield* Deferred.make<void>();
        let requests = 0;
        yield* withGitHub(
          {
            get: (input) =>
              Effect.gen(function* () {
                requests++;
                expect(input.redirect).toBe("error");
                if (requests === 2) {
                  yield* Deferred.succeed(started, undefined);
                  yield* Deferred.await(release);
                }
                return {
                  status: 200,
                  headers: {},
                  body: new TextEncoder().encode(
                    JSON.stringify({
                      number: 7,
                      state: "open",
                      draft: false,
                      merged: false,
                      merged_at: null,
                      updated_at: "2026-01-01T00:00:00.000Z",
                      base: { repo: { full_name: "jones/example" } },
                    }),
                  ),
                };
              }),
          },
          (gateway) =>
            Effect.gen(function* () {
              const h = yield* harnessFor(gateway),
                a = yield* h.create(),
                ref = (yield* h.committed({
                  operation: "register_reference",
                  identity: {
                    provider: "github",
                    source_instance_id: "github-synthetic",
                    resource_kind: "pull_request",
                    id_kind: "external",
                    native_id: "jones/example#7",
                    account_provenance: { kind: "not_account_scoped" },
                  },
                  pr_locator: {
                    host: "github.com",
                    repository_owner: "jones",
                    repository_name: "example",
                    number: 7,
                  },
                })).effects.native_reference_id!;
              yield* h.verify(ref);
              const membership = (yield* h.attach(a, ref)).effects.membership_ids[0]!;
              const pending = yield* h
                .submit({
                  operation: "refresh_linked_pr",
                  workstream_id: a,
                  expected_version: 2,
                  membership_id: membership,
                  expected_observation_version: 0,
                })
                .pipe(Effect.forkScoped);
              yield* Deferred.await(started);
              yield* h.committed({
                operation: "remove_membership",
                workstream_id: a,
                expected_version: 2,
                membership_id: membership,
              });
              yield* Deferred.succeed(release, undefined);
              const completion = yield* Fiber.join(pending).pipe(Effect.result);
              expect(completion._tag).toBe("Failure");
              if (completion._tag === "Failure")
                expect(completion.failure).toMatchObject({ detail: "version_conflict" });
              expect((yield* gateway.readReference(ref)).latest_observation).toBeNull();
              expect(
                (yield* gateway.readMemberships(a, { limit: 100 })).items[0]?.closed?.reason,
              ).toBe("removed");
              expect(requests).toBe(2);
            }),
        );
      }),
    ),
);
