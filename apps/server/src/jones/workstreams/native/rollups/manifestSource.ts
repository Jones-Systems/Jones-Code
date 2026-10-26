import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Schema from "effect/Schema";
import * as SqlClient from "effect/sql/SqlClient";
import { canonicalWorkstreamsSnapshotJson as recordKey } from "@t3tools/contracts/jones/workstreamsSnapshot";
import * as Wire from "@t3tools/contracts";
import { NativeHome } from "../mode/authorityMode.ts";
import * as RegistryStore from "../store/registryStore.ts";
import * as NativeSql from "../store/sql.ts";
import * as Bindings from "./bindings.ts";
import * as Evidence from "./evidence.ts";
import * as Generation from "./generation.ts";

const decodeMembershipEpisode = Schema.decodeUnknownSync(Wire.MembershipEpisode);

export interface Discovery {
  readonly manifests: readonly Evidence.Manifest[];
  readonly inspected: readonly {
    readonly workstream_id: string;
    readonly state: "ready" | "provider_unconfigured" | "content_unavailable";
    readonly freshness: Evidence.Freshness;
    readonly exclusions: readonly Evidence.Exclusion[];
  }[];
}
export const DiscoveryOptions = Context.Reference<{ readonly max_input_bytes: number }>(
  "t3/jones/workstreams/native/rollups/DiscoveryOptions",
  { defaultValue: () => ({ max_input_bytes: 8_388_608 }) },
);
export class ManifestSource extends Context.Service<
  ManifestSource,
  {
    readonly discoverDirtyManifests: (
      limit: number,
      now: string,
    ) => Effect.Effect<Discovery, Evidence.RollupError>;
  }
>()("t3/jones/workstreams/native/rollups/manifestSource") {}
const make = Effect.gen(function* () {
  const home = yield* NativeHome,
    registry = yield* RegistryStore.RegistryStore,
    db = yield* NativeSql.NativeSql,
    sql = yield* SqlClient.SqlClient,
    bindings = yield* Bindings.Bindings,
    provider = yield* Evidence.RollupEvidenceProvider,
    generation = yield* Generation.RollupGenerationProvider,
    options = yield* DiscoveryOptions;
  const scope = { registry_instance_id: home.registry_instance_id, owner_id: home.owner_id };
  const discoverDirtyManifests = Effect.fn("RollupManifestSource.discover")(
    (limit: number, now: string) =>
      Effect.gen(function* () {
        yield* Evidence.attempt(
          () =>
            Evidence.requireRollup(
              Number.isSafeInteger(limit) &&
                limit >= 1 &&
                limit <= 100 &&
                provider.contract === Evidence.EVIDENCE_CONTRACT &&
                Number.isSafeInteger(options.max_input_bytes) &&
                options.max_input_bytes >= 1 &&
                options.max_input_bytes <= 268_435_456,
              "invalid_request",
            ),
          "invalid_request",
        );
        const configured = (yield* bindings.list()).filter((b) => b.state === "active");
        const ids = [...new Set(configured.map((b) => b.workstream_id))].sort();
        const cursorId = Evidence.digest(
          Evidence.canonicalJson([
            scope.registry_instance_id,
            scope.owner_id,
            provider.id,
            configured
              .map((b) => [b.workstream_id, b.native_reference_id, b.binding_version])
              .sort(),
          ]),
        );
        const selected = yield* db.transaction(
          scope,
          Effect.gen(function* () {
            if (!ids.length) return [];
            const cursor = (yield* sql<{
              last_group_key: string | null;
            }>`SELECT last_group_key FROM jones_workstreams_rollup_discovery_cursors WHERE cursor_id=${cursorId}`)[0];
            let index = ids.findIndex((id) => id > (cursor?.last_group_key ?? ""));
            if (index < 0) index = 0;
            const chosen = Array.from(
              { length: Math.min(limit, ids.length) },
              (_, n) => ids[(index + n) % ids.length]!,
            );
            if (cursor)
              yield* sql`UPDATE jones_workstreams_rollup_discovery_cursors SET last_group_key=${chosen.at(-1)!},updated_at=${now} WHERE cursor_id=${cursorId}`;
            else
              yield* sql`INSERT INTO jones_workstreams_rollup_discovery_cursors(cursor_id,last_group_key,updated_at) VALUES(${cursorId},${chosen.at(-1)!},${now})`;
            return chosen;
          }),
        );
        const manifests: Evidence.Manifest[] = [],
          inspected: Discovery["inspected"][number][] = [];
        for (const id of selected) {
          const context = yield* registry.context(scope);
          if (
            !context ||
            context.state !== "active" ||
            context.home_environment_id !== home.home_environment_id ||
            context.server_generation !== home.server_generation ||
            home.home_environment_id !== home.served_by_environment_id
          )
            return yield* new Evidence.RollupError({ code: "content_unavailable" });
          const record = (yield* sql<{
            version: number;
          }>`SELECT version FROM jones_workstreams_records WHERE registry_instance_id=${scope.registry_instance_id}
        AND owner_id=${scope.owner_id} AND workstream_id=${id}`)[0];
          if (!record) continue;
          const rows = yield* sql<{
            membership_id: string;
            native_reference_id: string;
            kind: string;
            opened_registry_version: number;
            envelope: string;
          }>`
        SELECT membership_id,native_reference_id,kind,opened_registry_version,envelope FROM jones_workstreams_memberships WHERE registry_instance_id=${scope.registry_instance_id}
        AND owner_id=${scope.owner_id} AND workstream_id=${id} AND closed_registry_version IS NULL ORDER BY membership_id COLLATE BINARY LIMIT 1001`;
          yield* Evidence.attempt(() => Evidence.requireRollup(rows.length <= 1000));
          const memberships: Wire.MembershipEpisode[] = [];
          for (const row of rows) {
            const raw = yield* db.openJson(
              {
                ...scope,
                purpose: "membership",
                record_id: recordKey([row.membership_id]),
                revision: row.opened_registry_version,
              },
              row.envelope,
            );
            const value = yield* Evidence.attempt(() => decodeMembershipEpisode(raw));
            yield* Evidence.attempt(() =>
              Evidence.requireRollup(
                value.workstream_id === id &&
                  value.membership_id === row.membership_id &&
                  value.native_reference_id === row.native_reference_id &&
                  value.closed === null &&
                  value.kind === row.kind &&
                  value.opened.registry_version === row.opened_registry_version,
              ),
            );
            memberships.push(value);
          }
          memberships.sort((a, b) =>
            a.membership_id < b.membership_id ? -1 : a.membership_id > b.membership_id ? 1 : 0,
          );
          const group = configured.filter((b) => b.workstream_id === id);
          const eligible = group.filter(
            (b) =>
              b.provider_id === provider.id &&
              memberships.some((m) => m.native_reference_id === b.native_reference_id),
          );
          const described = yield* provider.describe(eligible);
          yield* Evidence.attempt(() =>
            Evidence.requireRollup(
              described.length === eligible.length &&
                new Set(described.map((d) => d.native_reference_id)).size === described.length &&
                described.every((d) =>
                  eligible.some((b) => b.native_reference_id === d.native_reference_id),
                ),
            ),
          );
          const references = yield* sql<{
            native_reference_id: string;
            registration_state: string;
          }>`SELECT r.native_reference_id,r.registration_state FROM jones_workstreams_references r
        JOIN jones_workstreams_memberships m ON m.registry_instance_id=r.registry_instance_id AND m.owner_id=r.owner_id AND m.native_reference_id=r.native_reference_id
        WHERE m.registry_instance_id=${scope.registry_instance_id} AND m.owner_id=${scope.owner_id} AND m.workstream_id=${id} AND m.closed_registry_version IS NULL LIMIT 1001`;
          const observations = yield* sql<{
            native_reference_id: string;
            observation_version: number;
          }>`SELECT o.native_reference_id,max(o.observation_version) AS observation_version FROM jones_workstreams_pr_observations o
        JOIN jones_workstreams_memberships m ON m.registry_instance_id=o.registry_instance_id AND m.owner_id=o.owner_id AND m.native_reference_id=o.native_reference_id
        WHERE m.registry_instance_id=${scope.registry_instance_id} AND m.owner_id=${scope.owner_id} AND m.workstream_id=${id} AND m.closed_registry_version IS NULL GROUP BY o.native_reference_id LIMIT 1001`;
          const sources: Evidence.Manifest["sources"][number][] = [],
            exclusions: Evidence.Exclusion[] = [];
          let bytes = 0;
          for (const member of memberships) {
            const binding = group.find((b) => b.native_reference_id === member.native_reference_id);
            const ref = references.find(
              (r) => r.native_reference_id === member.native_reference_id,
            );
            let reason: Evidence.Exclusion["reason"] | undefined;
            const descriptor = described.find(
              (d) => d.native_reference_id === member.native_reference_id,
            );
            if (!ref || ref.registration_state !== "attested" || !binding) reason = "unsupported";
            else if (binding.provider_id !== provider.id || !provider.configured || !descriptor)
              reason = "content_unavailable";
            else if (descriptor.kind === "excluded") reason = descriptor.reason;
            else {
              const source = descriptor.evidence;
              yield* Evidence.attempt(() =>
                Evidence.requireRollup(source.account_id === binding.account_id),
              );
              if (bytes + source.extraction_output_bytes > options.max_input_bytes)
                reason = "input_budget_exceeded";
              else {
                bytes += source.extraction_output_bytes;
                const obs = observations
                  .filter((o) => o.native_reference_id === member.native_reference_id)
                  .map((o) => o.observation_version);
                sources.push({
                  ...source,
                  native_reference_id: member.native_reference_id,
                  membership_id: member.membership_id,
                  membership_registry_version: member.opened.registry_version,
                  native_observation_version: obs.length ? Math.max(...obs) : null,
                });
              }
            }
            if (reason)
              exclusions.push({ native_reference_id: member.native_reference_id, reason });
          }
          if (!sources.length) {
            inspected.push({
              workstream_id: id,
              state: provider.configured ? "content_unavailable" : "provider_unconfigured",
              freshness: provider.configured ? "unknown" : "inaccessible",
              exclusions,
            });
            continue;
          }
          const declarations = yield* sql<{
            declaration_id: string;
            revision: number;
          }>`SELECT declaration_id,max(revision) AS revision FROM jones_workstreams_declarations
        WHERE registry_instance_id=${scope.registry_instance_id} AND owner_id=${scope.owner_id} AND workstream_id=${id} GROUP BY declaration_id ORDER BY declaration_id COLLATE BINARY LIMIT 1001`;
          const edges = yield* sql<{
            edge_id: string;
            opened_registry_version: number;
            closed_registry_version: number | null;
          }>`SELECT edge_id,opened_registry_version,closed_registry_version FROM jones_workstreams_edges
        WHERE registry_instance_id=${scope.registry_instance_id} AND owner_id=${scope.owner_id} AND (from_workstream_id=${id} OR to_workstream_id=${id}) ORDER BY edge_id COLLATE BINARY LIMIT 1001`;
          yield* Evidence.attempt(() =>
            Evidence.requireRollup(declarations.length <= 1000 && edges.length <= 1000),
          );
          const m = yield* Evidence.attempt(() =>
            Generation.createManifest({
              owner_id: scope.owner_id,
              workstream_id: id,
              workstream_version: record.version,
              registry_version: context.registry_version,
              contribution_policy: "active-primary-and-secondary-v1",
              sources,
              exclusions,
              extractor: generation.identity,
              declaration_revisions: declarations,
              relationship_revisions: edges.map((row) => ({
                edge_id: String(row.edge_id),
                opened_registry_version: Number(row.opened_registry_version),
                closed_registry_version:
                  row.closed_registry_version === null ? null : Number(row.closed_registry_version),
              })),
            }),
          );
          const current = yield* registry.context(scope);
          if (
            !current ||
            current.registry_version !== context.registry_version ||
            current.server_generation !== context.server_generation
          )
            continue;
          const existing =
            yield* sql`SELECT rollup_version FROM jones_workstreams_rollup_summaries WHERE registry_instance_id=${scope.registry_instance_id}
        AND owner_id=${scope.owner_id} AND workstream_id=${id} AND manifest_sha256=${m.manifest_sha256}`;
          if (!existing.length) manifests.push(m);
          inspected.push({
            workstream_id: id,
            state: "ready",
            freshness: Generation.summaryState(m).freshness,
            exclusions,
          });
        }
        return Evidence.immutable({ manifests, inspected });
      }).pipe(Effect.mapError(() => new Evidence.RollupError({ code: "content_unavailable" }))),
  );
  return ManifestSource.of({ discoverDirtyManifests });
});
/** @public Bounded durable discovery; no account-content database joins or implicit source mapping. */
export const layer = Layer.effect(ManifestSource, make);
