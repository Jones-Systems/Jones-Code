import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Schema from "effect/Schema";
import * as Wire from "@t3tools/contracts";
import type { WorkstreamTransport } from "../../../../workstreams/WorkstreamGateway.ts";
import type { AdmittedCaller } from "../mode/authorityMode.ts";
import * as LocalTransport from "../registry/localTransport.ts";
import * as Evidence from "./evidence.ts";
const decodeWorkstreamPage = Schema.decodeUnknownSync(Wire.WorkstreamPage);
const decodeWorkstreamDetail = Schema.decodeUnknownSync(Wire.WorkstreamDetail);
const decodeWorkstreamMembershipPage = Schema.decodeUnknownSync(Wire.WorkstreamMembershipPage);
const decodeWorkstreamDeclarationPage = Schema.decodeUnknownSync(Wire.WorkstreamDeclarationPage);
const decodeWorkstreamEdgePage = Schema.decodeUnknownSync(Wire.WorkstreamEdgePage);
const decodeWorkstreamHistoryPage = Schema.decodeUnknownSync(Wire.WorkstreamHistoryPage);
const decodeWorkstreamReferenceDetail = Schema.decodeUnknownSync(Wire.WorkstreamReferenceDetail);

export class DashboardClient extends Context.Service<
  DashboardClient,
  {
    readonly forCaller: (
      caller: AdmittedCaller,
    ) => Effect.Effect<
      Pick<
        WorkstreamTransport,
        | "listWorkstreams"
        | "getWorkstream"
        | "listMemberships"
        | "listDeclarations"
        | "listEdges"
        | "listHistory"
        | "getReference"
      >,
      Evidence.RollupError
    >;
  }
>()("t3/jones/workstreams/native/rollups/dashboard/DashboardClient") {}
/** @public Native authorized transport adapter. No HTTP or CP authentication implementation is ported. */
export const nativeClientLayer = Layer.effect(
  DashboardClient,
  Effect.gen(function* () {
    const native = yield* LocalTransport.LocalTransport;
    return {
      forCaller: (caller) =>
        native.forCaller(caller).pipe(
          Effect.map((a) => a.transport),
          Effect.mapError(
            (e) =>
              new Evidence.RollupError({
                code:
                  e.code === "forbidden" || e.code === "not_found"
                    ? "not_found"
                    : "content_unavailable",
              }),
          ),
        ),
    };
  }),
);
export type DashboardWorkstream = Omit<Wire.Workstream, "created_by" | "updated_by">;
export interface DashboardDetail {
  readonly workstream: DashboardWorkstream;
  readonly memberships: readonly Wire.MembershipEpisode[];
  readonly declarations: readonly Wire.DeclarationRevision[];
  readonly edges: readonly Wire.WorkstreamEdge[];
  readonly history: readonly (typeof Wire.WorkstreamAuditEvent.Type)[];
  readonly manually_linked_prs: readonly {
    readonly url: string;
    readonly native_reference_id: string;
    readonly locator: NonNullable<Wire.NativeReference["pr_locator"]>;
    readonly native_locator: Wire.NativeReference["identity"];
    readonly registration_state: Wire.NativeReference["registration"]["state"];
    readonly observation: Wire.WorkstreamReferenceDetail["latest_observation"];
  }[];
  readonly continuation: {
    readonly memberships_cursor: string | null;
    readonly declarations_cursor: string | null;
    readonly edges_cursor: string | null;
    readonly history_cursor: string | null;
  };
}
export class Dashboard extends Context.Service<
  Dashboard,
  {
    readonly list: (
      caller: AdmittedCaller,
      query?: { readonly limit?: number; readonly cursor?: string },
    ) => Effect.Effect<
      { readonly items: readonly DashboardWorkstream[]; readonly next_cursor: string | null },
      Evidence.RollupError
    >;
    readonly detail: (
      caller: AdmittedCaller,
      id: string,
    ) => Effect.Effect<DashboardDetail, Evidence.RollupError>;
  }
>()("t3/jones/workstreams/native/rollups/dashboard") {}
const contract = {
  contractVersion: Wire.WORKSTREAM_CONTRACT_HEADER_VERSION,
  contractManifest: Wire.WORKSTREAM_CONTRACT_MANIFEST_SHA256,
};
const same = (a: Wire.WorkstreamReadContext, b: Wire.WorkstreamReadContext) =>
  a.owner_id === b.owner_id &&
  a.server_generation === b.server_generation &&
  a.registry_version === b.registry_version;
const failure = (e: unknown) =>
  new Evidence.RollupError({
    code:
      typeof e === "object" && e !== null && "detail" in e && e.detail === "not_found"
        ? "not_found"
        : "content_unavailable",
  });
const projectWorkstream = (w: Wire.Workstream): DashboardWorkstream => ({
  workstream_id: w.workstream_id,
  owner_id: w.owner_id,
  name: w.name,
  lifecycle: w.lifecycle,
  progress: w.progress,
  delivery: w.delivery,
  freshness: w.freshness,
  sort_order: w.sort_order,
  version: w.version,
  created_at: w.created_at,
  updated_at: w.updated_at,
});
const make = Effect.gen(function* () {
  const clients = yield* DashboardClient;
  const list = Effect.fn("WorkstreamDashboard.list")(function* (
    caller: AdmittedCaller,
    query: { readonly limit?: number; readonly cursor?: string } = {},
  ) {
    const limit = query.limit ?? 50;
    yield* Evidence.attempt(
      () =>
        Evidence.requireRollup(
          Number.isSafeInteger(limit) &&
            limit >= 1 &&
            limit <= 100 &&
            (query.cursor === undefined || /^[A-Za-z0-9_-]{1,4096}$/.test(query.cursor)),
          "invalid_request",
        ),
      "invalid_request",
    );
    const client = yield* clients.forCaller(caller),
      page = yield* client
        .listWorkstreams({ ...contract, ...query, limit })
        .pipe(Effect.mapError(failure));
    return yield* Evidence.attempt(() => {
      const decoded = decodeWorkstreamPage(page);
      Evidence.requireRollup(
        decoded.items.length <= limit &&
          decoded.items.every((w) => w.owner_id === decoded.context.owner_id),
      );
      return Evidence.immutable({
        items: decoded.items.map(projectWorkstream),
        next_cursor: decoded.next_cursor,
      });
    });
  });
  const detail = Effect.fn("WorkstreamDashboard.detail")(function* (
    caller: AdmittedCaller,
    id: string,
  ) {
    yield* Evidence.attempt(
      () => Evidence.requireRollup(/^[A-Za-z0-9._:-]{1,512}$/.test(id), "invalid_request"),
      "invalid_request",
    );
    const client = yield* clients.forCaller(caller),
      query = { ...contract, workstreamId: id, limit: 100 };
    const detail = yield* client
      .getWorkstream({ ...contract, workstreamId: id })
      .pipe(Effect.mapError(failure));
    const [memberships, declarations, edges, history] = yield* Effect.all(
      [
        client.listMemberships(query),
        client.listDeclarations(query),
        client.listEdges(query),
        client.listHistory(query),
      ],
      { concurrency: 4 },
    ).pipe(Effect.mapError(failure));
    yield* Evidence.attempt(() => {
      decodeWorkstreamDetail(detail);
      decodeWorkstreamMembershipPage(memberships);
      decodeWorkstreamDeclarationPage(declarations);
      decodeWorkstreamEdgePage(edges);
      decodeWorkstreamHistoryPage(history);
      Evidence.requireRollup(
        detail.workstream.workstream_id === id &&
          detail.workstream.owner_id === detail.context.owner_id &&
          [memberships, declarations, edges, history].every(
            (p) => p.items.length <= 100 && same(p.context, detail.context),
          ) &&
          memberships.items.every((m) => m.workstream_id === id) &&
          declarations.items.every((d) => d.workstream_id === id) &&
          edges.items.every((e) => e.from_workstream_id === id || e.to_workstream_id === id) &&
          history.items.every(
            (e) =>
              e.workstream_versions.some((v) => v.workstream_id === id) ||
              e.lifecycle_declaration?.workstream_id === id,
          ),
      );
    });
    const ids = [
      ...new Set(
        memberships.items.filter((m) => m.closed === null).map((m) => m.native_reference_id),
      ),
    ];
    const references = yield* Effect.forEach(
      ids,
      (nativeReferenceId) => client.getReference({ ...contract, nativeReferenceId }),
      { concurrency: 4 },
    ).pipe(Effect.mapError(failure));
    return yield* Evidence.attempt(() => {
      const prs: DashboardDetail["manually_linked_prs"][number][] = [];
      references.forEach((value, index) => {
        const r = decodeWorkstreamReferenceDetail(value),
          reference = r.reference;
        Evidence.requireRollup(
          same(r.context, detail.context) &&
            reference.owner_id === detail.context.owner_id &&
            reference.native_reference_id === ids[index],
        );
        if (reference.pr_locator === null) return;
        const locator = reference.pr_locator,
          identity = reference.identity;
        Evidence.requireRollup(
          locator.host === "github.com" &&
            identity.provider === "github" &&
            identity.resource_kind === "pull_request" &&
            identity.id_kind === "external" &&
            identity.native_id ===
              `${locator.repository_owner}/${locator.repository_name}#${locator.number}` &&
            (r.latest_observation === null ||
              r.latest_observation.native_reference_id === reference.native_reference_id),
        );
        prs.push({
          url: `https://github.com/${locator.repository_owner}/${locator.repository_name}/pull/${locator.number}`,
          native_reference_id: reference.native_reference_id,
          locator,
          native_locator: identity,
          registration_state: reference.registration.state,
          observation: r.latest_observation,
        });
      });
      return Evidence.immutable({
        workstream: projectWorkstream(detail.workstream),
        memberships: decodeWorkstreamMembershipPage(memberships).items,
        declarations: decodeWorkstreamDeclarationPage(declarations).items,
        edges: decodeWorkstreamEdgePage(edges).items,
        history: decodeWorkstreamHistoryPage(history).items,
        manually_linked_prs: prs,
        continuation: {
          memberships_cursor: memberships.next_cursor,
          declarations_cursor: declarations.next_cursor,
          edges_cursor: edges.next_cursor,
          history_cursor: history.next_cursor,
        },
      });
    });
  });
  return Dashboard.of({ list, detail });
});
/** @public Snapshot-consistent CP-equivalent projections with explicit continuation cursors. */
export const layer = Layer.effect(Dashboard, make);
