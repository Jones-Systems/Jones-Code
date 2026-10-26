// @effect-diagnostics nodeBuiltinImport:off -- Process-local cursor signing and bounded in-memory snapshot sizing.
import * as NodeCrypto from "node:crypto";
import * as Wire from "@t3tools/contracts";
import {
  WorkstreamsNativeReadEnvelope,
  type WorkstreamsNativeReadSource,
} from "@t3tools/contracts/jones/workstreamsNativeRead";
import * as Context from "effect/Context";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Schema from "effect/Schema";
import { NativeHome, type AdmittedCaller } from "../mode/authorityMode.ts";
import * as Registry from "../registry/service.ts";
import * as Session from "../registry/session.ts";
import * as Store from "../store/registryStore.ts";
import * as Database from "../store/sql.ts";
import * as Keys from "../crypto/keyProvider.ts";
import { ProviderError } from "../providers/localAttestation.ts";
import { RegistryCursorCodec } from "../domain/cursor.ts";
import {
  collectionPosition,
  lifecycleCounts,
  pageCollection,
  referenceForRead,
} from "../domain/collections.ts";
import { requireAttested } from "../domain/decider.ts";
import { WorkstreamDomainError } from "../domain/errors.ts";
import type { PageQuery } from "../domain/state.ts";

export interface ProjectionItem {
  readonly workstream: Wire.Workstream;
  readonly active_memberships: { readonly primary: number; readonly secondary: number };
  readonly appearance: Wire.WorkstreamAppearance;
  readonly lifecycle: typeof Wire.WorkstreamLifecycleDeclaration.Type | null;
}
export interface ProjectionSnapshot {
  readonly envelope: WorkstreamsNativeReadEnvelope;
  readonly items: readonly ProjectionItem[];
  readonly counts: Wire.WorkstreamRegistryCounts;
  readonly authorization: Registry.Snapshot["authorization"];
}
export interface ProjectionPage {
  readonly envelope: WorkstreamsNativeReadEnvelope;
  readonly items: readonly ProjectionItem[];
  readonly next_cursor: string | null;
}
const isRegistryError = Schema.is(Session.RegistryError);
const isProviderError = Schema.is(ProviderError);
const isKeyUnavailable = Schema.is(Keys.KeyUnavailable);
const encodeMaterial = Schema.encodeEffect(Schema.fromJsonString(Schema.Unknown));
export const readFailure = (cause: unknown): Session.RegistryError =>
  isRegistryError(cause)
    ? cause
    : new Session.RegistryError({
        code:
          isProviderError(cause) && cause.code === "not_granted"
            ? "forbidden"
            : isKeyUnavailable(cause)
              ? "key_unavailable"
              : cause instanceof WorkstreamDomainError
                ? cause.code
                : "unavailable",
        cause,
      });

export class Projection extends Context.Service<
  Projection,
  {
    readonly capture: (
      caller: AdmittedCaller,
    ) => Effect.Effect<ProjectionSnapshot, Session.RegistryError>;
    readonly page: (
      caller: AdmittedCaller,
      query: PageQuery,
    ) => Effect.Effect<ProjectionPage, Session.RegistryError>;
    readonly assertCurrent: (
      caller: AdmittedCaller,
      snapshot: ProjectionSnapshot,
    ) => Effect.Effect<void, Session.RegistryError>;
  }
>()("t3/jones/workstreams/native/read/projection") {}

const decodeEnvelope = Schema.decodeUnknownEffect(WorkstreamsNativeReadEnvelope);
const decodeAppearance = Schema.decodeUnknownEffect(Wire.WorkstreamAppearance);
type Source = typeof WorkstreamsNativeReadSource.Type;
const byteOrder = (a: string, b: string) => Buffer.compare(Buffer.from(a), Buffer.from(b));

// A legacy-shaped registration is not evidence of a native provider observation.
const hasNativeEvidence = (raw: unknown, reference: Wire.NativeReference): boolean => {
  const registration = reference.registration;
  if (
    reference.identity.provider !== "t3" ||
    registration.state !== "attested" ||
    !registration.evidence
  )
    return false;
  if (typeof raw !== "object" || raw === null || !("native_attestation" in raw)) return false;
  const marker = raw.native_attestation;
  if (typeof marker !== "object" || marker === null) return false;
  return (
    "contract" in marker &&
    marker.contract === "workstreams-t3-provider/2.0.0" &&
    "evidence_sha256" in marker &&
    marker.evidence_sha256 === registration.evidence.evidence_sha256 &&
    "authority_namespace" in marker &&
    marker.authority_namespace === registration.evidence.authority_namespace &&
    "store_generation" in marker &&
    marker.store_generation === registration.evidence.store_generation &&
    "attested_at" in marker &&
    marker.attested_at === registration.attested_at &&
    "expires_at" in marker &&
    marker.expires_at === registration.expires_at
  );
};

export const layer = Layer.effect(
  Projection,
  Effect.gen(function* () {
    const home = yield* NativeHome;
    const registry = yield* Registry.Registry;
    const session = yield* Session.Session;
    const store = yield* Store.RegistryStore;
    const db = yield* Database.NativeSql;
    const keys = yield* Keys.WorkstreamsKeyProvider;
    const codec = new RegistryCursorCodec(NodeCrypto.randomBytes(32));
    const capture = Effect.fn("NativeRead.capture")((caller: AdmittedCaller) =>
      db
        .transaction(
          home,
          Effect.gen(function* () {
            const snapshot = yield* registry.snapshot(caller);
            const at = DateTime.formatIso(yield* DateTime.now);
            const { state, context, authorization } = snapshot;
            // The existing registry loader is whole-owner; cap the retained export/projection material.
            if (state.workstreams.size > 10_000)
              return yield* new Session.RegistryError({ code: "backpressure" });
            const rawReferences = yield* store.list(home, "references");
            const appearanceRows = yield* store.list(home, "appearance");
            const visible = [...state.references.values()].filter(
              (r) =>
                referenceForRead(
                  r,
                  home.owner_id,
                  caller.principal_id,
                  state.source_bindings.get(r.identity.source_instance_id),
                  at,
                ) !== null,
            );
            const bySource = new Map<string, Wire.NativeReference[]>();
            for (const reference of visible) {
              const id = reference.identity.source_instance_id;
              bySource.set(id, [...(bySource.get(id) ?? []), reference]);
            }
            for (const id of state.source_bindings.keys())
              if (!bySource.has(id)) bySource.set(id, []);
            const sources: Source[] = [];
            for (const [id, references] of [...bySource].toSorted((a, b) =>
              byteOrder(a[0], b[0]),
            )) {
              const binding = state.source_bindings.get(id);
              const mapped = binding?.state === "active";
              const observations = references.map(
                (r): Pick<Source, "attestation" | "observed_at"> => {
                  if (!mapped) return { attestation: "unknown", observed_at: null };
                  if (
                    r.registration.state === "expired" ||
                    (r.registration.expires_at !== null &&
                      Date.parse(r.registration.expires_at) <= Date.parse(at))
                  )
                    return { attestation: "expired", observed_at: r.registration.attested_at };
                  try {
                    requireAttested(r, at, binding);
                  } catch (cause) {
                    if (!(cause instanceof WorkstreamDomainError)) throw cause;
                    return { attestation: "unavailable", observed_at: null };
                  }
                  const raw = rawReferences.find(
                    (row) => row.metadata.native_reference_id === r.native_reference_id,
                  )?.value;
                  if (
                    !hasNativeEvidence(raw, r) ||
                    r.registration.attested_at === null ||
                    Date.parse(r.registration.attested_at) > Date.parse(at)
                  )
                    return { attestation: "unknown", observed_at: null };
                  return { attestation: "current", observed_at: r.registration.attested_at };
                },
              );
              const attestation =
                !mapped || observations.length === 0
                  ? "unknown"
                  : observations.every((x) => x.attestation === "current")
                    ? "current"
                    : observations.some((x) => x.attestation === "unavailable")
                      ? "unavailable"
                      : observations.some((x) => x.attestation === "expired")
                        ? "expired"
                        : "unknown";
              // Oldest evidence is the conservative observation for an aggregate source.
              const observed = observations
                .flatMap((x) => (x.observed_at === null ? [] : [x.observed_at]))
                .toSorted();
              sources.push({
                source_instance_id: id,
                mapping: mapped ? "mapped" : "unmapped",
                attestation,
                observed_at: attestation === "current" ? observed[0]! : null,
              });
            }
            if (sources.length > 1000)
              return yield* new Session.RegistryError({ code: "backpressure" });
            const envelope = yield* decodeEnvelope({
              contract: "workstreams-read/1.0.0",
              registry: {
                registry_instance_id: home.registry_instance_id,
                owner_id: context.owner_id,
                canonical_origin: context.canonical_origin,
                server_generation: context.server_generation,
                registry_version: context.registry_version,
              },
              provenance: {
                authority: "native-home",
                home_environment_id: context.home_environment_id,
                served_by_environment_id: home.served_by_environment_id,
                source: "live",
                observed_at: at,
                stale: false,
              },
              coverage: { registry: "complete", sources },
            });
            const visibleIds = new Set(visible.map((r) => r.native_reference_id));
            const items: ProjectionItem[] = [];
            for (const workstream of [...state.workstreams.values()].toSorted(
              (a, b) => a.sort_order - b.sort_order || byteOrder(a.workstream_id, b.workstream_id),
            )) {
              const memberships = [...state.memberships.values()].filter(
                (m) =>
                  m.workstream_id === workstream.workstream_id &&
                  m.closed === null &&
                  visibleIds.has(m.native_reference_id),
              );
              const row = appearanceRows.find(
                (a) => a.metadata.workstream_id === workstream.workstream_id,
              );
              const appearance = row
                ? yield* decodeAppearance(row.value)
                : { workstream_id: workstream.workstream_id, border_color: null, version: 0 };
              const lifecycle =
                state.lifecycle_declarations
                  .get(workstream.workstream_id)
                  ?.reduce<typeof Wire.WorkstreamLifecycleDeclaration.Type | null>(
                    (latest, next) => (!latest || next.revision > latest.revision ? next : latest),
                    null,
                  ) ?? null;
              items.push({
                workstream,
                appearance,
                lifecycle,
                active_memberships: {
                  primary: memberships.filter((m) => m.kind === "primary").length,
                  secondary: memberships.filter((m) => m.kind === "secondary").length,
                },
              });
            }
            const counts: Wire.WorkstreamRegistryCounts = {
              context: {
                owner_id: context.owner_id,
                server_generation: context.server_generation,
                registry_version: context.registry_version,
              },
              principal_id: caller.principal_id,
              observed_at: at,
              authority_effect: "none",
              ...lifecycleCounts(state),
            };
            const result = { envelope, items, counts, authorization };
            const encoded = yield* encodeMaterial(result);
            if (Buffer.byteLength(encoded) > 8 * 1024 * 1024)
              return yield* new Session.RegistryError({ code: "backpressure" });
            return result;
          }),
        )
        .pipe(Effect.mapError(readFailure)),
    );
    const page = Effect.fn("NativeRead.page")(function* (caller: AdmittedCaller, query: PageQuery) {
      const snapshot = yield* capture(caller);
      return yield* Effect.try({
        try: () => {
          const position = codec.decode(
            query,
            snapshot.authorization,
            "read-projection:global",
            snapshot.counts.context,
            Date.parse(snapshot.envelope.provenance.observed_at),
          );
          const found = pageCollection(
            "workstreams",
            snapshot.items.map((x) => x.workstream),
            position.after,
            position.limit + 1,
          );
          const ids = new Set(found.slice(0, position.limit).map((w) => w.workstream_id));
          return {
            envelope: snapshot.envelope,
            items: snapshot.items.filter((x) => ids.has(x.workstream.workstream_id)),
            next_cursor:
              found.length > position.limit
                ? codec.encode(
                    {
                      ...position,
                      after: collectionPosition("workstreams", found[position.limit - 1]!),
                    },
                    snapshot.authorization,
                    "read-projection:global",
                    snapshot.counts.context,
                  )
                : null,
          };
        },
        catch: readFailure,
      });
    });
    const assertCurrent = Effect.fn("NativeRead.assertCurrent")(
      (caller: AdmittedCaller, snapshot: ProjectionSnapshot) =>
        db
          .transaction(
            home,
            Effect.gen(function* () {
              const current = yield* session.authorize(caller, "workstreams:read");
              yield* keys.get(yield* keys.activeKeyId(home));
              if (
                current.context.server_generation !==
                  snapshot.envelope.registry.server_generation ||
                current.context.registry_version !== snapshot.envelope.registry.registry_version
              )
                return yield* new Session.RegistryError({ code: "cursor_stale" });
              if (
                current.authorization.grant.grant_version !==
                  snapshot.authorization.grant.grant_version ||
                current.authorization.actor.principal_id !==
                  snapshot.authorization.actor.principal_id
              )
                return yield* new Session.RegistryError({ code: "forbidden" });
            }),
          )
          .pipe(Effect.mapError(readFailure)),
    );
    return Projection.of({ capture, page, assertCurrent });
  }),
);
