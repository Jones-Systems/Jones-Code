import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Schema from "effect/Schema";
import * as Wire from "@t3tools/contracts/jones/workstreamsRollups";
import type { AdmittedCaller } from "../mode/authorityMode.ts";
import * as Session from "../registry/session.ts";
import * as Store from "./store.ts";
import * as Generation from "./generation.ts";
import * as Evidence from "./evidence.ts";
const decodeWorkstreamsRollupReadEffect = Schema.decodeUnknownEffect(Wire.WorkstreamsRollupRead);

export interface SummaryAuthorization {
  readonly principal_id: string;
  readonly owner_id: string;
  readonly workstream_id: string;
  readonly account_id: string;
  readonly decision_id: string;
  readonly authorization_revision: number;
  readonly allowed: boolean;
}
export class SummaryAuthorizer extends Context.Service<
  SummaryAuthorizer,
  {
    readonly authorize: (
      principalId: string,
      ownerId: string,
      workstreamId: string,
      accounts: readonly string[],
    ) => Effect.Effect<readonly SummaryAuthorization[], Evidence.RollupError>;
  }
>()("t3/jones/workstreams/native/rollups/summaryRead/SummaryAuthorizer") {}
/** @public Account read permission is unknown until explicitly supplied, including for imported content. */
export const unconfiguredAuthorizerLayer = Layer.succeed(SummaryAuthorizer, {
  authorize: () => Effect.fail(new Evidence.RollupError({ code: "provider_unconfigured" })),
});
export interface SummaryView {
  readonly workstream_id: string;
  readonly rollup_version: number;
  readonly manifest_sha256: string;
  readonly coverage: "complete" | "partial";
  readonly freshness: Evidence.Freshness;
  readonly semantic_status: Evidence.Result["semantic_status"];
  readonly outcome: Evidence.Result["outcome"];
  readonly blockers: Evidence.Result["blockers"];
  readonly decisions: Evidence.Result["decisions"];
  readonly next_steps: Evidence.Result["next_steps"];
  readonly confidence: number;
}
export class SummaryRead extends Context.Service<
  SummaryRead,
  {
    readonly read: (
      caller: AdmittedCaller,
      id: string,
    ) => Effect.Effect<typeof Wire.WorkstreamsRollupRead.Type, Evidence.RollupError>;
    readonly view: (
      caller: AdmittedCaller,
      id: string,
    ) => Effect.Effect<SummaryView, Evidence.RollupError>;
  }
>()("t3/jones/workstreams/native/rollups/summaryRead") {}
const validId = (v: string) => /^[A-Za-z0-9._:-]{1,512}$/.test(v);
export function validateAuthorizations(
  principal: string,
  m: Store.SummaryMetadata,
  values: readonly SummaryAuthorization[],
  prior?: readonly SummaryAuthorization[],
): void {
  Evidence.requireRollup(values.length === m.account_ids.length, "not_found");
  values.forEach((a, i) =>
    Evidence.requireRollup(
      a.allowed &&
        a.principal_id === principal &&
        a.owner_id === m.owner_id &&
        a.workstream_id === m.workstream_id &&
        a.account_id === m.account_ids[i] &&
        validId(a.decision_id) &&
        Number.isSafeInteger(a.authorization_revision) &&
        a.authorization_revision >= 1 &&
        (!prior ||
          (a.decision_id === prior[i]?.decision_id &&
            a.authorization_revision === prior[i]?.authorization_revision)),
      "not_found",
    ),
  );
}
const make = Effect.gen(function* () {
  const session = yield* Session.Session,
    store = yield* Store.RollupStore,
    authorizer = yield* SummaryAuthorizer,
    evidence = yield* Evidence.RollupEvidenceProvider,
    generation = yield* Generation.RollupGenerationProvider;
  const authorize = (caller: AdmittedCaller) =>
    session.authorize(caller, "workstreams:read").pipe(
      Effect.mapError(
        (e) =>
          new Evidence.RollupError({
            code:
              e.code === "forbidden" || e.code === "not_found"
                ? "not_found"
                : "content_unavailable",
          }),
      ),
    );
  const load = Effect.fn("WorkstreamSummary.load")(function* (caller: AdmittedCaller, id: string) {
    yield* Evidence.attempt(
      () => Evidence.requireRollup(validId(id), "invalid_request"),
      "invalid_request",
    );
    const firstRegistry = yield* authorize(caller);
    const metadata = yield* store.latestMetadata(id);
    if (!metadata) return { metadata: null, summary: null };
    yield* Evidence.attempt(() =>
      Evidence.requireRollup(
        metadata.workstream_id === id &&
          metadata.owner_id === firstRegistry.context.owner_id &&
          metadata.account_ids.length >= 1 &&
          metadata.account_ids.length <= 100 &&
          new Set(metadata.account_ids).size === metadata.account_ids.length &&
          metadata.account_ids.every(validId),
      ),
    );
    const first = yield* authorizer.authorize(
      caller.principal_id,
      metadata.owner_id,
      id,
      metadata.account_ids,
    );
    yield* Evidence.attempt(
      () => validateAuthorizations(caller.principal_id, metadata, first),
      "not_found",
    );
    yield* authorize(caller);
    const payload = yield* store.readSummary(metadata);
    const second = yield* authorizer.authorize(
      caller.principal_id,
      metadata.owner_id,
      id,
      metadata.account_ids,
    );
    yield* Evidence.attempt(
      () => validateAuthorizations(caller.principal_id, metadata, second, first),
      "not_found",
    );
    const secondRegistry = yield* authorize(caller);
    if (
      firstRegistry.authorization.grant.grant_id !== secondRegistry.authorization.grant.grant_id ||
      firstRegistry.authorization.grant.grant_version !==
        secondRegistry.authorization.grant.grant_version ||
      firstRegistry.context.server_generation !== secondRegistry.context.server_generation
    )
      return yield* new Evidence.RollupError({ code: "not_found" });
    const manifest = yield* store.manifest(id, metadata.manifest_sha256);
    yield* Evidence.attempt(() => {
      Generation.validateSummary(manifest, payload);
      Evidence.requireRollup(
        payload.workstream_id === id &&
          payload.rollup_version === metadata.rollup_version &&
          payload.manifest_sha256 === metadata.manifest_sha256 &&
          payload.output_sha256 === metadata.output_sha256,
      );
    });
    return { metadata, summary: payload };
  });
  const read = Effect.fn("WorkstreamSummary.read")(function* (caller: AdmittedCaller, id: string) {
    const loaded = yield* load(caller, id);
    return yield* decodeWorkstreamsRollupReadEffect({
      contract: "workstreams-rollups/1.0.0",
      state: loaded.summary
        ? "ready"
        : !evidence.configured || !generation.configured
          ? "provider_unconfigured"
          : "unavailable",
      summary: loaded.summary,
      freshness: loaded.metadata?.freshness ?? (!evidence.configured ? "inaccessible" : "unknown"),
    }).pipe(Effect.mapError(() => new Evidence.RollupError({ code: "content_unavailable" })));
  });
  const view = Effect.fn("WorkstreamSummary.view")(function* (caller: AdmittedCaller, id: string) {
    const { metadata, summary } = yield* load(caller, id);
    if (!metadata || !summary) return yield* new Evidence.RollupError({ code: "not_found" });
    return Evidence.immutable({
      workstream_id: id,
      rollup_version: summary.rollup_version,
      manifest_sha256: summary.manifest_sha256,
      coverage: metadata.coverage,
      freshness: metadata.freshness,
      semantic_status: summary.result.semantic_status,
      outcome: summary.result.outcome,
      blockers: summary.result.blockers,
      decisions: summary.result.decisions,
      next_steps: summary.result.next_steps,
      confidence: summary.result.confidence,
    });
  });
  return SummaryRead.of({ read, view });
});
/** @public Authorize, read, reauthorize identical decisions, then verify digest; no plaintext cache. */
export const layer = Layer.effect(SummaryRead, make);
