import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as DateTime from "effect/DateTime";
import * as Clock from "effect/Clock";
import * as Fiber from "effect/Fiber";
import * as Store from "./store.ts";
import * as Manifests from "./manifestSource.ts";
import * as Generation from "./generation.ts";
import * as Evidence from "./evidence.ts";
export const ApplicationOptions = Context.Reference<{
  readonly discovery_limit: number;
  readonly lease_ms: number;
  readonly provider_timeout_ms: number;
}>("t3/jones/workstreams/native/rollups/ApplicationOptions", {
  defaultValue: () => ({ discovery_limit: 100, lease_ms: 60000, provider_timeout_ms: 50000 }),
});
export class RollupAdmission extends Context.Service<
  RollupAdmission,
  {
    readonly tryAcquire: () => Effect.Effect<Effect.Effect<void> | null>;
  }
>()("t3/jones/workstreams/native/rollups/application/RollupAdmission") {}
/** @public Share this finite admission owner with other inference consumers through the parent composition. */
export const admissionLayer = Layer.sync(RollupAdmission, () => {
  let running = false;
  return {
    tryAcquire: () =>
      Effect.sync(() => {
        if (running) return null;
        running = true;
        let released = false;
        return Effect.sync(() => {
          if (!released) {
            released = true;
            running = false;
          }
        });
      }),
  };
});
export interface RunOutcome {
  readonly status:
    | "idle"
    | "published"
    | "provider_failed"
    | "provider_unconfigured"
    | "superseded";
  readonly discovered: number;
  readonly expired_claims: number;
  readonly overloaded: number;
  readonly workstream_id?: string;
  readonly rollup_version?: number;
  readonly inspected: Manifests.Discovery["inspected"];
}
export class RollupApplication extends Context.Service<
  RollupApplication,
  {
    readonly processOnce: () => Effect.Effect<RunOutcome, Evidence.RollupError>;
  }
>()("t3/jones/workstreams/native/rollups/application/RollupApplication") {}
const make = Effect.gen(function* () {
  const store = yield* Store.RollupStore,
    manifests = yield* Manifests.ManifestSource,
    evidence = yield* Evidence.RollupEvidenceProvider,
    generation = yield* Generation.RollupGenerationProvider,
    admission = yield* RollupAdmission,
    options = yield* ApplicationOptions,
    limits = yield* Manifests.DiscoveryOptions,
    clock = yield* Clock.Clock;
  yield* Evidence.attempt(
    () =>
      Evidence.requireRollup(
        Number.isSafeInteger(options.discovery_limit) &&
          options.discovery_limit >= 1 &&
          options.discovery_limit <= 100 &&
          Number.isSafeInteger(options.lease_ms) &&
          options.lease_ms >= 1000 &&
          options.lease_ms <= 299000 &&
          Number.isSafeInteger(options.provider_timeout_ms) &&
          options.provider_timeout_ms >= 100 &&
          options.provider_timeout_ms < options.lease_ms,
        "invalid_request",
      ),
    "invalid_request",
  );
  const now = () =>
    Effect.map(clock.currentTimeMillis, (n) => DateTime.formatIso(DateTime.makeUnsafe(n)));
  const processOnce = Effect.fn("RollupApplication.processOnce")(function* () {
    const start = yield* now(),
      expired_claims = yield* store.expireClaims(start);
    const discovery = yield* manifests.discoverDirtyManifests(options.discovery_limit, start);
    let overloaded = 0;
    for (const manifest of discovery.manifests)
      if ((yield* store.enqueue(manifest, start)) === "overloaded") overloaded++;
    const base = {
      discovered: discovery.manifests.length,
      expired_claims,
      overloaded,
      inspected: discovery.inspected,
    };
    if (!evidence.configured || !generation.configured)
      return { ...base, status: "provider_unconfigured" as const };
    const release = yield* admission.tryAcquire();
    if (!release) return { ...base, status: "idle" as const };
    const claimed = yield* store.claim(start, options.lease_ms).pipe(Effect.onError(() => release));
    if (!claimed) {
      yield* release;
      return { ...base, status: "idle" as const };
    }
    const claim = claimed,
      controller = new AbortController();
    let stage: "input_unavailable" | "provider_failure" = "input_unavailable",
      providerStarted = false,
      inputRunning = false,
      abandoned = false;
    const remaining = () =>
      Effect.map(clock.currentTimeMillis, (n) => Date.parse(claim.lease_expires_at) - n);
    const timeout = () =>
      Effect.sync(() => {
        abandoned = true;
        controller.abort();
      }).pipe(
        Effect.flatMap(() => Effect.fail(new Evidence.RollupError({ code: "provider_timeout" }))),
      );
    const authorize = Effect.fn("RollupApplication.authorize")(function* () {
      if (!(yield* store.verifyAdmission(claim.manifest))) return false;
      const budget = Math.min(options.provider_timeout_ms, yield* remaining());
      if (budget <= 0) return yield* timeout();
      const authorization = yield* evidence
        .verifyCurrentAuthorization(claim.manifest)
        .pipe(Effect.forkDetach);
      return yield* Fiber.join(authorization).pipe(
        Effect.timeoutOrElse({ duration: budget, orElse: timeout }),
        Effect.ensuring(Fiber.interrupt(authorization).pipe(Effect.forkDetach, Effect.asVoid)),
      );
    });
    const work = Effect.gen(function* () {
      if (!(yield* authorize()) || !(yield* store.verifyAdmission(claim.manifest)))
        return yield* new Evidence.RollupError({ code: "input_unavailable" });
      if (
        claim.manifest.sources.reduce(
          (total, source) => total + source.extraction_output_bytes,
          0,
        ) > limits.max_input_bytes
      )
        return yield* new Evidence.RollupError({ code: "input_unavailable" });
      let budget = yield* remaining();
      if (budget <= 0) return yield* timeout();
      inputRunning = true;
      const inputFiber = yield* evidence
        .readExtractionInputs(claim.manifest, controller.signal)
        .pipe(
          Effect.uninterruptible,
          Effect.ensuring(
            Effect.suspend(() => {
              inputRunning = false;
              return abandoned ? release : Effect.void;
            }),
          ),
          Effect.forkDetach,
        );
      const inputs = yield* Fiber.join(inputFiber).pipe(
        Effect.timeoutOrElse({ duration: budget, orElse: timeout }),
      );
      const input = yield* Evidence.attempt(() => {
        Evidence.requireRollup(
          Evidence.canonicalJson(generation.identity) ===
            Evidence.canonicalJson(claim.manifest.extractor),
          "input_unavailable",
        );
        return Generation.createProviderInput(claim.manifest, inputs);
      }, "input_unavailable");
      if (!(yield* authorize()) || !(yield* store.verifyAdmission(claim.manifest)))
        return yield* new Evidence.RollupError({ code: "input_unavailable" });
      budget = Math.min(options.provider_timeout_ms, yield* remaining());
      if (budget <= 0) return yield* timeout();
      stage = "provider_failure";
      providerStarted = true;
      // A timed-out noncooperative provider retains its admission until its actual effect settles.
      const generating = yield* generation
        .generate(input, controller.signal)
        .pipe(Effect.uninterruptible, Effect.ensuring(release), Effect.forkDetach);
      const result = yield* Fiber.join(generating).pipe(
        Effect.timeoutOrElse({ duration: budget, orElse: timeout }),
      );
      if ((yield* remaining()) <= 0) return yield* timeout();
      stage = "input_unavailable";
      const version = yield* store.nextRollupVersion(claim),
        at = yield* now();
      const summary = yield* Evidence.attempt(
        () =>
          Generation.createSummary(claim.manifest, {
            workstream_id: claim.workstream_id,
            rollup_version: version,
            manifest_sha256: claim.manifest.manifest_sha256,
            generated_at: at,
            result,
          }),
        "provider_failure",
      );
      budget = Math.min(options.provider_timeout_ms, yield* remaining());
      if (budget <= 0) return yield* timeout();
      yield* store
        .completeClaim(claim, summary, at)
        .pipe(Effect.timeoutOrElse({ duration: budget, orElse: timeout }));
      return {
        ...base,
        status: "published" as const,
        workstream_id: claim.workstream_id,
        rollup_version: version,
      };
    }).pipe(
      Effect.catchTags({
        WorkstreamsRollupError: (failure) =>
          Effect.gen(function* () {
            if (failure.code === "stale_claim")
              return { ...base, status: "superseded" as const, workstream_id: claim.workstream_id };
            const failed = yield* store
              .failClaim(
                claim,
                failure.code === "provider_timeout" ? "provider_timeout" : stage,
                yield* now(),
              )
              .pipe(
                Effect.map(() => false),
                Effect.catchTags({
                  WorkstreamsRollupError: (e) =>
                    e.code === "stale_claim" ? Effect.succeed(true) : Effect.fail(e),
                }),
              );
            return {
              ...base,
              status: failed ? ("superseded" as const) : ("provider_failed" as const),
              workstream_id: claim.workstream_id,
            };
          }),
      }),
      Effect.ensuring(
        Effect.suspend(() => {
          abandoned = true;
          controller.abort();
          return !providerStarted && !inputRunning ? release : Effect.void;
        }),
      ),
    );
    return yield* work;
  });
  return RollupApplication.of({ processOnce });
});
/** @public One bounded sweep step; timer ownership remains in the server runtime. */
export const layer = Layer.effect(RollupApplication, make);
