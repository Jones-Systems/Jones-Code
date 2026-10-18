import type { ProviderRuntimeBinding } from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";

export class RuntimePermitError extends Schema.TaggedError<RuntimePermitError>()(
  "RuntimePermitError",
  { reason: Schema.Literals(["revoked", "consumed", "not_current"]) },
) {}

export interface RuntimeOperationIdentity {
  readonly issuerId: string;
  readonly operationId: string;
  readonly admissionId: string;
  readonly participantId: string;
  readonly binding: ProviderRuntimeBinding;
  readonly evidenceRevision: number;
  readonly evidenceReference: string;
  /** Exact operation writers declared by the owner; never all operation writers by implication. */
  readonly writerReferences: ReadonlyArray<string>;
}

export type ScopeObservation =
  | { readonly state: "active" | "quiescent"; readonly coverage: "complete" | "partial" }
  | { readonly state: "unknown" };

export interface RuntimeOperationEvidence {
  readonly identity: RuntimeOperationIdentity;
  readonly outcome: "not_invoked" | "active" | "quiescent" | "unknown";
  readonly coverage: "complete" | "partial";
}

const permitBrand = Symbol("RuntimeOperationPermit");

export interface RuntimeOperationPermit<A, E> {
  readonly [permitBrand]: true;
  readonly identity: RuntimeOperationIdentity;
  /** Rechecked at renewal; expiration cannot invalidate authentic old retirement evidence. */
  readonly assertCurrent: Effect.Effect<void, RuntimePermitError>;
  readonly invoke: <E2, R>(
    commitStart: Effect.Effect<void, E2, R>,
  ) => Effect.Effect<A, E | E2 | RuntimePermitError, R>;
  readonly revoke: Effect.Effect<void>;
  readonly observe: Effect.Effect<RuntimeOperationEvidence>;
}

export type RuntimePermitAvailability<A, E> =
  | { readonly status: "available"; readonly permit: RuntimeOperationPermit<A, E> }
  | { readonly status: "unavailable"; readonly reason: "warm_runtime" | "unsupported" | "restart" };

interface OperationOwner<A, E> {
  readonly mode: "exclusive-operation";
  readonly identity: RuntimeOperationIdentity;
  readonly isCurrent: Effect.Effect<boolean>;
  /** Existing runtime owner serializes the effect with replacement/stop, then rechecks its binding. */
  readonly withCurrent: <V, F, R>(
    effect: Effect.Effect<V, F, R>,
  ) => Effect.Effect<V, F | RuntimePermitError, R>;
  readonly invoke: Effect.Effect<A, E>;
  /** Retained old scope evidence; independent of the current invocation permit's lifetime. */
  readonly observe: Effect.Effect<ScopeObservation>;
}

/** Source port only. No production issuer, persisted capability, or restart reconstruction is installed. */
export function makeRuntimeOperationPermit<A, E>(
  owner: OperationOwner<A, E> | { readonly mode: "warm_runtime" | "unsupported" | "restart" },
): RuntimePermitAvailability<A, E> {
  if (owner.mode !== "exclusive-operation") return { status: "unavailable", reason: owner.mode };
  const identity = Object.freeze({
    ...owner.identity,
    binding: Object.freeze({ ...owner.identity.binding }),
    writerReferences: Object.freeze([...owner.identity.writerReferences]),
  });
  const { isCurrent, withCurrent, invoke, observe } = owner;
  let observationRevision = 0;
  let claimed = false;
  let revoked = false;
  let inFlight = false;
  let startAttempted = false;
  let invocation: "not_started" | "uncertain" | "confirmed" = "not_started";
  const assertCurrent = Effect.gen(function* () {
    if (revoked) return yield* new RuntimePermitError({ reason: "revoked" });
    if (!(yield* isCurrent)) return yield* new RuntimePermitError({ reason: "not_current" });
    if (revoked) return yield* new RuntimePermitError({ reason: "revoked" });
  });
  const evidence = (
    outcome: RuntimeOperationEvidence["outcome"],
    coverage: "complete" | "partial",
  ): RuntimeOperationEvidence => Object.freeze({ identity, outcome, coverage });
  return {
    status: "available",
    permit: Object.freeze({
      [permitBrand]: true as const,
      identity,
      assertCurrent,
      invoke: <E2, R>(commitStart: Effect.Effect<void, E2, R>) =>
        Effect.suspend(() => {
          let ownsClaim = false;
          return Effect.gen(function* () {
            yield* Effect.suspend(() => {
              if (revoked) return Effect.fail(new RuntimePermitError({ reason: "revoked" }));
              if (claimed) return Effect.fail(new RuntimePermitError({ reason: "consumed" }));
              observationRevision += 1;
              claimed = true;
              inFlight = true;
              ownsClaim = true;
              return Effect.void;
            });
            yield* assertCurrent;
            // Set before entering the consumer effect: failed/interrupted commit may have persisted start.
            yield* Effect.suspend(() => {
              if (revoked) return Effect.fail(new RuntimePermitError({ reason: "revoked" }));
              observationRevision += 1;
              startAttempted = true;
              return Effect.void;
            });
            yield* commitStart;
            return yield* withCurrent(
              Effect.gen(function* () {
                yield* assertCurrent;
                yield* Effect.suspend(() => {
                  if (revoked) return Effect.fail(new RuntimePermitError({ reason: "revoked" }));
                  observationRevision += 1;
                  invocation = "uncertain";
                  return Effect.void;
                });
                const result = yield* invoke;
                yield* Effect.sync(() => {
                  observationRevision += 1;
                  invocation = "confirmed";
                });
                return result;
              }),
            );
          }).pipe(
            Effect.ensuring(
              Effect.sync(() => {
                if (ownsClaim) { observationRevision += 1; inFlight = false; }
              }),
            ),
          );
        }),
      revoke: Effect.sync(() => {
        observationRevision += 1;
        revoked = true;
      }),
      observe: Effect.gen(function* () {
        if (revoked && !startAttempted) return evidence("not_invoked", "complete");
        const revision = observationRevision;
        const observation = yield* observe;
        if (
          revision !== observationRevision ||
          observation.state === "unknown" ||
          observation.coverage !== "complete" ||
          identity.writerReferences.length === 0
        )
          return evidence("unknown", "partial");
        if (observation.state === "quiescent")
          return !inFlight && (revoked || claimed)
            ? evidence("quiescent", "complete")
            : evidence("unknown", "partial");
        if (invocation !== "confirmed") return evidence("unknown", "partial");
        const current = yield* assertCurrent.pipe(
          Effect.as(true),
          Effect.catchCause(() => Effect.succeed(false)),
        );
        return current && revision === observationRevision ? evidence("active", "complete") : evidence("unknown", "partial");
      }).pipe(Effect.catchCause(() => Effect.succeed(evidence("unknown", "partial")))),
    }),
  };
}
