import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import {
  revalidateRuntimeOperationStartAdmission,
  type RuntimeOperationIdentity,
  type RuntimePermitAvailability,
} from "../runtime/RuntimeOperationPermit.ts";
import {
  associationId,
  canonical,
  CheckoutError,
  type Binding,
  type Evidence,
  type Target,
} from "./CheckoutProtocol.ts";
import { CheckoutExecutorAuthority, OrdinaryCheckout } from "./OrdinaryCheckout.ts";

const attached = new WeakSet<object>();

/**
 * The actual issuer transfers exclusive use of a fresh permit to this adapter.
 * Currentness alone cannot prove freshness or ownership of a reused runtime.
 * No application layer installs this adapter or manufactures an issuer.
 */
export const makeCheckoutPermitAdapter = <A, E>(
  binding: Binding,
  expected: RuntimeOperationIdentity,
  availability: RuntimePermitAvailability<A, E>,
) =>
  Effect.gen(function* () {
    if (availability.status === "unavailable") return availability;
    const permit = availability.permit;
    const captured: Binding = Object.freeze({
      ...binding,
      source: Object.freeze({ ...binding.source }),
      relation: Object.freeze({ ...binding.relation }),
      provider: binding.provider === null ? null : Object.freeze({ ...binding.provider }),
    });
    const identity = Object.freeze({
      ...expected,
      binding: Object.freeze({ ...expected.binding }),
      writerReferences: Object.freeze([...expected.writerReferences]),
    });
    const bindingKey = canonical(captured);
    const identityKey = canonical(identity);
    const matchesIdentity = () => canonical(permit.identity) === identityKey;
    const provider = captured.provider;
    if (
      provider === null ||
      captured.issuerId !== identity.issuerId ||
      captured.operationId !== identity.operationId ||
      captured.admissionId !== identity.admissionId ||
      associationId(captured) !== identity.participantId ||
      provider.instanceId !== identity.binding.providerInstanceId ||
      provider.sessionId !== identity.binding.providerSessionId ||
      provider.threadId !== identity.binding.threadId ||
      provider.driver !== identity.binding.driver ||
      provider.generation !== identity.binding.runtimeGeneration ||
      provider.evidenceRevision !== identity.evidenceRevision ||
      !matchesIdentity() ||
      attached.has(permit)
    )
      return yield* new CheckoutError({ reason: "identity" });
    const current = revalidateRuntimeOperationStartAdmission(permit, identity, Effect.void).pipe(
      Effect.mapError((cause) => new CheckoutError({ reason: "unavailable", cause })),
    );
    yield* current;
    if (attached.has(permit)) return yield* new CheckoutError({ reason: "identity" });
    attached.add(permit);
    let attempted = false;
    const authorized = (now: string): Evidence => ({
      version: 1,
      issuerId: identity.issuerId,
      reference: identity.evidenceReference,
      outcome: "authorized",
      coverage: "unknown",
      observedAt: now,
    });
    const observe = (now: string) =>
      Effect.gen(function* () {
        if (!matchesIdentity()) return yield* new CheckoutError({ reason: "identity" });
        const observed = yield* permit.observe;
        if (canonical(observed.identity) !== identityKey)
          return yield* new CheckoutError({ reason: "identity" });
        return {
          version: 1 as const,
          issuerId: identity.issuerId,
          reference: identity.evidenceReference,
          outcome: observed.outcome,
          coverage: observed.coverage,
          observedAt: now,
        } satisfies Evidence;
      });
    // These services never escape the single repository call they authorize.
    const authority = (issued: Evidence, admission: () => boolean) =>
      CheckoutExecutorAuthority.of({
        assertCurrent: (candidate) =>
          canonical(candidate) === bindingKey && matchesIdentity()
            ? current
            : Effect.fail(new CheckoutError({ reason: "identity" })),
        reconcile: (candidate, evidence) =>
          Effect.gen(function* () {
            if (
              canonical(candidate) !== bindingKey ||
              canonical(evidence) !== canonical(issued) ||
              !matchesIdentity()
            )
              return yield* new CheckoutError({ reason: "identity" });
            if (evidence.outcome === "authorized") {
              if (!admission() || evidence.coverage !== "unknown")
                return yield* new CheckoutError({ reason: "identity" });
              yield* current;
              if (!admission()) return yield* new CheckoutError({ reason: "stale" });
            } else if (canonical(yield* observe(evidence.observedAt)) !== canonical(evidence)) {
              return yield* new CheckoutError({ reason: "stale" });
            }
          }),
      });
    const reserve = (now: string) =>
      Effect.gen(function* () {
        if (attempted) return yield* new CheckoutError({ reason: "stale" });
        const checkout = yield* OrdinaryCheckout;
        const evidence = authorized(now);
        let open = true;
        return yield* checkout
          .reserve({
            admissionId: captured.admissionId,
            source: captured.source,
            binding: captured,
            now,
            evidence,
          })
          .pipe(
            Effect.provideService(
              CheckoutExecutorAuthority,
              authority(evidence, () => open && !attempted),
            ),
            Effect.ensuring(
              Effect.sync(() => {
                open = false;
              }),
            ),
          );
      });
    const invoke = (
      kind: "start" | "join",
      input: { readonly target: Target; readonly expectedRevision: number; readonly now: string },
    ) =>
      Effect.suspend(() => {
        if (attempted) return Effect.fail(new CheckoutError({ reason: "stale" }));
        attempted = true;
        const evidence = authorized(input.now);
        const commit = Effect.suspend(() => {
          let open = true;
          return Effect.gen(function* () {
            const checkout = yield* OrdinaryCheckout;
            yield* checkout[kind]({
              binding: captured,
              evidence,
              target: input.target,
              expectedRevision: input.expectedRevision,
            }).pipe(
              Effect.provideService(
                CheckoutExecutorAuthority,
                authority(evidence, () => open),
              ),
            );
          }).pipe(
            Effect.ensuring(
              Effect.sync(() => {
                open = false;
              }),
            ),
          );
        });
        return permit
          .invoke(commit)
          .pipe(
            Effect.onExit((exit) =>
              Exit.isFailure(exit)
                ? permit.revoke.pipe(Effect.andThen(permit.observe), Effect.asVoid)
                : Effect.void,
            ),
          );
      });
    return {
      status: "available" as const,
      reserve,
      start: (input: {
        readonly target: Target;
        readonly expectedRevision: number;
        readonly now: string;
      }) => invoke("start", input),
      join: (input: {
        readonly target: Target;
        readonly expectedRevision: number;
        readonly now: string;
      }) => invoke("join", input),
      observe,
      revoke: permit.revoke,
      reconcile: (expectedRevision: number, now: string) =>
        Effect.gen(function* () {
          const checkout = yield* OrdinaryCheckout;
          const evidence = yield* observe(now);
          return yield* checkout
            .reconcileEvidence({ binding: captured, evidence, expectedRevision })
            .pipe(
              Effect.provideService(
                CheckoutExecutorAuthority,
                authority(evidence, () => false),
              ),
            );
        }),
      renew: (input: {
        readonly target: Target;
        readonly expectedRevision: number;
        readonly expectedOwnerRevision: number;
        readonly expiresAt: string;
        readonly now: string;
      }) =>
        Effect.gen(function* () {
          const checkout = yield* OrdinaryCheckout;
          const evidence = yield* observe(input.now);
          return yield* checkout
            .renew({
              binding: captured,
              evidence,
              target: input.target,
              expectedRevision: input.expectedRevision,
              expectedOwnerRevision: input.expectedOwnerRevision,
              expiresAt: input.expiresAt,
            })
            .pipe(
              Effect.provideService(
                CheckoutExecutorAuthority,
                authority(evidence, () => false),
              ),
            );
        }),
    };
  });
