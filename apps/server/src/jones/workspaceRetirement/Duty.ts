// @effect-diagnostics nodeBuiltinImport:off -- A run-local nonce distinguishes server holders; restart alone never proves a consumer absent.
import * as Schema from "effect/Schema";
import { PhysicalProcessObservation } from "@t3tools/contracts";
import { DutyHolderJson } from "./DurableCodecs.ts";
import { nativePid } from "./Reservation.ts";
import { reconcile } from "./ClaimReconciliation.ts";
import * as NodeCrypto from "node:crypto";
import * as Effect from "effect/Effect";
import * as Context from "effect/Context";
import * as Option from "effect/Option";
import * as SqlClient from "effect/sql/SqlClient";
import { Receipts, layer, RetirementError } from "./Receipts.ts";
import { captureNativeOwner, observeNativeOwner, type NativeDutyOwner } from "./NativeDutyOwner.ts";
import type { DutyIdentity } from "./DutyIdentity.ts";

export class NativeOwnerCapture extends Context.Reference<{
  readonly capture: (
    pid: number,
    expected?: { bootId: string; startTime: string },
  ) => Promise<NativeDutyOwner>;
}>("jones/workspaceRetirement/NativeOwnerCapture", {
  defaultValue: () => ({ capture: captureNativeOwner }),
}) {}

export class NativeOwnerObservation extends Context.Reference<{
  readonly observe: typeof observeNativeOwner;
}>("jones/workspaceRetirement/NativeOwnerObservation", {
  defaultValue: () => ({ observe: observeNativeOwner }),
}) {}

// Only actual duty identity is bound here; optional provenance/registry enrichment never gates ordinary work.
export const hooks = Effect.gen(function* () {
  const sql = yield* Effect.serviceOption(SqlClient.SqlClient);
  const store = Option.isSome(sql)
    ? yield* Receipts.pipe(
        Effect.provide(layer),
        Effect.provideService(SqlClient.SqlClient, sql.value),
      )
    : null;
  const nativeBindings = new WeakMap<
    object,
    (
      pid: number,
      expected?: { bootId: string; startTime: string },
    ) => Effect.Effect<void, RetirementError>
  >();
  const sessions = new Map<string, Effect.Effect<void, RetirementError>>();
  const identities = new WeakMap<object, DutyIdentity>();
  const nativeOwner = yield* NativeOwnerCapture;
  const observation = yield* NativeOwnerObservation;
  const owners = new WeakMap<object, NativeDutyOwner>();
  const parentExits = new WeakSet<object>();
  const claims = new WeakMap<
    object,
    { id: string; holder: string; worktree: string; kind: string; owner: string }
  >();
  const observeParentExit = Effect.fn("WorkspaceRetirement.observeTerminalParentExit")(function* (
    release: Effect.Effect<void, RetirementError>,
  ) {
    parentExits.add(release);
    const claim = claims.get(release);
    if (!claim || !store) return;
    const native = owners.get(release);
    const status =
      native === undefined
        ? "unknown"
        : yield* Effect.tryPromise({
            try: () => observation.observe(native),
            catch: () => "unknown" as const,
          }).pipe(Effect.catch(() => Effect.succeed("unknown" as const)));
    if (status === "verified_absent") {
      yield* store.reconcileClaim(claim.id, claim.holder, true);
      return;
    }
    yield* Effect.logDebug("Terminal duty retained after parent exit", {
      target: claim.worktree,
      incarnation: identities.get(release),
      kind: claim.kind,
      owner: claim.owner,
      claimId: claim.id,
      status,
      missingFact:
        native?.type === "unresolved"
          ? native.reason
          : "owned_native_session_absence_or_descendant_drain",
      nextAction:
        "Have this terminal owner observe its exact owned session absent or acknowledge actual descendant drain; then reconcile this target duty.",
    });
  });
  return {
    hold: Effect.fn("WorkspaceRetirement.holdDuty")(function* (
      worktree: string | null,
      kind: string,
      owner: string,
    ) {
      if (worktree === null || store === null) return Effect.void;
      const id = NodeCrypto.randomUUID();
      for (let attempt = 0; attempt <= 20; attempt++) {
        const holder = yield* Schema.encodeEffect(DutyHolderJson)({
          serverPid: nativePid,
          nonce: id,
          kind,
          owner,
        }).pipe(
          Effect.mapError(
            (cause) =>
              new RetirementError({
                target: worktree,
                reason: "claim_holder_encoding_failed",
                cause,
              }),
          ),
        );
        const result = yield* store
          .claim({ id, worktree, kind, owner, holder })
          .pipe(Effect.result);
        if (result._tag === "Success") {
          const release = store.releaseClaim(id);
          identities.set(release, result.success);
          claims.set(release, { id, holder, worktree, kind, owner });
          nativeBindings.set(release, (pid, expected) =>
            Effect.tryPromise({
              try: () => nativeOwner.capture(pid, expected),
              catch: (cause) =>
                new RetirementError({
                  target: worktree,
                  reason: "native_owner_capture_unavailable",
                  cause,
                }),
            }).pipe(
              Effect.flatMap((native) =>
                store.bindNativeOwner(id, holder, native).pipe(
                  Effect.andThen(Effect.sync(() => owners.set(release, native))),
                  Effect.andThen(
                    Effect.suspend(() =>
                      parentExits.has(release) ? observeParentExit(release) : Effect.void,
                    ),
                  ),
                ),
              ),
            ),
          );
          if (kind === "session") sessions.set(owner, release);
          return release;
        }
        if (result.failure.reason !== "retiring" || attempt === 20) return yield* result.failure;
        yield* Effect.sleep("250 millis");
      }
      return Effect.void;
    }),
    bindNative: (release: Effect.Effect<void, RetirementError>, pid: number) =>
      nativeBindings.get(release)?.(pid) ?? Effect.void,
    observeParentExit,
    claimed: (release: Effect.Effect<void, RetirementError>) => claims.has(release),
    identity: (release: Effect.Effect<void, RetirementError>) => identities.get(release),
    // Consume the observation already made by the provider's native lifecycle. No
    // extra diagnostics request, capture worker, provenance or start-path retry.
    bindSession: Effect.fn("WorkspaceRetirement.bindSessionOwner")(function* (
      owner: string,
      providerThreadId: string,
      generation: string,
    ) {
      if (Option.isNone(sql)) return;
      const release = sessions.get(owner);
      if (!release) return;
      const rows = yield* sql.value<{
        observation_json: string;
      }>`SELECT observation_json FROM jones_physical_process_evidence WHERE provider_thread_id=${providerThreadId} AND runtime_generation=${generation} ORDER BY rowid DESC LIMIT 1`;
      if (!rows[0]) return;
      const decoded = yield* Schema.decodeEffect(Schema.fromJsonString(PhysicalProcessObservation))(
        rows[0].observation_json,
      ).pipe(Effect.result);
      // Malformed historical evidence never establishes an owner or absence.
      if (decoded._tag === "Failure" || decoded.success.status !== "observed") return;
      const observation = decoded.success;
      yield* (
        nativeBindings.get(release)?.(observation.responder.pid, {
          bootId: observation.responder.bootId,
          startTime: observation.responder.startTime,
        }) ?? Effect.void
      );
    }),
  };
});

export const withDuty = Effect.fn("WorkspaceRetirement.withDuty")(function* <A, E, R>(
  worktree: string | null,
  kind: string,
  owner: string,
  effect: Effect.Effect<A, E, R>,
) {
  const duty = yield* hooks;
  const release = yield* duty.hold(worktree, kind, owner);
  return yield* effect.pipe(Effect.ensuring(release.pipe(Effect.orDie)));
});

export const startup = Effect.gen(function* () {
  const sql = yield* Effect.serviceOption(SqlClient.SqlClient);
  if (Option.isNone(sql)) return;
  const store = yield* Receipts.pipe(
    Effect.provide(layer),
    Effect.provideService(SqlClient.SqlClient, sql.value),
  );
  yield* reconcile(sql.value, store).pipe(
    Effect.catch((cause) =>
      Effect.logDebug("Workspace claim reconciliation unavailable", { cause }),
    ),
  );
});
