// @effect-diagnostics nodeBuiltinImport:off -- Provider association uses the frozen canonical SHA-256 digest.
import * as NodeCrypto from "node:crypto";
import * as Wire from "@t3tools/contracts";
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Schema from "effect/Schema";
import * as Semaphore from "effect/Semaphore";
import * as Option from "effect/Option";
import { NativeHome } from "../mode/authorityMode.ts";
import * as Registry from "./service.ts";
import * as Session from "./session.ts";
import * as EffectStore from "../store/effectStore.ts";
import * as ArchiveStore from "../store/archiveStore.ts";
import * as Receipts from "../store/receiptStore.ts";
import * as Recorded from "../runtime/recordedCaller.ts";
import * as Recovery from "../runtime/recovery.ts";
import * as Resolver from "../providers/referenceResolver.ts";
import * as Settlement from "../providers/localSettlement.ts";
import * as GitHub from "../providers/githubPrObserver.ts";
import type { ExternalIntent, RegistryExternalResult } from "../domain/state.ts";
import { canonicalJson } from "../domain/validation.ts";
import {
  matchingPrCache,
  observationCache,
  nativeSettlementResult,
} from "../domain/externalIntents.ts";

export class EffectRunner extends Context.Service<
  EffectRunner,
  {
    readonly run: (
      intent: ExternalIntent,
    ) => Effect.Effect<Wire.WorkstreamReceipt | null, Session.RegistryError>;
    readonly recoverOne: (
      commandId: string,
    ) => Effect.Effect<Wire.WorkstreamReceipt | null, Session.RegistryError>;
    readonly recover: () => Effect.Effect<void, Session.RegistryError>;
  }
>()("t3/jones/workstreams/native/registry/effectRunner") {}

const decodeSettlementRequest = Schema.decodeUnknownEffect(Wire.WorkstreamsNativeSettlementRequest);
const make = Effect.gen(function* () {
  const home = yield* NativeHome;
  const scope = { registry_instance_id: home.registry_instance_id, owner_id: home.owner_id };
  const store = yield* EffectStore.EffectStore;
  const archive = yield* ArchiveStore.ArchiveStore;
  const registry = yield* Registry.Registry;
  const receipts = yield* Receipts.ReceiptStore;
  const recording = yield* Recorded.RecordedCallers;
  const resolver = yield* Resolver.ReferenceResolver;
  const settlement = yield* Settlement.SourceSettlement;
  const github = yield* GitHub.LinkedPrObserver;
  const lock = yield* Semaphore.make(1);
  const observe = Effect.fn("NativeEffects.observe")(function* (
    intent: ExternalIntent,
  ): Effect.fn.Return<RegistryExternalResult | null, Session.RegistryError> {
    if (intent.kind === "verify_reference") {
      const value = yield* resolver
        .resolve({
          owner_id: intent.owner_id,
          principal_id: intent.actor.principal_id,
          identity: intent.reference.identity,
          pr_locator: intent.reference.pr_locator,
        })
        .pipe(Effect.mapError((cause) => new Session.RegistryError({ code: cause.code, cause })));
      return {
        registration: {
          state: value.state,
          attestation_version: intent.reference.registration.attestation_version + 1,
          attested_at: value.state === "attested" ? value.attested_at : null,
          expires_at: value.state === "attested" ? value.expires_at : null,
          evidence: value.state === "attested" ? value.attestation : null,
        },
      };
    }
    if (intent.kind === "refresh_linked_pr") {
      if (!intent.reference.pr_locator)
        return yield* new Session.RegistryError({ code: "identity_conflict" });
      const cache = matchingPrCache(
        intent.previous_observation,
        intent.reference,
        intent.authorization.server_generation,
        intent.source_binding,
      );
      const result = yield* github
        .observe({
          source_instance_id: intent.reference.identity.source_instance_id,
          locator: intent.reference.pr_locator,
          previous_success: intent.previous_observation?.last_success ?? null,
          etag: cache?.etag ?? null,
        })
        .pipe(Effect.mapError((cause) => new Session.RegistryError({ code: cause.code, cause })));
      const observation = {
        native_reference_id: intent.reference.native_reference_id,
        observation_version: (intent.previous_observation?.observation_version ?? 0) + 1,
        attempted_at: result.observed_at,
        outcome: result.outcome,
        retry_after_seconds: result.retry_after_seconds,
        last_success: result.last_success,
        command_id: intent.command.command_id,
      };
      return {
        observation,
        observation_cache:
          result.etag && result.last_success
            ? observationCache(intent, observation, result.etag)
            : null,
      };
    }
    const action = intent.command.action;
    if (action.operation !== "request_native_t3_settlement")
      return yield* new Session.RegistryError({ code: "unknown_effect" });
    const request = yield* decodeSettlementRequest({
      owner_id: intent.owner_id,
      principal_id: intent.actor.principal_id,
      command_id: intent.command.command_id,
      native_reference_id: intent.reference.native_reference_id,
      native_action: action.native_action,
      request_sha256: intent.request_sha256,
      reservation_sha256: NodeCrypto.createHash("sha256")
        .update(canonicalJson(intent))
        .digest("hex"),
      identity: intent.reference.identity,
      expected_authority_namespace: intent.source_binding.authority_namespace,
      expected_store_generation: intent.source_binding.store_generation,
    }).pipe(
      Effect.mapError((cause) => new Session.RegistryError({ code: "unknown_effect", cause })),
    );
    const input = {
      expected_home: {
        registry_instance_id: home.registry_instance_id,
        owner_id: home.owner_id,
        home_environment_id: home.home_environment_id,
        server_generation: home.server_generation,
      },
      request,
    };
    // Source lookup is first; an explicit absent attempt permits dispatch of the identical persisted intent.
    const previous = yield* settlement
      .lookup(input)
      .pipe(Effect.mapError((cause) => new Session.RegistryError({ code: cause.code, cause })));
    const value =
      previous.state === "unknown" && previous.reason === "attempt_not_found"
        ? yield* settlement
            .settle(input)
            .pipe(
              Effect.mapError((cause) => new Session.RegistryError({ code: cause.code, cause })),
            )
        : previous;
    if (value.state !== "terminal") return null;
    if (canonicalJson(value.request) !== canonicalJson(request))
      return yield* new Session.RegistryError({ code: "unknown_effect" });
    const expected = nativeSettlementResult(intent, value.result.native_outcome);
    if (canonicalJson(value.result) !== canonicalJson(expected))
      return yield* new Session.RegistryError({ code: "unknown_effect" });
    return expected;
  });
  const runIntent = Effect.fn("NativeEffects.run")((intent: ExternalIntent, recovery: boolean) =>
    lock.withPermit(
      Effect.gen(function* () {
        const check = Effect.fn("NativeEffects.recheck")(function* () {
          if (!recovery) return;
          const evidence = yield* Effect.serviceOption(Recorded.RecoveryCaller);
          if (Option.isNone(evidence))
            return yield* new Session.RegistryError({ code: "not_granted" });
          yield* evidence.value
            .check(
              "workstreams:write",
              intent.reference.identity.provider === "t3"
                ? intent.reference.identity.native_id
                : null,
            )
            .pipe(
              Effect.mapError((cause) => new Session.RegistryError({ code: "not_granted", cause })),
            );
        });
        yield* check();
        // Imported evidence must use the fenced lookup-only legacy reconciler, before any provider lookup or dispatch.
        const imported = yield* archive
          .contains(scope, intent.command.command_id)
          .pipe(
            Effect.mapError((cause) => new Session.RegistryError({ code: "unavailable", cause })),
          );
        if (imported) return yield* new Session.RegistryError({ code: "unknown_effect" });
        const existing = yield* store
          .result(scope, intent.command.command_id)
          .pipe(
            Effect.mapError((cause) => new Session.RegistryError({ code: "unavailable", cause })),
          );
        const result = (existing as RegistryExternalResult | null) ?? (yield* observe(intent));
        if (!result) return null;
        // The provider result survives a failed/stale completion transaction.
        yield* store
          .saveResult(scope, intent.command.command_id, result)
          .pipe(
            Effect.mapError((cause) => new Session.RegistryError({ code: "unavailable", cause })),
          );
        yield* check();
        return yield* registry.complete(
          intent,
          result,
          recovery && intent.kind === "native_t3_settlement",
        );
      }),
    ),
  );
  const recoverOne = Effect.fn("NativeEffects.recoverOne")(function* (id: string) {
    const item = (yield* store
      .active(scope)
      .pipe(
        Effect.mapError((cause) => new Session.RegistryError({ code: "unavailable", cause })),
      )).find((value) => value.command_id === id);
    if (!item) return null;
    // Historical and imported effects lack native recorded recovery metadata.
    // They belong to the legacy reconciler; never attempt_not_found -> settle.
    if (!item.recovery) return yield* new Session.RegistryError({ code: "unknown_effect" });
    const intent = item.intent as ExternalIntent;
    const record = item.recovery.recorded_caller;
    const evidence = yield* recording
      .readmitRecorded(record)
      .pipe(Effect.mapError((cause) => new Session.RegistryError({ code: "not_granted", cause })));
    yield* evidence
      .check(
        "workstreams:write",
        intent.reference.identity.provider === "t3" ? intent.reference.identity.native_id : null,
      )
      .pipe(Effect.mapError((cause) => new Session.RegistryError({ code: "not_granted", cause })));
    // Re-admission precedes retained receipt lookup, provider lookup and dispatch.
    const receipt = yield* receipts
      .read(scope, id)
      .pipe(Effect.mapError((cause) => new Session.RegistryError({ code: "unavailable", cause })));
    if (receipt?.origin !== "native")
      return yield* new Session.RegistryError({ code: "unknown_effect" });
    return yield* Recorded.withRecordedCaller(record, evidence, runIntent(intent, true));
  });
  const recover = Effect.fn("NativeEffects.recover")(function* () {
    const active = yield* store
      .active(scope)
      .pipe(Effect.mapError((cause) => new Session.RegistryError({ code: "unavailable", cause })));
    for (const item of active) yield* recoverOne(item.command_id);
  });
  return EffectRunner.of({ run: (intent) => runIntent(intent, false), recoverOne, recover });
});
export const layer = Layer.effect(EffectRunner, make);

/** Application composition provides this worker port using the same runner and SQL. */
export const rawRecoveryLayer = Layer.effect(
  Recovery.RawRecovery,
  Effect.gen(function* () {
    const runner = yield* EffectRunner,
      store = yield* EffectStore.EffectStore;
    const receipts = yield* Receipts.ReceiptStore,
      home = yield* NativeHome;
    return Recovery.RawRecovery.of({
      pending: Effect.fn("NativeEffects.pendingRecorded")(function* () {
        const scope = { registry_instance_id: home.registry_instance_id, owner_id: home.owner_id };
        const items = yield* store
          .active(scope)
          .pipe(Effect.mapError(() => new Recovery.RecoveryError({ stage: "reservations" })));
        return yield* Effect.forEach(items, (item) =>
          Effect.gen(function* () {
            const receipt = yield* receipts
              .read(scope, item.command_id)
              .pipe(Effect.mapError(() => new Recovery.RecoveryError({ stage: "reservations" })));
            return {
              command_id: item.command_id,
              origin: receipt?.origin === "native" ? ("native" as const) : ("legacy_open" as const),
              recorded_caller: item.recovery?.recorded_caller ?? null,
              next_poll_at: null,
            };
          }),
        );
      }),
      recoverOne: (id) =>
        runner.recoverOne(id).pipe(
          Effect.asVoid,
          Effect.mapError(() => new Recorded.RecordedCallerFence({ code: "unavailable" })),
        ),
    });
  }),
);
