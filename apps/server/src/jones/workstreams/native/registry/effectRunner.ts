// @effect-diagnostics nodeBuiltinImport:off -- Provider association uses the frozen canonical SHA-256 digest.
import * as NodeCrypto from "node:crypto";
import * as Wire from "@t3tools/contracts";
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Schema from "effect/Schema";
import * as Semaphore from "effect/Semaphore";
import { NativeHome } from "../mode/authorityMode.ts";
import * as Registry from "./service.ts";
import * as Session from "./session.ts";
import * as EffectStore from "../store/effectStore.ts";
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
    readonly recover: () => Effect.Effect<void, Session.RegistryError>;
  }
>()("t3/jones/workstreams/native/registry/effectRunner") {}

const decodeSettlementRequest = Schema.decodeUnknownEffect(Wire.WorkstreamsNativeSettlementRequest);
const make = Effect.gen(function* () {
  const home = yield* NativeHome;
  const scope = { registry_instance_id: home.registry_instance_id, owner_id: home.owner_id };
  const store = yield* EffectStore.EffectStore;
  const registry = yield* Registry.Registry;
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
        return yield* registry.complete(
          intent,
          result,
          recovery && intent.kind === "native_t3_settlement",
        );
      }),
    ),
  );
  const recover = Effect.fn("NativeEffects.recover")(function* () {
    const active = yield* store
      .active(scope)
      .pipe(Effect.mapError((cause) => new Session.RegistryError({ code: "unavailable", cause })));
    for (const item of active) yield* runIntent(item.intent as ExternalIntent, true);
  });
  return EffectRunner.of({ run: (intent) => runIntent(intent, false), recover });
});
export const layer = Layer.effect(EffectRunner, make);
