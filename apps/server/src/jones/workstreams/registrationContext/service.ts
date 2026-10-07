import packageJson from "../../../../package.json" with { type: "json" };
import type { T3WorkstreamBinding } from "@t3tools/contracts";
import {
  WORKSTREAMS_REGISTRATION_CONTEXT_MAX_JSON_DEPTH,
  WORKSTREAMS_REGISTRATION_CONTEXT_MAX_RESPONSE_BYTES,
  WorkstreamsRegistrationContextBuild,
  WorkstreamsRegistrationContextResponse,
} from "@t3tools/contracts";
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";

import type { NativeStoreAuthority } from "../../../environment/NativeStoreAuthority.ts";
import { NativeStoreAuthorityPersistenceError } from "../../../environment/nativeStoreAuthorityPersistence.ts";
import type { WorkstreamTransportError } from "../../../workstreams/WorkstreamGateway.ts";

export class RegistrationContextError extends Schema.TaggedError<RegistrationContextError>()(
  "RegistrationContextError",
  {
    reason: Schema.Literals([
      "unconfigured",
      "registry_unavailable",
      "invalid_response",
      "response_too_large",
      "response_too_deep",
      "binding_mismatch",
    ]),
  },
) {}

export type RegistrationContextBinding = Pick<
  T3WorkstreamBinding,
  "ownerId" | "principalId" | "authorizationRevision"
>;

export interface RegistrationContextPorts {
  readonly readRegistrationContext: Effect.Effect<string, WorkstreamTransportError>;
  readonly configuredBinding: RegistrationContextBinding | null;
  readonly authority: Pick<NativeStoreAuthority["Service"], "readCurrent">;
  readonly build?: Effect.Effect<Option.Option<WorkstreamsRegistrationContextBuild>>;
}

export type T3RegistrationAvailability =
  | { readonly state: "ready" }
  | {
      readonly state: "unavailable";
      readonly reason:
        | "descriptor_absent"
        | "build_unavailable"
        | "contract_mismatch"
        | "source_mismatch"
        | "authority_mismatch"
        | "generation_mismatch"
        | "authority_fenced"
        | "authority_unavailable";
    };

export interface QualifiedRegistrationContext {
  readonly context: WorkstreamsRegistrationContextResponse;
  readonly t3Availability: T3RegistrationAvailability;
}

export class WorkstreamsRegistrationContext extends Context.Service<
  WorkstreamsRegistrationContext,
  {
    readonly read: () => Effect.Effect<QualifiedRegistrationContext, RegistrationContextError>;
  }
>()("t3/jones/workstreams/registrationContext/service/WorkstreamsRegistrationContext") {}

const readRegistrationContextBuild = Schema.decodeUnknownEffect(
  WorkstreamsRegistrationContextBuild,
)((packageJson as { readonly jonesSource?: unknown }).jonesSource).pipe(Effect.option);

const withinDepth = (value: Schema.Json): boolean => {
  const pending = [{ value, depth: 0 }];
  while (pending.length > 0) {
    const current = pending.pop()!;
    if (current.depth > WORKSTREAMS_REGISTRATION_CONTEXT_MAX_JSON_DEPTH) return false;
    if (current.value !== null && typeof current.value === "object") {
      for (const child of Object.values(current.value)) {
        pending.push({ value: child, depth: current.depth + 1 });
      }
    }
  }
  return true;
};

const decodeResponse = Effect.fn("RegistrationContext.decodeResponse")(function* (text: string) {
  if (typeof text !== "string")
    return yield* new RegistrationContextError({ reason: "invalid_response" });
  if (
    text.length > WORKSTREAMS_REGISTRATION_CONTEXT_MAX_RESPONSE_BYTES ||
    new TextEncoder().encode(text).byteLength > WORKSTREAMS_REGISTRATION_CONTEXT_MAX_RESPONSE_BYTES
  ) {
    return yield* new RegistrationContextError({ reason: "response_too_large" });
  }
  const json = yield* Schema.decodeEffect(Schema.fromJsonString(Schema.Json))(text).pipe(
    Effect.mapError(() => new RegistrationContextError({ reason: "invalid_response" })),
  );
  if (!withinDepth(json))
    return yield* new RegistrationContextError({ reason: "response_too_deep" });
  return yield* Schema.decodeUnknownEffect(WorkstreamsRegistrationContextResponse)(json).pipe(
    Effect.mapError(() => new RegistrationContextError({ reason: "invalid_response" })),
  );
});

export const makeWorkstreamsRegistrationContext = (
  ports: RegistrationContextPorts,
): WorkstreamsRegistrationContext["Service"] => {
  const build = ports.build ?? readRegistrationContextBuild;
  const qualifyT3 = Effect.fn("RegistrationContext.qualifyT3")(function* (
    context: WorkstreamsRegistrationContextResponse,
  ): Effect.fn.Return<T3RegistrationAvailability> {
    const source = context.sources.find((entry) => entry.provider === "t3");
    if (source === undefined) return { state: "unavailable", reason: "descriptor_absent" };
    const actualBuild = yield* build;
    if (Option.isNone(actualBuild)) return { state: "unavailable", reason: "build_unavailable" };
    if (
      source.build.repository !== actualBuild.value.repository ||
      source.build.sha !== actualBuild.value.sha ||
      source.build.tree !== actualBuild.value.tree
    ) {
      return { state: "unavailable", reason: "contract_mismatch" };
    }
    const current = yield* ports.authority.readCurrent.pipe(
      Effect.map((tuple) => ({ state: "read" as const, tuple })),
      Effect.catch((error) =>
        Effect.succeed({
          state: "unavailable" as const,
          reason:
            error instanceof NativeStoreAuthorityPersistenceError && error.code === "fenced"
              ? ("authority_fenced" as const)
              : ("authority_unavailable" as const),
        }),
      ),
    );
    if (current.state === "unavailable") return current;
    if (source.source_instance_id !== current.tuple.environmentId) {
      return { state: "unavailable", reason: "source_mismatch" };
    }
    if (source.authority_namespace !== current.tuple.authorityNamespace) {
      return { state: "unavailable", reason: "authority_mismatch" };
    }
    if (source.store_generation !== current.tuple.storeGeneration) {
      return { state: "unavailable", reason: "generation_mismatch" };
    }
    return { state: "ready" };
  });

  const read = Effect.fn("RegistrationContext.read")(function* () {
    const binding = ports.configuredBinding;
    if (binding === null) return yield* new RegistrationContextError({ reason: "unconfigured" });
    const text = yield* ports.readRegistrationContext.pipe(
      Effect.mapError(() => new RegistrationContextError({ reason: "registry_unavailable" })),
    );
    const context = yield* decodeResponse(text);
    if (
      context.owner_id !== binding.ownerId ||
      context.principal_id !== binding.principalId ||
      context.authorization_revision !== binding.authorizationRevision
    ) {
      return yield* new RegistrationContextError({ reason: "binding_mismatch" });
    }
    const t3Availability = yield* qualifyT3(context);
    // Omission records local qualification failure, not absence from the registry.
    const qualified =
      t3Availability.state === "ready"
        ? context
        : { ...context, sources: context.sources.filter((source) => source.provider !== "t3") };
    return { context: qualified, t3Availability };
  });

  return WorkstreamsRegistrationContext.of({ read });
};
