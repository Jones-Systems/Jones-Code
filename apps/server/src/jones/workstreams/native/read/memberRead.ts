import {
  WorkstreamsNativeHomeRead,
  WorkstreamsNativeReadEnvelope,
} from "@t3tools/contracts/jones/workstreamsNativeRead";
import { WorkstreamsNativeHomeTarget } from "@t3tools/contracts/jones/workstreamsNative";
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";
import { RegistryError } from "../registry/session.ts";
import { readFailure } from "./projection.ts";
import { canonicalJson } from "../domain/validation.ts";
import { validateReadResult, type ReadResult } from "./routes.ts";

/** J3 owns real admission, pinned destination lookup and the single-hop transport. */
export class MemberReadAdmission extends Context.Service<
  MemberReadAdmission,
  {
    readonly current: () => Effect.Effect<
      {
        readonly expected_home: WorkstreamsNativeHomeTarget;
        readonly principal_id: string;
        readonly authorization_revision: number;
        readonly check: () => Effect.Effect<void, RegistryError>;
      },
      RegistryError
    >;
  }
>()("t3/jones/workstreams/native/read/memberRead/MemberReadAdmission") {}
export class HomeReadPort extends Context.Service<
  HomeReadPort,
  {
    readonly read: (
      home: WorkstreamsNativeHomeTarget,
      request: WorkstreamsNativeHomeRead,
    ) => Effect.Effect<ReadResult, RegistryError>;
  }
>()("t3/jones/workstreams/native/read/memberRead/HomeReadPort") {}
export class MemberReads extends Context.Service<
  MemberReads,
  {
    readonly read: (request: WorkstreamsNativeHomeRead) => Effect.Effect<ReadResult, RegistryError>;
  }
>()("t3/jones/workstreams/native/read/memberRead/MemberReads") {}

const decodeRequest = Schema.decodeUnknownEffect(WorkstreamsNativeHomeRead);
const decodeEnvelope = Schema.decodeUnknownEffect(WorkstreamsNativeReadEnvelope);
const decodeHome = Schema.decodeUnknownEffect(WorkstreamsNativeHomeTarget);
export const layer = Layer.effect(
  MemberReads,
  Effect.gen(function* () {
    const admission = yield* Effect.serviceOption(MemberReadAdmission);
    const port = yield* Effect.serviceOption(HomeReadPort);
    return MemberReads.of({
      read: Effect.fn("NativeRead.member")(function* (input) {
        const request = yield* decodeRequest(input).pipe(Effect.mapError(readFailure));
        if (Option.isNone(admission) || Option.isNone(port))
          return yield* new RegistryError({ code: "unavailable" });
        const captured = yield* admission.value.current();
        const target = yield* decodeHome(captured.expected_home).pipe(Effect.mapError(readFailure));
        yield* captured.check();
        const result = yield* port.value.read(target, request);
        const envelope = yield* decodeEnvelope(result.envelope).pipe(Effect.mapError(readFailure));
        const current = yield* admission.value.current();
        yield* current.check();
        yield* captured.check();
        if (
          current.principal_id !== captured.principal_id ||
          current.authorization_revision !== captured.authorization_revision ||
          result.authorization_revision !== current.authorization_revision
        )
          return yield* new RegistryError({ code: "forbidden" });
        if (
          canonicalJson(current.expected_home) !== canonicalJson(target) ||
          envelope.registry.registry_instance_id !== target.registry_instance_id ||
          envelope.registry.owner_id !== target.owner_id ||
          envelope.provenance.home_environment_id !== target.home_environment_id
        )
          return yield* new RegistryError({ code: "registry_mismatch" });
        if (envelope.registry.server_generation !== target.server_generation)
          return yield* new RegistryError({ code: "generation_conflict" });
        if (result.route !== request.route)
          return yield* new RegistryError({ code: "invalid_response" });
        // Projection consumers can display honest cache data. Closed v1 reads require a live home page.
        if (
          request.route !== "read-projection" &&
          (envelope.provenance.source !== "live" ||
            envelope.provenance.stale ||
            envelope.provenance.authority !== "native-home" ||
            envelope.coverage.registry !== "complete")
        )
          return yield* new RegistryError({ code: "unavailable" });
        const validated = yield* validateReadResult(request, { ...result, envelope });
        if (
          typeof validated.value === "object" &&
          validated.value !== null &&
          "principal_id" in validated.value &&
          validated.value.principal_id !== current.principal_id
        )
          return yield* new RegistryError({ code: "forbidden" });
        return validated;
      }),
    });
  }),
);
