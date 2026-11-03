// @effect-diagnostics nodeBuiltinImport:off -- Public operation digests must match the unchanged canonical request.
import * as NodeCrypto from "node:crypto";
import * as Wire from "@t3tools/contracts/jones/workstreamOperations";
import * as Access from "@t3tools/contracts/jones/workstreamsHomeAccess";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as DateTime from "effect/DateTime";
import * as Schema from "effect/Schema";
import * as Environment from "../../../../environment/ServerEnvironment.ts";
import * as Public from "../../operations/service.ts";
import { canonicalJson } from "../../operations/encoding.ts";
import * as Recorded from "../runtime/recordedCaller.ts";
import * as Application from "../runtime/memberApplication.ts";
import { RegistryError } from "../registry/session.ts";

const decodeAccess = Schema.decodeUnknownEffect(Access.WorkstreamsHomeAccessResult, {
  onExcessProperty: "error",
});
const decodeInput = Schema.decodeUnknownEffect(Wire.WorkstreamOperationSubmitRequest, {
  onExcessProperty: "error",
});
const decodeRead = Schema.decodeUnknownEffect(Wire.WorkstreamOperationReadRequest, {
  onExcessProperty: "error",
});
const decodeResult = Schema.decodeUnknownEffect(Wire.WorkstreamOperationResult, {
  onExcessProperty: "error",
});
const decodeResponse = Schema.decodeUnknownEffect(Wire.WorkstreamOperationReadResponse, {
  onExcessProperty: "error",
});
const decodeStatus = Schema.decodeUnknownEffect(Wire.WorkstreamSetupStatus, {
  onExcessProperty: "error",
});
const invalid = () => new RegistryError({ code: "invalid_response" });
const accessKey = (value: Access.WorkstreamsHomeAccessResult, writing: boolean) => {
  const { observed_at: _time, ...rest } = value;
  if (!writing) return canonicalJson(rest);
  // A legitimate operation advances registry version. No other authority or ceiling may change.
  return canonicalJson({
    ...rest,
    home: { ...rest.home, registry_version: 0 },
    capabilities: {
      ...rest.capabilities,
      context: { ...rest.capabilities.context, registry_version: 0 },
    },
  });
};
const readFallback = (
  input: Wire.WorkstreamOperationReadRequest,
): Wire.WorkstreamOperationReadResponse =>
  "operation_id" in input
    ? { operation: null }
    : "open" in input
      ? { operations: [] }
      : { assignment: { kind: "unavailable", reason: "offline" }, open_operation: null };
const make = Effect.gen(function* () {
  const app = yield* Application.MemberApplication;
  const environment = yield* Environment.ServerEnvironmentIdentity;
  const environmentId = yield* environment.getEnvironmentId;
  const access = Effect.fn("MemberPublicOperations.access")(function* () {
    return yield* app.access().pipe(Effect.flatMap(decodeAccess), Effect.mapError(invalid));
  });
  const run = Effect.fn("MemberPublicOperations.run")(function* <A>(
    writing: boolean,
    use: (before: Access.WorkstreamsHomeAccessResult) => Effect.Effect<A, RegistryError>,
  ) {
    const before = yield* access();
    if (writing && !before.capabilities.permissions.includes("workstreams:write"))
      return yield* new RegistryError({ code: "forbidden" });
    const result = yield* use(before);
    const after = yield* access();
    if (
      accessKey(before, writing) !== accessKey(after, writing) ||
      after.home.registry_version < before.home.registry_version
    )
      return yield* new RegistryError({ code: "forbidden" });
    return result;
  });
  const unavailableStatus = Effect.fn("MemberPublicOperations.unavailableStatus")(function* () {
    return {
      contract: Wire.WORKSTREAM_OPERATIONS_CONTRACT,
      environment_id: environmentId,
      observed_at: DateTime.formatIso(yield* DateTime.now),
      authority_effect: "none",
      activation: { state: "invalid", missing_keys: [] },
      registry: { state: "unavailable", registry_id: null, owner_id: null, can_write: false },
      t3_source: { state: "unavailable", reason: "authority_fenced" },
      placements: { readiness: "unavailable" },
      operations: { open: 0 },
      next_steps: [],
    } satisfies Wire.WorkstreamSetupStatus;
  });
  return Public.WorkstreamOperations.of({
    submit: Effect.fn("MemberPublicOperations.submit")((input) =>
      run(true, (home) =>
        Effect.gen(function* () {
          const parsed = yield* decodeInput(input).pipe(Effect.mapError(invalid));
          const request = parsed.request;
          const registry = request.kind === "set_primary" ? request.target : request.registry;
          if (
            registry.registry_id !== home.home.registry_instance_id ||
            registry.owner_id !== home.home.owner_id
          )
            return yield* new RegistryError({ code: "registry_mismatch" });
          const result = yield* decodeResult(yield* app.submitOperation(parsed)).pipe(
            Effect.mapError(invalid),
          );
          if (
            result.operation_id !== request.operation_id ||
            result.kind !== request.kind ||
            canonicalJson(result.origin) !== canonicalJson(request.origin) ||
            result.request_sha256 !==
              NodeCrypto.createHash("sha256").update(canonicalJson(request)).digest("hex")
          )
            return yield* invalid();
          return result;
        }),
      ).pipe(
        Effect.mapError((error) =>
          error.code === "operation_id_conflict" || error.code === "thread_operation_in_progress"
            ? new Public.OperationConflict({ code: error.code })
            : new Public.OperationInternalError({}),
        ),
      ),
    ),
    read: Effect.fn("MemberPublicOperations.read")((input) =>
      run(false, () =>
        Effect.gen(function* () {
          const parsed = yield* decodeRead(input).pipe(Effect.mapError(invalid));
          let response: Wire.WorkstreamOperationReadResponse;
          if ("operation_id" in parsed) {
            const value = yield* app.readPublicOperation(parsed.operation_id);
            response = { operation: value?.result ?? null };
          } else {
            const values = yield* app.openPublicOperations();
            if ("open" in parsed) response = { operations: values.map((value) => value.result) };
            else {
              const matching = values.filter(
                (value) =>
                  value.request.kind === "set_primary" &&
                  canonicalJson(value.request.thread) === canonicalJson(parsed.thread),
              );
              if (matching.length > 1) return yield* invalid();
              response = {
                assignment: { kind: "unverified" },
                open_operation: matching[0]?.result ?? null,
              };
            }
          }
          return yield* decodeResponse(response).pipe(Effect.mapError(invalid));
        }),
      ).pipe(Effect.catch(() => Effect.succeed(readFallback(input)))),
    ),
    // Application-owned MemberRecovery handles retained lookup. Public resume cannot mint recovery authority.
    resumeRecorded: () => Effect.fail(new Recorded.RecordedCallerFence({ code: "missing" })),
    setupStatus: Effect.fn("MemberPublicOperations.setupStatus")(() =>
      run(false, (home) =>
        Effect.gen(function* () {
          const operations = yield* app.openPublicOperations();
          return yield* decodeStatus({
            contract: Wire.WORKSTREAM_OPERATIONS_CONTRACT,
            environment_id: environmentId,
            observed_at: DateTime.formatIso(yield* DateTime.now),
            authority_effect: "none",
            activation: { state: "enabled", missing_keys: [] },
            registry: {
              state: "ready",
              registry_id: home.home.registry_instance_id,
              owner_id: home.home.owner_id,
              can_write: home.capabilities.permissions.includes("workstreams:write"),
            },
            t3_source: { state: "ready", reason: null },
            placements: { readiness: "trust-provider-required" },
            operations: { open: operations.length },
            next_steps: [],
          }).pipe(Effect.mapError(invalid));
        }),
      ).pipe(Effect.catch(() => unavailableStatus())),
    ),
  });
});
/** Public-v1 operations over the selected member owner; no command-absence or redispatch capability. */
export const layer = Layer.effect(Public.WorkstreamOperations, make);
