// @effect-diagnostics nodeBuiltinImport:off -- Public operation digests must match the unchanged canonical request.
import * as NodeCrypto from "node:crypto";
import type { NativeReference } from "@t3tools/contracts";
import type { WorkstreamsNativeHomeRead } from "@t3tools/contracts/jones/workstreamsNativeRead";
import * as Wire from "@t3tools/contracts/jones/workstreamOperations";
import * as Access from "@t3tools/contracts/jones/workstreamsHomeAccess";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as DateTime from "effect/DateTime";
import * as Clock from "effect/Clock";
import * as Authority from "../../../../environment/NativeStoreAuthority.ts";
import * as Placements from "../read/memberPlacements.ts";
import { validateReadResult, type ReadResult } from "../read/routes.ts";
import * as Schema from "effect/Schema";
import * as Environment from "../../../../environment/ServerEnvironment.ts";
import * as Mcp from "../../../../mcp/McpInvocationContext.ts";
import * as Option from "effect/Option";
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
interface AssignmentObservation {
  readonly assignment: Wire.WorkstreamOperationAssignment;
  readonly expiresAt: string | null;
  readonly trustKey: string | null;
}
const unverified = (): AssignmentObservation => ({
  assignment: { kind: "unverified" },
  expiresAt: null,
  trustKey: null,
});
const timestampKey = (value: string) =>
  value.slice(0, 19) + value.slice(19, -1).replace(/^\./, "").padEnd(6, "0");
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
  const placements = yield* Placements.MemberPlacements;
  const authority = yield* Authority.NativeStoreAuthority;
  const clock = yield* Clock.Clock;
  const nowKey = () =>
    timestampKey(DateTime.formatIso(DateTime.makeUnsafe(clock.currentTimeMillisUnsafe())));
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
  const currentEnvelope = (
    before: Access.WorkstreamsHomeAccessResult,
    envelope: ReadResult["envelope"],
    revision: number,
  ) =>
    revision === before.authorization_revision &&
    canonicalJson({
      ...envelope.registry,
      home_environment_id: envelope.provenance.home_environment_id,
    }) === canonicalJson(before.home) &&
    envelope.provenance.authority === "native-home" &&
    envelope.provenance.source === "live" &&
    !envelope.provenance.stale &&
    envelope.coverage.registry === "complete" &&
    envelope.provenance.served_by_environment_id === before.home.home_environment_id &&
    timestampKey(envelope.provenance.observed_at) <= nowKey();
  const sourceCurrent = (envelope: ReadResult["envelope"]) =>
    envelope.coverage.sources.some(
      (source) =>
        source.source_instance_id === environmentId &&
        source.mapping === "mapped" &&
        source.attestation === "current" &&
        source.observed_at !== null &&
        timestampKey(source.observed_at) <= timestampKey(envelope.provenance.observed_at),
    );
  const readHome = Effect.fn("MemberPublicOperations.readHome")(function* (
    before: Access.WorkstreamsHomeAccessResult,
    request: WorkstreamsNativeHomeRead,
  ) {
    const result = yield* validateReadResult(request, yield* app.read(request));
    if (!currentEnvelope(before, result.envelope, result.authorization_revision))
      return yield* invalid();
    return result;
  });
  const assignment = Effect.fn("MemberPublicOperations.assignment")(function* (
    before: Access.WorkstreamsHomeAccessResult,
    thread: { readonly environment_id: string; readonly thread_id: string },
  ): Effect.fn.Return<AssignmentObservation, RegistryError> {
    if (thread.environment_id !== environmentId)
      return yield* new RegistryError({ code: "forbidden" });
    const { result, envelope } = yield* placements.observe(
      { identities: [{ source_instance_id: environmentId, native_thread_id: thread.thread_id }] },
      "workstreams-t3-placement/2.0.0",
    );
    if (!currentEnvelope(before, envelope, result.page.context.authorization_revision))
      return yield* invalid();
    const trusted = result.trustedEnvironments.filter(
      (value) => value.environmentId === environmentId,
    );
    if (result.readiness !== "ready" || trusted.length !== 1 || !sourceCurrent(envelope))
      return unverified();
    const local = trusted[0]!;
    if (
      result.page.items.some(
        (item) =>
          item.authority_namespace !== local.authorityNamespace ||
          item.store_generation !== local.storeGeneration,
      )
    )
      return unverified();
    const trustKey = canonicalJson({
      readiness: result.readiness,
      trustedEnvironments: result.trustedEnvironments,
    });
    const primary = result.page.items.find((item) => item.kind === "primary");
    if (primary) {
      const detail = yield* readHome(before, {
        route: "workstream",
        params: { workstream_id: primary.workstream_id },
      });
      if (
        detail.route !== "workstream" ||
        detail.value.workstream.owner_id !== before.home.owner_id
      )
        return yield* invalid();
      return {
        assignment: {
          kind: "primary",
          workstream_id: primary.workstream_id,
          name: detail.value.workstream.name,
        },
        expiresAt: primary.expires_at,
        trustKey,
      };
    }
    // Empty placement output also covers expired/unverified references; absence alone is not unassigned evidence.
    const matches: NativeReference[] = [];
    const seenIds = new Set<string>(),
      seenCursors = new Set<string>();
    let cursor: string | undefined,
      snapshot: string | undefined,
      bytes = 0,
      count = 0,
      complete = false;
    for (let page = 0; page < 1000; page++) {
      const reply = yield* readHome(before, {
        route: "references",
        params: { limit: 100, ...(cursor === undefined ? {} : { cursor }) },
      });
      if (reply.route !== "references") return yield* invalid();
      if (!sourceCurrent(reply.envelope)) return unverified();
      const current = canonicalJson({ envelope: reply.envelope, context: reply.value.context });
      if (snapshot !== undefined && current !== snapshot)
        return yield* new RegistryError({ code: "cursor_stale" });
      snapshot = current;
      bytes += new TextEncoder().encode(canonicalJson(reply)).byteLength;
      count += reply.value.items.length;
      if (bytes > 16_777_216 || count > 1000)
        return yield* new RegistryError({ code: "backpressure" });
      for (const item of reply.value.items) {
        if (seenIds.has(item.native_reference_id)) return yield* invalid();
        seenIds.add(item.native_reference_id);
        const identity = item.identity;
        if (
          identity.provider === "t3" &&
          identity.source_instance_id === environmentId &&
          identity.resource_kind === "thread" &&
          identity.id_kind === "internal" &&
          identity.native_id === thread.thread_id &&
          identity.account_provenance.kind === "not_account_scoped"
        )
          matches.push(item);
      }
      if (reply.value.next_cursor === null) {
        complete = true;
        break;
      }
      if (seenCursors.has(reply.value.next_cursor))
        return yield* new RegistryError({ code: "cursor_invalid" });
      seenCursors.add(reply.value.next_cursor);
      cursor = reply.value.next_cursor;
    }
    if (!complete) return yield* new RegistryError({ code: "backpressure" });
    if (matches.length > 1) return yield* invalid();
    const reference = matches[0],
      registration = reference?.registration,
      evidence = registration?.evidence;
    if (
      !reference ||
      reference.owner_id !== before.home.owner_id ||
      registration?.state !== "attested" ||
      !evidence ||
      registration.attested_at === null ||
      registration.expires_at === null ||
      timestampKey(registration.attested_at) > timestampKey(envelope.provenance.observed_at) ||
      timestampKey(registration.expires_at) <= nowKey() ||
      evidence.provider !== "t3" ||
      evidence.source_instance_id !== environmentId ||
      evidence.native_id !== thread.thread_id ||
      evidence.authority_namespace !== local.authorityNamespace ||
      evidence.store_generation !== local.storeGeneration
    )
      return unverified();
    return { assignment: { kind: "unassigned" }, expiresAt: registration.expires_at, trustKey };
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
      Effect.gen(function* () {
        const parsed = yield* decodeInput(input).pipe(Effect.mapError(invalid));
        const request = parsed.request;
        const perform = Effect.gen(function* () {
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
        });
        const invocation = yield* Effect.serviceOption(Mcp.McpInvocationContext);
        if (
          request.kind === "set_primary" &&
          Option.isSome(invocation) &&
          (invocation.value.client?.linked ||
            (invocation.value.environmentId === request.thread.environment_id &&
              invocation.value.thread?.threadId === request.thread.thread_id))
        )
          // Source admission and the exact receiving-home writer independently qualify this target.
          return yield* perform;
        return yield* run(true, (home) => {
          const registry = request.kind === "set_primary" ? request.target : request.registry;
          return registry.registry_id === home.home.registry_instance_id &&
            registry.owner_id === home.home.owner_id
            ? perform
            : Effect.fail(new RegistryError({ code: "registry_mismatch" }));
        });
      }).pipe(
        Effect.mapError((error) =>
          error.code === "operation_id_conflict" || error.code === "thread_operation_in_progress"
            ? new Public.OperationConflict({ code: error.code })
            : new Public.OperationInternalError({}),
        ),
      ),
    ),
    read: Effect.fn("MemberPublicOperations.read")((input) =>
      Effect.gen(function* () {
        let observed: AssignmentObservation | undefined;
        const response = yield* run(false, (before) =>
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
                observed = yield* assignment(before, parsed.thread);
                response = {
                  assignment: observed.assignment,
                  open_operation: matching[0]?.result ?? null,
                };
              }
            }
            return yield* decodeResponse(response).pipe(Effect.mapError(invalid));
          }),
        );
        if (observed?.trustKey && "assignment" in response) {
          const current = yield* Effect.try({
            try: () => authority.trustProvider.readTrustSnapshot(),
            catch: invalid,
          });
          if (
            canonicalJson(current) !== observed.trustKey ||
            (observed.expiresAt !== null && timestampKey(observed.expiresAt) <= nowKey())
          )
            return { ...response, assignment: { kind: "unverified" as const } };
        }
        return response;
      }).pipe(Effect.catch(() => Effect.succeed(readFallback(input)))),
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
export const layer = Layer.effect(Public.WorkstreamOperations, make).pipe(
  Layer.provide(Placements.layer),
);
