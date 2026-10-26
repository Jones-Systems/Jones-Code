import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import type { WorkstreamCommand } from "@t3tools/contracts";
import { WORKSTREAM_OPERATIONS_CONTRACT } from "@t3tools/contracts/jones/workstreamOperations";
import * as DateTime from "effect/DateTime";
import type { WorkstreamOperationOrigin } from "@t3tools/contracts/jones/workstreamOperations";
import * as Gateway from "../../../../workstreams/WorkstreamGateway.ts";
import * as RegistrationContext from "../../registrationContext/service.ts";
import * as Local from "../registry/localTransport.ts";
import * as Registration from "../registry/registration.ts";
import * as HomeExecutor from "../operations/homeExecutor.ts";
import * as Durable from "../operations/durableEntries.ts";
import * as Operations from "../../operations/service.ts";
import { ExecutionContext } from "../../operations/ports.ts";
import * as Source from "../providers/localAttestation.ts";
import * as Resolver from "../providers/referenceResolver.ts";
import { NativeHome } from "../mode/authorityMode.ts";
import { canonicalJson } from "../../operations/encoding.ts";
import { nativeSetupStatus } from "../mode/setupStatus.ts";
import * as Admission from "./admission.ts";

type Run = <A, E, R>(
  permission: Admission.Permission,
  threadId: string | null,
  effect: (
    adapter: Local.NativeAdapter,
    registration: RegistrationContext.WorkstreamsRegistrationContext["Service"],
  ) => Effect.Effect<A, E, R>,
  origin?: WorkstreamOperationOrigin,
) => Effect.Effect<A, E | Source.ProviderError, Exclude<R, Admission.RequestCaller>>;

export class Entry extends Context.Service<
  Entry,
  {
    readonly run: Run;
    readonly gateway: Gateway.WorkstreamGateway["Service"];
    readonly registration: RegistrationContext.WorkstreamsRegistrationContext["Service"];
    readonly placementResponse: Local.NativeAdapter["placementResponse"];
    readonly lookupLegacyReceipt: Local.NativeAdapter["lookupLegacyReceipt"];
  }
>()("t3/jones/workstreams/native/runtime/entry") {}

const gatewayError = (cause: Source.ProviderError) =>
  new Gateway.WorkstreamGatewayError({
    reason: cause.code === "not_granted" ? "permission-denied" : "not-configured",
    detail: "Native Workstreams admission unavailable. See docs/operations/workstreams-native.md.",
    cause,
  });
export const layer = Layer.effect(
  Entry,
  Effect.gen(function* () {
    const home = yield* NativeHome;
    const boundary = yield* Admission.CallerBoundary,
      admission = yield* Admission.Admission,
      local = yield* Local.LocalTransport,
      registration = yield* Registration.Registration;
    const run: Run = Effect.fn("NativeEntry.run")(function* (permission, threadId, effect, origin) {
      const evidence = yield* boundary.current();
      if (origin && canonicalJson(origin) !== canonicalJson(evidence.origin))
        return yield* new Source.ProviderError({ code: "not_granted" });
      return yield* Effect.gen(function* () {
        const caller = yield* admission.admit(permission, threadId);
        return yield* effect(
          yield* local
            .forCaller(caller)
            .pipe(Effect.mapError(() => new Source.ProviderError({ code: "not_granted" }))),
          registration.forCaller(caller),
        );
      }).pipe(Effect.provideService(Admission.RequestCaller, evidence));
    });
    const read = <A>(
      f: (
        gateway: Gateway.WorkstreamGateway["Service"],
      ) => Effect.Effect<A, Gateway.WorkstreamGatewayError>,
    ) =>
      run("workstreams:read", null, (adapter) => f(adapter.gateway)).pipe(
        Effect.mapError((cause) =>
          cause._tag === "WorkstreamsProviderError" ? gatewayError(cause) : cause,
        ),
      );
    const write = <A>(
      f: (
        gateway: Gateway.WorkstreamGateway["Service"],
      ) => Effect.Effect<A, Gateway.WorkstreamGatewayError>,
    ) =>
      run("workstreams:write", null, (adapter) => f(adapter.gateway)).pipe(
        Effect.mapError((cause) =>
          cause._tag === "WorkstreamsProviderError" ? gatewayError(cause) : cause,
        ),
      );
    // Resolve thread-scoped raw commands through the admitted reference/membership before writing.
    const checkCommand = Effect.fn("NativeEntry.checkCommand")(function* (
      gateway: Gateway.WorkstreamGateway["Service"],
      command: WorkstreamCommand,
    ) {
      const action = command.action;
      const request = yield* Admission.RequestCaller;
      if (action.operation === "register_reference" && action.identity.provider === "t3") {
        if (action.identity.source_instance_id !== home.served_by_environment_id)
          return yield* gatewayError(new Source.ProviderError({ code: "not_granted" }));
        yield* request
          .check("workstreams:write", action.identity.native_id)
          .pipe(Effect.mapError(gatewayError));
        return;
      }
      let referenceId: string | null =
        "native_reference_id" in action ? action.native_reference_id : null;
      if ("membership_id" in action || "source_membership_id" in action) {
        const workstreamId =
          "workstream_id" in action ? action.workstream_id : action.source_workstream_id;
        const membershipId =
          "membership_id" in action ? action.membership_id : action.source_membership_id;
        let cursor: string | undefined;
        do {
          const page = yield* gateway.readMemberships(workstreamId, {
            limit: 100,
            ...(cursor ? { cursor } : {}),
          });
          const membership = page.items.find((item) => item.membership_id === membershipId);
          if (membership) {
            referenceId = membership.native_reference_id;
            break;
          }
          cursor = page.next_cursor ?? undefined;
        } while (cursor);
        if (referenceId === null) return;
      }
      if (referenceId) {
        const detail = yield* gateway.readReference(referenceId);
        if (
          detail.reference.identity.provider === "t3" &&
          detail.reference.identity.source_instance_id !== home.served_by_environment_id
        )
          return yield* gatewayError(new Source.ProviderError({ code: "not_granted" }));
        if (detail.reference.identity.provider === "t3")
          yield* request
            .check("workstreams:write", detail.reference.identity.native_id)
            .pipe(Effect.mapError(gatewayError));
      }
    });
    const submit = (command: WorkstreamCommand) =>
      run("workstreams:write", null, (adapter) =>
        Effect.gen(function* () {
          yield* checkCommand(adapter.gateway, command);
          return yield* adapter.gateway.submit(command);
        }),
      ).pipe(
        Effect.mapError((cause) =>
          cause._tag === "WorkstreamsProviderError" ? gatewayError(cause) : cause,
        ),
      );
    return Entry.of({
      run,
      gateway: {
        readAppearance: (ids) => read((g) => g.readAppearance(ids)),
        saveAppearance: (input) => write((g) => g.saveAppearance(input)),
        readThreadPlacements: (input) => read((g) => g.readThreadPlacements(input)),
        readRegistryCounts: () => read((g) => g.readRegistryCounts()),
        readSession: () => read((g) => g.readSession()),
        readMetadata: (input) => read((g) => g.readMetadata(input)),
        readDetail: (id) => read((g) => g.readDetail(id)),
        readReferences: (input) => read((g) => g.readReferences(input)),
        readReference: (id) => read((g) => g.readReference(id)),
        readMemberships: (id, input) => read((g) => g.readMemberships(id, input)),
        readDeclarations: (id, input) => read((g) => g.readDeclarations(id, input)),
        readEdges: (id, input) => read((g) => g.readEdges(id, input)),
        readHistory: (id, input) => read((g) => g.readHistory(id, input)),
        pollCommand: (id) => read((g) => g.pollCommand(id)),
        readReceipt: (id) => read((g) => g.readReceipt(id)),
        resubmit: (id, body) =>
          run("workstreams:write", null, (adapter) =>
            Effect.gen(function* () {
              // Decode only for admission; the gateway still submits the original retained bytes.
              const command = yield* decodeCommand(body);
              yield* checkCommand(adapter.gateway, command);
              return yield* adapter.gateway.resubmit(id, body);
            }),
          ).pipe(
            Effect.mapError((cause) =>
              cause._tag === "WorkstreamsProviderError" ? gatewayError(cause) : cause,
            ),
          ),
        submit,
        purgeAuthorization: () => {},
      },
      registration: {
        read: () =>
          run("workstreams:read", null, (_, value) => value.read()).pipe(
            Effect.mapError(
              () =>
                new RegistrationContext.RegistrationContextError({
                  reason: "registry_unavailable",
                }),
            ),
          ),
      },
      lookupLegacyReceipt: (input) =>
        run("workstreams:read", null, (adapter) => adapter.lookupLegacyReceipt(input)).pipe(
          Effect.mapError(() => new RegistryError({ code: "unavailable" })),
        ),
      placementResponse: (input, contract) =>
        run("workstreams:read", null, (adapter) => adapter.placementResponse(input, contract)).pipe(
          Effect.mapError(() => new RegistryError({ code: "unavailable" })),
        ),
    });
  }),
);
import * as Schema from "effect/Schema";
import { WorkstreamCommand as CommandSchema } from "@t3tools/contracts";
import { RegistryError } from "../registry/session.ts";
const decodeRawCommand = Schema.decodeUnknownEffect(Schema.fromJsonString(CommandSchema));
const decodeCommand = (body: string) =>
  decodeRawCommand(body).pipe(
    Effect.mapError(
      () =>
        new Gateway.WorkstreamGatewayError({
          reason: "invalid-response",
          detail: "Invalid Workstreams command.",
        }),
    ),
  );

export const gatewayLayer = Layer.effect(
  Gateway.WorkstreamGateway,
  Effect.map(Entry, (entry) => entry.gateway),
);
export const registrationLayer = Layer.effect(
  RegistrationContext.WorkstreamsRegistrationContext,
  Effect.map(Entry, (entry) => entry.registration),
);
export const homeTransportLayer = Layer.effect(
  HomeExecutor.HomeTransport,
  Effect.map(Entry, (entry) => ({
    lookup: (destination, id) =>
      entry
        .run("workstreams:read", null, (adapter) => adapter.homeTransport.lookup(destination, id))
        .pipe(
          Effect.mapError((cause) =>
            cause._tag === "WorkstreamsProviderError" ? gatewayError(cause) : cause,
          ),
        ),
    resubmit: (destination, id, body) =>
      entry
        .run("workstreams:write", null, (adapter) =>
          adapter.homeTransport.resubmit(destination, id, body),
        )
        .pipe(
          Effect.mapError((cause) =>
            cause._tag === "WorkstreamsProviderError" ? gatewayError(cause) : cause,
          ),
        ),
  })),
);
export const executionLayer = Layer.effect(
  ExecutionContext,
  Effect.gen(function* () {
    const entry = yield* Entry,
      executor = yield* HomeExecutor.HomeExecutor,
      entries = yield* Durable.DurableEntries;
    const run = <A>(
      permission: Admission.Permission,
      operation: Parameters<HomeExecutor.HomeExecutor["Service"]["assertSession"]>[0],
      f: (
        executor: HomeExecutor.HomeExecutor["Service"],
      ) => Effect.Effect<A, Gateway.WorkstreamGatewayError>,
    ) =>
      entry
        .run(
          permission,
          operation.thread_id,
          (adapter) => f(adapter.guardExecutor(executor)),
          operation.origin,
        )
        .pipe(
          Effect.mapError((cause) =>
            cause._tag === "WorkstreamsProviderError" ? gatewayError(cause) : cause,
          ),
        );
    return {
      mode: "native" as const,
      entries,
      executor: {
        assertSession: (operation, session) =>
          run("workstreams:read", operation, (value) => value.assertSession(operation, session)),
        readReceipt: (operation, id) =>
          run("workstreams:read", operation, (value) => value.readReceipt(operation, id)),
        dispatch: (operation, id, body) =>
          run("workstreams:write", operation, (value) => value.dispatch(operation, id, body)),
      },
    };
  }),
);

/** Wrap the one application operations service; its durable worker remains root-owned. */
export const operationsLayer = Layer.effect(
  Operations.WorkstreamOperations,
  Effect.gen(function* () {
    const entry = yield* Entry,
      operations = yield* Operations.WorkstreamOperations,
      home = yield* NativeHome,
      entries = yield* Durable.DurableEntries,
      admission = yield* Admission.Admission;
    const visible = Effect.fn("NativeEntry.visibleOperation")(function* (id: string) {
      const caller = yield* admission.admit("workstreams:read");
      const saved = yield* entries.read(id);
      const destination = saved?.operation.destination;
      return (
        !!saved &&
        !!destination &&
        canonicalJson(saved.request.origin) === canonicalJson(caller.origin) &&
        destination.registry_instance_id === home.registry_instance_id &&
        destination.owner_id === home.owner_id &&
        destination.home_environment_id === home.home_environment_id &&
        destination.home_generation === home.server_generation &&
        destination.principal_id === caller.principal_id &&
        destination.binding_version === caller.binding_version
      );
    });
    return Operations.WorkstreamOperations.of({
      submit: (input) =>
        entry
          .run(
            "workstreams:write",
            input.request.kind === "set_primary" ? input.request.thread.thread_id : null,
            () => operations.submit(input),
            input.request.origin,
          )
          .pipe(
            Effect.mapError((cause) =>
              cause._tag === "WorkstreamsProviderError"
                ? new Operations.OperationInternalError({})
                : cause,
            ),
          ),
      read: (input) =>
        entry
          .run("workstreams:read", "thread" in input ? input.thread.thread_id : null, () =>
            Effect.gen(function* () {
              if ("operation_id" in input && !(yield* visible(input.operation_id)))
                return { operation: null };
              const value = yield* operations.read(input);
              if ("operations" in value) {
                const allowed = yield* Effect.filter(value.operations, (operation) =>
                  visible(operation.operation_id),
                );
                return { operations: allowed };
              }
              if (
                "open_operation" in value &&
                value.open_operation &&
                !(yield* visible(value.open_operation.operation_id))
              )
                return { assignment: { kind: "unverified" as const }, open_operation: null };
              return value;
            }),
          )
          .pipe(
            Effect.catch(() =>
              Effect.succeed(
                "operation_id" in input
                  ? { operation: null }
                  : "open" in input
                    ? { operations: [] }
                    : { assignment: { kind: "unverified" as const }, open_operation: null },
              ),
            ),
          ),
      setupStatus: Effect.fn("NativeOperations.setupStatus")(function* () {
        const observed_at = DateTime.formatIso(yield* DateTime.now);
        const observed = yield* entry
          .run("workstreams:read", null, (adapter, registration) =>
            Effect.gen(function* () {
              const session = yield* adapter.gateway.readSession();
              const source = yield* registration.read().pipe(Effect.result);
              const placements = yield* adapter.gateway
                .readThreadPlacements({ identities: [] })
                .pipe(Effect.result);
              const pending = yield* entries.open();
              const allowed = yield* Effect.filter(pending, (saved) =>
                visible(saved.operation.operation_id),
              );
              return { session, source, placements, open: allowed.length };
            }),
          )
          .pipe(Effect.result);
        const ready = observed._tag === "Success" ? observed.success : null;
        return nativeSetupStatus({
          contract: WORKSTREAM_OPERATIONS_CONTRACT,
          environment_id: home.served_by_environment_id,
          observed_at,
          authority_effect: "none" as const,
          activation: { state: "enabled" as const, missing_keys: [] },
          registry: ready
            ? {
                state: "ready" as const,
                registry_id: home.registry_instance_id,
                owner_id: home.owner_id,
                can_write: ready.session.permissions.includes("workstreams:write"),
              }
            : {
                state: "unavailable" as const,
                registry_id: null,
                owner_id: null,
                can_write: false as const,
              },
          t3_source:
            ready?.source._tag === "Success" &&
            ready.source.success.t3Availability.state === "ready"
              ? { state: "ready" as const, reason: null }
              : {
                  state: "unavailable" as const,
                  reason:
                    ready?.source._tag === "Success" &&
                    ready.source.success.t3Availability.state === "unavailable"
                      ? ready.source.success.t3Availability.reason
                      : ("authority_unavailable" as const),
                },
          placements: {
            readiness:
              ready?.placements._tag === "Success"
                ? ready.placements.success.readiness
                : ("unavailable" as const),
          },
          operations: { open: ready?.open ?? 0 },
          next_steps: [],
        });
      }),
    });
  }),
);

export const referenceSourcesLayer = Layer.effect(
  Resolver.ReferenceSources,
  Effect.gen(function* () {
    const source = yield* Source.SourceAttestation,
      authorize = yield* Source.SourceAdmission,
      home = yield* NativeHome;
    return {
      github: null,
      selectT3: Effect.fn("NativeReferenceSources.selectT3")(function* (input) {
        if (input.identity.source_instance_id !== home.served_by_environment_id) return null;
        const binding = yield* authorize.authorize({
          expected_home: home,
          operation: "context",
          thread_id: input.identity.native_id,
        });
        if (binding.owner_id !== input.owner_id || binding.principal_id !== input.principal_id)
          return yield* new Source.ProviderError({ code: "not_granted" });
        return {
          expected_home: home,
          authority_namespace: binding.authority_namespace,
          store_generation: binding.store_generation,
          source,
        };
      }),
    };
  }),
);
