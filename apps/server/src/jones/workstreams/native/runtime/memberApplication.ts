import * as Context from "effect/Context";
import * as Crypto from "effect/Crypto";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as SqlClient from "effect/sql/SqlClient";
import * as Sessions from "../../../../persistence/AuthSessions.ts";
import * as Environment from "../../../../environment/ServerEnvironment.ts";
import * as Credentials from "../../../../mcp/McpSessionRegistry.ts";
import * as ProviderSessions from "../../../../orchestration-v2/ProviderSessionManager.ts";
import { ThreadManagementService } from "../../../../orchestration-v2/ThreadManagementService.ts";
import * as Links from "../../../../peer/PeerLinks.ts";
import * as Peer from "../../../../peer/PeerMcpClient.ts";
import * as Runtime from "../../runtimeIntegration/native.ts";
import * as Integration from "../integration.ts";
import * as Reads from "../read/memberRead.ts";
import * as Operations from "../operations/memberOperations.ts";
import * as Forwarder from "../forwarding/forwarder.ts";
import * as Transport from "../forwarding/memberTransport.ts";
import * as HomeClient from "../forwarding/homeClient.ts";
import * as Readmission from "../forwarding/readmissionV2.ts";
import * as SourceReadmission from "../forwarding/sourceReadmissionPort.ts";
import * as Store from "../store/operationStore.ts";
import { RegistryError } from "../registry/session.ts";
import * as Members from "./memberEnrollment.ts";
import * as Preparation from "./memberPreparation.ts";
import * as Pins from "./peerSessionPins.ts";
import * as Startup from "./startup.ts";

export class MemberApplication extends Context.Service<
  MemberApplication,
  {
    readonly read: Reads.MemberReads["Service"]["read"];
    readonly submitCommand: Transport.MemberWriteTransport["Service"]["submitCommand"];
    readonly saveAppearance: Transport.MemberWriteTransport["Service"]["saveAppearance"];
    readonly submitOperation: Operations.MemberOperations["Service"]["submit"];
    readonly submitOperationBody: Operations.MemberOperations["Service"]["submitBody"];
    readonly readOperation: Operations.MemberOperations["Service"]["read"];
    readonly openOperations: Operations.MemberOperations["Service"]["open"];
    readonly recoverOpen: Forwarder.MemberForwarder["Service"]["recoverOpen"];
    readonly lookupParent: SourceReadmission.SourceReadmission["Service"]["lookupParent"];
    readonly lookupRaw: SourceReadmission.SourceReadmission["Service"]["lookupRaw"];
  }
>()("t3/jones/workstreams/native/runtime/memberApplication") {}

type Inputs =
  | Startup.Startup
  | Environment.ServerEnvironmentIdentity
  | Sessions.AuthSessionRepository
  | Store.OperationStore
  | Crypto.Crypto
  | Peer.PeerMcpClient
  | Links.PeerLinks
  | Pins.PeerSessionPins
  | Runtime.NativeWorkstreamsRuntime
  | SqlClient.SqlClient
  | Forwarder.MemberForwardLocks
  | Members.MemberEnrollments
  | Reads.MemberReads;

const make = Effect.gen(function* () {
  const preparation = yield* Preparation.MemberPreparation;
  const application = Context.make(Startup.Startup, yield* Startup.Startup).pipe(
    Context.add(
      Environment.ServerEnvironmentIdentity,
      yield* Environment.ServerEnvironmentIdentity,
    ),
    Context.add(Sessions.AuthSessionRepository, yield* Sessions.AuthSessionRepository),
    Context.add(Store.OperationStore, yield* Store.OperationStore),
    Context.add(Crypto.Crypto, yield* Crypto.Crypto),
    Context.add(Peer.PeerMcpClient, yield* Peer.PeerMcpClient),
    Context.add(Links.PeerLinks, yield* Links.PeerLinks),
    Context.add(Pins.PeerSessionPins, yield* Pins.PeerSessionPins),
    Context.add(Runtime.NativeWorkstreamsRuntime, yield* Runtime.NativeWorkstreamsRuntime),
    Context.add(SqlClient.SqlClient, yield* SqlClient.SqlClient),
    Context.add(Forwarder.MemberForwardLocks, yield* Forwarder.MemberForwardLocks),
  );
  const threads = yield* Effect.serviceOption(ThreadManagementService);
  const providers = yield* Effect.serviceOption(ProviderSessions.ProviderSessionManagerV2);
  const credentials = yield* Effect.serviceOption(Credentials.McpSessionRegistry);
  const selected = Context.mergeAll(
    application,
    Option.isSome(threads) ? Context.make(ThreadManagementService, threads.value) : Context.empty(),
    Option.isSome(providers)
      ? Context.make(ProviderSessions.ProviderSessionManagerV2, providers.value)
      : Context.empty(),
    Option.isSome(credentials)
      ? Context.make(Credentials.McpSessionRegistry, credentials.value)
      : Context.empty(),
  );
  const sql = Context.get(application, SqlClient.SqlClient);
  const run = Effect.fn("MemberApplication.run")(function* <A, O>(
    requestLayer: Layer.Layer<O, RegistryError, Inputs>,
    effect: Effect.Effect<A, RegistryError, O>,
  ) {
    const suppliedSql = yield* Effect.serviceOption(SqlClient.SqlClient);
    if (Option.isSome(suppliedSql) && suppliedSql.value !== sql)
      return yield* new RegistryError({ code: "authority_fenced" });
    return yield* preparation
      .withPrepared((members) =>
        Effect.gen(function* () {
          // Only application resources are retained. Authentication comes from this call's original context.
          const prepared = Context.merge(yield* Effect.context<never>(), selected).pipe(
            Context.add(Members.MemberEnrollments, members),
          );
          // Target resolution acquires a home read only when needed, within this SAME owner scope.
          // Fresh submission, incoming readmission and recovery must not eagerly bootstrap a read caller.
          const context = Context.add(prepared, Reads.MemberReads, {
            read: (request, placement) =>
              Effect.flatMap(Reads.MemberReads, (reads) => reads.read(request, placement)).pipe(
                Effect.provide(
                  Layer.fresh(Integration.nativeMemberReadRequestLayer).pipe(
                    Layer.provide(Layer.succeedContext(prepared)),
                  ),
                ),
              ),
          });
          return yield* effect.pipe(
            Effect.provide(
              Layer.fresh(requestLayer).pipe(Layer.provide(Layer.succeedContext(context))),
            ),
          );
        }),
      )
      .pipe(Effect.provideService(SqlClient.SqlClient, sql));
  });
  const source = Layer.unwrap(
    Effect.sync(() =>
      Readmission.layer.pipe(Layer.provide(Integration.nativeMemberAdmissionRequestLayer)),
    ),
  );
  // Recovery consumes retained callers directly; it never runs the fresh-caller read bootstrap.
  const recovery = Layer.unwrap(
    Effect.sync(() =>
      Forwarder.layer.pipe(
        Layer.provide(HomeClient.layer),
        Layer.provide(Integration.nativeMemberAdmissionRequestLayer),
      ),
    ),
  );
  return MemberApplication.of({
    read: (request, placement) =>
      run(
        Integration.nativeMemberReadRequestLayer,
        Effect.flatMap(Reads.MemberReads, (reads) => reads.read(request, placement)),
      ),
    submitCommand: (input) =>
      run(
        Integration.nativeMemberForwardRequestLayer,
        Effect.flatMap(Transport.MemberWriteTransport, (transport) =>
          transport.submitCommand(input),
        ),
      ),
    saveAppearance: (input) =>
      run(
        Integration.nativeMemberForwardRequestLayer,
        Effect.flatMap(Transport.MemberWriteTransport, (transport) =>
          transport.saveAppearance(input),
        ),
      ),
    submitOperation: (input) =>
      run(
        Integration.nativeMemberForwardRequestLayer,
        Effect.flatMap(Operations.MemberOperations, (operations) => operations.submit(input)),
      ),
    submitOperationBody: (body) =>
      run(
        Integration.nativeMemberForwardRequestLayer,
        Effect.flatMap(Operations.MemberOperations, (operations) => operations.submitBody(body)),
      ),
    readOperation: (id) =>
      run(
        Integration.nativeMemberForwardRequestLayer,
        Effect.flatMap(Operations.MemberOperations, (operations) => operations.read(id)),
      ),
    openOperations: () =>
      run(
        Integration.nativeMemberForwardRequestLayer,
        Effect.flatMap(Operations.MemberOperations, (operations) => operations.open()),
      ),
    recoverOpen: () =>
      run(
        recovery,
        Effect.flatMap(Forwarder.MemberForwarder, (forwarder) => forwarder.recoverOpen()),
      ),
    lookupParent: (request, invocation) =>
      run(
        source,
        Effect.flatMap(SourceReadmission.SourceReadmission, (source) =>
          source.lookupParent(request, invocation),
        ),
      ),
    lookupRaw: (request, invocation) =>
      run(
        source,
        Effect.flatMap(SourceReadmission.SourceReadmission, (source) =>
          source.lookupRaw(request, invocation),
        ),
      ),
  });
});

/** @public Compose once with application-owned forward locks; every call gets fresh scoped member services. */
export const layer = Layer.effect(MemberApplication, make);
