import * as Effect from "effect/Effect";
import * as Digest from "effect/crypto/Digest";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as SqlClient from "effect/sql/SqlClient";
import * as Mcp from "../../../../mcp/McpInvocationContext.ts";
import * as Current from "../runtime/currentAuthority.ts";
import * as Pins from "../runtime/peerSessionPins.ts";
import * as Session from "../registry/session.ts";
import { NativeHome } from "../mode/authorityMode.ts";
import * as Keys from "../crypto/keyProvider.ts";
import * as Supplied from "./suppliedEvidence.ts";
import * as Boundary from "./peerWriteAdmission.ts";
import * as Home from "./homeTools.ts";
import * as Initial from "./peerEffectAdmission.ts";
import * as SourceQualification from "./sourceForwardQualification.ts";

/** The callback cannot escape the original registered invocation or current-owner scope. */
export const withPreparedReceivingAuthority = Effect.fn("ReceivingAuthority.withPrepared")(
  function* <A, E, R>(
    invocation: Mcp.McpInvocationScope,
    use: (fence: Boundary.ReceivingAuthorityFence["Service"]) => Effect.Effect<A, E, R>,
  ) {
    const sql = yield* SqlClient.SqlClient;
    if (Option.isSome(yield* Effect.serviceOption(sql.transactionService)))
      return yield* new Session.RegistryError({ code: "unavailable" });
    const current = yield* Current.CurrentAuthority;
    const native = yield* Pins.NativePeerSessions;
    return yield* current.withPrepared((prepared) =>
      Effect.gen(function* () {
        const registration = yield* native
          .current(invocation)
          .pipe(Effect.mapError(Boundary.mapFailure));
        const checkCurrent = Effect.fn("ReceivingAuthority.checkCurrent")(function* () {
          const selectedInvocation = yield* Effect.serviceOption(Mcp.McpInvocationContext);
          if (Option.isNone(selectedInvocation) || selectedInvocation.value !== invocation)
            return yield* new Session.RegistryError({ code: "not_granted" });
          yield* Current.checkPreparedAuthority(prepared).pipe(
            Effect.mapError(Boundary.mapFailure),
          );
          // Keep the ORIGINAL registration closure: re-looking up an equal replacement is insufficient.
          yield* registration.checkCurrent().pipe(Effect.mapError(Boundary.mapFailure));
        });
        const fence = Boundary.ReceivingAuthorityFence.of({
          prepare: Effect.fn("Boundary.ReceivingAuthorityFence.prepare")((exactInvocation) =>
            exactInvocation !== invocation
              ? Effect.fail(new Session.RegistryError({ code: "not_granted" }))
              : checkCurrent().pipe(Effect.as({ authority: prepared.authority, checkCurrent })),
          ),
        });
        return yield* use(fence);
      }).pipe(Effect.provideService(Mcp.McpInvocationContext, invocation)),
    );
  },
  (effect) => effect.pipe(Effect.mapError(Boundary.mapFailure)),
);

/** Construct HomeTools fresh inside the complete original invocation's current-owner scope. */
export const layer = Layer.effect(
  Home.HomeTools,
  Effect.gen(function* () {
    const current = yield* Current.CurrentAuthority;
    const native = yield* Pins.NativePeerSessions;
    const sql = yield* SqlClient.SqlClient;
    const home = yield* NativeHome;
    const keys = yield* Keys.WorkstreamsKeyProvider;
    const crypto = yield* Digest.Digest;
    const source = yield* Supplied.SourceForwardQualification;
    const local = yield* Session.Session;
    const initial = yield* Effect.serviceOption(Initial.InitialPeerExecution);
    const invoke = <A, E, R>(
      invocation: Mcp.McpInvocationScope,
      use: (tools: Home.HomeTools["Service"]) => Effect.Effect<A, E, R>,
    ) =>
      withPreparedReceivingAuthority(invocation, (fence) =>
        Effect.gen(function* () {
          const tools = yield* Home.HomeTools;
          return yield* SourceQualification.withQualificationScope(invocation, use(tools)).pipe(
            Effect.provideService(Pins.NativePeerSessions, native),
            Effect.provideService(SqlClient.SqlClient, sql),
          );
        }).pipe(
          Effect.provide(
            Home.layer.pipe(
              Layer.provide(
                Layer.mergeAll(
                  Layer.succeed(Boundary.ReceivingAuthorityFence, fence),
                  Layer.succeed(Pins.NativePeerSessions, native),
                  Layer.succeed(SqlClient.SqlClient, sql),
                  Layer.succeed(NativeHome, home),
                  Layer.succeed(Keys.WorkstreamsKeyProvider, keys),
                  Layer.succeed(Digest.Digest, crypto),
                  Layer.succeed(Supplied.SourceForwardQualification, source),
                  Layer.succeed(Session.Session, local),
                  Option.isSome(initial)
                    ? Layer.succeed(Initial.InitialPeerExecution, initial.value)
                    : Layer.empty,
                ),
              ),
            ),
          ),
        ),
      ).pipe(
        Effect.provideService(Current.CurrentAuthority, current),
        Effect.provideService(Pins.NativePeerSessions, native),
        Effect.provideService(SqlClient.SqlClient, sql),
      );
    return Home.HomeTools.of({
      access: Effect.fn("Home.HomeTools.access")((input, invocation) =>
        invoke(invocation, (tools) => tools.access(input, invocation)),
      ),
      registration: Effect.fn("Home.HomeTools.registration")((input, invocation) =>
        invoke(invocation, (tools) => tools.registration(input, invocation)),
      ),
      forward: Effect.fn("Home.HomeTools.forward")((input, invocation) =>
        invoke(invocation, (tools) => tools.forward(input, invocation)),
      ),
      readback: Effect.fn("Home.HomeTools.readback")((input, invocation) =>
        invoke(invocation, (tools) => tools.readback(input, invocation)),
      ),
    });
  }),
);
