import { expect, it } from "@effect/vitest";
import {
  AuthSessionId,
  EnvironmentAuthenticatedPrincipal,
  ORCHESTRATION_V2_WS_METHODS as Methods,
  ThreadId,
} from "@t3tools/contracts";
import * as NodeSqliteClient from "@t3tools/shared/nodeSqliteClient";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as ThreadLaunch from "../../orchestration-v2/ThreadLaunchService.ts";
import * as ThreadManagement from "../../orchestration-v2/ThreadManagementService.ts";
import * as ServerEnvironment from "../../environment/ServerEnvironment.ts";
import * as Startup from "../../serverRuntimeStartup.ts";
import * as NativeAuthority from "../nativeCreation/NativeCreationAuthority.ts";
import { make } from "./retainedHandlers.ts";

const principal = EnvironmentAuthenticatedPrincipal.of({
  sessionId: AuthSessionId.make("synthetic-retained-ws"),
  subject: "synthetic",
  method: "bearer-access-token",
  scopes: new Set(["orchestration:operate"]),
});
const submission = {
  schema: "t3.native-bootstrap-submission/v1" as const,
  preparationBase64: "synthetic-unopened-preparation",
  creationGuard: {
    schema: "t3.native-creation-guard/v1" as const,
    grantId: "synthetic",
    grantRevision: 1,
  },
};
// The handlers capture a client for linked-thread reads; these cases never query it.
const database = NodeSqliteClient.layer({ filename: ":memory:" });

const fixture = (
  trace: string[],
  launch: Partial<ThreadLaunch.ThreadLaunchService["Service"]> = {},
) =>
  Layer.mergeAll(
    database,
    Layer.mock(ThreadLaunch.ThreadLaunchService)(launch),
    Layer.mock(ThreadManagement.ThreadManagementService)({}),
    Layer.mock(ServerEnvironment.ServerEnvironment)({}),
    Layer.mock(Startup.ServerRuntimeStartup)({
      enqueueCommand: (effect) =>
        Effect.andThen(
          Effect.sync(() => {
            trace.push("admitted");
          }),
          effect,
        ),
    }),
  );

it.effect("passes the exact authenticated principal and submission through startup admission", () =>
  Effect.gen(function* () {
    const trace: string[] = [];
    let observed: unknown;
    const handlers = yield* make(principal).pipe(
      Effect.provide(
        fixture(trace, {
          dispatchNativeBootstrap: (input) =>
            Effect.gen(function* () {
              trace.push("owner");
              observed = yield* EnvironmentAuthenticatedPrincipal;
              expect(input).toBe(submission);
              return yield* new NativeAuthority.NativeCreationAuthorityError({
                code: "stale_grant",
                message: "Synthetic revoked grant",
              });
            }),
        }),
      ),
    );
    const error = yield* handlers[Methods.dispatchNativeBootstrap](submission).pipe(Effect.flip);
    expect(error.code).toBe("stale_grant");
    expect(observed).toBe(principal);
    expect(trace).toEqual(["admitted", "owner"]);
  }).pipe(Effect.scoped),
);

it.effect(
  "target observation refuses an unavailable runtime owner without enqueueing a command",
  () =>
    Effect.gen(function* () {
      const trace: string[] = [];
      const handlers = yield* make(principal).pipe(Effect.provide(fixture(trace)));
      const error = yield* handlers[Methods.readCurrentRuntimeStopTarget]({
        threadId: ThreadId.make("synthetic-thread"),
      }).pipe(Effect.flip);
      expect(error.reason).toBe("captured_runtime_stop_unavailable");
      expect(trace).toEqual([]);
    }).pipe(Effect.scoped),
);
