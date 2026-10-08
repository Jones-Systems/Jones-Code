import {
  AuthAccessWriteScope,
  AuthDiagnosticsReadScope,
  WS_METHODS,
  WsRpcGroup,
} from "@t3tools/contracts";
import { describe, expect, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as RpcTest from "effect/rpc/RpcTest";

import { RPC_REQUIRED_SCOPES } from "../auth/RpcAuthorization.ts";
import * as RpcAuthorization from "../auth/RpcAuthorization.ts";
import * as TokenAccountingService from "./TokenAccountingService.ts";

const method = WS_METHODS.serverReadTokenAccounting;
const group = WsRpcGroup.omit(
  ...[...WsRpcGroup.requests.keys()].filter(
    (tag): tag is Exclude<keyof typeof RPC_REQUIRED_SCOPES, typeof method> => tag !== method,
  ),
);
const handler = group.toLayerHandler(method, () =>
  Effect.flatMap(TokenAccountingService.TokenAccountingService, (service) => service.read),
);

describe("saved accounting RPC", () => {
  it.effect("returns the unconfigured result without enrolling a reader", () =>
    Effect.gen(function* () {
      const service = yield* TokenAccountingService.TokenAccountingService;
      expect(yield* service.isAvailable).toBe(false);
      const client = yield* RpcTest.makeClient(group).pipe(
        Effect.provide(Layer.mergeAll(handler, RpcAuthorization.layer([AuthDiagnosticsReadScope]))),
      );
      expect(yield* client[method]({})).toMatchObject({
        state: "unavailable",
        status: "unconfigured",
        reason: "reader_unconfigured",
        configuredReportId: null,
      });
    }).pipe(Effect.provide(TokenAccountingService.layer), Effect.scoped),
  );

  it.effect("rejects insufficient scope before invoking the reader", () =>
    Effect.gen(function* () {
      let reads = 0;
      const service = Layer.succeed(
        TokenAccountingService.TokenAccountingService,
        TokenAccountingService.TokenAccountingService.of({
          isAvailable: Effect.succeed(true),
          read: Effect.sync(() => {
            reads += 1;
            return {
              state: "unavailable" as const,
              status: "unconfigured" as const,
              reason: "reader_unconfigured" as const,
              configuredReportId: null,
              readAt: "2026-10-04T00:00:00.000Z",
            };
          }),
        }),
      );
      const client = yield* RpcTest.makeClient(group).pipe(
        Effect.provide(
          Layer.mergeAll(handler, RpcAuthorization.layer([AuthAccessWriteScope])).pipe(
            Layer.provide(service),
          ),
        ),
      );
      expect(yield* client[method]({}).pipe(Effect.flip)).toMatchObject({
        _tag: "EnvironmentAuthorizationError",
        requiredScope: AuthDiagnosticsReadScope,
      });
      expect(reads).toBe(0);
    }).pipe(Effect.scoped),
  );
});
