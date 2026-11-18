import { describe, expect, it } from "@effect/vitest";
import {
  AuthOrchestrationReadScope,
  AuthSourceControlWriteScope,
  ProjectId,
  WS_METHODS,
  WsRpcGroup,
} from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as RpcTest from "effect/rpc/RpcTest";

import * as RpcAuthorization from "../../auth/RpcAuthorization.ts";
import * as RepoFreshness from "../git/RepoFreshness.ts";
import * as RepoFreshnessWs from "./wsHandlers.ts";

const methods = [WS_METHODS.vcsGetRepoFreshness, WS_METHODS.vcsRefreshRepoFreshness] as const;
const group = WsRpcGroup.omit(
  ...Array.from(WsRpcGroup.requests.keys()).filter(
    (
      tag,
    ): tag is Exclude<
      keyof typeof RpcAuthorization.RPC_REQUIRED_SCOPES,
      (typeof methods)[number]
    > => !(methods as readonly string[]).includes(tag),
  ),
);
const projectId = ProjectId.make("repo-freshness-test");
const first = {
  remoteName: "origin",
  branchName: "main",
  commitSha: "b".repeat(40),
  checkedAtMs: 100,
};
const second = { ...first, commitSha: "e".repeat(40), checkedAtMs: 200 };

describe("repository freshness RPC authorization", () => {
  it.effect("restricts live checks to source-control writers and forwards the exact attempt", () =>
    Effect.gen(function* () {
      const calls: Array<readonly [string, ProjectId]> = [];
      const snapshot = (
        id: ProjectId,
        verification: typeof first,
      ): RepoFreshness.RepoFreshnessStatus => ({
        projectId: id,
        cadenceSeconds: 120,
        target: { status: "resolved", remoteName: verification.remoteName },
        latestVerified: verification,
        latestAttempt: { status: "succeeded", verification },
        freshness: "fresh",
      });
      const handlers = group.toLayer(RepoFreshnessWs.make()).pipe(
        Layer.provide(
          Layer.succeed(RepoFreshness.RepoFreshness, {
            status: (id) =>
              Effect.sync(() => {
                calls.push(["status", id]);
                return snapshot(id, first);
              }),
            checkForLaunch: () => Effect.die("RPC must use a caller-authorized checkout refresh"),
            refresh: (id) =>
              Effect.sync(() => {
                calls.push(["refresh", id]);
                return {
                  status: snapshot(id, second),
                  checkout: { status: "skipped", reason: "auto-pull-disabled" } as const,
                };
              }),
          }),
        ),
      );
      const reader = yield* RpcTest.makeClient(group).pipe(
        Effect.provide(Layer.merge(handlers, RpcAuthorization.layer([AuthOrchestrationReadScope]))),
      );
      expect(
        yield* reader[WS_METHODS.vcsRefreshRepoFreshness]({ projectId }).pipe(Effect.flip),
      ).toMatchObject({
        requiredPermission: AuthSourceControlWriteScope,
      });
      expect(calls).toEqual([]);
      expect(yield* reader[WS_METHODS.vcsGetRepoFreshness]({ projectId })).toMatchObject({
        projectId,
        latestVerified: first,
        sampledAtMs: expect.any(Number),
      });

      const writer = yield* RpcTest.makeClient(group).pipe(
        Effect.provide(
          Layer.merge(handlers, RpcAuthorization.layer([AuthSourceControlWriteScope])),
        ),
      );
      expect(
        yield* writer[WS_METHODS.vcsGetRepoFreshness]({ projectId }).pipe(Effect.flip),
      ).toMatchObject({
        requiredPermission: AuthOrchestrationReadScope,
      });
      expect(yield* writer[WS_METHODS.vcsRefreshRepoFreshness]({ projectId })).toMatchObject({
        status: { projectId, latestAttempt: { status: "succeeded", verification: second } },
        checkout: { status: "skipped", reason: "auto-pull-disabled" },
      });
      expect(calls).toEqual([
        ["status", projectId],
        ["refresh", projectId],
      ]);
    }),
  );
});
