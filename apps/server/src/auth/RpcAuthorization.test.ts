import {
  AuthOrchestrationOperateScope,
  AuthSessionId,
  AuthOrchestrationReadScope,
  AuthRelayReadScope,
  AuthRelayWriteScope,
  WS_METHODS,
  ORCHESTRATION_V2_WS_METHODS,
  WsRpcGroup,
} from "@t3tools/contracts";
import { describe, expect, it } from "@effect/vitest";
import * as Effect from "effect/Effect";

import {
  RPC_REQUIRED_SCOPES,
  assertLegacyBootstrapAllowed,
  requiredScopeForRpcMethod,
  requiredScopeForDeviceList,
} from "./RpcAuthorization.ts";

describe("RPC authorization scopes", () => {
  it("declares exactly one scope for every RPC in the server group", () => {
    expect(new Set(Object.keys(RPC_REQUIRED_SCOPES))).toEqual(new Set(WsRpcGroup.requests.keys()));
  });

  it("authorizes background policy reporting and observation deliberately", () => {
    expect(requiredScopeForRpcMethod(WS_METHODS.serverReportClientActivity)).toBe(
      AuthOrchestrationReadScope,
    );
    expect(requiredScopeForRpcMethod(WS_METHODS.serverReportHostPowerState)).toBe(
      AuthOrchestrationOperateScope,
    );
    expect(requiredScopeForRpcMethod(WS_METHODS.serverGetBackgroundPolicy)).toBe(
      AuthOrchestrationReadScope,
    );
    expect(requiredScopeForRpcMethod(WS_METHODS.subscribeBackgroundPolicy)).toBe(
      AuthOrchestrationReadScope,
    );
  });

  it("allows relay status reads without granting relay installation access", () => {
    expect(requiredScopeForRpcMethod(WS_METHODS.cloudGetRelayClientStatus)).toBe(
      AuthRelayReadScope,
    );
    expect(requiredScopeForRpcMethod(WS_METHODS.cloudInstallRelayClient)).toBe(AuthRelayWriteScope);
  });

  it("requires permission to operate on a thread before uploading feedback", () => {
    expect(requiredScopeForRpcMethod(WS_METHODS.providerUploadFeedback)).toBe(
      AuthOrchestrationOperateScope,
    );
  });

  it("requires write access to import agent session history", () => {
    expect(requiredScopeForRpcMethod(WS_METHODS.agentSessionsScan)).toBe(
      AuthOrchestrationReadScope,
    );
    expect(requiredScopeForRpcMethod(WS_METHODS.agentSessionsImport)).toBe(
      AuthOrchestrationOperateScope,
    );
  });

  it("requires operate for guarded dispatch and read for runtime attachment", () => {
    expect(requiredScopeForRpcMethod(ORCHESTRATION_V2_WS_METHODS.dispatchGuarded)).toBe(
      AuthOrchestrationOperateScope,
    );
    expect(requiredScopeForRpcMethod(ORCHESTRATION_V2_WS_METHODS.getThreadRuntimeAttachment)).toBe(
      AuthOrchestrationReadScope,
    );
  });

  it("keeps imported review and observation read scoped and explicit start operate scoped", () => {
    expect(requiredScopeForRpcMethod(ORCHESTRATION_V2_WS_METHODS.reviewImportedHistoryStart)).toBe(AuthOrchestrationReadScope);
    expect(requiredScopeForRpcMethod(ORCHESTRATION_V2_WS_METHODS.observeImportedHistoryStart)).toBe(AuthOrchestrationReadScope);
    expect(requiredScopeForRpcMethod(ORCHESTRATION_V2_WS_METHODS.startWithImportedHistory)).toBe(AuthOrchestrationOperateScope);
  });

  it("reads current runtime observation and operating counts without mutation permission", () => {
    expect(requiredScopeForRpcMethod(ORCHESTRATION_V2_WS_METHODS.getThreadRuntimeObservation)).toBe(AuthOrchestrationReadScope);
    expect(requiredScopeForRpcMethod(ORCHESTRATION_V2_WS_METHODS.getOperatingCounts)).toBe(AuthOrchestrationReadScope);
  });

  it("observes a current runtime STOP receipt under read permission", () => {
    expect(requiredScopeForRpcMethod(ORCHESTRATION_V2_WS_METHODS.observeCurrentThreadRuntimeStop)).toBe(AuthOrchestrationReadScope);
  });

  it("requires operate for both bootstrap RPCs and the exact current-runtime STOP mutation", () => {
    for (const method of ["orchestration.dispatchBootstrap", ORCHESTRATION_V2_WS_METHODS.dispatchNativeBootstrap,
      ORCHESTRATION_V2_WS_METHODS.stopCurrentThreadRuntime]) {
      expect(requiredScopeForRpcMethod(method)).toBe(AuthOrchestrationOperateScope);
    }
  });

  it("observes deletion cleanup under read permission without granting another mutation", () => {
    expect(requiredScopeForRpcMethod(ORCHESTRATION_V2_WS_METHODS.observeThreadDeletionCleanup)).toBe(
      AuthOrchestrationReadScope,
    );
  });

  it("reads saved accounting under orchestration read permission", () => {
    expect(requiredScopeForRpcMethod(WS_METHODS.serverReadTokenAccounting)).toBe(
      AuthOrchestrationReadScope,
    );
  });

  it("separates ACP Registry discovery from provisioning", () => {
    expect(requiredScopeForRpcMethod(WS_METHODS.serverSearchAcpRegistry)).toBe(
      AuthOrchestrationReadScope,
    );
    expect(requiredScopeForRpcMethod(WS_METHODS.serverPrepareAcpRegistryAgent)).toBe(
      AuthOrchestrationOperateScope,
    );
    expect(requiredScopeForRpcMethod(WS_METHODS.serverUninstallAcpRegistryManagedBinary)).toBe(
      AuthOrchestrationOperateScope,
    );
    expect(requiredScopeForRpcMethod(WS_METHODS.serverAcceptAcpRegistryUrlAuth)).toBe(
      AuthOrchestrationOperateScope,
    );
    expect(requiredScopeForRpcMethod(WS_METHODS.serverListAcpRegistrySessions)).toBe(
      AuthOrchestrationReadScope,
    );
    expect(requiredScopeForRpcMethod(WS_METHODS.serverImportAcpRegistrySession)).toBe(
      AuthOrchestrationOperateScope,
    );
    expect(requiredScopeForRpcMethod(WS_METHODS.serverLogoutAcpRegistry)).toBe(
      AuthOrchestrationOperateScope,
    );
  });

  it("reads the reviewer menu under the same scope as the pull request it belongs to", () => {
    // The candidate list is a read like the detail beside it, and asking somebody for a review is
    // a write like every other pull request operation.
    expect(requiredScopeForRpcMethod(WS_METHODS.pullRequestsChecks)).toBe(
      AuthOrchestrationReadScope,
    );
    expect(requiredScopeForRpcMethod(WS_METHODS.pullRequestsReviewerCandidates)).toBe(
      requiredScopeForRpcMethod(WS_METHODS.pullRequestsDetail),
    );
    expect(requiredScopeForRpcMethod(WS_METHODS.pullRequestsRequestReviewers)).toBe(
      requiredScopeForRpcMethod(WS_METHODS.pullRequestsComment),
    );
  });

  it("rejects unknown RPC method names", () => {
    for (const method of ["server.notRegistered", "toString", "constructor"]) {
      expect(() => requiredScopeForRpcMethod(method)).toThrow(
        `RPC method ${method} has no declared authorization scope.`,
      );
    }
  });
});

it("requires operate permission for host retry while preserving read-only listing", () => {
  expect(requiredScopeForDeviceList({})).toBe(AuthOrchestrationReadScope);
  expect(requiredScopeForDeviceList({ retryHostId: "remote-host" })).toBe(
    AuthOrchestrationOperateScope,
  );
});

it("requires operate permission for tool updates even alongside a read-only check", () => {
  expect(requiredScopeForDeviceList({ updateTool: "agent", inspectOnly: true })).toBe(
    AuthOrchestrationOperateScope,
  );
  expect(requiredScopeForDeviceList({ updateTool: "hub" })).toBe(AuthOrchestrationOperateScope);
});

it.effect("checks the verified auth session's permanent enrollment before ordinary V2 creation", () =>
  Effect.gen(function* () {
    const actorSessionId = AuthSessionId.make("verified-ordinary-create-session");
    let reads = 0;
    const error = yield* assertLegacyBootstrapAllowed({
      actorSessionId, command: { type: "thread.create" },
      hasAutomationEnrollment: (received) => Effect.sync(() => {
        expect(received).toBe(actorSessionId);
        reads++;
        return true;
      }),
    }).pipe(Effect.flip);
    expect(reads).toBe(1);
    expect(error.creationRejectionCode).toBe("stale_grant");
  }),
);

it.effect("permits ordinary V2 creation only with a successful unenrolled lookup", () =>
  Effect.gen(function* () {
    const actorSessionId = AuthSessionId.make("verified-ordinary-create-session");
    yield* assertLegacyBootstrapAllowed({
      actorSessionId, command: { type: "thread.create" },
      hasAutomationEnrollment: () => Effect.succeed(false),
    });
    const error = yield* assertLegacyBootstrapAllowed({
      actorSessionId, command: { type: "thread.create" },
      hasAutomationEnrollment: () => Effect.fail("enrollment lookup unavailable"),
    }).pipe(Effect.flip);
    expect(error.creationRejectionCode).toBe("unsupported_authority");
  }),
);
