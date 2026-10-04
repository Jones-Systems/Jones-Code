import { describe, expect, it } from "vite-plus/test";
import {
  AuthSessionId,
  AuthOrchestrationReadScope,
  AuthOrchestrationOperateScope,
  CommandId,
  MessageId,
  EnvironmentAuthenticatedAuth,
  EnvironmentAuthenticatedPrincipal,
  EnvironmentOrchestrationHttpApi,
  ORCHESTRATION_PROTOCOL_HEADER,
  ORCHESTRATION_PROTOCOL_VERSION_TEXT,
  ProviderInstanceId,
  ThreadId,
  type AuthEnvironmentScope,
  type NativeCommandObservationV2,
  type OrchestrationCommandObservation,
} from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as HttpApi from "effect/unstable/httpapi/HttpApi";
import * as HttpApiBuilder from "effect/unstable/httpapi/HttpApiBuilder";
import * as HttpRouter from "effect/unstable/http/HttpRouter";
import * as HttpServer from "effect/unstable/http/HttpServer";
import * as HttpServerRequest from "effect/unstable/http/HttpServerRequest";
import * as SqlClient from "effect/unstable/sql/SqlClient";

import { failEnvironmentAuthInvalid } from "../auth/http.ts";
import * as OrchestrationEventStore from "../persistence/Services/OrchestrationEventStore.ts";
import { NativeCreationRepository, NativeCreationRepositoryError } from "../persistence/Services/NativeCreationRepository.ts";
import * as ProjectEnrichmentService from "../project/ProjectEnrichmentService.ts";
import * as ProjectStore from "./ProjectStore.ts";
import * as ThreadManagementService from "./ThreadManagementService.ts";
import * as ProviderSessionGoalService from "./ProviderSessionGoalService.ts";
import { CommandObservationUnsupportedError } from "./CommandObservation.ts";
import { OrchestratorProjectionError } from "./Orchestrator.ts";
import { orchestrationHttpApiLayer } from "./http.ts";

const threadId = ThreadId.make("goal-http-thread");
const instanceId = ProviderInstanceId.make("codex-owner");
const goalPath = `/api/orchestration/threads/${threadId}/provider-goal-state`;
const observationInput = {
  threadId,
  commandId: CommandId.make("missing-command"),
  messageId: MessageId.make("missing-message"),
};
const nativeObservation: NativeCommandObservationV2 = {
  version: 2,
  ...observationInput,
  commandStatus: "not_found",
  identity: null,
  identityVerification: "missing",
  correlation: "missing",
  snapshot: { snapshotSequence: 7, targetEventSequence: 0, complete: true },
  correlatedMessageId: null,
  receipt: null,
  run: null,
  target: null,
};
const legacyObservation: OrchestrationCommandObservation = {
  ...observationInput,
  snapshotSequence: 7,
  commandStatus: "not_found",
  acceptedSequence: null,
  correlation: "missing",
  turn: null,
  target: null,
};

function fixture(options: {
  scopes?: AuthEnvironmentScope[];
  missingGoalService?: boolean;
  legacySupported?: boolean;
  observationFailure?: boolean;
  missingNativeRepository?: boolean;
  automationEnrolled?: boolean;
  enrollmentLookupFailure?: boolean;
} = {}) {
  let goalCalls = 0;
  let shellCalls = 0;
  let windowCalls = 0;
  const goalInputs: unknown[] = [];
  const nativeInputs: unknown[] = [];
  const legacyInputs: unknown[] = [];
  const enrollmentInputs: unknown[] = [];
  const dispatchInputs: unknown[] = [];
  const auth = Layer.succeed(EnvironmentAuthenticatedAuth, (httpEffect) =>
    Effect.gen(function* () {
      const request = yield* HttpServerRequest.HttpServerRequest;
      if (request.headers.authorization !== "Bearer fixture") {
        return yield* failEnvironmentAuthInvalid("missing_credential");
      }
      return yield* httpEffect.pipe(Effect.provideService(EnvironmentAuthenticatedPrincipal, {
        sessionId: AuthSessionId.make("http-fixture"),
        subject: "fixture",
        method: "bearer-access-token",
        scopes: new Set(options.scopes ?? [AuthOrchestrationReadScope]),
      }));
    }),
  );
  let group = orchestrationHttpApiLayer.pipe(Layer.provide(Layer.mergeAll(
    Layer.mock(SqlClient.SqlClient)({ withTransaction: (effect) => effect }),
    Layer.mock(ThreadManagementService.ThreadManagementService)({
      dispatch: (input) => Effect.sync(() => {
        dispatchInputs.push(input);
        throw new Error("HTTP rejection must not dispatch");
      }),
      dispatchNativeCreationStage: (input) => Effect.sync(() => {
        dispatchInputs.push(input);
        throw new Error("HTTP rejection must not dispatch native creation");
      }),
      observeCommand: (input) => Effect.suspend(() => {
        nativeInputs.push(input);
        return options.observationFailure
          ? Effect.fail(new OrchestratorProjectionError({ threadId }))
          : Effect.succeed(nativeObservation);
      }),
      observeLegacyCommand: (input) => Effect.suspend(() => {
        legacyInputs.push(input);
        if (options.observationFailure) {
          return Effect.fail(new OrchestratorProjectionError({ threadId }));
        }
        return options.legacySupported
          ? Effect.succeed(legacyObservation)
          : Effect.fail(new CommandObservationUnsupportedError({ reason: "observation_unsupported" }));
      }),
      getShellSnapshot: () => Effect.sync(() => {
        shellCalls++;
        return { schemaVersion: 2, snapshotSequence: 7, threads: [] };
      }),
      getThreadSnapshotWindow: () => Effect.sync(() => {
        windowCalls++;
        throw new Error("Unexpected history read");
      }),
    }),
    Layer.mock(OrchestrationEventStore.OrchestrationEventStore)({
      latestApplicationSequence: Effect.succeed(7),
    }),
    Layer.mock(ProjectStore.ProjectStoreV2)({ listShells: () => Effect.succeed([]) }),
    Layer.mock(ProjectEnrichmentService.ProjectEnrichmentService)({}),
  )));
  if (!options.missingNativeRepository) {
    group = group.pipe(Layer.provide(Layer.mock(NativeCreationRepository)({
      hasAutomationEnrollment: (sessionId) => Effect.suspend(() => {
        enrollmentInputs.push(sessionId);
        return options.enrollmentLookupFailure
          ? Effect.fail(new NativeCreationRepositoryError({ code: "unresolved_claim", message: "Enrollment lookup unavailable" }))
          : Effect.succeed(options.automationEnrolled ?? false);
      }),
    })));
  }
  if (!options.missingGoalService) {
    group = group.pipe(Layer.provide(Layer.mock(ProviderSessionGoalService.ProviderSessionGoalService)({
      get: (input) => Effect.sync(() => {
        goalCalls++;
        goalInputs.push(input);
        return { nativeThreadId: "native-thread", state: "inactive" as const, reasonCode: "goal_null" as const };
      }),
    })));
  }
  const routes = HttpApiBuilder.layer(HttpApi.make("environment").add(EnvironmentOrchestrationHttpApi)).pipe(
    Layer.provide(group),
    Layer.provide(auth),
    Layer.provide(HttpServer.layerServices),
  );
  return {
    ...HttpRouter.toWebHandler(routes, { disableLogger: true }),
    goalCalls: () => goalCalls,
    shellCalls: () => shellCalls,
    windowCalls: () => windowCalls,
    goalInputs,
    nativeInputs,
    legacyInputs,
    enrollmentInputs,
    dispatchInputs,
  };
}

const request = (path: string) => new Request(`http://test${path}`, {
  headers: {
    authorization: "Bearer fixture",
    [ORCHESTRATION_PROTOCOL_HEADER]: ORCHESTRATION_PROTOCOL_VERSION_TEXT,
  },
});

const guardedBootstrap = {
  type: "thread.turn.start",
  commandId: "http-guarded-bootstrap",
  threadId,
  message: { messageId: "http-bootstrap-message", role: "user", text: "Synthetic bootstrap", attachments: [] },
  modelSelection: { instanceId, model: "synthetic-model" },
  runtimeMode: "full-access",
  interactionMode: "default",
  createdAt: "2026-01-01T00:00:00.000Z",
  bootstrap: { runSetupScript: true },
  dispatchGuard: {
    observedSnapshotSequence: 0,
    expectedModelSelection: { instanceId, model: "synthetic-model" },
    expectedSessionStatus: null,
    expectedActiveTurnId: null,
    expectedLatestTurnId: null,
    requireIdle: true,
  },
};

function dispatchRequest(payload: unknown = guardedBootstrap, authenticated = true) {
  return new Request("http://test/api/orchestration/dispatch", {
    method: "POST",
    headers: { "content-type": "application/json", ...(authenticated ? { authorization: "Bearer fixture" } : {}) },
    body: JSON.stringify(payload),
  });
}

describe("orchestration HTTP dispatch rejection", () => {
  it("rejects guarded bootstrap with its exact reason after the current enrollment check", async () => {
    const f = fixture({ scopes: [AuthOrchestrationOperateScope] });
    try {
      const response = await f.handler(dispatchRequest());
      expect(response.status).toBe(400);
      expect(await response.json()).toMatchObject({ code: "invalid_request", reason: "dispatch_guard_bootstrap_unsupported" });
      expect(f.enrollmentInputs).toEqual([AuthSessionId.make("http-fixture")]);
      expect(f.dispatchInputs).toEqual([]);
      expect(f.nativeInputs).toEqual([]);
      expect(f.legacyInputs).toEqual([]);
      expect(f.goalCalls()).toBe(0);
      expect(f.shellCalls()).toBe(0);
      expect(f.windowCalls()).toBe(0);
    } finally { await f.dispose(); }
  });

  it("requires authentication and Operate scope before checking enrollment or dispatching", async () => {
    for (const scopes of [[], [AuthOrchestrationReadScope]] as AuthEnvironmentScope[][]) {
      const f = fixture({ scopes });
      try {
        expect((await f.handler(dispatchRequest(guardedBootstrap, false))).status).toBe(401);
        expect((await f.handler(dispatchRequest())).status).toBe(403);
        expect(f.enrollmentInputs).toEqual([]);
        expect(f.dispatchInputs).toEqual([]);
      } finally { await f.dispose(); }
    }
  });

  it("preserves enrollment rejection and fails closed when enrollment cannot be read", async () => {
    for (const options of [{ automationEnrolled: true }, { missingNativeRepository: true }, { enrollmentLookupFailure: true }]) {
      const f = fixture({ scopes: [AuthOrchestrationOperateScope], ...options });
      try {
        const response = await f.handler(dispatchRequest());
        expect(response.status).toBe(400);
        expect(await response.json()).toMatchObject({ code: "invalid_request", reason: "invalid_command" });
        expect(f.enrollmentInputs).toHaveLength("missingNativeRepository" in options ? 0 : 1);
        expect(f.dispatchInputs).toEqual([]);
      } finally { await f.dispose(); }
    }
  });

  it("rejects supported legacy payload shapes without restoring execution", async () => {
    const { bootstrap, dispatchGuard, ...ordinary } = guardedBootstrap;
    for (const payload of [ordinary, { ...ordinary, dispatchGuard }, { ...ordinary, bootstrap }]) {
      const f = fixture({ scopes: [AuthOrchestrationOperateScope] });
      try {
        const response = await f.handler(dispatchRequest(payload));
        expect(response.status).toBe(400);
        expect(await response.json()).toMatchObject({ code: "invalid_request", reason: "invalid_command" });
        expect(f.enrollmentInputs).toHaveLength("bootstrap" in payload ? 1 : 0);
        expect(f.dispatchInputs).toEqual([]);
      } finally { await f.dispose(); }
    }
  });
});

describe("orchestration V2 HTTP read routes", () => {
  it("returns the exact V2 observation from the actual service and validates identity input", async () => {
    const f = fixture();
    try {
      const path = `/api/orchestration/v2/threads/${threadId}/commands/missing-command`;
      const response = await f.handler(request(`${path}?messageId=missing-message`));
      expect(response.status).toBe(200);
      expect(await response.json()).toEqual(nativeObservation);
      expect(f.nativeInputs).toEqual([observationInput]);
      const missingMessage = await f.handler(request(path));
      expect(missingMessage.status).toBe(400);
      expect(f.nativeInputs).toEqual([observationInput]);
      expect(f.legacyInputs).toEqual([]);
      expect(f.goalCalls()).toBe(0);
      expect(f.shellCalls()).toBe(0);
      expect(f.windowCalls()).toBe(0);
    } finally { await f.dispose(); }
  });

  it("requires authentication and read scope before observing either command version", async () => {
    for (const prefix of ["/api/orchestration", "/api/orchestration/v2"]) {
      const path = `${prefix}/threads/${threadId}/commands/missing-command?messageId=missing-message`;
      for (const scopes of [[], [AuthOrchestrationOperateScope]] as AuthEnvironmentScope[][]) {
        const f = fixture({ scopes });
        try {
          expect((await f.handler(new Request(`http://test${path}`))).status).toBe(401);
          expect((await f.handler(request(path))).status).toBe(403);
          expect(f.goalCalls()).toBe(0);
          expect(f.shellCalls()).toBe(0);
          expect(f.windowCalls()).toBe(0);
          expect(f.nativeInputs).toEqual([]);
          expect(f.legacyInputs).toEqual([]);
        } finally { await f.dispose(); }
      }
    }
  });

  it("preserves faithful V1 observation and its required message identity", async () => {
    const f = fixture({ legacySupported: true });
    try {
      const path = `/api/orchestration/threads/${threadId}/commands/missing-command`;
      const response = await f.handler(request(`${path}?messageId=missing-message`));
      expect(response.status).toBe(200);
      expect(await response.json()).toEqual(legacyObservation);
      expect(f.legacyInputs).toEqual([observationInput]);
      expect((await f.handler(request(path))).status).toBe(400);
      expect(f.legacyInputs).toEqual([observationInput]);
      expect(f.nativeInputs).toEqual([]);
    } finally { await f.dispose(); }
  });

  it("maps observation read failure to the existing internal error for both versions", async () => {
    for (const prefix of ["/api/orchestration", "/api/orchestration/v2"]) {
      const f = fixture({ observationFailure: true });
      try {
        const path = `${prefix}/threads/${threadId}/commands/missing-command?messageId=missing-message`;
        const response = await f.handler(request(path));
        expect(response.status).toBe(500);
        expect(await response.json()).toMatchObject({ reason: "orchestration_thread_snapshot_failed" });
      } finally { await f.dispose(); }
    }
  });

  it("does not fabricate V1 command history from V2 records", async () => {
    const f = fixture();
    try {
      const response = await f.handler(request(`/api/orchestration/threads/${threadId}/commands/missing-command?messageId=missing-message`));
      expect(response.status).toBe(400);
      expect(await response.json()).toMatchObject({ reason: "observation_unsupported" });
      expect(f.legacyInputs).toEqual([observationInput]);
      expect(f.nativeInputs).toEqual([]);
      expect(f.goalCalls()).toBe(0);
      expect(f.shellCalls()).toBe(0);
    } finally { await f.dispose(); }
  });

  it("requires authentication and read scope before the resident goal read", async () => {
    for (const scopes of [[], [AuthOrchestrationOperateScope]] as AuthEnvironmentScope[][]) {
      const f = fixture({ scopes });
      try {
        const path = `${goalPath}?expectedInstanceId=${instanceId}`;
        expect((await f.handler(new Request(`http://test${path}`))).status).toBe(401);
        expect((await f.handler(request(path))).status).toBe(403);
        expect(f.goalCalls()).toBe(0);
      } finally { await f.dispose(); }
    }
  });

  it("returns exact-target goal metadata and rejects a missing instance before reading", async () => {
    const f = fixture();
    try {
      const response = await f.handler(request(`${goalPath}?expectedInstanceId=${instanceId}`));
      expect(response.status).toBe(200);
      expect(await response.json()).toEqual({
        schema: "t3.provider-goal-state/v1",
        threadId,
        providerInstanceId: instanceId,
        nativeThreadId: "native-thread",
        observedAtMs: expect.any(Number),
        state: "inactive",
        reasonCode: "goal_null",
      });
      expect(f.goalInputs).toEqual([{ threadId, expectedInstanceId: instanceId }]);
      expect((await f.handler(request(goalPath))).status).toBe(400);
      expect(f.goalCalls()).toBe(1);
    } finally { await f.dispose(); }
  });

  it("returns unsupported unknown when the resident goal capability is unavailable", async () => {
    const f = fixture({ missingGoalService: true });
    try {
      const response = await f.handler(request(`${goalPath}?expectedInstanceId=${instanceId}`));
      expect(response.status).toBe(200);
      expect(await response.json()).toMatchObject({ state: "unknown", reasonCode: "unsupported", nativeThreadId: null });
      expect(f.goalCalls()).toBe(0);
    } finally { await f.dispose(); }
  });

  it("preserves the ordinary active shell response", async () => {
    const f = fixture();
    try {
      const response = await f.handler(request("/api/orchestration/shell"));
      expect(response.status).toBe(200);
      expect(await response.json()).toEqual({ schemaVersion: 2, snapshotSequence: 7, projects: [], threads: [], archivedThreads: [] });
      expect(f.shellCalls()).toBe(1);
      expect(f.goalCalls()).toBe(0);
    } finally { await f.dispose(); }
  });

  it("rejects an invalid history cursor before loading a transcript window", async () => {
    const f = fixture();
    try {
      const response = await f.handler(request(`/api/orchestration/threads/${threadId}/history?cursor=invalid`));
      expect(response.status).toBe(400);
      expect(await response.json()).toMatchObject({ reason: "invalid_history_cursor" });
      expect(f.windowCalls()).toBe(0);
    } finally { await f.dispose(); }
  });
});
