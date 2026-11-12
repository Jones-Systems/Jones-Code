import { assert, it } from "@effect/vitest";
import * as Schema from "effect/Schema";
import * as DateTime from "effect/DateTime";
import * as Option from "effect/Option";
import {
  ExecutionEnvironmentCapabilities,
  NativeBootstrapCreationCapability,
} from "./environment.ts";
import {
  NATIVE_BOOTSTRAP_MAX_PREPARATION_BYTES,
  NATIVE_BOOTSTRAP_MAX_SUBMISSION_BYTES,
  NativeBootstrapSubmission,
  NativeCreationEffect,
  NativeCreationGuard,
  NativeCreationObservation,
} from "./nativeCreation.ts";
import { OrchestrationDispatchCommandError } from "./orchestrationDispatch.ts";
import {
  OrchestrationV2ThreadLaunchResult,
  OrchestrationV2DispatchCommandError,
} from "./orchestrationV2.ts";

const guard = {
  schema: "t3.native-creation-guard/v1",
  grantId: "grant-1",
  grantRevision: 1,
};
const submission = {
  schema: "t3.native-bootstrap-submission/v1",
  preparationBase64: "e30=",
  creationGuard: guard,
};
const capability = {
  submissionSchema: "t3.native-bootstrap-submission/v1",
  preparationSchema: "voice.t3-bootstrap-preparation/v1",
  observationSchema: "t3.native-creation-observation/v1",
  guardRequired: true,
};

const digest = "a".repeat(64);
const creation = {
  schema: "t3.native-creation-observation/v1",
  preparationId: "preparation-1",
  operationId: "operation-1",
  preparationSha256: digest,
  bindingDigest: digest,
  promptDigest: digest,
  commandDigest: digest,
  normalizedCommandDigest: digest,
  claimId: "claim-1",
  claimedBootId: "boot-1",
  claimedAt: "2026-10-02T12:00:00Z",
  actorSessionId: "session-1",
  grantId: "grant-1",
  grantRevision: 1,
  binding: {
    backendInstance: "backend-1",
    environmentId: "environment-1",
    projectId: "project-1",
    projectCwd: "/workspace/project",
    accountRef: "account-ref-1",
    accountBindingId: "account-binding-1",
    accountBindingRevision: 1,
    providerModelSelection: {
      instanceId: "codex",
      model: "model-1",
      options: [
        { id: "fast", value: true },
        { id: "effort", value: "high" },
      ],
    },
    runtimeMode: "full-access",
    interactionMode: "default",
    baseBranch: "main",
    startFromOrigin: true,
    runSetupScript: false,
    requestedBranch: "t3code/voice-branch",
  },
  incarnation: null,
  effects: [],
  unresolvedEffects: [],
  outcome: "unknown",
};
const acceptsWire = <S extends Schema.ConstraintDecoder<unknown>>(schema: S) => {
  const decode = Schema.decodeUnknownOption(schema);
  return (input: unknown) => Option.isSome(decode(input));
};

const effect = { effectId: "effect-1", ordinal: 0, timestamp: creation.claimedAt };
const decodeSubmission = Schema.decodeUnknownSync(NativeBootstrapSubmission);
const encodeSubmission = Schema.encodeSync(NativeBootstrapSubmission);
const decodeCreation = Schema.decodeUnknownSync(NativeCreationObservation);
const encodeCreation = Schema.encodeSync(NativeCreationObservation);

it("requires a closed guarded submission and a positive safe grant revision", () => {
  const accepts = acceptsWire(NativeBootstrapSubmission);
  assert.isTrue(accepts(submission));
  const decoded = decodeSubmission(submission);
  assert.deepEqual<unknown>(encodeSubmission(decoded), submission);
  assert.isFalse(accepts({ ...submission, creationGuard: undefined }));
  assert.isFalse(accepts({ ...submission, extra: true }));
  assert.isFalse(accepts({ ...submission, creationGuard: { ...guard, extra: true } }));
  for (const grantRevision of [0, -1, 1.5, Number.MAX_SAFE_INTEGER + 1, Infinity, NaN]) {
    assert.isFalse(acceptsWire(NativeCreationGuard)({ ...guard, grantRevision }));
  }
  assert.isTrue(
    acceptsWire(NativeCreationGuard)({ ...guard, grantRevision: Number.MAX_SAFE_INTEGER }),
  );
});

it("bounds decoded preparation bytes, rejects malformed base64 and bounds submission UTF-8", () => {
  const accepts = acceptsWire(NativeBootstrapSubmission);
  const atLimit = btoa("\0".repeat(NATIVE_BOOTSTRAP_MAX_PREPARATION_BYTES));
  assert.isTrue(accepts({ ...submission, preparationBase64: atLimit }));
  assert.isFalse(
    accepts({
      ...submission,
      preparationBase64: btoa("\0".repeat(NATIVE_BOOTSTRAP_MAX_PREPARATION_BYTES + 1)),
    }),
  );
  for (const preparationBase64 of ["", "e30", "e30=\n", "-___", "====", "A==="]) {
    assert.isFalse(accepts({ ...submission, preparationBase64 }));
  }
  assert.isFalse(
    accepts({
      ...submission,
      creationGuard: { ...guard, grantId: "é".repeat(NATIVE_BOOTSTRAP_MAX_SUBMISSION_BYTES / 2) },
    }),
  );
});

it("keeps legacy capabilities and observations decodable while requiring exact creation versions", () => {
  const decodeCapabilities = Schema.decodeUnknownSync(ExecutionEnvironmentCapabilities);
  assert.deepEqual<unknown>(decodeCapabilities({}), { repositoryIdentity: false });
  assert.deepEqual<unknown>(
    decodeCapabilities({ nativeBootstrapCreation: capability }).nativeBootstrapCreation,
    capability,
  );
  const acceptsCapability = acceptsWire(NativeBootstrapCreationCapability);
  assert.isFalse(acceptsCapability({ ...capability, extra: true }));
  assert.isFalse(acceptsCapability({ ...capability, guardRequired: false }));
  for (const field of ["submissionSchema", "preparationSchema", "observationSchema"]) {
    assert.isFalse(acceptsCapability({ ...capability, [field]: "unsupported/v2" }));
  }
  const decodeCreationField = Schema.decodeUnknownSync(
    Schema.Struct({ creation: OrchestrationV2ThreadLaunchResult.fields.creation }),
  );
  assert.deepEqual<unknown>(decodeCreationField({}), {});
  assert.deepEqual<unknown>(decodeCreationField({ creation }).creation, creation);
});

it("records historical unknown effects without prompt bytes or legacy provider normalization", () => {
  const accepts = acceptsWire(NativeCreationObservation);
  const started = {
    ...effect,
    kind: "fetch",
    phase: "started",
    projectCwd: "/workspace/project",
    baseRef: "origin/main",
  };
  assert.isTrue(accepts({ ...creation, effects: [started], unresolvedEffects: [effect.effectId] }));
  assert.isFalse(accepts({ ...creation, prompt: "private prompt" }));
  assert.isFalse(accepts({ ...creation, preparationBase64: submission.preparationBase64 }));
  assert.isFalse(
    accepts({
      ...creation,
      binding: {
        ...creation.binding,
        providerModelSelection: { provider: "codex", model: "model-1" },
      },
    }),
  );
  assert.isFalse(
    accepts({
      ...creation,
      binding: {
        ...creation.binding,
        providerModelSelection: { instanceId: "codex", model: "model-1", options: { fast: true } },
      },
    }),
  );
  assert.isFalse(accepts({ ...creation, incarnation: { eventId: "event-1" } }));
  assert.isFalse(accepts({ ...creation, preparationSha256: "not-a-digest" }));
  for (const binding of [
    { ...creation.binding, extra: true },
    {
      ...creation.binding,
      providerModelSelection: { ...creation.binding.providerModelSelection, extra: true },
    },
    {
      ...creation.binding,
      providerModelSelection: {
        ...creation.binding.providerModelSelection,
        options: [{ id: "fast", value: true, extra: true }],
      },
    },
  ]) {
    assert.isFalse(accepts({ ...creation, binding }));
  }
  assert.isFalse(
    accepts({ ...creation, incarnation: { eventId: "event-1", sequence: 1, extra: true } }),
  );
  const decoded = decodeCreation(creation);
  assert.deepEqual<unknown>(encodeCreation(decoded), creation);
});

it("requires stage-specific effect facts and preserves failed and unknown external results", () => {
  const accepts = acceptsWire(NativeCreationEffect);
  const fetch = {
    ...effect,
    kind: "fetch",
    projectCwd: "/workspace/project",
    baseRef: "origin/main",
  };
  assert.isFalse(accepts({ ...fetch, phase: "completed" }));
  for (const result of ["succeeded", "failed", "unknown"]) {
    assert.isTrue(accepts({ ...fetch, phase: "completed", result }));
  }
  assert.isFalse(accepts({ ...fetch, phase: "started", details: {} }));
  assert.isFalse(accepts({ ...effect, kind: "arbitrary", phase: "started" }));
  for (const commandType of [
    "thread.create",
    "thread.message.user.append",
    "thread.session.set",
    "thread.meta.update",
    "thread.turn.start",
    "thread.delete",
  ]) {
    const command = {
      ...effect,
      kind: "native_command",
      commandId: "command-1",
      threadId: "thread-1",
      commandType,
      commandDigest: digest,
    };
    assert.isTrue(accepts({ ...command, phase: "started" }));
    assert.isFalse(accepts({ ...command, phase: "completed" }));
    assert.isTrue(accepts({ ...command, phase: "completed", eventId: "event-1", sequence: 1 }));
  }
  const worktree = {
    projectCwd: "/workspace/project",
    worktreePath: "/native/worktrees/project/voice",
    branch: "t3code/voice-branch",
    baseRef: "origin/main",
    ownership: "claimed",
  };
  assert.isTrue(accepts({ ...effect, kind: "worktree", phase: "started", ...worktree }));
  assert.isTrue(
    accepts({ ...effect, kind: "worktree", phase: "completed", ...worktree, result: "unknown" }),
  );
  assert.isTrue(
    accepts({
      ...effect,
      kind: "setup",
      phase: "started",
      worktreePath: worktree.worktreePath,
      terminalId: null,
    }),
  );
  assert.isTrue(
    accepts({
      ...effect,
      kind: "setup",
      phase: "completed",
      worktreePath: worktree.worktreePath,
      terminalId: "terminal-1",
      exitCode: 1,
      result: "failed",
    }),
  );
  assert.isTrue(
    accepts({
      ...effect,
      kind: "cleanup",
      phase: "completed",
      resource: {
        kind: "thread",
        threadId: "thread-1",
        incarnation: { eventId: "created-event-1", sequence: 1 },
      },
      recoveryScopeId: "recovery-1",
      result: "unknown",
    }),
  );
  for (const action of [
    "tracker_registration",
    "bootstrap_detachment",
    "setup_detachment",
    "setup_completion_detachment",
  ]) {
    assert.isTrue(
      accepts({ ...effect, kind: "lifecycle", phase: "started", threadId: "thread-1", action }),
    );
  }
  assert.isFalse(
    accepts({
      ...effect,
      kind: "cleanup",
      phase: "started",
      resource: { kind: "thread", threadId: "thread-1" },
      recoveryScopeId: "recovery-1",
    }),
  );
  assert.isFalse(accepts({ ...effect, kind: "setup", phase: "completed", result: "failed" }));
});

it("keeps dispatch rejection codes optional and closed", () => {
  const decode = Schema.decodeUnknownSync(OrchestrationDispatchCommandError);
  const error = { _tag: "OrchestrationDispatchCommandError", message: "Rejected" };
  assert.equal(decode(error).creationRejectionCode, undefined);
  assert.equal(
    decode({ ...error, creationRejectionCode: "stale_grant" }).creationRejectionCode,
    "stale_grant",
  );
  assert.throws(() => decode({ ...error, creationRejectionCode: "invented" }));
});

it("keeps V2 dispatch rejection codes optional without altering current error fields", () => {
  const decode = Schema.decodeUnknownSync(OrchestrationV2DispatchCommandError);
  const error = {
    _tag: "OrchestrationV2DispatchCommandError",
    commandId: "command-1",
    commandType: "message.dispatch",
    message: "Rejected",
  };
  assert.equal(decode(error).creationRejectionCode, undefined);
  assert.equal(
    decode({ ...error, creationRejectionCode: "stale_grant" }).creationRejectionCode,
    "stale_grant",
  );
  assert.throws(() => decode({ ...error, creationRejectionCode: "invented" }));
});

it("does not reinterpret V2 execution commands as historical native command facts", () => {
  const accepts = acceptsWire(NativeCreationEffect);
  assert.isFalse(
    accepts({
      ...effect,
      kind: "native_command",
      phase: "started",
      commandId: "command-1",
      threadId: "thread-1",
      commandType: "message.dispatch",
      commandDigest: digest,
    }),
  );
});

it("round-trips current launch results in both upgrade directions without changing historical bytes", () => {
  const now = DateTime.makeUnsafe("2026-10-02T12:00:00Z");
  const projection = {
    thread: {
      createdBy: "user",
      creationSource: "server",
      id: "thread-1",
      projectId: "project-1",
      title: "Synthetic thread",
      providerInstanceId: "codex",
      modelSelection: { instanceId: "codex", model: "fixture-model" },
      runtimeMode: "full-access",
      interactionMode: "default",
      branch: null,
      worktreePath: null,
      activeProviderThreadId: null,
      lineage: { parentThreadId: null, relationshipToParent: null, rootThreadId: "thread-1" },
      forkedFrom: null,
      createdAt: now,
      updatedAt: now,
      archivedAt: null,
      deletedAt: null,
    },
    runs: [],
    attempts: [],
    nodes: [],
    subagents: [],
    providerSessions: [],
    providerThreads: [],
    providerTurns: [],
    runtimeRequests: [],
    messages: [],
    plans: [],
    turnItems: [],
    checkpointScopes: [],
    checkpoints: [],
    contextHandoffs: [],
    contextTransfers: [],
    visibleTurnItems: [],
    updatedAt: now,
  };
  const beforeCreation = Schema.Struct({
    threadId: OrchestrationV2ThreadLaunchResult.fields.threadId,
    projection: OrchestrationV2ThreadLaunchResult.fields.projection,
    resumed: OrchestrationV2ThreadLaunchResult.fields.resumed,
  });
  const decode = Schema.decodeUnknownSync(OrchestrationV2ThreadLaunchResult);
  const legacy = { threadId: "thread-1", projection, resumed: false };
  const acceptedLegacy = decode(legacy);
  assert.equal(acceptedLegacy.creation, undefined);
  assert.deepEqual<unknown>(acceptedLegacy, Schema.decodeUnknownSync(beforeCreation)(legacy));
  const accepted = decode({ ...legacy, creation });
  assert.deepEqual<unknown>(encodeCreation(accepted.creation!), creation);
  assert.deepEqual<unknown>(
    Schema.decodeUnknownSync(beforeCreation)({ ...legacy, creation }),
    acceptedLegacy,
  );
  assert.throws(() => decode({ ...legacy, creation: { ...creation, extra: true } }));
});
