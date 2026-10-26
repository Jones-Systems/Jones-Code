import { assert, it } from "@effect/vitest";
import { type RemoteAdmissionEnvelope } from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Exit from "effect/Exit";
import * as Admission from "../remoteAdmission/RemoteAdmission.ts";
import { admissionOperation, threadSendIdentity } from "./RemoteRequestProtocol.ts";
import * as Fixture from "./testkit.ts";

const envelope = (operationId: string, sessionId = "linked-session"): RemoteAdmissionEnvelope => ({
  identity: { clientId: sessionId, linkId: "link" }, operation: "t3_thread_send",
  payload: { params: { resultRequest: { schemaVersion: "t3.thread-result-request/v1", operationId } }, invocation: { client: { linked: true, sessionId, remoteOrigin: { environmentId: "authenticated-origin", linkSessionId: "linked-session" } } } },
  target: { projectId: "project", threadId: "receiver", runtimeMode: "full-access", interactionMode: "default" },
  receiverPolicy: { enabled: false, revision: "test-policy", modelSelection: null },
});

it("extracts only a descriptor bound to the authenticated linked session", () => {
  assert.deepEqual(admissionOperation(envelope("operation")), { origin: "authenticated-origin", operation: "operation" });
  assert.isUndefined(admissionOperation(envelope("operation", "different-session")));
  assert.isUndefined(admissionOperation({ ...envelope("operation"), identity: { clientId: "client", linkId: null } }));
  assert.isUndefined(admissionOperation({ ...envelope("operation"), payload: null }));
});

it("preserves existing MCP send receipt and message identities when reconciling a forwarded key", () => {
  assert.deepEqual(threadSendIdentity("linked:original/namespace", "fwd:original/key"), {
    commandId: "command:mcp:linked%3Aoriginal%2Fnamespace:thread-send:fwd%3Aoriginal%2Fkey",
    messageId: "message:mcp:linked%3Aoriginal%2Fnamespace:thread-send:fwd%3Aoriginal%2Fkey",
  });
});

it.effect("admission records its result identity atomically and a receiver fence refuses a late arrival", () => Effect.gen(function* () {
  const f = yield* Fixture.seed;
  const admissions = yield* Admission.RemoteAdmission;
  const first = yield* admissions.request({ idempotencyKey: "original-key", envelope: envelope("admitted-operation") });
  const rows = yield* f.sql`SELECT origin_environment_id, operation_id FROM jones_remote_admissions WHERE id = ${first.id}`;
  assert.deepEqual(rows, [{ origin_environment_id: "authenticated-origin", operation_id: "admitted-operation" }]);
  yield* f.sql.withTransaction(f.store.fence("authenticated-origin", "late-operation"));
  const refused = yield* admissions.request({ idempotencyKey: "late-key", envelope: envelope("late-operation") }).pipe(Effect.exit);
  assert.isTrue(Exit.isFailure(refused));
  assert.deepEqual(yield* f.sql`SELECT id FROM jones_remote_admissions WHERE operation_id = 'late-operation'`, []);
}).pipe(Effect.provide(Admission.layerWithEvaluator(() => Effect.succeed({ verdict: "pass", category: "none", reason: "Test admission" })).pipe(Layer.provideMerge(Fixture.layer)))));
