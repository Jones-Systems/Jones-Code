// @effect-diagnostics nodeBuiltinImport:off -- Regression binds the unchanged canonical public request digest.
import * as NodeCrypto from "node:crypto";
import { expect, it } from "@effect/vitest";
import * as OperationsWire from "@t3tools/contracts/jones/workstreamOperations";
import { WorkstreamsNativeSuppliedAttestation } from "@t3tools/contracts/jones/workstreamsNative";
import { WORKSTREAMS_T3_PROVIDER_V2_PROTOCOL } from "@t3tools/contracts";
import { RegistryError } from "../registry/session.ts";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Schema from "effect/Schema";
import * as Forwarder from "../forwarding/forwarder.ts";
import * as Operations from "./memberOperations.ts";
import { canonicalJson } from "../../operations/encoding.ts";

const decodeRequest = Schema.decodeUnknownSync(OperationsWire.WorkstreamOperationRequest);
const isResult = Schema.is(OperationsWire.WorkstreamOperationResult);
it.effect(
  "public operations preserve canonical digests and expose a pollable state for an ambiguous home reply",
  () => {
    const request = decodeRequest({
      contract: "workstream-operations/1.0.0",
      operation_id: "operation:adapter-001",
      kind: "create_workstream",
      origin: { kind: "agent" },
      registry: { registry_id: "11111111-1111-4111-8111-111111111111", owner_id: "owner:test" },
      workstream: {
        name: "Test",
        lifecycle: "active",
        progress: { state: "unknown" },
        sort_order: 0,
      },
    });
    const inputs: Forwarder.ForwardSubmission[] = [];
    const owner = Layer.succeed(Forwarder.MemberForwarder, {
      read: () => Effect.succeed(null),
      open: () => Effect.succeed([]),
      recover: () => Effect.succeed(null),
      recoverOpen: () => Effect.succeed([]),
      submit: (input) => {
        inputs.push(input);
        return Effect.succeed({
          operation_id: request.operation_id,
          state: "reconciling",
          next_poll_at: null,
          accepted_at: "2026-01-01T00:00:00.000Z",
          updated_at: "2026-01-01T00:00:00.000Z",
          result: null,
        });
      },
    });
    return Effect.gen(function* () {
      const operations = yield* Operations.MemberOperations;
      const result = yield* operations.submit({ request, wait_ms: 0 });
      expect(isResult(result)).toBe(true);
      expect(result.request_sha256).toBe(
        NodeCrypto.createHash("sha256").update(canonicalJson(request)).digest("hex"),
      );
      expect(result.state).toBe("reconciling");
      expect(inputs).toEqual([{ kind: "operation", body: canonicalJson(request) }]);
      expect((yield* Effect.exit(operations.submit({ request, wait_ms: 4000 })))._tag).toBe(
        "Failure",
      );
      const body = JSON.stringify(request, null, 2);
      yield* operations.submitBody(body);
      expect(inputs[1]?.body).toBe(body);
      const missingProvider = decodeRequest({
        contract: "workstream-operations/1.0.0",
        operation_id: "operation:evidence-001",
        kind: "set_primary",
        origin: { kind: "agent" },
        thread: { environment_id: "member:test", thread_id: "thread:test" },
        target: {
          kind: "unassigned",
          registry_id: "11111111-1111-4111-8111-111111111111",
          owner_id: "owner:test",
        },
        expected: { kind: "any" },
      });
      expect(
        (yield* Effect.exit(operations.submit({ request: missingProvider, wait_ms: 0 })))._tag,
      ).toBe("Failure");
      expect(inputs).toHaveLength(2);
    }).pipe(Effect.provide(Operations.layer.pipe(Layer.provide(owner))));
  },
);

const primary = decodeRequest({
  contract: "workstream-operations/1.0.0",
  operation_id: "operation:provider-port-001",
  kind: "set_primary",
  origin: { kind: "agent" },
  thread: { environment_id: "member:test", thread_id: "thread:test" },
  target: {
    kind: "unassigned",
    registry_id: "11111111-1111-4111-8111-111111111111",
    owner_id: "owner:test",
  },
  expected: { kind: "any" },
});
const decodeProof = Schema.decodeUnknownSync(WorkstreamsNativeSuppliedAttestation);
const proof = decodeProof({
  protocol: WORKSTREAMS_T3_PROVIDER_V2_PROTOCOL,
  state: "attested",
  request: {
    owner_id: "owner:test",
    principal_id: "principal:test",
    identity: {
      provider: "t3",
      source_instance_id: "member:test",
      resource_kind: "thread",
      id_kind: "internal",
      native_id: "thread:test",
      account_provenance: { kind: "not_account_scoped" },
    },
    expected_authority_namespace: "store:test",
    expected_store_generation: 1,
  },
  attestation: {
    provider: "t3",
    source_instance_id: "member:test",
    authority_namespace: "store:test",
    native_id: "thread:test",
    store_generation: 1,
    evidence_sha256: "a".repeat(64),
  },
  attested_at: "2026-01-01T00:00:00Z",
  expires_at: "2026-01-01T00:05:00Z",
});
const pendingView: Forwarder.ForwardView = {
  operation_id: primary.operation_id,
  state: "reconciling",
  next_poll_at: null,
  accepted_at: "2026-01-01T00:00:00.000Z",
  updated_at: "2026-01-01T00:00:00.000Z",
  result: null,
};
// These are adapter ports; real encrypted acceptance and caller admission are tested by Forwarder.
const forwardingOwner = (
  submit: Forwarder.MemberForwarder["Service"]["submit"],
  existing: Forwarder.ForwardView | null = null,
) =>
  Layer.succeed(Forwarder.MemberForwarder, {
    read: () => Effect.succeed(existing),
    open: () => Effect.succeed([]),
    recover: () => Effect.succeed(existing),
    recoverOpen: () => Effect.succeed([]),
    submit,
  });

it.effect(
  "fresh primary placement captures evidence before forwarding and preserves raw bytes",
  () => {
    const order: string[] = [];
    const inputs: Forwarder.ForwardSubmission[] = [];
    const body = JSON.stringify(primary, null, 2);
    const owner = forwardingOwner((input) =>
      Effect.sync(() => {
        order.push("submit");
        inputs.push(input);
        return pendingView;
      }),
    );
    const evidence = Layer.succeed(Operations.MemberOperationEvidence, {
      capture: (request) =>
        Effect.sync(() => {
          order.push("capture");
          expect(request).toEqual(primary);
          return proof;
        }),
    });
    return Effect.gen(function* () {
      const operations = yield* Operations.MemberOperations;
      yield* operations.submitBody(body);
      expect(order).toEqual(["capture", "submit"]);
      expect(inputs).toEqual([{ kind: "operation", body, evidence: proof }]);
    }).pipe(Effect.provide(Operations.layer.pipe(Layer.provide(Layer.mergeAll(owner, evidence)))));
  },
);

it.effect("provider refusal cannot accept or dispatch a fresh primary placement", () => {
  let submitted = 0;
  const owner = forwardingOwner(() =>
    Effect.sync(() => {
      submitted++;
      return pendingView;
    }),
  );
  const evidence = Layer.succeed(Operations.MemberOperationEvidence, {
    capture: () => Effect.fail(new RegistryError({ code: "provider_unconfigured" })),
  });
  return Effect.gen(function* () {
    const operations = yield* Operations.MemberOperations;
    expect(yield* Effect.result(operations.submitBody(canonicalJson(primary)))).toMatchObject({
      _tag: "Failure",
      failure: { code: "provider_unconfigured" },
    });
    expect(submitted).toBe(0);
  }).pipe(Effect.provide(Operations.layer.pipe(Layer.provide(Layer.mergeAll(owner, evidence)))));
});

it.effect("replaying a retained placement never asks for replacement evidence", () => {
  const inputs: Forwarder.ForwardSubmission[] = [];
  const body = JSON.stringify(primary, null, 2);
  const owner = forwardingOwner(
    (input) =>
      Effect.sync(() => {
        inputs.push(input);
        return pendingView;
      }),
    pendingView,
  );
  const evidence = Layer.succeed(Operations.MemberOperationEvidence, {
    capture: () => Effect.die("Replay must use its original retained verification envelope."),
  });
  return Effect.gen(function* () {
    const operations = yield* Operations.MemberOperations;
    yield* operations.submitBody(body);
    expect(inputs).toEqual([{ kind: "operation", body }]);
  }).pipe(Effect.provide(Operations.layer.pipe(Layer.provide(Layer.mergeAll(owner, evidence)))));
});
