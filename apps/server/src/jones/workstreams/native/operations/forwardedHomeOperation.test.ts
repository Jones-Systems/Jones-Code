import { expect, it } from "@effect/vitest";
import * as Crypto from "@effect/platform-node/NodeCrypto";
import * as Effect from "effect/Effect";
import type { WorkstreamsForwardRequest } from "@t3tools/contracts/jones/workstreamsForwarding";
import { digest, forwardChildIds } from "./forwardedHomeOperation.ts";
import { canonicalJson } from "../../operations/encoding.ts";
it.effect("frozen native child plan binds RAW bytes while public v1 digest stays canonical", () =>
  Effect.gen(function* () {
    const op = {
      contract: "workstream-operations/1.0.0",
      operation_id: "operation-000000000001",
      origin: { kind: "agent" },
      kind: "create_workstream",
      registry: { registry_id: "11111111-1111-4111-8111-111111111111", owner_id: "owner" },
      workstream: {
        name: "name",
        lifecycle: "active",
        progress: { state: "unknown" },
        sort_order: 0,
      },
    };
    const request = (body: string) =>
      ({
        kind: "operation",
        identity: {
          source_authority: { environment_id: "source" },
          source_operation_id: op.operation_id,
          forward_id: "jwf-" + digest("source\0" + op.operation_id).slice(0, 48),
        },
        request: { request: op },
        request_body: body,
      }) as Extract<WorkstreamsForwardRequest, { kind: "operation" }>;
    const compact = JSON.stringify(op),
      pretty = JSON.stringify(op, null, 2);
    const a = yield* forwardChildIds(request(compact)),
      b = yield* forwardChildIds(request(pretty));
    expect(a).toHaveLength(1);
    const otherForward = request(compact);
    Object.assign(otherForward.identity, { forward_id: "jwf-" + "e".repeat(48) });
    expect(yield* forwardChildIds(otherForward)).toEqual(a);
    const otherOperation = request(compact);
    Object.assign(otherOperation, {
      request: {
        ...otherOperation.request,
        request: { ...otherOperation.request.request, operation_id: "operation-000000000002" },
      },
    });
    expect(yield* forwardChildIds(otherOperation)).not.toEqual(a);
    expect(a[0]).toMatch(/^jwo-[a-f0-9]{40}-cre-1$/);
    expect(a).not.toEqual(b);
    expect(digest(canonicalJson(JSON.parse(compact)))).toBe(
      digest(canonicalJson(JSON.parse(pretty))),
    );
    expect(digest(compact)).not.toBe(digest(pretty));
    const primary = request(pretty);
    Object.assign(primary.request.request, { kind: "set_primary" });
    const ids = yield* forwardChildIds(primary);
    expect(ids).toHaveLength(11);
    expect(ids[0]).toMatch(/-reg-1$/);
    expect(ids[1]).toMatch(/-ver-1$/);
    expect(ids.at(-1)).toMatch(/-mem-9$/);
    expect(new Set(ids).size).toBe(11);
  }).pipe(Effect.provide(Crypto.layer)),
);
