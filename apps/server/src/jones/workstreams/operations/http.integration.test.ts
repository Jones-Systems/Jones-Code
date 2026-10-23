// @effect-diagnostics globalFetch:off -- Isolated loopback listeners exercise production HTTP, gateway, and signed CP transport.
import * as Effect from "effect/Effect";
import type { WorkstreamCommand } from "@t3tools/contracts";
import { expect, it } from "@effect/vitest";
import { parseWorkstreamOperationResult } from "@t3tools/contracts/jones/workstreamOperations";
import type {
  WorkstreamCreateRequest,
  WorkstreamSetPrimaryRequest,
} from "@t3tools/contracts/jones/workstreamOperations";
import { startLoopbackControlPlane, startEnvironment } from "./loopbackFixture.ts";
import { canonicalJson } from "./encoding.ts";
import { hash } from "./testFixtures.ts";

const create: WorkstreamCreateRequest = {
  contract: "workstream-operations/1.0.0",
  operation_id: "loopback-create-0001",
  origin: { kind: "app" },
  kind: "create_workstream",
  registry: { registry_id: "11111111-1111-4111-8111-111111111111", owner_id: "owner-fixture" },
  workstream: {
    name: "Operations loopback",
    lifecycle: "active",
    progress: { state: "progressing" },
    sort_order: 0,
  },
};
const submit = (
  origin: string,
  request: WorkstreamCreateRequest | WorkstreamSetPrimaryRequest,
  wait_ms = 3000,
) =>
  fetch(`${origin}/api/workstreams/operations/submit`, {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify({ request, wait_ms }),
    signal: AbortSignal.timeout(10_000),
  });

it("uses the production service over two loopback HTTP boundaries for create, replay, digest, and 409", async () => {
  const cp = await startLoopbackControlPlane();
  try {
    const environment = await startEnvironment(cp, "source-a");
    try {
      const response = await submit(environment.origin, create);
      expect(response.status).toBe(200);
      const result = parseWorkstreamOperationResult(await response.json());
      expect(result.state, JSON.stringify(result)).toBe("committed");
      expect(result.request_sha256).toBe(hash(canonicalJson(create)));
      const calls = { ...cp.observations };
      const replay = await submit(environment.origin, create, 0);
      expect(await replay.json()).toEqual(result);
      expect(cp.observations.submissions).toBe(calls.submissions);
      const read = await fetch(`${environment.origin}/api/workstreams/operations/read`, {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({ operation_id: create.operation_id }),
      });
      expect(await read.json()).toEqual({ operation: result });
      expect(cp.observations.capabilities).toBe(calls.capabilities);
      expect(cp.observations.receiptReads).toBe(calls.receiptReads);
      const conflict = await submit(environment.origin, {
        ...create,
        workstream: { ...create.workstream, name: "Different" },
      });
      expect(conflict.status).toBe(409);
      expect(await conflict.json()).toEqual({ code: "operation_id_conflict" });
      const invalid = await submit(environment.origin, create, 3001);
      expect(invalid.status).toBe(400);
      expect(cp.observations.submissions).toBe(calls.submissions);
    } finally {
      await environment.close();
    }
    const restarted = await startEnvironment(cp, "source-a");
    try {
      const recovered = parseWorkstreamOperationResult(
        await (await submit(restarted.origin, create)).json(),
      );
      expect(recovered.state).toBe("committed");
      expect(cp.observations.submissions).toBe(1);
    } finally {
      await restarted.close();
    }
  } finally {
    await cp.close();
  }
});

it("registers and verifies a new thread through production adapters and never writes on read", async () => {
  const cp = await startLoopbackControlPlane();
  try {
    const environment = await startEnvironment(cp, "source-a");
    try {
      const assignment: WorkstreamSetPrimaryRequest = {
        contract: create.contract,
        operation_id: "loopback-assign-0001",
        origin: { kind: "app" },
        kind: "set_primary",
        thread: { environment_id: "source-a", thread_id: "new-thread" },
        target: { ...create.registry, kind: "workstream", workstream_id: "ws-follow-on" },
        expected: { kind: "unassigned" },
      };
      const result = parseWorkstreamOperationResult(
        await (await submit(environment.origin, assignment)).json(),
      );
      expect(result.state, JSON.stringify(result)).toBe("committed");
      expect(result.children.map((c) => c.operation)).toEqual([
        "register_reference",
        "verify_reference",
        "attach_primary",
      ]);
      const writes = cp.observations.submissions;
      const read = await fetch(`${environment.origin}/api/workstreams/operations/read`, {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({ thread: assignment.thread }),
      });
      expect(await read.json()).toMatchObject({
        assignment: { kind: "primary", workstream_id: "ws-follow-on" },
        open_operation: null,
      });
      expect(cp.observations.submissions).toBe(writes);
    } finally {
      await environment.close();
    }
  } finally {
    await cp.close();
  }
});

it("enforces read/operate scope before touching the operation service", async () => {
  const cp = await startLoopbackControlPlane();
  try {
    const reader = await startEnvironment(
      cp,
      "source-a",
      undefined,
      new Set(["orchestration:read"]),
    );
    try {
      expect((await submit(reader.origin, create)).status).toBe(403);
      expect((await fetch(`${reader.origin}/api/workstreams/setup-status`)).status).toBe(200);
      expect(cp.observations.submissions).toBe(0);
    } finally {
      await reader.close();
    }
    const noScope = await startEnvironment(cp, "source-a", undefined, new Set());
    try {
      const before = cp.observations.capabilities;
      expect((await fetch(`${noScope.origin}/api/workstreams/setup-status`)).status).toBe(403);
      expect(cp.observations.capabilities).toBe(before);
    } finally {
      await noScope.close();
    }
  } finally {
    await cp.close();
  }
});

it("returns within the HTTP wait budget, retains background work, and returns a closed thread conflict", async () => {
  const cp = await startLoopbackControlPlane();
  let release = () => {};
  cp.faults.commandGate = new Promise<void>((resolve) => {
    release = resolve;
  });
  const entered = new Promise<void>((resolve) => {
    cp.faults.commandEntered = resolve;
  });
  try {
    const environment = await startEnvironment(cp, "source-a");
    try {
      const assignment: WorkstreamSetPrimaryRequest = {
        contract: create.contract,
        operation_id: "loopback-budget-001",
        origin: { kind: "app" },
        kind: "set_primary",
        thread: { environment_id: "source-a", thread_id: "budget-thread" },
        target: { ...create.registry, kind: "workstream", workstream_id: "ws-follow-on" },
        expected: { kind: "unassigned" },
      };
      const response = await submit(environment.origin, assignment, 0);
      expect(response.status).toBe(200);
      expect(parseWorkstreamOperationResult(await response.json()).state).toBe("in_progress");
      await entered;
      const conflict = await submit(
        environment.origin,
        { ...assignment, operation_id: "loopback-budget-002" },
        0,
      );
      expect(conflict.status).toBe(409);
      expect(await conflict.json()).toEqual({
        code: "thread_operation_in_progress",
        open_operation_id: assignment.operation_id,
      });
      const reads = cp.observations.capabilities;
      const memory = await fetch(`${environment.origin}/api/workstreams/operations/read`, {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({ open: true }),
      });
      expect(await memory.json()).toMatchObject({
        operations: [{ operation_id: assignment.operation_id }],
      });
      expect(cp.observations.capabilities).toBe(reads);
      release();
      const finished = parseWorkstreamOperationResult(
        await (await submit(environment.origin, assignment)).json(),
      );
      expect(finished.state).toBe("committed");
    } finally {
      release();
      await environment.close();
    }
  } finally {
    release();
    await cp.close();
  }
});

it("adopts a lost create reply and still creates once across a server restart", async () => {
  const cp = await startLoopbackControlPlane();
  cp.faults.commandResponse = "lost";
  try {
    const environment = await startEnvironment(cp, "source-a");
    let workstream: string | null | undefined;
    try {
      const result = parseWorkstreamOperationResult(
        await (await submit(environment.origin, create)).json(),
      );
      expect(result.state).toBe("committed");
      workstream = result.result?.workstream_id;
    } finally {
      await environment.close();
    }
    cp.faults.commandResponse = "valid";
    const restarted = await startEnvironment(cp, "source-a");
    try {
      const result = parseWorkstreamOperationResult(
        await (await submit(restarted.origin, create)).json(),
      );
      expect(result.state).toBe("committed");
      expect(result.result?.workstream_id).toBe(workstream);
      expect(cp.observations.submissions).toBe(1);
    } finally {
      await restarted.close();
    }
  } finally {
    await cp.close();
  }
});

it.live(
  "INV-6-prime: every possible late action is rejected for every settled expected/target/current combination",
  () =>
    Effect.gen(function* () {
      const cp = yield* Effect.acquireRelease(
        Effect.promise(() => startLoopbackControlPlane()),
        (cp) => Effect.promise(() => cp.close()),
      );
      const environment = yield* Effect.acquireRelease(
        Effect.promise(() => startEnvironment(cp, "source-a")),
        (environment) => Effect.promise(() => environment.close()),
      );
      const ids = ["ws-core-v1", "ws-follow-on"] as const;
      let sequence = 0;
      for (const current of [null, ...ids]) {
        const thread = { environment_id: "source-a", thread_id: `property-thread-${++sequence}` };
        const bootstrap: WorkstreamSetPrimaryRequest = {
          contract: create.contract,
          kind: "set_primary",
          operation_id: `property-bootstrap-${sequence}`,
          origin: { kind: "app" },
          thread,
          target: current
            ? { ...create.registry, kind: "workstream", workstream_id: current }
            : { ...create.registry, kind: "unassigned" },
          expected: { kind: "any" },
        };
        const ready = parseWorkstreamOperationResult(
          yield* Effect.promise(async () => (await submit(environment.origin, bootstrap)).json()),
        );
        expect(["committed", "already_satisfied"]).toContain(ready.state);
        for (const target of [null, ...ids])
          for (const expected of [
            { kind: "any" },
            { kind: "unassigned" },
            ...ids.map((workstream_id) => ({ kind: "primary" as const, workstream_id })),
          ] as const) {
            const isSettled =
              current === target ||
              (expected.kind === "unassigned" && current !== null) ||
              (expected.kind === "primary" && current !== expected.workstream_id);
            if (!isSettled) continue;
            const operation: WorkstreamSetPrimaryRequest = {
              ...bootstrap,
              operation_id: `property-operation-${++sequence}`,
              target: target
                ? { ...create.registry, kind: "workstream", workstream_id: target }
                : { ...create.registry, kind: "unassigned" },
              expected,
            };
            const result = parseWorkstreamOperationResult(
              yield* Effect.promise(async () =>
                (await submit(environment.origin, operation)).json(),
              ),
            );
            expect(result.state).toBe(current === target ? "already_satisfied" : "conflict");
            const binding = yield* environment.gateway.readSession();
            const groups = yield* Effect.all(
              ids.map((id) => environment.gateway.readDetail(id)),
              { concurrency: "unbounded" },
            );
            const version = (id: string) =>
              groups.find((g) => g.workstream.workstream_id === id)!.workstream.version;
            const actions: WorkstreamCommand["action"][] = [];
            if (target && (expected.kind === "any" || expected.kind === "unassigned")) {
              const reference = yield* environment.gateway.readReferences();
              const native_reference_id = reference.items.find(
                (r) => r.identity.native_id === thread.thread_id,
              )!.native_reference_id;
              for (const operation of ["attach_primary", "reattach_primary"] as const)
                actions.push({
                  operation,
                  workstream_id: target,
                  expected_version: version(target),
                  native_reference_id,
                });
            }
            if (expected.kind !== "unassigned")
              for (const source of ids) {
                if (
                  source === target ||
                  (expected.kind === "primary" && expected.workstream_id !== source)
                )
                  continue;
                actions.push(
                  target
                    ? {
                        operation: "move_primary",
                        source_workstream_id: source,
                        expected_source_version: version(source),
                        source_membership_id: "no-open-membership",
                        destination_workstream_id: target,
                        expected_destination_version: version(target),
                      }
                    : {
                        operation: "remove_membership",
                        workstream_id: source,
                        expected_version: version(source),
                        membership_id: "no-open-membership",
                      },
                );
              }
            for (const action of actions) {
              const receipt = yield* environment.gateway.submit({
                command_id: `property-late-action-${++sequence}`,
                expected_registry_version: binding.registryVersion,
                expected_server_generation: binding.serverGeneration,
                action,
              });
              expect(receipt.state).toBe("rejected");
            }
            expect((yield* environment.gateway.readSession()).registryVersion).toBe(
              binding.registryVersion,
            );
          }
      }
    }),
  30_000,
);
