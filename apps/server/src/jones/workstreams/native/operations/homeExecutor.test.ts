import type { WorkstreamCommand } from "@t3tools/contracts";
import type { WorkstreamsNativeReceiptLookup } from "@t3tools/contracts/jones/workstreamsNative";
import { canonicalJson } from "../../operations/encoding.ts";
import { hash as digest } from "../../operations/testFixtures.ts";
import { expect, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Layer from "effect/Layer";
import * as Bindings from "../store/bindingStore.ts";
import { testLayer, seed, binding, pending, now, hash, scope } from "../store/sql.testkit.ts";
import * as Home from "./homeExecutor.ts";
import type { Operation } from "../store/operationStore.ts";

it.effect(
  "unsupported legacy transport evidence never becomes receipt or resubmission permission",
  () => {
    let dispatches = 0;
    const operation: Operation = {
      operation_id: "operation:test",
      kind: "command",
      thread_environment_id: null,
      thread_id: null,
      origin: { kind: "agent" },
      destination: binding,
      child_command_ids: ["command:0"],
      request_body: "{}",
      request_sha256: hash,
      accepted_at: now,
      updated_at: now,
      state: "reconciling",
      next_poll_at: null,
      result_body: null,
    };
    const layer = Home.layer.pipe(
      Layer.provide(
        Layer.succeed(Home.HomeTransport, {
          lookup: () =>
            Effect.succeed({
              kind: "present",
              source: "legacy_archive",
              receipt: { ...pending("command:valid-0000").receipt, command_id: "command:0" },
            }),
          resubmit: () => {
            dispatches++;
            return Effect.die("unexpected redispatch");
          },
        }),
      ),
      Layer.provideMerge(testLayer()),
    );
    return Effect.gen(function* () {
      yield* seed;
      const bindings = yield* Bindings.BindingStore;
      yield* bindings.bind(binding, null);
      const home = yield* Home.HomeExecutor;
      expect(Exit.isFailure(yield* Effect.exit(home.readReceipt(operation, "command:0")))).toBe(
        true,
      );
      expect(Exit.isFailure(yield* Effect.exit(home.dispatch(operation, "command:0", "{}")))).toBe(
        true,
      );
      expect(dispatches).toBe(0);
    }).pipe(Effect.provide(layer));
  },
);

it.effect(
  "dispatch looks up the pinned home first and resubmits only identical persisted bytes after proven absence",
  () => {
    const command: WorkstreamCommand = {
      command_id: "command:exact-000000",
      expected_server_generation: 1,
      expected_registry_version: 0,
      action: {
        operation: "create_workstream",
        name: "synthetic",
        lifecycle: "active",
        progress: { state: "progressing" },
        sort_order: 0,
      },
    };
    const body = canonicalJson(command);
    const receipt = {
      ...pending(command.command_id, body).receipt,
      operation: "create_workstream" as const,
    };
    const operation: Operation = {
      operation_id: "operation-home-proof-0001",
      kind: "create_workstream",
      origin: { kind: "app" },
      thread_environment_id: null,
      thread_id: null,
      destination: binding,
      request_body: "{}",
      request_sha256: digest("{}"),
      child_command_ids: [command.command_id],
      accepted_at: now,
      updated_at: now,
      state: "dispatching",
      next_poll_at: null,
      result_body: canonicalJson({
        version: 1,
        regAbsent: false,
        unknown: {
          command,
          body,
          digest: digest(body),
          kind: "cre",
          ownerId: scope.owner_id,
          principalId: binding.principal_id,
        },
        result: {
          contract: "workstream-operations/1.0.0",
          operation_id: "operation-home-proof-0001",
          kind: "create_workstream",
          origin: { kind: "app" },
          request_sha256: digest("{}"),
          state: "in_progress",
          reason: null,
          detail: null,
          result: null,
          observed: null,
          accepted_at: now,
          updated_at: now,
          poll_after_ms: 250,
          children: [],
        },
      }),
    };
    let lookup: WorkstreamsNativeReceiptLookup = { kind: "present", source: "native", receipt };
    const trace: string[] = [];
    const layer = Home.layer.pipe(
      Layer.provide(
        Layer.succeed(Home.HomeTransport, {
          lookup: (target, id) =>
            Effect.sync(() => {
              expect(target).toEqual(binding);
              expect(id).toBe(command.command_id);
              trace.push("lookup");
              return lookup;
            }),
          resubmit: (target, id, actual) =>
            Effect.sync(() => {
              expect(target).toEqual(binding);
              expect(id).toBe(command.command_id);
              expect(actual).toBe(body);
              trace.push("dispatch");
              return { kind: "receipt" as const, receipt };
            }),
        }),
      ),
      Layer.provideMerge(testLayer()),
    );
    return Effect.gen(function* () {
      yield* seed;
      yield* (yield* Bindings.BindingStore).bind(binding, null);
      const home = yield* Home.HomeExecutor;
      expect(yield* home.dispatch(operation, command.command_id, body)).toEqual({
        kind: "receipt",
        receipt,
      });
      expect(trace).toEqual(["lookup"]);
      lookup = { kind: "unknown", reason: "import_incomplete" };
      expect(
        Exit.isFailure(yield* Effect.exit(home.dispatch(operation, command.command_id, body))),
      ).toBe(true);
      lookup = {
        kind: "proven_absent",
        witness: { archive: "complete", server_generation: 2, registry_version: 0 },
      };
      expect(
        Exit.isFailure(yield* Effect.exit(home.dispatch(operation, command.command_id, body))),
      ).toBe(true);
      lookup = {
        kind: "proven_absent",
        witness: { archive: "complete", server_generation: 1, registry_version: 0 },
      };
      yield* home.dispatch(operation, command.command_id, body);
      expect(trace).toEqual(["lookup", "lookup", "lookup", "lookup", "dispatch"]);
      expect(
        Exit.isFailure(
          yield* Effect.exit(home.dispatch(operation, command.command_id, body + " ")),
        ),
      ).toBe(true);
      expect(trace.at(-1)).toBe("dispatch");
    }).pipe(Effect.provide(layer));
  },
);

it.effect("tagged member intent never enters the home executor transport", () => {
  let calls = 0;
  const operation: Operation & { member_forward: unknown } = {
    operation_id: "operation:member-fence",
    kind: "command",
    thread_environment_id: null,
    thread_id: null,
    origin: { kind: "agent" },
    destination: binding,
    child_command_ids: ["command:member-fence"],
    request_body: "{}",
    request_sha256: hash,
    accepted_at: now,
    updated_at: now,
    state: "reconciling",
    next_poll_at: null,
    result_body: null,
    member_forward: {},
  };
  const layer = Home.layer.pipe(
    Layer.provide(
      Layer.succeed(Home.HomeTransport, {
        lookup: () => {
          calls++;
          return Effect.succeed({ kind: "unknown" as const, reason: "import_incomplete" });
        },
        resubmit: () => {
          calls++;
          return Effect.die("unexpected member redispatch through home executor");
        },
      }),
    ),
    Layer.provideMerge(testLayer()),
  );
  return Effect.gen(function* () {
    yield* seed;
    yield* (yield* Bindings.BindingStore).bind(binding, null);
    const home = yield* Home.HomeExecutor;
    expect(
      Exit.isFailure(yield* Effect.exit(home.readReceipt(operation, "command:member-fence"))),
    ).toBe(true);
    expect(
      Exit.isFailure(yield* Effect.exit(home.dispatch(operation, "command:member-fence", "{}"))),
    ).toBe(true);
    expect(calls).toBe(0);
  }).pipe(Effect.provide(layer));
});
