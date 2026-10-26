import { expect, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Layer from "effect/Layer";
import * as Bindings from "../store/bindingStore.ts";
import { testLayer, seed, binding, pending, now, hash } from "../store/sql.testkit.ts";
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
