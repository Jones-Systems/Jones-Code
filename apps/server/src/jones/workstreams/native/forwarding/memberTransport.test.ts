import { expect, it } from "@effect/vitest";
import { WorkstreamCommand } from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Schema from "effect/Schema";
import * as Forwarder from "./forwarder.ts";
import * as Transport from "./memberTransport.ts";

const command = Schema.decodeUnknownSync(WorkstreamCommand)({
  command_id: "command:transport-001",
  expected_server_generation: 1,
  expected_registry_version: 0,
  action: {
    operation: "create_workstream",
    name: "test",
    lifecycle: "active",
    progress: { state: "unknown" },
    sort_order: 0,
  },
});
it.effect(
  "Gateway command seam preserves exact body and refuses command/idempotency substitution",
  () => {
    const inputs: Forwarder.ForwardSubmission[] = [];
    const owner = Layer.succeed(Forwarder.MemberForwarder, {
      read: () => Effect.succeed(null),
      open: () => Effect.succeed([]),
      recover: () => Effect.succeed(null),
      recoverOpen: () => Effect.succeed([]),
      submit: (input) => {
        inputs.push(input);
        return Effect.succeed({
          operation_id: command.command_id,
          state: "reconciling",
          next_poll_at: null,
          accepted_at: "2026-01-01T00:00:00.000Z",
          updated_at: "2026-01-01T00:00:00.000Z",
          result: null,
        });
      },
    });
    return Effect.gen(function* () {
      const transport = yield* Transport.MemberWriteTransport;
      const body = " \n" + JSON.stringify(command);
      expect(
        (yield* transport.submitCommand({ body, command, idempotencyKey: command.command_id }))
          .state,
      ).toBe("reconciling");
      expect(inputs).toEqual([{ kind: "command", body }]);
      expect(
        (yield* Effect.exit(transport.submitCommand({ body, command, idempotencyKey: "other" })))
          ._tag,
      ).toBe("Failure");
      expect(inputs).toHaveLength(1);
    }).pipe(Effect.provide(Transport.layer.pipe(Layer.provide(owner))));
  },
);
