import { HttpClientError, HttpClientRequest, HttpClientResponse } from "effect/http";
import { expect, it } from "@effect/vitest";
import * as NodeServices from "@effect/platform-node/NodeServices";
import * as Operations from "@t3tools/contracts/jones/workstreamOperations";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Schema from "effect/Schema";
import * as TestConsole from "effect/testing/TestConsole";
import { Command } from "effect/cli";
import corpus from "../../../../../../packages/contracts/contracts/workstream-operations/v1/fixtures/conformance.json";
import { workstreamCommand } from "../../../cli/workstream.ts";
import {
  dispatchWorkstreamCli,
  workstreamCliRequestError,
  formatWorkstreamCliResult,
  makeWorkstreamOperationCommands,
  WorkstreamCliError,
  type WorkstreamCliInput,
  type WorkstreamCliPort,
} from "./cli.ts";

const request = Operations.parseWorkstreamOperationRequest({
  ...corpus.cases.find((c) => c.name === "set-primary")!.value,
  origin: { kind: "cli" },
});
const result: Operations.WorkstreamOperationResult = {
  contract: request.contract,
  operation_id: request.operation_id,
  origin: request.origin,
  kind: request.kind,
  request_sha256: "a".repeat(64),
  state: "reconciling",
  reason: "membership_effect_unknown",
  detail: null,
  observed: null,
  result: null,
  poll_after_ms: 30000,
  children: [],
  accepted_at: "2026-10-23T00:00:00Z",
  updated_at: "2026-10-23T00:00:00Z",
};
const port = (submit: WorkstreamCliPort["submit"]): WorkstreamCliPort => ({
  submit,
  list: () => Effect.die("unexpected list"),
  read: () => Effect.succeed({ operation: result }),
  setupStatus: () => Effect.die("unexpected setup"),
});

it("registers executable commands alongside existing enrollment commands", () => {
  expect(
    workstreamCommand.subcommands.flatMap((group) => group.commands.map((command) => command.name)),
  ).toEqual(
    expect.arrayContaining([
      "authority",
      "provider",
      "list",
      "create",
      "assign",
      "unassign",
      "status",
    ]),
  );
});

it.effect("parses assign and status through the real command assembly", () =>
  Effect.gen(function* () {
    const seen: WorkstreamCliInput[] = [];
    const cli = Command.make("workstream").pipe(
      Command.withSubcommands(
        makeWorkstreamOperationCommands((_flags, input) =>
          Effect.sync(() => {
            seen.push(input);
          }),
        ),
      ),
    );
    const run = Command.runWith(cli, { version: "test" });
    yield* run(["assign", "--request", JSON.stringify(request), "--wait-ms", "0"]);
    yield* run(["status", request.operation_id]);
    yield* run(["status", "--thread", "thread-1", "--environment", "env-1"]);
    expect(seen).toEqual([
      { command: "assign", request, wait_ms: 0 },
      { command: "status", query: { operation_id: request.operation_id } },
      { command: "status", query: { thread: { thread_id: "thread-1", environment_id: "env-1" } } },
    ]);
  }).pipe(Effect.provide(Layer.mergeAll(NodeServices.layer, TestConsole.layer))),
);

it.effect("a lost reply can be retried with exactly the same request and ID", () =>
  Effect.gen(function* () {
    const sent: Operations.WorkstreamOperationSubmitRequest[] = [];
    const api = port((input) => {
      sent.push(input);
      return sent.length === 1
        ? Effect.fail(
            new WorkstreamCliError({
              code: "request_failed",
              operation_id: input.request.operation_id,
            }),
          )
        : Effect.succeed(result);
    });
    const input = { command: "assign" as const, request, wait_ms: 0 };
    yield* dispatchWorkstreamCli(api, input).pipe(Effect.flip);
    expect(yield* dispatchWorkstreamCli(api, input)).toEqual(result);
    expect(sent).toEqual([
      { request, wait_ms: 0 },
      { request, wait_ms: 0 },
    ]);
  }),
);

it.effect("rejects unassign with an assign target before calling the server", () =>
  Effect.gen(function* () {
    const error = yield* dispatchWorkstreamCli(
      port(() => Effect.die("must not submit")),
      { command: "unassign", request, wait_ms: 0 },
    ).pipe(Effect.flip);
    expect(error.code).toBe("invalid_request");
  }),
);

it("shows pending and setup reasons without claiming success", () => {
  expect(formatWorkstreamCliResult(result)).toContain("Not confirmed");
  expect(formatWorkstreamCliResult(result)).toContain(result.operation_id);
  for (const item of corpus.cases) {
    if (Schema.is(Operations.WorkstreamSetupStatus)(item.value)) {
      expect(formatWorkstreamCliResult(item.value)).toContain(
        item.value.t3_source.reason ?? "ready",
      );
      for (const step of item.value.next_steps)
        expect(formatWorkstreamCliResult(item.value)).toContain(step.doc);
    }
  }
  expect(new WorkstreamCliError({ code: "unsupported" }).message).toContain("Update the server");
  expect(
    new WorkstreamCliError({ code: "request_failed", operation_id: request.operation_id }).message,
  ).toContain("SAME ID");
});

it("maps old-server HTTP responses and preserves operation conflicts", () => {
  const request = HttpClientRequest.post("http://127.0.0.1/api/workstreams/operations/submit");
  const response = HttpClientResponse.fromWeb(request, new Response(null, { status: 404 }));
  const error = new HttpClientError.HttpClientError({
    reason: new HttpClientError.StatusCodeError({ request, response }),
  });
  expect(workstreamCliRequestError(error, "operation-000001").code).toBe("unsupported");
  const conflict = workstreamCliRequestError(
    { code: "thread_operation_in_progress", open_operation_id: "operation-000002" },
    "operation-000001",
  );
  expect(conflict.open_operation_id).toBe("operation-000002");
  expect(conflict.message).toContain("operation-000002");
});
