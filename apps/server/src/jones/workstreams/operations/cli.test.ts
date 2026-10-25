import {
  FetchHttpClient,
  HttpClientError,
  HttpClientRequest,
  HttpClientResponse,
} from "effect/http";
import { expect, it } from "@effect/vitest";
import * as NodeServices from "@effect/platform-node/NodeServices";
import * as Operations from "@t3tools/contracts/jones/workstreamOperations";
import * as Effect from "effect/Effect";
import * as Cause from "effect/Cause";
import * as Exit from "effect/Exit";
import * as Layer from "effect/Layer";
import * as Schema from "effect/Schema";
import * as TestConsole from "effect/testing/TestConsole";
import { Command } from "effect/cli";
import corpus from "../../../../../../packages/contracts/contracts/workstream-operations/v1/fixtures/conformance.json" with { type: "json" };
import { workstreamCommand } from "../../../cli/workstream.ts";
import {
  dispatchWorkstreamCli,
  makeWorkstreamCliHttpPort,
  workstreamCliRequestError,
  formatWorkstreamCliResult,
  makeWorkstreamOperationCommands,
  WorkstreamCliError,
  type WorkstreamCliInput,
  type WorkstreamCliPort,
} from "./cli.ts";

import { startLoopbackControlPlane, startEnvironment } from "./loopbackFixture.ts";

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

it.live(
  "executes parsed CLI commands through the production HTTP adapter and operation service",
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
      const results: unknown[] = [];
      const cli = Command.make("workstream").pipe(
        Command.withSubcommands(
          makeWorkstreamOperationCommands((_flags, input) =>
            Effect.gen(function* () {
              const api = yield* makeWorkstreamCliHttpPort(environment.origin, {
                authorization: "Bearer synthetic",
              });
              results.push(yield* dispatchWorkstreamCli(api, input));
            }),
          ),
        ),
      );
      const run = (args: string[]) =>
        Command.runWith(cli, { version: "test" })(args).pipe(
          Effect.provide(
            Layer.mergeAll(NodeServices.layer, TestConsole.layer, FetchHttpClient.layer),
          ),
        );
      yield* run(["status", "--json"]);
      const setup = Operations.parseWorkstreamSetupStatus(results.at(-1));
      expect(setup.registry.registry_id).toBe(cp.authority.registry_instance_id);
      yield* run(["list", "--limit", "1"]);
      const registry = {
        registry_id: setup.registry.registry_id!,
        owner_id: setup.registry.owner_id!,
      };
      const create: Operations.WorkstreamCreateRequest = {
        contract: Operations.WORKSTREAM_OPERATIONS_CONTRACT,
        kind: "create_workstream",
        operation_id: "cli-http-create-0001",
        origin: { kind: "cli" },
        registry,
        workstream: {
          name: "CLI HTTP",
          lifecycle: "active",
          progress: { state: "progressing" },
          sort_order: 0,
        },
      };
      yield* run(["create", "--request", JSON.stringify(create)]);
      const created = Operations.parseWorkstreamOperationResult(results.at(-1));
      expect(created.state).toBe("committed");
      const writes = cp.observations.submissions;
      yield* run(["create", "--request", JSON.stringify(create)]);
      expect(results.at(-1)).toEqual(created);
      expect(cp.observations.submissions).toBe(writes);
      const assignment: Operations.WorkstreamSetPrimaryRequest = {
        contract: create.contract,
        kind: "set_primary",
        operation_id: "cli-http-assign-0001",
        origin: { kind: "cli" },
        thread: { environment_id: "source-a", thread_id: "cli-thread" },
        target: { ...registry, kind: "workstream", workstream_id: created.result!.workstream_id! },
        expected: { kind: "unassigned" },
      };
      yield* run(["assign", "--request", JSON.stringify(assignment)]);
      expect(Operations.parseWorkstreamOperationResult(results.at(-1)).state).toBe("committed");
      const beforeRead = cp.observations.submissions;
      yield* run(["status", "--thread", "cli-thread", "--environment", "source-a"]);
      expect(results.at(-1)).toMatchObject({
        assignment: { kind: "primary", workstream_id: created.result!.workstream_id },
      });
      yield* run(["status", assignment.operation_id]);
      expect(results.at(-1)).toMatchObject({ operation: { state: "committed" } });
      expect(cp.observations.submissions).toBe(beforeRead);
      const move: Operations.WorkstreamSetPrimaryRequest = {
        ...assignment,
        operation_id: "cli-http-move-00001",
        target: { ...registry, kind: "workstream", workstream_id: "ws-follow-on" },
        expected: { kind: "primary", workstream_id: created.result!.workstream_id! },
      };
      yield* run(["assign", "--request", JSON.stringify(move)]);
      expect(Operations.parseWorkstreamOperationResult(results.at(-1)).state).toBe("committed");
      yield* run([
        "unassign",
        "--request",
        JSON.stringify({
          ...assignment,
          operation_id: "cli-http-unassign-01",
          target: { ...registry, kind: "unassigned" },
          expected: { kind: "primary", workstream_id: "ws-follow-on" },
        }),
      ]);
      expect(Operations.parseWorkstreamOperationResult(results.at(-1)).state).toBe("committed");
      yield* run(["status", "--open"]);
      expect(results.at(-1)).toEqual({ operations: [] });
      const beforeForeign = cp.observations.submissions;
      const foreignExit = yield* run([
        "status",
        "--thread",
        "cli-thread",
        "--environment",
        "foreign",
      ]).pipe(Effect.exit);
      expect(Exit.isFailure(foreignExit)).toBe(true);
      if (Exit.isFailure(foreignExit)) {
        expect(() => {
          throw Cause.squash(foreignExit.cause);
        }).toThrow("foreign_thread");
      }
      expect(cp.observations.submissions).toBe(beforeForeign);
    }),
);

it.live("preserves typed HTTP conflicts and CONFIG request identity on resubmit", () =>
  Effect.gen(function* () {
    const cp = yield* Effect.acquireRelease(
      Effect.promise(() => startLoopbackControlPlane()),
      (cp) => Effect.promise(() => cp.close()),
    );
    const environment = yield* Effect.acquireRelease(
      Effect.promise(() => startEnvironment(cp, "source-a")),
      (environment) => Effect.promise(() => environment.close()),
    );
    yield* Effect.gen(function* () {
      const api = yield* makeWorkstreamCliHttpPort(environment.origin, {
        authorization: "Bearer synthetic",
      });
      const setup = yield* api.setupStatus();
      const create: Operations.WorkstreamCreateRequest = {
        contract: Operations.WORKSTREAM_OPERATIONS_CONTRACT,
        kind: "create_workstream",
        operation_id: "cli-http-conflict-01",
        origin: { kind: "cli" },
        registry: {
          registry_id: setup.registry.registry_id!,
          owner_id: setup.registry.owner_id!,
        },
        workstream: {
          name: "CLI conflict",
          lifecycle: "active",
          progress: { state: "progressing" },
          sort_order: 0,
        },
      };
      expect((yield* api.submit({ request: create, wait_ms: 3000 })).state).toBe("committed");
      const conflict = yield* api
        .submit({
          request: { ...create, workstream: { ...create.workstream, name: "Changed" } },
          wait_ms: 0,
        })
        .pipe(Effect.flip);
      expect(conflict.code).toBe("operation_id_conflict");
      expect(conflict.operation_id).toBe(create.operation_id);
      const foreign: Operations.WorkstreamSetPrimaryRequest = {
        contract: create.contract,
        kind: "set_primary",
        operation_id: "cli-http-foreign-01",
        origin: { kind: "cli" },
        thread: { environment_id: "foreign", thread_id: "cli-thread" },
        target: { ...create.registry, kind: "unassigned" },
        expected: { kind: "any" },
      };
      const writes = cp.observations.submissions;
      for (let attempt = 0; attempt < 2; attempt++) {
        const result = yield* dispatchWorkstreamCli(api, {
          command: "unassign",
          request: foreign,
          wait_ms: 3000,
        });
        expect(result).toMatchObject({
          state: "rejected",
          reason: "foreign_thread",
          operation_id: foreign.operation_id,
        });
        expect(formatWorkstreamCliResult(result)).toContain("SAME operation ID");
      }
      expect(cp.observations.submissions).toBe(writes);
    }).pipe(Effect.provide(FetchHttpClient.layer));
  }),
);

it("requires the same ID for every CONFIG and open result, and a new ID after contention", () => {
  for (const [state, reason] of [
    ["in_progress", null],
    ["reconciling", "membership_effect_unknown"],
    ["rejected", "foreign_thread"],
    ["rejected", "registry_mismatch"],
    ["rejected", "permission_denied"],
    ["unavailable", "offline"],
    ["unavailable", "not_configured"],
    ["unavailable", "registration_unavailable"],
  ] as const) {
    const value = Operations.parseWorkstreamOperationResult({
      ...result,
      state,
      reason,
      detail:
        reason === "registration_unavailable" ? { availability: "authority_unavailable" } : null,
      poll_after_ms: state === "in_progress" || state === "reconciling" ? 30000 : null,
    });
    expect(formatWorkstreamCliResult(value)).toContain("SAME operation ID");
  }
  const contention = Operations.parseWorkstreamOperationResult({
    ...result,
    state: "unavailable",
    reason: "contention",
    poll_after_ms: null,
  });
  expect(formatWorkstreamCliResult(contention)).toContain("start a new operation ID");
});
