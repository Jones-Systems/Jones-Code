import {
  AuthStandardClientScopes,
  EnvironmentHttpApi,
  T3WorkstreamListResult,
} from "@t3tools/contracts";
import * as Operations from "@t3tools/contracts/jones/workstreamOperations";
import * as Console from "effect/Console";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";
import { Argument, Command, Flag, GlobalFlag } from "effect/cli";
import { FetchHttpClient, HttpClientError } from "effect/http";
import * as HttpApiClient from "effect/http-api/HttpApiClient";
import * as EnvironmentAuth from "../../../auth/EnvironmentAuth.ts";
import * as ServerConfig from "../../../config.ts";
import {
  projectLocationFlags,
  resolveCliAuthConfig,
  type CliAuthLocationFlags,
} from "../../../cli/config.ts";
import { readPersistedServerRuntimeState } from "../../../serverRuntimeState.ts";

export class WorkstreamCliError extends Schema.TaggedError<WorkstreamCliError>()(
  "WorkstreamCliError",
  {
    code: Schema.Literals([
      "unsupported",
      "offline",
      "request_failed",
      "invalid_request",
      "foreign_thread",
      "operation_id_conflict",
      "thread_operation_in_progress",
    ]),
    operation_id: Schema.optional(Schema.String),
    open_operation_id: Schema.optional(Schema.String),
    cause: Schema.optional(Schema.Defect()),
  },
) {
  override get message(): string {
    const advice =
      this.code === "foreign_thread"
        ? "foreign_thread: use the server that owns this thread."
        : this.code === "thread_operation_in_progress"
          ? `Another operation is open for this thread: ${this.open_operation_id ?? "read status --open"}. Reconcile it before starting another.`
          : this.code === "operation_id_conflict"
            ? "That operation ID is already bound to a different request. Recover its original request before proceeding."
            : this.code === "unsupported"
              ? "This server does not support Workstream operations. Update the server."
              : this.code === "offline"
                ? "No running local server was found for this base directory. Start that server first."
                : this.code === "invalid_request"
                  ? "Invalid Workstream request. Check the command help and request fields."
                  : "The server request failed; the outcome may be unknown.";
    return `${advice}${this.operation_id === undefined ? "" : ` Operation ${this.operation_id}: read status, then retry the identical request with the SAME ID. Do not assume no change occurred.`}`;
  }
}

export interface WorkstreamCliPort {
  readonly list: (input: {
    readonly limit?: number;
    readonly cursor?: string;
  }) => Effect.Effect<typeof T3WorkstreamListResult.Type, WorkstreamCliError>;
  readonly submit: (
    input: Operations.WorkstreamOperationSubmitRequest,
  ) => Effect.Effect<Operations.WorkstreamOperationResult, WorkstreamCliError>;
  readonly read: (
    input: Operations.WorkstreamOperationReadRequest,
  ) => Effect.Effect<Operations.WorkstreamOperationReadResponse, WorkstreamCliError>;
  readonly setupStatus: () => Effect.Effect<Operations.WorkstreamSetupStatus, WorkstreamCliError>;
}
export type WorkstreamCliInput =
  | { readonly command: "list"; readonly limit?: number; readonly cursor?: string }
  | {
      readonly command: "create" | "assign" | "unassign";
      readonly request: Operations.WorkstreamOperationRequest;
      readonly wait_ms: number;
    }
  | { readonly command: "status"; readonly query?: Operations.WorkstreamOperationReadRequest };

/** No generated IDs, hidden retries, or client-side register/verify/mutate sequence. */
export const dispatchWorkstreamCli = (
  port: WorkstreamCliPort,
  input: WorkstreamCliInput,
): Effect.Effect<
  | T3WorkstreamListResult
  | Operations.WorkstreamOperationResult
  | Operations.WorkstreamOperationReadResponse
  | Operations.WorkstreamSetupStatus,
  WorkstreamCliError
> => {
  switch (input.command) {
    case "list":
      return port.list({
        ...(input.limit === undefined ? {} : { limit: input.limit }),
        ...(input.cursor === undefined ? {} : { cursor: input.cursor }),
      });
    case "status":
      return Effect.gen(function* () {
        if (input.query === undefined) return yield* port.setupStatus();
        if ("thread" in input.query) {
          const status = yield* port.setupStatus();
          if (input.query.thread.environment_id !== status.environment_id)
            return yield* new WorkstreamCliError({ code: "foreign_thread" });
        }
        return yield* port.read(input.query);
      });
    default:
      return Effect.gen(function* () {
        const payload = yield* Schema.decodeEffect(Operations.WorkstreamOperationSubmitRequest)({
          request: input.request,
          wait_ms: input.wait_ms,
        }).pipe(
          Effect.mapError((cause) => new WorkstreamCliError({ code: "invalid_request", cause })),
        );
        const request = payload.request;
        if (
          request.origin.kind !== "cli" ||
          (input.command === "create"
            ? request.kind !== "create_workstream"
            : request.kind !== "set_primary" ||
              request.target.kind !== (input.command === "assign" ? "workstream" : "unassigned"))
        ) {
          return yield* new WorkstreamCliError({ code: "invalid_request" });
        }
        return yield* port.submit(payload);
      });
  }
};

export const formatWorkstreamCliResult = (result: unknown): string => {
  const json = JSON.stringify(result, null, 2);
  if (Schema.is(Operations.WorkstreamOperationResult)(result)) {
    const done = result.state === "committed" || result.state === "already_satisfied";
    const next = done
      ? "Confirmed."
      : result.reason === "contention"
        ? "Contention: start a new operation ID before trying again."
        : result.state === "conflict"
          ? "Not confirmed. Inspect the observed assignment; use a new operation ID if you change the request."
          : result.state === "in_progress" ||
              result.state === "reconciling" ||
              (result.state === "rejected" &&
                ["foreign_thread", "registry_mismatch", "permission_denied"].includes(
                  result.reason,
                )) ||
              (result.state === "unavailable" &&
                ["offline", "not_configured", "registration_unavailable"].includes(result.reason))
            ? "Not confirmed. Keep the SAME operation ID and identical request; repair setup or routing if needed, then resubmit. Status does not refresh a failure."
            : "Not confirmed. This failure is sealed. Resubmit the identical request with the same ID to refresh it, or start a new operation.";
    return `${result.state}${result.reason === null ? "" : ` (${result.reason})`}: ${result.operation_id}\n${next}\n${json}`;
  }
  if (Schema.is(Operations.WorkstreamSetupStatus)(result)) {
    return `Activation: ${result.activation.state}; registry: ${result.registry.state}; source: ${result.t3_source.reason ?? "ready"}; placements: ${result.placements.readiness}\n${result.next_steps.map((step) => `${step.code}: ${step.doc}`).join("\n")}\n${json}`;
  }
  return json;
};

export const workstreamCliRequestError = (cause: unknown, operation_id?: string) => {
  if (Schema.is(Operations.EnvironmentWorkstreamOperationConflictError)(cause))
    return new WorkstreamCliError({
      ...cause,
      ...(operation_id === undefined ? {} : { operation_id }),
    });
  return new WorkstreamCliError({
    code:
      HttpClientError.isHttpClientError(cause) &&
      (cause.response?.status === 404 || cause.response?.status === 405)
        ? "unsupported"
        : "request_failed",
    ...(operation_id === undefined ? {} : { operation_id }),
    cause,
  });
};

/** The CLI and its HTTP integration tests share this exact transport adapter. */
export const makeWorkstreamCliHttpPort = Effect.fn("WorkstreamCli.makeWorkstreamCliHttpPort")(
  function* (baseUrl: string, headers: { readonly authorization: string }) {
    const client = yield* HttpApiClient.make(EnvironmentHttpApi, { baseUrl });
    return {
      list: (query) =>
        client.workstreams
          .list({ headers, payload: query })
          .pipe(Effect.mapError((cause) => workstreamCliRequestError(cause))),
      submit: (payload) =>
        client.workstreams
          .operationSubmit({ headers, payload })
          .pipe(
            Effect.mapError((cause) =>
              workstreamCliRequestError(cause, payload.request.operation_id),
            ),
          ),
      read: (payload) => {
        const read =
          "operation_id" in payload
            ? client.workstreams.operationRead({ headers, payload })
            : "thread" in payload
              ? client.workstreams.operationRead({ headers, payload })
              : client.workstreams.operationRead({ headers, payload });
        return read.pipe(Effect.mapError((cause) => workstreamCliRequestError(cause)));
      },
      setupStatus: () =>
        client.workstreams
          .setupStatus({ headers })
          .pipe(Effect.mapError((cause) => workstreamCliRequestError(cause))),
    } satisfies WorkstreamCliPort;
  },
);

const runLive = Effect.fn("WorkstreamCli.runLive")(function* (
  flags: CliAuthLocationFlags,
  input: WorkstreamCliInput,
  json: boolean,
) {
  const config = yield* resolveCliAuthConfig(flags, yield* GlobalFlag.LogLevel);
  // Follow the existing project CLI's local instance and short-lived auth session conventions.
  const runtime = yield* readPersistedServerRuntimeState(config.serverRuntimeStatePath);
  if (Option.isNone(runtime)) return yield* new WorkstreamCliError({ code: "offline" });
  return yield* Effect.gen(function* () {
    const auth = yield* EnvironmentAuth.EnvironmentAuth;
    return yield* Effect.acquireUseRelease(
      auth.issueSession({ scopes: AuthStandardClientScopes, label: "t3 workstream cli" }),
      (issued) =>
        Effect.gen(function* () {
          const port = yield* makeWorkstreamCliHttpPort(runtime.value.origin, {
            authorization: `Bearer ${issued.token}`,
          });
          const result = yield* dispatchWorkstreamCli(port, input);
          const output = json
            ? yield* Schema.encodeEffect(Schema.fromJsonString(Schema.Unknown))(result)
            : formatWorkstreamCliResult(result);
          yield* Console.log(output);
        }),
      (issued) => auth.revokeSession(issued.sessionId).pipe(Effect.ignore({ log: true })),
    );
  }).pipe(
    Effect.scoped,
    Effect.provide(
      EnvironmentAuth.layerRuntime.pipe(
        Layer.provideMerge(FetchHttpClient.layer),
        Layer.provide(ServerConfig.layer(config)),
      ),
    ),
  );
});

const common = {
  ...projectLocationFlags,
  json: Flag.Boolean("json").pipe(Flag.withDefault(false)),
};
const mutation = {
  ...common,
  request: Flag.String("request").pipe(
    Flag.withDescription(
      "Exact workstream-operations/1.0.0 request JSON with origin.kind cli and an explicit operation_id. Keep identical bytes/fields for retries.",
    ),
  ),
  waitMs: Flag.Int("wait-ms").pipe(
    Flag.withSchema(Operations.WorkstreamOperationSubmitRequest.fields.wait_ms),
    Flag.withDefault(3000),
  ),
};
type WorkstreamCliRunner<E, R> = (
  flags: CliAuthLocationFlags,
  input: WorkstreamCliInput,
  json: boolean,
) => Effect.Effect<void, E, R>;

const mutationCommand = <E, R>(
  run: WorkstreamCliRunner<E, R>,
  command: "create" | "assign" | "unassign",
) =>
  Command.make(command, mutation).pipe(
    Command.withDescription(
      `${command} through the local server. Pending is not success. Retry the same request ID after a lost reply or configuration failure.`,
    ),
    Command.withHandler((flags) =>
      Effect.gen(function* () {
        const request = yield* Schema.decodeEffect(
          Schema.fromJsonString(Operations.WorkstreamOperationRequest),
        )(flags.request).pipe(
          Effect.mapError((cause) => new WorkstreamCliError({ code: "invalid_request", cause })),
        );
        return yield* run(flags, { command, request, wait_ms: flags.waitMs }, flags.json);
      }),
    ),
  );
export const makeWorkstreamOperationCommands = <E, R>(run: WorkstreamCliRunner<E, R>) => [
  Command.make("list", {
    ...common,
    limit: Flag.Int("limit").pipe(
      Flag.withSchema(Schema.Int.check(Schema.isBetween({ minimum: 1, maximum: 100 }))),
      Flag.withDefault(100),
    ),
    cursor: Flag.String("cursor").pipe(Flag.optional),
  }).pipe(
    Command.withDescription("List local Workstreams; use the returned cursor for the next page."),
    Command.withHandler((flags) =>
      run(
        flags,
        {
          command: "list",
          limit: flags.limit,
          ...(Option.isSome(flags.cursor) ? { cursor: flags.cursor.value } : {}),
        },
        flags.json,
      ),
    ),
  ),
  mutationCommand(run, "create"),
  mutationCommand(run, "assign"),
  mutationCommand(run, "unassign"),
  Command.make("status", {
    ...common,
    operationId: Argument.String("operation-id").pipe(Argument.optional),
    thread: Flag.String("thread").pipe(Flag.optional),
    environment: Flag.String("environment").pipe(Flag.optional),
    open: Flag.Boolean("open").pipe(Flag.withDefault(false)),
  }).pipe(
    Command.withDescription(
      "Read setup, an operation ID, --open operations, or --thread ID --environment ID. Status never refreshes failed operations.",
    ),
    Command.withHandler((flags) =>
      Effect.gen(function* () {
        const thread = Option.getOrUndefined(flags.thread);
        const environment = Option.getOrUndefined(flags.environment);
        const operation_id = Option.getOrUndefined(flags.operationId);
        if (
          (thread === undefined) !== (environment === undefined) ||
          Number(thread !== undefined) + Number(operation_id !== undefined) + Number(flags.open) > 1
        )
          return yield* new WorkstreamCliError({ code: "invalid_request" });
        const raw =
          thread !== undefined && environment !== undefined
            ? { thread: { thread_id: thread, environment_id: environment } }
            : operation_id !== undefined
              ? { operation_id }
              : flags.open
                ? { open: true }
                : undefined;
        const query =
          raw === undefined
            ? undefined
            : yield* Schema.decodeUnknownEffect(Operations.WorkstreamOperationReadRequest)(
                raw,
              ).pipe(
                Effect.mapError(
                  (cause) => new WorkstreamCliError({ code: "invalid_request", cause }),
                ),
              );
        return yield* run(
          flags,
          { command: "status", ...(query === undefined ? {} : { query }) },
          flags.json,
        );
      }),
    ),
  ),
];

export const workstreamOperationCommands = makeWorkstreamOperationCommands(runLive);
