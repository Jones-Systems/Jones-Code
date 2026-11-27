import {
  AuthRequestsManageScope,
  EnvironmentHttpApi,
  EnvironmentId,
  RemoteAdmissionDigest,
} from "@t3tools/contracts";
import * as DTO from "@t3tools/contracts/jones/requestManagement";
import * as Console from "effect/Console";
import * as Duration from "effect/Duration";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as References from "effect/References";
import * as Schema from "effect/Schema";
import { Argument, Command, Flag, GlobalFlag } from "effect/cli";
import * as FetchHttpClient from "effect/http/FetchHttpClient";
import * as HttpApiClient from "effect/http-api/HttpApiClient";
import * as EnvironmentAuth from "../../auth/EnvironmentAuth.ts";
import { projectLocationFlags, resolveCliAuthConfig } from "../../cli/config.ts";
import * as ServerConfig from "../../config.ts";
import { readLiveServerRuntimeState } from "../../serverRuntimeState.ts";

class RequestsCliError extends Schema.TaggedError<RequestsCliError>()("RequestsCliError", {
  message: Schema.String,
}) {}

type LocalQuery =
  | ({ readonly action: "list" } & DTO.RequestListInput)
  | { readonly action: "inspect"; readonly id: string };

const encodeResult = Schema.encodeEffect(
  Schema.fromJsonString(Schema.Union([DTO.OwnerRequestsResult, DTO.OwnerRequestEditResult]), {
    space: 2,
  }),
);
const decodeLabel = Schema.decodeEffect(DTO.RequestLabel);

const withOwnerServer = Effect.fn("requests.cli.withOwnerServer")(
  function* (
    flags: { readonly baseDir: Option.Option<string> },
    input: DTO.OwnerRequestEdit | LocalQuery,
  ) {
    const logLevel = yield* GlobalFlag.LogLevel;
    const config = yield* resolveCliAuthConfig(flags, logLevel, { quietLogs: true });
    const runtime = yield* readLiveServerRuntimeState(config.serverRuntimeStatePath);
    if (Option.isNone(runtime))
      return yield* new RequestsCliError({
        message: "Start the local Jones Code server for this data directory.",
      });
    const origin = yield* Effect.try({
      try: () => new URL(runtime.value.origin),
      catch: () => new RequestsCliError({ message: "The local server origin is invalid." }),
    });
    if (
      origin.protocol !== "http:" ||
      (origin.hostname !== "127.0.0.1" &&
        origin.hostname !== "[::1]" &&
        origin.hostname !== "localhost") ||
      origin.port !== String(runtime.value.port) ||
      origin.username ||
      origin.password
    )
      return yield* new RequestsCliError({
        message: "Requests management requires the server's loopback route.",
      });
    const client = yield* HttpApiClient.make(EnvironmentHttpApi, { baseUrl: origin.origin });
    const descriptor = yield* client.metadata.descriptor().pipe(Effect.timeout("10 seconds"));
    if (descriptor.capabilities.requestManagement !== true)
      return yield* new RequestsCliError({
        message: "The running Jones Code server does not support Requests management.",
      });
    const receiverEnvironmentId = descriptor.environmentId;
    const mutation = "expectedRevision" in input;
    if (mutation && input.receiverEnvironmentId !== receiverEnvironmentId)
      return yield* new RequestsCliError({
        message: "The local server has a different environment ID.",
      });
    const run = Effect.gen(function* () {
      const auth = yield* EnvironmentAuth.EnvironmentAuth;
      return yield* Effect.acquireUseRelease(
        auth.issueSession({
          scopes: [AuthRequestsManageScope],
          subject: DTO.REQUESTS_CLI_SUBJECT,
          label: "Jones Code Requests CLI",
          ttl: Duration.minutes(1),
        }),
        (issued) =>
          Effect.gen(function* () {
            const headers = { authorization: `Bearer ${issued.token}` };
            if (mutation) return yield* client.requestManagement.edit({ headers, payload: input });
            const payload: DTO.OwnerRequestsQuery =
              input.action === "inspect"
                ? { receiverEnvironmentId, action: "inspect", id: input.id }
                : { ...input, receiverEnvironmentId, action: "list" };
            return yield* client.requestManagement.query({ headers, payload });
          }),
        (issued) => auth.revokeSession(issued.sessionId).pipe(Effect.ignore({ log: true })),
      ).pipe(Effect.timeout("15 seconds"));
    });
    const result = yield* run.pipe(
      Effect.provide(
        EnvironmentAuth.layerRuntime.pipe(
          Layer.provide(ServerConfig.layer(config)),
          Layer.provide(Layer.succeed(References.MinimumLogLevel, "Error")),
        ),
      ),
    );
    yield* Console.log(yield* encodeResult(result));
  },
  (effect) =>
    effect.pipe(
      Effect.provideService(FetchHttpClient.RequestInit, { redirect: "error" }),
      Effect.provide(FetchHttpClient.layer),
    ),
);

const id = Argument.String("request-id").pipe(Argument.withSchema(DTO.RequestId));
const targetFlags = {
  ...projectLocationFlags,
  id,
  receiverEnvironmentId: Flag.String("environment").pipe(Flag.withSchema(EnvironmentId)),
  expectedRevision: Flag.String("revision").pipe(Flag.withSchema(RemoteAdmissionDigest)),
};
const list = Command.make("list", {
  ...projectLocationFlags,
  view: Flag.Literals("view", ["pending", "all", "dismissed"]).pipe(Flag.withDefault("pending")),
  limit: Flag.Int("limit").pipe(Flag.withSchema(DTO.RequestPageSize), Flag.optional),
  after: Flag.String("after").pipe(Flag.withSchema(DTO.RequestId), Flag.optional),
}).pipe(
  Command.withDescription("List pending or retained requests on the local server."),
  Command.withHandler(({ baseDir, view, limit, after }) =>
    withOwnerServer(
      { baseDir },
      {
        action: "list",
        view,
        ...(Option.isSome(limit) ? { limit: limit.value } : {}),
        ...(Option.isSome(after) ? { after: after.value } : {}),
      },
    ),
  ),
);
const inspect = Command.make("inspect", { ...projectLocationFlags, id }).pipe(
  Command.withDescription("Inspect a retained request and its current revision."),
  Command.withHandler(({ baseDir, id }) => withOwnerServer({ baseDir }, { action: "inspect", id })),
);
const label = Command.make("label", {
  ...targetFlags,
  label: Argument.String("label").pipe(
    Argument.withDescription("New label. Pass an empty string to clear it."),
  ),
}).pipe(
  Command.withDescription("Edit a request's local label."),
  Command.withHandler(({ baseDir, label, ...target }) =>
    Effect.gen(function* () {
      const value = label === "" ? null : yield* decodeLabel(label);
      yield* withOwnerServer({ baseDir }, { ...target, action: "label", label: value });
    }),
  ),
);
const dismiss = Command.make("dismiss", targetFlags).pipe(
  Command.withDescription(
    "Hide an eligible completed request from the inbox while retaining its evidence.",
  ),
  Command.withHandler(({ baseDir, ...target }) =>
    withOwnerServer({ baseDir }, { ...target, action: "dismiss" }),
  ),
);
const restore = Command.make("restore", targetFlags).pipe(
  Command.withDescription("Return a dismissed request to the inbox."),
  Command.withHandler(({ baseDir, ...target }) =>
    withOwnerServer({ baseDir }, { ...target, action: "restore" }),
  ),
);

export const requestsCommand = Command.make("requests").pipe(
  Command.withDescription("Inspect and manage local Jones Code Requests."),
  Command.withSubcommands([list, inspect, label, dismiss, restore]),
);
