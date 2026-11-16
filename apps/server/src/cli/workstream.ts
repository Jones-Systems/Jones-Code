import * as SecureRandom from "effect/crypto/SecureRandom";
import * as Console from "effect/Console";
import * as Effect from "effect/Effect";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";
import { Argument, Command, Flag } from "effect/cli";
import { projectLocationFlags } from "./config.ts";
import { withThreadTools } from "./thread.ts";

const common = {
  ...projectLocationFlags,
  json: Flag.Boolean("json").pipe(Flag.withDefault(false)),
  environmentId: Flag.String("environment-id").pipe(Flag.optional),
};
const id = Argument.String("id");
const output = (json: boolean, value: unknown) =>
  Console.log(json ? JSON.stringify(value) : JSON.stringify(value, null, 2));
const commands = [
  Command.make("list", common).pipe(
    Command.withHandler((flags) =>
      withThreadTools(flags, (tools) =>
        tools
          .call(
            "t3_workstreams_list",
            { environmentId: Option.getOrUndefined(flags.environmentId) },
            Schema.Unknown,
          )
          .pipe(Effect.flatMap((r) => output(flags.json, r))),
      ),
    ),
  ),
  Command.make("create", {
    ...common,
    name: Argument.String("name"),
    id: Flag.String("id").pipe(Flag.optional),
  }).pipe(
    Command.withHandler((flags) =>
      withThreadTools(flags, (tools) =>
        Effect.gen(function* () {
          const chosen = Option.isSome(flags.id)
            ? flags.id.value
            : yield* (yield* SecureRandom.SecureRandom).uuidV4();
          if (Option.isNone(flags.id)) yield* Console.error(`Workstream id: ${chosen}`);
          return yield* tools
            .call(
              "t3_workstream_create",
              {
                id: chosen,
                name: flags.name.trim(),
                environmentId: Option.getOrUndefined(flags.environmentId),
              },
              Schema.Unknown,
            )
            .pipe(Effect.flatMap((r) => output(flags.json, r)));
        }),
      ),
    ),
  ),
  ...(["rename", "recolor", "reorder"] as const).map((action) =>
    Command.make(action, { ...common, id, value: Argument.String("value") }).pipe(
      Command.withHandler((flags) =>
        withThreadTools(flags, (tools) => {
          const value = flags.value;
          const patch =
            action === "rename"
              ? { name: value?.trim() }
              : action === "recolor"
                ? { color: value === "none" ? null : value?.toUpperCase() }
                : { orderKey: value };
          return tools
            .call(
              "t3_workstream_update",
              { id: flags.id, ...patch, environmentId: Option.getOrUndefined(flags.environmentId) },
              Schema.Unknown,
            )
            .pipe(Effect.flatMap((r) => output(flags.json, r)));
        }),
      ),
    ),
  ),
  Command.make("delete", { ...common, id }).pipe(
    Command.withHandler((flags) =>
      withThreadTools(flags, (tools) =>
        tools
          .call(
            "t3_workstream_update",
            {
              id: flags.id,
              delete: true,
              environmentId: Option.getOrUndefined(flags.environmentId),
            },
            Schema.Unknown,
          )
          .pipe(Effect.flatMap((r) => output(flags.json, r))),
      ),
    ),
  ),
  ...(["assign", "unassign", "read"] as const).map((action) =>
    Command.make(action, {
      ...common,
      threadId: Argument.String("thread"),
      ...(action === "assign" ? { workstreamId: Flag.String("workstream-id") } : {}),
    }).pipe(
      Command.withHandler((flags) =>
        withThreadTools(flags, (tools) =>
          Effect.gen(function* () {
            const target = {
              threadId: flags.threadId,
              environmentId: Option.getOrUndefined(flags.environmentId),
            };
            const read = yield* tools.call(
              "t3_thread_workstream_read",
              target,
              Schema.Struct({
                threadId: Schema.String,
                workstreamId: Schema.NullOr(Schema.String),
                workstreamName: Schema.NullOr(Schema.String),
                revision: Schema.Number,
              }),
            );
            const r =
              action === "read"
                ? read
                : yield* tools.call(
                    "t3_thread_workstream_set",
                    {
                      ...target,
                      workstreamId:
                        action === "unassign"
                          ? null
                          : "workstreamId" in flags
                            ? flags.workstreamId
                            : null,
                      expectedRevision: read.revision,
                    },
                    Schema.Unknown,
                  );
            yield* output(flags.json, r);
          }),
        ),
      ),
    ),
  ),
];
export const workstreamCommand = Command.make("workstream").pipe(
  Command.withDescription("Manage personal Workstreams on the running server."),
  Command.withSubcommands(commands),
);
