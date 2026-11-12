import { type WorkstreamAppearanceWrite, WorkstreamCommand } from "@t3tools/contracts";
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Schema from "effect/Schema";
import { canonicalJson } from "../../operations/encoding.ts";
import { RegistryError } from "../registry/session.ts";
import * as Forwarder from "./forwarder.ts";

/** Request-scoped write seam for Gateway; open states remain visible rather than becoming transient promises. */
export class MemberWriteTransport extends Context.Service<
  MemberWriteTransport,
  {
    readonly submitCommand: (input: {
      readonly body: string;
      readonly command: WorkstreamCommand;
      readonly idempotencyKey: string;
    }) => Effect.Effect<Forwarder.ForwardView, RegistryError>;
    readonly saveAppearance: (
      input: WorkstreamAppearanceWrite,
    ) => Effect.Effect<Forwarder.ForwardView, RegistryError>;
    readonly readOperation: (
      id: string,
    ) => Effect.Effect<Forwarder.ForwardView | null, RegistryError>;
  }
>()("t3/jones/workstreams/native/forwarding/memberTransport/MemberWriteTransport") {}
const decodeCommand = Schema.decodeUnknownEffect(Schema.fromJsonString(WorkstreamCommand));
const make = Effect.gen(function* () {
  const forwarder = yield* Forwarder.MemberForwarder;
  return MemberWriteTransport.of({
    submitCommand: Effect.fn("MemberWriteTransport.submitCommand")(function* (input) {
      const command = yield* decodeCommand(input.body).pipe(
        Effect.mapError(() => new RegistryError({ code: "invalid_request" })),
      );
      if (
        input.idempotencyKey !== input.command.command_id ||
        canonicalJson(command) !== canonicalJson(input.command)
      )
        return yield* new RegistryError({ code: "invalid_request" });
      return yield* forwarder.submit({ kind: "command", body: input.body });
    }),
    saveAppearance: Effect.fn("MemberWriteTransport.saveAppearance")((input) =>
      forwarder.submit({ kind: "appearance", body: canonicalJson(input) }),
    ),
    readOperation: forwarder.read,
  });
});
export const layer = Layer.effect(MemberWriteTransport, make);
