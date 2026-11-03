import { WorkstreamCommand, type WorkstreamReceipt } from "@t3tools/contracts";
import * as Wire from "@t3tools/contracts/jones/workstreamsForwarding";
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Schema from "effect/Schema";
import * as Admission from "../runtime/memberAdmission.ts";
import * as Store from "../store/operationStore.ts";
import { RegistryError } from "../registry/session.ts";
import * as Home from "./homeClient.ts";

/** Public recovery selects only a genuine immutable local raw-command intent, never a destination. */
export class MemberCommandReceipts extends Context.Service<
  MemberCommandReceipts,
  {
    readonly lookup: (input: {
      readonly command_id: string;
      readonly retained_body?: string;
    }) => Effect.Effect<WorkstreamReceipt, RegistryError>;
  }
>()("t3/jones/workstreams/native/forwarding/memberCommandReceipts") {}
const decodeCommand = Schema.decodeUnknownEffect(Schema.fromJsonString(WorkstreamCommand));
const decodeId = Schema.decodeUnknownEffect(Wire.WorkstreamsForwardCommandId);
const unknown = () => new RegistryError({ code: "unknown_effect" });
const invalid = () => new RegistryError({ code: "invalid_response" });
const make = Effect.gen(function* () {
  const store = yield* Store.OperationStore;
  const admission = yield* Admission.MemberAdmission;
  const home = yield* Home.HomeForwardClient;
  return MemberCommandReceipts.of({
    lookup: Effect.fn("MemberCommandReceipts.lookup")(function* (input) {
      yield* decodeId(input.command_id).pipe(Effect.mapError(unknown));
      const row = yield* store.readMemberForward(input.command_id).pipe(Effect.mapError(unknown));
      // Local absence is not home absence, and this port has no accept, advance or dispatch branch.
      if (row === null || row.kind !== "command") return yield* unknown();
      const admitted = yield* admission.readmitWrite(row.member_forward.recorded_caller);
      yield* admitted.checkCurrent();
      const request = yield* Effect.try({
        try: () => Wire.decodeWorkstreamsForwardBody(row.member_forward.forward_request_body),
        catch: invalid,
      });
      if (
        request.kind !== "command" ||
        request.identity.source_operation_id !== input.command_id ||
        row.operation_id !== input.command_id ||
        request.request.body !== row.request_body
      )
        return yield* invalid();
      const command = yield* decodeCommand(row.request_body).pipe(Effect.mapError(invalid));
      if (command.command_id !== input.command_id) return yield* invalid();
      // Compare only AFTER original-caller admission. A new credential cannot probe retained bytes.
      if (input.retained_body !== undefined && input.retained_body !== row.request_body)
        return yield* new RegistryError({ code: "operation_id_conflict" });
      const result = yield* home.readback(request, admitted);
      if (result.state !== "present") return yield* unknown();
      const value = yield* Home.validateForwardResult(request, result.result, admitted);
      if (value.kind !== "command") return yield* invalid();
      yield* admitted.checkCurrent();
      return value.result;
    }),
  });
});
export const layer = Layer.effect(MemberCommandReceipts, make);
