import {
  WORKSTREAMS_HOME_READ_RESULT_CONTRACT,
  WorkstreamsHomeReadResult,
  WorkstreamsHomeReadResultRequest,
} from "@t3tools/contracts/jones/workstreamsHomeReadResult";
import type { WorkstreamsNativeHomeTarget } from "@t3tools/contracts/jones/workstreamsNative";
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Schema from "effect/Schema";
import type { AdmittedCaller } from "../mode/authorityMode.ts";
import { RegistryError } from "../registry/session.ts";
import * as Routes from "../read/routes.ts";
import * as Member from "../read/memberRead.ts";
import { canonicalJson } from "../domain/validation.ts";

/** Root mounts genuine incoming Guard admission; this port mints no authority. */
export class HomePeerReadAdmission extends Context.Service<
  HomePeerReadAdmission,
  {
    readonly current: () => Effect.Effect<
      {
        readonly caller: AdmittedCaller;
        readonly authorization_revision: number;
        readonly checkCurrent: () => Effect.Effect<void, RegistryError>;
      },
      RegistryError
    >;
  }
>()("t3/jones/workstreams/native/forwarding/homeReadProtocol/HomePeerReadAdmission") {}

/** Root owns the authenticated single-hop peer, capability negotiation and transport retries. */
export class HomeReadResultClient extends Context.Service<
  HomeReadResultClient,
  {
    readonly read: (
      home: WorkstreamsNativeHomeTarget,
      request: WorkstreamsHomeReadResultRequest,
    ) => Effect.Effect<unknown, RegistryError>;
  }
>()("t3/jones/workstreams/native/forwarding/homeReadProtocol/HomeReadResultClient") {}

export class HomeReadProtocol extends Context.Service<
  HomeReadProtocol,
  {
    readonly read: (
      request: WorkstreamsHomeReadResultRequest,
    ) => Effect.Effect<WorkstreamsHomeReadResult, RegistryError>;
  }
>()("t3/jones/workstreams/native/forwarding/homeReadProtocol") {}

const decodeRequest = Schema.decodeUnknownEffect(WorkstreamsHomeReadResultRequest);
const decodeResult = Schema.decodeUnknownEffect(WorkstreamsHomeReadResult);

/** Source-only producer for jones_workstreams_home_read_result; no advertisement is installed here. */
export const layer = Layer.effect(
  HomeReadProtocol,
  Effect.gen(function* () {
    const admission = yield* HomePeerReadAdmission;
    const reads = yield* Routes.HomeReads;
    return HomeReadProtocol.of({
      read: Effect.fn("NativeReadProtocol.home")(function* (input) {
        const request = yield* decodeRequest(input, { onExcessProperty: "error" }).pipe(
          Effect.mapError((cause) => new RegistryError({ code: "invalid_request", cause })),
        );
        const captured = yield* admission.current();
        yield* captured.checkCurrent();
        const result = yield* reads.read(
          captured.caller,
          request.request,
          request.placement_contract,
        );
        yield* captured.checkCurrent();
        const current = yield* admission.current();
        yield* current.checkCurrent();
        if (
          canonicalJson(current.caller) !== canonicalJson(captured.caller) ||
          current.authorization_revision !== captured.authorization_revision ||
          result.authorization_revision !== captured.authorization_revision
        )
          return yield* new RegistryError({ code: "forbidden" });
        const validated = yield* Routes.validateReadResult(
          request.request,
          result,
          request.placement_contract,
        );
        if (
          validated.envelope.provenance.authority !== "native-home" ||
          validated.envelope.provenance.source !== "live" ||
          validated.envelope.provenance.stale ||
          validated.envelope.coverage.registry !== "complete"
        )
          return yield* new RegistryError({ code: "unavailable" });
        if (
          (validated.route === "counts" &&
            validated.value.principal_id !== captured.caller.principal_id) ||
          (validated.route === "placements" &&
            validated.value.context.principal_id !== captured.caller.principal_id)
        )
          return yield* new RegistryError({ code: "forbidden" });
        return { contract: WORKSTREAMS_HOME_READ_RESULT_CONTRACT, ...validated };
      }),
    });
  }),
);

/** Additive adapter; existing bare-page callers and tools keep their own independently decoded ports. */
export const homeReadPortLayer = Layer.effect(
  Member.HomeReadPort,
  Effect.gen(function* () {
    const client = yield* HomeReadResultClient;
    return Member.HomeReadPort.of({
      read: Effect.fn("NativeReadProtocol.member")(function* (home, request, placementContract) {
        const wrapped = yield* decodeRequest(
          {
            contract: WORKSTREAMS_HOME_READ_RESULT_CONTRACT,
            request,
            ...(placementContract === undefined ? {} : { placement_contract: placementContract }),
          },
          { onExcessProperty: "error" },
        ).pipe(Effect.mapError((cause) => new RegistryError({ code: "invalid_request", cause })));
        const raw = yield* client.read(home, wrapped);
        const result = yield* decodeResult(raw, { onExcessProperty: "error" }).pipe(
          Effect.mapError((cause) => new RegistryError({ code: "invalid_response", cause })),
        );
        const { contract: _contract, ...read } = result;
        return yield* Routes.validateReadResult(request, read, placementContract);
      }),
    });
  }),
);
