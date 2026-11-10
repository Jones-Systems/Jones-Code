import {
  WORKSTREAMS_HOME_READ_EXPORT_CONTRACT,
  WorkstreamsHomeReadExportRequest,
  WorkstreamsHomeReadExportResult,
} from "@t3tools/contracts/jones/workstreamsNativeExport";
import type { WorkstreamsNativeHomeTarget } from "@t3tools/contracts/jones/workstreamsNative";
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Schema from "effect/Schema";
import * as Stream from "effect/Stream";
import { canonicalJson } from "../domain/validation.ts";
import { NativeHome } from "../mode/authorityMode.ts";
import { RegistryError } from "../registry/session.ts";
import { streamSnapshot } from "../read/export.ts";
import * as Projection from "../read/projection.ts";
import * as ReadProtocol from "./homeReadProtocol.ts";

export class HomeExportClient extends Context.Service<
  HomeExportClient,
  {
    readonly read: (
      home: WorkstreamsNativeHomeTarget,
      request: WorkstreamsHomeReadExportRequest,
    ) => Effect.Effect<unknown, RegistryError>;
  }
>()("t3/jones/workstreams/native/forwarding/homeExportProtocol/HomeExportClient") {}
export class HomeExportProtocol extends Context.Service<
  HomeExportProtocol,
  {
    readonly read: (
      request: WorkstreamsHomeReadExportRequest,
    ) => Effect.Effect<WorkstreamsHomeReadExportResult, RegistryError>;
  }
>()("t3/jones/workstreams/native/forwarding/homeExportProtocol") {}
const decodeRequest = Schema.decodeUnknownEffect(WorkstreamsHomeReadExportRequest);
const decodeResult = Schema.decodeUnknownEffect(WorkstreamsHomeReadExportResult);
export const layer = Layer.effect(
  HomeExportProtocol,
  Effect.gen(function* () {
    const admission = yield* ReadProtocol.HomePeerReadAdmission;
    const projection = yield* Projection.Projection;
    const home = yield* NativeHome;
    const target: WorkstreamsNativeHomeTarget = {
      registry_instance_id: home.registry_instance_id,
      owner_id: home.owner_id,
      home_environment_id: home.home_environment_id,
      server_generation: home.server_generation,
    };
    return HomeExportProtocol.of({
      read: Effect.fn("HomeExportProtocol.read")(function* (input) {
        const request = yield* decodeRequest(input, { onExcessProperty: "error" }).pipe(
          Effect.mapError((cause) => new RegistryError({ code: "invalid_request", cause })),
        );
        if (canonicalJson(request.expected_home) !== canonicalJson(target))
          return yield* new RegistryError({ code: "registry_mismatch" });
        const captured = yield* admission.current();
        const current = Effect.gen(function* () {
          yield* captured.checkCurrent();
          const fresh = yield* admission.current();
          yield* fresh.checkCurrent();
          if (
            canonicalJson(fresh.caller) !== canonicalJson(captured.caller) ||
            fresh.authorization_revision !== captured.authorization_revision
          )
            return yield* new RegistryError({ code: "forbidden" });
          return captured.caller;
        });
        // The existing home producer checks its actual projection before every original page/trailer.
        const lines = yield* Stream.runCollect(
          streamSnapshot(current, request.page_size).pipe(
            Stream.provideService(Projection.Projection, projection),
          ),
        );
        yield* current;
        return yield* decodeResult(
          {
            contract: WORKSTREAMS_HOME_READ_EXPORT_CONTRACT,
            home: target,
            principal_id: captured.caller.principal_id,
            authorization_revision: captured.authorization_revision,
            jsonl: lines.join(""),
          },
          { onExcessProperty: "error" },
        ).pipe(Effect.mapError((cause) => new RegistryError({ code: "invalid_response", cause })));
      }),
    });
  }),
);
