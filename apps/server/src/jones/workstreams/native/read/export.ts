// @effect-diagnostics nodeBuiltinImport:off -- Hash exactly the emitted UTF-8 pre-trailer bytes; no persisted plaintext.
import * as NodeCrypto from "node:crypto";
import { WorkstreamsNativeReadExportTrailer } from "@t3tools/contracts/jones/workstreamsNativeRead";
import { canonicalWorkstreamsSnapshotJson } from "@t3tools/contracts/jones/workstreamsSnapshot";
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Schema from "effect/Schema";
import * as Stream from "effect/Stream";
import * as Admission from "../runtime/admission.ts";
import { RegistryError } from "../registry/session.ts";
import * as Projection from "./projection.ts";

export class ReadExport extends Context.Service<
  ReadExport,
  {
    readonly stream: (pageSize?: number) => Stream.Stream<string, RegistryError>;
  }
>()("t3/jones/workstreams/native/read/export/ReadExport") {}
const decodeTrailer = Schema.decodeUnknownEffect(WorkstreamsNativeReadExportTrailer);

export const layer = Layer.effect(
  ReadExport,
  Effect.gen(function* () {
    const projection = yield* Projection.Projection,
      boundary = yield* Admission.CallerBoundary,
      admission = yield* Admission.Admission;
    return ReadExport.of({
      stream: (pageSize = 100) =>
        Stream.unwrap(
          Effect.gen(function* () {
            if (!Number.isSafeInteger(pageSize) || pageSize < 1 || pageSize > 100)
              return yield* new RegistryError({ code: "invalid_request" });
            const evidence = yield* boundary
              .current()
              .pipe(Effect.mapError(Projection.readFailure));
            const admit = admission
              .admit("workstreams:read")
              .pipe(
                Effect.provideService(Admission.RequestCaller, evidence),
                Effect.mapError(Projection.readFailure),
              );
            const caller = yield* admit;
            const snapshot = yield* projection.capture(caller);
            const { envelope, items, counts } = snapshot;
            if (
              envelope.provenance.source !== "live" ||
              envelope.provenance.stale ||
              envelope.provenance.authority !== "native-home" ||
              envelope.coverage.registry !== "complete"
            )
              return yield* new RegistryError({ code: "unavailable" });
            const check = Effect.gen(function* () {
              const current = yield* admit;
              if (
                current.principal_id !== caller.principal_id ||
                current.binding_version !== caller.binding_version ||
                current.source_environment_id !== caller.source_environment_id
              )
                return yield* new RegistryError({ code: "forbidden" });
              yield* projection.assertCurrent(current, snapshot);
            });
            // Freeze bytes before yielding: appearance has its own CAS and need not bump registry_version.
            const lines = yield* Effect.try({
              try: () => {
                const result: string[] = [];
                for (let offset = 0; offset < Math.max(items.length, 1); offset += pageSize)
                  result.push(
                    canonicalWorkstreamsSnapshotJson({
                      envelope,
                      counts,
                      offset,
                      items: items.slice(offset, offset + pageSize),
                    }) + "\n",
                  );
                if (
                  result.reduce((size, line) => size + Buffer.byteLength(line), 0) >
                  16 * 1024 * 1024
                )
                  throw new RegistryError({ code: "backpressure" });
                return result;
              },
              catch: Projection.readFailure,
            });
            const hash = NodeCrypto.createHash("sha256");
            const pages = Stream.fromIterable(lines).pipe(
              Stream.mapEffect((line) =>
                check.pipe(
                  Effect.as(line),
                  Effect.tap(() =>
                    Effect.sync(() => {
                      hash.update(line, "utf8");
                    }),
                  ),
                ),
              ),
            );
            const trailer = Stream.fromEffect(
              Effect.gen(function* () {
                yield* check;
                const value = yield* decodeTrailer({
                  complete: true,
                  registry_version: envelope.registry.registry_version,
                  sha256: hash.digest("hex"),
                }).pipe(Effect.mapError(Projection.readFailure));
                return canonicalWorkstreamsSnapshotJson(value) + "\n";
              }),
            );
            return Stream.concat(pages, trailer);
          }),
        ),
    });
  }),
);
