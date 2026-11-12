import {
  WORKSTREAMS_HOME_READ_RESULT_CONTRACT,
  WorkstreamsHomeReadResult,
} from "@t3tools/contracts/jones/workstreamsHomeReadResult";
// @effect-diagnostics nodeBuiltinImport:off -- Validate the original home bytes in memory; no persisted export plaintext.
import * as NodeCrypto from "node:crypto";
import {
  WORKSTREAM_CONTRACT_HEADER_VERSION,
  WORKSTREAM_CONTRACT_MANIFEST_SHA256,
} from "@t3tools/contracts";
import {
  WORKSTREAMS_NATIVE_CONTRACT,
  WORKSTREAMS_NATIVE_READ_CONTRACT,
} from "@t3tools/contracts/jones/workstreamsNative";
import {
  WORKSTREAMS_HOME_READ_EXPORT_CONTRACT,
  WorkstreamsHomeReadExportResult,
} from "@t3tools/contracts/jones/workstreamsNativeExport";
import { validateNativeReadExport } from "../../../../../../../packages/client-runtime/src/state/workstreams/nativeExport.ts";
import type { NativeReadScope } from "../../../../../../../packages/client-runtime/src/state/workstreams/nativeRead.ts";
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Schema from "effect/Schema";
import { canonicalJson } from "../domain/validation.ts";
import { RegistryError } from "../registry/session.ts";
import * as Admission from "../runtime/memberAdmission.ts";
import * as Protocol from "../forwarding/homeExportProtocol.ts";
import * as Reads from "./memberRead.ts";

export class MemberExport extends Context.Service<
  MemberExport,
  { readonly read: (pageSize?: number) => Effect.Effect<string, RegistryError> }
>()("t3/jones/workstreams/native/read/memberExport") {}
const decodeCounts = Schema.decodeUnknownEffect(WorkstreamsHomeReadResult);
const decodeResult = Schema.decodeUnknownEffect(WorkstreamsHomeReadExportResult);
export const layer = Layer.effect(
  MemberExport,
  Effect.gen(function* () {
    const admission = yield* Admission.MemberAdmission;
    const reads = yield* Reads.HomeReadPort;
    const client = yield* Protocol.HomeExportClient;
    return MemberExport.of({
      read: Effect.fn("MemberExport.read")(function* (pageSize = 100) {
        if (!Number.isSafeInteger(pageSize) || pageSize < 1 || pageSize > 100)
          return yield* new RegistryError({ code: "invalid_request" });
        const local = yield* admission.current();
        const a = local.record.association;
        const limits = yield* local.checkCurrent();
        const check = Effect.gen(function* () {
          if (canonicalJson(yield* local.checkCurrent()) !== canonicalJson(limits))
            return yield* new RegistryError({ code: "forbidden" });
        });
        const observe = Effect.gen(function* () {
          yield* check;
          const raw = yield* reads
            .read(a.home, { route: "counts", params: {} })
            .pipe(Effect.provideService(Admission.RetainedMemberCaller, local.record));
          yield* check;
          const result = yield* decodeCounts(
            { contract: WORKSTREAMS_HOME_READ_RESULT_CONTRACT, ...raw },
            { onExcessProperty: "error" },
          ).pipe(
            Effect.mapError((cause) => new RegistryError({ code: "invalid_response", cause })),
          );
          if (result.route !== "counts")
            return yield* new RegistryError({ code: "invalid_response" });
          const e = result.envelope,
            value = result.value;
          if (
            e.registry.registry_instance_id !== a.home.registry_instance_id ||
            e.registry.owner_id !== a.home.owner_id ||
            e.registry.canonical_origin !== a.canonical_origin ||
            e.registry.server_generation !== a.home.server_generation ||
            e.provenance.home_environment_id !== a.home.home_environment_id ||
            e.provenance.served_by_environment_id !== a.home.home_environment_id ||
            e.provenance.source !== "live" ||
            e.provenance.stale ||
            e.provenance.authority !== "native-home" ||
            e.coverage.registry !== "complete" ||
            value.principal_id !== a.principal_id ||
            value.context.owner_id !== a.home.owner_id ||
            value.context.server_generation !== a.home.server_generation ||
            value.context.registry_version !== e.registry.registry_version ||
            value.observed_at !== e.provenance.observed_at
          )
            return yield* new RegistryError({ code: "forbidden" });
          // Counts are a transport validation context, never capability advertisement or server authority.
          return {
            capability: {
              contract: WORKSTREAMS_NATIVE_CONTRACT,
              mode: "native-member",
              home_link: "ok",
              registry: { ...a.home, canonical_origin: a.canonical_origin },
              reads: [WORKSTREAMS_NATIVE_READ_CONTRACT],
              placement_proofs: ["workstreams-t3-placement/1.0.0"],
            },
            binding: {
              registryId: a.canonical_origin,
              ownerId: a.home.owner_id,
              principalId: a.principal_id,
              authorizationRevision: result.authorization_revision,
              serverGeneration: a.home.server_generation,
              registryVersion: e.registry.registry_version,
              permissions: ["workstreams:read"],
              contractVersion: WORKSTREAM_CONTRACT_HEADER_VERSION,
              contractManifest: WORKSTREAM_CONTRACT_MANIFEST_SHA256,
            },
          } satisfies NativeReadScope;
        });
        const scope = yield* observe;
        const raw = yield* client
          .read(a.home, {
            contract: WORKSTREAMS_HOME_READ_EXPORT_CONTRACT,
            expected_home: a.home,
            page_size: pageSize,
          })
          .pipe(Effect.provideService(Admission.RetainedMemberCaller, local.record));
        yield* check;
        const result = yield* decodeResult(raw, { onExcessProperty: "error" }).pipe(
          Effect.mapError((cause) => new RegistryError({ code: "invalid_response", cause })),
        );
        if (
          canonicalJson(result.home) !== canonicalJson(a.home) ||
          result.principal_id !== a.principal_id ||
          result.authorization_revision !== scope.binding.authorizationRevision
        )
          return yield* new RegistryError({ code: "forbidden" });
        if (Buffer.byteLength(result.jsonl, "utf8") > 16 * 1024 * 1024 + 1024)
          return yield* new RegistryError({ code: "backpressure" });
        yield* Effect.tryPromise({
          try: (signal) =>
            validateNativeReadExport(
              (async function* () {
                yield result.jsonl;
              })(),
              {
                scope,
                signal,
                sha256: async (bytes) =>
                  NodeCrypto.createHash("sha256").update(bytes).digest("hex"),
              },
            ),
          catch: (cause) => new RegistryError({ code: "invalid_response", cause }),
        });
        const fresh = yield* observe;
        if (canonicalJson(fresh) !== canonicalJson(scope))
          return yield* new RegistryError({ code: "forbidden" });
        yield* check;
        // A single bounded release cannot expose a partial or unvalidated remote download.
        return result.jsonl;
      }),
    });
  }),
);
