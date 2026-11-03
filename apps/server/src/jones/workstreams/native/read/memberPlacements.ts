// @effect-diagnostics nodeBuiltinImport:off -- Exact bounded placement inventory hashing follows the frozen protocol.
import * as NodeCrypto from "node:crypto";
import * as Wire from "@t3tools/contracts";
import * as Native from "@t3tools/contracts/jones/workstreamsNativePlacement";
import type { WorkstreamsHomeAccessResult } from "@t3tools/contracts/jones/workstreamsHomeAccess";
import * as Clock from "effect/Clock";
import * as DateTime from "effect/DateTime";
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Schema from "effect/Schema";
import * as Authority from "../../../../environment/NativeStoreAuthority.ts";
import * as Application from "../runtime/memberApplication.ts";
import { RegistryError } from "../registry/session.ts";
import { canonicalJson } from "../domain/validation.ts";
import { validateReadResult } from "./routes.ts";

export class MemberPlacements extends Context.Service<
  MemberPlacements,
  {
    readonly load: (
      input: Wire.T3PlacementLoadRequest,
      contract: string,
    ) => Effect.Effect<Native.WorkstreamsNativePlacementResult, RegistryError>;
  }
>()("t3/jones/workstreams/native/read/memberPlacements") {}

const decodeInput = Schema.decodeUnknownEffect(Wire.T3PlacementLoadRequest);
const decodePage = Schema.decodeUnknownEffect(Native.WorkstreamsNativePlacementPage);
const decodeResult = Schema.decodeUnknownEffect(Native.WorkstreamsNativePlacementResult);
const timestampKey = (value: string) =>
  value.slice(0, 19) + value.slice(19, -1).replace(/^\./, "").padEnd(6, "0");
const accessKey = ({ observed_at: _time, ...value }: WorkstreamsHomeAccessResult) =>
  canonicalJson(value);

const make = Effect.gen(function* () {
  const application = yield* Application.MemberApplication;
  const authority = yield* Authority.NativeStoreAuthority;
  const clock = yield* Clock.Clock;
  const readTrust = () =>
    Effect.try({
      try: () => authority.trustProvider.readTrustSnapshot(),
      catch: () => new RegistryError({ code: "authority_fenced" }),
    });
  return MemberPlacements.of({
    load: Effect.fn("MemberPlacements.load")(function* (input, contract) {
      if (contract !== "workstreams-t3-placement/2.0.0")
        return yield* new RegistryError({ code: "contract_mismatch" });
      const request = yield* decodeInput(input, { onExcessProperty: "error" }).pipe(
        Effect.mapError(() => new RegistryError({ code: "invalid_request" })),
      );
      const before = yield* application.access();
      const originalAccess = accessKey(before);
      const trusted = yield* readTrust();
      const originalTrust = canonicalJson(trusted);
      const inventory = new Set(request.identities.map(Wire.t3PlacementIdentityKey));
      const digest = NodeCrypto.createHash("sha256")
        .update(Wire.t3PlacementInventoryJson(request.identities))
        .digest("hex");
      const items: Native.WorkstreamsNativeThreadPlacement[] = [];
      const seenItems = new Set<string>(),
        seenCursors = new Set<string>(),
        primary = new Set<string>();
      let first: Native.WorkstreamsNativePlacementPage | undefined;
      let snapshot: string | undefined;
      let cursor: string | undefined;
      let complete = false,
        bytes = 0;
      for (let pages = 0; pages < Wire.T3_PLACEMENT_MAX_PAGES; pages++) {
        const query = {
          route: "placements" as const,
          params: {
            identities: request.identities,
            limit: 100,
            ...(cursor === undefined ? {} : { cursor }),
          },
        };
        const result = yield* validateReadResult(
          query,
          yield* application.read(query, "workstreams-t3-placement/2.0.0"),
          "workstreams-t3-placement/2.0.0",
        );
        const page = yield* decodePage(result.value, { onExcessProperty: "error" }).pipe(
          Effect.mapError(() => new RegistryError({ code: "invalid_response" })),
        );
        const home = before.home,
          envelope = result.envelope,
          provenance = page.provenance;
        if (
          result.route !== "placements" ||
          result.placement_contract !== contract ||
          result.authorization_revision !== before.authorization_revision ||
          page.inventory_sha256 !== digest ||
          page.context.owner_id !== home.owner_id ||
          page.context.principal_id !== before.principal_id ||
          page.context.authorization_revision !== before.authorization_revision ||
          page.context.server_generation !== home.server_generation ||
          page.context.registry_version !== home.registry_version ||
          canonicalJson({
            ...envelope.registry,
            home_environment_id: envelope.provenance.home_environment_id,
          }) !== canonicalJson(home) ||
          envelope.provenance.authority !== "native-home" ||
          envelope.provenance.source !== "live" ||
          envelope.provenance.stale ||
          envelope.coverage.registry !== "complete" ||
          envelope.provenance.served_by_environment_id !== home.home_environment_id ||
          provenance.registry_instance_id !== home.registry_instance_id ||
          provenance.home_environment_id !== home.home_environment_id ||
          provenance.home_generation !== home.server_generation ||
          provenance.served_by_environment_id !== home.home_environment_id ||
          provenance.observed_at !== envelope.provenance.observed_at
        )
          return yield* new RegistryError({ code: "invalid_response" });
        const current = canonicalJson({ envelope, context: page.context, provenance });
        if (snapshot !== undefined && current !== snapshot)
          return yield* new RegistryError({ code: "cursor_stale" });
        snapshot = current;
        first ??= page;
        bytes += yield* Effect.try({
          try: () => new TextEncoder().encode(canonicalJson(result)).byteLength,
          catch: () => new RegistryError({ code: "invalid_response" }),
        });
        if (bytes > 16_777_216) return yield* new RegistryError({ code: "backpressure" });
        for (const item of page.items) {
          const key = Wire.t3PlacementIdentityKey(item);
          if (
            !inventory.has(key) ||
            seenItems.has(item.membership_id) ||
            (item.kind === "primary" && primary.has(key)) ||
            timestampKey(item.attested_at) > timestampKey(provenance.observed_at) ||
            timestampKey(item.expires_at) <= timestampKey(provenance.observed_at)
          )
            return yield* new RegistryError({ code: "invalid_response" });
          seenItems.add(item.membership_id);
          if (item.kind === "primary") primary.add(key);
          items.push(item);
        }
        if (items.length > 1000) return yield* new RegistryError({ code: "backpressure" });
        if (page.next_cursor === null) {
          complete = true;
          break;
        }
        if (seenCursors.has(page.next_cursor))
          return yield* new RegistryError({ code: "cursor_invalid" });
        seenCursors.add(page.next_cursor);
        cursor = page.next_cursor;
      }
      if (!complete || first === undefined)
        return yield* new RegistryError({ code: "backpressure" });
      const after = yield* application.access();
      const currentTrust = yield* readTrust();
      if (originalAccess !== accessKey(after) || originalTrust !== canonicalJson(currentTrust))
        return yield* new RegistryError({ code: "forbidden" });
      const now = timestampKey(
        DateTime.formatIso(DateTime.makeUnsafe(clock.currentTimeMillisUnsafe())),
      );
      if (items.some((item) => timestampKey(item.expires_at) <= now))
        return yield* new RegistryError({ code: "cursor_stale" });
      // Trust comes from this server's authority provider, independently of remote page contents.
      return yield* decodeResult(
        { page: { ...first, items, next_cursor: null }, ...trusted },
        {
          onExcessProperty: "error",
        },
      ).pipe(Effect.mapError(() => new RegistryError({ code: "invalid_response" })));
    }),
  });
});

export const layer = Layer.effect(MemberPlacements, make);
