import * as Schema from "effect/Schema";
import {
  T3WorkstreamBinding,
  t3PlacementIdentityKey,
  t3PlacementInventoryJson,
} from "@t3tools/contracts";
import { WorkstreamsNativeCapability } from "@t3tools/contracts/jones/workstreamsNative";
import {
  WORKSTREAMS_HOME_READ_RESULT_CONTRACT,
  WorkstreamsHomeReadResult,
  WorkstreamsHomeReadResultRequest,
  WorkstreamsProjectionValue,
} from "@t3tools/contracts/jones/workstreamsHomeReadResult";
import { WorkstreamsNativeReadEnvelope } from "@t3tools/contracts/jones/workstreamsNativeRead";
import {
  NativeWorkstreamReadCache,
  assertNativeWorkstreamBinding,
  isNativeWorkstreams,
} from "./native.ts";

export type NativeReadRoute = WorkstreamsHomeReadResult["route"];
export type NativeReadResult<R extends NativeReadRoute = NativeReadRoute> = Extract<
  WorkstreamsHomeReadResult,
  { readonly route: R }
>;
export interface NativeReadScope {
  readonly capability: WorkstreamsNativeCapability;
  readonly binding: T3WorkstreamBinding;
}
/** Authenticated binding belongs to the caller; this port neither retries nor selects peers. */
export type NativeReadTransport = (
  request: WorkstreamsHomeReadResultRequest,
  signal?: AbortSignal,
) => Promise<unknown>;
/** Hash exactly these bytes. Supply a platform SHA-256 implementation, returning lowercase hex. */
export type NativeReadSha256 = (bytes: Uint8Array) => Promise<string>;
export class NativeReadError extends Error {
  readonly code: "invalid_response" | "scope_changed" | "incomplete" | "cancelled" | "bounds";
  constructor(code: NativeReadError["code"], message: string) {
    super(message);
    this.name = "NativeReadError";
    this.code = code;
  }
}
const fail = (message: string): never => {
  throw new NativeReadError("invalid_response", message);
};
const decodeCapability = Schema.decodeSync(WorkstreamsNativeCapability);
const decodeBinding = Schema.decodeSync(T3WorkstreamBinding);
const decodeEnvelope = Schema.decodeUnknownSync(WorkstreamsNativeReadEnvelope);
const decodeRequest = Schema.decodeUnknownSync(WorkstreamsHomeReadResultRequest);
const decodeResult = Schema.decodeUnknownSync(WorkstreamsHomeReadResult);
const clone = <T>(value: T): T => JSON.parse(JSON.stringify(value)) as T;
const equal = (a: unknown, b: unknown): boolean => {
  if (a === b) return true;
  if (!a || !b || typeof a !== "object" || typeof b !== "object") return false;
  if (Array.isArray(a) || Array.isArray(b))
    return (
      Array.isArray(a) &&
      Array.isArray(b) &&
      a.length === b.length &&
      a.every((v, i) => equal(v, b[i]))
    );
  const x = a as Record<string, unknown>,
    y = b as Record<string, unknown>;
  return (
    Object.keys(x).length === Object.keys(y).length &&
    Object.keys(x).every((k) => Object.hasOwn(y, k) && equal(x[k], y[k]))
  );
};
export function validateNativeReadScope(scope: NativeReadScope): NativeReadScope {
  const capability = decodeCapability(scope.capability);
  const binding = decodeBinding(scope.binding, {
    onExcessProperty: "error",
  });
  if (
    !isNativeWorkstreams(capability) ||
    !capability.registry ||
    !capability.reads.includes("workstreams-read/1.0.0") ||
    !binding.permissions.includes("workstreams:read")
  )
    throw new NativeReadError("scope_changed", "Native read capability or permission unavailable.");
  assertNativeWorkstreamBinding(capability, binding);
  return clone({ capability, binding });
}
export function nativeReadScopeKey(scope: NativeReadScope): string {
  const { capability, binding } = validateNativeReadScope(scope);
  return JSON.stringify([capability.registry, binding]);
}
export function validateNativeReadEnvelope(
  input: unknown,
  scope: NativeReadScope,
): WorkstreamsNativeReadEnvelope {
  const read = decodeEnvelope(input);
  const { capability, binding } = validateNativeReadScope(scope);
  const registry = capability.registry!;
  if (capability.mode === "native-member" && capability.home_link !== "ok")
    throw new NativeReadError(
      "incomplete",
      "Native home link unavailable; only existing stale memory is readable.",
    );
  if (
    read.registry.registry_instance_id !== registry.registry_instance_id ||
    read.registry.owner_id !== registry.owner_id ||
    read.registry.canonical_origin !== registry.canonical_origin ||
    read.registry.server_generation !== registry.server_generation ||
    read.registry.registry_version !== binding.registryVersion ||
    read.provenance.home_environment_id !== registry.home_environment_id ||
    read.provenance.authority !== "native-home"
  )
    fail("Native home, owner, generation or registry binding changed.");
  return read;
}
/** Shared export/projection entity checks; registry authority is validated by the envelope boundary. */
export function validateNativeProjectionItems(
  items: (typeof WorkstreamsProjectionValue.Type)["items"],
  envelope: WorkstreamsNativeReadEnvelope,
): void {
  for (const item of items) {
    if (
      item.workstream.owner_id !== envelope.registry.owner_id ||
      item.appearance.workstream_id !== item.workstream.workstream_id ||
      (item.lifecycle &&
        (item.lifecycle.workstream_id !== item.workstream.workstream_id ||
          item.lifecycle.new_lifecycle !== item.workstream.lifecycle ||
          item.lifecycle.registry_version > envelope.registry.registry_version))
    )
      fail("Incoherent native projection entity.");
  }
}
export function assertNativeReadSnapshot(
  a: WorkstreamsNativeReadEnvelope,
  b: WorkstreamsNativeReadEnvelope,
): void {
  // observed_at and source coverage are part of the snapshot, not only registry_version.
  if (!equal(a, b)) fail("Native pages do not share an exact snapshot and currentness.");
}
function assertComplete(envelope: WorkstreamsNativeReadEnvelope): void {
  if (
    envelope.coverage.registry !== "complete" ||
    envelope.provenance.stale ||
    envelope.provenance.source !== "live"
  )
    throw new NativeReadError("incomplete", "Native registry coverage is incomplete or stale.");
}
export function nativeReadRequest<Q extends WorkstreamsHomeReadResultRequest["request"]>(
  request: Q,
  placementContract?: WorkstreamsHomeReadResultRequest["placement_contract"],
): WorkstreamsHomeReadResultRequest & { readonly request: Q } {
  return decodeRequest({
    contract: WORKSTREAMS_HOME_READ_RESULT_CONTRACT,
    request,
    ...(placementContract === undefined ? {} : { placement_contract: placementContract }),
  }) as WorkstreamsHomeReadResultRequest & { readonly request: Q };
}
export async function validateNativeReadResult(
  input: unknown,
  requestInput: WorkstreamsHomeReadResultRequest,
  scope: NativeReadScope,
  sha256?: NativeReadSha256,
): Promise<WorkstreamsHomeReadResult> {
  const request = decodeRequest(requestInput),
    expected = request.request;
  const result = decodeResult(input, { onExcessProperty: "error" });
  const binding = validateNativeReadScope(scope).binding;
  validateNativeReadEnvelope(result.envelope, scope);
  if (
    result.route !== expected.route ||
    result.authorization_revision !== binding.authorizationRevision
  )
    fail("Native route or authorization revision changed.");
  const value = result.value,
    registry = result.envelope.registry;
  if (
    "context" in value &&
    (value.context.owner_id !== registry.owner_id ||
      value.context.server_generation !== registry.server_generation ||
      value.context.registry_version !== registry.registry_version)
  )
    fail("Native route context mismatch.");
  const owner = (id: string) => {
    if (id !== binding.ownerId) fail("Native entity owner mismatch.");
  };
  switch (result.route) {
    case "counts":
      if (
        result.value.principal_id !== binding.principalId ||
        result.value.observed_at !== result.envelope.provenance.observed_at
      )
        fail("Native counts principal or observation mismatch.");
      break;
    case "workstreams":
      result.value.items.forEach((item) => owner(item.owner_id));
      break;
    case "references":
      result.value.items.forEach((item) => owner(item.owner_id));
      break;
    case "workstream":
      owner(result.value.workstream.owner_id);
      if (
        expected.route !== "workstream" ||
        result.value.workstream.workstream_id !== expected.params.workstream_id
      )
        fail("Wrong workstream detail.");
      break;
    case "reference":
      owner(result.value.reference.owner_id);
      if (
        expected.route !== "reference" ||
        result.value.reference.native_reference_id !== expected.params.native_reference_id
      )
        fail("Wrong reference detail.");
      if (
        result.value.latest_observation &&
        result.value.latest_observation.native_reference_id !==
          result.value.reference.native_reference_id
      )
        fail("Wrong reference observation.");
      break;
    case "memberships":
    case "declarations":
    case "lifecycle-declarations": {
      const workstreamId =
        "workstream_id" in expected.params ? expected.params.workstream_id : undefined;
      if (
        !("workstream_id" in expected.params) ||
        result.value.items.some((item) => item.workstream_id !== workstreamId)
      )
        fail("Wrong workstream collection.");
      break;
    }
    case "edges":
      if (
        expected.route !== "edges" ||
        result.value.items.some(
          (item) =>
            item.from_workstream_id !== expected.params.workstream_id &&
            item.to_workstream_id !== expected.params.workstream_id,
        )
      )
        fail("Wrong workstream edge.");
      break;
    case "history":
      if (expected.route !== "history") fail("Wrong history route.");
      if (
        expected.route === "history" &&
        expected.params.workstream_id &&
        result.value.items.some(
          (item) =>
            !item.workstream_versions.some(
              (w) => w.workstream_id === expected.params.workstream_id,
            ),
        )
      )
        fail("Wrong workstream history.");
      break;
    case "read-projection":
      validateNativeProjectionItems(result.value.items, result.envelope);
      break;
    case "appearance":
      if (expected.route !== "appearance") fail("Wrong appearance route.");
      if (result.value.supported && expected.route === "appearance") {
        const page = result.value.page;
        if (
          page.owner_id !== binding.ownerId ||
          page.server_generation !== binding.serverGeneration ||
          !page.permissions.includes("workstreams:read") ||
          page.items.length !== expected.params.workstream_ids.length ||
          new Set(page.items.map((i) => i.workstream_id)).size !== page.items.length ||
          page.items.some((i) => !expected.params.workstream_ids.includes(i.workstream_id))
        )
          fail("Incoherent appearance scope or entities.");
      }
      break;
    case "rollup-summary":
      if (
        expected.route !== "rollup-summary" ||
        result.value.workstream_id !== expected.params.workstream_id
      )
        fail("Wrong rollup workstream.");
      // Summary freshness belongs to WorkstreamsRollupRead, never inferred here.
      break;
    case "placements": {
      if (expected.route !== "placements") fail("Wrong placement route.");
      if (expected.route !== "placements") break;
      const selected = request.placement_contract ?? "workstreams-t3-placement/1.0.0";
      if (
        result.placement_contract !== selected ||
        !scope.capability.placement_proofs.includes(selected)
      )
        fail("Placement contract was not negotiated.");
      const page = result.value;
      if (selected === "workstreams-t3-placement/1.0.0" && page.items.length !== 0)
        fail("Native placements cannot be represented by nonempty frozen v1 proofs.");
      if (
        page.items.some(
          (item) =>
            Date.parse(item.attested_at) > Date.parse(result.envelope.provenance.observed_at) ||
            Date.parse(item.expires_at) <= Date.parse(result.envelope.provenance.observed_at),
        )
      )
        fail("Placement proof is expired or observed in the future.");
      if (
        page.context.principal_id !== binding.principalId ||
        page.context.authorization_revision !== binding.authorizationRevision
      )
        fail("Placement authorization mismatch.");
      if (!sha256) fail("Placement inventory requires a SHA-256 port.");
      if (
        page.inventory_sha256 !==
        (await sha256!(
          new TextEncoder().encode(t3PlacementInventoryJson(expected.params.identities)),
        ))
      )
        fail("Placement inventory digest mismatch.");
      const identities = new Set(expected.params.identities.map(t3PlacementIdentityKey));
      if (page.items.some((item) => !identities.has(t3PlacementIdentityKey(item))))
        fail("Unexpected placement identity.");
      if (result.placement_contract === "workstreams-t3-placement/2.0.0") {
        const p = result.value.provenance,
          e = result.envelope;
        if (
          p.registry_instance_id !== e.registry.registry_instance_id ||
          p.home_environment_id !== e.provenance.home_environment_id ||
          p.home_generation !== e.registry.server_generation ||
          p.served_by_environment_id !== e.provenance.served_by_environment_id ||
          p.observed_at !== e.provenance.observed_at
        )
          fail("Native placement provenance mismatch.");
      }
      break;
    }
  }
  if (
    "items" in value &&
    "limit" in expected.params &&
    expected.params.limit !== undefined &&
    value.items.length > expected.params.limit
  )
    fail("Native page exceeds requested limit.");
  return result;
}
type ValueItems<V> = V extends { readonly items: readonly (infer I)[] } ? I : never;
type ItemOf<R extends NativeReadRoute> = ValueItems<NativeReadResult<R>["value"]>;
export interface NativeCompleteRead<R extends NativeReadRoute = NativeReadRoute> {
  readonly request: WorkstreamsHomeReadResultRequest;
  /** Every page retains its closed route, value, envelope and authorization revision. */
  readonly pages: readonly NativeReadResult<R>[];
  readonly items: readonly ItemOf<R>[];
  readonly envelope: WorkstreamsNativeReadEnvelope;
  readonly authorization_revision: number;
  readonly stale: boolean;
  readonly readOnly: boolean;
}
export interface NativeWorkstreamReaderOptions {
  readonly read: NativeReadTransport;
  readonly scope: NativeReadScope;
  readonly sha256?: NativeReadSha256;
  /** Only explicit transport outages qualify; authorization/domain failures must return false. */
  readonly isTransientError?: (cause: unknown) => boolean;
  readonly cache?: NativeWorkstreamReadCache;
  readonly maxPages?: number;
  readonly maxItems?: number;
  readonly maxBytes?: number;
}
function isAuthorityFailure(cause: unknown): boolean {
  if (!cause || typeof cause !== "object") return false;
  const status =
    "status" in cause ? cause.status : "statusCode" in cause ? cause.statusCode : undefined;
  if (
    typeof status === "number" &&
    status >= 400 &&
    status < 500 &&
    status !== 408 &&
    status !== 429
  )
    return true;
  const code = "code" in cause ? cause.code : undefined;
  return (
    typeof code === "string" &&
    [
      "forbidden",
      "unauthorized",
      "revoked",
      "generation_mismatch",
      "generation_conflict",
      "authority_fenced",
      "fenced",
      "not_granted",
      "owner_mismatch",
      "principal_mismatch",
      "binding_mismatch",
      "not_found",
      "authorization_revision_mismatch",
      "home_mismatch",
      "scope_changed",
      "contract_mismatch",
      "invalid_response",
      "cursor_stale",
      "cursor_invalid",
    ].includes(code)
  );
}
const pageRoutes = new Set<NativeReadRoute>([
  "workstreams",
  "references",
  "read-projection",
  "memberships",
  "declarations",
  "lifecycle-declarations",
  "edges",
  "history",
  "placements",
]);
function itemKey(route: NativeReadRoute, item: unknown): string {
  const v = item as Record<string, unknown>;
  switch (route) {
    case "read-projection":
      return String((v.workstream as { workstream_id: string }).workstream_id);
    case "workstreams":
      return String(v.workstream_id);
    case "references":
      return String(v.native_reference_id);
    case "memberships":
    case "placements":
      return String(v.membership_id);
    case "declarations":
      return JSON.stringify([v.declaration_id, v.revision]);
    case "lifecycle-declarations":
      return String(v.lifecycle_declaration_id);
    case "edges":
      return String(v.edge_id);
    case "history":
      return String(v.event_id);
    default:
      return fail("Route is not a collection.");
  }
}
function staleCopy<R extends NativeReadRoute>(
  snapshot: NativeCompleteRead<R>,
): NativeCompleteRead<R> {
  const result = clone(snapshot);
  const stale = (e: WorkstreamsNativeReadEnvelope): WorkstreamsNativeReadEnvelope => ({
    ...e,
    provenance: { ...e.provenance, source: "cache", stale: true },
    coverage: { ...e.coverage, registry: "stale" },
  });
  return {
    ...result,
    stale: true,
    readOnly: true,
    envelope: stale(result.envelope),
    pages: result.pages.map((p) => ({ ...p, envelope: stale(p.envelope) })),
  };
}
/** One instance per UI read scope. Call setScope on binding refresh and invalidate on revocation. */
export class NativeWorkstreamReader {
  #scope: NativeReadScope;
  #scopeKey: string;
  #valid = true;
  #epoch = 0;
  #sequence = 0;
  #active = new Map<string, { id: number; controller: AbortController }>();
  readonly #cache: NativeWorkstreamReadCache;
  readonly #options: NativeWorkstreamReaderOptions;
  constructor(options: NativeWorkstreamReaderOptions) {
    this.#scope = validateNativeReadScope(options.scope);
    this.#scopeKey = nativeReadScopeKey(this.#scope);
    this.#cache = options.cache ?? new NativeWorkstreamReadCache();
    this.#cache.readProtected(this.#scopeKey, "");
    this.#options = { ...options };
    if (
      !Number.isSafeInteger(options.maxBytes ?? 16777216) ||
      (options.maxBytes ?? 16777216) < 1 ||
      (options.maxBytes ?? 16777216) > 16777216
    )
      throw new NativeReadError("bounds", "Invalid native byte bound.");
    for (const n of [options.maxPages ?? 1000, options.maxItems ?? 10000])
      if (!Number.isSafeInteger(n) || n < 1 || n > 10000)
        throw new NativeReadError("bounds", "Invalid native read bound.");
  }
  setScope(scope: NativeReadScope): void {
    // Purge before validating so a malformed replacement cannot leave protected bytes accessible.
    const previous = this.#scopeKey;
    let validated: NativeReadScope;
    try {
      validated = validateNativeReadScope(scope);
    } catch (cause) {
      this.invalidate();
      throw cause;
    }
    const key = nativeReadScopeKey(validated);
    if (key !== previous) this.invalidate();
    this.#valid = true;
    this.#scope = validated;
    this.#scopeKey = key;
  }
  invalidate(): void {
    this.#valid = false;
    this.#epoch++;
    for (const entry of this.#active.values()) entry.controller.abort();
    this.#active.clear();
    this.#cache.purge();
  }
  async read(
    request: WorkstreamsHomeReadResultRequest,
    signal?: AbortSignal,
  ): Promise<WorkstreamsHomeReadResult> {
    const snapshot = await this.#load(request, signal, false);
    return snapshot.pages[0]!;
  }
  async complete<R extends NativeReadRoute = NativeReadRoute>(
    request: WorkstreamsHomeReadResultRequest & { readonly request: { readonly route: R } },
    signal?: AbortSignal,
  ): Promise<NativeCompleteRead<R>> {
    return (await this.#load(request, signal, true)) as unknown as NativeCompleteRead<R>;
  }
  projection(signal?: AbortSignal): Promise<NativeCompleteRead<"read-projection">> {
    return this.complete(nativeReadRequest({ route: "read-projection", params: {} }), signal);
  }
  list(signal?: AbortSignal): Promise<NativeCompleteRead<"workstreams">> {
    return this.complete(nativeReadRequest({ route: "workstreams", params: {} }), signal);
  }
  detail(workstreamId: string, signal?: AbortSignal): Promise<NativeCompleteRead<"workstream">> {
    return this.complete(
      nativeReadRequest({ route: "workstream", params: { workstream_id: workstreamId } }),
      signal,
    );
  }
  history(workstreamId?: string, signal?: AbortSignal): Promise<NativeCompleteRead<"history">> {
    return this.complete(
      nativeReadRequest({
        route: "history",
        params: workstreamId === undefined ? {} : { workstream_id: workstreamId },
      }),
      signal,
    );
  }
  async #load(
    input: WorkstreamsHomeReadResultRequest,
    signal: AbortSignal | undefined,
    complete: boolean,
  ): Promise<NativeCompleteRead> {
    if (!this.#valid)
      throw new NativeReadError(
        "scope_changed",
        "Refresh native authorization before reading again.",
      );
    const request = decodeRequest(input),
      scope = clone(this.#scope),
      scopeKey = this.#scopeKey;
    if (
      complete &&
      "cursor" in request.request.params &&
      request.request.params.cursor !== undefined
    )
      throw new NativeReadError(
        "incomplete",
        "A complete native read must start at the first page.",
      );
    const key = JSON.stringify([complete, request]);
    const {
      limit: _limit,
      cursor: _cursor,
      ...selection
    } = request.request.params as Record<string, unknown>;
    const activeKey = JSON.stringify([
      request.request.route,
      request.placement_contract ?? null,
      Object.entries(selection).sort(([a], [b]) => a.localeCompare(b)),
    ]);
    this.#active.get(activeKey)?.controller.abort();
    const id = ++this.#sequence,
      epoch = this.#epoch,
      controller = new AbortController();
    this.#active.set(activeKey, { id, controller });
    const abort = () => controller.abort();
    signal?.addEventListener("abort", abort, { once: true });
    if (signal?.aborted) abort();
    const fence = () => {
      if (
        controller.signal.aborted ||
        epoch !== this.#epoch ||
        this.#active.get(activeKey)?.id !== id
      )
        throw new NativeReadError("cancelled", "Native read cancelled, replaced or invalidated.");
    };
    const pages: WorkstreamsHomeReadResult[] = [],
      items: unknown[] = [],
      cursors = new Set<string>(),
      identities = new Set<string>();
    let current = request;
    let bytes = 0,
      transientFailure = false;
    try {
      if (scope.capability.mode === "native-member" && scope.capability.home_link !== "ok") {
        fence();
        const cached = complete
          ? (this.#cache.readProtected(scopeKey, key) as NativeCompleteRead | null)
          : null;
        if (cached) return staleCopy(cached);
        throw new NativeReadError(
          "incomplete",
          "Native home link unavailable and no complete memory snapshot exists.",
        );
      }
      for (;;) {
        fence();
        let raw: unknown;
        try {
          raw = await this.#options.read(current, controller.signal);
        } catch (cause) {
          fence();
          if (!isAuthorityFailure(cause) && this.#options.isTransientError?.(cause) === true) {
            transientFailure = true;
            const cached = complete
              ? (this.#cache.readProtected(scopeKey, key) as NativeCompleteRead | null)
              : null;
            if (cached) return staleCopy(cached);
            throw cause;
          }
          this.invalidate();
          throw cause;
        }
        fence();
        const page = await validateNativeReadResult(raw, current, scope, this.#options.sha256);
        fence();
        bytes += new TextEncoder().encode(JSON.stringify(page)).length;
        if (bytes > (this.#options.maxBytes ?? 16777216))
          throw new NativeReadError("bounds", "Native read byte bound exceeded.");
        assertComplete(page.envelope);
        if (pages[0]) assertNativeReadSnapshot(pages[0].envelope, page.envelope);
        pages.push(page);
        if ("items" in page.value) {
          for (const item of page.value.items) {
            const identity = itemKey(page.route, item);
            if (identities.has(identity)) fail("Duplicate entity across native pages.");
            identities.add(identity);
            items.push(item);
          }
        }
        if (
          pages.length > (this.#options.maxPages ?? 1000) ||
          items.length > (this.#options.maxItems ?? 10000)
        )
          throw new NativeReadError("bounds", "Native read exceeds configured bounds.");
        const next = "next_cursor" in page.value ? page.value.next_cursor : null;
        if (!complete || !pageRoutes.has(page.route) || next === null) break;
        if (
          cursors.has(next) ||
          ("cursor" in current.request.params && next === current.request.params.cursor)
        )
          fail("Duplicate or cyclic native cursor.");
        cursors.add(next);
        if (pages.length >= (this.#options.maxPages ?? 1000))
          throw new NativeReadError("bounds", "Native page bound reached.");
        current = decodeRequest({
          ...request,
          request: { ...request.request, params: { ...request.request.params, cursor: next } },
        });
      }
      fence();
      const first = pages[0]!;
      const result: NativeCompleteRead = {
        request,
        pages,
        items: items as never[],
        envelope: first.envelope,
        authorization_revision: first.authorization_revision,
        stale: false,
        readOnly: false,
      };
      if (complete) this.#cache.writeProtected(scopeKey, key, result);
      return clone(result);
    } catch (cause) {
      if (cause instanceof NativeReadError && cause.code !== "cancelled") this.invalidate();
      // Closed schema decoding failures must also purge, never qualify as transport outages.
      if (!(cause instanceof NativeReadError) && !transientFailure) this.invalidate();
      throw cause;
    } finally {
      signal?.removeEventListener("abort", abort);
      if (this.#active.get(activeKey)?.id === id) this.#active.delete(activeKey);
    }
  }
}
