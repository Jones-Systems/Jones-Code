import * as NodeCrypto from "node:crypto";

import {
  WORKSTREAM_CONTRACT_FAMILY,
  WORKSTREAM_CONTRACT_HEADER_VERSION,
  WORKSTREAM_CONTRACT_MANIFEST_SHA256,
  WORKSTREAM_CONTRACT_VERSION,
  WORKSTREAM_MAX_PAGE_ITEMS,
  type T3WorkstreamBinding,
  type T3WorkstreamListResult,
  type Workstream,
  type WorkstreamCapabilities,
  type WorkstreamCommand,
  type WorkstreamDeclarationPage,
  type WorkstreamDetail,
  type WorkstreamEdgePage,
  type WorkstreamHistoryPage,
  type WorkstreamMembershipPage,
  type WorkstreamPage,
  type WorkstreamReceipt,
  type WorkstreamReferenceDetail,
  type WorkstreamReferencePage,
  T3PlacementPage,
  T3PlacementResult,
  T3PlacementLoadRequest,
  type T3PlacementRequest,
  type T3ThreadPlacement,
  T3_PLACEMENT_MAX_PAGES,
  WORKSTREAM_MAX_RESPONSE_BYTES,
  t3PlacementIdentityKey,
  t3PlacementInventoryJson,
} from "@t3tools/contracts";
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";

import type { T3PlacementTrustProvider } from "../environment/NativePlacementTrust.ts";

export {
  WORKSTREAM_CONTRACT_FAMILY,
  WORKSTREAM_CONTRACT_HEADER_VERSION,
  WORKSTREAM_CONTRACT_MANIFEST_SHA256,
  WORKSTREAM_CONTRACT_VERSION,
};
export type {
  Workstream as WorkstreamMetadata,
  WorkstreamCapabilities,
  WorkstreamCommand,
  WorkstreamPage,
  WorkstreamReceipt,
};
export type { T3PlacementTrustProvider } from "../environment/NativePlacementTrust.ts";

export class WorkstreamTransportError extends Schema.TaggedError<WorkstreamTransportError>()(
  "WorkstreamTransportError",
  {
    operation: Schema.String,
    effect: Schema.Literals(["no-effect", "unknown-effect"]),
    detail: Schema.String,
    cause: Schema.optional(Schema.Defect()),
  },
) {}

export class WorkstreamGatewayError extends Schema.TaggedError<WorkstreamGatewayError>()(
  "WorkstreamGatewayError",
  {
    reason: Schema.Literals([
      "offline",
      "stale",
      "contract-mismatch",
      "permission-denied",
      "version-conflict",
      "cursor-stale",
      "idempotency-conflict",
      "invalid-response",
      "unknown-effect",
    ]),
    detail: Schema.String,
    cause: Schema.optional(Schema.Defect()),
  },
) {}

interface PageInput {
  readonly limit: number;
  readonly cursor?: string;
}

interface ContractInput {
  readonly contractVersion: typeof WORKSTREAM_CONTRACT_HEADER_VERSION;
  readonly contractManifest: typeof WORKSTREAM_CONTRACT_MANIFEST_SHA256;
}

import type {
  WorkstreamAppearanceResult,
  WorkstreamAppearanceWrite,
  WorkstreamAppearance,
} from "@t3tools/contracts";

export interface WorkstreamTransport {
  readonly readAppearance?: (input: {
    readonly workstream_ids: readonly string[];
  }) => Effect.Effect<WorkstreamAppearanceResult, WorkstreamTransportError>;
  readonly saveAppearance?: (
    input: WorkstreamAppearanceWrite,
  ) => Effect.Effect<WorkstreamAppearance, WorkstreamTransportError>;
  readonly listThreadPlacements?: (
    input: T3PlacementRequest,
  ) => Effect.Effect<T3PlacementPage, WorkstreamTransportError>;
  readonly getCapabilities: (
    input: ContractInput,
  ) => Effect.Effect<WorkstreamCapabilities, WorkstreamTransportError>;
  readonly listWorkstreams: (
    input: PageInput & ContractInput,
  ) => Effect.Effect<WorkstreamPage, WorkstreamTransportError>;
  readonly getWorkstream: (
    input: { readonly workstreamId: string } & ContractInput,
  ) => Effect.Effect<WorkstreamDetail, WorkstreamTransportError>;
  readonly listReferences: (
    input: PageInput & ContractInput,
  ) => Effect.Effect<WorkstreamReferencePage, WorkstreamTransportError>;
  readonly getReference: (
    input: { readonly nativeReferenceId: string } & ContractInput,
  ) => Effect.Effect<WorkstreamReferenceDetail, WorkstreamTransportError>;
  readonly listMemberships: (
    input: { readonly workstreamId: string } & PageInput & ContractInput,
  ) => Effect.Effect<WorkstreamMembershipPage, WorkstreamTransportError>;
  readonly listDeclarations: (
    input: { readonly workstreamId: string } & PageInput & ContractInput,
  ) => Effect.Effect<WorkstreamDeclarationPage, WorkstreamTransportError>;
  readonly listEdges: (
    input: { readonly workstreamId: string } & PageInput & ContractInput,
  ) => Effect.Effect<WorkstreamEdgePage, WorkstreamTransportError>;
  readonly listHistory: (
    input: { readonly workstreamId?: string } & PageInput & ContractInput,
  ) => Effect.Effect<WorkstreamHistoryPage, WorkstreamTransportError>;
  readonly getCommand: (
    input: { readonly commandId: string } & ContractInput,
  ) => Effect.Effect<WorkstreamReceipt, WorkstreamTransportError>;
  readonly submitCommand: (
    input: {
      readonly body: string;
      readonly command: WorkstreamCommand;
      readonly idempotencyKey: string;
    } & ContractInput,
  ) => Effect.Effect<WorkstreamReceipt, WorkstreamTransportError>;
}

export interface WorkstreamGatewayOptions {
  readonly placementTrustProvider?: T3PlacementTrustProvider;
  readonly binding: Pick<
    T3WorkstreamBinding,
    "registryId" | "ownerId" | "principalId" | "authorizationRevision"
  >;
  readonly cacheCapacity?: number;
  readonly cacheMaxAgeMs?: number;
  readonly now?: () => number;
}

export class WorkstreamGateway extends Context.Service<
  WorkstreamGateway,
  {
    readonly readAppearance: (
      ids: readonly string[],
    ) => Effect.Effect<WorkstreamAppearanceResult, WorkstreamGatewayError>;
    readonly saveAppearance: (
      input: WorkstreamAppearanceWrite,
    ) => Effect.Effect<WorkstreamAppearance, WorkstreamGatewayError>;
    readonly readThreadPlacements: (
      input: T3PlacementLoadRequest,
    ) => Effect.Effect<T3PlacementResult, WorkstreamGatewayError>;
    readonly readSession: () => Effect.Effect<T3WorkstreamBinding, WorkstreamGatewayError>;
    readonly readMetadata: (input?: {
      readonly limit?: number;
      readonly cursor?: string;
    }) => Effect.Effect<T3WorkstreamListResult, WorkstreamGatewayError>;
    readonly readDetail: (
      workstreamId: string,
    ) => Effect.Effect<WorkstreamDetail, WorkstreamGatewayError>;
    readonly readReferences: (input?: {
      readonly limit?: number;
      readonly cursor?: string;
    }) => Effect.Effect<WorkstreamReferencePage, WorkstreamGatewayError>;
    readonly readReference: (
      nativeReferenceId: string,
    ) => Effect.Effect<WorkstreamReferenceDetail, WorkstreamGatewayError>;
    readonly readMemberships: (
      workstreamId: string,
      input?: { readonly limit?: number; readonly cursor?: string },
    ) => Effect.Effect<WorkstreamMembershipPage, WorkstreamGatewayError>;
    readonly readDeclarations: (
      workstreamId: string,
      input?: { readonly limit?: number; readonly cursor?: string },
    ) => Effect.Effect<WorkstreamDeclarationPage, WorkstreamGatewayError>;
    readonly readEdges: (
      workstreamId: string,
      input?: { readonly limit?: number; readonly cursor?: string },
    ) => Effect.Effect<WorkstreamEdgePage, WorkstreamGatewayError>;
    readonly readHistory: (
      workstreamId: string | undefined,
      input?: { readonly limit?: number; readonly cursor?: string },
    ) => Effect.Effect<WorkstreamHistoryPage, WorkstreamGatewayError>;
    readonly pollCommand: (
      commandId: string,
    ) => Effect.Effect<WorkstreamReceipt, WorkstreamGatewayError>;
    readonly submit: (
      command: WorkstreamCommand,
    ) => Effect.Effect<WorkstreamReceipt, WorkstreamGatewayError>;
    readonly purgeAuthorization: () => void;
  }
>()("t3/workstreams/WorkstreamGateway") {}

const contractInput: ContractInput = {
  contractVersion: WORKSTREAM_CONTRACT_HEADER_VERSION,
  contractManifest: WORKSTREAM_CONTRACT_MANIFEST_SHA256,
};

const canonicalJson = (value: unknown): string => {
  if (value === null || typeof value !== "object") return JSON.stringify(value);
  if (Array.isArray(value)) return `[${value.map(canonicalJson).join(",")}]`;
  const entries = Object.entries(value as Record<string, unknown>)
    .filter(([, entry]) => entry !== undefined)
    .toSorted(([left], [right]) => left.localeCompare(right));
  return `{${entries.map(([key, entry]) => `${JSON.stringify(key)}:${canonicalJson(entry)}`).join(",")}}`;
};

const transportFailure = (error: WorkstreamTransportError) =>
  new WorkstreamGatewayError({
    reason:
      error.detail === "cursor_stale"
        ? "cursor-stale"
        : error.effect === "unknown-effect"
          ? "unknown-effect"
          : "offline",
    detail: error.detail,
  });

function pageInput(input: { readonly limit?: number; readonly cursor?: string } = {}): PageInput {
  const limit = input.limit ?? 50;
  if (!Number.isSafeInteger(limit) || limit < 1 || limit > WORKSTREAM_MAX_PAGE_ITEMS) {
    throw new WorkstreamGatewayError({
      reason: "invalid-response",
      detail: "Workstream reads require a limit from 1 through 100.",
    });
  }
  return { limit, ...(input.cursor === undefined ? {} : { cursor: input.cursor }) };
}

export const make = (transport: WorkstreamTransport, options: WorkstreamGatewayOptions) =>
  Effect.sync(() => {
    const cacheCapacity = Math.max(1, options.cacheCapacity ?? 32);
    const cacheMaxAgeMs = Math.max(0, options.cacheMaxAgeMs ?? 30_000);
    const now = options.now ?? Date.now;
    const pageCache = new Map<
      string,
      { readonly result: T3WorkstreamListResult; readonly at: number }
    >();
    const terminalReceipts = new Map<string, WorkstreamReceipt>();
    const commandDigests = new Map<string, string>();
    let lastBindingKey: string | null = null;

    const purgeAuthorization = () => {
      pageCache.clear();
      terminalReceipts.clear();
      commandDigests.clear();
      lastBindingKey = null;
    };
    const remember = <V>(map: Map<string, V>, key: string, value: V) => {
      map.delete(key);
      map.set(key, value);
      while (map.size > cacheCapacity) {
        const oldest = map.keys().next().value;
        if (oldest === undefined) break;
        map.delete(oldest);
      }
    };

    const authorize = (permission: "workstreams:read" | "workstreams:write") =>
      transport.getCapabilities(contractInput).pipe(
        Effect.mapError(transportFailure),
        Effect.flatMap((capabilities) => {
          if (
            capabilities.contract_family !== WORKSTREAM_CONTRACT_FAMILY ||
            capabilities.contract_version !== WORKSTREAM_CONTRACT_VERSION ||
            capabilities.manifest_sha256 !== WORKSTREAM_CONTRACT_MANIFEST_SHA256
          )
            return Effect.fail(
              new WorkstreamGatewayError({
                reason: "contract-mismatch",
                detail: "Control-plane contract identity does not match the accepted binding.",
              }),
            );
          if (capabilities.context.owner_id !== options.binding.ownerId)
            return Effect.fail(
              new WorkstreamGatewayError({
                reason: "permission-denied",
                detail: "Control-plane owner binding does not match this activation.",
              }),
            );
          if (!capabilities.permissions.includes(permission))
            return Effect.fail(
              new WorkstreamGatewayError({
                reason: "permission-denied",
                detail: `Current binding does not grant ${permission}.`,
              }),
            );
          if (
            !Number.isSafeInteger(capabilities.max_page_items) ||
            capabilities.max_page_items < 1 ||
            capabilities.max_page_items > WORKSTREAM_MAX_PAGE_ITEMS
          )
            return Effect.fail(
              new WorkstreamGatewayError({
                reason: "invalid-response",
                detail: "Control-plane page bound is invalid.",
              }),
            );
          const binding: T3WorkstreamBinding = {
            registryId: options.binding.registryId,
            ownerId: options.binding.ownerId,
            principalId: options.binding.principalId,
            authorizationRevision: options.binding.authorizationRevision,
            serverGeneration: capabilities.context.server_generation,
            registryVersion: capabilities.context.registry_version,
            permissions: capabilities.permissions,
            contractVersion: WORKSTREAM_CONTRACT_HEADER_VERSION,
            contractManifest: WORKSTREAM_CONTRACT_MANIFEST_SHA256,
          };
          const key = [
            binding.registryId,
            binding.ownerId,
            binding.principalId,
            binding.authorizationRevision,
            binding.serverGeneration,
            binding.registryVersion,
            [...binding.permissions].sort().join(","),
            binding.contractVersion,
            binding.contractManifest,
          ].join("\u0000");
          if (lastBindingKey !== null && lastBindingKey !== key) purgeAuthorization();
          lastBindingKey = key;
          return Effect.succeed({ capabilities, binding, key });
        }),
      );

    const validateContext = <A>(
      value: A,
      binding: T3WorkstreamBinding,
    ): Effect.Effect<A, WorkstreamGatewayError> => {
      if (!value || typeof value !== "object" || !("context" in value))
        return Effect.fail(
          new WorkstreamGatewayError({
            reason: "invalid-response",
            detail: "Workstream response omitted its read context.",
          }),
        );
      const context = (value as { context?: unknown }).context;
      if (!context || typeof context !== "object")
        return Effect.fail(
          new WorkstreamGatewayError({
            reason: "invalid-response",
            detail: "Workstream response context is invalid.",
          }),
        );
      const candidate = context as Record<string, unknown>;
      if (
        candidate.owner_id !== binding.ownerId ||
        candidate.server_generation !== binding.serverGeneration ||
        candidate.registry_version !== binding.registryVersion
      )
        return Effect.fail(
          new WorkstreamGatewayError({
            reason: "stale",
            detail:
              "Workstream response does not match the current owner, generation, and registry revision.",
          }),
        );
      const ownerMatches = (item: unknown): boolean => {
        if (Array.isArray(item)) return item.every(ownerMatches);
        if (!item || typeof item !== "object") return true;
        const record = item as Record<string, unknown>;
        if (typeof record.owner_id === "string" && record.owner_id !== binding.ownerId) {
          return false;
        }
        return Object.values(record).every(ownerMatches);
      };
      if (!ownerMatches(value)) {
        return Effect.fail(
          new WorkstreamGatewayError({
            reason: "invalid-response",
            detail: "Workstream response contains an item from another owner.",
          }),
        );
      }
      return Effect.succeed(value);
    };

    const readSession = () =>
      authorize("workstreams:read").pipe(Effect.map(({ binding }) => binding));
    const readThreadPlacements = Effect.fn("WorkstreamGateway.readThreadPlacements")(function* (
      input: T3PlacementLoadRequest,
    ) {
      const invalid = (detail: string) =>
        new WorkstreamGatewayError({ reason: "invalid-response", detail });
      const request = yield* Schema.decodeUnknownEffect(T3PlacementLoadRequest)(input, {
        onExcessProperty: "error",
      }).pipe(Effect.mapError(() => invalid("Invalid placement inventory.")));
      const authorized = yield* authorize("workstreams:read");
      if (!transport.listThreadPlacements)
        return yield* new WorkstreamGatewayError({
          reason: "offline",
          detail: "Thread placements are unavailable.",
        });
      const inventory_sha256 = NodeCrypto.createHash("sha256")
        .update(t3PlacementInventoryJson(request.identities))
        .digest("hex");
      const requested = new Set(request.identities.map(t3PlacementIdentityKey));
      const items: T3ThreadPlacement[] = [];
      const memberships = new Set<string>();
      const references = new Map<string, string>();
      const cursors = new Set<string>();
      let cursor: string | undefined;
      let previous: readonly [string, string] | undefined;
      let finalPage: T3PlacementPage | undefined;
      let responseBytes = 0;
      for (let pageIndex = 0; pageIndex < T3_PLACEMENT_MAX_PAGES; pageIndex++) {
        const raw = yield* transport
          .listThreadPlacements({
            identities: request.identities,
            limit: 100,
            ...(cursor === undefined ? {} : { cursor }),
          })
          .pipe(Effect.mapError(transportFailure));
        const page = yield* Schema.decodeUnknownEffect(T3PlacementPage)(raw, {
          onExcessProperty: "error",
        }).pipe(
          Effect.mapError(
            () =>
              new WorkstreamGatewayError({
                reason: "invalid-response",
                detail: "Invalid thread placement page.",
              }),
          ),
        );
        yield* validateContext(page, authorized.binding);
        if (
          page.context.principal_id !== options.binding.principalId ||
          page.context.authorization_revision !== options.binding.authorizationRevision ||
          page.inventory_sha256 !== inventory_sha256 ||
          page.items.some(
            (item) =>
              !Number.isFinite(Date.parse(item.attested_at)) ||
              !Number.isFinite(Date.parse(item.expires_at)) ||
              Date.parse(item.attested_at) > now() ||
              Date.parse(item.expires_at) <= now() ||
              Date.parse(item.expires_at) <= Date.parse(item.attested_at),
          )
        )
          return yield* new WorkstreamGatewayError({
            reason: "stale",
            detail:
              "Thread placements do not match the current principal, grant, or attestation lifetime.",
          });
        responseBytes += Buffer.byteLength(canonicalJson(page));
        if (responseBytes > WORKSTREAM_MAX_RESPONSE_BYTES)
          return yield* invalid("Placement response workload exceeded.");
        for (const item of page.items) {
          const tuple = [item.native_reference_id, item.membership_id] as const;
          if (
            !requested.has(
              t3PlacementIdentityKey({
                source_instance_id: item.source_instance_id,
                native_thread_id: item.native_thread_id,
              }),
            ) ||
            memberships.has(item.membership_id) ||
            (previous &&
              (tuple[0] < previous[0] || (tuple[0] === previous[0] && tuple[1] <= previous[1])))
          )
            return yield* invalid("Repeated, unordered, or unrelated placement.");
          const {
            membership_id: _membership,
            workstream_id: _workstream,
            kind: _kind,
            ...routing
          } = item;
          const serialized = canonicalJson(routing);
          const prior = references.get(item.native_reference_id);
          if (prior !== undefined && prior !== serialized)
            return yield* invalid("Placement reference changed.");
          references.set(item.native_reference_id, serialized);
          previous = tuple;
          memberships.add(item.membership_id);
          items.push(item);
        }
        if (page.next_cursor === null) {
          finalPage = page;
          break;
        }
        if (cursors.has(page.next_cursor)) return yield* invalid("Placement cursor repeated.");
        cursors.add(page.next_cursor);
        cursor = page.next_cursor;
      }
      if (!finalPage) return yield* invalid("Placement page workload exceeded.");
      if (items.some((item) => Date.parse(item.expires_at) <= now()))
        return yield* invalid("Placement expired while loading.");
      const trustSnapshot = yield* Effect.try({
        try: () =>
          options.placementTrustProvider?.readTrustSnapshot() ?? {
            trustedEnvironments: [],
            readiness: "trust-provider-required" as const,
          },
        catch: () =>
          new WorkstreamGatewayError({
            reason: "invalid-response",
            detail: "Native trust provider unavailable.",
          }),
      });
      const { trustedEnvironments, readiness } = trustSnapshot;
      if (
        new Set(trustedEnvironments.map((value) => value.environmentId)).size !==
        trustedEnvironments.length
      ) {
        return yield* new WorkstreamGatewayError({
          reason: "invalid-response",
          detail: "Duplicate native trust environment.",
        });
      }
      const result = {
        page: { ...finalPage, items, next_cursor: null },
        trustedEnvironments,
        readiness,
      };
      if (Buffer.byteLength(canonicalJson(result)) > WORKSTREAM_MAX_RESPONSE_BYTES)
        return yield* invalid("Placement response workload exceeded.");
      return yield* Schema.decodeUnknownEffect(T3PlacementResult)(result, {
        onExcessProperty: "error",
      }).pipe(
        Effect.mapError(
          () =>
            new WorkstreamGatewayError({
              reason: "invalid-response",
              detail: "Invalid native trust snapshot.",
            }),
        ),
      );
    });
    const readMetadata: WorkstreamGateway["Service"]["readMetadata"] = (input = {}) =>
      Effect.gen(function* () {
        const authorized = yield* authorize("workstreams:read");
        const request = yield* Effect.try({
          try: () => pageInput(input),
          catch: (cause) => cause as WorkstreamGatewayError,
        });
        if (request.limit > authorized.capabilities.max_page_items)
          return yield* new WorkstreamGatewayError({
            reason: "invalid-response",
            detail: "Requested page exceeds the advertised bound.",
          });
        const cacheKey = `${authorized.key}\u0000${request.limit}\u0000${request.cursor ?? ""}`;
        const live = yield* Effect.result(
          transport
            .listWorkstreams({ ...request, ...contractInput })
            .pipe(Effect.mapError(transportFailure)),
        );
        if (live._tag === "Failure") {
          const cached = pageCache.get(cacheKey);
          if (live.failure.reason !== "offline" || cached === undefined) return yield* live.failure;
          return {
            ...cached.result,
            source: "cache",
            stale: now() - cached.at > cacheMaxAgeMs,
          };
        }
        yield* validateContext(live.success, authorized.binding);
        if (
          live.success.items.length > request.limit ||
          live.success.items.some((item) => item.owner_id !== authorized.binding.ownerId)
        )
          return yield* new WorkstreamGatewayError({
            reason: "invalid-response",
            detail: "Control-plane returned an over-bound or cross-owner Workstream page.",
          });
        const result: T3WorkstreamListResult = {
          binding: authorized.binding,
          items: live.success.items.map((item: Workstream) => ({
            workstreamId: item.workstream_id,
            name: item.name,
            lifecycle: item.lifecycle,
            progress: item.progress,
            delivery: item.delivery,
            freshness: item.freshness,
            sortOrder: item.sort_order,
            version: item.version,
            updatedAt: item.updated_at,
          })),
          nextCursor: live.success.next_cursor,
          source: "live",
          stale: false,
        };
        remember(pageCache, cacheKey, { result, at: now() });
        return result;
      });

    const boundRead = <A>(
      run: (request: PageInput & ContractInput) => Effect.Effect<A, WorkstreamTransportError>,
      input?: { readonly limit?: number; readonly cursor?: string },
    ) =>
      Effect.gen(function* () {
        const authorized = yield* authorize("workstreams:read");
        const page = yield* Effect.try({
          try: () => pageInput(input),
          catch: (cause) => cause as WorkstreamGatewayError,
        });
        const value = yield* run({ ...page, ...contractInput }).pipe(
          Effect.mapError(transportFailure),
        );
        return yield* validateContext(value, authorized.binding);
      });
    const boundDetail = <A>(run: () => Effect.Effect<A, WorkstreamTransportError>) =>
      Effect.gen(function* () {
        const authorized = yield* authorize("workstreams:read");
        const value = yield* run().pipe(Effect.mapError(transportFailure));
        return yield* validateContext(value, authorized.binding);
      });
    const validateReceipt = (
      receipt: WorkstreamReceipt,
      commandId: string,
      operation?: string,
      bodySha256?: string,
      binding?: T3WorkstreamBinding,
    ) => {
      if (
        receipt.command_id !== commandId ||
        (operation !== undefined && receipt.operation !== operation) ||
        (bodySha256 !== undefined && receipt.request_sha256 !== bodySha256) ||
        (binding !== undefined &&
          (receipt.owner_id !== binding.ownerId ||
            receipt.actor.principal_id !== binding.principalId ||
            receipt.server_generation !== binding.serverGeneration))
      )
        return Effect.fail(
          new WorkstreamGatewayError({
            reason: "invalid-response",
            detail:
              "Receipt is not bound to the current owner, principal, generation, command, and request.",
          }),
        );
      return Effect.succeed(receipt);
    };
    const pollCommand = (commandId: string) =>
      Effect.gen(function* () {
        const authorized = yield* authorize("workstreams:read");
        const receipt = yield* transport
          .getCommand({ commandId, ...contractInput })
          .pipe(Effect.mapError(transportFailure));
        return yield* validateReceipt(receipt, commandId, undefined, undefined, authorized.binding);
      });
    const submit = (command: WorkstreamCommand) =>
      Effect.gen(function* () {
        const authorized = yield* authorize("workstreams:write");
        if (
          command.expected_server_generation !== authorized.binding.serverGeneration ||
          command.expected_registry_version !== authorized.binding.registryVersion
        )
          return yield* new WorkstreamGatewayError({
            reason: "version-conflict",
            detail: "Command generation or registry revision is not current.",
          });
        const body = canonicalJson(command);
        const bodySha256 = NodeCrypto.createHash("sha256").update(body).digest("hex");
        const commandKey = [authorized.key, command.command_id].join("\u0000");
        const seenDigest = commandDigests.get(commandKey);
        if (seenDigest !== undefined && seenDigest !== bodySha256)
          return yield* new WorkstreamGatewayError({
            reason: "idempotency-conflict",
            detail: "This command ID was already used with different business bytes.",
          });
        remember(commandDigests, commandKey, bodySha256);
        const replayKey = [authorized.key, command.command_id, bodySha256].join("\u0000");
        const prior = terminalReceipts.get(replayKey);
        if (prior) return prior;
        const receipt = yield* transport
          .submitCommand({ body, command, idempotencyKey: command.command_id, ...contractInput })
          .pipe(Effect.mapError(transportFailure));
        yield* validateReceipt(
          receipt,
          command.command_id,
          command.action.operation,
          bodySha256,
          authorized.binding,
        );
        if (receipt.state === "committed" || receipt.state === "rejected")
          remember(terminalReceipts, replayKey, receipt);
        pageCache.clear();
        return receipt;
      });

    return WorkstreamGateway.of({
      readAppearance: (ids) =>
        Effect.gen(function* () {
          const authorized = yield* authorize("workstreams:read");
          if (!transport.readAppearance) return { supported: false as const };
          const result = yield* transport
            .readAppearance({ workstream_ids: ids })
            .pipe(Effect.mapError(transportFailure));
          if (
            result.supported &&
            (result.page.owner_id !== authorized.binding.ownerId ||
              result.page.server_generation !== authorized.binding.serverGeneration ||
              result.page.items.length !== ids.length ||
              result.page.items.some((item, index) => item.workstream_id !== ids[index]))
          ) {
            return yield* new WorkstreamGatewayError({
              reason: "invalid-response",
              detail: "Appearance owner, generation or requested inventory mismatch.",
            });
          }
          return result;
        }),
      saveAppearance: (input) =>
        Effect.gen(function* () {
          yield* authorize("workstreams:write");
          if (!transport.saveAppearance)
            return yield* new WorkstreamGatewayError({
              reason: "version-conflict",
              detail: "Appearance editing is unavailable.",
            });
          const result = yield* transport
            .saveAppearance(input)
            .pipe(Effect.mapError(transportFailure));
          if (
            result.workstream_id !== input.workstream_id ||
            result.border_color !== input.border_color ||
            (result.version !== input.expected_version &&
              result.version !== input.expected_version + 1)
          )
            return yield* new WorkstreamGatewayError({
              reason: "invalid-response",
              detail: "Appearance receipt mismatch.",
            });
          return result;
        }),
      readThreadPlacements,
      readSession,
      readMetadata,
      readDetail: (id) =>
        boundDetail(() => transport.getWorkstream({ workstreamId: id, ...contractInput })),
      readReferences: (input) => boundRead((request) => transport.listReferences(request), input),
      readReference: (id) =>
        boundDetail(() => transport.getReference({ nativeReferenceId: id, ...contractInput })),
      readMemberships: (id, input) =>
        boundRead((request) => transport.listMemberships({ workstreamId: id, ...request }), input),
      readDeclarations: (id, input) =>
        boundRead((request) => transport.listDeclarations({ workstreamId: id, ...request }), input),
      readEdges: (id, input) =>
        boundRead((request) => transport.listEdges({ workstreamId: id, ...request }), input),
      readHistory: (id, input) =>
        boundRead(
          (request) =>
            transport.listHistory({
              ...(id === undefined ? {} : { workstreamId: id }),
              ...request,
            }),
          input,
        ),
      pollCommand,
      submit,
      purgeAuthorization,
    });
  });
