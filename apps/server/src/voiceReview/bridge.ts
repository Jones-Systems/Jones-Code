import {
  VoiceReviewRecentList,
  VoiceReviewDiagnostics,
  ThreadRegistrySnapshot,
  ThreadRegistryComposedSnapshot,
  ThreadRegistryWorkstreams,
  ThreadRegistryEvents,
  ThreadRegistryAssociationPayload,
  ThreadRegistryLabelPayload,
  ThreadRegistryMutationReceipt,
  type ThreadRegistryThread,
  type T3PlacementIdentity,
  type T3PlacementResult,
  AuthOrchestrationOperateScope,
  AuthOrchestrationReadScope,
  type EnvironmentSessionPrincipalShape,
  VoiceReviewDraft,
  VoiceReviewDraftList,
  VoiceReviewMutationResult,
  VoiceReviewRevisionPayload,
  VoiceReviewEditSavePayload,
  VoiceReviewEditCancelPayload,
  VoiceReviewNotConfiguredError,
  VoiceReviewForbiddenError,
  VoiceReviewNotFoundError,
  VoiceReviewConflictError,
  VoiceReviewUnavailableError,
  VoiceReviewError,
  type VoiceReviewAction,
  type VoiceReviewMutationPayload,
} from "@t3tools/contracts";
import * as Schema from "effect/Schema";
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Clock from "effect/Clock";
// @effect-diagnostics-next-line nodeBuiltinImport:off - Effect FileSystem OpenFlag cannot express numeric O_NOFOLLOW | O_NONBLOCK credential guards.
import * as NodeFS from "node:fs";
// @effect-diagnostics-next-line nodeBuiltinImport:off - native descriptors preserve guarded open, fstat, bounded read, and finally close on the same credential file.
import * as NodeFSP from "node:fs/promises";
import {
  voiceReviewConfigFromEnv,
  voiceReviewNativeBindingFromEnv,
  type VoiceReviewConfig,
  type VoiceReviewNativeBinding,
} from "./config.ts";
import {
  makeVoiceReviewCompositionFactory,
  type VoiceReviewCompositionFactory,
} from "./composition.ts";
import { validateVoiceReviewNativePlacementResult } from "./native.ts";

export const VOICE_REVIEW_MAX_REQUEST_BYTES = 610_000;
const MAX_RESPONSE_BYTES = 24_000_000;
const REQUEST_TIMEOUT_MS = 10_000;
const Token = Schema.String.check(
  Schema.isMinLength(1),
  Schema.isMaxLength(4096),
  Schema.isPattern(/^[\x21-\x7e]+$/),
);

const decodeToken = Schema.decodeUnknownSync(Token);
const decodeVoiceReviewDraft = Schema.decodeUnknownSync(VoiceReviewDraft);
const decodeVoiceReviewDraftList = Schema.decodeUnknownSync(VoiceReviewDraftList);
const decodeVoiceReviewMutationResult = Schema.decodeUnknownSync(VoiceReviewMutationResult);
const decodeVoiceReviewEditSavePayload = Schema.decodeUnknownSync(VoiceReviewEditSavePayload);
const decodeVoiceReviewEditCancelPayload = Schema.decodeUnknownSync(VoiceReviewEditCancelPayload);
const decodeVoiceReviewRevisionPayload = Schema.decodeUnknownSync(VoiceReviewRevisionPayload);
const isVoiceReviewNotFoundError = Schema.is(VoiceReviewNotFoundError);
const isVoiceReviewConflictError = Schema.is(VoiceReviewConflictError);

const readToken = async (path: string): Promise<string> => {
  const file = await NodeFSP.open(
    path,
    NodeFS.constants.O_RDONLY | NodeFS.constants.O_NOFOLLOW | NodeFS.constants.O_NONBLOCK,
  );
  try {
    const info = await file.stat();
    if (
      !info.isFile() ||
      info.size > 4096 ||
      (info.mode & 0o077) !== 0 ||
      process.getuid === undefined ||
      info.uid !== process.getuid()
    )
      throw new VoiceReviewUnavailableError({});
    const buffer = Buffer.alloc(4097);
    const { bytesRead } = await file.read(buffer, 0, buffer.length, 0);
    if (bytesRead > 4096) throw new VoiceReviewUnavailableError({});
    return decodeToken(buffer.subarray(0, bytesRead).toString("utf8").trim());
  } finally {
    await file.close();
  }
};

const boundedJson = async (response: Response): Promise<unknown> => {
  const size = response.headers.get("content-length");
  if (size !== null && (!/^\d+$/.test(size) || Number(size) > MAX_RESPONSE_BYTES)) {
    await response.body?.cancel();
    throw new VoiceReviewUnavailableError({});
  }
  if (!response.body) throw new VoiceReviewUnavailableError({});
  const reader = response.body.getReader();
  try {
    const chunks: Uint8Array[] = [];
    let bytes = 0;
    for (;;) {
      const next = await reader.read();
      if (next.done) break;
      bytes += next.value.byteLength;
      if (bytes > MAX_RESPONSE_BYTES) throw new VoiceReviewUnavailableError({});
      chunks.push(next.value);
    }
    return JSON.parse(Buffer.concat(chunks).toString("utf8"));
  } finally {
    await reader.cancel().catch(() => undefined);
    reader.releaseLock();
  }
};

export interface VoiceReviewNativeReadPort {
  readonly identities: (
    threads: readonly ThreadRegistryThread[],
  ) => ReadonlyMap<string, T3PlacementIdentity>;
  readonly read: (
    principal: EnvironmentSessionPrincipalShape,
    identities: readonly T3PlacementIdentity[],
  ) => Promise<T3PlacementResult>;
}

export const makeVoiceReviewBridge = (
  config: VoiceReviewConfig | null,
  fetcher: typeof fetch = globalThis.fetch,
  native?: VoiceReviewNativeReadPort,
  now: () => number = () => Clock.Clock.defaultValue().currentTimeMillisUnsafe(),
) => {
  const authorize = (principal: EnvironmentSessionPrincipalShape, mutation: boolean) => {
    if (config === null) throw new VoiceReviewNotConfiguredError({});
    if (
      !config.allowed_session_ids.has(principal.sessionId) ||
      !principal.scopes.has(mutation ? AuthOrchestrationOperateScope : AuthOrchestrationReadScope)
    ) {
      throw new VoiceReviewForbiddenError({});
    }
    return config;
  };
  const request = async (trusted: VoiceReviewConfig, path: string, payload?: unknown) => {
    const controller = new AbortController();
    // @effect-diagnostics-next-line globalTimers:off - native fetch deadline; cleared on every completion.
    const timeout = setTimeout(() => controller.abort(), REQUEST_TIMEOUT_MS);
    try {
      const token = await readToken(trusted.reviewer_token_file);
      const body = payload === undefined ? undefined : JSON.stringify(payload);
      if (body !== undefined && Buffer.byteLength(body) > VOICE_REVIEW_MAX_REQUEST_BYTES) {
        throw new VoiceReviewConflictError({});
      }
      const response = await fetcher(`${trusted.broker_url}${path}`, {
        method: body === undefined ? "GET" : "POST",
        redirect: "error",
        cache: "no-store",
        signal: controller.signal,
        headers: {
          authorization: `Bearer ${token}`,
          accept: "application/json",
          ...(body === undefined ? {} : { "content-type": "application/json" }),
        },
        ...(body === undefined ? {} : { body }),
      });
      if (!response.ok) {
        await response.body?.cancel();
        if (response.status === 404) throw new VoiceReviewNotFoundError({});
        if (response.status === 409 || response.status === 422 || response.status === 400) {
          throw new VoiceReviewConflictError({});
        }
        throw new VoiceReviewUnavailableError({});
      }
      return await boundedJson(response);
    } catch (error) {
      if (isVoiceReviewNotFoundError(error) || isVoiceReviewConflictError(error)) throw error;
      throw new VoiceReviewUnavailableError({});
    } finally {
      clearTimeout(timeout);
    }
  };
  const get = async (principal: EnvironmentSessionPrincipalShape, id: string, mutation = false) => {
    const trusted = authorize(principal, mutation);
    const draft = decodeVoiceReviewDraft(
      await request(trusted, `/v1/prompt-review/drafts/${encodeURIComponent(id)}`),
    );
    if (draft.id !== id || draft.source_id !== trusted.source_id)
      throw new VoiceReviewNotFoundError({});
    return draft;
  };
  const checkedLimit = (limit: number) => {
    if (!Number.isSafeInteger(limit) || limit < 1 || limit > 200)
      throw new VoiceReviewConflictError({});
    return limit;
  };
  const metadataMutation = async (
    principal: EnvironmentSessionPrincipalShape,
    path: string,
    payload: typeof ThreadRegistryAssociationPayload.Type | typeof ThreadRegistryLabelPayload.Type,
  ) => {
    const trusted = authorize(principal, true);
    const result = Schema.decodeUnknownSync(ThreadRegistryMutationReceipt)(
      await request(trusted, `${path}?source_id=${encodeURIComponent(trusted.source_id)}`, payload),
    );
    if (
      result.request_id !== payload.request_id ||
      result.revision <= payload.expected_revision ||
      result.record.origin !== "owner" ||
      result.record.state !== payload.state ||
      ("subject" in payload
        ? !("subject" in result.record) ||
          result.record.subject !== payload.subject ||
          result.record.workstream_ref !== payload.workstream_ref
        : !("label_id" in result.record) || result.record.label_id !== payload.label_id)
    )
      throw new VoiceReviewUnavailableError({});
    return result;
  };
  return {
    recent: async (principal: EnvironmentSessionPrincipalShape, limit = 50) => {
      const trusted = authorize(principal, false);
      const result = Schema.decodeUnknownSync(VoiceReviewRecentList)(
        await request(
          trusted,
          `/v1/prompt-review/recent?source_id=${encodeURIComponent(trusted.source_id)}&limit=${checkedLimit(limit)}`,
        ),
      );
      return {
        ...result,
        entries: result.entries
          .filter((entry) => entry.draft.source_id === trusted.source_id)
          .map((entry) => {
            const terminal = entry.draft.state === "deleted" || entry.draft.state === "expired";
            const unavailable = terminal || entry.text_state !== "available";
            return unavailable
              ? {
                  ...entry,
                  text: null,
                  original_source_text: null,
                  text_origin: null,
                  text_state: terminal
                    ? (entry.draft.state as "deleted" | "expired")
                    : entry.text_state,
                  draft: { ...entry.draft, text: null },
                }
              : entry;
          }),
      };
    },
    registrySnapshot: async (
      principal: EnvironmentSessionPrincipalShape,
      cursor?: string,
      limit = 50,
    ) => {
      const trusted = authorize(principal, false);
      const query = new URLSearchParams({
        limit: String(checkedLimit(limit)),
        source_id: trusted.source_id,
      });
      if (cursor !== undefined) query.set("cursor", cursor);
      const result = Schema.decodeUnknownSync(ThreadRegistrySnapshot)(
        await request(trusted, `/v1/registry/snapshot?${query}`),
      );
      const memberships = new Map<
        string,
        (typeof ThreadRegistryComposedSnapshot.Type)["threads"][number]["native_memberships"]
      >();
      const unavailable = new Set(result.unavailable);
      try {
        if (native === undefined) throw new Error("native_read_unavailable");
        const identities = native.identities(result.threads);
        if (result.threads.some((thread) => !identities.has(thread.thread_key)))
          unavailable.add("native_memberships");
        const unique = new Map(
          result.threads.flatMap((thread) => {
            const identity = identities.get(thread.thread_key);
            return identity === undefined ? [] : [[JSON.stringify(identity), identity] as const];
          }),
        );
        if (unique.size > 0) {
          const placements = validateVoiceReviewNativePlacementResult(
            await native.read(principal, Array.from(unique.values())),
            now(),
          );
          if (placements.readiness !== "ready") throw new Error("native_trust_unavailable");
          for (const thread of result.threads) {
            const identity = identities.get(thread.thread_key);
            if (identity === undefined) continue;
            memberships.set(
              thread.thread_key,
              placements.page.items
                .filter(
                  (item) =>
                    item.source_instance_id === identity.source_instance_id &&
                    item.native_thread_id === identity.native_thread_id,
                )
                .map((placement) => ({
                  workstream_ref: `native:${placement.workstream_id}`,
                  placement,
                })),
            );
          }
        }
      } catch {
        memberships.clear();
        unavailable.add("native_memberships");
      }
      return Schema.decodeUnknownSync(ThreadRegistryComposedSnapshot)({
        ...result,
        partial: result.partial || unavailable.size > result.unavailable.length,
        unavailable: Array.from(unavailable),
        threads: result.threads.map((thread) => ({
          ...thread,
          native_memberships: memberships.get(thread.thread_key) ?? [],
        })),
      });
    },
    registryWorkstreams: async (principal: EnvironmentSessionPrincipalShape) => {
      const trusted = authorize(principal, false);
      return Schema.decodeUnknownSync(ThreadRegistryWorkstreams)(
        await request(
          trusted,
          `/v1/registry/workstreams?source_id=${encodeURIComponent(trusted.source_id)}`,
        ),
      );
    },
    registryEvents: async (principal: EnvironmentSessionPrincipalShape, after = 0, limit = 50) => {
      const trusted = authorize(principal, false);
      if (!Number.isSafeInteger(after) || after < 0) throw new VoiceReviewConflictError({});
      return Schema.decodeUnknownSync(ThreadRegistryEvents)(
        await request(
          trusted,
          `/v1/registry/events?source_id=${encodeURIComponent(trusted.source_id)}&after=${after}&limit=${checkedLimit(limit)}`,
        ),
      );
    },
    correctAssociation: async (
      principal: EnvironmentSessionPrincipalShape,
      payload: typeof ThreadRegistryAssociationPayload.Type,
    ) =>
      metadataMutation(
        principal,
        "/v1/registry/associations",
        Schema.decodeUnknownSync(ThreadRegistryAssociationPayload)(payload),
      ),
    correctLabel: async (
      principal: EnvironmentSessionPrincipalShape,
      payload: typeof ThreadRegistryLabelPayload.Type,
    ) =>
      metadataMutation(
        principal,
        "/v1/registry/labels",
        Schema.decodeUnknownSync(ThreadRegistryLabelPayload)(payload),
      ),
    diagnostics: async (principal: EnvironmentSessionPrincipalShape, id: string) => {
      const trusted = authorize(principal, false);
      await get(principal, id);
      const result = Schema.decodeUnknownSync(VoiceReviewDiagnostics)(
        await request(trusted, `/v1/prompt-review/drafts/${encodeURIComponent(id)}/diagnostics`),
      );
      if (result.draft_id !== id || result.source_id !== trusted.source_id)
        throw new VoiceReviewNotFoundError({});
      return result;
    },
    list: async (
      principal: EnvironmentSessionPrincipalShape,
      scope: "pending" | "recent",
      limit: number,
    ) => {
      const trusted = authorize(principal, false);
      const list = decodeVoiceReviewDraftList(
        await request(
          trusted,
          `/v1/prompt-review/drafts?scope=${scope}&limit=${checkedLimit(limit)}`,
        ),
      );
      return {
        ...list,
        drafts: list.drafts.filter((draft) => draft.source_id === trusted.source_id),
      };
    },
    get,
    mutate: async (
      principal: EnvironmentSessionPrincipalShape,
      id: string,
      action: VoiceReviewAction,
      payload: VoiceReviewMutationPayload,
    ) => {
      const trusted = authorize(principal, true);
      const validated =
        action === "edit-save"
          ? decodeVoiceReviewEditSavePayload(payload)
          : action === "edit-cancel"
            ? decodeVoiceReviewEditCancelPayload(payload)
            : decodeVoiceReviewRevisionPayload(payload);
      await get(principal, id, true);
      const result = decodeVoiceReviewMutationResult(
        await request(
          trusted,
          `/v1/prompt-review/drafts/${encodeURIComponent(id)}/${action}`,
          validated,
        ),
      );
      if (
        result.draft.id !== id ||
        result.draft.source_id !== trusted.source_id ||
        (action !== "edit-begin" && result.edit_handle !== null)
      )
        throw new VoiceReviewUnavailableError({});
      return result;
    },
  };
};

export class VoiceReviewDependencies extends Context.Service<
  VoiceReviewDependencies,
  {
    readonly config: VoiceReviewConfig | null;
    readonly fetcher: typeof fetch;
    readonly binding?: VoiceReviewNativeBinding | null;
    readonly compositionFactory?: VoiceReviewCompositionFactory;
  }
>()("t3/voiceReview/bridge/VoiceReviewDependencies") {}

export class VoiceReview extends Context.Service<
  VoiceReview,
  {
    readonly recent: (
      principal: EnvironmentSessionPrincipalShape,
      limit: number,
    ) => Effect.Effect<typeof VoiceReviewRecentList.Type, VoiceReviewError>;
    readonly registrySnapshot: (
      principal: EnvironmentSessionPrincipalShape,
      cursor: string | undefined,
      limit: number,
    ) => Effect.Effect<ThreadRegistryComposedSnapshot, VoiceReviewError>;
    readonly registryWorkstreams: (
      principal: EnvironmentSessionPrincipalShape,
    ) => Effect.Effect<ThreadRegistryWorkstreams, VoiceReviewError>;
    readonly registryEvents: (
      principal: EnvironmentSessionPrincipalShape,
      after: number,
      limit: number,
    ) => Effect.Effect<ThreadRegistryEvents, VoiceReviewError>;
    readonly correctAssociation: (
      principal: EnvironmentSessionPrincipalShape,
      payload: typeof ThreadRegistryAssociationPayload.Type,
    ) => Effect.Effect<ThreadRegistryMutationReceipt, VoiceReviewError>;
    readonly correctLabel: (
      principal: EnvironmentSessionPrincipalShape,
      payload: typeof ThreadRegistryLabelPayload.Type,
    ) => Effect.Effect<ThreadRegistryMutationReceipt, VoiceReviewError>;
    readonly diagnostics: (
      principal: EnvironmentSessionPrincipalShape,
      id: string,
    ) => Effect.Effect<typeof VoiceReviewDiagnostics.Type, VoiceReviewError>;
    readonly list: (
      principal: EnvironmentSessionPrincipalShape,
      scope: "pending" | "recent",
      limit: number,
    ) => Effect.Effect<VoiceReviewDraftList, VoiceReviewError>;
    readonly get: (
      principal: EnvironmentSessionPrincipalShape,
      id: string,
    ) => Effect.Effect<VoiceReviewDraft, VoiceReviewError>;
    readonly mutate: (
      principal: EnvironmentSessionPrincipalShape,
      id: string,
      action: VoiceReviewAction,
      payload: VoiceReviewMutationPayload,
    ) => Effect.Effect<VoiceReviewMutationResult, VoiceReviewError>;
  }
>()("t3/voiceReview/bridge/VoiceReview") {}

const isReviewError = Schema.is(VoiceReviewError);
const make = Effect.gen(function* () {
  const { config, fetcher, binding, compositionFactory } = yield* VoiceReviewDependencies;
  const clock = yield* Clock.Clock;
  const now = () => clock.currentTimeMillisUnsafe();
  const bridge = makeVoiceReviewBridge(config, fetcher, undefined, now);
  const call = <A>(run: () => Promise<A>) =>
    Effect.tryPromise({
      try: run,
      catch: (error) => (isReviewError(error) ? error : new VoiceReviewUnavailableError({})),
    });
  return VoiceReview.of({
    recent: (principal, limit) => call(() => bridge.recent(principal, limit)),
    registrySnapshot: (principal, cursor, limit) =>
      Effect.gen(function* () {
        const native =
          compositionFactory === undefined
            ? undefined
            : yield* compositionFactory({
                binding: binding ?? null,
                reviewConfig: config,
                principal,
              });
        return yield* call(() =>
          makeVoiceReviewBridge(config, fetcher, native, now).registrySnapshot(
            principal,
            cursor,
            limit,
          ),
        );
      }),
    registryWorkstreams: (principal) => call(() => bridge.registryWorkstreams(principal)),
    registryEvents: (principal, after, limit) =>
      call(() => bridge.registryEvents(principal, after, limit)),
    correctAssociation: (principal, payload) =>
      call(() => bridge.correctAssociation(principal, payload)),
    correctLabel: (principal, payload) => call(() => bridge.correctLabel(principal, payload)),
    diagnostics: (principal, id) => call(() => bridge.diagnostics(principal, id)),
    list: (principal, scope, limit) => call(() => bridge.list(principal, scope, limit)),
    get: (principal, id) => call(() => bridge.get(principal, id)),
    mutate: (principal, id, action, payload) =>
      call(() => bridge.mutate(principal, id, action, payload)),
  });
});

export const layer = Layer.effect(VoiceReview, make);
export const dependenciesLayer = Layer.sync(VoiceReviewDependencies, () => ({
  config: voiceReviewConfigFromEnv(process.env),
  fetcher: globalThis.fetch,
}));

export const dependenciesLayerLive = Layer.effect(
  VoiceReviewDependencies,
  Effect.gen(function* () {
    const compositionFactory = yield* makeVoiceReviewCompositionFactory();
    return {
      config: voiceReviewConfigFromEnv(process.env),
      fetcher: globalThis.fetch,
      binding: voiceReviewNativeBindingFromEnv(process.env),
      compositionFactory,
    };
  }),
);
