import {
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
// @effect-diagnostics-next-line nodeBuiltinImport:off - Effect FileSystem OpenFlag cannot express numeric O_NOFOLLOW | O_NONBLOCK credential guards.
import * as NodeFS from "node:fs";
// @effect-diagnostics-next-line nodeBuiltinImport:off - native descriptors preserve guarded open, fstat, bounded read, and finally close on the same credential file.
import * as NodeFSP from "node:fs/promises";
import { voiceReviewConfigFromEnv, type VoiceReviewConfig } from "./config.ts";

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

export const makeVoiceReviewBridge = (
  config: VoiceReviewConfig | null,
  fetcher: typeof fetch = globalThis.fetch,
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
  const request = async (
    trusted: VoiceReviewConfig,
    path: string,
    payload?: VoiceReviewMutationPayload,
  ) => {
    const controller = new AbortController();
    // @effect-diagnostics-next-line globalTimers:off - native fetch deadline; cleared on every completion.
    const timeout = setTimeout(() => controller.abort(), REQUEST_TIMEOUT_MS);
    try {
      const token = await readToken(trusted.reviewer_token_file);
      const body = payload === undefined ? undefined : JSON.stringify(payload);
      if (body !== undefined && Buffer.byteLength(body) > VOICE_REVIEW_MAX_REQUEST_BYTES) {
        throw new VoiceReviewConflictError({});
      }
      const response = await fetcher(`${trusted.broker_url}/v1/prompt-review/drafts${path}`, {
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
    const draft = decodeVoiceReviewDraft(await request(trusted, `/${encodeURIComponent(id)}`));
    if (draft.id !== id || draft.source_id !== trusted.source_id)
      throw new VoiceReviewNotFoundError({});
    return draft;
  };
  return {
    list: async (
      principal: EnvironmentSessionPrincipalShape,
      scope: "pending" | "recent",
      limit: number,
    ) => {
      const trusted = authorize(principal, false);
      const list = decodeVoiceReviewDraftList(
        await request(trusted, `?scope=${scope}&limit=${limit}`),
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
        await request(trusted, `/${encodeURIComponent(id)}/${action}`, validated),
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
  { readonly config: VoiceReviewConfig | null; readonly fetcher: typeof fetch }
>()("t3/voiceReview/bridge/VoiceReviewDependencies") {}

export class VoiceReview extends Context.Service<
  VoiceReview,
  {
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
  const { config, fetcher } = yield* VoiceReviewDependencies;
  const bridge = makeVoiceReviewBridge(config, fetcher);
  const call = <A>(run: () => Promise<A>) =>
    Effect.tryPromise({
      try: run,
      catch: (error) => (isReviewError(error) ? error : new VoiceReviewUnavailableError({})),
    });
  return VoiceReview.of({
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
