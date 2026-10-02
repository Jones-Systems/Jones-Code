import * as Schema from "effect/Schema";
// @effect-diagnostics-next-line nodeBuiltinImport:off - synchronous configuration parsing requires native absolute-path validation before credential access.
import * as NodePath from "node:path";

export interface VoiceReviewConfig {
  readonly broker_url: string;
  readonly reviewer_token_file: string;
  readonly source_id: string;
  readonly allowed_session_ids: ReadonlySet<string>;
}
const SessionIds = Schema.Array(
  Schema.String.check(Schema.isNonEmpty(), Schema.isMaxLength(256)),
).check(Schema.isMinLength(1), Schema.isMaxLength(100));

const decodeSessionIds = Schema.decodeUnknownSync(SessionIds);

export const voiceReviewConfigFromEnv = (env: NodeJS.ProcessEnv): VoiceReviewConfig | null => {
  try {
    const rawUrl = env.T3CODE_VOICE_REVIEW_BROKER_URL;
    const tokenFile = env.T3CODE_VOICE_REVIEW_REVIEWER_TOKEN_FILE;
    const sourceId = env.T3CODE_VOICE_REVIEW_SOURCE_ID;
    const sessions = env.T3CODE_VOICE_REVIEW_ALLOWED_SESSION_IDS;
    if (!rawUrl || !tokenFile || !sourceId || !sessions || !NodePath.isAbsolute(tokenFile))
      return null;
    if (sourceId.trim() !== sourceId || sourceId.includes("*") || sourceId.length > 256)
      return null;
    const url = new URL(rawUrl);
    if (url.username || url.password || url.search || url.hash || url.pathname !== "/") return null;
    if (
      url.protocol !== "https:" &&
      !(url.protocol === "http:" && ["127.0.0.1", "[::1]"].includes(url.hostname))
    )
      return null;
    const ids = decodeSessionIds(JSON.parse(sessions));
    if (ids.some((id) => id.trim() !== id || id.includes("*"))) return null;
    return {
      broker_url: url.origin,
      reviewer_token_file: tokenFile,
      source_id: sourceId,
      allowed_session_ids: new Set(ids),
    };
  } catch {
    return null;
  }
};
