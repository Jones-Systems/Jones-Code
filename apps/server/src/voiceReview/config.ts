import type { EnvironmentSessionPrincipalShape } from "@t3tools/contracts";
import * as Schema from "effect/Schema";
// @effect-diagnostics-next-line nodeBuiltinImport:off - synchronous configuration parsing requires native absolute-path validation before credential access.
import * as NodePath from "node:path";

export interface VoiceReviewConfig {
  readonly broker_url: string;
  readonly reviewer_token_file: string;
  readonly source_id: string;
  readonly allowed_session_ids: ReadonlySet<string>;
  readonly allowed_dpop_identities?: readonly {
    readonly subject: string;
    readonly proof_key_thumbprint: string;
  }[];
}
const EnrollmentName = Schema.String.check(
  Schema.isNonEmpty(),
  Schema.isMaxLength(256),
  Schema.isPattern(/^[^\s*\u0000-\u001f\u007f]+$/),
);
const SessionIds = Schema.Array(
  Schema.String.check(Schema.isNonEmpty(), Schema.isMaxLength(256)),
).check(Schema.isMaxLength(100));
const DpopIdentity = Schema.Record(Schema.String, Schema.Unknown)
  .check(Schema.isPropertyNames(Schema.Literals(["subject", "proof_key_thumbprint"])))
  .pipe(
    Schema.decodeTo(
      Schema.Struct({
        subject: EnrollmentName,
        proof_key_thumbprint: EnrollmentName,
      }),
    ),
  );
const decodeDpopIdentities = Schema.decodeUnknownSync(
  Schema.Array(DpopIdentity).check(Schema.isMaxLength(100)),
);

const decodeSessionIds = Schema.decodeUnknownSync(SessionIds);

export const voiceReviewConfigFromEnv = (env: NodeJS.ProcessEnv): VoiceReviewConfig | null => {
  try {
    const rawUrl = env.T3CODE_VOICE_REVIEW_BROKER_URL;
    const tokenFile = env.T3CODE_VOICE_REVIEW_REVIEWER_TOKEN_FILE;
    const sourceId = env.T3CODE_VOICE_REVIEW_SOURCE_ID;
    const sessions = env.T3CODE_VOICE_REVIEW_ALLOWED_SESSION_IDS;
    const identities = env.T3CODE_VOICE_REVIEW_ALLOWED_DPOP_IDENTITIES;
    if (!rawUrl || !tokenFile || !sourceId || !NodePath.isAbsolute(tokenFile)) return null;
    if (sourceId.trim() !== sourceId || sourceId.includes("*") || sourceId.length > 256)
      return null;
    const url = new URL(rawUrl);
    if (url.username || url.password || url.search || url.hash || url.pathname !== "/") return null;
    if (
      url.protocol !== "https:" &&
      !(url.protocol === "http:" && ["127.0.0.1", "[::1]"].includes(url.hostname))
    )
      return null;
    const ids = sessions === undefined ? [] : decodeSessionIds(JSON.parse(sessions));
    const dpop = identities === undefined ? [] : decodeDpopIdentities(JSON.parse(identities));
    if (ids.some((id) => id.trim() !== id || id.includes("*"))) return null;
    if (ids.length + dpop.length === 0 || new Set(ids).size !== ids.length) return null;
    const identityKeys = dpop.map((identity) =>
      JSON.stringify([identity.subject, identity.proof_key_thumbprint]),
    );
    if (new Set(identityKeys).size !== dpop.length) return null;
    return {
      broker_url: url.origin,
      reviewer_token_file: tokenFile,
      source_id: sourceId,
      allowed_session_ids: new Set(ids),
      allowed_dpop_identities: dpop,
    };
  } catch {
    return null;
  }
};

export const isVoiceReviewPrincipalEnrolled = (
  config: VoiceReviewConfig,
  principal: EnvironmentSessionPrincipalShape,
): boolean =>
  config.allowed_session_ids.has(principal.sessionId) ||
  (principal.method === "dpop-access-token" &&
    principal.proofKeyThumbprint !== undefined &&
    (config.allowed_dpop_identities?.some(
      (identity) =>
        identity.subject === principal.subject &&
        identity.proof_key_thumbprint === principal.proofKeyThumbprint,
    ) ??
      false));

const NativeBindingName = Schema.String.check(
  Schema.isNonEmpty(),
  Schema.isMaxLength(256),
  Schema.isPattern(/^[^\s*\u0000-\u001f\u007f]+$/),
);
export const VoiceReviewNativeBinding = Schema.Record(Schema.String, Schema.Unknown)
  .check(
    Schema.isPropertyNames(
      Schema.Literals(["registry_host", "registry_environment", "native_environment_id"]),
    ),
  )
  .pipe(
    Schema.decodeTo(
      Schema.Struct({
        registry_host: NativeBindingName,
        registry_environment: NativeBindingName,
        native_environment_id: Schema.String.check(
          Schema.isPattern(/^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$/),
        ),
      }),
    ),
  );
export type VoiceReviewNativeBinding = typeof VoiceReviewNativeBinding.Type;

export const voiceReviewNativeBindingFromEnv = (
  env: NodeJS.ProcessEnv,
): VoiceReviewNativeBinding | null => {
  try {
    const raw = env.T3CODE_VOICE_REVIEW_NATIVE_BINDING;
    if (raw === undefined || raw.length > 4096) return null;
    return Schema.decodeUnknownSync(VoiceReviewNativeBinding)(JSON.parse(raw));
  } catch {
    return null;
  }
};
