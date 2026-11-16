import { CONTINUITY_RELOAD_CUE } from "./capsule.ts";
import { CONTINUITY_LIMITS, utf8Bytes } from "./model.ts";

/** Keep the JSON atomic while fitting Codex's per-value context transport. */
export function codexContinuityContext(
  text: string,
): Record<string, { readonly kind: "untrusted"; readonly value: string }> | null {
  const bytes = utf8Bytes(text);
  if (bytes > CONTINUITY_LIMITS.capsuleBytes || !text.endsWith(CONTINUITY_RELOAD_CUE)) return null;
  if (bytes <= 4000) return { jones_goal_continuity: { kind: "untrusted", value: text } };
  const body = text.slice(0, -CONTINUITY_RELOAD_CUE.length);
  if (utf8Bytes(body) > 4000 || utf8Bytes(CONTINUITY_RELOAD_CUE) > 4000) return null;
  return {
    jones_goal_continuity: { kind: "untrusted", value: body },
    jones_goal_continuity_reload: { kind: "untrusted", value: CONTINUITY_RELOAD_CUE },
  };
}
