import * as Schema from "effect/Schema";
import { CompanionHostUnavailable } from "./CompanionHostRegistry.ts";

const isUnavailable = Schema.is(CompanionHostUnavailable);
const encoder = new TextEncoder();
export const companionCloseReason = (cause: unknown): string | undefined => {
  if (!isUnavailable(cause)) return undefined;
  const label = cause.state === "unknown" ? "" : cause.label.trim();
  const encode = (label: string) =>
    JSON.stringify({ hostId: cause.hostId, label, state: cause.state });
  const fits = (text: string) => encoder.encode(text).byteLength <= 123;
  const full = encode(label);
  if (fits(full)) return full;
  // RFC 6455 leaves 123 UTF-8 bytes for the entire close reason, including JSON escaping.
  const codePoints = Array.from(label);
  for (let length = codePoints.length - 1; length > 0; length--) {
    const prefix = codePoints.slice(0, length).join("").trimEnd();
    if (!prefix) continue;
    const truncated = encode(`${prefix}…`);
    if (fits(truncated)) return truncated;
  }
  const empty = encode("");
  return fits(empty) ? empty : "";
};
