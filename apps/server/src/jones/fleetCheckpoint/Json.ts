// Native synchronous hashing binds the frozen cross-language wire identity before any effect.
// @effect-diagnostics nodeBuiltinImport:off
import * as NodeCrypto from "node:crypto";

export type Document = { readonly [key: string]: unknown };

export const document = (value: unknown): Document => {
  if (
    value === null ||
    typeof value !== "object" ||
    Array.isArray(value) ||
    (Object.getPrototypeOf(value) !== Object.prototype && Object.getPrototypeOf(value) !== null)
  )
    throw new TypeError("Expected a portable document.");
  return value as Document;
};

export const exact = (value: unknown, keys: readonly string[]): Document => {
  const result = document(value);
  if (Object.keys(result).sort().join("\0") !== [...keys].sort().join("\0"))
    throw new TypeError("Unexpected document fields.");
  return result;
};

/** Matches the frozen v1 ASCII/integer canonicalization; executable command order is never sorted. */
export const canonical = (value: unknown, depth = 0): string => {
  if (depth > 24) throw new TypeError("Document depth exceeded.");
  if (value === null || typeof value === "boolean") return JSON.stringify(value);
  if (typeof value === "number" && Number.isSafeInteger(value)) return JSON.stringify(value);
  if (typeof value === "string" && /^[\x20-\x7e]*$/.test(value)) return JSON.stringify(value);
  if (Array.isArray(value) && value.length <= 10000)
    return `[${value.map((v) => canonical(v, depth + 1)).join(",")}]`;
  const object = document(value);
  const keys = Object.keys(object).sort();
  if (keys.length > 256) throw new TypeError("Document width exceeded.");
  return `{${keys.map((key) => `${canonical(key, depth + 1)}:${canonical(object[key], depth + 1)}`).join(",")}}`;
};

export const digest = (value: unknown) =>
  NodeCrypto.createHash("sha256").update(canonical(value)).digest("hex");

export const decode = (raw: string): Document => {
  if (Buffer.byteLength(raw) > 256 * 1024) throw new TypeError("Document size exceeded.");
  const value: unknown = JSON.parse(raw);
  // The fixed controller CLI emits canonical bytes. This also rejects duplicate object keys.
  if (raw.trim() !== canonical(value)) throw new TypeError("Noncanonical controller response.");
  return document(value);
};
