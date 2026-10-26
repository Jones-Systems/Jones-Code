// @effect-diagnostics nodeBuiltinImport:off -- Snapshot byte ordering and SHA-256 are protocol primitives.
import * as NodeCrypto from "node:crypto";
import { canonicalWorkstreamsSnapshotJson } from "@t3tools/contracts/jones/workstreamsSnapshot";
import * as Schema from "effect/Schema";
import manifest from "./fixtures/v1/manifest.json" with { type: "json" };
import lineSchema from "./fixtures/v1/snapshot.schema.json" with { type: "json" };
export const canonical = canonicalWorkstreamsSnapshotJson;
export const digest = (bytes: string | Uint8Array): string =>
  NodeCrypto.createHash("sha256").update(bytes).digest("hex");
export const classes: readonly string[] = manifest.class_order;
export const tableKeys: Readonly<Record<string, readonly string[]>> = manifest.table_keys;
export const extensions = [
  "jones.principal_environments",
  "jones.legacy_archive",
  "jones.import_runs",
  "jones.rollup_bindings",
] as const;
export const extensionVersion = "jones-workstreams-transfer/1.0.0";
export class TransitionError extends Schema.TaggedError<TransitionError>()(
  "WorkstreamsTransitionError",
  {
    reason: Schema.String,
  },
) {
  override get message() {
    return "Workstreams transition refused.";
  }
}
export function refuse(reason: string): never {
  throw new TransitionError({ reason });
}
export function compareKeys(a: readonly unknown[], b: readonly unknown[]): number {
  for (let i = 0; i < a.length; i++) {
    const x = a[i],
      y = b[i];
    if (typeof x !== typeof y) return refuse("key_type");
    const cmp =
      typeof x === "string" && typeof y === "string"
        ? Buffer.compare(Buffer.from(x), Buffer.from(y))
        : typeof x === "number" && typeof y === "number"
          ? x - y
          : typeof x === "boolean" && typeof y === "boolean"
            ? Number(x) - Number(y)
            : refuse("key_type");
    if (cmp) return cmp;
  }
  return a.length - b.length;
}
type Spec = {
  type?: string;
  const?: unknown;
  enum?: unknown[];
  oneOf?: Spec[];
  anyOf?: Spec[];
  properties?: Record<string, Spec>;
  required?: string[];
  additionalProperties?: boolean;
  items?: Spec;
  minimum?: number;
  maximum?: number;
  minLength?: number;
  maxLength?: number;
  minItems?: number;
  maxItems?: number;
  pattern?: string;
};
// This interpreter implements every validation keyword used by the immutable X1 draft-07 schema.
// No coercion or defaults: extra columns and unqualified variants fail before any SQL write.
function matches(s: Spec, value: unknown): boolean {
  if (s.oneOf && s.oneOf.filter((x) => matches(x, value)).length !== 1) return false;
  if (s.anyOf && !s.anyOf.some((x) => matches(x, value))) return false;
  if ("const" in s && canonical(value) !== canonical(s.const)) return false;
  if (s.enum && !s.enum.some((x) => canonical(x) === canonical(value))) return false;
  if (s.type === "null") return value === null;
  if (s.type === "boolean" && typeof value !== "boolean") return false;
  if (
    s.type === "integer" &&
    (!Number.isSafeInteger(value) ||
      typeof value !== "number" ||
      value < (s.minimum ?? -Infinity) ||
      value > (s.maximum ?? Infinity))
  )
    return false;
  if (s.type === "string") {
    if (
      typeof value !== "string" ||
      [...value].length < (s.minLength ?? 0) ||
      [...value].length > (s.maxLength ?? Infinity) ||
      (s.pattern && !new RegExp(s.pattern).test(value))
    )
      return false;
  }
  if (s.type === "array") {
    if (
      !Array.isArray(value) ||
      value.length < (s.minItems ?? 0) ||
      value.length > (s.maxItems ?? Infinity) ||
      (s.items && !value.every((x) => matches(s.items!, x)))
    )
      return false;
  }
  if (s.type === "object") {
    if (!value || typeof value !== "object" || Array.isArray(value)) return false;
    const row = value as Record<string, unknown>;
    if (s.required?.some((k) => !Object.hasOwn(row, k))) return false;
    if (
      Object.entries(row).some(([k, v]) =>
        s.properties && Object.hasOwn(s.properties, k)
          ? !matches(s.properties[k]!, v)
          : s.additionalProperties === false,
      )
    )
      return false;
  }
  return true;
}
export function validateLine(value: unknown): void {
  canonical(value);
  if (!matches(lineSchema as Spec, value)) refuse("schema_invalid");
}

export function rowFields(name: string): readonly string[] {
  const visit = (s: unknown): readonly string[] | undefined => {
    if (!s || typeof s !== "object") return;
    const o = s as { properties?: { class?: { const?: string }; row?: { required?: string[] } } };
    if (o.properties?.class?.const === name) return o.properties.row?.required;
    for (const v of Object.values(s)) {
      const result = Array.isArray(v) ? v.map(visit).find(Boolean) : visit(v);
      if (result) return result;
    }
  };
  return visit(lineSchema) ?? refuse("unknown_class");
}
