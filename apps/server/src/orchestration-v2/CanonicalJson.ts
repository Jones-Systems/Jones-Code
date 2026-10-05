import * as NodeCrypto from "node:crypto";

export function canonicalJson(value: unknown): string {
  return JSON.stringify(value, (_key, child: unknown) => {
    if (child !== null && typeof child === "object" && !Array.isArray(child)) {
      return Object.fromEntries(
        Object.entries(child).sort(([left], [right]) => (left < right ? -1 : left > right ? 1 : 0)),
      );
    }
    return child;
  });
}

export const sha256 = (value: string | Uint8Array): string =>
  NodeCrypto.createHash("sha256").update(value).digest("hex");
