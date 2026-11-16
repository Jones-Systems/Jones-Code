// @effect-diagnostics nodeBuiltinImport:off - Pure identity hashing pins the same ID on both environments.
import * as NodeCrypto from "node:crypto";
import type { LinkedThreadKind, ThreadEndpointRef } from "@t3tools/contracts/jones/linkedThreads";

export const sameEndpoint = (a: ThreadEndpointRef, b: ThreadEndpointRef) =>
  a.environmentId === b.environmentId && a.threadId === b.threadId;
const compare = (a: ThreadEndpointRef, b: ThreadEndpointRef) =>
  a.environmentId < b.environmentId
    ? -1
    : a.environmentId > b.environmentId
      ? 1
      : a.threadId < b.threadId
        ? -1
        : a.threadId > b.threadId
          ? 1
          : 0;
export function linkedThreadIdentity(
  kind: LinkedThreadKind,
  from: ThreadEndpointRef,
  to: ThreadEndpointRef,
) {
  const [a, b] = kind === "peer" && compare(from, to) > 0 ? [to, from] : [from, to];
  const linkId =
    "tl1:" +
    NodeCrypto.createHash("sha256")
      .update(
        JSON.stringify([
          "t3.thread-link/v1",
          kind,
          a.environmentId,
          a.threadId,
          b.environmentId,
          b.threadId,
        ]),
        "utf8",
      )
      .digest("hex");
  return { linkId, from: a, to: b };
}

export function handoffRequestKey(key: string, message = false): string {
  const suffix = message ? ":message" : "";
  const plain = `handoff:${key}${suffix}`;
  return plain.length <= 256
    ? plain
    : `handoff:${NodeCrypto.createHash("sha256").update(key, "utf8").digest("hex")}${suffix}`;
}
