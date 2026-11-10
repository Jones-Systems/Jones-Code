// @effect-diagnostics-next-line nodeBuiltinImport:off -- Synchronous digests preserve the frozen cursor and exact-reservation wire bytes without I/O.
import * as NodeCrypto from "node:crypto";
import type { AuthorizedOwner, PageQuery, ReadContext } from "./state.ts";
import { requireDomain, WorkstreamDomainError } from "./errors.ts";

export interface CursorPosition {
  readonly after: readonly (number | string)[] | null;
  readonly limit: number;
  readonly highWater: number | null;
  readonly expires: number;
}

export class RegistryCursorCodec {
  private readonly key: Buffer;
  constructor(key: Uint8Array) {
    if (key.byteLength < 32) throw new Error("invalid_workstream_cursor_key");
    this.key = Buffer.from(key);
  }

  private scope(authorization: AuthorizedOwner, route: string): string {
    return NodeCrypto.createHash("sha256")
      .update(JSON.stringify([authorization.owner_id, authorization.actor.principal_id, route]))
      .digest("base64url");
  }

  decode(
    query: PageQuery,
    authorization: AuthorizedOwner,
    route: string,
    context: ReadContext,
    now: number,
  ): CursorPosition {
    const limit = query.limit ?? 50;
    requireDomain(Number.isSafeInteger(limit) && limit >= 1 && limit <= 100, "invalid_request");
    if (!query.cursor) return { after: null, limit, highWater: null, expires: now + 900_000 };
    try {
      requireDomain(/^[A-Za-z0-9_-]{1,512}$/.test(query.cursor), "cursor_invalid");
      const bytes = Buffer.from(query.cursor, "base64url");
      requireDomain(
        bytes.toString("base64url") === query.cursor && bytes.length > 32,
        "cursor_invalid",
      );
      const payload = bytes.subarray(0, -32);
      const signature = bytes.subarray(-32);
      requireDomain(
        NodeCrypto.timingSafeEqual(
          signature,
          NodeCrypto.createHmac("sha256", this.key).update(payload).digest(),
        ),
        "cursor_invalid",
      );
      const fields: unknown = JSON.parse(payload.toString("utf8"));
      requireDomain(Array.isArray(fields) && fields.length === 8, "cursor_invalid");
      const [version, scope, generation, registryVersion, savedLimit, after, highWater, expires] =
        fields;
      requireDomain(
        version === 2 &&
          scope === this.scope(authorization, route) &&
          savedLimit === limit &&
          (after === null ||
            (Array.isArray(after) &&
              after.length >= 1 &&
              after.length <= 3 &&
              after.every(
                (value) =>
                  (typeof value === "string" && value.length >= 1 && value.length <= 512) ||
                  (Number.isSafeInteger(value) && (value as number) >= 0),
              ))) &&
          Number.isSafeInteger(expires) &&
          expires > now &&
          (highWater === null || (Number.isSafeInteger(highWater) && highWater >= 0)),
        "cursor_invalid",
      );
      requireDomain(
        generation === context.server_generation && registryVersion === context.registry_version,
        "cursor_stale",
      );
      return { after, limit, highWater, expires };
    } catch (error) {
      if (error instanceof WorkstreamDomainError) throw error;
      throw new WorkstreamDomainError("cursor_invalid");
    }
  }

  encode(
    position: CursorPosition,
    authorization: AuthorizedOwner,
    route: string,
    context: ReadContext,
  ): string {
    const payload = Buffer.from(
      JSON.stringify([
        2,
        this.scope(authorization, route),
        context.server_generation,
        context.registry_version,
        position.limit,
        position.after,
        position.highWater,
        position.expires,
      ]),
    );
    const result = Buffer.concat([
      payload,
      NodeCrypto.createHmac("sha256", this.key).update(payload).digest(),
    ]).toString("base64url");
    requireDomain(result.length <= 512, "backpressure");
    return result;
  }
}
