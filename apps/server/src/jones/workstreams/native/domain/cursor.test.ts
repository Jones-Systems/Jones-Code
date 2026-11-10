import { describe, expect, it } from "vite-plus/test";
import { RegistryCursorCodec } from "./cursor.ts";
import type { AuthorizedOwner } from "./state.ts";
const authorization: AuthorizedOwner = {
  owner_id: "owner-one",
  actor: { principal_id: "principal-one" },
  server_generation: 1,
  grant: {
    owner_id: "owner-one",
    principal_id: "principal-one",
    grant_id: "grant-one",
    grant_version: 1,
    state: "active",
    permissions: ["workstreams:read"],
  },
};
const context = { owner_id: "owner-one", server_generation: 1, registry_version: 7 };
const position = { after: [7, "event:Z"] as const, limit: 7, highWater: 11, expires: 900000 };
describe("v2 native cursor parity", () => {
  it("round trips high-water and binds owner, principal, route, generation, version, limit and lifetime", () => {
    const codec = new RegistryCursorCodec(Buffer.alloc(32, 1));
    const cursor = codec.encode(position, authorization, "/history", context);
    expect(codec.decode({ cursor, limit: 7 }, authorization, "/history", context, 0)).toEqual(
      position,
    );
    const invalid = [
      () => codec.decode({ cursor, limit: 1 }, authorization, "/history", context, 0),
      () =>
        codec.decode(
          { cursor, limit: 7 },
          { ...authorization, owner_id: "other-owner" },
          "/history",
          context,
          0,
        ),
      () =>
        codec.decode(
          { cursor, limit: 7 },
          { ...authorization, actor: { principal_id: "other-principal" } },
          "/history",
          context,
          0,
        ),
      () => codec.decode({ cursor, limit: 7 }, authorization, "/workstreams", context, 0),
      () => codec.decode({ cursor, limit: 7 }, authorization, "/history", context, 900000),
      () =>
        new RegistryCursorCodec(Buffer.alloc(32, 2)).decode(
          { cursor, limit: 7 },
          authorization,
          "/history",
          context,
          0,
        ),
    ];
    for (const attempt of invalid) expect(attempt).toThrow("cursor_invalid");
    expect(() =>
      codec.decode(
        { cursor, limit: 7 },
        authorization,
        "/history",
        { ...context, server_generation: 2 },
        0,
      ),
    ).toThrow("cursor_stale");
    expect(() =>
      codec.decode(
        { cursor, limit: 7 },
        authorization,
        "/history",
        { ...context, registry_version: 8 },
        0,
      ),
    ).toThrow("cursor_stale");
  });
  it("rejects malformed and noncanonical encodings and oversized payloads", () => {
    const codec = new RegistryCursorCodec(Buffer.alloc(32));
    for (const cursor of ["!", "a", "A".repeat(513), "AAAA="])
      expect(() => codec.decode({ cursor }, authorization, "/history", context, 0)).toThrow(
        "cursor_invalid",
      );
    expect(() => new RegistryCursorCodec(Buffer.alloc(31))).toThrow(
      "invalid_workstream_cursor_key",
    );
    expect(() => codec.decode({ limit: 101 }, authorization, "/history", context, 0)).toThrow(
      "invalid_request",
    );
    expect(() =>
      codec.encode({ ...position, after: ["x".repeat(512)] }, authorization, "/history", context),
    ).toThrow("backpressure");
  });
});
