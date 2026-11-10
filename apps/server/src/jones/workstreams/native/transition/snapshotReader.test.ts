// @effect-diagnostics nodeBuiltinImport:off -- Ed25519 permit and AES-GCM conformance require Node crypto primitives.
import * as NodeCrypto from "node:crypto";
import { expect, it } from "@effect/vitest";
import vectors from "./fixtures/v1/vectors.json" with { type: "json" };
import { canonical, digest, validateLine } from "./canonical.ts";
import {
  parseSnapshot,
  encodeSnapshot,
  decryptSnapshot,
  encryptSnapshot,
  verifyPermit,
} from "./snapshotReader.ts";
const key = Buffer.alloc(32, 7);
it("matches every pinned canonical and populated/empty byte vector", () => {
  for (const v of vectors.canonical) expect(canonical(v.value)).toBe(v.canonical);
  for (const v of [vectors.empty_snapshot, vectors.full_snapshot]) {
    const s = parseSnapshot(v.canonical_jsonl);
    expect(s.snapshot_sha256).toBe(v.snapshot_sha256);
    expect(encodeSnapshot(s.header, s.rows)).toBe(v.canonical_jsonl);
    expect(
      decryptSnapshot(
        encryptSnapshot(v.canonical_jsonl, "synthetic-transition-key", key),
        "synthetic-transition-key",
        key,
      ),
    ).toBe(v.canonical_jsonl);
  }
});
it("matches the independent AES-GCM vector", () => {
  const v = vectors.encryption,
    cipher = NodeCrypto.createCipheriv(
      "aes-256-gcm",
      Buffer.from(v.key_hex, "hex"),
      Buffer.from(v.nonce_hex, "hex"),
    );
  cipher.setAAD(Buffer.from(v.aad));
  expect(Buffer.concat([cipher.update(v.plaintext), cipher.final()]).toString("hex")).toBe(
    v.ciphertext_hex,
  );
  expect(cipher.getAuthTag().toString("hex")).toBe(v.tag_hex);
});
it("checks signatures and rejects a substituted header", () => {
  const { privateKey, publicKey } = NodeCrypto.generateKeyPairSync("ed25519"),
    h = parseSnapshot(vectors.empty_snapshot.canonical_jsonl).header;
  const permit = {
    key_id: "operator",
    header_sha256: digest(canonical(h)),
    signature: NodeCrypto.sign(
      null,
      Buffer.from(canonical(["workstreams-snapshot-export-permit-v1", h])),
      privateKey,
    ).toString("base64"),
  };
  expect(() => verifyPermit(h, permit, publicKey)).not.toThrow();
  expect(() => verifyPermit({ ...h, open_effects: 1 }, permit, publicKey)).toThrow();
});
it("fails closed on wrong keys, tamper, truncation, reordered or trailing frames", () => {
  const encrypted = encryptSnapshot(vectors.full_snapshot.canonical_jsonl, "key", key),
    frames = encrypted.trimEnd().split("\n");
  for (const bytes of [
    frames.slice(0, -1).join("\n") + "\n",
    encrypted + frames[0] + "\n",
    frames.toReversed().join("\n") + "\n",
    encrypted.replace('"ct":"', '"ct":"AAAA'),
  ])
    expect(() => decryptSnapshot(bytes, "key", key)).toThrow();
  expect(() => decryptSnapshot(encrypted, "key", Buffer.alloc(32, 9))).toThrow();
});
it("rejects unsupported versions, incomplete streams and duplicate memberships before apply", () => {
  const s = parseSnapshot(vectors.full_snapshot.canonical_jsonl);
  expect(() =>
    parseSnapshot(
      encodeSnapshot(
        { ...s.header, producer: { ...s.header.producer, schema_version: 999 } },
        s.rows,
      ),
    ),
  ).toThrow();
  expect(() => parseSnapshot(vectors.full_snapshot.canonical_jsonl.slice(0, -1))).toThrow();
  const active = s.rows.find(
    (r) => r.class === "workstream_memberships" && r.row.closed_at === null,
  )!;
  expect(() =>
    parseSnapshot(
      encodeSnapshot(s.header, [
        ...s.rows,
        {
          ...active,
          key: [active.row.owner_id as string, "membership:duplicate"],
          row: { ...active.row, membership_id: "membership:duplicate" },
        },
      ]),
    ),
  ).toThrow();
});

it("closed schemas reject inherited object property names", () => {
  const header = parseSnapshot(vectors.empty_snapshot.canonical_jsonl).header;
  const extra = JSON.parse(canonical(header).replace("{", '{"__proto__":{},'));
  expect(() => validateLine(extra)).toThrow();
});
