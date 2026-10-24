// @effect-diagnostics nodeBuiltinImport:off -- Tests qualify exact vendored contract bytes without network or live state.
import * as NodeCrypto from "node:crypto";
import * as NodeFS from "node:fs";
import { expect, it } from "vite-plus/test";
import {
  decodeRegistryBinding,
  REGISTRY_BINDING_MANIFEST_SHA256,
} from "./workstreamsRegistryBinding.ts";
import type { RegistryBinding } from "./workstreamsRegistryBinding.ts";
const vectors = JSON.parse(
  NodeFS.readFileSync(
    new URL("./workstreamsRegistryBinding/contract/vectors.json.raw", import.meta.url),
    "utf8",
  ),
) as { binding: RegistryBinding; token: string };
const manifest = JSON.parse(
  NodeFS.readFileSync(
    new URL("./workstreamsRegistryBinding/contract/manifest.json.raw", import.meta.url),
    "utf8",
  ),
) as { files: { path: string; sha256: string }[] };

const encode = (value: unknown) => Buffer.from(JSON.stringify(value)).toString("base64url");
it("qualifies the independent corpus pinned at producer 562bd5cb51de8410560b63bba9119a104be3bb34", () => {
  const digest = (name: string) =>
    NodeCrypto.createHash("sha256")
      .update(
        NodeFS.readFileSync(
          new URL(`./workstreamsRegistryBinding/contract/${name}.raw`, import.meta.url),
        ),
      )
      .digest("hex");
  expect(digest("manifest.json")).toBe(REGISTRY_BINDING_MANIFEST_SHA256);
  expect(digest("vectors.json")).toBe(
    "95b642211ea44271cdd8b9cdd15eadfa8fed9fb54cbd18abc07111e3c6880aa2",
  );
  for (const file of manifest.files) expect(digest(file.path)).toBe(file.sha256);
  expect(decodeRegistryBinding(vectors.token)).toEqual(vectors.binding);
});
const invalidBindings: ReadonlyArray<readonly [string, string | number]> = [
  ["protocol", "workstreams-registry-binding/2.0.0"],
  ["manifest_sha256", "a".repeat(64)],
  ["registry_instance_id", "11111111-1111-4111-1111-111111111111"],
  ["registry_instance_id", "AAAAAAAA-1111-4111-8111-111111111111"],
  ["principal_id", "principal"],
  ["owner_id", "bad owner"],
  ["owner_id", "x".repeat(129)],
  ["authorization_revision", 0],
  ["authorization_revision", 1.5],
  ["authorization_revision", Number.MAX_SAFE_INTEGER + 1],
  ...[
    "http://127.0.0.1",
    "https://EXAMPLE.test",
    "https://example.test/",
    "https://example.test:443",
    "https://u:p@example.test",
    "https://example.test?x",
    "https://example.test#x",
    " https://example.test",
    `https://${"a".repeat(505)}.test`,
  ].map((origin) => ["canonical_origin", origin] as const),
];
it.each(invalidBindings)("rejects invalid %s %s", (key, value) => {
  expect(() =>
    decodeRegistryBinding(encode({ ...vectors.binding, [key as string]: value })),
  ).toThrow();
});
it("rejects duplicate, reordered, missing, extra, escaped and noncanonical token encodings", () => {
  const raw = JSON.stringify(vectors.binding);
  const { owner_id: _owner, ...missing } = vectors.binding;
  const invalid = [
    "",
    vectors.token + "=",
    vectors.token + "\n",
    "a".repeat(2732),
    encode({ ...vectors.binding, extra: true }),
    encode(missing),
    encode(Object.fromEntries(Object.entries(vectors.binding).toReversed())),
    ...[
      raw.replace("{", '{"protocol":"wrong",'),
      " " + raw,
      raw.replace("owner.synthetic", "owner\\u002esynthetic"),
    ].map((value) => Buffer.from(value).toString("base64url")),
    Buffer.concat([Buffer.from(raw), Buffer.from([0xff])]).toString("base64url"),
    encode(null),
    encode([]),
    encode(42),
  ];
  for (const token of invalid) expect(() => decodeRegistryBinding(token), token).toThrow();
});
