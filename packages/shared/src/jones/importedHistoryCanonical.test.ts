import { expect, it } from "vite-plus/test";
import * as DateTime from "effect/DateTime";
import * as Schema from "effect/Schema";
import { importedHistoryCanonicalJson } from "./importedHistoryCanonical.ts";
it("preserves historical sorted-object JSON bytes, arrays and unicode", () => {
  expect(importedHistoryCanonicalJson({ z: [{ b: "π 😀", a: 1 }, 2], a: null })).toBe(
    '{"a":null,"z":[{"a":1,"b":"π 😀"},2]}',
  );
  expect(importedHistoryCanonicalJson({ z: undefined, a: [undefined, null] })).toBe(
    '{"a":[null,null]}',
  );
});
it("canonicalizes schema-encoded DateTime rather than DateTime internals", () => {
  const encoded = Schema.encodeSync(
    Schema.Struct({ at: Schema.DateTimeUtc, label: Schema.String }),
  )({ at: DateTime.makeUnsafe("2026-10-07T12:34:56.000Z"), label: "synthetic" });
  expect(importedHistoryCanonicalJson(encoded)).toBe(
    '{"at":"2026-10-07T12:34:56.000Z","label":"synthetic"}',
  );
});
