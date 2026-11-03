import { expect, it } from "vite-plus/test";
import * as Schema from "effect/Schema";
import { WorkstreamsNativeReadExportQuery } from "./workstreamsNativeExport.ts";

const decode = Schema.decodeUnknownSync(WorkstreamsNativeReadExportQuery);

it("decodes an absent or bounded export page size without accepting caller or home authority", () => {
  expect(decode({})).toEqual({});
  expect(decode({ page_size: "1" })).toEqual({ page_size: 1 });
  expect(decode({ page_size: "100" })).toEqual({ page_size: 100 });
  for (const page_size of ["0", "101", "-1", "1.5", "NaN", "Infinity", "", "abc", ["1", "2"]])
    expect(() => decode({ page_size })).toThrow();
  for (const input of [
    { caller: { principal_id: "forged" } },
    { home_environment_id: "forged" },
    { page_size: "1", extra: "forged" },
  ])
    expect(() => decode(input)).toThrow();
});
