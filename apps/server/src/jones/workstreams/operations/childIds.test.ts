import * as NodeCrypto from "@effect/platform-node/NodeCrypto";
import * as Effect from "effect/Effect";
import { describe, expect, it } from "vite-plus/test";
import { workstreamChildId, type WorkstreamChildIdInput } from "./childIds.ts";

const input: WorkstreamChildIdInput = {
  environment_id: "env:a",
  operation_id: "operation-00000001",
  request_sha256: "a".repeat(64),
  kind: "mem",
  attempt: 1,
};

const runChildId = (input: WorkstreamChildIdInput) =>
  Effect.runPromise(workstreamChildId(input).pipe(Effect.provide(NodeCrypto.layer)));

describe("workstreamChildId", () => {
  it("matches independent SHA-256 golden vectors, including NUL separators", async () => {
    expect(await runChildId(input)).toBe("jwo-18fb4ee632ef4528a383c5bc78080045ba5b25fb-mem-1");
    expect(
      await runChildId({
        environment_id: "env-b",
        operation_id: "Operation:000002",
        request_sha256: "0123456789abcdef".repeat(4),
        kind: "cre",
        attempt: 9,
      }),
    ).toBe("jwo-1f7c1fab62fe11660004ae05e74b3e10eb8a86b9-cre-9");
  });

  it("gives all 36 child slots distinct stable command IDs", async () => {
    const ids = new Set<string>();
    for (const kind of ["reg", "ver", "mem", "cre"] as const) {
      for (let attempt = 1; attempt <= 9; attempt++) {
        const child = { ...input, kind, attempt };
        const id = await runChildId(child);
        expect(await runChildId(child)).toBe(id);
        expect(id).toMatch(/^[A-Za-z0-9][A-Za-z0-9._:-]{15,127}$/);
        ids.add(id);
      }
    }
    expect(ids.size).toBe(36);
  });

  it("separates environment, operation and exact request digest", async () => {
    const variants = [
      input,
      { ...input, environment_id: "env:b" },
      { ...input, operation_id: "operation-00000002" },
      { ...input, request_sha256: "b".repeat(64) },
    ];
    expect(new Set(await Promise.all(variants.map(runChildId))).size).toBe(4);
  });

  it("rejects invalid IDs, digest spelling, child kinds and out-of-range attempts", async () => {
    const invalid: WorkstreamChildIdInput[] = [
      ...["", "-env", "env\0other", "env\n", "a".repeat(129)].map((environment_id) => ({
        ...input,
        environment_id,
      })),
      ...["short", "operation-00000001\n", "a".repeat(129)].map((operation_id) => ({
        ...input,
        operation_id,
      })),
      ...["a".repeat(63), "A".repeat(64), "g".repeat(64), `${input.request_sha256}\n`].map(
        (request_sha256) => ({ ...input, request_sha256 }),
      ),
      ...[0, 10, -1, 1.5, NaN, Infinity].map((attempt) => ({ ...input, attempt })),
      { ...input, kind: "other" as WorkstreamChildIdInput["kind"] },
      { ...input, environment_id: 123 as unknown as string },
    ];
    for (const candidate of invalid) await expect(runChildId(candidate)).rejects.toThrow("Invalid");
  });
});
