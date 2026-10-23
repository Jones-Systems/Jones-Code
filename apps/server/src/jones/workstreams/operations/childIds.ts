import * as Crypto from "effect/Crypto";
import * as Effect from "effect/Effect";
import * as Hex from "effect/encoding/Hex";

export interface WorkstreamChildIdInput {
  readonly environment_id: string;
  readonly operation_id: string;
  readonly request_sha256: string;
  readonly kind: "reg" | "ver" | "mem" | "cre";
  readonly attempt: number;
}

/** The caller supplies the canonical request digest; this does not allocate attempts. */
export function workstreamChildId(input: WorkstreamChildIdInput) {
  return Effect.gen(function* () {
    if (
      typeof input.environment_id !== "string" ||
      typeof input.operation_id !== "string" ||
      typeof input.request_sha256 !== "string" ||
      !/^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}(?![\s\S])/.test(input.environment_id) ||
      !/^[A-Za-z0-9][A-Za-z0-9._:-]{15,127}(?![\s\S])/.test(input.operation_id) ||
      !/^[a-f0-9]{64}(?![\s\S])/.test(input.request_sha256) ||
      !["reg", "ver", "mem", "cre"].includes(input.kind) ||
      !Number.isInteger(input.attempt) ||
      input.attempt < 1 ||
      input.attempt > 9
    ) {
      throw new Error("Invalid workstream child ID input.");
    }
    const crypto = yield* Crypto.Crypto;
    const bytes = yield* crypto.digest(
      "SHA-256",
      new TextEncoder().encode(
        `jwo/1\0${input.environment_id}\0${input.operation_id}\0${input.request_sha256}`,
      ),
    );
    const digest = Hex.encode(bytes).slice(0, 40);
    return `jwo-${digest}-${input.kind}-${input.attempt}`;
  });
}
