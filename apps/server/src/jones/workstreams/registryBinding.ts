// @effect-diagnostics-next-line nodeBuiltinImport:off -- Exact response-byte hashing and constant-time MAC comparison implement the pinned wire contract.
import * as NodeCrypto from "node:crypto";
import type { RegistryDispatch } from "@t3tools/shared/jones/workstreamsRegistryBinding";

export class RegistryBindingProofFailure extends Error {
  constructor() {
    super("registry_binding_invalid");
  }
}

/** Internal evidence only; the transport converts it to the submit-only port tag. */
export class RegistryBindingRefusal extends Error {
  constructor() {
    super("registry_binding_not_current");
  }
}

export function verifyRegistryResponse(input: {
  readonly response: Response;
  readonly body: Uint8Array;
  readonly token: string;
  readonly requestSignature: string;
  readonly secret: Uint8Array;
  readonly contractVersion: string;
  readonly contractManifest: string;
}): void {
  const headers = input.response.headers;
  const dispatch = headers.get("x-control-registry-dispatch");
  const supplied = headers.get("x-control-registry-response-mac") ?? "";
  if (
    headers.get("x-control-registry-binding") !== input.token ||
    (dispatch !== "dispatched" && dispatch !== "not-dispatched") ||
    !/^[A-Za-z0-9_-]{43}$/.test(supplied)
  )
    throw new RegistryBindingProofFailure();
  const version = headers.get("x-control-contract-version") ?? "";
  const manifest = headers.get("x-control-contract-manifest") ?? "";
  const canonical = [
    "workstreams-registry-response/1.0.0",
    input.requestSignature,
    String(input.response.status),
    version,
    manifest,
    input.token,
    dispatch satisfies RegistryDispatch,
    NodeCrypto.createHash("sha256").update(input.body).digest("hex"),
  ].join("\n");
  const expected = NodeCrypto.createHmac("sha256", input.secret).update(canonical).digest();
  const actual = Buffer.from(supplied, "base64url");
  if (
    actual.length !== expected.length ||
    actual.toString("base64url") !== supplied ||
    !NodeCrypto.timingSafeEqual(expected, actual)
  )
    throw new RegistryBindingProofFailure();
  const matchingContract = version === input.contractVersion && manifest === input.contractManifest;
  if (
    input.response.status === 409 &&
    dispatch === "not-dispatched" &&
    (matchingContract || (version === "" && manifest === "")) &&
    Buffer.from(input.body).equals(Buffer.from('{"code":"registry_binding_not_current"}'))
  ) {
    throw new RegistryBindingRefusal();
  }
  // Only the exact envelope above proves effectlessness. Even receipt GET may enter business code.
  if (!matchingContract || dispatch !== "dispatched") throw new RegistryBindingProofFailure();
}
