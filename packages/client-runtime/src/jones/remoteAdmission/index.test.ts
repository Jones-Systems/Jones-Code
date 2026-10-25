import { describe, expect, it } from "vite-plus/test";
import {
  RemoteEnvironmentAuthFetchError,
  RemoteEnvironmentAuthTimeoutError,
  RemoteEnvironmentAuthUndeclaredStatusError,
} from "../../rpc/http.ts";
import { normalizeRemoteAdmissionError, RemoteAdmissionError } from "./index.ts";

const privateDetail = "synthetic-private-payload";

describe("remote admission failure normalization", () => {
  it.each([
    [401, "unauthorized"],
    [403, "unauthorized"],
    [404, "unsupported"],
    [405, "unsupported"],
    [429, "transient"],
    [503, "transient"],
    [409, "other"],
  ] as const)("bounds HTTP %s to %s without retaining the URL or response", (status, kind) => {
    const error = normalizeRemoteAdmissionError(
      new RemoteEnvironmentAuthUndeclaredStatusError(`https://${privateDetail}.invalid`, status),
    );
    expect(error).toBeInstanceOf(RemoteAdmissionError);
    expect(error.kind).toBe(kind);
    expect(JSON.stringify(error)).not.toContain(privateDetail);
    expect(error).not.toHaveProperty("cause");
    expect(error).not.toHaveProperty("requestUrl");
  });

  it.each([
    ["EnvironmentAuthInvalidError", "unauthorized"],
    ["EnvironmentScopeRequiredError", "unauthorized"],
    ["EnvironmentOperationForbiddenError", "unauthorized"],
    ["EnvironmentResourceNotFoundError", "unsupported"],
    ["EnvironmentInternalError", "other"],
  ] as const)("preserves the declared %s classification without its payload", (_tag, kind) => {
    const error = normalizeRemoteAdmissionError({
      _tag,
      message: privateDetail,
      reason: privateDetail,
    });
    expect(error.kind).toBe(kind);
    expect(JSON.stringify(error)).not.toContain(privateDetail);
  });

  it("keeps transport failures retryable and unknown failures bounded", () => {
    for (const failure of [
      new RemoteEnvironmentAuthFetchError({
        message: privateDetail,
        cause: { payload: privateDetail },
      }),
      new RemoteEnvironmentAuthTimeoutError(`https://${privateDetail}.invalid`, 45_000),
    ]) {
      const error = normalizeRemoteAdmissionError(failure);
      expect(error.kind).toBe("transient");
      expect(JSON.stringify(error)).not.toContain(privateDetail);
    }
    expect(normalizeRemoteAdmissionError(new Error(privateDetail)).kind).toBe("other");
    expect(normalizeRemoteAdmissionError(null).message).not.toContain(privateDetail);
    const classified = new RemoteAdmissionError("unauthorized");
    Object.defineProperty(classified, "message", { value: privateDetail });
    expect(normalizeRemoteAdmissionError(classified).message).not.toContain(privateDetail);
  });
});
