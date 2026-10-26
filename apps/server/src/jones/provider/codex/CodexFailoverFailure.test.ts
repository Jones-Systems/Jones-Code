import type { ServerNotification__CodexErrorInfo } from "effect-codex-app-server/schema";
import { describe, expect, it } from "vite-plus/test";

import { classifyCodexFailoverFailure } from "./CodexFailoverFailure.ts";

describe("Codex failover failure evidence", () => {
  it.each([
    "usageLimitExceeded",
    "rateLimitExceeded",
    { responseTooManyFailedAttempts: { httpStatusCode: 429 } },
    { httpConnectionFailed: { httpStatusCode: 429 } },
  ] satisfies ServerNotification__CodexErrorInfo[])("recognizes exhausted usage: %j", (info) => {
    expect(classifyCodexFailoverFailure(info)).toBe("usage_limit");
  });

  it.each([
    "serverOverloaded",
    "internalServerError",
    "flexUnavailable",
    { httpConnectionFailed: {} },
    { responseStreamConnectionFailed: { httpStatusCode: null } },
    { responseStreamDisconnected: { httpStatusCode: 503 } },
    { responseTooManyFailedAttempts: { httpStatusCode: 502 } },
    { responseTooManyFailedAttempts: { httpStatusCode: 500 } },
    { httpConnectionFailed: { httpStatusCode: 408 } },
    { responseStreamConnectionFailed: { httpStatusCode: 504 } },
  ] satisfies ServerNotification__CodexErrorInfo[])(
    "recognizes terminal availability: %j",
    (info) => {
      expect(classifyCodexFailoverFailure(info)).toBe("unavailable");
    },
  );

  it.each([400, 401, 403, 404, 409, 422, 501])(
    "does not route an HTTP %i rejection to another account",
    (httpStatusCode) => {
      for (const info of [
        { httpConnectionFailed: { httpStatusCode } },
        { responseStreamConnectionFailed: { httpStatusCode } },
        { responseStreamDisconnected: { httpStatusCode } },
        { responseTooManyFailedAttempts: { httpStatusCode } },
      ]) {
        expect(classifyCodexFailoverFailure(info)).toBeNull();
      }
    },
  );

  it.each([
    undefined,
    null,
    "contextWindowExceeded",
    "sessionBudgetExceeded",
    "cyberPolicy",
    "misalignmentPolicyViolation",
    "tooManyDenials",
    "unauthorized",
    "badRequest",
    "threadRollbackFailed",
    "sandboxError",
    "other",
    { responseTooManyFailedAttempts: {} },
    { responseTooManyFailedAttempts: { httpStatusCode: null } },
  ] satisfies Array<ServerNotification__CodexErrorInfo | null | undefined>)(
    "leaves nonavailability or unknown evidence unchanged: %j",
    (info) => {
      expect(classifyCodexFailoverFailure(info)).toBeNull();
    },
  );
});
