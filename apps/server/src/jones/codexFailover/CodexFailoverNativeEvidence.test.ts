import { assert, describe, it } from "@effect/vitest";
import type { ServerNotification__CodexErrorInfo } from "effect-codex-app-server/schema";

import {
  codexFailoverContraryInfo,
  makeCodexFailoverNativeEvidence,
} from "./CodexFailoverNativeEvidence.ts";

describe("Codex failover native evidence", () => {
  it("uses completion info first, otherwise only a matching or absent message", () => {
    const memory = makeCodexFailoverNativeEvidence<object>();
    const owner = {};
    for (const status of [400, 401, 403]) {
      memory.observe("turn", owner, { message: "failed", codexErrorInfo: "usageLimitExceeded" });
      const info = { httpConnectionFailed: { httpStatusCode: status } };
      assert.deepEqual(
        memory.take("turn", owner, { message: "failed", codexErrorInfo: info }),
        info,
      );
    }
    for (const message of [undefined, "failed"]) {
      memory.observe("turn", owner, { message: "failed", codexErrorInfo: "usageLimitExceeded" });
      assert.equal(memory.take("turn", owner, { message }), "usageLimitExceeded");
    }
    memory.observe("turn", owner, { message: "failed", codexErrorInfo: "usageLimitExceeded" });
    assert.isUndefined(memory.take("turn", owner, { message: "replacement" }));
  });

  it("replaces earlier info even when the latest final notification has none", () => {
    const memory = makeCodexFailoverNativeEvidence<object>();
    const owner = {};
    memory.observe("turn", owner, { message: "failed", codexErrorInfo: "usageLimitExceeded" });
    memory.observe("turn", owner, { message: "failed" });
    assert.isUndefined(memory.take("turn", owner, { message: "failed" }));
  });

  it("consumes evidence once, fences owners, forgets cleanups, and bounds memory at 1024", () => {
    const memory = makeCodexFailoverNativeEvidence<object>();
    const owner = {};
    const error = { message: "failed", codexErrorInfo: "usageLimitExceeded" as const };
    memory.observe("turn", owner, error);
    assert.isUndefined(memory.take("turn", {}, {}));
    assert.isUndefined(memory.take("turn", owner, {}));
    for (let index = 0; index < 1024; index++) memory.observe(String(index), owner, error);
    memory.observe("0", owner, error);
    memory.observe("1024", owner, error);
    assert.isUndefined(memory.take("1", owner, {}));
    assert.equal(memory.take("0", owner, {}), "usageLimitExceeded");
    assert.isUndefined(memory.take("0", owner, {}));
    memory.forget("2");
    assert.isUndefined(memory.take("2", owner, {}));
    memory.clear();
    assert.isUndefined(memory.take("1024", owner, {}));
  });

  it("recognizes contrary HTTP variants and native authentication or bad-request info", () => {
    const variants = [
      "httpConnectionFailed",
      "responseStreamConnectionFailed",
      "responseStreamDisconnected",
      "responseTooManyFailedAttempts",
    ] as const;
    for (const variant of variants) {
      for (const httpStatusCode of [400, 401, 403, 429, 503]) {
        const info = { [variant]: { httpStatusCode } } as ServerNotification__CodexErrorInfo;
        assert.equal(codexFailoverContraryInfo(info), [400, 401, 403].includes(httpStatusCode));
      }
    }
    assert.isTrue(codexFailoverContraryInfo("unauthorized"));
    assert.isTrue(codexFailoverContraryInfo("badRequest"));
    assert.isFalse(codexFailoverContraryInfo("other"));
  });
});
