import { assert, it } from "@effect/vitest";
import { makeProviderFailure } from "@t3tools/provider-core/server/failure";
import { ProviderInstanceId, ProviderSessionId } from "@t3tools/contracts";

import { ProviderSessionOpenError } from "../../orchestration-v2/ProviderSessionManager.ts";
import { RuntimeUseError } from "../cleanup/RuntimeUseProtocol.ts";
import { startFailureCauseMessage } from "./startFailureMessage.ts";

const openError = (cause?: unknown) =>
  new ProviderSessionOpenError({
    instanceId: ProviderInstanceId.make("codex_example"),
    providerSessionId: ProviderSessionId.make("provider-session:example"),
    ...(cause === undefined ? {} : { cause }),
  });

const OPEN_MESSAGE =
  "Failed to open provider instance codex_example session provider-session:example.";

it("keeps the session-open text when the cause is a tagged error without a message", () => {
  const cause = new RuntimeUseError({ reason: "runtime_use_unavailable" });
  assert.equal(cause.message, "");
  const error = openError(cause);

  const message = startFailureCauseMessage(error, error.cause);

  assert.equal(message, OPEN_MESSAGE);
  assert.equal(makeProviderFailure({ cause: error, message }).message, OPEN_MESSAGE);
});

it("prefers a nonblank nested error or string reason", () => {
  const nested = new Error("The previous native writer has not finished retiring.");
  assert.equal(startFailureCauseMessage(openError(nested), nested), nested.message);
  const reason = "The native session changed before opening.";
  assert.equal(startFailureCauseMessage(openError(reason), reason), reason);
});

it("falls back to the start error for blank, missing or non-error causes", () => {
  for (const cause of [undefined, "", "   ", new Error("  "), { reason: "opaque" }]) {
    assert.equal(startFailureCauseMessage(openError(), cause), OPEN_MESSAGE);
  }
});
