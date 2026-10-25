import { expect, it } from "@effect/vitest";
import { EnvironmentId, ThreadId, OrchestratorMcpFailure } from "@t3tools/contracts";
import type { LinkedThreadLink } from "@t3tools/contracts/jones/linkedThreads";
import * as Effect from "effect/Effect";
import type { McpInvocationScope } from "../../mcp/McpInvocationContext.ts";
import type { PeerForwarding } from "../../peer/PeerForwarding.ts";
import { mirrorFailureState, mirrorLink } from "./mirror.ts";

const link: LinkedThreadLink = {
  linkId: "tl1:" + "a".repeat(64),
  kind: "peer",
  from: { environmentId: EnvironmentId.make("here"), threadId: ThreadId.make("a") },
  to: { environmentId: EnvironmentId.make("there"), threadId: ThreadId.make("b") },
  basis: "manual",
  assertedBy: "agent",
  reverse: "unknown",
  acceptance: null,
  handoff: null,
  remote: null,
  createdAt: "now",
  updatedAt: "now",
  acceptedAt: null,
};
const scope: McpInvocationScope = {
  environmentId: EnvironmentId.make("here"),
  issuedAt: 0,
  requestNamespace: "test",
  capabilities: new Set(["orchestration"]),
  thread: {
    threadId: ThreadId.make("a"),
    providerInstanceId: "provider" as never,
    providerSessionId: "session",
  },
  client: undefined,
};
it("distinguishes definitive mirror refusal from ambiguous transport", () => {
  expect(
    mirrorFailureState(
      new OrchestratorMcpFailure({
        code: "invalid_request",
        message: "Unknown tool t3_thread_link",
      }),
    ),
  ).toBe("unavailable");
  expect(
    mirrorFailureState(
      new OrchestratorMcpFailure({ code: "orchestration_error", message: "Connection closed" }),
    ),
  ).toBe("unknown");
});
it.effect("reports both sides without requiring a reverse link or bouncing incoming copies", () =>
  Effect.gen(function* () {
    const forwarding = {
      call: () =>
        Effect.fail(
          new OrchestratorMcpFailure({ code: "invalid_request", message: "Unknown tool" }),
        ),
    } as unknown as PeerForwarding["Service"];
    expect(
      (yield* mirrorLink(
        scope,
        link,
        { action: "link", kind: "peer", from: link.from, to: link.to },
        forwarding,
      )).state,
    ).toBe("unavailable");
    expect(
      (yield* mirrorLink(
        { ...scope, thread: undefined },
        link,
        { action: "unlink", linkId: link.linkId },
        forwarding,
      )).state,
    ).toBe("unavailable");
    expect(
      (yield* mirrorLink(
        {
          ...scope,
          thread: undefined,
          client: { sessionId: "peer", label: "Peer", access: "full-access", linked: true },
        },
        link,
        { action: "unlink", linkId: link.linkId },
        forwarding,
      )).state,
    ).toBe("recorded");
  }),
);
