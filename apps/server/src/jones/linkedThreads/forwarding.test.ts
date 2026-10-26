import { expect, it } from "@effect/vitest";
import { EnvironmentId, ThreadId } from "@t3tools/contracts";
import * as NodeCrypto from "@effect/platform-node/NodeCrypto";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";

import * as Schema from "effect/Schema";
import * as Invocation from "../../mcp/McpInvocationContext.ts";
import { liveThreadShell } from "../../mcp/McpToolAccess.testkit.ts";
import { ProjectToolkit } from "../../mcp/toolkits/project/tools.ts";
import * as Threads from "../../orchestration-v2/ThreadManagementService.ts";
import * as Forwarding from "../../peer/PeerForwarding.ts";
import * as Peers from "../../peer/PeerMcpClient.ts";
import * as PeerLinks from "../../peer/PeerLinks.ts";
import * as Links from "./LinkedThreads.ts";
import { linkedThreadIdentity } from "./identity.ts";

it.effect(
  "forwards authenticated launch origins, records the returned copy, and excludes remote delegation",
  () => {
    const here = EnvironmentId.make("here"),
      away = EnvironmentId.make("away"),
      origin = ThreadId.make("origin"),
      destination = ThreadId.make("destination");
    const scope: Invocation.McpInvocationScope = {
      environmentId: here,
      issuedAt: 0,
      requestNamespace: "forward-test",
      capabilities: new Set(["orchestration"]),
      thread: {
        threadId: origin,
        providerInstanceId: liveThreadShell(origin).providerInstanceId,
        providerSessionId: "session",
      },
      client: undefined,
    };
    const id = linkedThreadIdentity(
      "launch",
      { environmentId: here, threadId: origin },
      { environmentId: away, threadId: destination },
    ).linkId;
    const calls: Parameters<Peers.PeerMcpClient["Service"]["call"]>[0][] = [];
    const copies: Parameters<Links.LinkedThreads["Service"]["recordRemoteLaunch"]>[0][] = [];
    const services = Layer.mergeAll(
      NodeCrypto.layer,
      Layer.mock(Threads.ThreadManagementService)({
        getThreadShell: (threadId) => Effect.succeed(liveThreadShell(threadId)),
      }),
      Layer.mock(PeerLinks.PeerLinks)({ get: () => Effect.succeedNone }),
      Layer.mock(Peers.PeerMcpClient)({
        call: (input) => {
          calls.push(input);
          return Schema.decodeEffect(input.success)({
            threadId: destination,
            projectId: liveThreadShell(destination).projectId,
            modelSelection: liveThreadShell(destination).modelSelection,
            runId: null,
            status: null,
            ...(input.arguments.title === "Old"
              ? {}
              : { originLink: { linkId: id, state: "recorded" } }),
          }).pipe(Effect.orDie);
        },
      }),
      Layer.mock(Links.LinkedThreads)({
        recordRemoteLaunch: (input) => {
          copies.push(input);
          return Effect.succeed({
            linkId: id,
            state: "recorded",
            reverse: input.reported?.state === "recorded" ? "recorded" : "unavailable",
          });
        },
      }),
    );
    return Effect.gen(function* () {
      const forwarding = yield* Forwarding.PeerForwarding;
      const result = yield* forwarding.call(scope, ProjectToolkit.tools.t3_thread_launch, away, {
        title: "New",
        clientRequestId: "key",
      });
      expect(result.originLink?.reverse).toBe("recorded");
      expect(copies.length).toBe(1);
      expect(calls[0]?.origin).toEqual({
        schemaVersion: "t3.linked-message-origin/v1",
        environmentId: here,
        thread: { threadId: origin, title: origin },
      });
      expect(copies[0]?.reported?.linkId).toBe(id);
      const old = yield* forwarding.call(scope, ProjectToolkit.tools.t3_thread_launch, away, {
        title: "Old",
      });
      expect(old.originLink?.reverse).toBe("unavailable");
      expect(copies.length).toBe(2);
      yield* forwarding.call(scope, ProjectToolkit.tools.t3_thread_launch, away, {
        title: "Delegated",
        delegatedFrom: { environmentId: here, threadId: origin, title: "Origin" },
      });
      expect(copies.length).toBe(2);
    }).pipe(Effect.provide(Forwarding.layer.pipe(Layer.provide(services))));
  },
);
