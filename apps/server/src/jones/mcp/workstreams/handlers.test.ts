import { expect, it } from "@effect/vitest";
import * as NodeCrypto from "@effect/platform-node/NodeCrypto";
import {
  EnvironmentId,
  OrchestratorMcpEnvironmentTarget,
  OrchestratorMcpFailure,
  ThreadId,
} from "@t3tools/contracts";
import * as Personal from "../../workstreams/personal/service.ts";
import * as Agent from "../../workstreams/personal/agentService.ts";
import * as P from "@t3tools/contracts/jones/workstreamsPersonal";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Schema from "effect/Schema";
import * as Stream from "effect/Stream";
import * as Invocation from "../../../mcp/McpInvocationContext.ts";
import * as Access from "../../../mcp/McpToolAccess.ts";
import { ThreadManagementService } from "../../../orchestration-v2/ThreadManagementService.ts";
import * as PeerForwarding from "../../../peer/PeerForwarding.ts";
import * as PeerLinks from "../../../peer/PeerLinks.ts";
import * as PeerMcpClient from "../../../peer/PeerMcpClient.ts";
import * as ProjectService from "../../../project/ProjectService.ts";
import { liveThreadShell } from "../../../mcp/McpToolAccess.testkit.ts";
import { handlers } from "./handlers.ts";
import { WorkstreamToolkit } from "./tools.ts";

const scope: Invocation.McpInvocationScope = {
  environmentId: EnvironmentId.make("env-1"),
  thread: undefined,
  client: { sessionId: "test", label: "test", access: "full-access" },
  capabilities: new Set(["orchestration"]),
  issuedAt: 0,
  requestNamespace: "test",
};
const threads = Layer.mock(ThreadManagementService)({ getThreadShell: () => Effect.succeed(null) });
const workstream = P.newPersonalWorkstream({
  id: "group",
  name: "Personal",
  orderKey: "n",
  now: "2026-01-01T00:00:00.000Z",
  origin: "env:env-1",
});
const threadId = ThreadId.make("thread");
const assignment = { threadId, workstreamId: null, revision: 0, updatedAt: null };
const backend = (calls: unknown[]) =>
  Layer.mergeAll(
    Layer.mock(Personal.PersonalWorkstreams)({
      snapshot: () =>
        Effect.sync(() => {
          calls.push(["list", {}]);
          return {
            environmentId: scope.environmentId,
            sequence: 0,
            workstreams: [workstream],
            assignments: [],
            importIssue: null,
          };
        }),
      readDefinitions: () => Effect.succeed([]),
      readThread: (id) =>
        Effect.sync(() => {
          calls.push(["read", { threadId: id }]);
          return { assignment: { ...assignment, threadId: id }, workstream: null };
        }),
    }),
    Layer.mock(Agent.AgentWorkstreams)({
      create: (input) =>
        Effect.sync(() => {
          calls.push(["create", input]);
          return { outcome: "applied" as const, workstream, sequence: 1 };
        }),
      update: (input) =>
        Effect.sync(() => {
          calls.push(["update", input]);
          return { outcome: "applied" as const, workstream, sequence: 1 };
        }),
      set: (input) =>
        Effect.sync(() => {
          calls.push(["set", input]);
          return {
            outcome: "already_satisfied" as const,
            assignment,
            workstream: null,
            sequence: 0,
          };
        }),
    }),
  );
const build = (calls: unknown[]) =>
  WorkstreamToolkit.pipe(
    Effect.provide(Access.HandlersLayer.layer(handlers).pipe(Layer.provide(backend(calls)))),
  );
const isMcpFailure = Schema.is(OrchestratorMcpFailure);
type WorkstreamToolName = keyof typeof WorkstreamToolkit.tools;
const remoteEnvironment = EnvironmentId.make("env-remote");
const routedCreate = { id: "stable-personal-id", name: "Routed Workstream" };
const routedInputs: ReadonlyArray<readonly [WorkstreamToolName, Record<string, unknown>]> = [
  ["t3_workstreams_list", {}],
  ["t3_workstream_create", routedCreate],
  ["t3_thread_workstream_set", { threadId, workstreamId: null, expectedRevision: 0 }],
  ["t3_thread_workstream_read", { threadId }],
  ["t3_workstream_update", { id: workstream.id, name: "Renamed" }],
];
const routedDecoders = {
  t3_workstreams_list: Schema.decodeUnknownEffect(
    WorkstreamToolkit.tools.t3_workstreams_list.parametersSchema,
  ),
  t3_workstream_create: Schema.decodeUnknownEffect(
    WorkstreamToolkit.tools.t3_workstream_create.parametersSchema,
  ),
  t3_thread_workstream_set: Schema.decodeUnknownEffect(
    WorkstreamToolkit.tools.t3_thread_workstream_set.parametersSchema,
  ),
  t3_thread_workstream_read: Schema.decodeUnknownEffect(
    WorkstreamToolkit.tools.t3_thread_workstream_read.parametersSchema,
  ),
  t3_workstream_update: Schema.decodeUnknownEffect(
    WorkstreamToolkit.tools.t3_workstream_update.parametersSchema,
  ),
};
const routedCall = (
  toolkit: Effect.Success<ReturnType<typeof build>>,
  name: WorkstreamToolName,
  args: unknown,
) =>
  Effect.gen(function* () {
    switch (name) {
      case "t3_workstreams_list": {
        const input = yield* routedDecoders.t3_workstreams_list(args);
        return yield* toolkit
          .handle("t3_workstreams_list", input)
          .pipe(Stream.unwrap, Stream.runCollect);
      }
      case "t3_workstream_create": {
        const input = yield* routedDecoders.t3_workstream_create(args);
        return yield* toolkit
          .handle("t3_workstream_create", input)
          .pipe(Stream.unwrap, Stream.runCollect);
      }
      case "t3_thread_workstream_set": {
        const input = yield* routedDecoders.t3_thread_workstream_set(args);
        return yield* toolkit
          .handle("t3_thread_workstream_set", input)
          .pipe(Stream.unwrap, Stream.runCollect);
      }
      case "t3_thread_workstream_read": {
        const input = yield* routedDecoders.t3_thread_workstream_read(args);
        return yield* toolkit
          .handle("t3_thread_workstream_read", input)
          .pipe(Stream.unwrap, Stream.runCollect);
      }
      case "t3_workstream_update": {
        const input = yield* routedDecoders.t3_workstream_update(args);
        return yield* toolkit
          .handle("t3_workstream_update", input)
          .pipe(Stream.unwrap, Stream.runCollect);
      }
    }
  });

interface RoutedObservation {
  readonly environmentId: EnvironmentId;
  readonly tool: string;
  readonly arguments: Record<string, unknown>;
  readonly limits: PeerMcpClient.PeerCallLimits;
}
const routingFixture = (callerThreads = threads) =>
  Effect.gen(function* () {
    const localCalls: unknown[] = [];
    const remoteCalls: unknown[] = [];
    const sent: RoutedObservation[] = [];
    const receiving = yield* build(remoteCalls);
    let unavailable = false;
    let receivingScope: Invocation.McpInvocationScope = {
      ...scope,
      environmentId: remoteEnvironment,
      requestNamespace: "original-receiving-session",
      client: {
        sessionId: "remote-link",
        label: "fixture link",
        access: "full-access",
        linked: true,
      },
    };
    const receive = Effect.fn("WorkstreamRoutingFixture.receive")(
      function* (
        input: Omit<Parameters<PeerMcpClient.PeerMcpClient["Service"]["call"]>[0], "success">,
      ) {
        sent.push({
          environmentId: input.environmentId,
          tool: input.tool,
          arguments: structuredClone(input.arguments),
          limits: { ...input.limits },
        });
        if (unavailable)
          return yield* new OrchestratorMcpFailure({
            code: "orchestration_error",
            message: "The original peer is unavailable.",
          });
        const name = routedInputs.find(([name]) => name === input.tool)?.[0];
        if (name === undefined)
          return yield* new OrchestratorMcpFailure({
            code: "invalid_request",
            message: "Unexpected peer tool.",
          });
        const output = yield* routedCall(receiving, name, input.arguments).pipe(
          Effect.provideService(Invocation.McpInvocationContext, {
            ...receivingScope,
            client: { ...receivingScope.client!, narrowedTo: input.limits },
          }),
        );
        return output.at(-1)?.result;
      },
      Effect.provide(threads),
      Effect.mapError((error) =>
        isMcpFailure(error)
          ? error
          : new OrchestratorMcpFailure({
              code: "orchestration_error",
              message: "The peer refused or returned an invalid result.",
            }),
      ),
    );
    const call: PeerMcpClient.PeerMcpClient["Service"]["call"] = Effect.fn(
      "WorkstreamRoutingFixture.call",
    )((input) => {
      const success: Schema.Decoder<(typeof input.success)["Type"]> = input.success;
      const decode = Schema.decodeUnknownEffect(success);
      return receive(input).pipe(
        Effect.flatMap((result) =>
          decode(result).pipe(
            Effect.mapError(
              () =>
                new OrchestratorMcpFailure({
                  code: "orchestration_error",
                  message: "The peer refused or returned an invalid result.",
                }),
            ),
          ),
        ),
      );
    });
    const forwarding = PeerForwarding.layer.pipe(
      Layer.provide(Layer.succeed(PeerMcpClient.PeerMcpClient, { call })),
      Layer.provide(Layer.mock(PeerLinks.PeerLinks)({ enabled: Effect.succeed(true) })),
      Layer.provide(
        Layer.mock(ProjectService.ProjectService)({ getById: () => Effect.succeedNone }),
      ),
      Layer.provide(NodeCrypto.layer),
      Layer.provide(callerThreads),
    );
    const sending = yield* build(localCalls).pipe(Effect.provide(forwarding));
    return {
      localCalls,
      remoteCalls,
      sent,
      sending,
      makeUnavailable: () => {
        unavailable = true;
      },
      makeReceivingReadOnly: () => {
        receivingScope = {
          ...receivingScope,
          client: { ...receivingScope.client!, access: "read-only" },
        };
      },
    };
  });

it("all five personal Workstreams tool schemas expose the standard environment target", () => {
  for (const [name, args] of routedInputs) {
    const tool = WorkstreamToolkit.tools[name];
    expect(tool.parametersSchema.fields.environmentId).toBe(OrchestratorMcpEnvironmentTarget);
    const decode = Schema.decodeUnknownSync(tool.parametersSchema);
    expect(decode({ ...args, environmentId: remoteEnvironment })).toEqual({
      ...args,
      environmentId: remoteEnvironment,
    });
    expect(decode(args)).toEqual(args);
    for (const invalid of [
      { ...args, environmentId: 1 },
      { ...args, environmentId: null },
    ])
      expect(() => decode(invalid)).toThrow();
  }
});

it.effect(
  "routes all five personal Workstreams tools once with unchanged intent to the named server",
  () =>
    Effect.gen(function* () {
      const f = yield* routingFixture();
      for (const [name, args] of routedInputs) {
        const original = structuredClone(args);
        yield* routedCall(f.sending, name, { ...args, environmentId: remoteEnvironment });
        expect(args).toEqual(original);
      }
      expect(f.localCalls).toEqual([]);
      expect(f.sent).toHaveLength(routedInputs.length);
      for (const [index, [name, args]] of routedInputs.entries()) {
        expect(f.sent[index]).toEqual({
          environmentId: remoteEnvironment,
          tool: name,
          arguments: args,
          limits: { runtimeMode: "full-access", interactionMode: "default" },
        });
        expect(f.sent[index]!.arguments).not.toHaveProperty("environmentId");
      }
      expect(f.remoteCalls).toEqual([
        ["list", {}],
        ["create", routedCreate],
        ["set", routedInputs[2]![1]],
        ["read", { threadId }],
        ["update", routedInputs[4]![1]],
      ]);
    }).pipe(Effect.provideService(Invocation.McpInvocationContext, scope), Effect.provide(threads)),
);

it.effect("an explicit local target never forwards or reaches the domain payload", () =>
  Effect.gen(function* () {
    const f = yield* routingFixture();
    yield* routedCall(f.sending, "t3_workstreams_list", { environmentId: scope.environmentId });
    yield* routedCall(f.sending, "t3_workstream_create", {
      environmentId: scope.environmentId,
      ...routedCreate,
    });
    expect(f.sent).toEqual([]);
    expect(f.remoteCalls).toEqual([]);
    expect(f.localCalls).toEqual([
      ["list", {}],
      ["create", routedCreate],
    ]);
  }).pipe(Effect.provideService(Invocation.McpInvocationContext, scope), Effect.provide(threads)),
);

it.effect("read-only callers may route reads but cannot dispatch any mutation", () =>
  Effect.gen(function* () {
    const f = yield* routingFixture();
    yield* routedCall(f.sending, "t3_workstreams_list", { environmentId: remoteEnvironment });
    expect(f.sent[0]?.limits).toEqual({
      runtimeMode: "approval-required",
      interactionMode: "default",
    });
    for (const name of [
      "t3_workstream_create",
      "t3_thread_workstream_set",
      "t3_workstream_update",
    ] as const) {
      const args = routedInputs.find(([candidate]) => candidate === name)![1];
      const result = yield* routedCall(f.sending, name, {
        ...args,
        environmentId: remoteEnvironment,
      });
      expect(JSON.stringify(result)).toContain("capability_denied");
    }
    expect(f.sent).toHaveLength(1);
    expect(f.localCalls).toEqual([]);
  }).pipe(
    Effect.provideService(Invocation.McpInvocationContext, {
      ...scope,
      client: { ...scope.client!, access: "read-only" },
    }),
    Effect.provide(threads),
  ),
);

it.effect("fenced linked callers cannot use these tools for a second relay", () =>
  Effect.gen(function* () {
    const f = yield* routingFixture();
    for (const [name, args] of routedInputs) {
      const result = yield* routedCall(f.sending, name, {
        ...args,
        environmentId: remoteEnvironment,
      });
      expect(JSON.stringify(result)).toContain("capability_denied");
    }
    expect(f.sent).toEqual([]);
    expect(f.localCalls).toEqual([]);
    expect(f.remoteCalls).toEqual([]);
  }).pipe(
    Effect.provideService(Invocation.McpInvocationContext, {
      ...scope,
      client: { ...scope.client!, access: "auto", linked: true },
    }),
    Effect.provide(threads),
  ),
);

it.effect("a missing orchestration capability refuses before peer dispatch", () =>
  Effect.gen(function* () {
    const f = yield* routingFixture();
    for (const [name, args] of routedInputs) {
      const result = yield* routedCall(f.sending, name, {
        ...args,
        environmentId: remoteEnvironment,
      });
      expect(JSON.stringify(result)).toContain("capability_denied");
    }
    expect(f.sent).toEqual([]);
    expect(f.localCalls).toEqual([]);
  }).pipe(
    Effect.provideService(Invocation.McpInvocationContext, {
      ...scope,
      capabilities: new Set<never>(),
    }),
    Effect.provide(threads),
  ),
);

it.effect(
  "peer failure neither retries nor falls back; an explicit retry retains the original stable creation id",
  () =>
    Effect.gen(function* () {
      const f = yield* routingFixture();
      f.makeUnavailable();
      for (let attempt = 0; attempt < 2; attempt++) {
        const output = yield* routedCall(f.sending, "t3_workstream_create", {
          environmentId: remoteEnvironment,
          ...routedCreate,
        });
        expect(JSON.stringify(output)).toContain("The original peer is unavailable");
        expect(f.sent).toHaveLength(attempt + 1);
      }
      expect(f.sent[0]).toEqual(f.sent[1]);
      expect(f.localCalls).toEqual([]);
      expect(f.remoteCalls).toEqual([]);
    }).pipe(Effect.provideService(Invocation.McpInvocationContext, scope), Effect.provide(threads)),
);

it.effect("receiving access refusal cannot cause a local mutation or a replacement dispatch", () =>
  Effect.gen(function* () {
    const f = yield* routingFixture();
    f.makeReceivingReadOnly();
    const output = yield* routedCall(f.sending, "t3_workstream_create", {
      environmentId: remoteEnvironment,
      ...routedCreate,
    });
    expect(JSON.stringify(output)).toContain("peer refused");
    expect(f.sent).toHaveLength(1);
    expect(f.remoteCalls).toEqual([]);
    expect(f.localCalls).toEqual([]);
  }).pipe(Effect.provideService(Invocation.McpInvocationContext, scope), Effect.provide(threads)),
);

it.effect("provider-run callers forward their original thread mode ceiling", () =>
  Effect.gen(function* () {
    const shell = liveThreadShell(ThreadId.make("scheduled-caller"), {
      runtimeMode: "auto-accept-edits",
      interactionMode: "plan",
    });
    const callerThreads = Layer.mock(ThreadManagementService)({
      getThreadShell: (id) => Effect.succeed(id === shell.id ? shell : null),
    });
    const original: Invocation.McpInvocationScope = {
      ...scope,
      client: undefined,
      thread: {
        threadId: shell.id,
        providerInstanceId: shell.providerInstanceId,
        providerSessionId: "original-scheduled-run-credential",
      },
      requestNamespace: "scheduled-run-original-namespace",
    };
    const f = yield* routingFixture(callerThreads);
    yield* routedCall(f.sending, "t3_workstreams_list", {
      environmentId: remoteEnvironment,
    }).pipe(Effect.provideService(Invocation.McpInvocationContext, original));
    expect(f.sent).toHaveLength(1);
    expect(f.sent[0]?.limits).toEqual({
      runtimeMode: "auto-accept-edits",
      interactionMode: "plan",
    });
    expect(f.sent[0]?.arguments).toEqual({});
    expect(f.localCalls).toEqual([]);
    expect(original.requestNamespace).toBe("scheduled-run-original-namespace");
  }).pipe(Effect.provideService(Invocation.McpInvocationContext, scope), Effect.provide(threads)),
);
