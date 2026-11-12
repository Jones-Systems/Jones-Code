import { assert, it } from "@effect/vitest";
import { NodeId, ProviderThreadId, ThreadId, TurnItemId } from "@t3tools/contracts";
import * as BackgroundLiveness from "./ProviderSessionBackgroundLiveness.ts";

type Input = Parameters<typeof BackgroundLiveness.providerSessionBackgroundLiveness>[0];
type Item = Input["turnItems"][number];
const threadId = ThreadId.make("parent");
const providerThreadId = ProviderThreadId.make("provider-parent");
const input: Input = {
  runtimeLive: true,
  threadId,
  providerThreadId,
  providerThreads: [],
  subagents: [],
  turnItems: [],
};
const agent = (status: Input["subagents"][number]["status"]): Input["subagents"][number] => ({
  id: NodeId.make("agent"),
  threadId,
  status,
});
const shell = (parentItemId: Item["parentItemId"] = null): Item => ({
  id: TurnItemId.make("shell"),
  threadId,
  providerThreadId,
  parentItemId,
  nodeId: null,
  type: "command_execution",
  status: "running",
});
const observe = BackgroundLiveness.providerSessionBackgroundLiveness;

it("gives nested running agents precedence over standalone monitors", () => {
  assert.equal(
    observe({ ...input, subagents: [agent("running")], turnItems: [shell()] }),
    "working",
  );
  assert.equal(
    observe({ ...input, turnItems: [{ ...shell(), type: "subagent" }, shell()] }),
    "working",
  );
  assert.equal(
    observe({
      ...input,
      providerThreads: [
        { id: providerThreadId, pendingBackgroundTasks: [{ taskId: "agent", kind: "subagent" }] },
      ],
      turnItems: [shell()],
    }),
    "working",
  );
});

it("classifies standalone shells, persistent tools, and typed rosters as monitoring", () => {
  assert.equal(observe({ ...input, turnItems: [shell()] }), "monitoring");
  assert.equal(
    observe({
      ...input,
      turnItems: [{ ...shell(), type: "dynamic_tool", input: { persistent: true } }],
    }),
    "monitoring",
  );
  for (const kind of ["monitor", "command"] as const) {
    assert.equal(
      observe({
        ...input,
        providerThreads: [
          { id: providerThreadId, pendingBackgroundTasks: [{ taskId: "task", kind }] },
        ],
      }),
      "monitoring",
    );
  }
});

it("excludes inert agents and commands or persistent tools owned by an agent", () => {
  const item: Item = {
    ...shell(),
    id: TurnItemId.make("agent-item"),
    type: "subagent",
    status: "idle",
  };
  for (const child of [
    shell(item.id),
    { ...shell(), nodeId: agent("idle").id },
    { ...shell(item.id), type: "dynamic_tool" as const, input: { persistent: true } },
  ]) {
    assert.isNull(
      observe({
        ...input,
        subagents: [agent("idle"), agent("completed")],
        turnItems: [item, child],
      }),
    );
  }
});

it("clears liveness after the runtime dies and does not revive inert rows", () => {
  assert.isNull(
    observe({ ...input, runtimeLive: false, subagents: [agent("running")], turnItems: [shell()] }),
  );
  assert.isNull(observe({ ...input, subagents: [agent("pending"), agent("waiting")] }));
  assert.isNull(observe({ ...input, turnItems: [{ ...shell(), status: "completed" }] }));
});

it("ignores evidence belonging to another thread or provider conversation", () => {
  assert.isNull(
    observe({
      ...input,
      subagents: [{ ...agent("running"), threadId: ThreadId.make("other") }],
      turnItems: [
        { ...shell(), providerThreadId: ProviderThreadId.make("other") },
        { ...shell(), threadId: ThreadId.make("other") },
      ],
      providerThreads: [
        {
          id: ProviderThreadId.make("other"),
          pendingBackgroundTasks: [{ taskId: "other", kind: "subagent" }],
        },
      ],
    }),
  );
});

it("does not invent monitoring from generic rosters or nonpersistent tool inputs", () => {
  assert.isNull(observe({ ...input, providerThreads: [{ id: providerThreadId }] }));
  assert.isNull(
    observe({
      ...input,
      providerThreads: [
        {
          id: providerThreadId,
          pendingBackgroundTasks: [{ taskId: "unknown", kind: "background_task" }],
        },
      ],
    }),
  );
  for (const value of [
    null,
    undefined,
    [],
    "persistent",
    { persistent: false },
    { persistent: "true" },
    {},
  ]) {
    assert.isNull(
      observe({ ...input, turnItems: [{ ...shell(), type: "dynamic_tool", input: value }] }),
    );
  }
});
