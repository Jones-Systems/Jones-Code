import { expect, it } from "vite-plus/test";
import { NodeId, ProviderThreadId, ThreadId, TurnItemId, type OrchestrationV2Subagent, type OrchestrationV2TurnItem } from "@t3tools/contracts";
import { providerSessionBackgroundLiveness } from "./ProviderSessionBackgroundLiveness.ts";

const threadId = ThreadId.make("parent");
const providerThreadId = ProviderThreadId.make("provider-parent");
const input = { runtimeLive: true, threadId, providerThreadId, providerThreads: [], subagents: [], turnItems: [] };
const agent = (status: OrchestrationV2Subagent["status"]) => ({ id: NodeId.make("agent"), threadId, status }) as OrchestrationV2Subagent;
const shell = (parentItemId: OrchestrationV2TurnItem["parentItemId"] = null) => ({ id: TurnItemId.make("shell"), threadId, providerThreadId, parentItemId, nodeId: null, type: "command_execution", status: "running" }) as OrchestrationV2TurnItem;

it("classifies nested running agents as working and gives agents precedence over monitors", () => {
  expect(providerSessionBackgroundLiveness({ ...input, subagents: [agent("running")], turnItems: [shell()] })).toBe("working");
});

it("classifies standalone shells and monitors as monitoring", () => {
  expect(providerSessionBackgroundLiveness({ ...input, turnItems: [shell()] })).toBe("monitoring");
  expect(providerSessionBackgroundLiveness({ ...input, providerThreads: [{ id: providerThreadId, pendingBackgroundTasks: [{ taskId: "monitor", kind: "monitor" }] }] })).toBe("monitoring");
});

it("excludes inert and terminal agents and monitors owned by an agent", () => {
  const item = { ...shell(), id: TurnItemId.make("agent-item"), type: "subagent", status: "idle" } as OrchestrationV2TurnItem;
  expect(providerSessionBackgroundLiveness({ ...input, subagents: [agent("idle"), agent("completed")], turnItems: [item, shell(item.id)] })).toBeNull();
});

it("clears liveness after the runtime dies and never revives work from status rows", () => {
  expect(providerSessionBackgroundLiveness({ ...input, runtimeLive: false, subagents: [agent("running")], turnItems: [shell()] })).toBeNull();
  expect(providerSessionBackgroundLiveness({ ...input, subagents: [agent("pending"), agent("waiting")] })).toBeNull();
});
