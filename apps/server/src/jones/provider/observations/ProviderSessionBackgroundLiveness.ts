import type {
  OrchestrationV2ProviderThread,
  OrchestrationV2Subagent,
  OrchestrationV2TurnItem,
  ProviderThreadId,
  ThreadId,
} from "@t3tools/contracts";

export function providerSessionBackgroundLiveness(input: {
  readonly runtimeLive: boolean;
  readonly threadId: ThreadId;
  readonly providerThreadId: ProviderThreadId;
  readonly providerThreads: ReadonlyArray<
    Pick<OrchestrationV2ProviderThread, "id" | "pendingBackgroundTasks">
  >;
  readonly subagents: ReadonlyArray<Pick<OrchestrationV2Subagent, "id" | "threadId" | "status">>;
  readonly turnItems: ReadonlyArray<
    Pick<
      OrchestrationV2TurnItem,
      "id" | "threadId" | "providerThreadId" | "parentItemId" | "nodeId" | "type" | "status"
    > & { readonly input?: unknown }
  >;
}): "working" | "monitoring" | null {
  if (!input.runtimeLive) return null;
  const items = input.turnItems.filter(
    (item) => item.threadId === input.threadId && item.providerThreadId === input.providerThreadId,
  );
  const agentItems = new Set(items.filter((item) => item.type === "subagent").map((item) => item.id));
  const agents = input.subagents.filter((agent) => agent.threadId === input.threadId);
  const agentNodes = new Set(agents.map((agent) => agent.id));
  const tasks =
    input.providerThreads.find((thread) => thread.id === input.providerThreadId)
      ?.pendingBackgroundTasks ?? [];
  if (
    agents.some((agent) => agent.status === "running") ||
    items.some((item) => item.type === "subagent" && item.status === "running") ||
    tasks.some((task) => task.kind === "subagent")
  ) {
    return "working";
  }
  const monitoring = items.some((item) => {
    if (item.status !== "running") return false;
    if (
      (item.parentItemId !== null && agentItems.has(item.parentItemId)) ||
      (item.nodeId !== null && agentNodes.has(item.nodeId))
    ) {
      return false;
    }
    if (item.type === "command_execution") return true;
    return (
      item.type === "dynamic_tool" &&
      item.input !== null &&
      typeof item.input === "object" &&
      !Array.isArray(item.input) &&
      Reflect.get(item.input, "persistent") === true
    );
  });
  return monitoring || tasks.some((task) => task.kind === "monitor" || task.kind === "command")
    ? "monitoring"
    : null;
}
