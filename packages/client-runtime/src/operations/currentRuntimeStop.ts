import {
  type ExecutionEnvironmentCapabilities,
  type StopCurrentThreadRuntimeInput,
  type StopCurrentThreadRuntimeResult,
  type ReadCurrentRuntimeStopTargetResult,
  type CurrentRuntimeStopTarget,
  CommandId,
  type ThreadId,
} from "@t3tools/contracts";
export interface CurrentRuntimeRestartPort {
  readonly readPointer: () => StopCurrentThreadRuntimeInput | null;
  readonly reserve: (input: StopCurrentThreadRuntimeInput) => void;
  readonly clear: (input: StopCurrentThreadRuntimeInput) => void;
  readonly capture: () => Promise<ReadCurrentRuntimeStopTargetResult>;
  readonly submit: (
    input: StopCurrentThreadRuntimeInput,
  ) => Promise<StopCurrentThreadRuntimeResult>;
  readonly observe: (
    input: StopCurrentThreadRuntimeInput,
  ) => Promise<StopCurrentThreadRuntimeResult | null>;
  readonly refresh: (target: CurrentRuntimeStopTarget) => Promise<void>;
  readonly commandId: () => CommandId;
}
export interface CurrentRuntimeRestartOutcome {
  readonly status: "stopped" | "accepted" | "unknown" | "unavailable";
  readonly reason: string;
}
export async function restartCapturedCurrentRuntime(
  threadId: ThreadId,
  capability: ExecutionEnvironmentCapabilities["currentRuntimeStop"],
  port: CurrentRuntimeRestartPort,
): Promise<CurrentRuntimeRestartOutcome> {
  let pointer: StopCurrentThreadRuntimeInput | null;
  try {
    pointer = port.readPointer();
  } catch {
    return {
      status: "unknown",
      reason: "The saved stop identity could not be read. No new stop was sent.",
    };
  }
  if (capability === undefined)
    return {
      status: pointer === null ? "unavailable" : "unknown",
      reason: "This server does not expose captured runtime stop. No stop was sent.",
    };
  if (
    pointer !== null &&
    (pointer.threadId !== threadId || pointer.target.binding.threadId !== threadId)
  )
    return {
      status: "unknown",
      reason: "The saved stop identity belongs to a different thread. No new stop was sent.",
    };
  try {
    let result: StopCurrentThreadRuntimeResult | null;
    if (pointer !== null) {
      result = await port.observe(pointer);
    } else {
      const captured = await port.capture();
      if (captured.status === "unavailable")
        return { status: "unavailable", reason: captured.reason };
      if (
        captured.target.binding.threadId !== threadId ||
        !capability.supportedDrivers.some((driver) => driver === captured.target.binding.driver)
      )
        return { status: "unavailable", reason: "This runtime has no qualified stop capture." };
      pointer = { commandId: port.commandId(), threadId, target: captured.target };
      port.reserve(pointer);
      result = await port.submit(pointer);
    }
    if (
      result === null ||
      result.commandId !== pointer.commandId ||
      result.threadId !== pointer.threadId
    )
      return {
        status: "unknown",
        reason: "The original stop receipt is unavailable. No new stop was sent.",
      };
    if (result.status === "stopped") {
      await port.refresh(pointer.target);
      port.clear(pointer);
      return {
        status: "stopped",
        reason:
          "The captured runtime stopped. Your next message starts a fresh session. Background coverage is partial.",
      };
    }
    return {
      status: result.status,
      reason:
        result.status === "accepted"
          ? "The original stop was accepted. Run this action again to check its status; no new stop will be sent."
          : "The original stop completion is unknown. No stop will be repeated and no provider refresh was performed.",
    };
  } catch {
    return {
      status: "unknown",
      reason:
        "The original stop or its refresh could not be confirmed. Its saved identity is retained; no stop will be repeated.",
    };
  }
}
