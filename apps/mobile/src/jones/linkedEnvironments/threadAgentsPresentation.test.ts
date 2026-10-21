import { expect, it } from "vite-plus/test";

import { resolveSubagentRowPresentation, subagentThreadTarget } from "../../features/threads/threadAgentsPresentation";

const base = {
  title: null,
  prompt: "Audit the timestamps",
  status: "running" as const,
  result: null,
  childThreadId: "thread-child" as never,
};


it("opens a task in a linked environment there, only when this app knows it", () => {
    const remote = {
      ...base,
      childThreadId: null,
      remoteChild: {
        environmentId: "environment-box" as never,
        threadId: "thread-on-box" as never,
        label: "Box",
      },
    };
    expect(resolveSubagentRowPresentation(remote).runsOn).toBe("Box");
    expect(resolveSubagentRowPresentation(remote, false).canOpenThread).toBe(false);
    expect(resolveSubagentRowPresentation(remote, true).canOpenThread).toBe(true);
    const here = "environment-here" as never;
    expect(subagentThreadTarget(remote, here, () => false)).toBeNull();
    expect(subagentThreadTarget(remote, here, (id) => id === "environment-box")).toEqual({
      environmentId: "environment-box",
      threadId: "thread-on-box",
    });
    expect(subagentThreadTarget(base, here, () => false)).toEqual({
      environmentId: here,
      threadId: "thread-child",
    });
  });
