import { EnvironmentId, ThreadId } from "@t3tools/contracts";
import { expect, it } from "vite-plus/test";

import {
  resolveSubagentRowPresentation,
  subagentThreadTarget,
} from "../../features/threads/threadAgentsPresentation";

const base = {
  title: null,
  prompt: "Audit the timestamps",
  status: "running" as const,
  result: null,
  childThreadId: ThreadId.make("thread-child"),
};

it("keeps remote agent navigation bound to the target and the linking environment", () => {
  const here = EnvironmentId.make("environment-here");
  const environmentId = EnvironmentId.make("environment-box");
  const threadId = ThreadId.make("thread-on-box");
  const remote = {
    ...base,
    childThreadId: null,
    remoteChild: {
      environmentId,
      threadId,
      label: "Box",
    },
  };
  expect(resolveSubagentRowPresentation(remote).runsOn).toBe("Box");
  expect(resolveSubagentRowPresentation(remote, false).canOpenThread).toBe(false);
  expect(resolveSubagentRowPresentation(remote, true).canOpenThread).toBe(true);
  const noLink = () => false;
  const linked = (id: EnvironmentId) => id === environmentId;
  const noConnection = () => undefined;
  const connected = (id: EnvironmentId) => (id === environmentId ? { connected: true } : undefined);
  const offline = () => ({ connected: false });
  const directTarget = { environmentId, threadId };
  const readOnlyTarget = {
    ...directTarget,
    linked: { viaEnvironmentId: here, label: "Box" },
  };

  expect(subagentThreadTarget(remote, here, connected, linked)).toEqual(directTarget);
  expect(subagentThreadTarget(remote, here, offline, linked)).toEqual(readOnlyTarget);
  expect(subagentThreadTarget(remote, here, noConnection, linked)).toEqual(readOnlyTarget);
  expect(subagentThreadTarget(remote, here, offline, noLink)).toEqual(directTarget);
  // The linked sheet explains the missing link when there is neither route.
  expect(subagentThreadTarget(remote, here, noConnection, noLink)).toEqual(readOnlyTarget);
  expect(subagentThreadTarget(base, here, noConnection, noLink)).toEqual({
    environmentId: here,
    threadId: base.childThreadId,
  });
  expect(subagentThreadTarget({ childThreadId: null }, here, noConnection, noLink)).toBeNull();
});
