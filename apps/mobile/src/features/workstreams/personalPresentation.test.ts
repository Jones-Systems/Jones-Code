import { describe, expect, it } from "vite-plus/test";
import {
  personalEnvironmentStatusText,
  personalWorkstreamFailureText,
} from "./personalPresentation";
import {
  createPersonalWorkstreamsController,
  groupPersonalThreads,
  type PersonalWorkstreamsTransport,
} from "@t3tools/client-runtime/jones/workstreams/personal";
import { AtomRegistry } from "effect/reactivity";
import { EnvironmentId, ThreadId } from "@t3tools/contracts";

describe("personal mobile compatibility", () => {
  it("suppresses cached grouping and all writes after an old-server downgrade", async () => {
    const registry = AtomRegistry.make();
    let calls = 0;
    const transport: PersonalWorkstreamsTransport = {
      subscribe: () => {
        calls++;
        return () => {};
      },
      put: async () => {
        calls++;
        throw new Error("Unexpected put");
      },
      assign: async () => {
        calls++;
        throw new Error("Unexpected assign");
      },
    };
    const controller = createPersonalWorkstreamsController({
      registry,
      origin: "synthetic",
      transport,
    });
    const environmentId = EnvironmentId.make("env:synthetic");
    const thread = { environmentId, id: ThreadId.make("thread:synthetic") };
    controller.setEnvironments([
      { environmentId, generation: 0, capable: true, connected: false, writable: false },
    ]);
    controller.loadCache({
      environmentId,
      sequence: 1,
      importIssue: null,
      workstreams: [
        {
          id: "group",
          name: "Synthetic",
          lifecycle: "active",
          color: null,
          orderKey: "n",
          rev: 1,
          origin: "synthetic",
          createdAt: "2026-01-01T00:00:00.000Z",
          updatedAt: "2026-01-01T00:00:00.000Z",
          deletedAt: null,
        },
      ],
      assignments: [
        {
          threadId: thread.id,
          workstreamId: "group",
          revision: 1,
          updatedAt: "2026-01-01T00:00:00.000Z",
        },
      ],
    });
    expect(groupPersonalThreads(registry.get(controller.state), [thread]).groups).toHaveLength(1);
    controller.setEnvironments([
      { environmentId, generation: 1, capable: false, connected: true, writable: true },
    ]);
    expect(groupPersonalThreads(registry.get(controller.state), [thread]).groups).toEqual([]);
    expect(controller.canAssign(thread)).toBe(false);
    const result = await controller.assign(thread, "group");
    expect(result.ok).toBe(false);
    expect(calls).toBe(0);
    controller.dispose();
    registry.dispose();
  });
  it("maps typed failures and condition rows to frozen plain strings", () => {
    expect(
      personalWorkstreamFailureText(
        {
          action: "assign",
          reason: "changed_elsewhere",
          environmentId: null,
          dedupeKey: "assign:t:changed_elsewhere",
        },
        "Laptop",
      ),
    ).toBe("This thread was moved somewhere else. Try again.");
    expect(
      personalWorkstreamFailureText(
        {
          action: "create",
          reason: "read_only",
          environmentId: null,
          dedupeKey: "create:t:read_only",
        },
        "Laptop",
      ),
    ).toBe("Couldn't create the Workstream. You have read-only access to Laptop.");
    expect(personalEnvironmentStatusText.sync_failed).toBe("Couldn't sync");
  });
});
