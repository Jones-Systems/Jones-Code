import {
  EnvironmentId,
  RuntimeRequestId,
  ThreadId,
  type OrchestrationV2ShellSnapshot,
} from "@t3tools/contracts";
import * as Option from "effect/Option";
import { Atom, AtomRegistry } from "effect/reactivity";
import { describe, expect, it } from "vite-plus/test";
import { PrimaryConnectionTarget } from "../connection/model.ts";
import { v2Now, v2ShellSnapshot, v2ThreadShell } from "../state/orchestrationV2TestFixtures.ts";
import { childThreadInputsTitle, createEnvironmentThreadShellAtoms } from "../state/threadShell.ts";

const environmentId = EnvironmentId.make("environment-v2");
const remoteEnvironmentId = EnvironmentId.make("remote-environment-v2");

function makeHarness(environmentIds: ReadonlyArray<EnvironmentId> = [environmentId]) {
  const snapshotAtom = Atom.family((_environmentId: EnvironmentId) =>
    Atom.make<OrchestrationV2ShellSnapshot | null>(v2ShellSnapshot),
  );
  const catalogValueAtom = Atom.make({
    isReady: true,
    entries: new Map(
      environmentIds.map((id) => [
        id,
        {
          target: new PrimaryConnectionTarget({
            environmentId: id,
            label: "Environment",
            httpBaseUrl: "https://example.test",
            wsBaseUrl: "wss://example.test",
          }),
          profile: Option.none(),
          enabled: true,
        },
      ]),
    ),
  });
  return {
    registry: AtomRegistry.make(),
    snapshotAtom,
    catalogValueAtom,
    threads: createEnvironmentThreadShellAtoms({ catalogValueAtom, snapshotAtom }),
  };
}

describe("v2 thread shell lists", () => {
  it("lists a task a linked environment runs for a listed thread under that thread", () => {
    const { registry, threads, snapshotAtom, catalogValueAtom } = makeHarness([
      environmentId,
      remoteEnvironmentId,
    ]);
    const parent = v2ThreadShell;
    const task = {
      ...v2ThreadShell,
      id: ThreadId.make("remote-task"),
      creationSource: "mcp" as const,
      status: "running" as const,
      activityRunStatus: "running" as const,
      delegatedFrom: { environmentId, threadId: parent.id, title: parent.title },
    };
    const helper = {
      ...task,
      id: ThreadId.make("remote-helper"),
      delegatedFrom: undefined,
      lineage: {
        ...task.lineage,
        parentThreadId: task.id,
        relationshipToParent: "subagent" as const,
      },
    };
    const approval = {
      id: RuntimeRequestId.make("cat-file"),
      kind: "command" as const,
      createdAt: v2Now,
    };
    registry.set(snapshotAtom(environmentId), { ...v2ShellSnapshot, threads: [parent] });
    registry.set(snapshotAtom(remoteEnvironmentId), {
      ...v2ShellSnapshot,
      threads: [task, helper],
    });
    const parentRef = { environmentId, threadId: parent.id };
    const inputs = threads.childThreadInputsAtom(parentRef);
    const disposeNavigation = registry.mount(threads.navigationThreadShellsAtom);
    const disposeParent = registry.mount(threads.threadShellAtom(parentRef));
    const disposeInputs = registry.mount(inputs);
    const listed = () =>
      registry
        .get(threads.navigationThreadShellsAtom)
        .map((thread) => `${thread.environmentId}/${thread.id}`);
    expect(listed()).toEqual([`${environmentId}/${parent.id}`]);

    // The helper the task started there asks for approval: the parent here hears of it.
    registry.set(snapshotAtom(remoteEnvironmentId), {
      ...v2ShellSnapshot,
      threads: [task, { ...helper, pendingRuntimeRequest: approval }],
    });
    expect(registry.get(threads.threadShellAtom(parentRef))?.pendingSubagentRequest).toBe(
      "approval",
    );
    expect(registry.get(inputs).map((thread) => `${thread.environmentId}/${thread.id}`)).toEqual([
      `${remoteEnvironmentId}/${helper.id}`,
    ]);

    registry.set(snapshotAtom(remoteEnvironmentId), {
      ...v2ShellSnapshot,
      threads: [{ ...task, pendingRuntimeRequest: { ...approval, kind: "user_input" } }, helper],
    });
    expect(registry.get(threads.threadShellAtom(parentRef))?.pendingSubagentRequest).toBe(
      "user_input",
    );
    expect(registry.get(inputs).map((thread) => `${thread.environmentId}/${thread.id}`)).toEqual([
      `${remoteEnvironmentId}/${task.id}`,
    ]);
    expect(childThreadInputsTitle(registry.get(inputs))).toBe("Subagent needs input");
    expect(listed()).toEqual([`${environmentId}/${parent.id}`]);

    registry.set(snapshotAtom(remoteEnvironmentId), {
      ...v2ShellSnapshot,
      threads: [
        {
          ...task,
          activityRunStatus: null,
          pendingRuntimeRequest: { ...approval, kind: "user_input" },
        },
        helper,
      ],
    });
    expect(registry.get(threads.threadShellAtom(parentRef))?.pendingSubagentRequest).toBeNull();
    expect(registry.get(inputs)).toEqual([]);

    // A task whose parent is gone there is listed on its own, as is one whose
    // parent's environment is switched off.
    registry.set(snapshotAtom(environmentId), { ...v2ShellSnapshot, threads: [] });
    expect(listed()).toEqual([`${remoteEnvironmentId}/${task.id}`]);
    registry.set(snapshotAtom(environmentId), { ...v2ShellSnapshot, threads: [parent] });
    expect(listed()).toEqual([`${environmentId}/${parent.id}`]);
    const catalog = registry.get(catalogValueAtom);
    const entries = new Map(catalog.entries);
    entries.set(environmentId, { ...entries.get(environmentId)!, enabled: false });
    registry.set(catalogValueAtom, { ...catalog, entries });
    expect(listed()).toEqual([`${remoteEnvironmentId}/${task.id}`]);
    disposeInputs();
    disposeParent();
    disposeNavigation();
    registry.dispose();
  });
});
