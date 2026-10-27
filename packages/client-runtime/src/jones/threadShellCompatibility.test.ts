import {
  EnvironmentId,
  RuntimeRequestId,
  RunId,
  ThreadId,
  type OrchestrationV2ShellSnapshot,
  type OrchestrationV2ThreadShell,
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
  it.each([
    [environmentId, remoteEnvironmentId],
    [remoteEnvironmentId, environmentId],
  ])("keeps delegated orphans accessible when %s has no parent row", (originId, targetId) => {
    const { registry, threads, snapshotAtom } = makeHarness([environmentId, remoteEnvironmentId]);
    const task = {
      ...v2ThreadShell,
      id: ThreadId.make("delegated-after-reload"),
      delegatedFrom: {
        environmentId: originId,
        threadId: ThreadId.make("missing-parent"),
        title: "Missing parent",
      },
    };
    const root = { ...v2ThreadShell, id: ThreadId.make("ordinary-root") };
    const fork = {
      ...v2ThreadShell,
      id: ThreadId.make("ordinary-fork"),
      lineage: {
        ...v2ThreadShell.lineage,
        parentThreadId: root.id,
        relationshipToParent: "fork" as const,
      },
    };
    // A genuinely absent parent must leave its delegated work reachable.
    // Snapshot reduction must retain existing parents rather than guessing
    // that every absent parent was omitted by pagination.
    registry.set(snapshotAtom(originId), { ...v2ShellSnapshot, threads: [] });
    registry.set(snapshotAtom(targetId), { ...v2ShellSnapshot, threads: [task, root, fork] });
    const dispose = registry.mount(threads.navigationThreadShellsAtom);
    try {
      expect(registry.get(threads.navigationThreadShellsAtom).map((thread) => thread.id)).toEqual([
        task.id,
        root.id,
        fork.id,
      ]);
    } finally {
      dispose();
      registry.dispose();
    }
  });

  it("keeps delegated quiet lists stable through streaming and exposes their parent membership", () => {
    const { registry, threads, snapshotAtom } = makeHarness([environmentId, remoteEnvironmentId]);
    const task = {
      ...v2ThreadShell,
      id: ThreadId.make("remote-stream"),
      latestRunId: RunId.make("stream-run"),
      activeRunId: RunId.make("stream-run"),
      status: "running" as const,
      itemCount: 1,
      delegatedFrom: { environmentId, threadId: v2ThreadShell.id, title: v2ThreadShell.title },
    };
    registry.set(snapshotAtom(environmentId), { ...v2ShellSnapshot, threads: [v2ThreadShell] });
    registry.set(snapshotAtom(remoteEnvironmentId), { ...v2ShellSnapshot, threads: [task] });
    const dispose = registry.mount(threads.quietThreadShellsAtom);
    const disposeMembership = registry.mount(threads.delegatedUnderParentAtom);
    try {
      const first = registry.get(threads.quietThreadShellsAtom);
      const membership = registry.get(threads.delegatedUnderParentAtom);
      expect(first).toHaveLength(2);
      expect(membership.size).toBe(1);
      registry.set(snapshotAtom(remoteEnvironmentId), {
        ...v2ShellSnapshot,
        threads: [{ ...task, itemCount: 4 }],
      });
      expect(registry.get(threads.quietThreadShellsAtom)).toBe(first);
      expect(registry.get(threads.delegatedUnderParentAtom)).toBe(membership);
      registry.set(snapshotAtom(remoteEnvironmentId), {
        ...v2ShellSnapshot,
        threads: [{ ...task, title: "Renamed remote task" }],
      });
      expect(registry.get(threads.quietThreadShellsAtom)).not.toBe(first);
      expect(
        registry.get(threads.quietThreadShellsAtom).find((thread) => thread.id === task.id)?.title,
      ).toBe("Renamed remote task");
    } finally {
      disposeMembership();
      dispose();
      registry.dispose();
    }
  });

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

describe("delegated requests remain reachable", () => {
  const approval = {
    id: RuntimeRequestId.make("delegate-request"),
    kind: "command" as const,
    createdAt: v2Now,
  };
  const delegate = (originId: EnvironmentId, parent: OrchestrationV2ThreadShell) => ({
    ...v2ThreadShell,
    id: ThreadId.make("delegate"),
    status: "waiting" as const,
    activityRunStatus: "waiting" as const,
    delegatedFrom: { environmentId: originId, threadId: parent.id, title: parent.title },
  });

  it.each([
    [environmentId, remoteEnvironmentId],
    [remoteEnvironmentId, environmentId],
  ])("reveals own and nested requests under a settled parent in %s", (originId, targetId) => {
    const { registry, threads, snapshotAtom } = makeHarness([originId, targetId]);
    const parent = { ...v2ThreadShell, settledOverride: "settled" as const };
    const task = delegate(originId, parent);
    const helper = {
      ...v2ThreadShell,
      id: ThreadId.make("local-helper"),
      status: "waiting" as const,
      activityRunStatus: "waiting" as const,
      lineage: {
        rootThreadId: task.id,
        parentThreadId: task.id,
        relationshipToParent: "subagent" as const,
      },
    };
    registry.set(snapshotAtom(originId), { ...v2ShellSnapshot, threads: [parent] });
    registry.set(snapshotAtom(targetId), { ...v2ShellSnapshot, threads: [task, helper] });
    const dispose = registry.mount(threads.navigationThreadShellsAtom);
    const listed = () =>
      registry
        .get(threads.navigationThreadShellsAtom)
        .map((row) => `${row.environmentId}/${row.id}`);
    try {
      expect(listed()).toEqual([`${originId}/${parent.id}`]);
      for (const kind of ["command", "user_input"] as const) {
        for (const nested of [false, true]) {
          registry.set(snapshotAtom(targetId), {
            ...v2ShellSnapshot,
            threads: [
              { ...task, pendingRuntimeRequest: nested ? null : { ...approval, kind } },
              { ...helper, pendingRuntimeRequest: nested ? { ...approval, kind } : null },
            ],
          });
          expect(listed()).toEqual([`${originId}/${parent.id}`, `${targetId}/${task.id}`]);
          expect(
            registry.get(threads.threadShellAtom({ environmentId: originId, threadId: parent.id }))
              ?.pendingSubagentRequest,
          ).toBe(kind === "command" ? "approval" : "user_input");
          registry.set(snapshotAtom(targetId), { ...v2ShellSnapshot, threads: [task, helper] });
          expect(listed()).toEqual([`${originId}/${parent.id}`]);
        }
      }
      registry.set(snapshotAtom(originId), {
        ...v2ShellSnapshot,
        threads: [{ ...parent, settledOverride: null }],
      });
      registry.set(snapshotAtom(targetId), {
        ...v2ShellSnapshot,
        threads: [{ ...task, pendingRuntimeRequest: approval }],
      });
      expect(listed()).toEqual([`${originId}/${parent.id}`]);
    } finally {
      dispose();
      registry.dispose();
    }
  });

  it.each(["settled", null] as const)(
    "walks an owning subagent to its %s root",
    (settledOverride) => {
      const { registry, threads, snapshotAtom } = makeHarness([environmentId, remoteEnvironmentId]);
      const root = { ...v2ThreadShell, settledOverride };
      const owner = {
        ...v2ThreadShell,
        id: ThreadId.make("owning-subagent"),
        lineage: {
          rootThreadId: root.id,
          parentThreadId: root.id,
          relationshipToParent: "subagent" as const,
        },
      };
      const task = delegate(environmentId, owner);
      registry.set(snapshotAtom(environmentId), { ...v2ShellSnapshot, threads: [root, owner] });
      registry.set(snapshotAtom(remoteEnvironmentId), {
        ...v2ShellSnapshot,
        threads: [{ ...task, pendingRuntimeRequest: approval }],
      });
      const dispose = registry.mount(threads.navigationThreadShellsAtom);
      try {
        expect(registry.get(threads.navigationThreadShellsAtom).map((row) => row.id)).toEqual(
          settledOverride === "settled" ? [root.id, task.id] : [root.id],
        );
        registry.set(snapshotAtom(remoteEnvironmentId), { ...v2ShellSnapshot, threads: [task] });
        expect(registry.get(threads.navigationThreadShellsAtom).map((row) => row.id)).toEqual([
          root.id,
        ]);
      } finally {
        dispose();
        registry.dispose();
      }
    },
  );

  it("keeps a delegate listed until its origin loads, without using colliding destination IDs", () => {
    const { registry, threads, snapshotAtom, catalogValueAtom } = makeHarness([
      environmentId,
      remoteEnvironmentId,
    ]);
    const parent = v2ThreadShell;
    const task = delegate(environmentId, parent);
    // A same-ID row at the destination is never the origin's parent.
    registry.set(snapshotAtom(environmentId), null);
    registry.set(snapshotAtom(remoteEnvironmentId), {
      ...v2ShellSnapshot,
      threads: [task, parent],
    });
    const dispose = registry.mount(threads.navigationThreadShellsAtom);
    const listed = () =>
      registry
        .get(threads.navigationThreadShellsAtom)
        .map((row) => `${row.environmentId}/${row.id}`);
    try {
      expect(listed()).toEqual([
        `${remoteEnvironmentId}/${task.id}`,
        `${remoteEnvironmentId}/${parent.id}`,
      ]);
      // An origin that never loads leaves the delegate reachable through later updates.
      registry.set(snapshotAtom(remoteEnvironmentId), {
        ...v2ShellSnapshot,
        threads: [{ ...task, title: "Still waiting for origin" }, parent],
      });
      expect(listed()).toContain(`${remoteEnvironmentId}/${task.id}`);
      registry.set(snapshotAtom(environmentId), { ...v2ShellSnapshot, threads: [parent] });
      expect(listed()).not.toContain(`${remoteEnvironmentId}/${task.id}`);
      const catalog = registry.get(catalogValueAtom);
      const setEnabled = (enabled: boolean) => {
        const entries = new Map(catalog.entries);
        entries.set(environmentId, { ...entries.get(environmentId)!, enabled });
        registry.set(catalogValueAtom, { ...catalog, entries });
      };
      setEnabled(false);
      expect(listed()).toContain(`${remoteEnvironmentId}/${task.id}`);
      setEnabled(true);
      expect(listed()).not.toContain(`${remoteEnvironmentId}/${task.id}`);
    } finally {
      dispose();
      registry.dispose();
    }
  });

  it.each(["settled", null] as const)(
    "reveals C in an A → B → C remote chain with A %s, without transitive badges",
    (settledOverride) => {
      const thirdId = EnvironmentId.make("third-environment");
      const { registry, threads, snapshotAtom } = makeHarness([
        environmentId,
        remoteEnvironmentId,
        thirdId,
      ]);
      const a = { ...v2ThreadShell, settledOverride };
      const b = {
        ...delegate(environmentId, a),
        id: ThreadId.make("b"),
        status: "idle" as const,
        activityRunStatus: null,
      };
      const c = {
        ...delegate(remoteEnvironmentId, b),
        id: ThreadId.make("c"),
        pendingRuntimeRequest: approval,
      };
      registry.set(snapshotAtom(environmentId), { ...v2ShellSnapshot, threads: [a] });
      registry.set(snapshotAtom(remoteEnvironmentId), { ...v2ShellSnapshot, threads: [b] });
      registry.set(snapshotAtom(thirdId), { ...v2ShellSnapshot, threads: [c] });
      const dispose = registry.mount(threads.navigationThreadShellsAtom);
      try {
        expect(registry.get(threads.navigationThreadShellsAtom).map((row) => row.id)).toEqual([
          a.id,
          c.id,
        ]);
        expect(
          registry.get(threads.threadShellAtom({ environmentId, threadId: a.id }))
            ?.pendingSubagentRequest,
        ).toBeNull();
        expect(
          registry.get(
            threads.threadShellAtom({ environmentId: remoteEnvironmentId, threadId: b.id }),
          )?.pendingSubagentRequest,
        ).toBe("approval");
        registry.set(snapshotAtom(thirdId), {
          ...v2ShellSnapshot,
          threads: [{ ...c, pendingRuntimeRequest: null }],
        });
        expect(registry.get(threads.navigationThreadShellsAtom).map((row) => row.id)).toEqual([
          a.id,
        ]);
      } finally {
        dispose();
        registry.dispose();
      }
    },
  );

  it.each(["missing", "null", "self-loop", "two-row-cycle"] as const)(
    "reveals asking work through a %s parent chain and re-hides it after response",
    (shape) => {
      const { registry, threads, snapshotAtom } = makeHarness([environmentId, remoteEnvironmentId]);
      const ownerId = ThreadId.make("owner");
      const middleId = ThreadId.make("middle");
      const owner = {
        ...v2ThreadShell,
        id: ownerId,
        lineage: {
          rootThreadId: v2ThreadShell.id,
          parentThreadId: shape === "null" ? null : shape === "self-loop" ? ownerId : middleId,
          relationshipToParent: "subagent" as const,
        },
      };
      const middle = {
        ...owner,
        id: middleId,
        lineage: { ...owner.lineage, parentThreadId: ownerId },
      };
      const task = delegate(environmentId, owner);
      registry.set(snapshotAtom(environmentId), {
        ...v2ShellSnapshot,
        threads: shape === "two-row-cycle" ? [owner, middle] : [owner],
      });
      registry.set(snapshotAtom(remoteEnvironmentId), {
        ...v2ShellSnapshot,
        threads: [{ ...task, pendingRuntimeRequest: approval }],
      });
      const dispose = registry.mount(threads.navigationThreadShellsAtom);
      try {
        expect(registry.get(threads.navigationThreadShellsAtom).map((row) => row.id)).toEqual([
          task.id,
        ]);
        registry.set(snapshotAtom(remoteEnvironmentId), { ...v2ShellSnapshot, threads: [task] });
        expect(registry.get(threads.navigationThreadShellsAtom)).toEqual([]);
      } finally {
        dispose();
        registry.dispose();
      }
    },
  );
});
