import { RegistryContext, useAtomValue } from "@effect/atom-react";
import {
  EnvironmentId,
  ProjectId,
  ProviderInstanceId,
  MessageId,
  RuntimeRequestId,
  ThreadId,
  type AuthSessionState,
  type OrchestrationV2ShellSnapshot,
  type OrchestrationV2ThreadShell,
  type ScopedThreadRef,
} from "@t3tools/contracts";
import * as Cause from "effect/Cause";
import * as DateTime from "effect/DateTime";
import * as Option from "effect/Option";
import { AsyncResult, Atom } from "effect/reactivity";
import { Profiler, StrictMode, memo, useLayoutEffect } from "react";
import { expect, it, vi } from "vite-plus/test";
import { act, render } from "../../../test/render";
import { PrimaryConnectionTarget } from "@t3tools/client-runtime/connection";
import {
  createEnvironmentThreadShellAtoms,
  isShownUnderParent,
} from "@t3tools/client-runtime/state/threads";
import type { EnvironmentThreadShell } from "@t3tools/client-runtime/state/models";

vi.mock("../../connection/runtime", () => ({ connectionAtomRuntime: {} }));
vi.mock("@t3tools/client-runtime/state/session", async () => {
  const { Atom, AsyncResult } = await import("effect/reactivity");
  return {
    createEnvironmentSessionAtoms: () => ({
      sessionStateAtom: Atom.family((_id: string) =>
        Atom.make<Result>(AsyncResult.initial<AuthSessionState>()),
      ),
    }),
  };
});
import { appAtomRegistry as registry } from "../../rpc/atomRegistry";
import {
  environmentSession,
  readEnvironmentScopeAccess,
  useEnvironmentScope,
} from "../../state/session";
import { usePendingSidebarThread, usePendingSidebarThreads } from "./sidebar";
import { pendingSends } from "./store";
import type { ChatMessage } from "../../types";

const v2Now = DateTime.makeUnsafe("2026-06-20T00:00:00.000Z");
const v2ThreadShell: OrchestrationV2ThreadShell = {
  id: ThreadId.make("thread-v2"),
  projectId: ProjectId.make("project-v2"),
  title: "Thread",
  providerInstanceId: ProviderInstanceId.make("codex"),
  modelSelection: { instanceId: ProviderInstanceId.make("codex"), model: "gpt-5.4" },
  runtimeMode: "full-access",
  interactionMode: "default",
  branch: null,
  worktreePath: null,
  activeProviderThreadId: null,
  lineage: {
    rootThreadId: ThreadId.make("thread-v2"),
    parentThreadId: null,
    relationshipToParent: null,
  },
  forkedFrom: null,
  createdBy: "user",
  creationSource: "web",
  latestRunId: null,
  activeRunId: null,
  status: "idle",
  pendingRuntimeRequest: null,
  latestVisibleMessage: null,
  latestUserMessageAt: null,
  hasActionableProposedPlan: false,
  itemCount: 0,
  visibleItemCount: 0,
  createdAt: v2Now,
  updatedAt: v2Now,
  archivedAt: null,
  settledOverride: null,
  settledAt: null,
  lastVisitedAt: null,
  deletedAt: null,
};

const v2ShellSnapshot: OrchestrationV2ShellSnapshot = {
  schemaVersion: 1,
  snapshotSequence: 0,
  projects: [],
  threads: [],
  archivedThreads: [],
};

const A = EnvironmentId.make("pending-sidebar-a");
const B = EnvironmentId.make("pending-sidebar-b");
const IDS = Object.freeze(Array.from({ length: 8 }, (_, i) => ThreadId.make(`row-${i}`)));
const REFS = Object.freeze(
  [A, B].flatMap((environmentId) =>
    IDS.map((threadId) => Object.freeze({ environmentId, threadId })),
  ),
);
const keyOf = (ref: ScopedThreadRef) => JSON.stringify([ref.environmentId, ref.threadId]);
const ALL_KEYS = REFS.map(keyOf).sort();
const TARGET = REFS[0]!;
const TARGET_KEY = keyOf(TARGET);
const T0 = "2026-01-01T12:00:00.000Z";
const T1 = "2026-01-01T12:00:01.000Z";
const GRANTED: AuthSessionState = Object.freeze({
  authenticated: true,
  auth: {
    policy: "remote-reachable",
    bootstrapMethods: [],
    sessionMethods: [],
    sessionCookieName: "synthetic",
  },
  scopes: ["orchestration:operate"],
} satisfies AuthSessionState);
type Result = AsyncResult.AsyncResult<AuthSessionState, unknown>;
const success = (value: AuthSessionState, waiting = false): Result =>
  Object.freeze({ ...AsyncResult.success(value, { waiting }), timestamp: 0 });
const OK = success(GRANTED);
const WAITING = success(GRANTED, true);
const FAILED: Result = Object.freeze({
  ...AsyncResult.failure<AuthSessionState, unknown>(Cause.fail("synthetic-retry"), {
    waiting: true,
  }),
  previousSuccess: Option.some({ ...AsyncResult.success(GRANTED), timestamp: 0 }),
});
// This cast names the writable source supplied by the explicit producer test double.
const sessionSource = (id: EnvironmentId) =>
  environmentSession.sessionStateAtom(id) as Atom.Writable<Result, Result>;
const wireRow = (id: ThreadId, title: string = id): OrchestrationV2ThreadShell =>
  Object.freeze({
    ...v2ThreadShell,
    id,
    title,
    lineage: { rootThreadId: id, parentThreadId: null, relationshipToParent: null },
  });
const WIRES = Object.freeze(IDS.map((id) => wireRow(id)));
const snapshot = (
  threads: ReadonlyArray<OrchestrationV2ThreadShell>,
): OrchestrationV2ShellSnapshot =>
  Object.freeze({ ...v2ShellSnapshot, threads: Object.freeze([...threads]) });
const message = (id: string, createdAt: string): ChatMessage =>
  Object.freeze({
    id: MessageId.make(id),
    role: "user",
    text: "synthetic",
    runId: null,
    createdAt,
    updatedAt: createdAt,
    streaming: false,
  });
type InputLedger = {
  row: EnvironmentThreadShell | undefined;
  all: readonly EnvironmentThreadShell[];
};
function InputHookCarrier(props: {
  thread: EnvironmentThreadShell;
  threads: readonly EnvironmentThreadShell[];
  onCommit: (row: EnvironmentThreadShell, all: readonly EnvironmentThreadShell[]) => void;
}) {
  const row = usePendingSidebarThread(props.thread);
  const all = usePendingSidebarThreads(props.threads);
  useLayoutEffect(() => {
    props.onCommit(row, all);
  });
  return null;
}

type Graph = ReturnType<typeof createEnvironmentThreadShellAtoms>;
type Ledger = {
  marks: Set<string>;
  rows: Map<string, { allowed: boolean; row: EnvironmentThreadShell }>;
  all: readonly EnvironmentThreadShell[];
  visible: readonly EnvironmentThreadShell[];
  listMarks: number;
  profilerCommits: number;
};
const RowHookCarrier = memo(function RowHookCarrier(props: {
  graph: Graph;
  refValue: ScopedThreadRef;
  ledger: Ledger;
}) {
  const raw = useAtomValue(props.graph.threadShellAtom(props.refValue));
  if (raw === null) throw new Error("Frozen carrier membership lost a shell");
  const row = usePendingSidebarThread(raw);
  const allowed = useEnvironmentScope(props.refValue.environmentId, "orchestration:operate");
  useLayoutEffect(() => {
    const key = keyOf(props.refValue);
    props.ledger.marks.add(key);
    props.ledger.rows.set(key, { row, allowed });
  });
  return (
    <span data-row={keyOf(props.refValue)} data-allowed={String(allowed)}>
      {row.title}
    </span>
  );
});
function ListHookCarrier(props: {
  graph: Graph;
  onCommit: (
    all: readonly EnvironmentThreadShell[],
    visible: readonly EnvironmentThreadShell[],
  ) => void;
}) {
  const quiet = useAtomValue(props.graph.quietThreadShellsAtom);
  const all = usePendingSidebarThreads(quiet);
  const under = useAtomValue(props.graph.delegatedUnderParentAtom);
  const visible = all.filter(
    (row) =>
      !isShownUnderParent(
        { environmentId: row.environmentId, threadId: row.id },
        row.source.lineage,
        under,
      ),
  );
  useLayoutEffect(() => {
    props.onCommit(all, visible);
  });
  return <span data-list-count={visible.length} />;
}

it("isolates pending sidebar commits and preserves permission and delegated visibility", async () => {
  expect(pendingSends.getSnapshot()).toEqual([]); // Refuse pre-existing state; never clear it.
  const snapshotAtom = Atom.family((_id: EnvironmentId) =>
    Atom.make<OrchestrationV2ShellSnapshot | null>(null),
  );
  const catalogValueAtom = Atom.make({
    isReady: true,
    entries: new Map(
      [A, B].map((environmentId) => [
        environmentId,
        {
          enabled: true,
          profile: Option.none(),
          target: new PrimaryConnectionTarget({
            environmentId,
            label: "Synthetic",
            httpBaseUrl: "https://example.test",
            wsBaseUrl: "wss://example.test",
          }),
        },
      ]),
    ),
  });
  const graph = createEnvironmentThreadShellAtoms({ snapshotAtom, catalogValueAtom });
  registry.set(snapshotAtom(A), snapshot(WIRES));
  registry.set(snapshotAtom(B), snapshot(WIRES));
  registry.set(sessionSource(A), OK);
  registry.set(sessionSource(B), OK);
  const ledger: Ledger = {
    marks: new Set(),
    rows: new Map(),
    all: [],
    visible: [],
    listMarks: 0,
    profilerCommits: 0,
  };
  const ownedKeys = new Set<string>();
  let screen: Awaited<ReturnType<typeof render>> | undefined;
  const reset = () => {
    ledger.marks.clear();
    ledger.listMarks = 0;
    ledger.profilerCommits = 0;
  };
  const admit = (ref: ScopedThreadRef, msg: ChatMessage) => {
    const key = pendingSends.admit(ref.environmentId, ref.threadId, msg);
    ownedKeys.add(key);
    return key;
  };
  try {
    screen = await render(
      <StrictMode>
        <RegistryContext.Provider value={registry}>
          <Profiler
            id="pending-send-hook-graph"
            onRender={() => {
              ledger.profilerCommits++;
            }}
          >
            {REFS.map((refValue) => (
              <RowHookCarrier
                key={keyOf(refValue)}
                graph={graph}
                refValue={refValue}
                ledger={ledger}
              />
            ))}
            <ListHookCarrier
              graph={graph}
              onCommit={(all, visible) => {
                ledger.listMarks++;
                ledger.all = all;
                ledger.visible = visible;
              }}
            />
          </Profiler>
        </RegistryContext.Provider>
      </StrictMode>,
      { router: false },
    );
    expect([...ledger.rows.keys()].sort()).toEqual(ALL_KEYS);
    expect(ledger.visible).toHaveLength(16);
    expect([...ledger.rows.values()].every((r) => r.allowed)).toBe(true);
    // Count committed hook consumers; render attempts may be repeated by StrictMode.
    const pendingStep = async (
      name: string,
      mutate: () => void,
      changed: readonly string[],
      listChanged: boolean,
    ) => {
      const quiet = registry.get(graph.quietThreadShellsAtom);
      const raw = REFS.map((ref) => registry.get(graph.threadShellAtom(ref)));
      const sessionA = registry.get(sessionSource(A));
      const beforeAll = ledger.all;
      const beforeEntries = pendingSends.getSnapshot();
      reset();
      await act(mutate);
      const observed = [...ledger.marks].sort();
      expect(pendingSends.getSnapshot()).not.toBe(beforeEntries);
      expect(registry.get(graph.quietThreadShellsAtom)).toBe(quiet);
      REFS.forEach((ref, i) => expect(registry.get(graph.threadShellAtom(ref))).toBe(raw[i]));
      expect(registry.get(sessionSource(A))).toBe(sessionA);
      expect(observed, name).toEqual([...changed].sort());
      if (listChanged) {
        expect(ledger.listMarks, name).toBeGreaterThan(0);
      } else {
        expect(ledger.listMarks, name).toBe(0);
        expect(ledger.all, name).toBe(beforeAll);
      }
      if (observed.length || listChanged) expect(ledger.profilerCommits, name).toBeGreaterThan(0);
      else expect(ledger.profilerCommits, name).toBe(0);
    };
    const target = () => ledger.rows.get(TARGET_KEY)!.row;
    const baseTarget = target();
    let first = "";
    await pendingStep(
      "admit-one",
      () => {
        first = admit(TARGET, message("send-first", T0));
      },
      [TARGET_KEY],
      true,
    );
    expect(target().runtime?.status).toBe("running");
    expect(target().latestUserMessageAt).toBe(T0);
    const firstOverlay = target();
    let sameTime = "";
    await pendingStep(
      "same-time-working-send",
      () => {
        sameTime = admit(TARGET, message("send-same-time", T0));
      },
      [],
      false,
    );
    expect(target()).toBe(firstOverlay);
    await pendingStep(
      "remove-same-time-send",
      () => {
        pendingSends.dismiss(sameTime);
      },
      [],
      false,
    );
    let accepted = 0;
    await pendingStep(
      "acceptance-callback-only",
      () => {
        pendingSends.whenAccepted(A, TARGET.threadId, MessageId.make("send-first"), () => {
          accepted++;
        });
      },
      [],
      false,
    );
    await pendingStep(
      "issuing-only",
      () => {
        pendingSends.issuing(first);
      },
      [],
      false,
    );
    expect(target().latestUserMessageAt).toBe(T0);
    await pendingStep(
      "accepted-metadata-only",
      () => {
        pendingSends.finish(first, true);
      },
      [],
      false,
    );
    expect(target().runtime?.status).toBe("running");
    expect(accepted).toBe(1);
    let newer = "";
    await pendingStep(
      "newer-working-send",
      () => {
        newer = admit(TARGET, message("send-newer", T1));
      },
      [TARGET_KEY],
      true,
    );
    expect(target().latestUserMessageAt).toBe(T1);
    let last = "";
    await pendingStep(
      "last-entry-with-earlier-time",
      () => {
        last = admit(TARGET, message("send-last", T0));
      },
      [TARGET_KEY],
      true,
    );
    expect(target().latestUserMessageAt).toBe(T0);
    await pendingStep(
      "last-entry-becomes-inactive",
      () => {
        pendingSends.failBeforeDispatch(A, TARGET.threadId, MessageId.make("send-last"));
      },
      [TARGET_KEY],
      true,
    );
    expect(target().latestUserMessageAt).toBe(T1);
    await pendingStep(
      "remove-inactive-last-entry",
      () => {
        pendingSends.dismiss(last);
      },
      [],
      false,
    );
    await pendingStep(
      "newer-fails-before-dispatch",
      () => {
        pendingSends.failBeforeDispatch(A, TARGET.threadId, MessageId.make("send-newer"));
      },
      [TARGET_KEY],
      true,
    );
    expect(target().latestUserMessageAt).toBe(T0);
    await pendingStep(
      "dismiss-inactive-newer",
      () => {
        pendingSends.dismiss(newer);
      },
      [],
      false,
    );
    await pendingStep(
      "dismiss-last-working",
      () => {
        pendingSends.dismiss(first);
      },
      [TARGET_KEY],
      true,
    );
    expect(target()).toBe(baseTarget);

    // Same thread ID in the other environment must not receive A's overlay.
    expect(
      ledger.rows.get(keyOf({ environmentId: B, threadId: TARGET.threadId }))!.row.runtime,
    ).toBeNull();
    const newRef = Object.freeze({ environmentId: B, threadId: ThreadId.make("new-pending") });
    const newWire = wireRow(newRef.threadId, "Server promoted title");
    let localKey = "";
    await pendingStep(
      "admit-unlisted",
      () => {
        localKey = admit(newRef, message("send-local", T0));
      },
      [],
      false,
    );
    const draft = {
      ...baseTarget,
      environmentId: B,
      id: newRef.threadId,
      title: "Local pending title",
      source: { ...newWire, title: "Local pending title" },
    };
    await pendingStep(
      "attach-local-shell",
      () => {
        pendingSends.attachThread(B, newRef.threadId, draft);
      },
      [],
      true,
    );
    expect(
      ledger.all.filter((r) => r.environmentId === B && r.id === newRef.threadId),
    ).toHaveLength(1);
    expect(ledger.all.find((r) => r.id === newRef.threadId)?.title).toBe("Local pending title");
    await act(() => {
      registry.set(snapshotAtom(B), snapshot([...WIRES, newWire]));
    });
    expect(
      ledger.all.filter((r) => r.environmentId === B && r.id === newRef.threadId),
    ).toHaveLength(1);
    expect(ledger.all.find((r) => r.id === newRef.threadId)?.title).toBe("Server promoted title");
    await pendingStep(
      "remove-promoted-pending",
      () => {
        pendingSends.dismiss(localKey);
      },
      [],
      true,
    );

    const orphanRef = { environmentId: B, threadId: ThreadId.make("local-only") };
    let orphanKey = "";
    await pendingStep(
      "admit-local-only",
      () => {
        orphanKey = admit(orphanRef, message("send-orphan", T0));
      },
      [],
      false,
    );
    const orphan = { ...draft, id: orphanRef.threadId, source: wireRow(orphanRef.threadId) };
    await pendingStep(
      "attach-local-only",
      () => {
        pendingSends.attachThread(B, orphanRef.threadId, orphan);
      },
      [],
      true,
    );
    await pendingStep(
      "local-only-becomes-inactive",
      () => {
        pendingSends.failBeforeDispatch(B, orphanRef.threadId, MessageId.make("send-orphan"));
      },
      [],
      true,
    );
    expect(ledger.all.find((row) => row.id === orphanRef.threadId)).toBe(orphan);
    await pendingStep(
      "remove-inactive-local-only",
      () => {
        pendingSends.dismiss(orphanKey);
      },
      [],
      true,
    );
    expect(ledger.all.some((row) => row.id === orphanRef.threadId)).toBe(false);

    // Every intermediate access state is asserted, never just equal endpoints.
    const permissionStep = async (
      name: string,
      value: Result,
      status: string,
      allowed: boolean,
    ) => {
      reset();
      await act(() => {
        registry.set(sessionSource(A), value);
      });
      expect(readEnvironmentScopeAccess(A, "orchestration:operate").status, name).toBe(status);
      for (const ref of REFS) {
        expect(ledger.rows.get(keyOf(ref))!.allowed, name).toBe(
          ref.environmentId === A ? allowed : true,
        );
      }
      expect(ledger.listMarks, name).toBe(0);
    };
    await permissionStep("initial-is-checking", AsyncResult.initial(), "checking", false);
    await permissionStep("grant-before-refresh", OK, "granted", true);
    await permissionStep("refresh-stale-success-checking", WAITING, "checking", false);
    await permissionStep("retry-failure-with-previous-success", FAILED, "check_failed", false);
    await permissionStep(
      "settled-failure-with-previous-success",
      { ...FAILED, waiting: false },
      "check_failed",
      false,
    );
    await permissionStep(
      "signed-out",
      success({ ...GRANTED, authenticated: false }),
      "signed_out",
      false,
    );
    await permissionStep(
      "scope-revoked",
      success({ ...GRANTED, scopes: [] }),
      "scope_missing",
      false,
    );
    await permissionStep("regrant", success(GRANTED), "granted", true);

    // Request-aware delegated visibility uses the real Atom graph.
    const parent = WIRES[0]!;
    const taskId = ThreadId.make("linked-task");
    const childId = ThreadId.make("linked-task-child");
    const task: OrchestrationV2ThreadShell = {
      ...wireRow(taskId),
      status: "running",
      activityRunStatus: "running",
      delegatedFrom: { environmentId: A, threadId: parent.id, title: parent.title },
    };
    const child: OrchestrationV2ThreadShell = {
      ...wireRow(childId),
      creationSource: "mcp",
      status: "running",
      activityRunStatus: "running",
      lineage: { rootThreadId: taskId, parentThreadId: taskId, relationshipToParent: "subagent" },
      pendingRuntimeRequest: {
        id: RuntimeRequestId.make("linked-question"),
        kind: "user_input",
        createdAt: v2Now,
      },
    };
    const taskRef = { environmentId: B, threadId: taskId };
    const visibleTask = () => ledger.visible.some((r) => r.environmentId === B && r.id === taskId);
    const hiddenTask = () =>
      isShownUnderParent(taskRef, task.lineage, registry.get(graph.delegatedUnderParentAtom));
    await act(() => {
      registry.set(snapshotAtom(B), snapshot([...WIRES, newWire, task, child]));
    });
    expect(hiddenTask()).toBe(true);
    expect(visibleTask()).toBe(false);
    expect(registry.get(graph.threadShellAtom(TARGET))?.pendingSubagentRequest).toBe("user_input");
    const settledParent = { ...parent, settledOverride: "settled" as const };
    await act(() => {
      registry.set(snapshotAtom(A), snapshot([settledParent, ...WIRES.slice(1)]));
    });
    expect(hiddenTask()).toBe(false);
    expect(visibleTask()).toBe(true);
    await act(() => {
      registry.set(snapshotAtom(B), snapshot([...WIRES, newWire, task]));
    });
    expect(hiddenTask()).toBe(true); // No reachable question: the loaded parent still owns it.
    expect(visibleTask()).toBe(false);
    const missingParentTask = {
      ...task,
      delegatedFrom: { ...task.delegatedFrom!, threadId: ThreadId.make("absent-parent") },
    };
    await act(() => {
      registry.set(snapshotAtom(B), snapshot([...WIRES, newWire, missingParentTask]));
    });
    expect(hiddenTask()).toBe(false);
    expect(visibleTask()).toBe(true);

    // The same mounted hooks must replace their cache when base props or scope change.
    const input: InputLedger = { row: undefined, all: [] };
    const recordInput = (row: EnvironmentThreadShell, all: readonly EnvironmentThreadShell[]) => {
      input.row = row;
      input.all = all;
    };
    const baseA = { ...baseTarget, title: "Base A" };
    const baseB = { ...baseA, environmentId: B, title: "Base B" };
    let aKey = "";
    let bKey = "";
    await act(() => {
      aKey = admit(TARGET, message("input-a", T0));
      bKey = admit({ ...TARGET, environmentId: B }, message("input-b", T1));
    });
    await screen.rerender(
      <StrictMode>
        <InputHookCarrier thread={baseA} threads={[baseA]} onCommit={recordInput} />
      </StrictMode>,
    );
    expect(input.row?.latestUserMessageAt).toBe(T0);
    expect(input.all[0]?.latestUserMessageAt).toBe(T0);
    const changedBase = {
      ...baseA,
      title: "Changed base",
      hasPendingApprovals: true,
      blockedOnThreadWait: true,
      runtime: { ...firstOverlay.runtime!, lastError: "Preserved base error", updatedAt: T1 },
    };
    await screen.rerender(
      <StrictMode>
        <InputHookCarrier thread={changedBase} threads={[changedBase]} onCommit={recordInput} />
      </StrictMode>,
    );
    expect(input.row?.title).toBe("Changed base");
    expect(input.row?.hasPendingApprovals).toBe(true);
    expect(input.row?.blockedOnThreadWait).toBe(false);
    expect(input.row?.runtime?.lastError).toBe("Preserved base error");
    expect(input.all[0]?.title).toBe("Changed base");
    await screen.rerender(
      <StrictMode>
        <InputHookCarrier thread={baseB} threads={[baseB]} onCommit={recordInput} />
      </StrictMode>,
    );
    expect(input.row?.environmentId).toBe(B);
    expect(input.row?.latestUserMessageAt).toBe(T1);
    expect(input.all[0]?.title).toBe("Base B");
    await act(() => {
      pendingSends.dismiss(bKey);
    });
    expect(input.row).toBe(baseB);
    expect(input.all[0]).toBe(baseB);
    await screen.rerender(
      <StrictMode>
        <InputHookCarrier thread={changedBase} threads={[changedBase]} onCommit={recordInput} />
      </StrictMode>,
    );
    expect(input.row?.latestUserMessageAt).toBe(T0);
    await act(() => {
      pendingSends.dismiss(aKey);
    });
    expect(input.row).toBe(changedBase);
    expect(input.row?.blockedOnThreadWait).toBe(true);
    expect(input.all[0]).toBe(changedBase);
  } finally {
    try {
      if (screen) await screen[Symbol.asyncDispose]();
    } finally {
      for (const key of ownedKeys) pendingSends.dismiss(key);
      registry.dispose();
    }
  }
});
