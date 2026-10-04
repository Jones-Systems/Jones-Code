import {
  EMPTY_ENVIRONMENT_THREAD_STATE,
  type EnvironmentThreadState,
} from "@t3tools/client-runtime/state/threads";
import {
  EnvironmentId,
  MessageId,
  ProviderInstanceId,
  ProviderSessionId,
  ProviderThreadId,
  ProjectId,
  RunId,
  ThreadId,
  type OrchestrationV2ThreadShell,
  type OrchestrationV2ThreadRuntimeObservationResult,
  type OrchestrationV2OperatingCountsResult,
  type OrchestrationV2ShellSnapshot,
} from "@t3tools/contracts";
import { makeThreadFixture, makeThreadProjectionFixture } from "../test-fixtures";
import * as Option from "effect/Option";
import { AsyncResult, Atom, AtomRegistry } from "effect/unstable/reactivity";
import { describe, expect, it } from "vite-plus/test";

import {
  createOperatingCountAtom,
  createRunningThreadKeepAliveAtom,
  createThreadOperatingStatesAtom,
} from "./threads";
import { scopedThreadKey } from "@t3tools/client-runtime/environment";

const LOCAL = EnvironmentId.make("local");
const REMOTE = EnvironmentId.make("remote");

describe("current web Operating observations", () => {
  it("follows the active owner across selected-account changes and releases archived rows", () => {
    const registry = AtomRegistry.make();
    const owner = ProviderThreadId.make("active-owner");
    const thread = makeThreadFixture({ environmentId: LOCAL, activeProviderThreadId: owner });
    const threads = Atom.make([thread]);
    const current = Atom.make(true);
    const result: OrchestrationV2ThreadRuntimeObservationResult = {
      threadId: thread.id,
      observation: {
        status: "monitoring",
        observedAt: "2026-10-03T02:32:29Z",
        binding: {
          threadId: thread.id,
          providerThreadId: owner,
          providerSessionId: ProviderSessionId.make("resident-session"),
          instanceId: ProviderInstanceId.make("active-account"),
          runtimeGeneration: "generation-1",
        },
      },
    };
    const observation = Atom.make(AsyncResult.success(result));
    const states = createThreadOperatingStatesAtom({
      threadsAtom: threads,
      isCurrentAtom: () => current,
      observationAtom: () => observation,
    });
    const key = scopedThreadKey({ environmentId: LOCAL, threadId: thread.id });
    const release = registry.mount(states);
    try {
      expect(registry.get(states).get(key)).toMatchObject({
        operating: true,
        workstreamRunning: false,
        backgroundDisplay: "monitoring",
      });
      registry.set(threads, [
        {
          ...thread,
          modelSelection: {
            instanceId: ProviderInstanceId.make("next-account"),
            model: "next-model",
          },
        },
      ]);
      expect(registry.get(states).get(key)?.operating).toBe(true);
      registry.set(observation, AsyncResult.success(result, { waiting: true }));
      expect(registry.get(states).get(key)).toMatchObject({
        operating: false,
        backgroundStatus: "unknown",
      });
      registry.set(observation, AsyncResult.success(result));
      registry.set(threads, [
        { ...thread, activeProviderThreadId: ProviderThreadId.make("replacement-owner") },
      ]);
      expect(registry.get(states).get(key)?.operating).toBe(false);
      registry.set(threads, [{ ...thread, archivedAt: "2026-10-03T02:33:00Z" }]);
      expect(registry.get(states).has(key)).toBe(false);
    } finally {
      release();
      registry.dispose();
    }
  });

  it("does not confirm cached foreground work or turn unknown background into monitoring", () => {
    const registry = AtomRegistry.make();
    const thread = makeThreadFixture({ environmentId: LOCAL });
    const running = {
      ...thread,
      runtime: {
        status: "running" as const,
        activeRunId: null,
        providerInstanceId: thread.providerInstanceId,
        providerName: null,
        lastError: null,
        updatedAt: thread.updatedAt,
      },
    };
    const threads = Atom.make([running]);
    const current = Atom.make(false);
    const observation = Atom.make(
      AsyncResult.success<OrchestrationV2ThreadRuntimeObservationResult>({
        threadId: thread.id,
        observation: { status: "unknown", reason: "runtime_not_observed" },
      }),
    );
    const states = createThreadOperatingStatesAtom({
      threadsAtom: threads,
      isCurrentAtom: () => current,
      observationAtom: () => observation,
    });
    const key = scopedThreadKey({ environmentId: LOCAL, threadId: thread.id });
    const release = registry.mount(states);
    try {
      expect(registry.get(states).get(key)).toMatchObject({
        operating: false,
        workstreamRunning: false,
        backgroundStatus: "unknown",
      });
      registry.set(current, true);
      expect(registry.get(states).get(key)).toMatchObject({
        operating: true,
        workstreamRunning: true,
        backgroundDisplay: null,
      });
      registry.set(threads, [{ ...running, hasPendingApprovals: true }]);
      expect(registry.get(states).get(key)).toMatchObject({
        foregroundAttention: "approval",
        operating: true,
      });
    } finally {
      release();
      registry.dispose();
    }
  });

  it("reads the exact project aggregate and makes refreshing, outdated, and absent counts unavailable", () => {
    const registry = AtomRegistry.make();
    const request = { environmentId: LOCAL, projectId: ProjectId.make("selected-project") };
    const requests = Atom.make([request]);
    const current = Atom.make(true);
    const snapshot = Atom.make<OrchestrationV2ShellSnapshot | null>({
      schemaVersion: 2,
      snapshotSequence: 10,
      projects: [],
      threads: [],
      archivedThreads: [],
    });
    const counts: OrchestrationV2OperatingCountsResult = {
      total: 7,
      operating: 5,
      foregroundWaitingApproval: 1,
      foregroundWaitingInput: 1,
      foregroundWaitingPlan: 0,
      backgroundOperating: 3,
      backgroundUnknown: 2,
      snapshotSequence: 10,
      observedAt: "2026-10-03T02:32:29Z",
      backgroundSampledAt: "2026-10-03T02:32:29Z",
    };
    const query = Atom.make<AsyncResult.AsyncResult<OrchestrationV2OperatingCountsResult>>(
      AsyncResult.success(counts),
    );
    const seen: Array<typeof request> = [];
    const count = createOperatingCountAtom({
      requestsAtom: requests,
      isCurrentAtom: () => current,
      snapshotAtom: () => snapshot,
      countsAtom: (ref) => {
        seen.push(ref as typeof request);
        return query;
      },
    });
    const release = registry.mount(count);
    try {
      expect(registry.get(count)).toBe(5);
      expect(seen).toEqual([request]);
      registry.set(query, AsyncResult.success(counts, { waiting: true }));
      expect(registry.get(count)).toBeNull();
      registry.set(query, AsyncResult.success({ ...counts, snapshotSequence: 9 }));
      expect(registry.get(count)).toBeNull();
      registry.set(query, AsyncResult.initial());
      expect(registry.get(count)).toBeNull();
      registry.set(query, AsyncResult.success({ ...counts, operating: 0 }));
      expect(registry.get(count)).toBe(0);
      registry.set(current, false);
      expect(registry.get(count)).toBeNull();
    } finally {
      release();
      registry.dispose();
    }
  });
});

type Status = "running" | "starting" | "idle";
function shell(id: string, status: Status | null) {
  return { id: ThreadId.make(id), status: status ?? "idle" } satisfies Pick<
    OrchestrationV2ThreadShell,
    "id" | "status"
  >;
}
function detail(id: string, status: Status, overrides: Partial<EnvironmentThreadState> = {}) {
  const projection = makeThreadProjectionFixture();
  const threadId = ThreadId.make(id);
  const thread = {
    ...projection,
    thread: { ...projection.thread, id: threadId },
    runs:
      status === "idle"
        ? []
        : [
            {
              id: RunId.make(`run-${id}`),
              threadId,
              ordinal: 1,
              providerInstanceId: projection.thread.providerInstanceId,
              modelSelection: projection.thread.modelSelection,
              providerThreadId: null,
              userMessageId: MessageId.make(`message-${id}`),
              rootNodeId: null,
              activeAttemptId: null,
              status,
              requestedAt: projection.updatedAt,
              startedAt: null,
              completedAt: null,
              checkpointId: null,
              contextHandoffId: null,
            },
          ],
  };
  return AsyncResult.success<EnvironmentThreadState>({
    ...EMPTY_ENVIRONMENT_THREAD_STATE,
    status: "live",
    data: Option.some(thread),
    ...overrides,
  });
}

function makeHarness() {
  // Registry cleanup runs only on `flush`, like the real deferred task.
  const tasks: Array<() => void> = [];
  const registry = AtomRegistry.make({
    scheduleTask: (task) => {
      tasks.push(task);
      return () => {};
    },
  });
  const flush = () => {
    for (let task = tasks.shift(); task !== undefined; task = tasks.shift()) task();
  };
  const environmentIds = Atom.make<ReadonlyArray<EnvironmentId>>([LOCAL, REMOTE]).pipe(
    Atom.keepAlive,
  );
  const threads = Atom.family((_environmentId: EnvironmentId) =>
    Atom.make<ReadonlyArray<ReturnType<typeof shell>>>([]).pipe(Atom.keepAlive),
  );
  // Stand-ins for the thread state atoms. Each one lives only while mounted,
  // as the real stream does.
  const keys = new Set<string>();
  const states = Atom.family((_key: string) =>
    Atom.make<AsyncResult.AsyncResult<EnvironmentThreadState>>(
      AsyncResult.success(EMPTY_ENVIRONMENT_THREAD_STATE),
    ),
  );
  const stateAtom = (environmentId: EnvironmentId, threadId: string) => {
    const key = `${environmentId}:${threadId}`;
    keys.add(key);
    return states(key);
  };
  const keepAlive = createRunningThreadKeepAliveAtom({
    environmentIdsAtom: environmentIds,
    threadsAtom: threads,
    stateAtom,
  });
  registry.mount(keepAlive);
  return {
    registry,
    environmentIds,
    threads,
    stateAtom,
    keepAlive,
    openStreams: () => {
      flush();
      return [...keys].filter((key) => registry.getNodes().has(states(key))).toSorted();
    },
  };
}

describe("createRunningThreadKeepAliveAtom", () => {
  it("keeps running threads open across shell updates and thread view visits", () => {
    const h = makeHarness();
    h.registry.set(h.threads(LOCAL), [shell("a", "running"), shell("b", "idle"), shell("c", null)]);
    h.registry.set(h.threads(REMOTE), [shell("d", "starting")]);
    expect(h.openStreams()).toEqual(["local:a", "remote:d"]);

    // A thread view that comes and goes shares the kept stream.
    const live = detail("a", "running");
    h.registry.set(h.stateAtom(LOCAL, "a"), live);
    h.registry.mount(h.stateAtom(LOCAL, "a"))();

    // A shell update that starts or stops nothing does not rebuild the set.
    const kept = h.registry.get(h.keepAlive);
    h.registry.set(h.threads(LOCAL), [shell("a", "running"), shell("b", "idle")]);
    expect(h.registry.get(h.keepAlive)).toBe(kept);
    expect(h.openStreams()).toEqual(["local:a", "remote:d"]);
    expect(h.registry.get(h.stateAtom(LOCAL, "a"))).toBe(live);
  });

  it("holds a stopped thread until its own stream is live and shows the stop", () => {
    const h = makeHarness();
    h.registry.set(h.threads(LOCAL), [
      shell("a", "running"),
      shell("b", "running"),
      shell("c", "running"),
    ]);
    // "b" has not loaded yet. "c" hit a stream error.
    h.registry.set(h.stateAtom(LOCAL, "a"), detail("a", "running"));
    h.registry.set(
      h.stateAtom(LOCAL, "c"),
      detail("c", "running", { status: "cached", error: Option.some("Could not sync.") }),
    );

    // The shell reports the stops first. A failed stream cannot deliver its
    // stop, so only it is released now.
    h.registry.set(h.threads(LOCAL), [shell("a", "idle"), shell("b", "idle"), shell("c", "idle")]);
    expect(h.openStreams()).toEqual(["local:a", "local:b"]);

    h.registry.set(h.stateAtom(LOCAL, "a"), detail("a", "idle"));
    h.registry.set(h.stateAtom(LOCAL, "b"), detail("b", "idle", { status: "synchronizing" }));
    expect(h.openStreams()).toEqual(["local:b"]);
    h.registry.set(h.stateAtom(LOCAL, "b"), detail("b", "idle"));
    expect(h.openStreams()).toEqual([]);
  });

  it("follows environments that connect and go away", () => {
    const h = makeHarness();
    h.registry.set(h.environmentIds, [LOCAL]);
    h.registry.set(h.threads(REMOTE), [shell("d", "running")]);
    expect(h.openStreams()).toEqual([]);

    h.registry.set(h.environmentIds, [LOCAL, REMOTE]);
    expect(h.openStreams()).toEqual(["remote:d"]);

    // Removal drops every mount, including one still waiting for its stop.
    h.registry.set(h.stateAtom(REMOTE, "d"), detail("d", "running"));
    h.registry.set(h.threads(REMOTE), [shell("d", "idle")]);
    expect(h.openStreams()).toEqual(["remote:d"]);
    h.registry.set(h.environmentIds, [LOCAL]);
    expect(h.openStreams()).toEqual([]);
  });
});
