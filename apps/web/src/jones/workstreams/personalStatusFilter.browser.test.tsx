import { scopeThreadRef, scopedThreadKey } from "@t3tools/client-runtime/environment";
import {
  personalThreadKey,
  type PersonalWorkstreamsState,
} from "@t3tools/client-runtime/jones/workstreams/personal";
import { DEFAULT_CLIENT_SETTINGS, EnvironmentId, RunId, ThreadId } from "@t3tools/contracts";
import * as DateTime from "effect/DateTime";
import { describe, expect, it, vi } from "vite-plus/test";
import { act, render } from "../../../test/render";
import {
  __resetClientSettingsPersistenceForTests,
  __setClientSettingsForTests,
} from "../../hooks/useSettings";
import { makeThreadFixture } from "../../test-fixtures";
import type { SidebarThreadSummary } from "../../types";
import { useUiStateStore } from "../../uiStateStore";
import { useThreadStatusSelection } from "../threadStatusFilter/useThreadStatusFilter";
import {
  personalStatusFilterKey,
  usePersonalStatusFilter,
  usePersonalStatusSelection,
} from "./personalStatusFilter";

const alphaId = "personal-status-browser-alpha";
const betaId = "personal-status-browser-beta";
const completed = "2099-11-16T12:00:00.000Z";
const visited = "2099-11-16T11:00:00.000Z";
const now = "2099-11-16T12:30:00.000Z";
const key = (thread: SidebarThreadSummary) =>
  scopedThreadKey(scopeThreadRef(thread.environmentId, thread.id));

function fixture() {
  const environmentId = EnvironmentId.make("personal-status-browser");
  const make = (id: string, status: "working" | "input" | "done") => {
    const thread = makeThreadFixture({
      environmentId,
      id: ThreadId.make(id),
      lastVisitedAt: visited,
      hasPendingUserInput: status === "input",
      latestRun: {
        runId: RunId.make(`run-${id}`),
        status: "completed",
        assistantMessageId: null,
        requestedAt: visited,
        startedAt: visited,
        completedAt: completed,
      },
      runtime: null,
    });
    return {
      ...thread,
      source: { ...thread.source, latestRunCompletedAt: DateTime.makeUnsafe(completed) },
      runtime:
        status === "working"
          ? {
              status: "running" as const,
              providerName: "Codex",
              providerInstanceId: thread.providerInstanceId,
              activeRunId: null,
              lastError: null,
              updatedAt: completed,
            }
          : null,
    };
  };
  const alphaWorking = make("alpha-working", "working");
  const alphaDone = make("alpha-done", "done");
  const betaInput = make("beta-input", "input");
  const betaDone = make("beta-done", "done");
  const unassigned = make("unassigned-done", "done");
  const threads = [betaDone, alphaWorking, unassigned, betaInput, alphaDone];
  const state: PersonalWorkstreamsState = {
    workstreams: [alphaId, betaId].map((id) => ({
      id,
      name: id,
      lifecycle: "active",
      color: null,
      orderKey: id,
      createdAt: visited,
      updatedAt: completed,
    })),
    assignmentsByThreadKey: new Map(
      [alphaWorking, alphaDone, betaInput, betaDone].map((thread) => [
        personalThreadKey(thread),
        {
          threadId: thread.id,
          workstreamId: thread.id.startsWith("alpha") ? alphaId : betaId,
          revision: 1,
          updatedAt: completed,
        },
      ]),
    ),
    environments: [],
    pendingThreadKeys: new Set(),
    canCreate: false,
  };
  const previous = new Map(
    [alphaId, betaId].map((id) => [
      personalStatusFilterKey(id),
      localStorage.getItem(personalStatusFilterKey(id)),
    ]),
  );
  const previousVisits = useUiStateStore.getState().threadLastVisitedAtById;
  for (const storageKey of previous.keys()) localStorage.removeItem(storageKey);
  __setClientSettingsForTests(DEFAULT_CLIENT_SETTINGS);
  useUiStateStore.setState({ threadLastVisitedAtById: {} });
  return {
    state,
    threads,
    alphaWorking,
    alphaDone,
    betaInput,
    betaDone,
    unassigned,
    [Symbol.dispose]() {
      for (const [storageKey, value] of previous) {
        if (value === null) localStorage.removeItem(storageKey);
        else localStorage.setItem(storageKey, value);
      }
      useUiStateStore.setState({ threadLastVisitedAtById: previousVisits });
      __resetClientSettingsPersistenceForTests();
      vi.restoreAllMocks();
    },
  };
}

type Input = {
  state: PersonalWorkstreamsState;
  threads: readonly SidebarThreadSummary[];
  activeThreadKey?: string;
};
type Preference = ReturnType<typeof usePersonalStatusFilter>;
const preferences = new Map<string, Preference>();

function PreferenceProbe({ id }: { id: string }) {
  preferences.set(id, usePersonalStatusFilter(id));
  return null;
}

function Selection({ state, threads, activeThreadKey }: Input) {
  const global = useThreadStatusSelection(threads, "done", "current", now, activeThreadKey);
  const selected = usePersonalStatusSelection(threads, state, global.threads, now, activeThreadKey);
  return (
    <>
      {state.workstreams.map(({ id }) => (
        <PreferenceProbe key={id} id={id} />
      ))}
      <output>{JSON.stringify(selected.map((thread) => thread.id))}</output>
    </>
  );
}

const visible = (container: HTMLElement) =>
  JSON.parse(container.querySelector("output")!.textContent!) as string[];
const ids = (...threads: SidebarThreadSummary[]) => threads.map((thread) => thread.id);
const setFilter = (id: string, filter: Parameters<Preference["setFilter"]>[0]) =>
  act(() => preferences.get(id)!.setFilter(filter));

describe("personal status selection subscriptions", () => {
  it("combines independent overrides, clears to inherit and persists through remount", async () => {
    using f = fixture();
    await using screen = await render(<Selection {...f} />, { router: false });
    expect(visible(screen.container)).toEqual(ids(f.betaDone, f.unassigned, f.alphaDone));
    await setFilter(alphaId, "working");
    await setFilter(betaId, "input");
    expect(visible(screen.container)).toEqual(ids(f.alphaWorking, f.unassigned, f.betaInput));
    await screen.rerender(null);
    await screen.rerender(<Selection {...f} />);
    expect(visible(screen.container)).toEqual(ids(f.alphaWorking, f.unassigned, f.betaInput));
    await setFilter(alphaId, (selected) => (selected === "working" ? null : "working"));
    expect(localStorage.getItem(personalStatusFilterKey(alphaId))).toBeNull();
    expect(localStorage.getItem(personalStatusFilterKey(betaId))).toBe('"input"');
    expect(visible(screen.container)).toEqual(ids(f.unassigned, f.betaInput, f.alphaDone));
  });

  it("updates external storage, assignments and the set of extant groups without changing hook count", async () => {
    using f = fixture();
    await using screen = await render(<Selection {...f} />, { router: false });
    await act(() => {
      localStorage.setItem(personalStatusFilterKey(alphaId), '"working"');
      window.dispatchEvent(new StorageEvent("storage", { key: personalStatusFilterKey(alphaId) }));
    });
    expect(visible(screen.container)).toEqual(ids(f.betaDone, f.alphaWorking, f.unassigned));
    const assignments = new Map(f.state.assignmentsByThreadKey);
    const assignment = assignments.get(personalThreadKey(f.alphaWorking))!;
    assignments.set(personalThreadKey(f.alphaWorking), { ...assignment, workstreamId: betaId });
    await screen.rerender(
      <Selection {...f} state={{ ...f.state, assignmentsByThreadKey: assignments }} />,
    );
    expect(visible(screen.container)).toEqual(ids(f.betaDone, f.unassigned));
    await screen.rerender(<Selection {...f} state={{ ...f.state, workstreams: [] }} />);
    expect(visible(screen.container)).toEqual(ids(f.betaDone, f.unassigned, f.alphaDone));
    await screen.rerender(<Selection {...f} />);
    expect(visible(screen.container)).toEqual(ids(f.betaDone, f.alphaWorking, f.unassigned));
    await act(() => {
      localStorage.setItem(personalStatusFilterKey(alphaId), "malformed");
      window.dispatchEvent(new StorageEvent("storage", { key: null }));
    });
    expect(visible(screen.container)).toEqual(ids(f.betaDone, f.unassigned, f.alphaDone));
  });

  it.each(["done", "input"] as const)(
    "retains positive %s routes and resets when the local selection changes",
    async (filter) => {
      using f = fixture();
      const thread =
        filter === "done" ? f.alphaDone : { ...f.alphaDone, hasPendingUserInput: true };
      const input = { ...f, threads: [thread], activeThreadKey: key(thread) };
      await using screen = await render(<Selection {...input} />, { router: false });
      await setFilter(alphaId, filter);
      expect(visible(screen.container)).toEqual(ids(thread));
      const acknowledged = { ...thread, lastVisitedAt: completed, hasPendingUserInput: false };
      await screen.rerender(<Selection {...input} threads={[acknowledged]} />);
      expect(visible(screen.container)).toEqual(ids(thread));
      await setFilter(alphaId, filter === "done" ? "hide-done" : "hide-input");
      expect(visible(screen.container)).toEqual(ids(thread));
      await setFilter(alphaId, filter);
      expect(visible(screen.container)).toEqual([]);
    },
  );

  it("removes snoozed input from a work stream's local filter and restores it on wake", async () => {
    using f = fixture();
    const thread = { ...f.alphaDone, hasPendingUserInput: true, pendingUserInputAt: visited };
    const input = { ...f, threads: [thread], activeThreadKey: key(thread) };
    await using screen = await render(<Selection {...input} />, { router: false });
    await setFilter(alphaId, "input");
    expect(visible(screen.container)).toEqual(ids(thread));
    await screen.rerender(
      <Selection
        {...input}
        threads={[{ ...thread, snoozedAt: completed, snoozedUntil: "2100-01-01T00:00:00.000Z" }]}
      />,
    );
    expect(visible(screen.container)).toEqual([]);
    await screen.rerender(<Selection {...input} />);
    expect(visible(screen.container)).toEqual(ids(thread));
  });

  it("uses upstream activity-window classification and preserves Hide working outside that window", async () => {
    using f = fixture();
    __setClientSettingsForTests({
      ...DEFAULT_CLIENT_SETTINGS,
      threadActivityWindowEnabled: true,
      threadActivityWindowAmount: 1,
      threadActivityWindowUnit: "minutes",
    });
    await using screen = await render(<Selection {...f} />, { router: false });
    await setFilter(alphaId, "working");
    expect(visible(screen.container)).toEqual([]);
    await setFilter(alphaId, "hide-working");
    expect(visible(screen.container)).toEqual(ids(f.alphaDone));
    await setFilter(alphaId, "hide-done");
    expect(visible(screen.container)).toEqual(ids(f.alphaWorking, f.alphaDone));
  });
});
