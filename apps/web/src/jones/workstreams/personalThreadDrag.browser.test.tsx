import "~/index.css";
import { useLayoutEffect, useState } from "react";
import { RegistryContext, useAtomValue } from "@effect/atom-react";
import { Atom, AtomRegistry } from "effect/reactivity";
import { describe, expect, it, vi } from "vite-plus/test";
import { page, userEvent } from "vite-plus/test/browser";
import {
  createPersonalWorkstreamsController,
  groupPersonalThreads,
  personalThreadKey,
  type PersonalWorkstreamsController,
  type PersonalWorkstreamsState,
} from "@t3tools/client-runtime/jones/workstreams/personal";
import type { EnvironmentThreadShell } from "@t3tools/client-runtime/state/models";
import { EnvironmentId, ThreadId } from "@t3tools/contracts";
import { act, render, fakeTimers } from "../../../test/render";
import {
  WorkstreamNativeSidebar,
  type NativeThreadExternalDrop,
} from "../../components/workstreams/WorkstreamNativeSidebar";
import { scopeThreadRef, scopedThreadKey } from "@t3tools/client-runtime/environment";
import { sortActiveThreadsByOrderKey } from "@t3tools/client-runtime/state/thread-sort";
import { placePersonalThread } from "./personalThreadPlacement";
import { dropPinnedThreadOnWorkstream } from "./sidebarThreadDrop";

const environmentId = EnvironmentId.make("drag-browser-env");
const makeThread = (id: string) =>
  ({
    environmentId,
    id: ThreadId.make(id),
    title: `Card ${id}`,
    pinnedAt: null,
  }) as EnvironmentThreadShell;
const source = makeThread("source");
const first = makeThread("first");
const last = makeThread("last");
const group = (id: string) => ({
  id,
  name: id,
  lifecycle: "active" as const,
  color: null,
  orderKey: id,
  createdAt: "2026-11-12T12:00:00.000Z",
  updatedAt: "2026-11-12T12:00:00.000Z",
});
const assignment = (thread: EnvironmentThreadShell, workstreamId: string) =>
  [
    personalThreadKey(thread),
    { threadId: thread.id, workstreamId, revision: 1, updatedAt: null },
  ] as const;

function fixture() {
  const registry = AtomRegistry.make();
  const initial: PersonalWorkstreamsState = {
    workstreams: [group("Drag Alpha"), group("Drag Empty")],
    environments: [{ environmentId, status: "ready", confirmed: true, workstreamCount: 2 }],
    assignmentsByThreadKey: new Map([
      assignment(first, "Drag Alpha"),
      assignment(last, "Drag Alpha"),
    ]),
    pendingThreadKeys: new Set(),
    canCreate: true,
  };
  const state = Atom.make(initial);
  const controller: PersonalWorkstreamsController = {
    state,
    canAssign: () => true,
    create: vi.fn(),
    rename: vi.fn(),
    setColor: vi.fn(),
    setLifecycle: vi.fn(),
    reorder: vi.fn(),
    remove: vi.fn(),
    assign: vi.fn(async () => ({ ok: true as const, value: undefined })),
    createAndAssign: vi.fn(),
  };
  const placeThread = vi.fn(
    async (
      _thread: EnvironmentThreadShell,
      _destinationId: string | null,
      _neighbor: EnvironmentThreadShell | null,
      _after: boolean,
      _unpin?: () => Promise<boolean>,
    ) => true,
  );
  const reorder = vi.fn(async () => {});
  const activate = vi.fn();
  const externalDropRef: { current: NativeThreadExternalDrop | null } = { current: null };
  let canOperate = () => true;
  let canReorder = (_thread: EnvironmentThreadShell) => true;
  const ui = (threads = [source, first, last], summaryThreads = threads) => (
    <RegistryContext.Provider value={registry}>
      <ul>
        {summaryThreads
          .filter((entry) => !threads.includes(entry))
          .map((entry) => (
            <li
              key={personalThreadKey(entry)}
              data-thread-item={personalThreadKey(entry)}
              className="h-12"
            >
              {entry.title}
            </li>
          ))}
      </ul>
      <WorkstreamNativeSidebar
        controller={controller}
        threads={threads}
        summaryThreads={summaryThreads}
        externalDropRef={externalDropRef}
        renderThread={(thread) => (
          <li className="h-12 rounded-md border p-2" onClick={activate}>
            <span>{thread.title}</span>
          </li>
        )}
        canOperate={(thread) => canOperate() && controller.canAssign(thread)}
        canReorder={(thread) => canReorder(thread)}
        reorder={reorder}
        placeThread={placeThread}
      />
    </RegistryContext.Provider>
  );
  return {
    registry,
    initial,
    state,
    controller,
    placeThread,
    reorder,
    activate,
    externalDropRef,
    ui,
    setCanOperate: (value: boolean) => {
      canOperate = () => value;
    },
    setCanReorder: (value: (thread: EnvironmentThreadShell) => boolean) => {
      canReorder = value;
    },
    [Symbol.dispose]() {
      registry.dispose();
      for (const id of ["Drag Alpha", "Drag Empty"])
        localStorage.removeItem(`jones:workstreams:collapsed:${id}`);
      localStorage.removeItem("jones:workstreams:unassigned-collapsed");
      localStorage.removeItem("jones:workstreams:named-hidden");
      localStorage.removeItem("jones:workstreams:shared-status-filter");
    },
  };
}

type Point = { x: number; y: number };
function point(element: Element, fraction = 0.5): Point {
  const rect = element.getBoundingClientRect();
  return { x: rect.left + rect.width / 2, y: rect.top + rect.height * fraction };
}
function pointer(target: EventTarget, type: string, at: Point, buttons = 1) {
  target.dispatchEvent(
    new PointerEvent(type, {
      bubbles: true,
      cancelable: true,
      isPrimary: true,
      pointerId: 1,
      button: 0,
      buttons,
      clientX: at.x,
      clientY: at.y,
    }),
  );
}
function row(container: HTMLElement, thread: EnvironmentThreadShell) {
  return [...container.querySelectorAll<HTMLElement>("[data-personal-thread]")].find(
    (element) => element.dataset.personalThread === personalThreadKey(thread),
  )!;
}
function section(container: HTMLElement, id: string | null) {
  return [...container.querySelectorAll<HTMLElement>("[data-native-workstream]")].find(
    (element) => element.dataset.nativeWorkstream === JSON.stringify(id),
  )!;
}
function container() {
  const element = document.createElement("div");
  element.style.width = "360px";
  return element;
}
async function start(sourceRow: HTMLElement, destination: Point) {
  await act(() => {
    pointer(sourceRow, "pointerdown", point(sourceRow));
    pointer(document, "pointermove", destination);
  });
}
const insertion = () => document.querySelector<HTMLElement>("[data-personal-thread-insertion]");

describe("personal thread pointer placement", () => {
  it.each([
    { pinned: false, release: "pointerup", sameGroup: false, rejectUnpin: false },
    { pinned: false, release: "pointermove", sameGroup: true, rejectUnpin: false },
    { pinned: true, release: "pointerup", sameGroup: false, rejectUnpin: false },
    { pinned: true, release: "pointerup", sameGroup: false, rejectUnpin: true },
  ])(
    "reads back membership and saved neighbor order ($pinned, $release, $sameGroup, $rejectUnpin)",
    async ({ pinned, release, sameGroup, rejectUnpin }) => {
      const registry = AtomRegistry.make();
      const at = "2026-11-17T12:00:00.000Z";
      const shellKey = (thread: EnvironmentThreadShell) =>
        scopedThreadKey(scopeThreadRef(thread.environmentId, thread.id));
      const moved = { ...source, pinnedAt: pinned ? at : null };
      const hidden = makeThread("hidden");
      const members = sameGroup ? [first, last, moved] : [first, last, hidden];
      const stationary = pinned ? [hidden, first, last] : members.filter((row) => row !== moved);
      const trace: string[] = [];
      const controller = createPersonalWorkstreamsController({
        registry,
        origin: "drag-integration",
        now: () => at,
        transport: {
          subscribe: (connection, emit) => {
            emit({
              environmentId: connection.environmentId,
              sequence: 0,
              workstreams: [group("Drag Alpha")].map((entry) => ({
                ...entry,
                deletedAt: null,
                rev: 1,
                origin: "drag-integration",
              })),
              assignments: members.map((thread) => ({
                threadId: thread.id,
                workstreamId: "Drag Alpha",
                revision: 1,
                updatedAt: at,
              })),
              importIssue: null,
            });
            return () => {};
          },
          put: vi.fn(),
          assign: async (_connection, input) => {
            trace.push("assign");
            return {
              outcome: "applied",
              sequence: 1,
              assignment: {
                threadId: input.threadId,
                workstreamId: input.workstream?.id ?? null,
                revision: input.expectedRevision + 1,
                updatedAt: at,
              },
              workstream: input.workstream,
            };
          },
        },
      });
      controller.setEnvironments([
        { environmentId, generation: 1, connected: true, capable: true, writable: true },
      ]);
      using _cleanup = {
        [Symbol.dispose]() {
          controller.dispose();
          registry.dispose();
          localStorage.removeItem("jones:workstreams:collapsed:Drag Alpha");
        },
      };
      const bridge: { current: NativeThreadExternalDrop | null } = { current: null };
      let savedRows = [...stationary, moved].map((thread, index) => ({
        ...thread,
        activeOrderKey:
          thread === hidden && !pinned
            ? "m"
            : thread === moved && !pinned && !sameGroup
              ? "t"
              : null,
        createdAt: new Date(Date.UTC(2026, 10, 20 - index)).toISOString(),
      }));
      const alphaOrder = () =>
        groupPersonalThreads(
          registry.get(controller.state),
          sortActiveThreadsByOrderKey(savedRows.filter((row) => row.pinnedAt == null)),
        ).groups[0]!.threads.map(({ id }) => id);
      const acknowledged: Array<{ id: string; key: string; siblings: string[] }> = [];
      let resolveUnpin!: () => void;
      const unpinReply = new Promise<void>((resolve) => {
        resolveUnpin = resolve;
      });
      const onUnpinError = vi.fn();
      let unpin: () => Promise<void> = async () => {
        throw new Error("Pinned fixture was not mounted");
      };
      function Integration() {
        const state = useAtomValue(controller.state);
        const [rows, setRows] = useState(savedRows);
        const persist = (
          target: EnvironmentThreadShell,
          patch: Partial<(typeof savedRows)[number]>,
        ) => {
          savedRows = savedRows.map((row) => (row.id === target.id ? { ...row, ...patch } : row));
          setRows(savedRows);
        };
        const unpinArrival = async () => {
          trace.push("unpin");
          await unpinReply;
          if (rejectUnpin) throw new Error("Synthetic server rejected unpin");
          persist(moved, { pinnedAt: null });
          trace.push("unpinned");
        };
        useLayoutEffect(() => {
          unpin = unpinArrival;
        });
        const active = sortActiveThreadsByOrderKey(
          rows.filter((thread) => thread.pinnedAt == null),
        );
        const visible = active.filter((thread) => thread.id !== hidden.id);
        return (
          <>
            <ul>
              {rows
                .filter((thread) => thread.pinnedAt != null)
                .map((thread) => (
                  <li
                    key={thread.id}
                    data-thread-item={personalThreadKey(thread)}
                    style={{ height: 73 }}
                  >
                    {thread.title}
                  </li>
                ))}
            </ul>
            <WorkstreamNativeSidebar
              controller={controller}
              threads={visible}
              summaryThreads={rows}
              externalDropRef={bridge}
              renderThread={(thread) => (
                <li style={{ height: thread.id === source.id ? 73 : 48 }}>{thread.title}</li>
              )}
              canOperate={() => true}
              canReorder={() => true}
              reorder={async () => {}}
              placeThread={(thread, destinationId, neighbor, after, unpin) =>
                placePersonalThread({
                  thread,
                  destinationId,
                  neighbor,
                  after,
                  state,
                  activeThreads: visible,
                  orderRoster: rows,
                  activeKeysById: new Map(
                    rows.map((entry) => [shellKey(entry), entry.activeOrderKey]),
                  ),
                  reorderableKeys: new Set(rows.map(shellKey)),
                  allowReorder: true,
                  controller,
                  canOperate: () => true,
                  unpin,
                  reorder: async (target, orderKey) => {
                    trace.push(`reorder ${target.id}=${orderKey}`);
                    if (savedRows.find((entry) => entry.id === target.id)?.pinnedAt != null)
                      return false;
                    persist(target, { activeOrderKey: orderKey });
                    acknowledged.push({
                      id: target.id,
                      key: orderKey,
                      siblings: alphaOrder().filter((id) => id !== moved.id),
                    });
                    return true;
                  },
                  onAssignmentFailure: (failure) => {
                    throw new Error(failure.reason);
                  },
                  onPositionFailure: (_saved, cause) => {
                    throw cause;
                  },
                })
              }
            />
          </>
        );
      }
      await using screen = await render(
        <RegistryContext.Provider value={registry}>
          <Integration />
        </RegistryContext.Provider>,
        { container: container() },
      );
      const neighbor = row(screen.container, first);
      const originalTop = neighbor.getBoundingClientRect().top;
      const destination = point(neighbor, pinned ? 0.8 : 0.2);
      if (pinned)
        await act(() => {
          expect(bridge.current!.locateExternal(destination, moved)).toBe(true);
        });
      else await start(row(screen.container, moved), destination);
      const gap = insertion()!;
      expect(gap.getBoundingClientRect().height).toBe(73);
      expect(neighbor.getBoundingClientRect().top - originalTop).toBe(pinned ? 0 : 73);
      expect(pinned ? gap.previousElementSibling : gap.nextElementSibling).toBe(neighbor);
      expect(
        registry.get(controller.state).assignmentsByThreadKey.get(personalThreadKey(moved))
          ?.workstreamId,
      ).toBe(sameGroup ? "Drag Alpha" : undefined);
      for (const fraction of [0.1, 0.5, 0.9, 0.5]) {
        if (pinned)
          await act(() => {
            expect(bridge.current!.locateExternal(point(gap, fraction), moved)).toBe(true);
          });
        else await act(() => pointer(document, "pointermove", point(gap, fraction)));
        expect(insertion()).toBe(gap);
        expect(neighbor.getBoundingClientRect().top - originalTop).toBe(pinned ? 0 : 73);
      }
      if (pinned) {
        try {
          await act(() => {
            expect(
              dropPinnedThreadOnWorkstream({
                bridge: bridge.current,
                point: point(gap),
                thread: moved,
                unpin,
                onError: onUnpinError,
              }),
            ).toBe(true);
          });
          await vi.waitFor(() => expect(trace.length).toBeGreaterThan(1));
          expect(trace).toEqual(["assign", "unpin"]);
          expect(acknowledged).toEqual([]);
        } finally {
          await act(resolveUnpin);
        }
      } else {
        const landing = point(gap);
        await act(() => pointer(document, release, landing, 0));
        if (release === "pointermove") await act(() => pointer(document, "pointerup", landing, 0));
      }
      expect(
        registry.get(controller.state).assignmentsByThreadKey.get(personalThreadKey(moved))
          ?.workstreamId,
      ).toBe("Drag Alpha");
      expect(insertion()).toBeNull();
      if (rejectUnpin) {
        expect(onUnpinError).toHaveBeenCalledOnce();
        expect(trace).toEqual(["assign", "unpin"]);
        expect(savedRows.find(({ id }) => id === moved.id)?.pinnedAt).toBe(at);
        expect(alphaOrder()).toEqual(stationary.map(({ id }) => id));
        return;
      }
      const expected = pinned ? [hidden, first, moved, last] : [moved, ...stationary];
      if (pinned)
        expect(trace).toEqual([
          "assign",
          "unpin",
          "unpinned",
          "reorder last=n",
          "reorder source=h",
        ]);
      for (const write of acknowledged) {
        expect(write.siblings, `after ${write.id}=${write.key}`).toEqual(
          stationary.map(({ id }) => id),
        );
      }
      expect(alphaOrder()).toEqual(expected.map(({ id }) => id));
      if (!pinned && !sameGroup)
        expect(acknowledged.map(({ id, key }) => [id, key])).toEqual([
          [last.id, "g"],
          [first.id, "d"],
          [moved.id, "c"],
        ]);
      expect(
        [
          ...section(screen.container, "Drag Alpha").querySelectorAll<HTMLElement>(
            "[data-personal-thread]",
          ),
        ].map((entry) => entry.dataset.personalThread),
      ).toEqual(expected.filter((entry) => entry !== hidden).map(personalThreadKey));
    },
  );

  it.each([
    ["Drag Alpha", false],
    ["Drag Alpha", true],
    [null, false],
    [null, true],
  ] as const)(
    "highlights the whole %s section (collapsed=%s) and clears on leave and drop",
    async (id, collapsed) => {
      using setup = fixture();
      await using screen = await render(setup.ui(), { container: container() });
      if (collapsed)
        await userEvent.click(
          page.getByRole("button", { name: `Collapse ${id ?? "Unassigned"}`, exact: true }),
        );
      const destination = section(screen.container, id);
      const at = point(destination.querySelector("button")!);
      const from = row(screen.container, id === null ? first : source);
      const shadow = getComputedStyle(destination).boxShadow;
      const fill = getComputedStyle(destination).backgroundColor;
      const bounds = destination.getBoundingClientRect().toJSON();
      await start(from, at);
      expect([...screen.container.querySelectorAll("[data-thread-drop-target]")]).toEqual([
        destination,
      ]);
      expect(getComputedStyle(destination).boxShadow).not.toBe(shadow);
      if (collapsed) {
        expect(getComputedStyle(destination).backgroundColor).not.toBe(fill);
        expect(getComputedStyle(destination).backgroundColor).toMatch(/(?:0\.0325|3\.25%)/);
      } else expect(getComputedStyle(destination).backgroundColor).toBe(fill);
      expect(destination.getBoundingClientRect().toJSON()).toEqual(bounds);
      const outside = { x: screen.container.getBoundingClientRect().right + 20, y: at.y };
      await act(() => pointer(document, "pointermove", outside));
      expect(screen.container.querySelector("[data-thread-drop-target]")).toBeNull();
      expect(getComputedStyle(destination).boxShadow).toBe(shadow);
      expect(setup.placeThread).not.toHaveBeenCalled();
      await act(() => pointer(document, "pointermove", at));
      expect(destination.hasAttribute("data-thread-drop-target")).toBe(true);
      await act(() =>
        document.dispatchEvent(new KeyboardEvent("keydown", { code: "Escape", bubbles: true })),
      );
      expect(screen.container.querySelector("[data-thread-drop-target]")).toBeNull();
      expect(setup.placeThread).not.toHaveBeenCalled();
      await start(from, at);
      expect(destination.hasAttribute("data-thread-drop-target")).toBe(true);
      await act(() => pointer(document, "pointerup", at, 0));
      expect(screen.container.querySelector("[data-thread-drop-target]")).toBeNull();
      expect(setup.placeThread).toHaveBeenCalledExactlyOnceWith(
        id === null ? first : source,
        id,
        null,
        false,
        undefined,
      );
    },
  );

  it.each([false, true])(
    "highlights pinned external destinations including collapsed=%s and clears through the bridge",
    async (collapsed) => {
      using setup = fixture();
      await using screen = await render(setup.ui(), { container: container() });
      const pinned = { ...source, pinnedAt: "2026-11-12T12:00:00.000Z" };
      for (const id of ["Drag Alpha", null]) {
        if (collapsed)
          await userEvent.click(
            page.getByRole("button", { name: `Collapse ${id ?? "Unassigned"}`, exact: true }),
          );
        const destination = section(screen.container, id);
        const at = point(destination.querySelector("button")!);
        const shadow = getComputedStyle(destination).boxShadow;
        await act(() => {
          expect(setup.externalDropRef.current!.locateExternal(at, pinned)).toBe(true);
        });
        expect([...screen.container.querySelectorAll("[data-thread-drop-target]")]).toEqual([
          destination,
        ]);
        expect(getComputedStyle(destination).boxShadow).not.toBe(shadow);
        await act(() => setup.externalDropRef.current!.clear());
        expect(screen.container.querySelector("[data-thread-drop-target]")).toBeNull();
        expect(getComputedStyle(destination).boxShadow).toBe(shadow);
        await act(() => {
          expect(setup.externalDropRef.current!.locateExternal(at, pinned)).toBe(true);
        });
        await act(async () => {
          expect(await setup.externalDropRef.current!.dropExternal(at, pinned)).toBe(true);
        });
        expect(screen.container.querySelector("[data-thread-drop-target]")).toBeNull();
      }
    },
  );

  it("scrolls while holding a card at the top edge and stops on Escape", async () => {
    using setup = fixture();
    const viewport = container();
    viewport.dataset.slot = "scroll-area-viewport";
    viewport.style.cssText =
      "position:fixed;top:100px;left:0;width:360px;height:300px;overflow:auto";
    await using screen = await render(setup.ui(), { container: viewport });
    const spacer = document.createElement("div");
    spacer.style.height = "2000px";
    viewport.append(spacer);
    viewport.scrollTop = 200;
    using _timers = fakeTimers();
    const edge = { x: 100, y: 105 };
    await start(row(screen.container, source), edge);
    await act(() => vi.advanceTimersByTime(200));
    expect(viewport.scrollTop).toBeLessThan(150);
    await act(() => document.dispatchEvent(new KeyboardEvent("keydown", { code: "Escape" })));
    const top = viewport.scrollTop;
    await act(() => vi.advanceTimersByTime(200));
    expect(viewport.scrollTop).toBe(top);
    expect(document.querySelector("[data-personal-thread-drag]")).toBeNull();
    expect(setup.placeThread).not.toHaveBeenCalled();
  });

  it("lifts the existing card, preserves its source slot, and shows the exact cross-group insertion", async () => {
    using setup = fixture();
    await using screen = await render(setup.ui(), { container: container() });
    const sourceRow = row(screen.container, source);
    const target = row(screen.container, first);
    const origin = point(sourceRow);
    const before = point(target, 0.2);
    const originalRect = sourceRow.getBoundingClientRect();
    await start(sourceRow, before);
    const lifted = document.querySelector<HTMLElement>("[data-personal-thread-drag]")!;
    expect(lifted.textContent).toContain(source.title);
    expect(lifted.getAttribute("aria-hidden")).toBe("true");
    expect(lifted.inert).toBe(true);
    expect(getComputedStyle(lifted).pointerEvents).toBe("none");
    expect(getComputedStyle(lifted.querySelector("span")!).pointerEvents).toBe("none");
    expect(Number(getComputedStyle(sourceRow).opacity)).toBeLessThan(0.5);
    expect(sourceRow.getBoundingClientRect().height).toBe(originalRect.height);
    expect(lifted.getBoundingClientRect().top).toBeCloseTo(originalRect.top + before.y - origin.y);
    expect(insertion()?.dataset.personalThreadInsertion).toBe("before");
    expect(insertion()?.nextElementSibling).toBe(target);
    expect(insertion()!.getBoundingClientRect().top).toBeCloseTo(
      target.getBoundingClientRect().top - originalRect.height,
    );
    expect(insertion()!.getBoundingClientRect().height).toBe(originalRect.height);
    expect(
      document.elementFromPoint(before.x, before.y)?.closest("[data-personal-thread-insertion]"),
    ).toBe(insertion());
    const slot = insertion()!.dataset.personalThreadSlot;
    await act(() => pointer(document, "pointermove", point(insertion()!)));
    expect(insertion()!.dataset.personalThreadSlot).toBe(slot);
    const after = point(target, 0.8);
    await act(() => pointer(document, "pointermove", after));
    expect(insertion()?.dataset.personalThreadInsertion).toBe("after");
    expect(insertion()!.getBoundingClientRect().bottom).toBeCloseTo(
      target.getBoundingClientRect().bottom + originalRect.height,
    );
    expect(lifted.getBoundingClientRect().top).toBeCloseTo(originalRect.top + after.y - origin.y);
    await act(() => pointer(document, "pointerup", after, 0));
    expect(setup.placeThread).toHaveBeenCalledExactlyOnceWith(
      source,
      "Drag Alpha",
      first,
      true,
      undefined,
    );
    expect(setup.controller.assign).not.toHaveBeenCalled();
    expect(setup.reorder).not.toHaveBeenCalled();
    expect(document.querySelector("[data-personal-thread-drag]")).toBeNull();
    expect(sourceRow.style.opacity).toBe("");
    expect(insertion()).toBeNull();
    const title = sourceRow.querySelector<HTMLElement>("span")!;
    await act(() => title.click());
    expect(setup.activate).not.toHaveBeenCalled();
    await act(() => {
      pointer(title, "pointerdown", point(title));
      pointer(document, "pointerup", point(title), 0);
      title.click();
    });
    expect(setup.activate).toHaveBeenCalledTimes(1);
  });

  it.each([
    { after: false, release: "pointermove" },
    { after: true, release: "pointerup" },
  ])(
    "allows supported Unassigned reordering in either direction ($after, $release)",
    async ({ after, release }) => {
      using setup = fixture();
      setup.registry.set(setup.state, { ...setup.initial, assignmentsByThreadKey: new Map() });
      await using screen = await render(setup.ui(), { container: container() });
      const moved = after ? source : last;
      const neighbor = after ? last : source;
      const at = point(row(screen.container, neighbor), after ? 0.8 : 0.2);
      await start(row(screen.container, moved), at);
      expect(insertion()?.dataset.personalThreadInsertion).toBe(after ? "after" : "before");
      const landing = point(insertion()!);
      await act(() => pointer(document, release, landing, 0));
      if (release === "pointermove") await act(() => pointer(document, "pointerup", landing, 0));
      expect(setup.placeThread).toHaveBeenCalledExactlyOnceWith(
        moved,
        null,
        neighbor,
        after,
        undefined,
      );
    },
  );

  it("uses exact same-group slots and natural placement on a group header", async () => {
    using setup = fixture();
    await using screen = await render(setup.ui(), { container: container() });
    const from = row(screen.container, last);
    const before = point(row(screen.container, first), 0.2);
    await start(from, before);
    await act(() => pointer(document, "pointerup", before, 0));
    expect(setup.placeThread).toHaveBeenLastCalledWith(last, "Drag Alpha", first, false, undefined);
    const header = section(screen.container, "Drag Alpha").querySelector("button")!;
    const at = point(header);
    await start(row(screen.container, source), at);
    expect(insertion()).toBeNull();
    await act(() => pointer(document, "pointerup", at, 0));
    expect(setup.placeThread).toHaveBeenLastCalledWith(
      source,
      "Drag Alpha",
      null,
      false,
      undefined,
    );
  });

  it("keeps Unassigned in automatic order when active reordering is unavailable", async () => {
    using setup = fixture();
    setup.registry.set(setup.state, { ...setup.initial, assignmentsByThreadKey: new Map() });
    setup.setCanReorder(() => false);
    await using screen = await render(setup.ui(), { container: container() });
    const at = point(row(screen.container, last), 0.8);
    await start(row(screen.container, source), at);
    expect(insertion()).toBeNull();
    await act(() => pointer(document, "pointerup", at, 0));
    expect(setup.placeThread).not.toHaveBeenCalled();
    expect(setup.controller.assign).not.toHaveBeenCalled();
    expect(setup.reorder).not.toHaveBeenCalled();
  });

  it("places a group card into Unassigned and appends at blank destination space", async () => {
    using setup = fixture();
    await using screen = await render(setup.ui(), { container: container() });
    const unassignedRow = row(screen.container, source);
    const before = point(unassignedRow, 0.2);
    await start(row(screen.container, first), before);
    expect(insertion()?.nextElementSibling).toBe(unassignedRow);
    await act(() => pointer(document, "pointerup", before, 0));
    expect(setup.placeThread).toHaveBeenLastCalledWith(first, null, source, false, undefined);
    const list = section(screen.container, "Drag Alpha").querySelector<HTMLElement>(
      "[data-personal-thread-list]",
    )!;
    list.style.paddingBottom = "20px";
    const rect = list.getBoundingClientRect();
    const blank = { x: rect.left + rect.width / 2, y: rect.bottom - 8 };
    await start(row(screen.container, source), blank);
    expect(insertion()?.previousElementSibling).toBe(row(screen.container, last));
    expect(insertion()?.dataset.personalThreadInsertion).toBe("after");
    await act(() => pointer(document, "pointerup", blank, 0));
    expect(setup.placeThread).toHaveBeenLastCalledWith(source, "Drag Alpha", last, true, undefined);
  });

  it("shows an empty expanded insertion slot and only membership feedback when collapsed", async () => {
    using setup = fixture();
    await using screen = await render(setup.ui(), { container: container() });
    const empty = section(screen.container, "Drag Empty");
    const at = point(empty.querySelector("[data-personal-thread-list]")!);
    await start(row(screen.container, source), at);
    expect(insertion()!.getBoundingClientRect().height).toBe(
      row(screen.container, source).getBoundingClientRect().height,
    );
    await act(() => pointer(document, "pointermove", point(insertion()!)));
    await act(() => pointer(document, "pointerup", point(insertion()!), 0));
    expect(setup.placeThread).toHaveBeenLastCalledWith(source, "Drag Empty", null, true, undefined);
    await userEvent.click(page.getByRole("button", { name: "Collapse Drag Alpha", exact: true }));
    const collapsed = point(section(screen.container, "Drag Alpha").querySelector("button")!);
    await start(row(screen.container, source), collapsed);
    expect(insertion()).toBeNull();
    await act(() => pointer(document, "pointerup", collapsed, 0));
    expect(setup.placeThread).toHaveBeenLastCalledWith(
      source,
      "Drag Alpha",
      null,
      false,
      undefined,
    );
  });

  it.each([
    { id: "Drag Alpha", boundary: "pointerleave" },
    { id: null, boundary: "outside viewport" },
    { id: "Drag Alpha", boundary: "lostpointercapture" },
  ])("discards a missed release after $boundary in $id", async ({ id, boundary }) => {
    using setup = fixture();
    setup.registry.set(setup.state, {
      ...setup.initial,
      assignmentsByThreadKey: new Map(
        id === null ? [] : [source, first, last].map((thread) => assignment(thread, id)),
      ),
    });
    await using screen = await render(setup.ui(), { container: container() });
    const sourceRow = row(screen.container, source);
    const lastRow = row(screen.container, last);
    await start(sourceRow, point(lastRow, 0.8));
    expect(insertion()?.previousElementSibling).toBe(lastRow);
    const outside = { x: -1, y: point(lastRow).y };

    await act(() => {
      if (boundary === "outside viewport") {
        pointer(document.documentElement, "pointermove", outside);
      } else if (boundary === "lostpointercapture") {
        pointer(sourceRow, boundary, point(lastRow));
      } else {
        document.documentElement.dispatchEvent(
          new PointerEvent("pointerleave", {
            isPrimary: true,
            pointerType: "mouse",
            pointerId: 1,
            buttons: 1,
            clientX: outside.x,
            clientY: outside.y,
          }),
        );
      }
    });
    const gapAfterBoundary = insertion();
    const reentry = point(lastRow, 0.8);
    await act(() => pointer(document, "pointermove", reentry, 0));
    expect(setup.placeThread).not.toHaveBeenCalled();
    expect(gapAfterBoundary).toBeNull();
    expect(document.querySelector("[data-personal-thread-drag]")).toBeNull();
    expect(sourceRow.style.opacity).toBe("");

    await start(sourceRow, point(lastRow, 0.2));
    expect(insertion()?.nextElementSibling).toBe(lastRow);
    const release = id === null ? "pointermove" : "pointerup";
    await act(() => pointer(document, release, point(lastRow, 0.8), 0));
    expect(setup.placeThread).toHaveBeenCalledExactlyOnceWith(source, id, last, true, undefined);
    expect(insertion()).toBeNull();
  });

  it.each(["Escape", "pointercancel", "unmount"])(
    "cleans the card and sensor after %s",
    async (cancel) => {
      using setup = fixture();
      await using screen = await render(setup.ui(), { container: container() });
      const from = row(screen.container, source);
      const at = point(row(screen.container, first), 0.2);
      await start(from, at);
      expect(document.querySelector("[data-personal-thread-drag]")).not.toBeNull();
      expect(section(screen.container, "Drag Alpha").hasAttribute("data-thread-drop-target")).toBe(
        true,
      );
      if (cancel === "unmount") await screen.unmount();
      else
        await act(() => {
          if (cancel === "Escape")
            document.dispatchEvent(new KeyboardEvent("keydown", { code: "Escape", bubbles: true }));
          else pointer(document, "pointercancel", at, 0);
        });
      expect(document.querySelector("[data-personal-thread-drag]")).toBeNull();
      expect(from.style.opacity).toBe("");
      expect(insertion()).toBeNull();
      expect(screen.container.querySelector("[data-thread-drop-target]")).toBeNull();
      await act(() => pointer(document, "pointerup", at, 0));
      expect(setup.placeThread).not.toHaveBeenCalled();
    },
  );

  it.each(["source membership", "neighbor membership", "missing neighbor", "deleted destination"])(
    "does not write after stale %s",
    async (change) => {
      using setup = fixture();
      await using screen = await render(setup.ui(), { container: container() });
      const at = point(row(screen.container, first), 0.2);
      await start(row(screen.container, source), at);
      if (change === "missing neighbor") await screen.rerender(setup.ui([source, last]));
      else
        await act(() =>
          setup.registry.set(setup.state, {
            ...setup.initial,
            ...(change === "deleted destination"
              ? { workstreams: [group("Drag Empty")] }
              : {
                  assignmentsByThreadKey: new Map([
                    ...setup.initial.assignmentsByThreadKey,
                    assignment(change === "source membership" ? source : first, "Drag Empty"),
                  ]),
                }),
          }),
        );
      await act(() => pointer(document, "pointerup", at, 0));
      expect(setup.placeThread).not.toHaveBeenCalled();
      expect(setup.controller.assign).not.toHaveBeenCalled();
      expect(setup.reorder).not.toHaveBeenCalled();
      expect(document.querySelector("[data-personal-thread-drag]")).toBeNull();
    },
  );

  it("gates internal placement by operability and both reorder participants", async () => {
    using setup = fixture();
    await using screen = await render(setup.ui(), { container: container() });
    const at = point(row(screen.container, first), 0.2);
    setup.setCanReorder((thread) => thread.id !== first.id);
    await start(row(screen.container, source), at);
    expect(insertion()).toBeNull();
    await act(() => pointer(document, "pointerup", at, 0));
    expect(setup.placeThread).toHaveBeenLastCalledWith(
      source,
      "Drag Alpha",
      null,
      false,
      undefined,
    );
    setup.placeThread.mockClear();
    setup.setCanOperate(false);
    await start(row(screen.container, source), at);
    expect(document.querySelector("[data-personal-thread-drag]")).toBeNull();
    await act(() => pointer(document, "pointerup", at, 0));
    expect(setup.placeThread).not.toHaveBeenCalled();
  });

  it("passes pinned ordinals to the authoritative callback and does not unpin a rejected placement", async () => {
    using setup = fixture();
    setup.setCanReorder((thread) => thread.id !== source.id);
    setup.placeThread.mockResolvedValue(false);
    await using screen = await render(setup.ui(), { container: container() });
    const pinned = { ...source, pinnedAt: "2026-11-12T12:00:00.000Z" };
    const at = point(row(screen.container, first), 0.2);
    const unpin = vi.fn(async () => {});
    await act(() => {
      expect(setup.externalDropRef.current!.locateExternal(at, pinned)).toBe(true);
    });
    expect(insertion()?.dataset.personalThreadInsertion).toBe("before");
    await act(() => {
      expect(
        dropPinnedThreadOnWorkstream({
          bridge: setup.externalDropRef.current,
          point: at,
          thread: pinned,
          unpin,
          onError: vi.fn(),
        }),
      ).toBe(true);
    });
    expect(setup.placeThread).toHaveBeenCalledExactlyOnceWith(
      pinned,
      "Drag Alpha",
      first,
      false,
      expect.any(Function),
    );
    expect(unpin).not.toHaveBeenCalled();
    expect(setup.controller.assign).not.toHaveBeenCalled();
  });

  it.each(["Drag Alpha", null])(
    "accepts a pinned arrival before its existing group's first row: %s",
    async (destinationId) => {
      using setup = fixture();
      const pinned = { ...source, pinnedAt: "2026-11-12T12:00:00.000Z" };
      setup.registry.set(setup.state, {
        ...setup.initial,
        assignmentsByThreadKey: new Map(
          destinationId === null
            ? []
            : [
                assignment(source, destinationId),
                assignment(first, destinationId),
                assignment(last, destinationId),
              ],
        ),
      });
      await using screen = await render(setup.ui([first, last], [pinned, first, last]), {
        container: container(),
      });
      const at = point(row(screen.container, first), 0.2);
      await act(() => {
        expect(setup.externalDropRef.current!.locateExternal(at, pinned)).toBe(true);
      });
      expect(insertion()?.dataset.personalThreadInsertion).toBe("before");
      await act(async () => {
        expect(await setup.externalDropRef.current!.dropExternal(at, pinned)).toBe(true);
      });
      expect(setup.placeThread).toHaveBeenCalledExactlyOnceWith(
        pinned,
        destinationId,
        first,
        false,
        undefined,
      );
    },
  );

  it.each(["source", "neighbor", "destination"])(
    "rejects external placement after stale %s",
    async (change) => {
      using setup = fixture();
      await using screen = await render(setup.ui(), { container: container() });
      const pinned = { ...source, pinnedAt: "2026-11-12T12:00:00.000Z" };
      const at = point(row(screen.container, first), 0.2);
      await act(() => {
        expect(setup.externalDropRef.current!.locateExternal(at, pinned)).toBe(true);
      });
      await act(() =>
        setup.registry.set(setup.state, {
          ...setup.initial,
          ...(change === "destination"
            ? { workstreams: [group("Drag Empty")] }
            : {
                assignmentsByThreadKey: new Map([
                  ...setup.initial.assignmentsByThreadKey,
                  assignment(change === "source" ? source : first, "Drag Empty"),
                ]),
              }),
        }),
      );
      await act(async () => {
        expect(await setup.externalDropRef.current!.dropExternal(at, pinned)).toBe(false);
      });
      expect(setup.placeThread).not.toHaveBeenCalled();
    },
  );
});
