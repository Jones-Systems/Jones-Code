import "~/index.css";
import { RegistryContext } from "@effect/atom-react";
import { Atom, AtomRegistry } from "effect/reactivity";
import { describe, expect, it, vi } from "vite-plus/test";
import { page, userEvent } from "vite-plus/test/browser";
import {
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
    ) => true,
  );
  const reorder = vi.fn(async () => {});
  const activate = vi.fn();
  const externalDropRef: { current: NativeThreadExternalDrop | null } = { current: null };
  let canOperate = () => true;
  let canReorder = (_thread: EnvironmentThreadShell) => true;
  const ui = (threads = [source, first, last], summaryThreads = threads) => (
    <RegistryContext.Provider value={registry}>
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
    expect(insertion()?.parentElement).toBe(target);
    expect(insertion()!.getBoundingClientRect().top).toBeCloseTo(
      target.getBoundingClientRect().top,
    );
    expect(document.elementFromPoint(before.x, before.y)?.closest("[data-personal-thread]")).toBe(
      target,
    );
    const after = point(target, 0.8);
    await act(() => pointer(document, "pointermove", after));
    expect(insertion()?.dataset.personalThreadInsertion).toBe("after");
    expect(insertion()!.getBoundingClientRect().bottom).toBeCloseTo(
      target.getBoundingClientRect().bottom,
    );
    expect(lifted.getBoundingClientRect().top).toBeCloseTo(originalRect.top + after.y - origin.y);
    await act(() => pointer(document, "pointerup", after, 0));
    expect(setup.placeThread).toHaveBeenCalledExactlyOnceWith(source, "Drag Alpha", first, true);
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

  it.each([false, true])(
    "allows supported Unassigned reordering in either direction: after=%s",
    async (after) => {
      using setup = fixture();
      setup.registry.set(setup.state, { ...setup.initial, assignmentsByThreadKey: new Map() });
      await using screen = await render(setup.ui(), { container: container() });
      const moved = after ? source : last;
      const neighbor = after ? last : source;
      const at = point(row(screen.container, neighbor), after ? 0.8 : 0.2);
      await start(row(screen.container, moved), at);
      expect(insertion()?.dataset.personalThreadInsertion).toBe(after ? "after" : "before");
      await act(() => pointer(document, "pointerup", at, 0));
      expect(setup.placeThread).toHaveBeenCalledExactlyOnceWith(moved, null, neighbor, after);
    },
  );

  it("uses exact same-group slots and natural placement on a group header", async () => {
    using setup = fixture();
    await using screen = await render(setup.ui(), { container: container() });
    const from = row(screen.container, last);
    const before = point(row(screen.container, first), 0.2);
    await start(from, before);
    await act(() => pointer(document, "pointerup", before, 0));
    expect(setup.placeThread).toHaveBeenLastCalledWith(last, "Drag Alpha", first, false);
    const header = section(screen.container, "Drag Alpha").querySelector("button")!;
    const at = point(header);
    await start(row(screen.container, source), at);
    expect(insertion()).toBeNull();
    await act(() => pointer(document, "pointerup", at, 0));
    expect(setup.placeThread).toHaveBeenLastCalledWith(source, "Drag Alpha", null, false);
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
    expect(insertion()?.parentElement).toBe(unassignedRow);
    await act(() => pointer(document, "pointerup", before, 0));
    expect(setup.placeThread).toHaveBeenLastCalledWith(first, null, source, false);
    const list = section(screen.container, "Drag Alpha").querySelector<HTMLElement>(
      "[data-personal-thread-list]",
    )!;
    list.style.paddingBottom = "20px";
    const rect = list.getBoundingClientRect();
    const blank = { x: rect.left + rect.width / 2, y: rect.bottom - 8 };
    await start(row(screen.container, source), blank);
    expect(insertion()?.parentElement).toBe(row(screen.container, last));
    expect(insertion()?.dataset.personalThreadInsertion).toBe("after");
    await act(() => pointer(document, "pointerup", blank, 0));
    expect(setup.placeThread).toHaveBeenLastCalledWith(source, "Drag Alpha", last, true);
  });

  it("assigns naturally to empty and collapsed destinations without promising a slot", async () => {
    using setup = fixture();
    await using screen = await render(setup.ui(), { container: container() });
    const empty = section(screen.container, "Drag Empty");
    const at = point(empty.querySelector("[data-personal-thread-list]")!);
    await start(row(screen.container, source), at);
    expect(insertion()).toBeNull();
    await act(() => pointer(document, "pointerup", at, 0));
    expect(setup.placeThread).toHaveBeenLastCalledWith(source, "Drag Empty", null, true);
    await userEvent.click(page.getByRole("button", { name: "Collapse Drag Alpha", exact: true }));
    const collapsed = point(section(screen.container, "Drag Alpha").querySelector("button")!);
    await start(row(screen.container, source), collapsed);
    expect(insertion()).toBeNull();
    await act(() => pointer(document, "pointerup", collapsed, 0));
    expect(setup.placeThread).toHaveBeenLastCalledWith(source, "Drag Alpha", null, false);
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
    expect(setup.placeThread).toHaveBeenLastCalledWith(source, "Drag Alpha", null, false);
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
    expect(setup.placeThread).toHaveBeenCalledExactlyOnceWith(pinned, "Drag Alpha", first, false);
    expect(unpin).not.toHaveBeenCalled();
    expect(setup.controller.assign).not.toHaveBeenCalled();
    setup.placeThread.mockResolvedValue(true);
    await act(() => {
      dropPinnedThreadOnWorkstream({
        bridge: setup.externalDropRef.current,
        point: at,
        thread: pinned,
        unpin,
        onError: vi.fn(),
      });
    });
    expect(unpin).toHaveBeenCalledTimes(1);
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
