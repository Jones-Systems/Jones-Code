import { DndContext, useDraggable, useSensor, useSensors, type SensorProps } from "@dnd-kit/core";
import type { EnvironmentThreadShell } from "@t3tools/client-runtime/state/models";
import { dropPinnedThreadOnWorkstream } from "../jones/workstreams/sidebarThreadDrop";
import { afterEach, beforeEach, describe, expect, it, vi } from "vite-plus/test";
import { act, StrictMode, useState } from "react";
import { createRoot } from "react-dom/client";
import { SidebarDragLifecycle, SidebarPointerSensor } from "./Sidebar.pointer";

const sensors: SidebarPointerSensor[] = [];
let hidden = false;

function pointer(type: string, values: PointerEventInit = {}) {
  return new PointerEvent(type, {
    bubbles: true,
    cancelable: true,
    pointerId: 1,
    clientX: 10,
    clientY: 10,
    buttons: 1,
    ...values,
  });
}

function gesture(
  extraOptions: Partial<ConstructorParameters<typeof SidebarPointerSensor>[0]["options"]> = {},
) {
  const callbacks = {
    onStart: vi.fn(),
    onMove: vi.fn(),
    onEnd: vi.fn(),
    onCancel: vi.fn(),
    onAbort: vi.fn(),
    onPending: vi.fn(),
  };
  const onFinish = vi.fn();
  const props = {
    active: "thread",
    event: pointer("pointerdown"),
    options: { distance: 6, onAttach: vi.fn(), onFinish, ...extraOptions },
    ...callbacks,
  } as unknown as SensorProps<ConstructorParameters<typeof SidebarPointerSensor>[0]["options"]>;
  const sensor = new SidebarPointerSensor(props);
  sensors.push(sensor);
  return { sensor, onFinish, ...callbacks };
}

beforeEach(() => {
  vi.useFakeTimers();
  vi.stubGlobal("IS_REACT_ACT_ENVIRONMENT", true);
  hidden = false;
  vi.spyOn(document, "hidden", "get").mockImplementation(() => hidden);
});
afterEach(() => {
  for (const sensor of sensors.splice(0)) sensor.cancel();
  // Cancelled sensors keep suppressing clicks on the shared document until the next press.
  document.dispatchEvent(pointer("pointerdown"));
  vi.runAllTimers();
  vi.useRealTimers();
  vi.restoreAllMocks();
  vi.unstubAllGlobals();
});

describe("sidebar pointer lifecycle", () => {
  it("keeps a click idle and starts only after the drag threshold", () => {
    const click = gesture();
    document.dispatchEvent(pointer("pointermove", { clientY: 16 }));
    expect(click.onStart).not.toHaveBeenCalled();
    document.dispatchEvent(pointer("pointerup", { buttons: 0 }));
    expect(click.onAbort).toHaveBeenCalledOnce();
    expect(click.onFinish).toHaveBeenCalledOnce();

    const drag = gesture();
    document.dispatchEvent(pointer("pointermove", { clientY: 17 }));
    expect(drag.onStart).toHaveBeenCalledExactlyOnceWith({ x: 10, y: 10 });
    document.dispatchEvent(pointer("pointerup", { buttons: 0 }));
    expect(drag.onEnd).toHaveBeenCalledOnce();
    expect(drag.onAbort).not.toHaveBeenCalled();
    expect(drag.onFinish).toHaveBeenCalledOnce();
  });

  it("moves to the pointer on the move that starts the drag", () => {
    const drag = gesture();
    document.dispatchEvent(pointer("pointermove", { clientY: 90 }));
    expect(drag.onStart).toHaveBeenCalledExactlyOnceWith({ x: 10, y: 10 });
    expect(drag.onMove).toHaveBeenCalledExactlyOnceWith({ x: 10, y: 90 });
  });

  const interruptions = {
    blur: () => window.dispatchEvent(new Event("blur")),
    hidden: () => {
      hidden = true;
      document.dispatchEvent(new Event("visibilitychange"));
    },
    pagehide: () => window.dispatchEvent(new Event("pagehide")),
    resize: () => window.dispatchEvent(new Event("resize")),
    escape: () => document.dispatchEvent(new KeyboardEvent("keydown", { code: "Escape" })),
    pointercancel: () => document.dispatchEvent(pointer("pointercancel")),
    "missed release": () =>
      document.dispatchEvent(pointer("pointermove", { buttons: 0, clientY: 100 })),
  };
  for (const [name, interrupt] of Object.entries(interruptions)) {
    it.each([false, true])(`cancels on ${name}, started=%s, and ignores late events`, (started) => {
      const drag = gesture();
      if (started) document.dispatchEvent(pointer("pointermove", { clientY: 20 }));
      interrupt();
      document.dispatchEvent(pointer("pointermove", { clientY: 100 }));
      document.dispatchEvent(pointer("pointerup", { buttons: 0 }));
      drag.sensor.cancel();
      expect(drag.onCancel).toHaveBeenCalledOnce();
      expect(drag.onEnd).not.toHaveBeenCalled();
      expect(drag.onFinish).toHaveBeenCalledOnce();
      expect(drag.onStart).toHaveBeenCalledTimes(started ? 1 : 0);
      expect(drag.onAbort).toHaveBeenCalledTimes(started ? 0 : 1);

      hidden = false;
      const next = gesture();
      document.dispatchEvent(pointer("pointermove", { clientY: 20 }));
      document.dispatchEvent(pointer("pointerup", { buttons: 0 }));
      expect(next.onStart).toHaveBeenCalledOnce();
      expect(next.onEnd).toHaveBeenCalledOnce();
    });
  }

  it("hands claimed moves and drops to the context handlers and cancels the sort", () => {
    const onMove = vi.fn((point: { x: number; y: number }) => point.x > 50);
    const onDrop = vi.fn((point: { x: number; y: number }) => point.x > 50);
    const drag = gesture({ onMove, onDrop });
    document.dispatchEvent(pointer("pointermove", { clientY: 20 }));
    document.dispatchEvent(pointer("pointermove", { clientX: 30, clientY: 30 }));
    expect(drag.onMove).toHaveBeenNthCalledWith(1, { x: 10, y: 20 });
    expect(drag.onMove).toHaveBeenNthCalledWith(2, { x: 30, y: 30 });
    document.dispatchEvent(pointer("pointermove", { clientX: 90, clientY: 30 }));
    expect(drag.onMove).toHaveBeenCalledTimes(2);
    document.dispatchEvent(pointer("pointerup", { buttons: 0, clientX: 90, clientY: 30 }));
    expect(onDrop).toHaveBeenCalledExactlyOnceWith({ x: 90, y: 30 });
    expect(drag.onCancel).toHaveBeenCalledOnce();
    expect(drag.onEnd).not.toHaveBeenCalled();
    expect(drag.onFinish).toHaveBeenCalledExactlyOnceWith(true);

    const inside = gesture({ onMove, onDrop });
    document.dispatchEvent(pointer("pointermove", { clientY: 20 }));
    document.dispatchEvent(pointer("pointerup", { buttons: 0, clientX: 10, clientY: 20 }));
    expect(inside.onEnd).toHaveBeenCalledOnce();
    expect(inside.onCancel).not.toHaveBeenCalled();
  });

  it("drops context when a zero-button move precedes pointerup", () => {
    const onDrop = vi.fn((point: { x: number; y: number }) => point.x > 50);
    const drag = gesture({ onMove: (point) => point.x > 50, onDrop });
    document.dispatchEvent(pointer("pointermove", { clientY: 20 }));
    document.dispatchEvent(pointer("pointermove", { clientX: 90, clientY: 30 }));

    // Native desktop releases can report the released button on the final move.
    document.dispatchEvent(pointer("pointermove", { buttons: 0, clientX: 94, clientY: 32 }));
    document.dispatchEvent(pointer("pointerup", { buttons: 0, clientX: 94, clientY: 32 }));
    document.dispatchEvent(pointer("pointermove", { buttons: 0, clientX: 100, clientY: 40 }));

    expect(onDrop).toHaveBeenCalledExactlyOnceWith({ x: 94, y: 32 });
    expect(drag.onCancel).toHaveBeenCalledOnce();
    expect(drag.onEnd).not.toHaveBeenCalled();
    expect(drag.onFinish).toHaveBeenCalledExactlyOnceWith(true);
  });

  it("cancels a missed sidebar release without committing a reorder", () => {
    const onDrop = vi.fn((point: { x: number; y: number }) => point.x > 50);
    const drag = gesture({ onMove: (point) => point.x > 50, onDrop });
    document.dispatchEvent(pointer("pointermove", { clientX: 90, clientY: 30 }));
    document.dispatchEvent(pointer("pointermove", { buttons: 0, clientX: 30, clientY: 40 }));
    document.dispatchEvent(pointer("pointerup", { buttons: 0, clientX: 30, clientY: 40 }));

    expect(onDrop).toHaveBeenCalledExactlyOnceWith({ x: 30, y: 40 });
    expect(drag.onCancel).toHaveBeenCalledOnce();
    expect(drag.onEnd).not.toHaveBeenCalled();
    expect(drag.onFinish).toHaveBeenCalledExactlyOnceWith(true);
  });

  it("does not drop context when the button is released before activation", () => {
    const onDrop = vi.fn(() => true);
    const drag = gesture({ onDrop });
    document.dispatchEvent(pointer("pointermove", { buttons: 0, clientX: 90, clientY: 30 }));
    document.dispatchEvent(pointer("pointerup", { buttons: 0, clientX: 90, clientY: 30 }));

    expect(onDrop).not.toHaveBeenCalled();
    expect(drag.onStart).not.toHaveBeenCalled();
    expect(drag.onAbort).toHaveBeenCalledOnce();
    expect(drag.onFinish).toHaveBeenCalledExactlyOnceWith(false);
  });

  it("resumes reordering when a context drag returns to the sidebar", () => {
    const drag = gesture({
      onMove: (point) => point.x > 50,
      onDrop: (point) => point.x > 50,
    });
    document.dispatchEvent(pointer("pointermove", { clientY: 20 }));
    document.dispatchEvent(pointer("pointermove", { clientX: 30, clientY: 30 }));
    document.dispatchEvent(pointer("pointermove", { clientX: 90, clientY: 100 }));
    document.dispatchEvent(pointer("pointermove", { clientX: 90, clientY: 200 }));
    expect(drag.onMove).toHaveBeenCalledTimes(2);
    expect(drag.onMove).toHaveBeenLastCalledWith({ x: 30, y: 30 });

    document.dispatchEvent(pointer("pointermove", { clientX: 30, clientY: 60 }));
    expect(drag.onMove).toHaveBeenLastCalledWith({ x: 30, y: 60 });
    document.dispatchEvent(pointer("pointerup", { buttons: 0, clientX: 30, clientY: 60 }));
    expect(drag.onEnd).toHaveBeenCalledOnce();
    expect(drag.onCancel).not.toHaveBeenCalled();
  });

  it.each([false, true])("suppresses a cancelled release click, started=%s", (started) => {
    const drag = gesture();
    if (started) document.dispatchEvent(pointer("pointermove", { clientY: 20 }));
    drag.sensor.cancel();
    vi.advanceTimersByTime(1000);
    const releaseClick = new Event("click");
    const releasePropagation = vi.spyOn(releaseClick, "stopPropagation");
    document.dispatchEvent(releaseClick);
    expect(releasePropagation).toHaveBeenCalledOnce();
    document.dispatchEvent(pointer("pointerdown"));
    const nextClick = new Event("click");
    const nextPropagation = vi.spyOn(nextClick, "stopPropagation");
    document.dispatchEvent(nextClick);
    expect(nextPropagation).not.toHaveBeenCalled();
  });

  it.each([false, true])("allows a fresh click after an outside release, started=%s", (started) => {
    const drag = gesture();
    if (started) document.dispatchEvent(pointer("pointermove", { clientY: 20 }));
    drag.sensor.cancel();
    document.dispatchEvent(pointer("pointerdown"));
    const click = new Event("click");
    const propagation = vi.spyOn(click, "stopPropagation");
    document.dispatchEvent(click);
    expect(propagation).not.toHaveBeenCalled();
  });

  it("does not move after cancellation during activation", () => {
    const drag = gesture();
    drag.onStart.mockImplementation(() => drag.sensor.cancel());
    document.dispatchEvent(pointer("pointermove", { clientY: 20 }));
    expect(drag.onCancel).toHaveBeenCalledOnce();
    expect(drag.onMove).not.toHaveBeenCalled();
    expect(drag.onFinish).toHaveBeenCalledOnce();
  });

  it("ignores events from other pointers", () => {
    const drag = gesture();
    document.dispatchEvent(pointer("pointermove", { pointerId: 2, buttons: 0, clientY: 100 }));
    document.dispatchEvent(pointer("pointercancel", { pointerId: 2 }));
    document.dispatchEvent(pointer("pointerup", { pointerId: 2 }));
    expect(drag.onStart).not.toHaveBeenCalled();
    expect(drag.onFinish).not.toHaveBeenCalled();
    document.dispatchEvent(pointer("pointermove", { clientY: 20 }));
    expect(drag.onStart).toHaveBeenCalledOnce();
  });

  it.each([false, true])("cancels when search unmounts the list, started=%s", (started) => {
    const root = createRoot(document.createElement("div"));
    let drag: ReturnType<typeof gesture> | undefined;
    act(() =>
      root.render(
        <StrictMode>
          <SidebarDragLifecycle onUnmount={() => drag?.sensor.cancel()} />
        </StrictMode>,
      ),
    );
    drag = gesture();
    if (started) document.dispatchEvent(pointer("pointermove", { clientY: 20 }));
    act(() => root.unmount());
    document.dispatchEvent(pointer("pointermove", { clientY: 100 }));
    document.dispatchEvent(pointer("pointerup", { buttons: 0 }));
    expect(drag.onStart).toHaveBeenCalledTimes(started ? 1 : 0);
    expect(drag.onCancel).toHaveBeenCalledOnce();
    expect(drag.onEnd).not.toHaveBeenCalled();
    expect(drag.onFinish).toHaveBeenCalledOnce();
  });

  it("detaches a cancelled sensor before a replacement gesture starts", () => {
    const previous = gesture();
    document.dispatchEvent(pointer("pointermove", { clientY: 20 }));
    previous.sensor.cancel();
    const next = gesture();
    document.dispatchEvent(pointer("pointermove", { clientY: 20 }));
    document.dispatchEvent(pointer("pointerup", { buttons: 0 }));
    expect(previous.onCancel).toHaveBeenCalledOnce();
    expect(previous.onEnd).not.toHaveBeenCalled();
    expect(next.onEnd).toHaveBeenCalledOnce();
  });
});

describe("pinned Workstream viewport pointer drops", () => {
  it("hits the release target after scrolling despite a clamped dnd modifier, then unpins after commit", async () => {
    const container = document.createElement("div");
    document.body.append(container);
    const root = createRoot(container);
    const assigned: string[] = [];
    const deltas: Array<{ x: number; y: number }> = [];
    const ordinaryDrop = vi.fn();
    const failures = vi.fn();
    let commit!: (value: boolean) => void;
    const thread = {
      id: "pinned",
      environmentId: "synthetic",
      pinnedAt: "2026-10-01T00:00:00Z",
    } as EnvironmentThreadShell;
    const hit = (point: { x: number; y: number }) =>
      document
        .elementFromPoint(point.x, point.y)
        ?.closest<HTMLElement>("[data-workstream-target]") ?? null;
    const bridge = {
      locateExternal: (point: { x: number; y: number }) => {
        const target = hit(point);
        container
          .querySelectorAll("[data-preview]")
          .forEach((element) => element.removeAttribute("data-preview"));
        target?.setAttribute("data-preview", "true");
        return target !== null;
      },
      dropExternal: (point: { x: number; y: number }) => {
        const target = hit(point);
        if (!target) return null;
        assigned.push(target.dataset.workstreamTarget!);
        return new Promise<boolean>((resolve) => {
          commit = resolve;
        });
      },
      clear: () =>
        container
          .querySelectorAll("[data-preview]")
          .forEach((element) => element.removeAttribute("data-preview")),
    };
    function Source() {
      const { setNodeRef, listeners, attributes } = useDraggable({ id: "pinned" });
      return (
        <div
          ref={setNodeRef}
          {...listeners}
          {...attributes}
          data-pinned-source
          style={{ height: 30 }}
        >
          Pinned thread
        </div>
      );
    }
    function Harness() {
      const [pinned, setPinned] = useState(true);
      const sensors = useSensors(
        useSensor(SidebarPointerSensor, {
          distance: 6,
          onAttach: (sensor: SidebarPointerSensor) => {
            sensorsToCancel.push(sensor);
          },
          onFinish: bridge.clear,
          onMove: (point: { x: number; y: number }) => {
            bridge.locateExternal(point);
            return false;
          },
          onDrop: (point: { x: number; y: number }) =>
            dropPinnedThreadOnWorkstream({
              bridge,
              point,
              thread,
              unpin: async () => setPinned(false),
              onError: failures,
            }),
        }),
      );
      return (
        <DndContext
          sensors={sensors}
          autoScroll={false}
          modifiers={[({ transform }) => ({ ...transform, x: 0, y: Math.min(5, transform.y) })]}
          onDragMove={(event) => deltas.push(event.delta)}
          onDragEnd={ordinaryDrop}
        >
          <div
            style={{
              position: "fixed",
              left: 0,
              top: 0,
              width: 240,
              background: "white",
              zIndex: 1000,
            }}
          >
            {pinned ? <Source /> : <div data-unpinned>Assigned thread</div>}
            <div data-scroll-shelf style={{ height: 180, overflowY: "scroll" }}>
              <div style={{ height: 250 }} />
              <div data-workstream-target="alpha" style={{ height: 80 }}>
                Alpha
              </div>
              <div data-workstream-target="beta" style={{ height: 80 }}>
                Beta
              </div>
              <div style={{ height: 300 }} />
            </div>
          </div>
        </DndContext>
      );
    }
    const sensorsToCancel: SidebarPointerSensor[] = [];
    try {
      await act(async () => root.render(<Harness />));
      const shelf = container.querySelector<HTMLElement>("[data-scroll-shelf]")!;
      const source = container.querySelector<HTMLElement>("[data-pinned-source]")!;
      await act(async () =>
        source.dispatchEvent(pointer("pointerdown", { isPrimary: true, clientX: 40, clientY: 15 })),
      );
      await act(async () =>
        document.dispatchEvent(pointer("pointermove", { clientX: 40, clientY: 80 })),
      );
      await act(async () => {
        shelf.scrollTop = 240;
        shelf.dispatchEvent(new Event("scroll"));
        await vi.advanceTimersByTimeAsync(20);
      });
      const alpha = container.querySelector<HTMLElement>('[data-workstream-target="alpha"]')!;
      const beta = container.querySelector<HTMLElement>('[data-workstream-target="beta"]')!;
      const alphaY = alpha.getBoundingClientRect().top + 20;
      const betaY = beta.getBoundingClientRect().top + 20;
      await act(async () =>
        document.dispatchEvent(pointer("pointermove", { clientX: 40, clientY: alphaY })),
      );
      expect(alpha.getAttribute("data-preview")).toBe("true");
      expect(deltas.length).toBeGreaterThan(0);
      const adjusted = deltas.at(-1)!;
      expect(hit({ x: 40 + adjusted.x, y: 15 + adjusted.y })).not.toBe(alpha);
      // Release can move to a different target without a final pointermove.
      await act(async () =>
        document.dispatchEvent(pointer("pointerup", { clientX: 40, clientY: betaY, buttons: 0 })),
      );
      expect(assigned).toEqual(["beta"]);
      expect(ordinaryDrop).not.toHaveBeenCalled();
      expect(container.querySelector("[data-pinned-source]")).not.toBeNull();
      expect(container.querySelector("[data-preview]")).toBeNull();
      await act(async () => commit(true));
      expect(container.querySelector("[data-pinned-source]")).toBeNull();
      expect(container.querySelector("[data-unpinned]")).not.toBeNull();
      expect(failures).not.toHaveBeenCalled();
    } finally {
      await act(async () => {
        sensorsToCancel.forEach((sensor) => sensor.cancel());
        root.unmount();
      });
      container.remove();
    }
  });

  it("clears cancellation feedback and never reuses an earlier release position", async () => {
    const thread = {
      id: "pinned",
      environmentId: "synthetic",
      pinnedAt: "2026-10-01T00:00:00Z",
    } as EnvironmentThreadShell;
    let preview: { x: number; y: number } | null = null;
    const assigned: Array<{ x: number; y: number }> = [];
    const unpin = vi.fn(async () => undefined);
    const bridge = {
      locateExternal: (point: { x: number; y: number }) => {
        preview = point;
        return true;
      },
      dropExternal: (point: { x: number; y: number }) => {
        assigned.push(point);
        return Promise.resolve(true);
      },
      clear: () => {
        preview = null;
      },
    };
    const options = {
      onMove: (point: { x: number; y: number }) => {
        bridge.locateExternal(point);
        return false;
      },
      onDrop: (point: { x: number; y: number }) =>
        dropPinnedThreadOnWorkstream({ bridge, point, thread, unpin, onError: vi.fn() }),
      onFinish: bridge.clear,
    };
    const cancelled = gesture(options);
    document.dispatchEvent(pointer("pointermove", { clientY: 100 }));
    expect(preview).toEqual({ x: 10, y: 100 });
    document.dispatchEvent(new KeyboardEvent("keydown", { code: "Escape" }));
    expect(preview).toBeNull();
    document.dispatchEvent(pointer("pointerup", { clientY: 110, buttons: 0 }));
    expect(assigned).toEqual([]);
    expect(unpin).not.toHaveBeenCalled();
    expect(cancelled.onEnd).not.toHaveBeenCalled();

    const click = gesture(options);
    document.dispatchEvent(pointer("pointerup", { clientY: 110, buttons: 0 }));
    expect(click.onAbort).toHaveBeenCalledOnce();
    expect(assigned).toEqual([]);

    const next = gesture(options);
    document.dispatchEvent(pointer("pointermove", { clientY: 40 }));
    document.dispatchEvent(pointer("pointerup", { clientY: 50, buttons: 0 }));
    await Promise.resolve();
    expect(assigned).toEqual([{ x: 10, y: 50 }]);
    expect(unpin).toHaveBeenCalledOnce();
    expect(next.onCancel).toHaveBeenCalledOnce();
    expect(next.onEnd).not.toHaveBeenCalled();
    expect(preview).toBeNull();
  });
});
