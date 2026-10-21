import type { SensorProps } from "@dnd-kit/core";
import { describe, expect, it, vi } from "vite-plus/test";
import { fakeTimers } from "../../test/render";
import { SidebarPointerSensor } from "./Sidebar.pointer";

const sensors: SidebarPointerSensor[] = [];

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

function setup() {
  const stack = new DisposableStack();
  stack.use(fakeTimers());
  stack.defer(() => {
    for (const sensor of sensors.splice(0)) sensor.cancel();
    // Cancelled sensors keep suppressing clicks on the shared document until the next press.
    document.dispatchEvent(pointer("pointerdown"));
    vi.runAllTimers();
  });
  return stack;
}

describe("sidebar pointer lifecycle", () => {
  it("drops context when a zero-button move precedes pointerup", () => {
    using _environment = setup();
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

  // regression: #17373 a zero-button release outside a drop target must cancel, not reorder
  it("cancels a missed sidebar release without committing a reorder", () => {
    using _environment = setup();
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
