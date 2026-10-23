// @vitest-environment jsdom
import { act } from "react";
import { createRoot, type Root } from "react-dom/client";
import { beforeEach, afterEach, describe, expect, it, vi } from "vite-plus/test";
import {
  WorkstreamOperationError,
  WorkstreamOperationJoined,
} from "@t3tools/client-runtime/jones/workstreams/operations";
import type { WorkstreamListView } from "../../state/workstreams";
import { WorkstreamPicker } from "./WorkstreamPicker";
import { operationFixtureData, operationFixtureResult } from "./operations.fixtures";
let root: Root;
let container: HTMLDivElement;
let controller: WorkstreamListView;
const close = vi.fn();
const button = (text: string) =>
  [...document.querySelectorAll<HTMLButtonElement>("button")].find(
    (item) => item.textContent === text,
  )!;
const render = async () =>
  act(async () =>
    root.render(
      <WorkstreamPicker
        controller={controller}
        thread={{ environmentId: "env:a", id: "thread", title: "Synthetic thread" }}
        onClose={close}
        commandId={async () => "operation-fixture-0001"}
      />,
    ),
  );
const search = async (value: string) =>
  act(async () => {
    const input = document.querySelector<HTMLInputElement>('[aria-label="Search Workstreams"]')!;
    Object.getOwnPropertyDescriptor(HTMLInputElement.prototype, "value")!.set!.call(input, value);
    input.dispatchEvent(new Event("input", { bubbles: true }));
  });
beforeEach(() => {
  vi.stubGlobal("IS_REACT_ACT_ENVIRONMENT", true);
  container = document.createElement("div");
  document.body.append(container);
  root = createRoot(container);
  close.mockReset();
  controller = {
    data: operationFixtureData,
    error: null,
    loading: false,
    placements: null,
    placementInventory: { coverage: "complete", identities: [], json: "[]", totalIdentities: 0 },
    refresh: vi.fn(),
    submit: vi.fn(),
    runBindingOperation: vi.fn(),
    loadDetail: vi.fn(),
    loadReference: vi.fn(),
    assignNativeThread: vi.fn(async () => {}),
    createWorkstream: vi.fn(async () => "created"),
  };
});
afterEach(async () => {
  await act(async () => root.unmount());
  container.remove();
  vi.unstubAllGlobals();
});
describe("Workstream picker", () => {
  it("offers the exact existing name before creation and supports explicit Unassigned", async () => {
    await render();
    await search("alpha");
    expect(button("alpha (existing)")).toBeDefined();
    expect(button("Create 'alpha'")).toBeUndefined();
    await act(async () => button("Unassigned").click());
    expect(controller.assignNativeThread).toHaveBeenCalledWith(
      expect.objectContaining({ id: "thread" }),
      null,
      expect.anything(),
    );
    expect(controller.createWorkstream).not.toHaveBeenCalled();
    expect(close).toHaveBeenCalledOnce();
  });
  it("coalesces create clicks and waits for assignment before closing without changing display data", async () => {
    let finish!: (value: string) => void;
    vi.mocked(controller.createWorkstream!).mockReturnValue(
      new Promise((resolve) => {
        finish = resolve;
      }),
    );
    await render();
    await search("New objective");
    await act(async () => {
      button("Create 'New objective'").click();
      button("Create 'New objective'").click();
    });
    expect(controller.createWorkstream).toHaveBeenCalledTimes(1);
    expect(controller.assignNativeThread).not.toHaveBeenCalled();
    expect(close).not.toHaveBeenCalled();
    expect(controller.data?.items).toEqual(operationFixtureData.items);
    await act(async () => {
      finish("created");
    });
    expect(controller.assignNativeThread).toHaveBeenCalledWith(
      expect.anything(),
      "created",
      expect.anything(),
    );
    expect(close).toHaveBeenCalledOnce();
  });
  it("shows observed conflict and retries only after the explicit retry action", async () => {
    vi.mocked(controller.assignNativeThread!).mockRejectedValueOnce(
      new WorkstreamOperationError(operationFixtureResult("conflict")),
    );
    await render();
    await act(async () => button("alpha").click());
    expect(document.body.textContent).toContain("Currently in Beta");
    expect(close).not.toHaveBeenCalled();
    expect(controller.assignNativeThread).toHaveBeenCalledTimes(1);
    await act(async () => button("Retry").click());
    expect(controller.assignNativeThread).toHaveBeenLastCalledWith(
      expect.anything(),
      "alpha",
      expect.objectContaining({ retry: true }),
    );
  });
  it("keeps a pending choice open until its operation is committed", async () => {
    const pending = operationFixtureResult("reconciling");
    vi.mocked(controller.assignNativeThread!).mockRejectedValueOnce(
      new WorkstreamOperationError(pending),
    );
    controller = {
      ...controller,
      operationEntries: [
        { key: "thread:thread", request: null, operationId: pending.operation_id, result: pending },
      ],
    };
    await render();
    await act(async () => button("beta").click());
    expect(document.body.textContent).toContain("Reconciling");
    expect(close).not.toHaveBeenCalled();
    controller = {
      ...controller,
      operationEntries: [
        {
          key: "thread:thread",
          request: null,
          operationId: pending.operation_id,
          result: operationFixtureResult(),
        },
      ],
    };
    await render();
    expect(close).toHaveBeenCalled();
  });
  it("joins an existing operation without closing as though our choice committed", async () => {
    const pending = operationFixtureResult("reconciling");
    vi.mocked(controller.assignNativeThread!).mockRejectedValueOnce(
      new WorkstreamOperationJoined(pending.operation_id),
    );
    controller = {
      ...controller,
      operationEntries: [
        {
          key: "joined:thread:thread",
          request: null,
          operationId: pending.operation_id,
          result: pending,
        },
      ],
    };
    await render();
    await act(async () => button("alpha").click());
    expect(close).not.toHaveBeenCalled();
    controller = {
      ...controller,
      operationEntries: [
        {
          key: "joined:thread:thread",
          request: null,
          operationId: pending.operation_id,
          result: operationFixtureResult(),
        },
      ],
    };
    await render();
    expect(close).not.toHaveBeenCalled();
    expect(document.body.textContent).toContain("Retry your choice");
    expect(controller.assignNativeThread).toHaveBeenCalledTimes(1);
  });
  it("aborts on unmount and ignores late results", async () => {
    let finish!: () => void;
    vi.mocked(controller.assignNativeThread!).mockReturnValue(
      new Promise((resolve) => {
        finish = resolve;
      }),
    );
    await render();
    await act(async () => button("beta").click());
    const signal = vi.mocked(controller.assignNativeThread!).mock.calls[0]![2].signal!;
    await act(async () => root.render(null));
    expect(signal.aborted).toBe(true);
    await act(async () => finish());
    expect(close).not.toHaveBeenCalled();
  });
});
