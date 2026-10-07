import { afterEach, describe, expect, it, vi } from "vite-plus/test";
import type { WorkstreamAppearance, WorkstreamAppearanceWrite } from "@t3tools/contracts";
import { visitElements } from "../../test/reactElementTree";
import { reactHookHarness as hooks } from "../../test/reactHookHarness";

vi.mock("react", async (importOriginal) => {
  const actual = await importOriginal<typeof import("react")>();
  const { reactHookHarness } = await import("../../test/reactHookHarness");
  return {
    ...actual,
    useState: reactHookHarness.useState,
    useRef: reactHookHarness.useRef,
    useMemo: reactHookHarness.useMemo,
    useCallback: reactHookHarness.useCallback,
  };
});
vi.mock("react/compiler-runtime", async () => {
  const { reactHookHarness } = await import("../../test/reactHookHarness");
  return { c: reactHookHarness.useMemoCache };
});

import { WorkstreamColorDialog } from "./WorkstreamColorDialog";

type Props = Parameters<typeof WorkstreamColorDialog>[0];
function render(props: Props) {
  hooks.beginRender();
  return WorkstreamColorDialog(props);
}
function control(tree: unknown, label: string) {
  const found = visitElements(
    tree,
    (element) => element.props["aria-label"] === label || element.props.children === label,
  );
  if (!found) throw new Error(`Missing dialog control: ${label}`);
  return found;
}
function click(tree: unknown, label: string) {
  const element = control(tree, label);
  if (element.props.disabled) throw new Error(`Disabled dialog control: ${label}`);
  const callback = element.props.onClick;
  if (typeof callback !== "function") throw new Error(`Missing click handler: ${label}`);
  callback();
}
function hex(tree: unknown, value: string) {
  const element = control(tree, "Hex color");
  if (element.props.disabled) throw new Error("Hex editor is disabled");
  const callback = element.props.onChange;
  if (typeof callback !== "function") throw new Error("Missing hex change handler");
  callback({ target: { value } });
}
function deferred<T>() {
  let resolve!: (value: T) => void;
  let reject!: (reason: Error) => void;
  const promise = new Promise<T>((success, failure) => {
    resolve = success;
    reject = failure;
  });
  return { promise, resolve, reject };
}
function fixture() {
  const saved = { workstream_id: "workstream:color", border_color: "#2563EB", version: 4 };
  const createCommandId = vi.fn(async () => "appearance-command-0001");
  const onClose = vi.fn();
  const save = vi.fn(async (input: WorkstreamAppearanceWrite): Promise<WorkstreamAppearance> => ({
    workstream_id: input.workstream_id,
    border_color: input.border_color,
    version: 5,
  }));
  return {
    props: { name: "Color feature", saved, generation: 7, createCommandId, onClose, save },
    saved,
    save,
    onClose,
    createCommandId,
  };
}

describe("Workstream border color dialog interactions", () => {
  afterEach(() => hooks.reset());

  it("previews preset and hex edits without writing until Save", async () => {
    const fixtureData = fixture();
    const started = deferred<WorkstreamAppearanceWrite>();
    const response = deferred<WorkstreamAppearance>();
    fixtureData.save.mockImplementation((input) => {
      started.resolve(input);
      return response.promise;
    });
    let tree = render(fixtureData.props);
    click(tree, "Red #DC2626");
    tree = render(fixtureData.props);
    expect(control(tree, "Border preview").props.style).toEqual({ borderLeftColor: "#DC2626" });
    expect(fixtureData.save).not.toHaveBeenCalled();
    hex(tree, "#a1b2c3");
    tree = render(fixtureData.props);
    expect(control(tree, "Border preview").props.style).toEqual({ borderLeftColor: "#A1B2C3" });
    expect(fixtureData.save).not.toHaveBeenCalled();
    expect(fixtureData.saved.border_color).toBe("#2563EB");
    click(tree, "Save");
    const input = await started.promise;
    expect(input).toEqual({
      command_id: "appearance-command-0001",
      workstream_id: "workstream:color",
      expected_server_generation: 7,
      expected_version: 4,
      border_color: "#A1B2C3",
    });
    expect(fixtureData.onClose).not.toHaveBeenCalled();
    response.resolve({
      workstream_id: input.workstream_id,
      border_color: input.border_color,
      version: 5,
    });
    await Promise.resolve();
    expect(fixtureData.onClose).toHaveBeenCalledOnce();
  });

  it("cancels a modified preview without writing or allocating a command", () => {
    const fixtureData = fixture();
    let tree = render(fixtureData.props);
    click(tree, "Red #DC2626");
    tree = render(fixtureData.props);
    hex(tree, "#ABCDEF");
    tree = render(fixtureData.props);
    click(tree, "Cancel");
    expect(fixtureData.onClose).toHaveBeenCalledOnce();
    expect(fixtureData.save).not.toHaveBeenCalled();
    expect(fixtureData.createCommandId).not.toHaveBeenCalled();
    expect(fixtureData.saved).toEqual({
      workstream_id: "workstream:color",
      border_color: "#2563EB",
      version: 4,
    });
  });

  it("freezes an uncertain save and retries its exact command without allocating another ID", async () => {
    const fixtureData = fixture();
    const started = deferred<WorkstreamAppearanceWrite>();
    const first = deferred<WorkstreamAppearance>();
    fixtureData.save.mockImplementationOnce((input) => {
      started.resolve(input);
      return first.promise;
    });
    let tree = render(fixtureData.props);
    hex(tree, "#123ABC");
    tree = render(fixtureData.props);
    click(tree, "Save");
    const originalRequest = await started.promise;
    first.reject(new Error("Unconfirmed response"));
    await Promise.resolve();
    tree = render(fixtureData.props);
    expect(control(tree, "Hex color").props.disabled).toBe(true);
    expect(control(tree, "Red #DC2626").props.disabled).toBe(true);
    expect(control(tree, "Reset to automatic").props.disabled).toBe(true);
    expect(fixtureData.onClose).not.toHaveBeenCalled();
    const retryStarted = deferred<WorkstreamAppearanceWrite>();
    const retryResponse = deferred<WorkstreamAppearance>();
    fixtureData.save.mockImplementationOnce((input) => {
      retryStarted.resolve(input);
      return retryResponse.promise;
    });
    click(tree, "Retry save");
    expect(await retryStarted.promise).toBe(originalRequest);
    expect(fixtureData.createCommandId).toHaveBeenCalledOnce();
    expect(fixtureData.save.mock.calls[1]?.[0]).toEqual(fixtureData.save.mock.calls[0]?.[0]);
    retryResponse.resolve({
      workstream_id: originalRequest.workstream_id,
      border_color: originalRequest.border_color,
      version: 5,
    });
    await Promise.resolve();
    expect(fixtureData.onClose).toHaveBeenCalledOnce();
  });
});
