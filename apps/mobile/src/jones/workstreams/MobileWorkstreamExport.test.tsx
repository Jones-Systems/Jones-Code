import { act, type ReactNode } from "react";
import { createRoot, type Root } from "react-dom/client";
import { afterEach, beforeEach, expect, it, vi } from "vite-plus/test";
import type { EnvironmentNativeCompletedExport } from "@t3tools/client-runtime/jones/workstreams/native-http";
import type { MobileWorkstreamSnapshot } from "../../features/workstreams/projection";
import { data } from "../../features/workstreams/actions.fixtures";
import { MobileWorkstreamExport } from "./MobileWorkstreamExport";
const mocks = vi.hoisted(() => ({ share: vi.fn() }));
vi.mock("../../lib/attachmentDownload", () => ({ shareGeneratedAttachment: mocks.share }));
vi.mock("react-native", () => ({
  View: ({ children }: { children?: ReactNode }) => <div>{children}</div>,
  Text: ({ children }: { children?: ReactNode }) => <span>{children}</span>,
  Pressable: ({
    children,
    onPress,
    disabled,
  }: {
    children?: ReactNode;
    onPress: () => void;
    disabled: boolean;
  }) => (
    <button onClick={onPress} disabled={disabled}>
      {children}
    </button>
  ),
}));
type Props = Parameters<typeof MobileWorkstreamExport>[0];
const snapshot = {
  prepared: { environmentId: "env:member" },
  generation: 1,
  data,
  stale: false,
  nativeCapability: { contract: "workstreams-native/1.0.0", mode: "native-home" },
} as MobileWorkstreamSnapshot;
let root: Root;
let container: HTMLDivElement;
const completed = (bytes: Uint8Array): EnvironmentNativeCompletedExport => ({
  bytes,
  completion: {
    complete: true,
    pages: [],
    preTrailerBytes: 0,
    trailer: {
      complete: true,
      registry_version: data.binding.registryVersion,
      sha256: "a".repeat(64),
    },
  },
});
const render = async (props: Props) => {
  await act(async () => root.render(<MobileWorkstreamExport {...props} />));
};
const click = async () => {
  await act(async () => container.querySelector("button")!.click());
};
beforeEach(() => {
  vi.stubGlobal("IS_REACT_ACT_ENVIRONMENT", true);
  mocks.share.mockReset();
  mocks.share.mockResolvedValue(true);
  container = document.createElement("div");
  document.body.append(container);
  root = createRoot(container);
});
afterEach(async () => {
  await act(async () => root.unmount());
  container.remove();
  vi.unstubAllGlobals();
});
it("shares only the completed exact bytes with read access and the native source anchor", async () => {
  const bytes = new TextEncoder().encode('{"name":"雪"}\r\n{"complete":true}\n');
  const load = vi.fn(async () => completed(bytes));
  await render({
    api: { bindingRevision: "original", exportNative: load },
    snapshot: {
      ...snapshot,
      data: { ...data, binding: { ...data.binding, permissions: ["workstreams:read"] } },
    },
  });
  await click();
  expect(load).toHaveBeenCalledTimes(1);
  expect(mocks.share).toHaveBeenCalledWith({
    bytes,
    attachment: { name: "workstreams.ndjson", mimeType: "application/x-ndjson" },
    signal: expect.any(AbortSignal),
    sourceIdentifier: "workstreams-export",
  });
});
it("cancels replaced binding and discards late completion", async () => {
  let resolve!: (value: EnvironmentNativeCompletedExport) => void;
  let signal: AbortSignal | undefined;
  const load = vi.fn<NonNullable<Props["api"]["exportNative"]>>((_snapshot, options) => {
    signal = options?.signal;
    return new Promise<EnvironmentNativeCompletedExport>((yes) => {
      resolve = yes;
    });
  });
  const api = { bindingRevision: "original", exportNative: load };
  await render({ api, snapshot });
  await click();
  await click();
  await render({ api: { ...api, bindingRevision: "replacement" }, snapshot });
  expect(signal?.aborted).toBe(true);
  await act(async () => resolve(completed(new Uint8Array([1]))));
  expect(mocks.share).not.toHaveBeenCalled();
});
it("refuses stale data and missing read permission without invoking export", async () => {
  const load = vi.fn(async () => completed(new Uint8Array()));
  for (const current of [
    { ...snapshot, stale: true },
    { ...snapshot, data: { ...data, binding: { ...data.binding, permissions: [] } } },
  ]) {
    await render({ api: { bindingRevision: "original", exportNative: load }, snapshot: current });
    await click();
  }
  expect(load).not.toHaveBeenCalled();
});
it("does not share failed exports and keeps protected failure text out of the UI", async () => {
  await render({
    api: {
      bindingRevision: "original",
      exportNative: async () => {
        throw new Error("private-token");
      },
    },
    snapshot,
  });
  await click();
  expect(mocks.share).not.toHaveBeenCalled();
  expect(container.textContent).toContain("Export could not be completed");
  expect(container.textContent).not.toContain("private-token");
});
it("hides the action for legacy or unsupported environments", async () => {
  await render({ api: { bindingRevision: "legacy" }, snapshot });
  expect(container.querySelector("button")).toBeNull();
  await render({
    api: {
      bindingRevision: "legacy",
      exportNative: async () => completed(new Uint8Array()),
    },
    snapshot: { ...snapshot, nativeCapability: undefined },
  });
  expect(container.querySelector("button")).toBeNull();
});

it("reports an unavailable save/share sheet without claiming the export was saved", async () => {
  mocks.share.mockResolvedValue(false);
  await render({
    api: {
      bindingRevision: "original",
      exportNative: async () => completed(new Uint8Array([1])),
    },
    snapshot,
  });
  await click();
  expect(container.textContent).toContain("Export could not be completed");
  expect(container.textContent).not.toContain("downloaded");
});
