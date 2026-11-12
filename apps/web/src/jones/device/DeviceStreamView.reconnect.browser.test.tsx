import { RegistryContext } from "@effect/atom-react";
import { AtomRegistry } from "effect/reactivity";
import { useSyncExternalStore } from "react";
import { act, render, fakeTimers, stubGlobal } from "../../../test/render";
import { EnvironmentId } from "@t3tools/contracts";
import { expect, it, vi } from "vite-plus/test";

const accessStore = {
  value: { httpBase: "http://test", wsBase: "ws://test", query: {}, credentials: true },
  listeners: new Set<() => void>(),
  retire: new Set<() => void>(),
  lifecycle: routeLifecycle(),
  refresh() {
    for (const retire of accessStore.retire) retire();
    accessStore.value = { ...accessStore.value };
    accessStore.lifecycle = routeLifecycle();
    for (const listener of accessStore.listeners) listener();
  },
  subscribe(listener: () => void) {
    accessStore.listeners.add(listener);
    return () => accessStore.listeners.delete(listener);
  },
};
vi.mock(import("~/jones/device/useDeviceStreamRoute"), async (original) => ({
  ...(await original()),
  useDeviceStreamRoute: (() => {
    const access = useSyncExternalStore(accessStore.subscribe, () => accessStore.value);
    return {
      route: { access, kind: "proxy", phase: "connected", generation: "test" },
      proxy: access,
      report: reportRoute,
      unauthorized: refreshRoute,
      retryAccess: refreshRoute,
      lifecycle: accessStore.lifecycle,
    };
  }) as never,
}));
function reportRoute() {}
function refreshRoute() {
  accessStore.refresh();
}
function routeLifecycle() {
  return {
    bind(retire: () => void) {
      accessStore.retire.add(retire);
      return () => {
        accessStore.retire.delete(retire);
      };
    },
    report: reportRoute,
    unauthorized: refreshRoute,
  };
}
import { DeviceStreamView } from "~/components/device/DeviceStreamView";

// MJPEG frames come from the device hub, so images keep their src without loading it and a
// test publishes a frame by setting the natural size and firing "load".
const imageSources = new WeakMap<HTMLImageElement, string>();
const framedImages = new WeakSet<HTMLImageElement>();
let images: HTMLImageElement[] = [];
const srcDescriptor = Object.getOwnPropertyDescriptor(HTMLImageElement.prototype, "src")!;
const naturalWidthDescriptor = Object.getOwnPropertyDescriptor(
  HTMLImageElement.prototype,
  "naturalWidth",
)!;
const naturalHeightDescriptor = Object.getOwnPropertyDescriptor(
  HTMLImageElement.prototype,
  "naturalHeight",
)!;
const removeAttribute = Element.prototype.removeAttribute;
function stubImages() {
  Object.defineProperty(HTMLImageElement.prototype, "src", {
    configurable: true,
    get(this: HTMLImageElement) {
      return imageSources.get(this) ?? "";
    },
    set(this: HTMLImageElement, value: string) {
      if (!images.includes(this)) images.push(this);
      imageSources.set(this, value);
    },
  });
  for (const name of ["naturalWidth", "naturalHeight"]) {
    Object.defineProperty(HTMLImageElement.prototype, name, {
      configurable: true,
      get(this: HTMLImageElement) {
        if (!framedImages.has(this)) return 0;
        return name === "naturalWidth" ? 400 : 800;
      },
    });
  }
  Element.prototype.removeAttribute = function (this: Element, name: string) {
    if (this instanceof HTMLImageElement && name === "src") imageSources.delete(this);
    return removeAttribute.call(this, name);
  };
}
function restoreImages() {
  Object.defineProperty(HTMLImageElement.prototype, "src", srcDescriptor);
  Object.defineProperty(HTMLImageElement.prototype, "naturalWidth", naturalWidthDescriptor);
  Object.defineProperty(HTMLImageElement.prototype, "naturalHeight", naturalHeightDescriptor);
  Element.prototype.removeAttribute = removeAttribute;
}
async function publishFrame(image: HTMLImageElement) {
  framedImages.add(image);
  await act(async () => image.dispatchEvent(new Event("load")));
}
let container: HTMLElement;
async function setup() {
  const resources = new AsyncDisposableStack();
  try {
    resources.use(fakeTimers());
    images = [];
    stubImages();
    resources.defer(restoreImages);
    const videoDecoder = Object.getOwnPropertyDescriptor(globalThis, "VideoDecoder");
    if (videoDecoder) {
      Reflect.deleteProperty(globalThis, "VideoDecoder");
      resources.defer(() => {
        Object.defineProperty(globalThis, "VideoDecoder", videoDecoder);
      });
    }
    resources.use(stubGlobal("fetch", () => Promise.resolve(new Response("prime"))));
    resources.use(
      stubGlobal(
        "WebSocket",
        class {
          static OPEN = 1;
          readyState = 1;
          onmessage?: (event: { data: ArrayBuffer }) => void;
          send() {}
          close() {}
        },
      ),
    );
    const registry = AtomRegistry.make();
    resources.defer(() => registry.dispose());
    container = document.createElement("div");
    container.style.cssText = "width: 400px; height: 800px; display: flex";
    resources.use(
      await render(
        <RegistryContext.Provider value={registry}>
          <DeviceStreamView
            hostId="local"
            environmentId={EnvironmentId.make("test")}
            deviceId="test"
            platform="ios"
            visible={true}
            allowPhoneView={false}
          />
        </RegistryContext.Provider>,
        { container },
      ),
    );
    return Object.assign(resources, { images });
  } catch (error) {
    await resources.disposeAsync();
    throw error;
  }
}

const alert = () => container.querySelector('[role="alert"]');
const reconnect = () =>
  act(async () =>
    [...container.querySelectorAll("button")]
      .find((button) => button.textContent?.includes("Reconnect"))!
      .click(),
  );

it("offers Reconnect after the shared timeout and receives a frame after refreshing access for retry", async () => {
  await using fixture = await setup();
  const { images } = fixture;
  await act(async () => {
    await vi.advanceTimersByTimeAsync(15_000);
  });
  expect(alert()?.textContent).toContain("No video");
  expect(images[0]!.src).toBe("");
  await reconnect();
  expect(alert()).toBeNull();
  const currentImage = container.querySelector("img")!;
  expect(currentImage.src).toContain("stream.mjpeg");
  await publishFrame(currentImage);
  expect(container.querySelector('[role="status"]')?.textContent).toContain("Server connection");
  expect(container.querySelectorAll("button")).toHaveLength(0);
  // Flush the atom registry's deferred removal of the retired session.
  await act(() => vi.advanceTimersByTimeAsync(0));
  expect(vi.getTimerCount()).toBe(0);
});
