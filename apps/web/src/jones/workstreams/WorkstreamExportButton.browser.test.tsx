import { expect, it, vi } from "vite-plus/test";
import { act, render, fakeTimers } from "../../../test/render";
import type { EnvironmentNativeCompletedExport } from "@t3tools/client-runtime/jones/workstreams/native-http";
import { data } from "../../components/workstreams/nativeWorkstreamActions.fixtures";
import { WorkstreamExportButton } from "./WorkstreamExportButton";

function downloadEvidence() {
  let blob: Blob | undefined;
  const createUrl = vi.spyOn(URL, "createObjectURL").mockImplementation((value) => {
    if (!(value instanceof Blob)) throw new Error("Expected export bytes");
    blob = value;
    return "blob:export";
  });
  const revoke = vi.spyOn(URL, "revokeObjectURL").mockImplementation(() => {});
  const download = vi.spyOn(HTMLAnchorElement.prototype, "click").mockImplementation(() => {});
  return {
    get blob() {
      return blob;
    },
    createUrl,
    revoke,
    download,
    [Symbol.dispose]() {
      download.mockRestore();
      revoke.mockRestore();
      createUrl.mockRestore();
    },
  };
}
const completed = (bytes: Uint8Array): EnvironmentNativeCompletedExport => ({
  bytes,
  // Presentation consumes only bytes; validation belongs to the real controller suite.
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
const controller = (
  load: (options?: { signal?: AbortSignal }) => Promise<EnvironmentNativeCompletedExport>,
) => ({ data, stale: false, exportNative: load });
const deferred = <T,>() => {
  let resolve!: (value: T) => void;
  const promise = new Promise<T>((yes) => {
    resolve = yes;
  });
  return { promise, resolve };
};
const click = async (container: HTMLElement) => {
  await act(async () => container.querySelector("button")!.click());
};
it("downloads exact original UTF-8 bytes for read-only access and releases the URL", async () => {
  using io = downloadEvidence();
  const bytes = new TextEncoder().encode('{"name":"雪"}\r\n{"complete":true}\n');
  const load = vi.fn(async () => completed(bytes));
  await using screen = await render(
    <WorkstreamExportButton
      controller={{
        ...controller(load),
        data: {
          ...data,
          binding: { ...data.binding, permissions: ["workstreams:read"] },
        },
      }}
    />,
  );
  using _timers = fakeTimers();
  await click(screen.container);
  expect(load).toHaveBeenCalledTimes(1);
  expect(io.download).toHaveBeenCalledTimes(1);
  expect(io.blob?.type).toBe("application/x-ndjson");
  const actual = await io.blob!.arrayBuffer();
  expect(Array.from(new Uint8Array(actual))).toEqual(Array.from(bytes));
  expect(screen.container.textContent).toContain("downloaded");
  expect(io.revoke).not.toHaveBeenCalled();
  vi.advanceTimersByTime(30_000);
  expect(io.revoke).toHaveBeenCalledWith("blob:export");
});
it("does not release partial or failed exports or expose protected error text", async () => {
  using io = downloadEvidence();
  await using screen = await render(
    <WorkstreamExportButton
      controller={controller(async () => {
        throw new Error("private-token");
      })}
    />,
  );
  await click(screen.container);
  expect(io.createUrl).not.toHaveBeenCalled();
  expect(screen.container.textContent).toContain("Export could not be completed");
  expect(screen.container.textContent).not.toContain("private-token");
});
it.each(["stale", "cached", "permission"] as const)(
  "blocks %s data before requesting bytes",
  async (reason) => {
    using _io = downloadEvidence();
    const load = vi.fn(async () => completed(new Uint8Array()));
    await using screen = await render(
      <WorkstreamExportButton
        controller={{
          ...controller(load),
          data: {
            ...data,
            stale: reason === "stale",
            source: reason === "cached" ? "cache" : "live",
            binding: {
              ...data.binding,
              permissions: reason === "permission" ? [] : ["workstreams:read"],
            },
          },
        }}
      />,
    );
    await click(screen.container);
    expect(load).not.toHaveBeenCalled();
  },
);
it("cancels scope replacement and suppresses late completion even if the producer ignores cancellation", async () => {
  using io = downloadEvidence();
  const pending = deferred<EnvironmentNativeCompletedExport>();
  const load = vi.fn((options?: { signal?: AbortSignal }) => {
    void options;
    return pending.promise;
  });
  await using screen = await render(<WorkstreamExportButton controller={controller(load)} />);
  await click(screen.container);
  await click(screen.container);
  expect(load).toHaveBeenCalledTimes(1);
  const signal = load.mock.calls[0]?.[0]?.signal;
  await screen.rerender(
    <WorkstreamExportButton
      controller={{
        ...controller(load),
        data: {
          ...data,
          binding: {
            ...data.binding,
            authorizationRevision: data.binding.authorizationRevision + 1,
          },
        },
      }}
    />,
  );
  expect(signal?.aborted).toBe(true);
  await act(async () => pending.resolve(completed(new Uint8Array([1]))));
  expect(io.createUrl).not.toHaveBeenCalled();
  expect(screen.container.textContent).not.toContain("downloaded");
});
it("aborts on unmount and hides export when the native producer is absent", async () => {
  using io = downloadEvidence();
  const pending = deferred<EnvironmentNativeCompletedExport>();
  let signal: AbortSignal | undefined;
  await using screen = await render(
    <WorkstreamExportButton
      controller={controller((options) => {
        signal = options?.signal;
        return pending.promise;
      })}
    />,
  );
  await click(screen.container);
  await screen.rerender(<WorkstreamExportButton controller={{ data, stale: false }} />);
  expect(signal?.aborted).toBe(true);
  expect(screen.container.querySelector("button")).toBeNull();
  await act(async () => pending.resolve(completed(new Uint8Array())));
  expect(io.createUrl).not.toHaveBeenCalled();
});
