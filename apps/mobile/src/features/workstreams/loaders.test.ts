import { describe, expect, it, vi } from "vite-plus/test";
import { loadCompleteWorkstreamList } from "./loaders";
import { data } from "./actions.fixtures";
describe("mobile Workstream metadata paging", () => {
  it("completes all pages before publishing metadata", async () => {
    const load = vi.fn(async (cursor?: string) =>
      cursor
        ? { ...data, items: [data.items[1]!] }
        : { ...data, items: [data.items[0]!], nextCursor: "next" },
    );
    expect((await loadCompleteWorkstreamList(load)).items).toEqual(data.items);
    expect(load).toHaveBeenNthCalledWith(2, "next");
  });
  it("rejects a changed principal or registry revision between pages", async () => {
    await expect(
      loadCompleteWorkstreamList(async (cursor) =>
        cursor
          ? { ...data, binding: { ...data.binding, principalId: "different" } }
          : { ...data, nextCursor: "next" },
      ),
    ).rejects.toThrow("binding changed");
  });
  it("does not follow a repeated cursor indefinitely", async () => {
    await expect(
      loadCompleteWorkstreamList(async () => ({ ...data, nextCursor: "next" })),
    ).rejects.toThrow("cursor repeated");
  });
  it("discards a response arriving after its connection was aborted", async () => {
    const controller = new AbortController();
    await expect(
      loadCompleteWorkstreamList(
        async () => {
          controller.abort();
          return data;
        },
        { signal: controller.signal },
      ),
    ).rejects.toThrow();
  });
});
