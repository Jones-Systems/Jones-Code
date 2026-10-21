import { afterEach, describe, expect, it, vi } from "vite-plus/test";
import { findEnvironmentUpdate } from "./environmentMaintenance";

afterEach(() => vi.unstubAllGlobals());

describe("Jones environment release compatibility", () => {
  const signal = new AbortController().signal;
  it("walks release pages to find the host's channel", async () => {
    const fetchMock = vi
      .fn()
      .mockResolvedValueOnce(
        Response.json(Array.from({ length: 100 }, () => ({ tag_name: "v2.0.0" }))),
      )
      .mockResolvedValueOnce(Response.json([{ tag_name: "v1.0.0-nightly.20260923.2" }]));
    vi.stubGlobal("fetch", fetchMock);
    expect(await findEnvironmentUpdate("1.0.0-nightly.20260923.1", signal)).toBe(
      "1.0.0-nightly.20260923.2",
    );
    expect(fetchMock.mock.calls[1]?.[0]).toContain("page=2");
    expect(fetchMock.mock.calls[1]?.[1]).toEqual({ signal });
  });

  it("never queries upstream releases for a Jones preview", async () => {
    const fetchMock = vi.fn();
    vi.stubGlobal("fetch", fetchMock);
    await expect(findEnvironmentUpdate("0.0.44-preview.20261002.100.2", signal)).rejects.toThrow(
      "separate Download and Install",
    );
    expect(fetchMock).not.toHaveBeenCalled();
  });
  it("uses the upstream feed only when the caller identifies an upstream environment", async () => {
    const fetchMock = vi.fn().mockResolvedValue(new Response(
      '<feed><entry><id>tag:github.com,2008:Repository/1/v1.0.0-preview.20260924.3</id></entry></feed>',
    ));
    vi.stubGlobal("fetch", fetchMock);

    expect(await findEnvironmentUpdate("1.0.0-preview.20260923.1", signal, true)).toBe(
      "1.0.0-preview.20260924.3",
    );
    expect(fetchMock).toHaveBeenCalledTimes(1);
    expect(fetchMock).toHaveBeenCalledWith(
      "https://github.com/pingdotgg/t3code/releases.atom",
      { signal, headers: { Accept: "application/atom+xml" } },
    );
  });

});
