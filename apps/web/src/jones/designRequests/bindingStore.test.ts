import { DESIGN_REQUEST_HELD_REASONS } from "@t3tools/contracts/jones/designRequests";
import { describe, expect, it } from "vite-plus/test";

import {
  DESIGN_REQUEST_BINDINGS_KEY,
  findDesignRequestBinding,
  pairDesignRequestBinding,
  readDesignRequestBindings,
  removeDesignRequestBinding,
} from "./bindingStore";
import { DESIGN_REQUEST_HELD_FIXES } from "./heldReasons";
import { registryStatusFromError } from "./registry";

const memoryStorage = () => {
  const values = new Map<string, string>();
  return {
    getItem: (key: string) => values.get(key) ?? null,
    setItem: (key: string, value: string) => void values.set(key, value),
  };
};

describe("design request bindings", () => {
  it("pairs one origin and project to one existing workstream and replaces it on re-pair", () => {
    const storage = memoryStorage();
    const origin = "http://100.113.248.16:4339";
    const first = pairDesignRequestBinding(
      storage,
      { galleryOrigin: origin, projectKey: "form2", workstreamId: "ws-1" },
      () => "a".repeat(32),
    );
    pairDesignRequestBinding(
      storage,
      { galleryOrigin: origin, projectKey: "other", workstreamId: "ws-2" },
      () => "b".repeat(32),
    );
    expect(findDesignRequestBinding(readDesignRequestBindings(storage), origin, "form2")).toEqual(
      first,
    );
    pairDesignRequestBinding(
      storage,
      { galleryOrigin: origin, projectKey: "form2", workstreamId: "ws-3" },
      () => "c".repeat(32),
    );
    const bindings = readDesignRequestBindings(storage);
    expect(bindings).toHaveLength(2);
    expect(findDesignRequestBinding(bindings, origin, "form2")?.workstreamId).toBe("ws-3");
    // Origin and project must both match; a different port is a different origin.
    expect(findDesignRequestBinding(bindings, "http://100.113.248.16:4340", "form2")).toBeNull();
    removeDesignRequestBinding(storage, "c".repeat(32));
    expect(
      findDesignRequestBinding(readDesignRequestBindings(storage), origin, "form2"),
    ).toBeNull();
  });

  it("treats malformed or extended stored state as no bindings", () => {
    const storage = memoryStorage();
    storage.setItem(DESIGN_REQUEST_BINDINGS_KEY, "{not json");
    expect(readDesignRequestBindings(storage)).toEqual([]);
    storage.setItem(
      DESIGN_REQUEST_BINDINGS_KEY,
      JSON.stringify({
        version: 1,
        bindings: [
          {
            bindingId: "a".repeat(32),
            galleryOrigin: "x",
            projectKey: "p",
            workstreamId: "w",
            threadId: "t",
          },
        ],
      }),
    );
    expect(readDesignRequestBindings(storage)).toEqual([]);
  });
});

describe("design request registry status", () => {
  it("separates missing APIs and sign-in failures from transient errors", () => {
    expect(registryStatusFromError({ _tag: "EnvironmentHttpUnauthorizedError" })).toBe(
      "unauthenticated",
    );
    expect(registryStatusFromError({ _tag: "ResponseError", response: { status: 403 } })).toBe(
      "unauthenticated",
    );
    expect(registryStatusFromError({ _tag: "ResponseError", response: { status: 404 } })).toBe(
      "unsupported",
    );
    expect(registryStatusFromError(new Error("network"))).toBe("error");
  });

  it("has a fix sentence for every held reason", () => {
    for (const reason of DESIGN_REQUEST_HELD_REASONS)
      expect(DESIGN_REQUEST_HELD_FIXES[reason].length).toBeGreaterThan(10);
  });
});
