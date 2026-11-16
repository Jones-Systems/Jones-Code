import { afterEach, beforeEach, describe, expect, it, vi } from "vite-plus/test";
import {
  PERSONAL_THREAD_LIMITS,
  personalThreadExpandedKey,
  personalThreadLimitKey,
  readPersonalThreadWindow,
  windowPersonalThreads,
} from "./personalThreadWindow";

describe("personal thread window storage", () => {
  let stored: Map<string, string>;

  beforeEach(() => {
    stored = new Map();
    vi.stubGlobal("window", {
      localStorage: { getItem: (key: string) => stored.get(key) ?? null },
    });
    vi.spyOn(console, "error").mockImplementation(() => {});
  });

  afterEach(() => {
    vi.restoreAllMocks();
    vi.unstubAllGlobals();
  });

  it("defaults missing preferences to five threads without expansion", () => {
    expect(readPersonalThreadWindow("work-a")).toEqual({ limit: 5, expanded: false });
  });

  it("reads valid preferences independently for each workstream", () => {
    for (const limit of PERSONAL_THREAD_LIMITS) {
      const id = `work-${limit}`;
      stored.set(personalThreadLimitKey(id), JSON.stringify(limit));
      stored.set(personalThreadExpandedKey(id), "true");
    }
    for (const limit of PERSONAL_THREAD_LIMITS) {
      expect(readPersonalThreadWindow(`work-${limit}`)).toEqual({ limit, expanded: true });
    }
    expect(readPersonalThreadWindow("work-unset")).toEqual({ limit: 5, expanded: false });
  });

  it.each(["not-json", "7", '"10"', "null", "{}"])(
    "falls back for invalid stored limit %s without discarding valid expansion",
    (value) => {
      stored.set(personalThreadLimitKey("work-a"), value);
      stored.set(personalThreadExpandedKey("work-a"), "true");
      expect(readPersonalThreadWindow("work-a")).toEqual({ limit: 5, expanded: true });
    },
  );

  it.each(["not-json", '"true"', "1", "null"])(
    "falls back for invalid stored expansion %s without discarding a valid limit",
    (value) => {
      stored.set(personalThreadLimitKey("work-a"), "15");
      stored.set(personalThreadExpandedKey("work-a"), value);
      expect(readPersonalThreadWindow("work-a")).toEqual({ limit: 15, expanded: false });
    },
  );

  it("uses defaults when browser storage is unavailable", () => {
    vi.stubGlobal("window", {
      get localStorage() {
        throw new Error("storage unavailable");
      },
    });
    expect(readPersonalThreadWindow("work-a")).toEqual({ limit: 5, expanded: false });
  });
});

describe("personal thread window visibility", () => {
  const threads = Array.from({ length: 16 }, (_, index) => ({ id: `thread-${16 - index}` }));

  it("keeps the natural order and thread identities at each limit", () => {
    for (const limit of PERSONAL_THREAD_LIMITS) {
      const visible = windowPersonalThreads(threads, limit, false);
      expect(visible).toHaveLength(limit);
      visible.forEach((thread, index) => expect(thread).toBe(threads[index]));
      expect(threads).toHaveLength(16);
    }
  });

  it("returns the original array when expanded", () => {
    expect(windowPersonalThreads(threads, 5, true)).toBe(threads);
  });

  it("returns the original array when no threads are hidden", () => {
    for (const visible of [[], threads.slice(0, 3), threads.slice(0, 5)]) {
      expect(windowPersonalThreads(visible, 5, false)).toBe(visible);
    }
  });
});
