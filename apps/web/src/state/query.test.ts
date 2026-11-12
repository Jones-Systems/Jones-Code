import { PullRequestOperationError } from "@t3tools/contracts";
import * as Option from "effect/Option";
import { AsyncResult, Atom } from "effect/reactivity";
import { describe, expect, it, vi } from "vite-plus/test";

const { useAtomValue, refresh } = vi.hoisted(() => ({
  useAtomValue: vi.fn(),
  refresh: vi.fn(),
}));
vi.mock("@effect/atom-react", () => ({
  useAtomValue,
  useAtomRefresh: () => refresh,
}));

import { useEnvironmentQuery } from "./query";

const atom = Atom.make(AsyncResult.initial<string, PullRequestOperationError>());

describe("environment query error metadata", () => {
  it("retains typed pause metadata and held data without changing the readable error", () => {
    const error = new PullRequestOperationError({
      operation: "detail",
      detail: "Provider reads are paused.",
      reason: "rate-limited",
      retryAt: 60_000,
    });
    const held = AsyncResult.success("held");
    useAtomValue.mockReturnValue(AsyncResult.fail(error, { previousSuccess: Option.some(held) }));
    const query = useEnvironmentQuery(atom);
    expect(query.data).toBe("held");
    expect(query.error).toBe(error.message);
    expect(query.failure).toBe(error);
    expect(query.dataUpdatedAt).toBe(held.timestamp);
  });

  it("clears the failure metadata after recovery", () => {
    useAtomValue.mockReturnValue(AsyncResult.success("fresh"));
    const query = useEnvironmentQuery(atom);
    expect(query.data).toBe("fresh");
    expect(query.error).toBeNull();
    expect(query.failure).toBeNull();
  });
});
