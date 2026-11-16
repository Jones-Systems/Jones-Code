import { describe, expect, it } from "vite-plus/test";
import { designRequestRouteToken, resolveDesignRequestRoute } from "./route.ts";
import { data, placement, placements, routeInput, thread } from "./testFixtures.ts";
const reason = (input: Parameters<typeof routeInput>[0]) => {
  const result = resolveDesignRequestRoute(routeInput(input));
  return result.state === "held" ? result.reason : result.state;
};
describe("personal Workstreams design routing", () => {
  it("routes a confirmed environment-local assignment without registry or placement proof", () => {
    const result = resolveDesignRequestRoute(routeInput());
    expect(result.state).toBe("routable");
    if (result.state !== "routable") return;
    expect(result.workstream).toEqual({ id: "ws-design", name: "Form 2 design" });
    expect(result.membershipId).toBe("personal:1");
    const { archivedAt: _archivedAt, ...destination } = thread;
    expect(result.thread).toEqual(destination);
    expect(result.routeToken).toBe(designRequestRouteToken("0".repeat(32), placement()));
    expect(designRequestRouteToken("0".repeat(32), placement({ revision: 2 }))).not.toBe(
      result.routeToken,
    );
  });
  it("holds missing, ambiguous, cached and remote assignments", () => {
    expect(reason({ data: { ...data, confirmed: false } })).toBe("registry-stale");
    expect(reason({ placements: placements([]) })).toBe("no-primary");
    expect(
      reason({ placements: placements([placement(), placement({ threadId: "thread-2" })]) }),
    ).toBe("multiple-primary");
    expect(reason({ placements: placements([placement({ environmentId: "remote" })]) })).toBe(
      "thread-not-local",
    );
    expect(reason({ threads: [{ ...thread, archivedAt: "2026-11-12T12:00:00.000Z" }] })).toBe(
      "thread-archived",
    );
    expect(reason({ registry: "unsupported" })).toBe("runtime-unsupported");
    expect(reason({ binding: { ...routeInput().binding!, workstreamId: "deleted" } })).toBe(
      "workstream-not-found",
    );
  });
});
