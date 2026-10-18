import { describe, expect, it } from "vite-plus/test";

import { designRequestRouteToken, resolveDesignRequestRoute } from "./route.ts";
import { data, NOW, placement, placements, routeInput, thread } from "./testFixtures.ts";

const reason = (input: Parameters<typeof routeInput>[0]) => {
  const route = resolveDesignRequestRoute(routeInput(input));
  return route.state === "held" ? route.reason : route.state;
};

describe("design request routing", () => {
  it("routes binding → workstream → single attested primary → local thread, copying its modes", () => {
    const route = resolveDesignRequestRoute(routeInput());
    expect(route).toEqual({
      state: "routable",
      routeToken: designRequestRouteToken("0".repeat(32), placement()),
      bindingId: "0".repeat(32),
      workstream: { id: "ws-design", name: "Form 2 design" },
      thread: {
        environmentId: "env-primary",
        id: "thread-1",
        title: "Form 2 build",
        runtimeMode: "approval-required",
        interactionMode: "default",
      },
      membershipId: "membership-1",
    });
    // Same inputs, same token; any routing identity change, a different token.
    expect(resolveDesignRequestRoute(routeInput())).toEqual(route);
    const moved = resolveDesignRequestRoute(
      routeInput({ placements: placements([placement({ membership_id: "membership-2" })]) }),
    );
    expect(moved.state === "routable" && moved.routeToken).not.toBe(
      route.state === "routable" && route.routeToken,
    );
    expect(route.state === "routable" && route.routeToken).toMatch(/^[a-f0-9]{32}$/);
  });

  it("ignores secondary memberships and other workstreams", () => {
    const items = [
      placement(),
      placement({ membership_id: "m-sec", kind: "secondary", native_thread_id: "thread-2" }),
      placement({
        membership_id: "m-other",
        workstream_id: "ws-other",
        native_thread_id: "thread-2",
      }),
    ];
    expect(reason({ placements: placements(items) })).toBe("routable");
  });

  it("holds every unresolvable case with a named reason", () => {
    expect(reason({ binding: null })).toBe("no-binding");
    expect(reason({ registry: "unsupported" })).toBe("runtime-unsupported");
    expect(reason({ registry: "unauthenticated" })).toBe("not-authenticated");
    expect(reason({ registry: "loading" })).toBe("registry-stale");
    expect(reason({ data: { ...data, source: "cache" } })).toBe("registry-stale");
    expect(reason({ data: { ...data, stale: true } })).toBe("registry-stale");
    expect(reason({ placements: null })).toBe("registry-stale");
    expect(
      reason({
        placements: { ...placements(), context: { ...placements().context, registry_version: 8 } },
      }),
    ).toBe("registry-stale");
    expect(reason({ binding: { ...routeInput().binding!, workstreamId: "ws-gone" } })).toBe(
      "workstream-not-found",
    );
    expect(reason({ placements: placements([]) })).toBe("no-primary");
    expect(
      reason({
        placements: placements([
          placement(),
          placement({ membership_id: "m-2", native_thread_id: "thread-2" }),
        ]),
      }),
    ).toBe("multiple-primary");
    expect(
      reason({ placements: placements([placement({ attested_at: "2026-10-09T12:30:00.000Z" })]) }),
    ).toBe("primary-not-attested");
    expect(
      reason({ placements: placements([placement({ expires_at: "2026-10-09T12:00:00.000Z" })]) }),
    ).toBe("placement-expired");
    expect(reason({ now: NOW + 2 * 60 * 60 * 1000 })).toBe("placement-expired");
    expect(reason({ placements: placements([placement({ store_generation: 4 })]) })).toBe(
      "primary-not-attested",
    );
    expect(
      reason({
        placements: {
          ...placements(),
          readiness: "trust-provider-required",
          trustedEnvironments: [],
        },
      }),
    ).toBe("primary-not-attested");
    expect(
      reason({ placements: placements([placement({ source_instance_id: "env-remote" })]) }),
    ).toBe("thread-not-local");
    expect(reason({ primaryEnvironmentId: null })).toBe("thread-not-local");
    expect(reason({ threads: [] })).toBe("thread-not-local");
    expect(reason({ threads: [{ ...thread, archivedAt: "2026-10-09T10:00:00.000Z" }] })).toBe(
      "thread-archived",
    );
    expect(reason({ threads: [{ ...thread, runtimeMode: undefined }] })).toBe(
      "thread-config-unknown",
    );
  });
});
