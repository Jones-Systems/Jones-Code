// @effect-diagnostics nodeBuiltinImport:off - reads the user guide to check its anchors.
import * as NodeFS from "node:fs";

import { T3_CONNECT_TROUBLESHOOTING_SECTIONS } from "@t3tools/contracts";
import { describe, expect, it } from "@effect/vitest";

import {
  classifyReachability,
  formatReachabilityFailure,
  type ReachabilityFacts,
} from "./ConnectReachability.ts";

const checkedAt = "2026-10-19T00:00:00.000Z";
const connected = { readyConnections: 4, hostname: "env.example.test", protocol: "quic" };
const notConnected = { readyConnections: 0, hostname: null, protocol: null };
const facts = (overrides: Partial<ReachabilityFacts>): ReachabilityFacts => ({
  managedTunnelConfigured: true,
  connector: connected,
  endpoint: null,
  edgeReachable: null,
  exitNode: null,
  ...overrides,
});

describe("classifyReachability", () => {
  it("keeps an anonymous 403 separate from local health and signed-in reachability", () => {
    const result = classifyReachability(
      facts({
        endpoint: { kind: "status", status: 403, rayId: "0123456789abcdef-IAD" },
        local: "matching_environment",
        edgeReachable: true,
      }),
      checkedAt,
    );
    expect(result).toMatchObject({ status: "unreachable", cause: "unexpected_response" });
    if (result.status !== "unreachable") throw new Error("Expected host probe failure");
    const text = formatReachabilityFailure(result);
    expect(text).toContain("could not be confirmed from this host");
    expect(text).toContain("HTTP 403");
    expect(text).toContain("server answers locally");
    expect(text).toContain("signed-in device is unverified");
    expect(text).not.toContain("NOT reachable");
    expect(text).not.toContain("Restart");
  });

  it("does not claim local health for an absent or mismatched descriptor", () => {
    for (const local of [undefined, "unverified", "different_environment"] as const) {
      const result = classifyReachability(
        facts({
          endpoint: { kind: "status", status: 403 },
          ...(local === undefined ? {} : { local }),
        }),
        checkedAt,
      );
      expect(result.status === "unreachable" && result.reason).not.toContain("server answers locally");
    }
  });

  it.each([
    [{ kind: "status", status: 502 }, "origin_unreachable"],
    [{ kind: "status", status: 530 }, "no_connector"],
    [{ kind: "network_error" }, "timeout"],
  ] as const)("maps endpoint result %o to %s", (endpoint, cause) => {
    expect(classifyReachability(facts({ endpoint, edgeReachable: true }), checkedAt)).toMatchObject(
      { status: "unreachable", cause },
    );
  });

  it("keeps the endpoint cause when the TCP edge test cannot explain it", () => {
    // QUIC (UDP) can connect while TCP 7844 is blocked.
    expect(
      classifyReachability(
        facts({ endpoint: { kind: "status", status: 502 }, edgeReachable: false }),
        checkedAt,
      ),
    ).toMatchObject({ cause: "origin_unreachable" });
    expect(
      classifyReachability(
        facts({
          connector: { readyConnections: null, hostname: "env.example.test", protocol: null },
          endpoint: { kind: "status", status: 502 },
          edgeReachable: false,
        }),
        checkedAt,
      ),
    ).toMatchObject({ cause: "origin_unreachable" });
    // Failed DNS also fails the edge lookups.
    expect(
      classifyReachability(
        facts({
          connector: { readyConnections: null, hostname: "env.example.test", protocol: null },
          endpoint: { kind: "dns_failed" },
          edgeReachable: false,
        }),
        checkedAt,
      ),
    ).toMatchObject({ cause: "dns_failed" });
  });

  it("waits on a connector that has not connected while the edge answers", () => {
    expect(
      classifyReachability(facts({ connector: notConnected, edgeReachable: true }), checkedAt),
    ).toMatchObject({ cause: "connector_not_ready" });
  });
});

describe("T3 Connect troubleshooting links", () => {
  it("each section names a heading in the user guide", () => {
    const guide = NodeFS.readFileSync(
      new URL("../../../../docs/user/t3-connect-troubleshooting.md", import.meta.url),
      "utf8",
    );
    // GitHub's heading anchors: lowercase, drop punctuation, spaces become hyphens.
    const anchors = new Set(
      [...guide.matchAll(/^#{2,3} (.+)$/gm)].map(([, heading]) =>
        heading!
          .toLowerCase()
          .replace(/[^\p{L}\p{N} _-]/gu, "")
          .replace(/ /g, "-"),
      ),
    );
    for (const section of T3_CONNECT_TROUBLESHOOTING_SECTIONS) {
      expect(anchors, section).toContain(section);
    }
  });
});
