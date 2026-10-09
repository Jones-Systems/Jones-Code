import { describe, expect, it } from "vite-plus/test";
import { EnvironmentId, type AuthSessionState } from "@t3tools/contracts";
import {
  BearerConnectionTarget,
  RelayConnectionTarget,
  type PreparedConnection,
} from "@t3tools/client-runtime/connection";
import { companionTicketEligibility } from "./ticket.ts";
const environmentId = EnvironmentId.make("env");
const prepared: PreparedConnection = {
  environmentId,
  label: "Fixture",
  httpBaseUrl: "https://fixture.test",
  socketUrl: "wss://fixture.test/ws",
  httpAuthorization: { _tag: "Bearer", token: "fixture-only" },
  target: new BearerConnectionTarget({ environmentId, label: "Fixture", connectionId: "fixture" }),
};
const session: AuthSessionState = {
  authenticated: true,
  auth: {
    policy: "remote-reachable",
    bootstrapMethods: ["one-time-token"],
    sessionMethods: ["bearer-access-token"],
    sessionCookieName: "fixture",
  },
  scopes: ["preview:operate", "orchestration:read"],
  sessionMethod: "bearer-access-token",
};
const input = { registered: true, enabled: true, prepared, session };
describe("renderer transferable ticket eligibility", () => {
  it("uses a registered enabled connection and current grants", () => {
    expect(companionTicketEligibility(input)).toBeNull();
    expect(companionTicketEligibility({ ...input, registered: false })).toEqual({
      _tag: "unavailable",
    });
    expect(companionTicketEligibility({ ...input, enabled: false })).toEqual({
      _tag: "unavailable",
    });
    expect(
      companionTicketEligibility({
        ...input,
        session: { ...session, scopes: ["orchestration:read"] },
      }),
    ).toEqual({ _tag: "auth_required" });
    expect(
      companionTicketEligibility({ ...input, prepared: { ...prepared, httpAuthorization: null } }),
    ).toEqual({ _tag: "auth_required" });
  });
  it("rejects relay and base paths as unsupported before a ticket request", () => {
    expect(
      companionTicketEligibility({
        ...input,
        prepared: {
          ...prepared,
          target: new RelayConnectionTarget({ environmentId, label: "Relay" }),
        },
      }),
    ).toEqual({ _tag: "unsupported" });
    expect(
      companionTicketEligibility({
        ...input,
        prepared: { ...prepared, httpBaseUrl: "https://fixture.test/prefix" },
      }),
    ).toEqual({ _tag: "unsupported" });
  });
});
