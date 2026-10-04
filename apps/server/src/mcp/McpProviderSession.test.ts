import { describe, expect, it } from "vite-plus/test";
import { EnvironmentId, ProviderInstanceId, ThreadId } from "@t3tools/contracts";
import {
  withAgentDeviceEnvironment,
  setMcpProviderSession,
  readMcpProviderSession,
  clearMcpProviderSession,
  type McpProviderSessionConfig,
} from "./McpProviderSession.ts";

describe("device CLI environment", () => {
  it("preserves provider credentials and commands while routing devices to the owned daemon", () => {
    const environment = withAgentDeviceEnvironment(
      { PATH: "/provider/bin:/usr/bin", PROVIDER_KEY: "fixture" },
      {
        agentDeviceEnvironment: {
          PATH: "/t3/device/bin",
          PATH_SEPARATOR: ":",
          AGENT_DEVICE_DAEMON_BASE_URL: "http://127.0.0.1:9000",
          AGENT_DEVICE_DAEMON_AUTH_TOKEN: "fixture-device",
        },
      },
    );
    expect(environment).toEqual({
      PATH: "/t3/device/bin:/provider/bin:/usr/bin",
      PROVIDER_KEY: "fixture",
      AGENT_DEVICE_DAEMON_BASE_URL: "http://127.0.0.1:9000",
      AGENT_DEVICE_DAEMON_AUTH_TOKEN: "fixture-device",
    });
  });

  it("does not grant CLI access when device access was not supplied", () => {
    const environment = { PATH: "/usr/bin", PROVIDER_KEY: "fixture" };
    expect(withAgentDeviceEnvironment(environment, undefined)).toBe(environment);
    expect(withAgentDeviceEnvironment(environment, {})).toBe(environment);
  });
});

describe("MCP provider grants", () => {
  it("rejects contradictory prompt availability without replacing a valid credential", () => {
    const threadId = ThreadId.make("provider-grant-fixture");
    const config: McpProviderSessionConfig = {
      environmentId: EnvironmentId.make("fixture-environment"),
      threadId,
      providerSessionId: "fixture-session",
      providerInstanceId: ProviderInstanceId.make("codex"),
      endpoint: "http://127.0.0.1/mcp",
      authorizationHeader: "Bearer fixture",
      capabilities: new Set(["orchestration", "worktree"]),
      browserToolsAvailable: false,
    };
    try {
      setMcpProviderSession(config);
      expect(() => setMcpProviderSession({ ...config, browserToolsAvailable: true })).toThrow(
        "MCP browser availability contradicts its preview grant",
      );
      expect(() =>
        setMcpProviderSession({ ...config, capabilities: new Set(["preview"]) }),
      ).toThrow("MCP browser availability contradicts its preview grant");
      expect(readMcpProviderSession(threadId)).toBe(config);
    } finally {
      clearMcpProviderSession(threadId);
    }
    expect(readMcpProviderSession(threadId)).toBeUndefined();
  });
});
