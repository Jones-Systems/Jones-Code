import * as Schema from "effect/Schema";
import { describe, expect, it } from "vite-plus/test";

import { ServerConfig } from "../server.ts";

const encodedConfig = {
  environment: {
    environmentId: "environment-1",
    label: "Test environment",
    platform: { os: "linux", arch: "x64" },
    serverVersion: "1.0.0",
    capabilities: {},
  },
  auth: {
    policy: "loopback-browser",
    bootstrapMethods: [],
    sessionMethods: [],
    sessionCookieName: "t3_session",
  },
  cwd: "/workspace",
  keybindingsConfigPath: "/workspace/keybindings.json",
  keybindings: [],
  issues: [],
  providers: [],
  availableEditors: [],
  observability: {
    logsDirectoryPath: "/workspace/logs",
    localTracingEnabled: true,
    otlpTracesEnabled: false,
    otlpMetricsEnabled: false,
    otlpLogsEnabled: false,
  },
  settings: {},
};

const descriptor = {
  environmentId: "environment-1",
  authorityNamespace: "authority-namespace-1",
  storeGeneration: 3,
};

const decodeServerConfig = Schema.decodeUnknownSync(ServerConfig);
// The schema as it was before this field existed, standing in for an older client build.
const { nativeStoreAuthority: _, ...previousFields } = ServerConfig.fields;
const decodePreviousServerConfig = Schema.decodeUnknownSync(Schema.Struct(previousFields));

describe("ServerConfig native store authority", () => {
  it("decodes a config without the descriptor as unknown authority", () => {
    const config = decodeServerConfig(encodedConfig);

    expect("nativeStoreAuthority" in config).toBe(false);
  });

  it("decodes the advertised descriptor", () => {
    const config = decodeServerConfig({ ...encodedConfig, nativeStoreAuthority: descriptor });

    expect(config.nativeStoreAuthority).toEqual(descriptor);
  });

  it("lets a client built before the field decode a config that carries it", () => {
    const config = decodePreviousServerConfig({
      ...encodedConfig,
      nativeStoreAuthority: descriptor,
    });

    expect(config.cwd).toBe("/workspace");
    expect("nativeStoreAuthority" in config).toBe(false);
  });
});
