import * as Schema from "effect/Schema";
import { describe, expect, it } from "vite-plus/test";

import { DesktopEnvironmentBootstrapSchema, DesktopUpdateStateSchema } from "./ipc.ts";

describe("DesktopEnvironmentBootstrapSchema", () => {
  const decode = Schema.decodeUnknownSync(DesktopEnvironmentBootstrapSchema);

  it("preserves the concrete running distro separately from the backend id", () => {
    expect(
      decode({
        id: "wsl:default",
        label: "WSL (Ubuntu)",
        runningDistro: "Ubuntu",
        httpBaseUrl: "http://127.0.0.1:3774/",
        wsBaseUrl: "ws://127.0.0.1:3774/",
      }),
    ).toEqual({
      id: "wsl:default",
      label: "WSL (Ubuntu)",
      runningDistro: "Ubuntu",
      httpBaseUrl: "http://127.0.0.1:3774/",
      wsBaseUrl: "ws://127.0.0.1:3774/",
    });
  });

  it("allows non-running and non-WSL bootstraps to report no running distro", () => {
    expect(
      decode({
        id: "primary",
        label: "Windows",
        runningDistro: null,
        httpBaseUrl: null,
        wsBaseUrl: null,
      }).runningDistro,
    ).toBeNull();
  });
});

describe("DesktopUpdateStateSchema Jones reports", () => {
  const codec = Schema.toCodecJson(DesktopUpdateStateSchema);
  const decode = Schema.decodeUnknownSync(codec);
  const encode = Schema.encodeSync(codec);
  const releaseState = {
    enabled: false,
    status: "disabled",
    channel: "latest",
    currentVersion: "1.2.3",
    hostArch: "arm64",
    appArch: "arm64",
    runningUnderArm64Translation: false,
    availableVersion: null,
    downloadedVersion: null,
    releaseNotes: [],
    omittedReleaseCount: 0,
    downloadPercent: null,
    checkedAt: null,
    message: null,
    errorContext: null,
    canRetry: false,
  } as const;

  it("keeps legacy release reports valid without adding Jones metadata", () => {
    expect(decode(encode(releaseState))).toEqual(releaseState);
    expect(decode(encode(releaseState))).not.toHaveProperty("jones");
  });

  it("round-trips the source tree, artifact identity, and immutable staged handle", () => {
    const jones = {
      source: "jones-actions",
      channel: "jones-main",
      phase: "staged",
      capability: { check: true, download: false, install: false, reason: "native-consent-required" },
      stagedHandle: "e".repeat(64),
      provenance: {
        repository: "Jones-Systems/Jones-Code",
        sourceSha: "b".repeat(40),
        sourceTree: "c".repeat(40),
        workflow: "build-jones.yml",
        runId: 12,
        runAttempt: 2,
        artifactId: 13,
        artifactDigest: "sha256:" + "d".repeat(64),
        platform: "darwin",
        architecture: "arm64",
        version: "1.2.4-preview.20261007.1.2",
        payloadSha256: "f".repeat(64),
      },
    } as const;
    const staged = {
      ...releaseState,
      enabled: true,
      status: "downloaded" as const,
      downloadedVersion: jones.provenance.version,
      jones,
    };
    expect(decode(encode(staged))).toEqual(staged);
  });
});
