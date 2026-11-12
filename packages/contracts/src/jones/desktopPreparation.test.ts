import * as Schema from "effect/Schema";
import { describe, expect, it } from "vite-plus/test";

import { DesktopUpdateStateSchema } from "../ipc.ts";

describe("DesktopUpdateStateSchema background preparation", () => {
  const codec = Schema.toCodecJson(DesktopUpdateStateSchema);
  const decode = Schema.decodeUnknownSync(codec);
  const encode = Schema.encodeSync(codec);
  const releaseState = {
    enabled: true,
    status: "idle",
    channel: "latest",
    currentVersion: "0.0.44-preview.20261020.10.1",
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

  it("keeps states from desktops without background preparation valid", () => {
    expect(decode(encode(releaseState))).not.toHaveProperty("jonesPreparation");
  });

  it("round-trips the preparation phase and its target", () => {
    const state = {
      ...releaseState,
      jonesPreparation: {
        enabled: true,
        phase: "settling",
        targetVersion: "0.0.44-preview.20261020.11.1",
        replacingVersion: "0.0.44-preview.20261020.10.1",
        nextAt: "2026-10-20T12:10:00.000Z",
      },
    } as const;
    expect(decode(encode(state))).toEqual(state);
    expect(() =>
      decode({ ...state, jonesPreparation: { enabled: true, phase: "installing" } }),
    ).toThrow();
  });
});
