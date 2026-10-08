import * as Schema from "effect/Schema";
import * as DateTime from "effect/DateTime";
import { describe, expect, it } from "vite-plus/test";

import {
  DesktopTelemetryControlMessage,
  ResourceTelemetryProcess,
  ResourceTelemetryProcessOwner,
} from "./resourceTelemetry.ts";

const decodeOwner = Schema.decodeUnknownSync(ResourceTelemetryProcessOwner);
const processCodec = Schema.toCodecJson(ResourceTelemetryProcess);
const decodeProcess = Schema.decodeUnknownSync(processCodec);
const encodeProcess = Schema.encodeSync(processCodec);

describe("ResourceTelemetryProcessOwner", () => {
  it("accepts a provider and an opaque thread identifier", () => {
    expect(decodeOwner({ kind: "provider", provider: "codex", threadId: "thread-1" })).toEqual({
      kind: "provider",
      provider: "codex",
      threadId: "thread-1",
    });
  });

  it("rejects empty ownership fields and unknown owner kinds", () => {
    expect(() => decodeOwner({ kind: "provider", provider: " ", threadId: "thread-1" })).toThrow();
    expect(() => decodeOwner({ kind: "provider", provider: "codex", threadId: " " })).toThrow();
    expect(() =>
      decodeOwner({ kind: "terminal", provider: "codex", threadId: "thread-1" }),
    ).toThrow();
  });

  it("keeps older process payloads valid and round-trips optional ownership", () => {
    const process: ResourceTelemetryProcess = {
      identity: { pid: 42, startTimeMs: 0 },
      ppid: 1,
      childPids: [],
      depth: 1,
      name: "codex",
      command: "codex app-server",
      status: "Running",
      category: "server-child",
      cpuPercent: 0,
      cpuTimeMs: 0,
      residentBytes: 0,
      peakResidentBytes: 0,
      virtualBytes: 0,
      ioReadBytes: 0,
      ioWriteBytes: 0,
      ioReadBytesPerSecond: 0,
      ioWriteBytesPerSecond: 0,
      ioSemantics: "storage",
      runTimeMs: 0,
      firstSeenAt: DateTime.makeUnsafe(0),
      lastSeenAt: DateTime.makeUnsafe(0),
    };
    const olderPayload = encodeProcess(process);
    expect(olderPayload).not.toHaveProperty("owner");
    const olderProcess = decodeProcess(olderPayload);
    expect(olderProcess.identity).toEqual(process.identity);
    expect(olderProcess.owner).toBeUndefined();

    const owner = { kind: "provider", provider: "codex", threadId: "thread-1" } as const;
    const ownedProcess = decodeProcess(
      encodeProcess({ ...process, category: "provider-root", owner }),
    );
    expect(ownedProcess.owner).toEqual(owner);
  });
});

describe("DesktopTelemetryControlMessage Jones selection", () => {
  const codec = Schema.toCodecJson(DesktopTelemetryControlMessage);
  const decode = Schema.decodeUnknownSync(codec);
  const encode = Schema.encodeSync(codec);
  const legacyRequest = {
    version: 1,
    type: "requestDesktopUpdate",
    requestId: "request-1",
  } as const;

  it("retains legacy requests and separate qualified Check and Download controls", () => {
    expect(decode(encode(legacyRequest))).toEqual(legacyRequest);
    const check = { ...legacyRequest, action: "check" } as const;
    expect(decode(encode(check))).toEqual(check);
    const download = {
      ...legacyRequest,
      action: "download",
      artifactId: 13,
      sourceSha: "b".repeat(40),
    } as const;
    expect(decode(encode(download))).toEqual(download);
  });

  it("rejects an unsupported control action or incorrectly typed selection", () => {
    for (const invalid of [
      { action: "install" },
      { action: "download", artifactId: "13", sourceSha: "b".repeat(40) },
      { action: "download", artifactId: 13, sourceSha: 42 },
    ]) {
      expect(() => decode(JSON.stringify({ ...legacyRequest, ...invalid }))).toThrow();
    }
  });
});
