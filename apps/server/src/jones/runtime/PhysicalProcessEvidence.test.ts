import * as DateTime from "effect/DateTime";
import * as PlatformError from "effect/PlatformError";
import { describe, expect, it } from "@effect/vitest";
import type { PhysicalProcessIdentity } from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as ByteSize from "effect/ByteSize";
import * as Option from "effect/Option";
import * as Fiber from "effect/Fiber";
import * as TestClock from "effect/testing/TestClock";
import { makePhysicalProcessLease, readLocalProcess } from "./PhysicalProcessEvidence.ts";

const identity: PhysicalProcessIdentity = {
  platform: "linux",
  bootId: "11111111-1111-1111-1111-111111111111",
  pidNamespace: "pid:[123]",
  pid: 42,
  startTime: "12345",
  startTimeUnit: "linux-clock-ticks-since-boot",
  executableIdentity: "1:2:300:400",
  build: "unknown",
};
const reply = (pid = 42) => ({
  process: { id: pid, residentMemoryBytes: 100, physicalFootprintBytes: null },
  gauges: [],
});
const make = (overrides: Partial<Parameters<typeof makePhysicalProcessLease>[0]> = {}) =>
  makePhysicalProcessLease({
    runtimeGeneration: "generation-1",
    launcher: identity,
    enabled: true,
    localResponderProven: true,
    isCurrent: () => true,
    request: () => Effect.succeed(reply()),
    readProcess: () => Effect.succeed({ status: "observed", identity }),
    ...overrides,
  });

describe("physical responder evidence", () => {
  it.effect(
    "correlates two replies with the captured local incarnation without copying provider extras",
    () =>
      Effect.gen(function* () {
        let requests = 0;
        const lease = make({
          request: () =>
            Effect.sync(() => {
              requests++;
              return { ...reply(), secret: "never-persist", path: "/private/source" };
            }),
        });
        const result = yield* lease.capture;
        expect(result).toEqual({
          status: "observed",
          runtimeGeneration: "generation-1",
          launcherPid: 42,
          responder: identity,
          source: "codex.server/diagnostics+local-os",
        });
        expect(requests).toBe(2);
        expect(JSON.stringify(result)).not.toContain("private");
      }),
  );
  it.effect.each([
    ["unsupported", Effect.fail({ code: -32601, errorMessage: "private" }), "unsupported"],
    ["transport", Effect.fail({ code: 123, errorMessage: "private" }), "transport"],
    ["malformed", Effect.succeed({ process: { id: "42" } }), "malformed"],
    ["fractional PID", Effect.succeed(reply(1.5)), "malformed"],
    ["zero PID", Effect.succeed(reply(0)), "malformed"],
    ["wrapper", Effect.succeed(reply(43)), "responder_mismatch"],
  ] as const)("%s", ([_name, effect, reason]) =>
    Effect.gen(function* () {
      expect(yield* make({ request: () => effect }).capture).toEqual({
        status: "unknown",
        runtimeGeneration: "generation-1",
        reason,
      });
    }),
  );
  it.effect("does not probe disabled capability", () =>
    Effect.gen(function* () {
      let requests = 0;
      const result = yield* make({
        enabled: false,
        request: () =>
          Effect.sync(() => {
            requests++;
            return reply();
          }),
      }).capture;
      expect(result).toMatchObject({ reason: "capability_disabled" });
      expect(requests).toBe(0);
    }),
  );
  it.effect(
    "does not equate a forwarding executable's coincident PID with a proven local responder",
    () =>
      Effect.gen(function* () {
        expect(yield* make({ localResponderProven: false }).capture).toMatchObject({
          reason: "provenance_unavailable",
        });
      }),
  );
  it.effect("keeps unproven launch and OS evidence unknown", () =>
    Effect.gen(function* () {
      expect(yield* make({ launcher: null }).capture).toMatchObject({
        reason: "launcher_unavailable",
      });
      expect(
        yield* make({ readProcess: () => Effect.succeed({ status: "unknown" }) }).capture,
      ).toMatchObject({
        reason: "os_unavailable",
      });
    }),
  );
  it.effect.each([
    { pid: 43 },
    { bootId: "22222222-2222-2222-2222-222222222222" },
    { startTime: "99999" },
    { pidNamespace: "pid:[456]" },
    { executableIdentity: "1:2:301:401" },
  ])("invalidates changed OS identity %j", (changed) =>
    Effect.gen(function* () {
      const lease = make({
        readProcess: () =>
          Effect.succeed({ status: "observed", identity: { ...identity, ...changed } }),
      });
      expect(yield* lease.capture).toMatchObject({ reason: "incarnation_changed" });
      expect(yield* lease.capture).toMatchObject({ reason: "retired" });
    }),
  );
  it.effect("rejects replacement during diagnostics", () =>
    Effect.gen(function* () {
      let current = true;
      const lease = make({
        isCurrent: () => current,
        request: () =>
          Effect.sync(() => {
            current = false;
            return reply();
          }),
      });
      expect(yield* lease.capture).toMatchObject({ reason: "retired" });
    }),
  );
  it.effect("rejects PID reuse between fresh replies", () =>
    Effect.gen(function* () {
      let reads = 0;
      const lease = make({
        readProcess: () =>
          Effect.sync(() => ({
            status: "observed" as const,
            identity: ++reads === 1 ? identity : { ...identity, startTime: "later" },
          })),
      });
      expect(yield* lease.capture).toMatchObject({ reason: "incarnation_changed" });
    }),
  );
  it.effect("bounds stalled transport", () =>
    Effect.gen(function* () {
      const fiber = yield* Effect.forkChild(make({ request: () => Effect.never }).capture);
      yield* TestClock.adjust("2 seconds");
      expect(yield* Fiber.join(fiber)).toMatchObject({ reason: "timeout" });
    }),
  );
});

describe("synthetic local OS evidence", () => {
  const filesystem = (namespace = "pid:[123]", statPid = "42") =>
    FileSystem.makeNoop({
      readFileString: (path) =>
        Effect.succeed(
          path.endsWith("boot_id")
            ? identity.bootId
            : `${statPid} (name with ) parentheses) S ${Array(18).fill("0").join(" ")} 12345 0`,
        ),
      readLink: (path) => Effect.succeed(path.includes("self") ? "pid:[123]" : namespace),
      stat: () =>
        Effect.succeed({
          type: "File",
          dev: 1n,
          ino: Option.some(2n),
          size: ByteSize.bytes(300),
          mtime: Option.some(DateTime.toDateUtc(DateTime.makeUnsafe(400))),
          atime: Option.none(),
          birthtime: Option.none(),
          mode: 0,
          nlink: Option.none(),
          uid: Option.none(),
          gid: Option.none(),
          rdev: Option.none(),
          blksize: Option.none(),
          blocks: Option.none(),
        }),
    });
  it.effect("extracts explicit clock ticks and sanitized executable metadata", () =>
    Effect.gen(function* () {
      expect(
        yield* readLocalProcess(42).pipe(
          Effect.provideService(FileSystem.FileSystem, filesystem()),
        ),
      ).toEqual({ status: "observed", identity });
    }),
  );
  it.effect("rejects foreign PID namespaces and malformed process identity", () =>
    Effect.gen(function* () {
      expect(
        yield* readLocalProcess(42).pipe(
          Effect.provideService(FileSystem.FileSystem, filesystem("pid:[456]")),
        ),
      ).toEqual({ status: "unknown" });
      expect(
        yield* readLocalProcess(42).pipe(
          Effect.provideService(FileSystem.FileSystem, filesystem("pid:[123]", "43")),
        ),
      ).toEqual({ status: "unknown" });
      expect(
        yield* readLocalProcess(42).pipe(
          Effect.provideService(FileSystem.FileSystem, FileSystem.makeNoop({})),
        ),
      ).toEqual({ status: "unknown" });
    }),
  );
  it.effect.each(["NotFound", "PermissionDenied"] as const)(
    "distinguishes missing process stat from unreadable identity: %s",
    (reason) =>
      Effect.gen(function* () {
        const fs = filesystem();
        const readProcess = (pid: number) =>
          readLocalProcess(pid).pipe(
            Effect.provideService(FileSystem.FileSystem, {
              ...fs,
              readFileString: (path, encoding) =>
                path === "/proc/42/stat"
                  ? Effect.fail(
                      PlatformError.systemError({
                        _tag: reason,
                        module: "FileSystem",
                        method: "readFileString",
                      }),
                    )
                  : fs.readFileString(path, encoding),
            }),
          );
        expect(yield* readProcess(42)).toEqual({
          status: reason === "NotFound" ? "absent" : "unknown",
        });
        expect(yield* make({ readProcess }).capture).toMatchObject({
          status: "unknown",
          reason: reason === "NotFound" ? "process_absent" : "os_unavailable",
        });
      }),
  );
});
