import { assert, it } from "@effect/vitest";
import { runCapturedProcess } from "./capturedProcess.ts";

it("stops only its captured child, escalating after grace, and settles once it is reaped", async () => {
  const flood = await runCapturedProcess({
    command: "/bin/sh",
    args: ["-c", "while :; do echo flood; done"],
    deadlineMs: 10_000,
    outputLimit: 1024,
  });
  assert.equal(flood.outcome, "output-limit");
  // A child that ignores SIGTERM is killed after the grace period and reaped.
  const stubborn = await runCapturedProcess({
    command: "/bin/sh",
    args: ["-c", "trap '' TERM; while :; do :; done"],
    deadlineMs: 100,
    outputLimit: 1024,
  });
  assert.deepEqual([stubborn.outcome, stubborn.signal], ["deadline", "SIGKILL"]);
  const missing = await runCapturedProcess({
    command: "/nonexistent/jones-probe",
    args: [],
    deadlineMs: 1_000,
    outputLimit: 1024,
  });
  assert.equal(missing.outcome, "spawn-failed");
});
