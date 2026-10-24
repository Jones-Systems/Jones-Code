// @effect-diagnostics nodeBuiltinImport:off globalTimers:off - Standalone benchmark owns its load timer through the enclosing finally.
import * as NodeFS from "node:fs";
import * as NodeFSP from "node:fs/promises";
import * as NodePath from "node:path";
import * as NodePerfHooks from "node:perf_hooks";
import * as NodeTimers from "node:timers";
import * as NodeTimersPromises from "node:timers/promises";
import * as NodeUtil from "node:util";
import {
  qualifiedPayloadDigest,
  type QualifiedDigestTiming,
  type QualifiedRuntimeArtifact,
} from "../../../apps/server/src/jones/cloud/qualifiedRuntime.ts";

const args = NodeUtil.parseArgs({
  allowPositionals: true,
  options: {
    platform: { type: "string", default: "linux" },
    workflow: { type: "string" },
    iterations: { type: "string", default: "3" },
    version: { type: "string" },
  },
});
const directory = args.positionals[0];
const iterations = Number(args.values.iterations);
const platform = args.values.platform;
const workflow = args.values.workflow;
if (
  directory === undefined ||
  args.positionals.length !== 1 ||
  (platform !== "linux" && platform !== "darwin") ||
  !Number.isInteger(iterations) ||
  iterations < 1 ||
  iterations > 10 ||
  (workflow !== undefined &&
    ![
      ".github/workflows/artifact-cli-linux.yml",
      ".github/workflows/artifact-cli-mac.yml",
      ".github/workflows/artifact-desktop-mac.yml",
    ].includes(workflow))
) {
  throw new Error(
    "Usage: digest-benchmark.ts <runtime-directory> [--platform linux|darwin] [--workflow path] [--iterations 1..10] [--version preview-version]",
  );
}

const controller = new AbortController();
const cancel = () => controller.abort();
process.once("SIGINT", cancel);
process.once("SIGTERM", cancel);
const samples: Array<{
  mode: "idle" | "loaded";
  digest: string;
  bytesPerSecond: number;
  timing: QualifiedDigestTiming;
}> = [];
const measure = async (mode: "idle" | "loaded") => {
  for (let iteration = 0; iteration < iterations; iteration++) {
    let timing: QualifiedDigestTiming | undefined;
    const digest = await qualifiedPayloadDigest(
      directory,
      platform,
      workflow as QualifiedRuntimeArtifact["workflow"] | undefined,
      {
        signal: controller.signal,
        ...(args.values.version === undefined ? {} : { runtimeVersion: args.values.version }),
        onDigestTiming: (value) => {
          timing = value;
        },
      },
    );
    if (timing === undefined) throw new Error("Digest timing was not reported.");
    samples.push({
      mode,
      digest,
      bytesPerSecond: timing.wallMs > 0 ? (timing.bytes * 1_000) / timing.wallMs : 0,
      timing,
    });
  }
};

try {
  await measure("idle");
  // Read one bounded buffer repeatedly; neither the measured tree nor scratch is written.
  const handle = await NodeFSP.open(
    NodePath.join(directory, "t3"),
    NodeFS.constants.O_RDONLY | NodeFS.constants.O_NOFOLLOW,
  );
  const buffer = Buffer.alloc(64 * 1024);
  const loadStop = new AbortController();
  let loadFailure: unknown;
  const load = (async () => {
    try {
      while (!loadStop.signal.aborted && !controller.signal.aborted) {
        await handle.read(buffer, 0, buffer.byteLength, 0);
        await NodeTimersPromises.setImmediate();
      }
    } catch (cause) {
      loadFailure = cause;
      controller.abort();
    }
  })();
  const timer = NodeTimers.setInterval(() => {
    const until = NodePerfHooks.performance.now() + 2;
    while (NodePerfHooks.performance.now() < until) {
      // Controlled two-millisecond load on the same loop as the digest.
    }
  }, 10);
  try {
    await measure("loaded");
  } finally {
    NodeTimers.clearInterval(timer);
    loadStop.abort();
    await load;
    await handle.close();
  }
  if (loadFailure !== undefined) throw loadFailure;
  if (samples.some((sample) => sample.digest !== samples[0]?.digest))
    throw new Error("Runtime payload changed between measurements.");
  process.stdout.write(`${JSON.stringify({ schema: 1, platform, iterations, samples })}\n`);
} finally {
  process.removeListener("SIGINT", cancel);
  process.removeListener("SIGTERM", cancel);
}
