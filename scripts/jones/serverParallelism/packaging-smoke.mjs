// Positive handshake enforces heap selection, FD3 guardian and same-build native
// load in ProcessSlot itself. No substitute server entry or native implementation.
import { mkdtemp, realpath, mkdir, writeFile, readFile, rm, readdir } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join, isAbsolute } from "node:path";
import { ComputeSupervisor } from "../../../apps/server/src/jones/serverCompute/supervisor.ts";
import {
  ProcessSlot,
  withComputeChildOperations,
} from "../../../apps/server/src/jones/serverCompute/processSlot.ts";
import { GenerationJournal } from "../../../apps/server/src/jones/serverCompute/journal.ts";
import { minimalWorkerEnvironment } from "../../../apps/server/src/jones/serverCompute/workerCommand.ts";
import { qualificationDecision } from "./qualificationResult.mjs";
const [manifestPath, output, profile] = process.argv.slice(2);
if (!isAbsolute(output ?? "") || !["laptop", "vps"].includes(profile))
  throw new Error("packaging-smoke.mjs <manifest> <absolute output> <laptop|vps>");
const manifest = JSON.parse(await readFile(manifestPath, "utf8")),
  count = profile === "vps" ? 3 : 2;
const command = {
  ...manifest,
  env: minimalWorkerEnvironment(
    process.env,
    manifest.runtimeForm === "sea",
    manifest.runtimeForm === "electron",
  ),
};
const scratch = await realpath(await mkdtemp(join(tmpdir(), "jones-sp-package-")));
const journalPath = join(scratch, "journal"),
  journal = await GenerationJournal.acquire(journalPath, count);
let cancelNext = false,
  cancellationDispatched = false,
  retired = 0,
  started = 0,
  cleanupProved = false;
const cancellation = new AbortController();
const runtimeGenerations = [];
const supervisor = new ComputeSupervisor({
  count,
  build: command.build,
  journal,
  start: async (boot, register, onExit) => {
    const child = await ProcessSlot.start(command, boot, register, onExit);
    runtimeGenerations.push({
      pid: child.pid,
      bootstrap: child.bootstrap,
      runtimeMetadata: child.runtimeMetadata ?? null,
    });
    started++;
    return withComputeChildOperations(child, {
      request: (request, signal, timing) => {
        const pending = child.request(request, signal, timing);
        if (cancelNext && request.operation === "searchContents") {
          cancelNext = false;
          cancellationDispatched = true;
          cancellation.abort();
        }
        return pending;
      },
      retire: async () => {
        await child.retire();
        retired++;
      },
    });
  },
});
const stop = () => cancellation.abort();
process.once("SIGINT", stop);
process.once("SIGTERM", stop);
let report;
try {
  const handles = [];
  for (let index = 0; index < count; index++) {
    const cwd = join(scratch, `workspace-${index}`);
    await mkdir(cwd);
    for (let file = 0; file < 128; file++)
      await writeFile(join(cwd, `file-${file}.txt`), "needle Ελληνικά\n".repeat(256));
    handles.push(supervisor.register({ cwd, variant: "content" }));
  }
  await Promise.all(handles.map((handle) => supervisor.ensure(handle)));
  const useful = await Promise.all(
    handles.map(async (handle) => {
      const listing = await supervisor.run(handle, "list", undefined);
      const search = await supervisor.run(handle, "search", { query: "file", limit: 32 });
      const content = await supervisor.run(handle, "searchContents", {
        query: "Ελληνικά",
        limit: 32,
        caseSensitive: true,
        wholeWord: true,
        useRegex: false,
      });
      return (
        listing.entries.length === 128 && search.entries.length > 0 && content.matches.length > 0
      );
    }),
  );
  cancelNext = true;
  let cancelled = false;
  try {
    await supervisor.run(
      handles[0],
      "searchContents",
      { query: "absent", limit: 32, caseSensitive: true, wholeWord: false, useRegex: false },
      cancellation.signal,
    );
  } catch (cause) {
    cancelled = cause?.reason === "cancelled";
  }
  await supervisor.shutdown();
  cleanupProved = true;
  const remaining = await readdir(journalPath);
  report = {
    predicates: {
      sameBuildNativeHandshake: started >= count,
      usefulOperations: useful.every(Boolean),
      cancellation: cancelled && cancellationDispatched,
      capturedExit: retired === started,
      journalRetirement: remaining.length === 0,
      runtimeMetadataRetained:
        runtimeGenerations.length >= count &&
        runtimeGenerations.every((generation) => generation.runtimeMetadata !== null),
    },
    started,
    retired,
    remaining,
    runtimeGenerations,
    heapFd3GuardianMeaning:
      "ProcessSlot.start accepts only after ready/GO/nativeReady/loaded and validated 256MiB heap range",
  };
} catch (cause) {
  report = { predicates: { smokeCompleted: false }, error: String(cause) };
} finally {
  try {
    if (!cleanupProved) {
      await supervisor.shutdown();
      cleanupProved = true;
    }
  } catch (cause) {
    report = { ...report, cleanupError: String(cause) };
  }
  process.removeListener("SIGINT", stop);
  process.removeListener("SIGTERM", stop);
  const predicates = { ...report.predicates, cleanupProved };
  const decision = qualificationDecision(predicates);
  await writeFile(
    output,
    JSON.stringify(
      {
        schema: "jones.server-parallelism.packaging/v1",
        ...report,
        predicates,
        ...decision,
        source: process.env.JONES_SP_SOURCE_SHA ?? null,
        runtimeForm: manifest.runtimeForm,
        build: manifest.build,
        profile,
        scratch,
        cleanupProved,
      },
      null,
      2,
    ) + "\n",
    { flag: "wx", mode: 0o600 },
  );
  if (cleanupProved) await rm(scratch, { recursive: true });
  if (!decision.passed) process.exitCode = 1;
}
