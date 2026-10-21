// @effect-diagnostics nodeBuiltinImport:off globalTimers:off
// Qualified staging of one downloaded artifact. It runs only inside the preparation worker,
// so extraction, tree hashes and candidate validation never use the serving event loop.
import * as NodeChildProcess from "node:child_process";
import * as NodeFSP from "node:fs/promises";
import * as NodePath from "node:path";
import {
  validateJonesStagedArtifact,
  type JonesStagedArtifact,
} from "@t3tools/shared/jones/jonesActions";
import {
  currentQualifiedRuntimeBinding,
  qualifiedRuntimeArtifactFromJonesStage,
  QualifiedRuntimeBlockedError,
  stageQualifiedRuntime,
  type StagedQualifiedRuntime,
} from "../../cloud/qualifiedRuntime.ts";
import { extractQualifiedLinuxArchive } from "../../cloud/qualifiedArchive.ts";
import { extractQualifiedDarwinRuntime } from "../../cloud/qualifiedDarwinRuntime.ts";
import { withOwnedScratch, type OwnedScratchLeftover } from "../../cloud/ownedScratch.ts";
import {
  decodeServicePreflightResult,
  qualifiedServicePreflightFailure,
} from "../../../cloud/servicePreflight.ts";
import { SERVICE_LAUNCHER_PROTOCOL } from "../../../cloud/serviceProtocol.ts";
import type { MigrationPlan } from "../migrationPlan.ts";
import { PREPARATION_COMMAND, type PreparationHost } from "./protocol.ts";

const PREFLIGHT_TIMEOUT_MS = 30_000;
const DARWIN_TOOL_TIMEOUT_MS = 5 * 60_000;
const OUTPUT_LIMIT = 8 * 1024 * 1024;

export interface BoundedProcessResult {
  readonly code: number | null;
  readonly stdout: string;
  readonly timedOut: boolean;
  readonly truncated: boolean;
  readonly invalidUtf8: boolean;
}

/**
 * Runs one command with the worker's environment, discarding stderr. A timeout, output limit or
 * abort kills the child; an abort rejects with the signal's reason once the child has closed.
 */
export function runBoundedProcess(input: {
  readonly command: string;
  readonly args: ReadonlyArray<string>;
  readonly timeoutMs: number;
  readonly outputLimit: number;
  readonly signal?: AbortSignal;
}): Promise<BoundedProcessResult> {
  return new Promise((resolve, reject) => {
    input.signal?.throwIfAborted();
    const child = NodeChildProcess.spawn(input.command, [...input.args], {
      stdio: ["ignore", "pipe", "ignore"],
    });
    const chunks: Buffer[] = [];
    let bytes = 0;
    let truncated = false;
    let timedOut = false;
    let settled = false;
    const kill = () => child.kill("SIGKILL");
    const timer = setTimeout(() => {
      timedOut = true;
      kill();
    }, input.timeoutMs);
    input.signal?.addEventListener("abort", kill, { once: true });
    const finish = (settle: () => void) => {
      if (settled) return;
      settled = true;
      clearTimeout(timer);
      input.signal?.removeEventListener("abort", kill);
      settle();
    };
    child.stdout.on("data", (chunk: Buffer) => {
      if (truncated) return;
      if (bytes + chunk.length > input.outputLimit) {
        truncated = true;
        kill();
        return;
      }
      bytes += chunk.length;
      chunks.push(chunk);
    });
    child.once("error", (error) => {
      if (child.pid === undefined) finish(() => reject(error));
    });
    child.once("close", (code) =>
      finish(() => {
        if (input.signal?.aborted) return reject(input.signal.reason);
        let stdout = "";
        let invalidUtf8 = false;
        try {
          stdout = new TextDecoder("utf-8", { fatal: true }).decode(Buffer.concat(chunks));
        } catch {
          invalidUtf8 = true;
        }
        resolve({ code, stdout, timedOut, truncated, invalidUtf8 });
      }),
    );
  });
}

/** The same bound service preflight the in-process path ran; the candidate reads its own manifest. */
export async function preflightQualifiedCandidate(input: {
  readonly entryPath: string;
  readonly databasePath: string;
  readonly version: string;
  readonly signal?: AbortSignal;
}): Promise<MigrationPlan | undefined> {
  let result: BoundedProcessResult;
  try {
    result = await runBoundedProcess({
      command: input.entryPath,
      args: [
        "__service-preflight",
        "--database-path",
        input.databasePath,
        "--launcher-protocol",
        String(SERVICE_LAUNCHER_PROTOCOL),
      ],
      timeoutMs: PREFLIGHT_TIMEOUT_MS,
      outputLimit: OUTPUT_LIMIT,
      ...(input.signal === undefined ? {} : { signal: input.signal }),
    });
  } catch (cause) {
    if (input.signal?.aborted) throw cause;
    throw new Error("startup-gate-unavailable: Could not preflight the qualified candidate.", {
      cause,
    });
  }
  if (result.timedOut || result.truncated || result.invalidUtf8)
    throw new Error("startup-gate-unavailable: The candidate preflight response was incomplete.");
  const reason = qualifiedServicePreflightFailure({
    code: result.code,
    stdout: result.stdout,
    version: input.version,
  });
  if (reason !== undefined) throw new Error(reason);
  let parsed: unknown;
  try {
    parsed = JSON.parse(result.stdout.trim());
  } catch (cause) {
    throw new Error(
      "startup-gate-unavailable: The candidate returned an invalid service preflight.",
      { cause },
    );
  }
  const decoded = decodeServicePreflightResult(parsed);
  return decoded?.status === "ready" ? decoded.migrationPlan : undefined;
}

/**
 * Once installed, the candidate prepares the next update with its own worker. A candidate whose
 * packaged worker cannot start would strand the host, so staging refuses it. Any protocol is
 * accepted: the candidate's server only ever talks to its own worker.
 */
export async function probeCandidatePreparationWorker(
  entryPath: string,
  signal?: AbortSignal,
): Promise<void> {
  let result: BoundedProcessResult | undefined;
  try {
    result = await runBoundedProcess({
      command: entryPath,
      args: [PREPARATION_COMMAND, "--protocol"],
      timeoutMs: PREFLIGHT_TIMEOUT_MS,
      outputLimit: 4096,
      ...(signal === undefined ? {} : { signal }),
    });
  } catch (cause) {
    if (signal?.aborted) throw cause;
  }
  let protocol: unknown;
  try {
    if (result?.code === 0 && !result.timedOut && !result.truncated && !result.invalidUtf8)
      protocol = (JSON.parse(result.stdout.trim()) as { readonly protocol?: unknown } | null)
        ?.protocol;
  } catch {
    protocol = undefined;
  }
  if (typeof protocol !== "number" || !Number.isSafeInteger(protocol) || protocol < 1)
    throw new QualifiedRuntimeBlockedError(
      "invalid-artifact",
      "The candidate cannot run its off-process update preparation worker.",
    );
}

export interface QualifiedArtifactStage {
  readonly value: StagedQualifiedRuntime & { readonly migrationPlan?: MigrationPlan };
  readonly leftover?: OwnedScratchLeftover;
}

/**
 * Revalidates the downloaded artifact, binds the running home, extracts into owned scratch under
 * `scratchParent`, and stages the runtime with its durable receipt. Every integrity check the
 * in-process stage ran is kept; Linux candidates also prove their own preparation worker starts.
 */
export async function stageQualifiedArtifact(input: {
  readonly artifact: JonesStagedArtifact;
  readonly baseDir: string;
  readonly dbPath: string;
  readonly activeVersion: string;
  readonly host: PreparationHost;
  readonly scratchParent: string;
  readonly signal?: AbortSignal;
}): Promise<QualifiedArtifactStage> {
  const { host, signal } = input;
  const options = signal === undefined ? undefined : { signal };
  const staged = await validateJonesStagedArtifact(
    NodePath.dirname(input.artifact.payloadPath),
    input.artifact.candidate,
  );
  if (
    staged.candidate.platform !== host.platform ||
    staged.candidate.architecture !== host.architecture
  )
    throw new Error("This service requires an Actions artifact matching its native platform.");
  signal?.throwIfAborted();
  const binding = await currentQualifiedRuntimeBinding(
    input.baseDir,
    input.activeVersion,
    host,
    options,
  );
  if (binding.dbPath !== (await NodeFSP.realpath(input.dbPath)))
    throw new Error("This qualified staging binding does not match the running database.");
  await NodeFSP.mkdir(NodePath.join(binding.baseDir, "runtime"), { recursive: true, mode: 0o700 });
  // Scratch removal can fail after the stage is durable; the stage then stands and the leftover
  // is reported instead of failing a valid Download.
  return await withOwnedScratch(input.scratchParent, ".jones-extract-", async (scratch) => {
    if (staged.candidate.platform === "darwin") {
      await extractQualifiedDarwinRuntime({
        artifact: staged,
        destination: scratch,
        baseDir: binding.baseDir,
        run: async (command) => {
          const result = await runBoundedProcess({
            ...command,
            timeoutMs: DARWIN_TOOL_TIMEOUT_MS,
            outputLimit: OUTPUT_LIMIT,
            ...(signal === undefined ? {} : { signal }),
          });
          return { code: result.code ?? 1, stdout: result.stdout };
        },
      });
    } else await extractQualifiedLinuxArchive(staged.payloadPath, scratch);
    signal?.throwIfAborted();
    await validateJonesStagedArtifact(NodePath.dirname(staged.payloadPath), staged.candidate);
    signal?.throwIfAborted();
    let migrationPlan: MigrationPlan | undefined;
    const qualifiedStage = await stageQualifiedRuntime({
      artifact: qualifiedRuntimeArtifactFromJonesStage(staged, scratch),
      binding,
      host,
      validate: async (entryPath) => {
        // Darwin Download inspects app metadata and ASAR; it never launches an unsigned
        // candidate against the owner's native service home.
        if (staged.candidate.platform === "darwin") return;
        migrationPlan = await preflightQualifiedCandidate({
          entryPath,
          databasePath: binding.dbPath,
          version: staged.receipt.version,
          ...(signal === undefined ? {} : { signal }),
        });
        await probeCandidatePreparationWorker(entryPath, signal);
      },
    });
    return { ...qualifiedStage, ...(migrationPlan === undefined ? {} : { migrationPlan }) };
  });
}
