// @effect-diagnostics nodeBuiltinImport:off
// Native detached-helper boundary needs exact process and durable file identity.
import * as NodeChildProcess from "node:child_process";
import * as NodeFSP from "node:fs/promises";
import * as NodeFS from "node:fs";
import * as NodePath from "node:path";
import * as Schema from "effect/Schema";

const TrialDescriptor = Schema.Struct({
  protocol: Schema.Literal(1),
  transactionId: Schema.String,
  home: Schema.String,
  databasePath: Schema.String,
  profile: Schema.String,
  environmentId: Schema.String,
  version: Schema.String,
  sourceSha: Schema.String,
  sourceTree: Schema.String,
  listener: Schema.String,
  trialReceiptPath: Schema.String,
  commitGrantPath: Schema.String,
});
const BuildIdentity = Schema.Struct({
  jonesSource: Schema.Struct({
    repository: Schema.Literal("Jones-Systems/Jones-Code"),
    sha: Schema.String,
    tree: Schema.String,
  }),
});

const decodeDescriptor = Schema.decodeUnknownSync(Schema.fromJsonString(TrialDescriptor));
const decodeBuildIdentity = Schema.decodeUnknownSync(BuildIdentity);

async function writeDurableReceipt(path: string, value: unknown): Promise<void> {
  const file = await NodeFSP.open(path, "wx", 0o600);
  try {
    await file.writeFile(JSON.stringify(value) + "\n");
    await file.sync();
  } finally {
    await file.close();
  }
  const parent = await NodeFSP.open(NodePath.dirname(path), "r");
  try {
    await parent.sync();
  } finally {
    await parent.close();
  }
}

/** With a trial descriptor, waits for a matching grant; callers must hold command/provider readiness until then. */
export async function awaitJonesTrialCommit(input: {
  descriptorPath?: string | undefined;
  home: string;
  databasePath: string;
  profile?: string | undefined;
  environmentId: string;
  version: string;
  buildMetadata: unknown;
  listener: string;
  signal?: AbortSignal;
}): Promise<void> {
  if (input.descriptorPath === undefined) return;
  const descriptorPath = await NodeFSP.realpath(input.descriptorPath);
  const descriptor = decodeDescriptor(await NodeFSP.readFile(descriptorPath, "utf8"));
  const build = decodeBuildIdentity(input.buildMetadata);
  if (
    descriptor.transactionId === "" ||
    descriptor.home !== (await NodeFSP.realpath(input.home)) ||
    descriptor.databasePath !== (await NodeFSP.realpath(input.databasePath)) ||
    input.profile === undefined ||
    descriptor.profile !== (await NodeFSP.realpath(input.profile)) ||
    descriptor.environmentId !== input.environmentId ||
    descriptor.version !== input.version ||
    descriptor.sourceSha !== build.jonesSource.sha ||
    descriptor.sourceTree !== build.jonesSource.tree ||
    descriptor.listener !== input.listener ||
    NodePath.dirname(descriptor.trialReceiptPath) !== NodePath.dirname(descriptorPath) ||
    NodePath.dirname(descriptor.commitGrantPath) !== NodePath.dirname(descriptorPath)
  )
    throw new Error("Jones trial identity does not match this native runtime.");

  const identity = await new Promise<string>((resolve, reject) =>
    NodeChildProcess.execFile(
      "/bin/ps",
      ["-p", String(process.pid), "-o", "lstart=", "-o", "command="],
      { maxBuffer: 16384 },
      (error, stdout) =>
        error === null
          ? resolve(stdout.trim())
          : reject(new Error("Could not prove the Jones backend process identity.")),
    ),
  );
  const receipt = {
    transactionId: descriptor.transactionId,
    version: input.version,
    sourceSha: descriptor.sourceSha,
    environmentId: input.environmentId,
    home: descriptor.home,
    databasePath: descriptor.databasePath,
    profile: descriptor.profile,
    listener: input.listener,
    resumeHeld: true,
    backendProcess: { pid: process.pid, identity },
  };
  await writeDurableReceipt(descriptor.trialReceiptPath, receipt);
  await new Promise<void>((resolve, reject) => {
    let reading = false;
    let finished = false;
    const finish = (error?: unknown) => {
      if (finished) return;
      finished = true;
      watcher.close();
      input.signal?.removeEventListener("abort", abort);
      if (error === undefined) resolve();
      else reject(error);
    };
    const abort = () => finish(new Error("Jones trial was cancelled before commit."));
    const inspectGrant = async () => {
      if (reading || finished) return;
      reading = true;
      try {
        let raw: string;
        try {
          raw = await NodeFSP.readFile(descriptor.commitGrantPath, "utf8");
        } catch (error) {
          if ((error as NodeJS.ErrnoException).code === "ENOENT") return;
          throw error;
        }
        const grant = JSON.parse(raw) as Record<string, unknown>;
        for (const key of [
          "protocol",
          "transactionId",
          "sourceSha",
          "environmentId",
          "home",
          "databasePath",
          "profile",
        ] as const) {
          if (grant[key] !== descriptor[key])
            throw new Error("Jones commit grant does not match the trial.");
        }
        if (grant.generation !== descriptor.transactionId)
          throw new Error("Jones commit generation mismatch.");
        // Reserve dispatch before opening the gate. Recovery must treat this marker as an uncertain effect.
        await writeDurableReceipt(
          NodePath.join(NodePath.dirname(descriptorPath), "resume-dispatched.json"),
          receipt,
        );
        finish();
      } catch (error) {
        finish(error);
      } finally {
        reading = false;
      }
    };
    const watcher = NodeFS.watch(NodePath.dirname(descriptorPath), () => {
      void inspectGrant();
    });
    watcher.on("error", finish);
    input.signal?.addEventListener("abort", abort, { once: true });
    if (input.signal?.aborted) abort();
    else void inspectGrant();
  });
}
