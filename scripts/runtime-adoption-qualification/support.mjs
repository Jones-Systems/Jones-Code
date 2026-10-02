import * as NodeCrypto from "node:crypto";
import * as NodeFSP from "node:fs/promises";
import * as NodeFS from "node:fs";
import * as NodePath from "node:path";
import * as NodeURL from "node:url";
import * as Cause from "effect/Cause";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";

const worktree = NodeURL.fileURLToPath(new URL("../../", import.meta.url));

export async function sha256File(filename) {
  return NodeCrypto.createHash("sha256")
    .update(await NodeFSP.readFile(filename))
    .digest("hex");
}

async function allocateScratch(options) {
  const parent = NodePath.resolve(
    options.scratchRoot ??
      process.env.T4_QUAL_SCRATCH_ROOT ??
      NodePath.join(worktree, ".t3/runtime-adoption-qualification"),
  );
  await NodeFSP.mkdir(parent, { recursive: true });
  const canonicalParent = await NodeFSP.realpath(parent);
  if (canonicalParent === "/tmp" || canonicalParent.startsWith("/tmp/")) {
    throw new Error("Qualification scratch must not use /tmp");
  }
  const label = (options.label ?? "run").replace(/[^a-zA-Z0-9_-]/g, "-");
  const root = await NodeFSP.mkdtemp(NodePath.join(canonicalParent, `${label}-`));
  const records = [];
  const evidenceInput = options.evidenceDir ?? process.env.T4_QUAL_EVIDENCE_DIR;
  const evidenceDir = evidenceInput ? NodePath.resolve(evidenceInput) : undefined;
  if (evidenceDir && (evidenceDir === root || evidenceDir.startsWith(`${root}${NodePath.sep}`))) {
    await NodeFSP.rm(root, { recursive: true, force: true });
    throw new Error("Evidence directory must survive scratch cleanup");
  }
  return {
    context: { root, record: (value) => records.push(value) },
    finalize: async (status, error) => {
      if (error !== undefined)
        records.push({ error: error instanceof Error ? error.message : String(error) });
      try {
        if (evidenceDir) {
          NodeFS.mkdirSync(evidenceDir, { recursive: true });
          NodeFS.writeFileSync(
            NodePath.join(evidenceDir, `${NodePath.basename(root)}.json`),
            `${JSON.stringify({ schema: "jones-runtime-qualification-run/v1", root, status, records }, null, 2)}\n`,
          );
        }
      } finally {
        await NodeFSP.rm(root, { recursive: true, force: true });
      }
    },
  };
}

export async function withRunScratch(options, callback) {
  const scratch = await allocateScratch(options);
  let status = "failure";
  let failure;
  try {
    const result = await callback(scratch.context);
    status = "success";
    return result;
  } catch (error) {
    status = "failure";
    failure = error;
    throw error;
  } finally {
    await scratch.finalize(status, failure);
  }
}

export function withRunScratchEffect(options, useEffect) {
  return Effect.acquireUseRelease(
    Effect.tryPromise(() => allocateScratch(options)),
    (scratch) => Effect.scoped(Effect.suspend(() => useEffect(scratch.context))),
    (scratch, exit) =>
      Effect.tryPromise(() =>
        scratch.finalize(
          Exit.isSuccess(exit)
            ? "success"
            : Cause.hasInterruptsOnly(exit.cause)
              ? "cancelled"
              : "failure",
          Exit.isFailure(exit) ? Cause.pretty(exit.cause) : undefined,
        ),
      ),
  );
}

export function qualifyPackageSourceDiff(changedPaths, packageKnip, qualificationKnip) {
  const outsideHarness = changedPaths.filter(
    (name) => !name.startsWith("scripts/runtime-adoption-qualification/"),
  );
  if (outsideHarness.length === 0) return { productionDiffPaths: [], nonBuildMetadataDiff: [] };
  if (outsideHarness.length !== 1 || outsideHarness[0] !== "knip.jsonc") {
    throw new Error("Candidate package source differs from qualified production source");
  }
  if (!Buffer.isBuffer(packageKnip) || !Buffer.isBuffer(qualificationKnip)) {
    throw new Error("Exact Knip source bytes are required");
  }
  const anchor = Buffer.from('        "smoke-cli-archive.ts",\n');
  const insertion = Buffer.from('        "runtime-adoption-qualification/run.mjs",\n');
  const offset = packageKnip.indexOf(anchor);
  const scriptsEntry = Buffer.from('    "scripts": {\n');
  const section = packageKnip.indexOf(scriptsEntry);
  const entry = packageKnip.indexOf(Buffer.from('      "entry": [\n'), section);
  if (
    section < 0 ||
    entry < 0 ||
    offset < entry ||
    packageKnip.indexOf(anchor, offset + anchor.length) !== -1 ||
    packageKnip.indexOf(insertion) !== -1 ||
    packageKnip.subarray(entry, offset).includes(Buffer.from("      ],"))
  ) {
    throw new Error("Expected unique scripts entry anchor is absent from package Knip bytes");
  }
  const expected = Buffer.concat([
    packageKnip.subarray(0, offset + anchor.length),
    insertion,
    packageKnip.subarray(offset + anchor.length),
  ]);
  if (!qualificationKnip.equals(expected)) {
    throw new Error("Knip metadata differs beyond the exact runner entry insertion");
  }
  const hash = (bytes) => NodeCrypto.createHash("sha256").update(bytes).digest("hex");
  return {
    productionDiffPaths: [],
    nonBuildMetadataDiff: [
      {
        path: "knip.jsonc",
        entry: "runtime-adoption-qualification/run.mjs",
        packageSha256: hash(packageKnip),
        qualificationSha256: hash(qualificationKnip),
        proofKind: "exact-one-entry-byte-insertion",
      },
    ],
  };
}

export async function bindArtifactInputs(inputs, observedSource) {
  const descriptor =
    typeof inputs === "string" ? JSON.parse(await NodeFSP.readFile(inputs, "utf8")) : inputs;
  if (descriptor?.schema !== "jones-runtime-artifact-inputs/v1") {
    throw new Error("Unsupported runtime artifact inputs schema");
  }
  const reasons = [];
  if (descriptor.repository !== "Jones-Systems/Jones-Code")
    throw new Error("Artifact repository is not Jones-Systems/Jones-Code");
  if (observedSource?.commit && !/^[a-f0-9]{40}$/i.test(observedSource.commit))
    throw new Error("Malformed observed source commit");
  if (
    descriptor.acceptedCumulativeSource &&
    !/^[a-f0-9]{40}$/i.test(descriptor.acceptedCumulativeSource)
  ) {
    throw new Error("Malformed accepted cumulative source commit");
  }
  if (observedSource?.clean === false) throw new Error("Observed source is dirty");
  if (observedSource?.repository && descriptor.repository !== observedSource.repository) {
    throw new Error("Artifact repository mismatch");
  }
  if (!descriptor.acceptedCumulativeSource) reasons.push("acceptedCumulativeSource is missing");
  if (!observedSource?.commit || observedSource.clean !== true || !observedSource.repository) {
    reasons.push("observed source identity is missing or dirty");
  }
  if (descriptor.acceptedCumulativeSource && observedSource?.commit) {
    if (descriptor.acceptedCumulativeSource !== observedSource.commit) {
      throw new Error("acceptedCumulativeSource does not match the exact observed source");
    }
    if (descriptor.repository !== observedSource.repository)
      throw new Error("Artifact repository mismatch");
  }
  const candidate = descriptor.candidate;
  if (candidate?.sourceCommit && candidate.sourceCommit !== descriptor.acceptedCumulativeSource) {
    throw new Error("Candidate source commit differs from accepted package source");
  }
  if (
    candidate?.sourceTree &&
    descriptor.acceptedCumulativeTree &&
    candidate.sourceTree !== descriptor.acceptedCumulativeTree
  ) {
    throw new Error("Candidate source tree differs from accepted package tree");
  }
  for (const field of ["version", "path", "sha256"]) {
    if (!candidate?.[field]) reasons.push(`candidate.${field} is missing`);
  }
  if (
    candidate?.version &&
    !/^(0|[1-9]\d*)\.(0|[1-9]\d*)\.(0|[1-9]\d*)(?:-(?:0|[1-9]\d*|\d*[a-zA-Z-][a-zA-Z0-9-]*)(?:\.(?:0|[1-9]\d*|\d*[a-zA-Z-][a-zA-Z0-9-]*))*)?(?:\+[a-zA-Z0-9-]+(?:\.[a-zA-Z0-9-]+)*)?$/.test(
      candidate.version,
    )
  ) {
    throw new Error("Malformed candidate semver version");
  }
  for (const [field, accepted] of [
    ["platform", ["linux", "darwin", "win32"]],
    ["architecture", ["x64", "arm64"]],
    ["channel", ["stable", "preview"]],
  ]) {
    if (!candidate?.[field]) reasons.push(`candidate.${field} is missing`);
    else if (!accepted.includes(candidate[field]))
      throw new Error(`Unsupported candidate ${field}`);
  }
  for (const field of ["path", "runnerPath"]) {
    if (candidate?.[field] && !NodePath.isAbsolute(candidate[field]))
      throw new Error(`Candidate ${field} must be absolute`);
  }
  for (const field of ["sha256", "runnerSha256"]) {
    if (candidate?.[field] && !/^[a-f0-9]{64}$/i.test(candidate[field]))
      throw new Error(`Malformed candidate ${field}`);
  }
  const verifyFile = async (filename, expected, kind) => {
    let actual;
    try {
      actual = await sha256File(filename);
    } catch (error) {
      if (error.code === "ENOENT") {
        reasons.push(`${kind} file is missing`);
        return;
      }
      throw error;
    }
    if (actual !== expected.toLowerCase()) throw new Error(`${kind} hash mismatch`);
  };
  if (candidate?.path && candidate.sha256) {
    await verifyFile(candidate.path, candidate.sha256, "Candidate package");
  }
  if (candidate?.runnerPath || candidate?.runnerSha256) {
    if (!candidate.runnerPath || !candidate.runnerSha256)
      reasons.push("candidate runner path/hash pair is incomplete");
    else await verifyFile(candidate.runnerPath, candidate.runnerSha256, "Candidate runner");
  }
  return reasons.length
    ? { status: "unbound", reasons }
    : { status: "bound", source: observedSource, candidate };
}
