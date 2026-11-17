// @effect-diagnostics nodeBuiltinImport:off -- Read-only no-follow file observation for the preserve plan; this module never writes or removes.
import * as NodeFS from "node:fs";
import * as NodeFSP from "node:fs/promises";
import * as NodePath from "node:path";
import { sensitive } from "./Filesystem.ts";
import type { IgnoredResult } from "./IgnoredClassifier.ts";

/**
 * Content classes follow `spec.recoverability-first-worktree-reclaim`:
 * recreatable, regenerable, preservable (a preserve branch can carry it) and
 * unrecreatable. A reason can also be `liveness` (identity, locks, consumers)
 * or `unobserved`; both keep their hold semantics.
 */
export type ContentClass = "recreatable" | "regenerable" | "preservable" | "unrecreatable";
export type ReasonClass = "preservable" | "unrecreatable" | "liveness" | "unobserved";
export type RecoverabilityVerdict =
  /** Nothing to hold: tracked content is recreatable and ignored output is regenerable. */
  | "recreatable"
  /** Every hold is preservable and the preserve plan excludes nothing. */
  | "retirable_after_preserve"
  /** At least one reason is unrecreatable or about liveness or identity. */
  | "held"
  /** No hold, but an observation is missing. */
  | "unknown";

export interface PreserveExclusion {
  path: string;
  /** `sensitive_name`, `secret_pattern:<name>`, `oversized` or `nested_repository`. */
  reason: string;
  origin: "tracked" | "untracked";
}
export interface PreservePlan {
  schema: "workspace-preserve-plan/v1";
  /** Paths a preserve commit adds from the worktree, in code-point order. */
  selected: string[];
  /** Tracked paths absent from the worktree; the preserve commit removes them. */
  deletions: string[];
  /** Paths no preserve commit may carry. Each is unrecreatable and named exactly. */
  excluded: PreserveExclusion[];
  selectedBytes: number;
  excludedBytes: number;
}
export interface Recoverability {
  schema: "workspace-recoverability/v1";
  verdict: RecoverabilityVerdict;
  /** Class of each reason in `QualificationResult.reasons`. */
  reasons: Record<string, ReasonClass>;
  /** Ignored roots deleted with the checkout under the regenerable classes. */
  regenerable: string[];
}

export const PRESERVE_LIMITS = {
  fileBytes: 50 * 1024 ** 2,
  totalBytes: 500 * 1024 ** 2,
  scanBytes: 2 * 1024 ** 2,
} as const;

// Fixed set from the engineering spec. A match records only the pattern name.
export const SECRET_PATTERNS: readonly { name: string; pattern: RegExp }[] = [
  { name: "private_key", pattern: /-----BEGIN [A-Z ]*PRIVATE KEY-----/ },
  { name: "aws_access_key", pattern: /AKIA[0-9A-Z]{16}/ },
  { name: "github_token", pattern: /gh[pousr]_[A-Za-z0-9]{36,}/ },
  { name: "github_pat", pattern: /github_pat_[A-Za-z0-9_]{20,}/ },
  { name: "sk_key", pattern: /sk-[A-Za-z0-9_-]{20,}/ },
  { name: "slack_token", pattern: /xox[baprs]-[A-Za-z0-9-]{10,}/ },
  {
    name: "credential_assignment",
    pattern: /(api[_-]?key|secret|token|password)\s*[:=]\s*['"][^'"\s]{12,}/i,
  },
];

export const secretPatternMatch = (text: string) =>
  SECRET_PATTERNS.find((candidate) => candidate.pattern.test(text))?.name ?? null;

/** Reads a regular file of at most `limit` bytes without following a final symlink. */
async function readTextCandidate(path: string, limit: number) {
  const handle = await NodeFSP.open(
    path,
    NodeFS.constants.O_RDONLY | NodeFS.constants.O_NOFOLLOW | NodeFS.constants.O_NONBLOCK,
  );
  try {
    const before = await handle.stat();
    if (!before.isFile() || before.size > limit) return null;
    const bytes = Buffer.alloc(before.size);
    let offset = 0;
    while (offset < bytes.length) {
      const read = await handle.read(bytes, offset, bytes.length - offset, null);
      if (!read.bytesRead) break;
      offset += read.bytesRead;
    }
    return bytes.subarray(0, offset);
  } finally {
    await handle.close();
  }
}

// Git treats a NUL in the first 8000 bytes as binary; binary files are not scanned.
const looksBinary = (bytes: Buffer) => bytes.subarray(0, 8000).includes(0);

const codePointOrder = (a: string, b: string) => (a < b ? -1 : a > b ? 1 : 0);

/**
 * Selects what a preserve commit would carry and what it must exclude.
 * Sensitive-named paths are never opened. Content is scanned only for selected,
 * non-sensitive, text files of at most 2 MiB. `checkpoint` throws when the
 * caller's observation budget is exhausted.
 */
export async function planPreservation(input: {
  worktree: string;
  /** Tracked paths reported changed, including a rename's origin path. */
  tracked: readonly string[];
  /** Untracked non-ignored paths; a trailing `/` marks a nested repository. */
  untracked: readonly string[];
  checkpoint: () => void;
}): Promise<PreservePlan> {
  const excluded: PreserveExclusion[] = [];
  const deletions = new Set<string>();
  let excludedBytes = 0;
  const candidates: { path: string; size: number; origin: PreserveExclusion["origin"] }[] = [];
  const entries = [
    ...input.tracked.map((path) => ({ path, origin: "tracked" as const })),
    ...input.untracked.map((path) => ({ path, origin: "untracked" as const })),
  ];
  for (const { path, origin } of entries) {
    input.checkpoint();
    const bare = path.replace(/\/$/, "");
    const absolute = NodePath.join(input.worktree, bare);
    const info = await NodeFSP.lstat(absolute).catch((cause: NodeJS.ErrnoException) => {
      if (cause.code === "ENOENT" || cause.code === "ENOTDIR") return null;
      throw cause;
    });
    if (!info) {
      // Only a tracked path can be a deletion; an untracked path that vanished is gone.
      if (origin === "tracked") deletions.add(bare);
      continue;
    }
    // A directory has no preservable size of its own; its files are not enumerated.
    const size = info.isDirectory() ? 0 : info.size;
    if (path.endsWith("/") || info.isDirectory()) {
      excluded.push({ path, reason: "nested_repository", origin });
      continue;
    }
    if (bare.split("/").some(sensitive)) {
      excluded.push({ path: bare, reason: "sensitive_name", origin });
      excludedBytes += size;
      continue;
    }
    if (size > PRESERVE_LIMITS.fileBytes) {
      excluded.push({ path: bare, reason: "oversized", origin });
      excludedBytes += size;
      continue;
    }
    if (info.isFile() && info.size <= PRESERVE_LIMITS.scanBytes) {
      const bytes = await readTextCandidate(absolute, PRESERVE_LIMITS.scanBytes);
      const match =
        bytes && !looksBinary(bytes) ? secretPatternMatch(bytes.toString("latin1")) : null;
      if (match) {
        excluded.push({ path: bare, reason: `secret_pattern:${match}`, origin });
        excludedBytes += size;
        continue;
      }
    }
    candidates.push({ path: bare, size, origin });
  }
  // A set over the total bound sheds its largest files until it fits.
  let total = candidates.reduce((sum, item) => sum + item.size, 0);
  const shed = new Set<string>();
  for (const item of [...candidates].sort(
    (a, b) => b.size - a.size || codePointOrder(a.path, b.path),
  )) {
    if (total <= PRESERVE_LIMITS.totalBytes) break;
    shed.add(item.path);
    total -= item.size;
    excludedBytes += item.size;
    excluded.push({ path: item.path, reason: "oversized", origin: item.origin });
  }
  const selected = candidates.filter((item) => !shed.has(item.path));
  return {
    schema: "workspace-preserve-plan/v1",
    selected: selected.map((item) => item.path).sort(codePointOrder),
    deletions: [...deletions].sort(codePointOrder),
    excluded: excluded.sort(
      (a, b) => codePointOrder(a.path, b.path) || codePointOrder(a.reason, b.reason),
    ),
    selectedBytes: selected.reduce((sum, item) => sum + item.size, 0),
    excludedBytes,
  };
}

const LIVENESS = new Set([
  "identity_mismatch",
  "primary_checkout",
  "locked",
  "project_root",
  "active_consumer",
  "obligation",
  "setup_unfinalized",
]);
const PRESERVABLE = new Set(["dirty_tracked", "untracked", "repository_durability_unproven"]);

/**
 * Classifies each qualification reason. Reasons not named here stay
 * `unrecreatable`, so a new hold is conservative until it is classified.
 */
export function classifyRecoverability(input: {
  reasons: readonly string[];
  unknown: ReadonlySet<string>;
  plan?: PreservePlan | undefined;
  ignored?: readonly IgnoredResult[] | undefined;
}): Recoverability {
  const excludedFrom = (origin: PreserveExclusion["origin"]) =>
    input.plan?.excluded.some((item) => item.origin === origin) === true;
  const classes: Record<string, ReasonClass> = {};
  for (const reason of input.reasons) {
    classes[reason] = input.unknown.has(reason)
      ? "unobserved"
      : LIVENESS.has(reason)
        ? "liveness"
        : PRESERVABLE.has(reason) &&
            input.plan !== undefined &&
            !(reason === "dirty_tracked" && excludedFrom("tracked")) &&
            !(reason === "untracked" && excludedFrom("untracked"))
          ? "preservable"
          : "unrecreatable";
  }
  const values = Object.values(classes);
  const verdict: RecoverabilityVerdict = values.some(
    (value) => value === "unrecreatable" || value === "liveness",
  )
    ? "held"
    : values.includes("unobserved")
      ? "unknown"
      : values.includes("preservable")
        ? "retirable_after_preserve"
        : "recreatable";
  return {
    schema: "workspace-recoverability/v1",
    verdict,
    reasons: classes,
    regenerable: (input.ignored ?? [])
      .filter((item) => item.outcome === "disposable" && item.basis === "regenerable_output")
      .map((item) => item.path),
  };
}
