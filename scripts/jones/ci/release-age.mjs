// Checks locked registry packages against the host's pnpm minimumReleaseAge and
// maintains the minimumReleaseAgeExclude block that CI itself never applies.
// Node built-ins only, and it never runs pnpm: pnpm rewrites the lock on this host.
import * as NodeChildProcess from "node:child_process";
import * as NodeCrypto from "node:crypto";
import * as NodeFSP from "node:fs/promises";
import * as NodePath from "node:path";
import * as NodeURL from "node:url";

/** Mirrors the host's `minimumReleaseAge: 30240` (21 days), in minutes. */
export const DEFAULT_MIN_AGE_MINUTES = 30_240;
export const REGISTRY = "https://registry.npmjs.org";
const RETRIES = 3;
/** Waits before each retry; a longer registry outage still fails closed. */
const BACKOFF_MS = [2_000, 5_000, 15_000];
const CONCURRENCY = 16;
const REQUEST_TIMEOUT_MS = 60_000;
const EXCLUDE_KEY = "minimumReleaseAgeExclude";
const EXACT_VERSION = /^\d+\.\d+\.\d+(?:-[0-9A-Za-z.-]+)?(?:\+[0-9A-Za-z.-]+)?$/;

const idOf = (name, version) => `${name}@${version}`;
const compare = (a, b) => (a < b ? -1 : a > b ? 1 : 0);
const defaultSleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));
const formatDays = (minutes) => `${(minutes / 1440).toFixed(1)}d`;

function unquote(value) {
  const quote = value[0];
  return (quote === '"' || quote === "'") && value.at(-1) === quote ? value.slice(1, -1) : value;
}

/**
 * Registry packages from the lockfile's final YAML document. File, directory,
 * git and tarball resolutions are skipped: the registry has no publish time for them.
 */
export function parseLockPackages(lockText) {
  // The first document locks packageManagerDependencies (pnpm, @pnpm/exe): pnpm resolves
  // and installs them without minimumReleaseAge, and verifies only the final document.
  const documents = lockText.split(/^---[ \t]*$/m);
  const lines = documents.at(-1).split("\n");
  const packages = new Map();
  let inPackages = false;
  let current;
  for (const line of lines) {
    if (/^\S/.test(line)) {
      inPackages = line.trimEnd() === "packages:";
      current = undefined;
      continue;
    }
    if (!inPackages) continue;
    const key = /^ {2}(\S.*):[ \t]*$/.exec(line);
    if (key) {
      const full = unquote(key[1]);
      const at = full.indexOf("@", 1);
      current =
        at > 0 && EXACT_VERSION.test(full.slice(at + 1))
          ? { name: full.slice(0, at), version: full.slice(at + 1) }
          : undefined;
      continue;
    }
    const resolution = /^ {4}resolution: (\{.*\})[ \t]*$/.exec(line);
    if (resolution && current && /^\{integrity: [^,}\s]+\}$/.test(resolution[1])) {
      packages.set(idOf(current.name, current.version), current);
    }
  }
  return packages;
}

/** The `minimumReleaseAgeExclude` block: its entries and the line range it occupies. */
export function parseExcludeBlock(workspaceText) {
  const lines = workspaceText.split("\n");
  const start = lines.findIndex((line) => new RegExp(`^${EXCLUDE_KEY}:`).test(line));
  if (start < 0) return { start: -1, end: -1, entries: [], comments: false };
  const header = lines[start].replace(/\s+#.*$/, "").trimEnd();
  if (header !== `${EXCLUDE_KEY}:` && header !== `${EXCLUDE_KEY}: []`) {
    throw new Error(`${EXCLUDE_KEY} must be a block list, one "name@version" per line`);
  }
  const entries = [];
  let comments = false;
  let pendingComment = false;
  let end = start + 1;
  for (let index = start + 1; index < lines.length; index++) {
    const line = lines[index];
    if (/^\s*$/.test(line)) continue;
    if (/^\s*#/.test(line)) {
      pendingComment = true;
      continue;
    }
    // YAML also allows a mapping's list items at column 0.
    const item = /^\s*-\s+("[^"]*"|'[^']*'|\S+)\s*(?:#.*)?$/.exec(line);
    if (!item) break;
    // A comment only belongs to the block when more list items follow it.
    comments ||= pendingComment;
    pendingComment = false;
    entries.push({ raw: unquote(item[1]), line: index + 1 });
    end = index + 1;
  }
  return { start, end, entries, comments };
}

function splitExact(raw) {
  if (/[*?]/.test(raw)) return { problem: "uses a wildcard" };
  const at = raw.indexOf("@", 1);
  if (at < 0) return { problem: "has no version" };
  const name = raw.slice(0, at);
  const version = raw.slice(at + 1);
  if (!EXACT_VERSION.test(version)) return { problem: "is not an exact version" };
  return { name, version };
}

/**
 * Exclusions must name one exact locked version. Anything broader would keep
 * excluding the next young version of the same package.
 */
export function validateExcludes(entries, lockPackages) {
  const problems = [];
  const seen = new Set();
  const valid = new Map();
  for (const { raw, line } of entries) {
    const exact = splitExact(raw);
    if (exact.problem) {
      problems.push(`pnpm-workspace.yaml:${line}: "${raw}" ${exact.problem}; use "name@x.y.z".`);
      continue;
    }
    const id = idOf(exact.name, exact.version);
    if (seen.has(id)) problems.push(`pnpm-workspace.yaml:${line}: "${raw}" is listed twice.`);
    seen.add(id);
    if (!lockPackages.has(id)) {
      problems.push(
        `pnpm-workspace.yaml:${line}: "${raw}" is not a registry package in pnpm-lock.yaml; remove it.`,
      );
      continue;
    }
    valid.set(id, exact);
  }
  return { problems, valid };
}

/** Replace the exclusion block with `ids`, sorted. Everything else is kept byte for byte. */
export function rewriteExcludeBlock(workspaceText, ids) {
  const block = parseExcludeBlock(workspaceText);
  if (block.comments) {
    throw new Error(`${EXCLUDE_KEY} contains comments; move them out before using --write.`);
  }
  const sorted = [...new Set(ids)].sort(compare);
  const body = sorted.length
    ? [`${EXCLUDE_KEY}:`, ...sorted.map((id) => `  - "${id}"`)]
    : [`${EXCLUDE_KEY}: []`];
  if (block.start < 0) {
    if (!sorted.length) return workspaceText;
    return `${workspaceText.replace(/\n*$/, "\n")}\n${body.join("\n")}\n`;
  }
  const lines = workspaceText.split("\n");
  lines.splice(block.start, block.end - block.start, ...body);
  return lines.join("\n");
}

async function fetchTimes(name, wanted, { fetch, sleep, retries }) {
  const url = `${REGISTRY}/${encodeURIComponent(name).replace(/^%40/, "@")}`;
  let failure = "";
  for (let attempt = 0; attempt <= retries; attempt++) {
    if (attempt > 0) await sleep(BACKOFF_MS[Math.min(attempt, BACKOFF_MS.length) - 1]);
    try {
      const response = await fetch(url, {
        headers: { accept: "application/json" },
        signal: AbortSignal.timeout(REQUEST_TIMEOUT_MS),
      });
      if (response.status === 404) return { error: "not found in the npm registry" };
      if (!response.ok) {
        failure = `registry answered HTTP ${response.status}`;
        if (response.status !== 429 && response.status < 500) return { error: failure };
        continue;
      }
      const time = (await response.json()).time ?? {};
      return { times: new Map(wanted.map((version) => [version, time[version]])) };
    } catch (error) {
      failure = `registry request failed: ${error instanceof Error ? error.message : error}`;
    }
  }
  return { error: `${failure} (after ${retries} retries)` };
}

/**
 * Publish times for the given packages, one registry request per package name.
 * Resolves to a Map of `name@version` to `{ published: Date }` or `{ error }`.
 */
export async function lookupPublishTimes(packages, deps) {
  const { fetch, sleep = defaultSleep, retries = RETRIES, concurrency = CONCURRENCY } = deps;
  const byName = new Map();
  for (const { name, version } of packages) {
    byName.set(name, [...(byName.get(name) ?? []), version]);
  }
  const queue = [...byName];
  const results = new Map();
  const worker = async () => {
    for (let next = queue.shift(); next; next = queue.shift()) {
      const [name, versions] = next;
      const found = await fetchTimes(name, versions, { fetch, sleep, retries });
      for (const version of versions) {
        const published = found.times?.get(version);
        const time = published ? new Date(published) : undefined;
        results.set(
          idOf(name, version),
          time && !Number.isNaN(time.getTime())
            ? { published: time }
            : { error: found.error ?? "the registry lists no publish time for this version" },
        );
      }
    }
  };
  await Promise.all(Array.from({ length: Math.min(concurrency, queue.length) }, worker));
  return results;
}

const USAGE = [
  "usage: release-age.mjs [--min-age-minutes N] [--root DIR]",
  "         (no mode)      validate the exclusion list offline",
  "         --base <sha>   also check lock entries added since <sha>",
  "         --all          also check every locked package",
  "         --all --write  rewrite the exclusion block to match",
].join("\n");

const sha256 = (text) => NodeCrypto.createHash("sha256").update(text).digest("hex");

/**
 * Boundary dispositions in resolutions.json bind pnpm-workspace.yaml by hash, so
 * any exclusion edit stales them. An anchor is rebound only when it approved the
 * file exactly as it was before the rewrite, so the exclusion block is the only
 * change. Any other anchor stays stale for boundary review and is counted.
 */
export function refreshBoundaryAnchors(resolutionsText, previousText, nextText) {
  const previous = sha256(previousText);
  const next = sha256(nextText);
  let stale = 0;
  const text = resolutionsText.replace(
    /("path": "pnpm-workspace\.yaml",\s*"sha256": ")([0-9a-f]{64})(")/g,
    (match, head, hash, tail) => {
      if (hash === previous) return `${head}${next}${tail}`;
      if (hash !== next) stale += 1;
      return match;
    },
  );
  return { text, stale };
}

class UsageError extends Error {}

function readOptions(argv) {
  const options = { base: undefined, all: false, write: false, minAge: DEFAULT_MIN_AGE_MINUTES };
  for (let index = 0; index < argv.length; index++) {
    const arg = argv[index];
    if (arg === "--all") options.all = true;
    else if (arg === "--write") options.write = true;
    else if (arg === "--base" || arg === "--min-age-minutes" || arg === "--root") {
      const value = argv[++index];
      if (value === undefined || value.startsWith("--"))
        throw new UsageError(`${arg} needs a value`);
      if (arg === "--base") options.base = value;
      else if (arg === "--root") options.root = value;
      else if (/^\d+$/.test(value)) options.minAge = Number(value);
      else throw new UsageError("--min-age-minutes needs a whole number of minutes");
    } else throw new UsageError(`Unknown option: ${arg}`);
  }
  if (options.write && !options.all) throw new UsageError("--write requires --all");
  if (options.base !== undefined && options.all)
    throw new UsageError("--base and --all are exclusive");
  return options;
}

function defaultReadBaseFile(root, sha, path) {
  // A leading dash would read as a git option; the rest is a plain ref or SHA.
  if (!/^[A-Za-z0-9][A-Za-z0-9._/^~-]*$/.test(sha)) {
    throw new UsageError(`--base needs a commit or ref, got "${sha}"`);
  }
  return NodeChildProcess.execFileSync("git", ["show", `${sha}:${path}`], {
    cwd: root,
    encoding: "utf8",
    maxBuffer: 256 * 1024 * 1024,
    stdio: ["ignore", "pipe", "pipe"],
  });
}

const FIX =
  "Fix: run `node scripts/jones/ci/release-age.mjs --all --write` and commit pnpm-workspace.yaml.\n" +
  "Exclusions are exact versions and expire on their own; do not change the host pnpm policy.";

/**
 * Run the guard. Exit codes: 0 pass, 1 policy violation, 2 usage error or the
 * registry could not be read (an unverified lock never passes).
 */
export async function run(argv, deps = {}) {
  const {
    fetch = globalThis.fetch,
    now = () => new Date(),
    sleep = defaultSleep,
    readFile = (path) => NodeFSP.readFile(path, "utf8"),
    writeFile = (path, text) => NodeFSP.writeFile(path, text),
    readBaseFile = defaultReadBaseFile,
    out = (text) => process.stdout.write(`${text}\n`),
    err = (text) => process.stderr.write(`${text}\n`),
    retries,
    concurrency,
  } = deps;
  try {
    const options = readOptions(argv);
    const root =
      options.root ?? NodePath.resolve(NodeURL.fileURLToPath(import.meta.url), "../../../..");
    const workspacePath = NodePath.join(root, "pnpm-workspace.yaml");
    const workspaceText = await readFile(workspacePath);
    const lock = parseLockPackages(await readFile(NodePath.join(root, "pnpm-lock.yaml")));
    const block = parseExcludeBlock(workspaceText);
    const { problems, valid } = validateExcludes(block.entries, lock);
    const lookup = { fetch, sleep, retries, concurrency };
    const nowMs = now().getTime();
    const ageOf = (published) => (nowMs - published.getTime()) / 60_000;
    const describe = (id, published) =>
      `${id} (published ${published.toISOString().slice(0, 10)}, ${formatDays(ageOf(published))} old)`;

    if (options.write) {
      if (problems.length) out(`Pruning stale exclusions:\n  ${problems.join("\n  ")}`);
    } else if (problems.length) {
      err(`Exclusion list problems:\n  ${problems.join("\n  ")}\n${FIX}`);
      return 1;
    }
    if (options.base === undefined && !options.all) {
      out(`Release-age exclusions are valid (${valid.size} exact versions).`);
      return 0;
    }

    let candidates;
    if (options.all) candidates = [...lock.values()];
    else {
      const base = parseLockPackages(readBaseFile(root, options.base, "pnpm-lock.yaml"));
      candidates = [...lock.values()].filter((entry) => !base.has(idOf(entry.name, entry.version)));
      // An exclusion dropped since base can expose a version that is still young.
      const baseExcludes = parseExcludeBlock(
        readBaseFile(root, options.base, "pnpm-workspace.yaml"),
      ).entries;
      const unexcluded = new Map();
      for (const { raw } of baseExcludes) {
        const exact = splitExact(raw);
        if (exact.problem) continue;
        const id = idOf(exact.name, exact.version);
        if (!valid.has(id) && lock.has(id) && base.has(id)) unexcluded.set(id, lock.get(id));
      }
      candidates.push(...unexcluded.values());
      if (!candidates.length) {
        out(
          `No new registry packages or dropped exclusions since ${options.base}; nothing to check.`,
        );
        return 0;
      }
    }

    const times = await lookupPublishTimes(candidates, lookup);
    const unverified = [...times].filter(([, result]) => result.error);
    if (unverified.length) {
      err(
        `Could not verify ${unverified.length} package(s) against ${REGISTRY}:\n  ${unverified
          .slice(0, 20)
          .map(([id, result]) => `${id}: ${result.error}`)
          .join("\n  ")}`,
      );
      return 2;
    }

    const young = [];
    const agedOut = [];
    for (const [id, { published }] of times) {
      const tooYoung = ageOf(published) < options.minAge;
      if (tooYoung) young.push({ id, published });
      else if (valid.has(id)) agedOut.push({ id, published });
    }
    young.sort((a, b) => compare(a.id, b.id));
    agedOut.sort((a, b) => compare(a.id, b.id));
    const policy = `${options.minAge} minutes (${formatDays(options.minAge)})`;

    if (options.write) {
      const keep = new Set(young.map(({ id }) => id));
      const next = rewriteExcludeBlock(workspaceText, keep);
      if (next !== workspaceText) await writeFile(workspacePath, next);
      const resolutionsPath = NodePath.join(root, "scripts/jones/boundary/resolutions.json");
      const resolutions = await readFile(resolutionsPath).catch((error) => {
        if (error?.code === "ENOENT") return undefined;
        throw error;
      });
      const refreshed =
        resolutions === undefined
          ? undefined
          : refreshBoundaryAnchors(resolutions, workspaceText, next);
      if (refreshed && refreshed.text !== resolutions) {
        await writeFile(resolutionsPath, refreshed.text);
        out("Refreshed the pnpm-workspace.yaml hashes in scripts/jones/boundary/resolutions.json.");
      }
      if (refreshed?.stale) {
        err(
          `Boundary review needed: ${refreshed.stale} pnpm-workspace.yaml anchor(s) in ` +
            "scripts/jones/boundary/resolutions.json did not approve the file as it was before " +
            "this write, so they were left stale. Review those dispositions with " +
            "`node scripts/jones/check-boundary.ts --strict`.",
        );
      }
      const added = young.filter(({ id }) => !valid.has(id)).length;
      out(
        `Rewrote ${EXCLUDE_KEY}: ${keep.size} entries (${added} added, ${
          block.entries.length - (keep.size - added)
        } removed) for policy ${policy}.`,
      );
      return 0;
    }

    if (agedOut.length) {
      out(
        `Aged-out exclusions (old enough now; --all --write removes them):\n  ${agedOut
          .map(({ id, published }) => describe(id, published))
          .join("\n  ")}`,
      );
    }
    const violations = young.filter(({ id }) => !valid.has(id));
    if (violations.length) {
      err(
        `${violations.length} locked version(s) are younger than the ${policy} release-age policy and are not excluded:\n  ${violations
          .map(({ id, published }) => describe(id, published))
          .join("\n  ")}\n${FIX}`,
      );
      return 1;
    }
    out(`Checked ${times.size} locked package(s) against ${policy}: none violate it.`);
    return 0;
  } catch (error) {
    const message = error instanceof Error ? error.message : String(error);
    err(error instanceof UsageError ? `${message}\n${USAGE}` : message);
    return 2;
  }
}

if (
  process.argv[1] &&
  import.meta.url === NodeURL.pathToFileURL(NodePath.resolve(process.argv[1])).href
) {
  process.exitCode = await run(process.argv.slice(2));
}
