// @effect-diagnostics nodeBuiltinImport:off - readdir with file types avoids an lstat per child
import * as NodeFSP from "node:fs/promises";
import * as NodeModule from "node:module";
import * as NodeFS from "node:fs";
import * as NodePath from "node:path";

import type {
  DirItem,
  FileItem,
  FileFinder as FileFinderType,
  GrepCursor,
  MixedItem,
  Result,
  WatchEvent,
  WatchUnsubscribe,
} from "@ff-labs/fff-node";
import * as Clock from "effect/Clock";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Option from "effect/Option";
import * as Path from "effect/Path";

import type {
  ProjectEntry,
  ProjectEntryKind,
  ProjectSearchContentsInput,
  ProjectSearchContentsResult,
  ProjectSearchEntriesResult,
} from "@t3tools/contracts";
import * as HostProcess from "@t3tools/shared/HostProcess";
import { PROJECT_FAVICON_EXTENSIONS, isProjectFaviconPath } from "@t3tools/shared/projectFavicon";
import { scoreSubsequenceMatch } from "@t3tools/shared/searchRanking";

import { CurrentProbeOwner, WorkspaceSearchProbe } from "../serverCompute/searchProbeClient.ts";
import {
  PROBE_LIMITS,
  probeFailure,
  type SearchGitProbeFailure,
} from "../serverCompute/searchProbeProtocol.ts";
import {
  type IgnoreDecision,
  type IgnoreFile,
  parseIgnoreFile,
} from "../../workspace/IgnorePatterns.ts";

// fff-node stays external to the CLI bundle because it dlopens a native
// library. A static `import` of an external package is a hard error inside a
// Node single-executable (only built-ins resolve there), so load it through
// `require`, which reads from the real filesystem in every runtime.
const requireForFff = NodeModule.createRequire(import.meta.url);
// Resolve through the filesystem, including the repository's ASAR/native-loading patch.
// Refuse an accidentally substituted package before creating any finder.
let packageDirectory = NodePath.dirname(requireForFff.resolve("@ff-labs/fff-node"));
let pinned = false;
for (let depth = 0; depth < 4; depth++) {
  try {
    const metadata = JSON.parse(
      NodeFS.readFileSync(NodePath.join(packageDirectory, "package.json"), "utf8"),
    ) as {
      name?: string;
      version?: string;
    };
    if (metadata.name === "@ff-labs/fff-node") {
      pinned = metadata.version === "0.11.0";
      break;
    }
  } catch {
    /* The resolved JS entry may be nested below the package root. */
  }
  packageDirectory = NodePath.dirname(packageDirectory);
}
if (!pinned) throw new Error("Unsupported workspace native package.");
const { FileFinder } = requireForFff("@ff-labs/fff-node") as typeof import("@ff-labs/fff-node");

// A glob intersects existing extension constraints; fff ORs bare `*.ext` constraints together.
const IMAGE_SEARCH_CONSTRAINT = `**/*.{${PROJECT_FAVICON_EXTENSIONS.map((extension) =>
  extension.slice(1).replace(/[a-z]/g, (letter) => `[${letter}${letter.toUpperCase()}]`),
).join(",")}}`;

// fff 0.11 disables filename fallback above this match count per base/overflow segment.
const FFF_FILENAME_FALLBACK_MAX_MATCHES = 15_000;

const WORKSPACE_INDEX_MAX_ENTRIES = 25_000;
const WORKSPACE_INDEX_PAGE_SIZE = WORKSPACE_INDEX_MAX_ENTRIES + 2;
const WORKSPACE_INDEX_SCAN_TIMEOUT = "15 seconds";
const WORKSPACE_INDEX_SCAN_TIMEOUT_MS = 15_000;
const CONTENT_SEARCH_MAX_MATCHES_PER_FILE = 100;
// fff's lineContent cap; matches past it are reported without ranges.
const CONTENT_LINE_MAX_LENGTH = 512;
const SYMLINK_SCAN_CONCURRENCY = 16;
// A scan no watcher can invalidate is reused this long, so typing does not rescan every folder.
const UNWATCHED_SYMLINK_SCAN_TTL_MS = 2_000;
// A rescan that outlasts this leaves the read on the previous scan.
const SYMLINK_RESCAN_WAIT = "5 millis";
// Each page is a synchronous native call; small pages let searches run between them.
const SYMLINK_DIRECTORY_PAGE_SIZE = 1_000;

import type {
  WorkspaceSearchIndex,
  WorkspaceSearchIndexVariant,
} from "../../workspace/WorkspaceSearchIndex.ts";
export type { WorkspaceSearchIndex } from "../../workspace/WorkspaceSearchIndex.ts";
import {
  WorkspaceSearchIndexCreateFailed,
  WorkspaceSearchIndexScanTimedOut,
  WorkspaceSearchIndexSearchFailed,
  WorkspaceSearchIndexRefreshFailed,
  WorkspaceSearchIndexDestroyFailed,
} from "../serverCompute/workspaceErrors.ts";
import { collectContentSearchPages } from "./contentSearchPages.ts";

type SearchEvidence = Pick<WorkspaceSearchIndexSearchFailed, "queryLength" | "pageSize">;

// Only the host separator becomes `/`: on POSIX `\` is a file name character.
function toPosixPath(input: string, hostIsWindows: boolean): string {
  return hostIsWindows ? input.replaceAll("\\", "/") : input;
}

function trimDirectorySeparator(input: string): string {
  return input.endsWith("/") ? input.slice(0, -1) : input;
}

function parentPathOf(input: string): string | undefined {
  const separatorIndex = input.lastIndexOf("/");
  return separatorIndex === -1 ? undefined : input.slice(0, separatorIndex);
}

function toProjectEntry(item: MixedItem, hostIsWindows: boolean): ProjectEntry | null {
  const normalizedPath = trimDirectorySeparator(toPosixPath(item.item.relativePath, hostIsWindows));
  if (!normalizedPath) {
    return null;
  }

  return {
    path: normalizedPath,
    kind: item.type,
  };
}

function toFileEntry(item: FileItem, hostIsWindows: boolean): ProjectEntry | null {
  const normalizedPath = trimDirectorySeparator(toPosixPath(item.relativePath, hostIsWindows));
  return normalizedPath ? { path: normalizedPath, kind: "file" } : null;
}

function toDirectoryEntry(item: DirItem, hostIsWindows: boolean): ProjectEntry | null {
  const normalizedPath = trimDirectorySeparator(toPosixPath(item.relativePath, hostIsWindows));
  return normalizedPath ? { path: normalizedPath, kind: "directory" } : null;
}

function isLoneFuzzyPath(query: string): boolean {
  const trimmed = query.trim();
  return (
    !/\s/u.test(trimmed) &&
    (trimmed.startsWith("/") || (trimmed.endsWith("/") && !/^[*!\\]/u.test(trimmed)))
  );
}

type SearchPage<I> = { readonly items: ReadonlyArray<I>; readonly totalMatched: number };

function mapFileSearchResult(
  result: SearchPage<FileItem>,
  limit: number,
  hostIsWindows: boolean,
  imageOnly = false,
): ProjectSearchEntriesResult {
  const entries = result.items.flatMap((item) => {
    const entry = toFileEntry(item, hostIsWindows);
    return entry && (!imageOnly || isProjectFaviconPath(entry.path)) ? [entry] : [];
  });
  return {
    entries: entries.slice(0, limit),
    truncated: entries.length > limit || result.totalMatched > result.items.length,
  };
}

function mapDirectorySearchResult(
  result: SearchPage<DirItem>,
  limit: number,
  hostIsWindows: boolean,
): ProjectSearchEntriesResult {
  const entries = result.items.flatMap((item) => {
    const entry = toDirectoryEntry(item, hostIsWindows);
    return entry ? [entry] : [];
  });
  const rootDirectoryCount = result.items.some((item) => item.relativePath.length === 0) ? 1 : 0;
  return {
    entries: entries.slice(0, limit),
    truncated: result.totalMatched - rootDirectoryCount > limit,
  };
}

function mapMixedSearchResult(
  result: SearchPage<MixedItem>,
  limit: number,
  hostIsWindows: boolean,
): { readonly entries: ProjectEntry[]; readonly truncated: boolean } {
  const entries: ProjectEntry[] = [];
  for (const item of result.items) {
    const entry = toProjectEntry(item, hostIsWindows);
    if (entry) {
      entries.push(entry);
    }
    if (entries.length >= limit) {
      break;
    }
  }

  const rootDirectoryCount = result.items.some(
    (item) => item.type === "directory" && item.item.relativePath.length === 0,
  )
    ? 1
    : 0;
  return {
    entries,
    truncated: result.totalMatched - rootDirectoryCount > limit,
  };
}

const WORD_CHARACTER = /[\p{Letter}\p{Mark}\p{Number}_]/u;

function codePointAt(line: string, index: number): string | undefined {
  const codePoint = line.codePointAt(index);
  return codePoint === undefined ? undefined : String.fromCodePoint(codePoint);
}

function codePointBefore(line: string, index: number): string | undefined {
  if (index <= 0) return undefined;
  const previousCodeUnit = line.charCodeAt(index - 1);
  const previousIndex =
    previousCodeUnit >= 0xdc00 && previousCodeUnit <= 0xdfff ? index - 2 : index - 1;
  return codePointAt(line, previousIndex);
}

function buildContentSearchQuery(input: Omit<ProjectSearchContentsInput, "cwd">): {
  readonly searchQuery: string;
  readonly regexMode: boolean;
} {
  if (input.caseSensitive) {
    return { searchQuery: input.query, regexMode: input.useRegex };
  }
  return input.useRegex
    ? { searchQuery: `(?i)${input.query}`, regexMode: true }
    : { searchQuery: input.query.toLowerCase(), regexMode: false };
}

function usesAsciiWholeWordPrefilter(input: Omit<ProjectSearchContentsInput, "cwd">): boolean {
  // fff parses glob and path syntax even in regex mode, so only a bare ASCII word qualifies.
  return input.wholeWord && !input.useRegex && /^\w+$/.test(input.query);
}

function findLiteralRanges(line: string, literal: RegExp) {
  return Array.from(line.matchAll(literal), (match) => ({
    start: match.index,
    end: match.index + match[0].length,
  }));
}

function mapContentMatchRanges(
  line: string,
  byteRanges: ReadonlyArray<readonly [number, number]>,
): Array<{ readonly start: number; readonly end: number }> {
  const lineBytes = Buffer.from(line);
  const toStringIndex = (byteOffset: number) => lineBytes.subarray(0, byteOffset).toString().length;
  return byteRanges.map(([startByte, endByte]) => ({
    start: toStringIndex(startByte),
    end: toStringIndex(endByte),
  }));
}

function escapeRegExp(input: string): string {
  return input.replace(/[\\^$.*+?()[\]{}|/]/g, "\\$&");
}

// Git before 2.55 reports wrong `--column` values for every match after a line's first, so
// later matches are found by their text, searching on from the end of the previous one.
function locateRegexMatches(
  line: string,
  matches: { readonly column: number; readonly texts: ReadonlyArray<string> } | undefined,
): Array<readonly [number, number]> {
  if (!matches) return [];
  const lineBytes = Buffer.from(line);
  const ranges: Array<readonly [number, number]> = [];
  let start = matches.column - 1;
  for (const text of matches.texts) {
    const textBytes = Buffer.from(text);
    if (ranges.length > 0) start = lineBytes.indexOf(textBytes, ranges.at(-1)![1]);
    if (start < 0) break;
    ranges.push([start, start + textBytes.length]);
  }
  return ranges;
}

// `git grep -z` records: NUL-separated fields, one record per line; a truncated last line is dropped.
function parseGitGrepRecords(
  stdout: string,
  fieldCount: number,
  truncated: boolean,
): Array<Array<string>> {
  const lines = stdout.split("\n");
  if (truncated || lines.at(-1) === "") lines.pop();
  const records: Array<Array<string>> = [];
  for (const line of lines) {
    const fields = line.split("\0");
    if (fields.length < fieldCount) continue;
    // Only the last field (the line or match text) can contain NUL bytes.
    records.push([...fields.slice(0, fieldCount - 1), fields.slice(fieldCount - 1).join("\0")]);
  }
  return records;
}

// Post-filter whole words: consuming boundary regexes swallow adjacent separators and widen ranges; \b misses punctuation edges.
function isWholeWordRange(
  line: string,
  range: { readonly start: number; readonly end: number },
): boolean {
  if (range.end <= range.start) return false;
  const isWord = (character: string | undefined) =>
    character !== undefined && WORD_CHARACTER.test(character);
  const leftIsBoundary =
    range.start === 0 ||
    !isWord(codePointBefore(line, range.start)) ||
    !isWord(codePointAt(line, range.start));
  const rightIsBoundary =
    range.end >= line.length ||
    !isWord(codePointAt(line, range.end)) ||
    !isWord(codePointBefore(line, range.end));
  return leftIsBoundary && rightIsBoundary;
}

function withDirectoryAncestors(entries: ReadonlyArray<ProjectEntry>): ProjectEntry[] {
  const entryByPath = new Map(entries.map((entry) => [entry.path, entry]));
  for (const entry of entries) {
    let parentPath = parentPathOf(entry.path);
    while (parentPath) {
      if (!entryByPath.has(parentPath)) {
        entryByPath.set(parentPath, { path: parentPath, kind: "directory" });
      }
      parentPath = parentPathOf(parentPath);
    }
  }
  return [...entryByPath.values()];
}

interface ScannedDirectory {
  readonly directory: string;
  readonly links: ReadonlyArray<ProjectEntry>;
  /** Every symlink in the folder, including ones dropped for leaving the workspace. */
  readonly linkPaths: ReadonlyArray<string>;
  /** A nested repository (or the workspace's own), whose ignore rules own what is below it. */
  readonly isRepository: boolean;
  readonly ignoreFile: string | undefined;
}

const joinEntryPath = (directory: string, name: string) =>
  directory ? `${directory}/${name}` : name;

/**
 * fff skips symlinks, and `followSymlinks` drops file links, dedupes folder links, and walks out
 * of the workspace. So links are read from the folders fff already indexed (ignore rules applied),
 * without descending into them, and kept only when they resolve inside the workspace.
 */
const scanDirectory = Effect.fn("WorkspaceSearchIndex.scanDirectory")(function* (
  fileSystem: FileSystem.FileSystem,
  path: Path.Path,
  cwd: string,
  root: string,
  directory: string,
) {
  // Effect's readDirectory returns bare names; one lstat per child would cost far more.
  const children = yield* Effect.promise(() =>
    NodeFSP.readdir(path.join(cwd, directory), { withFileTypes: true }).catch(() => []),
  );
  const links: ProjectEntry[] = [];
  const linkPaths: string[] = [];
  let isRepository = false;
  let ignoreFile: string | undefined;
  for (const child of children) {
    if (child.name === ".git") isRepository = true;
    if (child.name === ".ignore" && child.isFile()) {
      ignoreFile = yield* fileSystem
        .readFileString(path.join(cwd, directory, child.name))
        .pipe(Effect.orElseSucceed(() => undefined));
    }
    if (!child.isSymbolicLink()) continue;
    const entryPath = joinEntryPath(directory, child.name);
    linkPaths.push(entryPath);
    const absolutePath = path.join(cwd, entryPath);
    const target = yield* Effect.all([
      fileSystem.realPath(absolutePath),
      fileSystem.stat(absolutePath),
    ]).pipe(Effect.orElseSucceed(() => undefined));
    if (!target) continue;
    const relative = path.relative(root, target[0]);
    if (
      relative === ".." ||
      relative.startsWith(`..${path.sep}`) ||
      path.isAbsolute(relative) ||
      relative.split(path.sep).includes(".git") ||
      (target[1].type !== "Directory" && target[1].type !== "File")
    ) {
      continue;
    }
    links.push({ path: entryPath, kind: target[1].type === "Directory" ? "directory" : "file" });
  }
  return { directory, links, linkPaths, isRepository, ignoreFile } satisfies ScannedDirectory;
});

/** Whether `entryPath` is `directory` itself or below it. */
const isWithin = (entryPath: string, directory: string) =>
  directory === "" || entryPath.startsWith(`${directory}/`);

/**
 * fff reads `.ignore` files alongside git's rules, and like ripgrep the nearest one that matches
 * decides, ahead of any `.gitignore`. `ignoreFiles` is ordered deepest folder first.
 */
function decideByIgnoreFiles(
  entry: ProjectEntry,
  ignoreFiles: ReadonlyArray<{ readonly directory: string; readonly file: IgnoreFile }>,
): IgnoreDecision | undefined {
  for (const { directory, file } of ignoreFiles) {
    if (!isWithin(entry.path, directory) || entry.path === directory) continue;
    const relative = directory ? entry.path.slice(directory.length + 1) : entry.path;
    // Like git and ripgrep, a link is never a folder to a folder-only rule, whatever it targets.
    const decision = file.decide(relative, false);
    if (decision) return decision;
  }
  return undefined;
}

/** Places each symlink before the first index result that matches the query less closely. */
function mergeSymlinkMatches(
  indexed: ProjectSearchEntriesResult,
  linkPaths: ReadonlySet<string>,
  symlinks: ReadonlyArray<ProjectEntry>,
  query: string,
  limit: number,
): ProjectSearchEntriesResult {
  if (linkPaths.size === 0) return indexed;
  // fff's watcher indexes links created after its scan, under a kind of its own choosing, so a
  // path it already returned takes the link's kind (or drops out of a kind-filtered search).
  const symlinkByPath = new Map(symlinks.map((entry) => [entry.path, entry]));
  const score = (entry: ProjectEntry) =>
    scoreSubsequenceMatch(entry.path.toLowerCase(), query) ?? Number.POSITIVE_INFINITY;
  const indexedPaths = new Set(indexed.entries.map((entry) => entry.path));
  const pending = symlinks
    .flatMap((entry) => {
      const entryScore = score(entry);
      return Number.isFinite(entryScore) && !indexedPaths.has(entry.path)
        ? [{ entry, score: entryScore }]
        : [];
    })
    .toSorted((left, right) => left.score - right.score);
  const entries: ProjectEntry[] = [];
  let nextPending = 0;
  for (const indexedEntry of indexed.entries) {
    const entry = linkPaths.has(indexedEntry.path)
      ? symlinkByPath.get(indexedEntry.path)
      : indexedEntry;
    if (!entry) continue;
    const entryScore = score(entry);
    while (nextPending < pending.length && pending[nextPending]!.score < entryScore) {
      entries.push(pending[nextPending++]!.entry);
    }
    entries.push(entry);
  }
  for (; nextPending < pending.length; nextPending += 1) entries.push(pending[nextPending]!.entry);
  return {
    entries: entries.slice(0, limit),
    truncated: indexed.truncated || entries.length > limit,
  };
}

// Git also ignores paths from .git/info/exclude and the global excludes file.
const listGitIgnoredPaths = Effect.fn("WorkspaceSearchIndex.listGitIgnoredPaths")(function* (
  probes: WorkspaceSearchProbe["Service"],
  hostIsWindows: boolean,
) {
  const result = yield* probes.run({ kind: "ignoredPaths" });
  if (result.exitCode !== 0 || result.stdoutTruncated) return [];
  // Keep the trailing slash so directory-only rules can match.
  return result.stdout
    .split("\0")
    .filter(Boolean)
    .map((entry) => toPosixPath(entry, hostIsWindows));
});

function mixedItemPath(item: MixedItem): string {
  return item.item.relativePath;
}

function isIgnoredPath(
  ignoredPaths: ReadonlySet<string>,
  relativePath: string,
  hostIsWindows: boolean,
): boolean {
  if (ignoredPaths.size === 0) return false;
  let candidate: string | undefined = trimDirectorySeparator(
    toPosixPath(relativePath, hostIsWindows),
  );
  while (candidate) {
    if (ignoredPaths.has(candidate)) return true;
    candidate = parentPathOf(candidate);
  }
  return false;
}

const createFinder = Effect.fn("WorkspaceSearchIndex.createFinder")(function* (
  cwd: string,
  variant: WorkspaceSearchIndexVariant,
  qualification?: { readonly logFilePath: string },
) {
  const result = yield* Effect.try({
    try: () =>
      FileFinder.create({
        basePath: cwd,
        ...(qualification ? { logFilePath: qualification.logFilePath } : {}),
        disableMmapCache: true,
        // Content indexing costs CPU/memory; path-only consumers use the lightweight index.
        disableContentIndexing: variant !== "content",
        disableWatch: false,
        cacheBudgetMaxFiles: 1024,
        cacheBudgetMaxBytes: 32 * 1024 * 1024,
        // fff also applies its cache item limit to uncached grep reads. Keep its
        // default searchable file size; the byte/count limits still bound retained cache.
        aiMode: false,
        enableFsRootScanning: true,
        enableHomeDirScanning: true,
      }),
    catch: (cause) =>
      new WorkspaceSearchIndexCreateFailed({
        cwd,
        reason: "FileFinder.create threw unexpectedly.",
        cause,
      }),
  });
  if (result.ok) return result.value;
  return yield* new WorkspaceSearchIndexCreateFailed({
    cwd,
    reason: result.error,
  });
});

const waitForIndexReady = Effect.fn("WorkspaceSearchIndex.waitForIndexReady")(function* <E>(
  cwd: string,
  finder: FileFinderType,
  onFailure: (input: { readonly reason: string; readonly cause?: unknown }) => E,
): Effect.fn.Return<void, E | WorkspaceSearchIndexScanTimedOut> {
  const result = yield* Effect.tryPromise({
    try: () => finder.waitForIndexReady(WORKSPACE_INDEX_SCAN_TIMEOUT_MS),
    catch: (cause) =>
      onFailure({
        reason: "FileFinder.waitForIndexReady rejected unexpectedly.",
        cause,
      }),
  }).pipe(Effect.uninterruptible);
  if (!result.ok) {
    return yield* Effect.fail(onFailure({ reason: result.error }));
  }
  if (!result.value) {
    return yield* new WorkspaceSearchIndexScanTimedOut({
      cwd,
      timeout: WORKSPACE_INDEX_SCAN_TIMEOUT,
    });
  }
});

export const make = Effect.fn("WorkspaceSearchIndex.make")(function* (
  cwd: string,
  variant: WorkspaceSearchIndexVariant = "paths",
  qualification?: { readonly logFilePath: string },
) {
  const hostIsWindows = yield* HostProcess.isWindows;
  const fileSystem = yield* FileSystem.FileSystem;
  const path = yield* Path.Path;
  const finder = yield* Effect.acquireRelease(createFinder(cwd, variant, qualification), (finder) =>
    Effect.try({
      try: () => finder.destroy(),
      catch: (cause) => new WorkspaceSearchIndexDestroyFailed({ cwd, cause }),
    }).pipe(Effect.orDie),
  );
  let initialScanTimedOut = false;
  // The content index reads every file, which takes minutes where antivirus scans each open,
  // so content searches do not wait for it; git grep answers until it is ready.
  let contentIndexPending = variant === "content";
  const probes = yield* WorkspaceSearchProbe;
  const root = yield* fileSystem.realPath(cwd).pipe(Effect.orElseSucceed(() => cwd));

  /** The nearest `.ignore` with a pattern matching the path itself decides. */
  const makeIgnoreFileDecider = () => {
    const ignoreFiles = new Map<string, IgnoreFile | undefined>();
    const readIgnoreFile = Effect.fn("WorkspaceSearchIndex.readIgnoreFile")(function* (
      directory: string,
    ) {
      if (ignoreFiles.has(directory)) return ignoreFiles.get(directory);
      const contents = yield* Effect.gen(function* () {
        const actual = yield* fileSystem.realPath(path.join(root, directory, ".ignore"));
        const relative = path.relative(root, actual);
        // Ignore rules come from workspace files, including when a rule file is a symlink.
        if (relative === ".." || relative.startsWith(`..${path.sep}`) || path.isAbsolute(relative))
          return undefined;
        const info = yield* fileSystem.stat(actual);
        if (info.type !== "File") return undefined;
        return yield* fileSystem.readFileString(actual);
      }).pipe(Effect.orElseSucceed(() => undefined));
      const file = contents === undefined ? undefined : parseIgnoreFile(contents);
      ignoreFiles.set(directory, file);
      return file;
    });
    return Effect.fn("WorkspaceSearchIndex.decideByIgnoreFile")(function* (
      relativePath: string,
      isFolder: boolean,
    ) {
      let directory = parentPathOf(relativePath) ?? "";
      while (true) {
        const file = yield* readIgnoreFile(directory);
        const relative = directory ? relativePath.slice(directory.length + 1) : relativePath;
        const decision = file?.decide(relative, isFolder);
        if (decision || directory === "") return decision;
        directory = parentPathOf(directory) ?? "";
      }
    });
  };

  const loadIgnoredPaths = Effect.fn("WorkspaceSearchIndex.loadIgnoredPaths")(function* () {
    const decide = makeIgnoreFileDecider();
    const ignored = new Set<string>();
    for (const entry of yield* listGitIgnoredPaths(probes, hostIsWindows)) {
      const relativePath = trimDirectorySeparator(entry);
      if ((yield* decide(relativePath, entry.endsWith("/"))) !== "included") {
        ignored.add(relativePath);
      }
    }
    return ignored;
  });
  let ignoredPaths = yield* loadIgnoredPaths().pipe(
    Effect.mapError(
      (cause) => new WorkspaceSearchIndexCreateFailed({ cwd, reason: cause.message, cause }),
    ),
  );
  if (variant === "paths") {
    yield* waitForIndexReady(
      cwd,
      finder,
      ({ reason, cause }) =>
        new WorkspaceSearchIndexCreateFailed({
          cwd,
          reason,
          cause,
        }),
    ).pipe(
      Effect.catchTags({
        WorkspaceSearchIndexScanTimedOut: () =>
          Effect.sync(() => {
            initialScanTimedOut = true;
          }),
      }),
    );
  }

  // fff's isScanning takes the picker lock too, so progress is read off the JS thread.
  const isScanning = Effect.promise(async () => {
    try {
      const progress = await finder.getScanProgressAsync();
      return !progress.ok || progress.value.isScanning;
    } catch {
      return true;
    }
  });
  const hasIncompleteInitialScan = () => (initialScanTimedOut ? isScanning : Effect.succeed(false));

  const isContentIndexReady = Effect.fn("WorkspaceSearchIndex.isContentIndexReady")(function* () {
    if (!contentIndexPending) return true;
    const progress = yield* Effect.promise(() =>
      finder.getScanProgressAsync().catch(() => undefined),
    );
    if (progress?.ok && !progress.value.isScanning && progress.value.isWarmupComplete) {
      contentIndexPending = false;
    }
    return !contentIndexPending;
  });

  const runGitGrep = Effect.fn("WorkspaceSearchIndex.runGitGrep")(function* (
    pattern: string,
    options: {
      readonly regexMode: boolean;
      readonly ignoreCase: boolean;
      readonly maxMatchesPerFile: number;
      readonly onlyMatching: boolean;
      readonly pageSize: number;
    },
  ) {
    return yield* probes
      .run({
        kind: "grep",
        pattern,
        regexMode: options.regexMode,
        ignoreCase: options.ignoreCase,
        onlyMatching: options.onlyMatching,
      })
      .pipe(
        Effect.asSome,
        Effect.mapError(
          (cause) =>
            new WorkspaceSearchIndexSearchFailed({
              cwd,
              queryLength: pattern.length,
              pageSize: options.pageSize,
              reason: cause.message,
              cause,
            }),
        ),
      );
  });

  // fff leaves out what `.ignore` files exclude, which git cannot evaluate; the nearest
  // `.ignore` that matches a path or one of its folders decides, as in decideByIgnoreFiles.
  const makeIgnoreFileFilter = () => {
    const decide = makeIgnoreFileDecider();
    return Effect.fn("WorkspaceSearchIndex.isExcludedByIgnoreFiles")(function* (
      relativePath: string,
    ) {
      const segments = relativePath.split("/");
      for (let depth = 1; depth <= segments.length; depth += 1) {
        const candidate = segments.slice(0, depth).join("/");
        if ((yield* decide(candidate, depth < segments.length)) === "ignored") return true;
      }
      return false;
    });
  };

  // fff's grep before its content index is ready opens every file itself, as slowly as the build.
  // None when git cannot search this folder (not a repository, git missing, no PCRE for a regex).
  const searchContentsWithGit = Effect.fn("WorkspaceSearchIndex.searchContentsWithGit")(function* (
    input: Omit<ProjectSearchContentsInput, "cwd">,
  ): Effect.fn.Return<
    Option.Option<ProjectSearchContentsResult>,
    WorkspaceSearchIndexSearchFailed
  > {
    const options = {
      ignoreCase: !input.caseSensitive,
      maxMatchesPerFile: CONTENT_SEARCH_MAX_MATCHES_PER_FILE,
      onlyMatching: false,
      pageSize: input.limit,
    };
    let regexMode = input.useRegex;
    let regexFallbackError: string | undefined;
    let result = yield* runGitGrep(input.query, { ...options, regexMode });
    if (regexMode && Option.isSome(result) && result.value.exitCode === 128) {
      if (/USE_LIBPCRE|Perl-compatible/i.test(result.value.stderr)) return Option.none();
      // fff searches an invalid pattern as a literal and reports why.
      regexFallbackError = result.value.stderr.trim().split("\n")[0] || "Invalid regex";
      regexMode = false;
      result = yield* runGitGrep(input.query, { ...options, regexMode });
    }
    if (Option.isNone(result) || result.value.exitCode > 1) return Option.none();

    // Per line: the first match's byte column and every match's text, in order.
    const regexMatches = new Map<string, { readonly column: number; readonly texts: string[] }>();
    let rangesTruncated = false;
    if (regexMode) {
      // Ranges come from git too: running the user's pattern in JS could backtrack without bound.
      const ranges = yield* runGitGrep(input.query, { ...options, regexMode, onlyMatching: true });
      // Without ranges, whole-word filtering would drop every hit and report no matches.
      if (Option.isNone(ranges) || ranges.value.exitCode > 1) return Option.none();
      rangesTruncated = ranges.value.stdoutTruncated;
      if (ranges.value.exitCode === 0) {
        for (const record of parseGitGrepRecords(
          ranges.value.stdout,
          4,
          ranges.value.stdoutTruncated,
        )) {
          const [relativePath, lineNumber, column, text] = record as [
            string,
            string,
            string,
            string,
          ];
          const key = `${relativePath}\0${lineNumber}`;
          const lineMatches = regexMatches.get(key);
          if (lineMatches) lineMatches.texts.push(text);
          else regexMatches.set(key, { column: Number(column), texts: [text] });
        }
      }
    }
    // The trusted Git prefilter uses ASCII folding for bare words. Match its case rules
    // when deriving ranges, then apply the same Unicode word boundaries as the native path.
    const unicode = usesAsciiWholeWordPrefilter(input) ? "" : "u";
    const literal = regexMode
      ? undefined
      : new RegExp(escapeRegExp(input.query), `g${input.caseSensitive ? "" : "i"}${unicode}`);

    const isExcludedByIgnoreFiles = makeIgnoreFileFilter();
    const matches: Array<ProjectSearchContentsResult["matches"][number]> = [];
    let rangesCutOff = false;
    for (const record of parseGitGrepRecords(
      result.value.stdout,
      3,
      result.value.stdoutTruncated,
    )) {
      const [relativePath, lineNumber, rawLine] = record as [string, string, string];
      if (yield* isExcludedByIgnoreFiles(relativePath)) continue;
      const key = `${relativePath}\0${lineNumber}`;
      // Lines past the cut-off of a truncated ranges run have no ranges; stop there instead.
      if (rangesTruncated && !regexMatches.has(key)) {
        rangesCutOff = true;
        break;
      }
      const lineContent = (rawLine.endsWith("\r") ? rawLine.slice(0, -1) : rawLine).slice(
        0,
        CONTENT_LINE_MAX_LENGTH,
      );
      const matchRanges = (
        literal
          ? findLiteralRanges(lineContent, literal)
          : mapContentMatchRanges(
              lineContent,
              locateRegexMatches(lineContent, regexMatches.get(key)),
            )
      ).filter(
        (range) =>
          range.end <= lineContent.length &&
          (!input.wholeWord || isWholeWordRange(lineContent, range)),
      );
      if (input.wholeWord && matchRanges.length === 0) continue;
      matches.push({
        path: relativePath,
        lineNumber: Number(lineNumber),
        lineContent,
        matchRanges,
      });
      if (matches.length > input.limit) break;
    }
    return Option.some({
      matches: matches.slice(0, input.limit),
      truncated: matches.length > input.limit || result.value.stdoutTruncated || rangesCutOff,
      ...(regexFallbackError !== undefined ? { regexFallbackError } : {}),
    });
  });

  const runSearch = Effect.fn("WorkspaceSearchIndex.runSearch")(function* <A>(
    query: string,
    pageSize: number,
    operation: "directorySearch" | "fileSearch" | "grep" | "mixedSearch",
    execute: () => Promise<Result<A>>,
  ): Effect.fn.Return<A, WorkspaceSearchIndexSearchFailed> {
    // fff's searches wait on its picker lock, which its watcher can hold for minutes.
    const result = yield* Effect.tryPromise({
      try: execute,
      catch: (cause) =>
        new WorkspaceSearchIndexSearchFailed({
          cwd,
          queryLength: query.length,
          pageSize,
          reason: `FileFinder.${operation} rejected unexpectedly.`,
          cause,
        }),
    });
    if (!result.ok) {
      return yield* new WorkspaceSearchIndexSearchFailed({
        cwd,
        queryLength: query.length,
        pageSize,
        reason: result.error,
      });
    }
    return result.value;
  });

  const listIndexedDirectories = Effect.fn("WorkspaceSearchIndex.listIndexedDirectories")(
    function* () {
      const directories: string[] = [];
      // fff's pageIndex is an offset into the results, not a page number.
      for (let pageIndex = 0; ; pageIndex += SYMLINK_DIRECTORY_PAGE_SIZE) {
        const result = yield* Effect.promise(async () => {
          try {
            return await finder.directorySearchAsync("", {
              maxThreads: 1,
              pageIndex,
              pageSize: SYMLINK_DIRECTORY_PAGE_SIZE,
            });
          } catch {
            return undefined;
          }
        });
        if (!result?.ok) return directories;
        for (const item of result.value.items) {
          const directory = trimDirectorySeparator(toPosixPath(item.relativePath, hostIsWindows));
          if (directory) directories.push(directory);
        }
        if (result.value.items.length < SYMLINK_DIRECTORY_PAGE_SIZE) return directories;
      }
    },
  );

  /** Ignore rules belong to the repository that owns a path, so each one answers for its own. */
  const listGitIgnored = Effect.fn("WorkspaceSearchIndex.listGitIgnored")(function* (
    repository: string,
    entries: ReadonlyArray<ProjectEntry>,
    evidence: SearchEvidence,
  ) {
    const relativePaths = entries.map((entry) =>
      repository ? entry.path.slice(repository.length + 1) : entry.path,
    );
    // Bound each upload without dropping a repository's candidates. A completed
    // check-ignore failure applies to the whole group, never just its last batch.
    const batches: string[][] = [];
    let batch: string[] = [],
      bytes = 0;
    for (const relative of relativePaths) {
      const size = Buffer.byteLength(relative) + 1;
      if (size > PROBE_LIMITS.pathBytes + 1 || size > PROBE_LIMITS.stdinBytes)
        return yield* new WorkspaceSearchIndexSearchFailed({
          cwd,
          ...evidence,
          reason: "Git ignore candidate exceeds the limit.",
          cause: probeFailure("oversize"),
        });
      if (batch.length === PROBE_LIMITS.paths || bytes + size > PROBE_LIMITS.stdinBytes) {
        batches.push(batch);
        batch = [];
        bytes = 0;
      }
      batch.push(relative);
      bytes += size;
    }
    if (batch.length) batches.push(batch);
    const failedProbe = (cause: SearchGitProbeFailure) =>
      new WorkspaceSearchIndexSearchFailed({ cwd, ...evidence, reason: cause.message, cause });
    const ignored: string[] = [];
    for (const paths of batches) {
      const result = yield* Effect.result(
        probes.run({ kind: "ignoredCandidates", repository, paths }),
      );
      if (result._tag === "Success") {
        const output = result.success;
        if (output.exitCode === 1) continue;
        if (output.exitCode === 0 && !output.stdoutTruncated && !output.stdoutInvalidUtf8) {
          ignored.push(
            ...output.stdout
              .split("\0")
              .filter(Boolean)
              .map((relativePath) => (repository ? `${repository}/${relativePath}` : relativePath)),
          );
          continue;
        }
      } else if (result.failure.reason !== "process-failed") {
        return yield* failedProbe(result.failure);
      }

      // Git can exit before consuming stdin outside a repository. Require the
      // separate presence probe to prove absence even when the pipe write failed.
      const presence = yield* probes
        .run({ kind: "repositoryPresence", repository })
        .pipe(Effect.mapError(failedProbe));
      if (
        presence.exitCode === 128 &&
        /^fatal: not a git repository(?:[ :(.]|$)/i.test(presence.stderr) &&
        !presence.stderrTruncated &&
        !presence.stderrInvalidUtf8
      )
        return [];
      if (result._tag === "Failure") return yield* failedProbe(result.failure);
      if (presence.exitCode === 0 && !presence.stdoutTruncated && !presence.stdoutInvalidUtf8)
        return entries.map((entry) => entry.path);
      return yield* new WorkspaceSearchIndexSearchFailed({
        cwd,
        ...evidence,
        reason: "Repository presence is unavailable.",
        cause: probeFailure("process-failed"),
      });
    }
    return ignored;
  });

  const scanSymlinkEntries = Effect.fn("WorkspaceSearchIndex.scanSymlinkEntries")(function* (
    evidence: SearchEvidence,
  ) {
    const indexComplete = !(yield* isScanning);
    const directories = yield* listIndexedDirectories();
    const scanned = yield* Effect.forEach(
      ["", ...directories],
      (directory) => scanDirectory(fileSystem, path, cwd, root, directory),
      { concurrency: SYMLINK_SCAN_CONCURRENCY },
    );
    const candidates = scanned.flatMap((directory) => directory.links);
    const linkPaths = new Set(scanned.flatMap((directory) => directory.linkPaths));
    if (candidates.length === 0) return { entries: candidates, linkPaths, indexComplete };

    const ignoreFiles = scanned
      .flatMap(({ directory, ignoreFile }) =>
        ignoreFile === undefined ? [] : [{ directory, file: parseIgnoreFile(ignoreFile) }],
      )
      .toSorted((left, right) => right.directory.length - left.directory.length);
    const decisions = new Map(
      candidates.map((entry) => [entry.path, decideByIgnoreFiles(entry, ignoreFiles)]),
    );
    const repositories = [
      "",
      ...scanned.flatMap(({ directory, isRepository }) =>
        isRepository && directory ? [directory] : [],
      ),
    ].toSorted((left, right) => right.length - left.length);
    const entriesByRepository = Map.groupBy(
      candidates.filter((entry) => decisions.get(entry.path) === undefined),
      (entry) =>
        repositories.find(
          (repository) => isWithin(entry.path, repository) && entry.path !== repository,
        ) ?? "",
    );
    const ignored = new Set(
      (yield* Effect.forEach(
        entriesByRepository,
        ([repository, entries]) => listGitIgnored(repository, entries, evidence),
        { concurrency: 4 },
      )).flat(),
    );
    return {
      entries: candidates.filter((entry) =>
        decisions.get(entry.path) === undefined
          ? !ignored.has(entry.path)
          : decisions.get(entry.path) === "included",
      ),
      linkPaths,
      indexComplete,
    };
  });

  // One scan runs at a time and outlives the reads that started it. Only the first read waits for
  // it in full: after that a change or expiry marks the result stale, and reads wait briefly for
  // the rescan before answering from the last finished one, so file churn cannot stall search.
  type SymlinkScan = Effect.Success<ReturnType<typeof scanSymlinkEntries>>;
  let lastSymlinkScan: SymlinkScan | undefined;
  let lastSymlinkScanExpiresAt: number | undefined;
  let pendingSymlinkScan:
    | Deferred.Deferred<SymlinkScan, WorkspaceSearchIndexSearchFailed>
    | undefined;
  let symlinkScanStale = false;
  let knownLinkPaths: ReadonlySet<string> = new Set();
  const invalidateSymlinkScan = () => {
    symlinkScanStale = true;
  };
  const isKnownLink = (absolutePath: string) =>
    [cwd, root].some((base) =>
      knownLinkPaths.has(toPosixPath(path.relative(base, absolutePath), hostIsWindows)),
    );
  const onWatchBatch = (events: ReadonlyArray<WatchEvent>) => {
    // fff reports a link replaced in place as `modified`, so edits to known links count too.
    const editsOnly = events.every(
      (event) =>
        event.kind === "modified" &&
        !/(^|[\\/])\.(git)?ignore$/.test(event.path) &&
        !isKnownLink(event.path),
    );
    if (!editsOnly) invalidateSymlinkScan();
  };
  // fff refuses subscriptions until its watcher is up, which can trail the initial scan, so
  // each read retries until one is in place. Subscribing waits on fff's picker lock, which its
  // watcher holds while it indexes a new folder, so it runs off the JS thread.
  let unwatch: WatchUnsubscribe | undefined;
  yield* Effect.addFinalizer(() => Effect.sync(() => unwatch?.()));
  const ensureWatching = Effect.fn("WorkspaceSearchIndex.ensureWatching")(function* () {
    if (unwatch) return true;
    const result = yield* Effect.promise(() =>
      finder.watchAsync(onWatchBatch).catch(() => undefined),
    );
    if (result?.ok) unwatch = result.value;
    return unwatch !== undefined;
  });
  const indexScope = yield* Effect.scope;
  const startSymlinkScan = Effect.fn("WorkspaceSearchIndex.startSymlinkScan")(function* (
    evidence: SearchEvidence,
  ) {
    const lease = yield* probes.beginScan.pipe(
      Effect.mapError(
        (cause) =>
          new WorkspaceSearchIndexSearchFailed({ cwd, ...evidence, reason: cause.message, cause }),
      ),
    );
    symlinkScanStale = false;
    const done = yield* Deferred.make<SymlinkScan, WorkspaceSearchIndexSearchFailed>();
    pendingSymlinkScan = done;
    const scanDeadline =
      (yield* Clock.currentTimeMillis) + PROBE_LIMITS.residentMs - PROBE_LIMITS.cancelReserveMs;
    yield* Effect.gen(function* () {
      // Subscribing before the scan starts means no change during it goes unseen.
      const watched = yield* ensureWatching();
      const result = yield* scanSymlinkEntries(evidence);
      // Check the independent scan lifetime before publishing, including scans
      // whose final operation happened to require no Git probe.
      if ((yield* Clock.currentTimeMillis) >= scanDeadline)
        return yield* new WorkspaceSearchIndexSearchFailed({
          cwd,
          ...evidence,
          reason: "Resident scan lease expired.",
          cause: probeFailure("deadline"),
        });
      yield* lease.end.pipe(
        Effect.mapError(
          (cause) =>
            new WorkspaceSearchIndexSearchFailed({
              cwd,
              ...evidence,
              reason: cause.message,
              cause,
            }),
        ),
      );
      lastSymlinkScan = result;
      knownLinkPaths = result.linkPaths;
      // Without a watcher, or before fff has seen every folder, the result is kept only briefly.
      lastSymlinkScanExpiresAt =
        watched && result.indexComplete
          ? undefined
          : (yield* Clock.currentTimeMillis) + UNWATCHED_SYMLINK_SCAN_TTL_MS;
      return result;
    }).pipe(
      Effect.provideService(CurrentProbeOwner, lease.owner),
      Effect.timeoutOrElse({
        duration: PROBE_LIMITS.residentMs - PROBE_LIMITS.cancelReserveMs,
        orElse: () =>
          Effect.fail(
            new WorkspaceSearchIndexSearchFailed({
              cwd,
              ...evidence,
              reason: "Resident scan lease expired.",
              cause: probeFailure("deadline"),
            }),
          ),
      }),
      Effect.ensuring(lease.end.pipe(Effect.orDie)),
      Effect.onExit((exit) =>
        Effect.sync(() => {
          pendingSymlinkScan = undefined;
          if (exit._tag === "Failure") symlinkScanStale = true;
        }).pipe(Effect.andThen(Deferred.done(done, exit))),
      ),
      // The grant-to-fork handoff is masked; the independent scan body must
      // still observe its own timeout and index-scope cancellation.
      Effect.interruptible,
      Effect.forkIn(indexScope),
    );
    return done;
  }, Effect.uninterruptible);
  const listSymlinkEntries = Effect.fn("WorkspaceSearchIndex.listSymlinkEntries")(function* (
    evidence: SearchEvidence,
  ) {
    if (
      lastSymlinkScanExpiresAt !== undefined &&
      (yield* Clock.currentTimeMillis) >= lastSymlinkScanExpiresAt
    ) {
      symlinkScanStale = true;
    }
    const pending =
      pendingSymlinkScan ??
      (lastSymlinkScan === undefined || symlinkScanStale
        ? yield* startSymlinkScan(evidence)
        : undefined);
    let result = lastSymlinkScan;
    if (pending && result === undefined) {
      result = yield* Deferred.await(pending);
    } else if (pending) {
      const fresh = yield* Deferred.await(pending).pipe(Effect.timeoutOption(SYMLINK_RESCAN_WAIT));
      if (Option.isSome(fresh)) result = fresh.value;
    }
    if (result === undefined)
      return yield* Effect.die("The symlink scan finished without a result.");
    return {
      // Git's ignore rules hide a link the same way searchVisible hides the files it indexes.
      entries: result.entries.filter(
        (entry) => !isIgnoredPath(ignoredPaths, entry.path, hostIsWindows),
      ),
      linkPaths: result.linkPaths,
    };
  });

  // Grows the first page until hidden entries can no longer crowd visible ones out of it.
  const searchVisible = Effect.fn("WorkspaceSearchIndex.searchVisible")(function* <I, Metadata>(
    query: string,
    pageSize: number,
    operation: "directorySearch" | "fileSearch" | "mixedSearch",
    execute: (pageSize: number) => Promise<Result<SearchPage<I> & Metadata>>,
    pathOf: (item: I) => string,
  ): Effect.fn.Return<SearchPage<I> & Metadata, WorkspaceSearchIndexSearchFailed> {
    let size = pageSize;
    while (true) {
      const fetchSize = size;
      const result = yield* runSearch(query, fetchSize, operation, () => execute(fetchSize));
      if (ignoredPaths.size === 0) return result;
      const items = result.items.filter(
        (item) => !isIgnoredPath(ignoredPaths, pathOf(item), hostIsWindows),
      );
      if (
        items.length >= pageSize ||
        result.items.length >= result.totalMatched ||
        fetchSize >= WORKSPACE_INDEX_PAGE_SIZE
      ) {
        return {
          ...result,
          items,
          totalMatched: result.totalMatched - (result.items.length - items.length),
        };
      }
      size = Math.min(WORKSPACE_INDEX_PAGE_SIZE, fetchSize * 4);
    }
  });

  const refreshContent: WorkspaceSearchIndex["Service"]["refreshContent"] = Effect.fn(
    "WorkspaceSearchIndex.refreshContent",
  )(function* () {
    // A content rescan can retain stale native file bytes or miss newly created files.
    // Restart that index on the same handle; both operations run off the JS thread.
    const operation = variant === "content" ? "reindexAsync" : "scanFilesAsync";
    const result = yield* Effect.tryPromise({
      try: () => (variant === "content" ? finder.reindexAsync(cwd) : finder.scanFilesAsync()),
      catch: (cause) =>
        new WorkspaceSearchIndexRefreshFailed({
          cwd,
          reason: `FileFinder.${operation} rejected unexpectedly.`,
          cause,
        }),
    }).pipe(Effect.uninterruptible);
    if (!result.ok) {
      return yield* new WorkspaceSearchIndexRefreshFailed({
        cwd,
        reason: result.error,
      });
    }
    // A rescan that outlasts the wait keeps running, and the finder answers
    // from its previous index until it finishes, so the index stays usable.
    yield* waitForIndexReady(
      cwd,
      finder,
      ({ reason, cause }) =>
        new WorkspaceSearchIndexRefreshFailed({
          cwd,
          reason,
          cause,
        }),
    );
    ignoredPaths = yield* loadIgnoredPaths().pipe(
      Effect.mapError(
        (cause) => new WorkspaceSearchIndexRefreshFailed({ cwd, reason: cause.message, cause }),
      ),
    );
    invalidateSymlinkScan();
  });

  const refresh: WorkspaceSearchIndex["Service"]["refresh"] = () =>
    refreshContent().pipe(
      Effect.catchTags({ WorkspaceSearchIndexScanTimedOut: () => Effect.void }),
    );

  const list: WorkspaceSearchIndex["Service"]["list"] = Effect.fn("WorkspaceSearchIndex.list")(
    function* () {
      const incompleteBeforeQuery = yield* hasIncompleteInitialScan();
      const result = yield* searchVisible(
        "",
        WORKSPACE_INDEX_PAGE_SIZE,
        "mixedSearch",
        (pageSize) => finder.mixedSearchAsync("", { maxThreads: 1, pageSize }),
        mixedItemPath,
      );
      const mapped = mapMixedSearchResult(result, WORKSPACE_INDEX_MAX_ENTRIES, hostIsWindows);
      const symlinks = yield* listSymlinkEntries({
        queryLength: 0,
        pageSize: WORKSPACE_INDEX_PAGE_SIZE,
      });
      // fff's watcher can index a link the scan rejected; only accepted links are listed.
      const indexed = mapped.entries.filter((entry) => !symlinks.linkPaths.has(entry.path));
      const sortedEntries = withDirectoryAncestors([...indexed, ...symlinks.entries]).toSorted(
        (left, right) => left.path.localeCompare(right.path),
      );
      const entries = sortedEntries.slice(0, WORKSPACE_INDEX_MAX_ENTRIES);
      return {
        entries,
        truncated:
          incompleteBeforeQuery || mapped.truncated || entries.length < sortedEntries.length,
      };
    },
  );

  const searchNames = Effect.fn("WorkspaceSearchIndex.searchNames")(function* (
    query: string,
    limit: number,
    kind: ProjectEntryKind | undefined,
    imageOnly: boolean | undefined,
    evidence: SearchEvidence,
  ) {
    const { entries: allSymlinks, linkPaths } = yield* listSymlinkEntries(evidence);
    const symlinks = allSymlinks.filter((entry) =>
      imageOnly
        ? entry.kind === "file" && isProjectFaviconPath(entry.path)
        : kind === undefined || entry.kind === kind,
    );
    // Index results that are filtered-out links get dropped in the merge, so fetch past them.
    const indexLimit = limit + linkPaths.size - symlinks.length;
    const pageSize = Math.max(1, indexLimit + 1);
    const merge = (indexed: ProjectSearchEntriesResult) =>
      mergeSymlinkMatches(indexed, linkPaths, symlinks, query, limit);
    if (kind === "file" || imageOnly) {
      const searchFiles = (fileQuery: string, size: number) =>
        searchVisible(
          query,
          size,
          "fileSearch",
          (fetchSize) => finder.fileSearchAsync(fileQuery, { maxThreads: 1, pageSize: fetchSize }),
          (item: FileItem) => item.relativePath,
        );
      // Adding a constraint changes fff's lone-path parse; offset pages can skip tied matches.
      if (imageOnly && isLoneFuzzyPath(query)) {
        const result = yield* searchFiles(query, WORKSPACE_INDEX_PAGE_SIZE);
        return merge(mapFileSearchResult(result, indexLimit, hostIsWindows, true));
      }
      let result = yield* searchFiles(
        imageOnly ? `${query} ${IMAGE_SEARCH_CONSTRAINT}` : query,
        pageSize,
      );
      if (
        imageOnly &&
        query.trim() &&
        result.totalFiles > FFF_FILENAME_FALLBACK_MAX_MATCHES &&
        result.totalMatched > 0
      ) {
        const unfiltered = yield* runSearch(query, 1, "fileSearch", () =>
          finder.fileSearchAsync(query, { maxThreads: 1, pageSize: 1 }),
        );
        if (unfiltered.totalMatched > FFF_FILENAME_FALLBACK_MAX_MATCHES) {
          result = yield* searchFiles(query, WORKSPACE_INDEX_PAGE_SIZE);
        }
      }
      return merge(mapFileSearchResult(result, indexLimit, hostIsWindows, imageOnly));
    }
    if (kind === "directory") {
      const result = yield* searchVisible(
        query,
        pageSize,
        "directorySearch",
        (size) => finder.directorySearchAsync(query, { maxThreads: 1, pageSize: size }),
        (item) => item.relativePath,
      );
      return merge(mapDirectorySearchResult(result, indexLimit, hostIsWindows));
    }
    const result = yield* searchVisible(
      query,
      pageSize,
      "mixedSearch",
      (size) => finder.mixedSearchAsync(query, { maxThreads: 1, pageSize: size }),
      mixedItemPath,
    );
    return merge(mapMixedSearchResult(result, indexLimit, hostIsWindows));
  });

  const search: WorkspaceSearchIndex["Service"]["search"] = Effect.fn(
    "WorkspaceSearchIndex.search",
  )(function* (rawQuery, limit, kind, imageOnly) {
    // fff parses any word containing "[" or "{" as a glob and has no escape for them; fuzzy
    // matching is subsequence-based, so dropping them still matches "[id]" and "a{b}" paths.
    // "{" is dropped only when the query finds nothing as a glob, so "*.{ts,tsx}" keeps working.
    const query = rawQuery.replaceAll("[", "");
    const incompleteBeforeQuery = yield* hasIncompleteInitialScan();
    // A resident scan keeps the triggering read's budget, including if another
    // read later shares its pending result. Native page failures use runSearch's actual size.
    const evidence = {
      queryLength: rawQuery.length,
      pageSize: imageOnly ? WORKSPACE_INDEX_PAGE_SIZE : Math.max(1, limit + 1),
    };
    let mapped = yield* searchNames(query, limit, kind, imageOnly, evidence);
    if (mapped.entries.length === 0 && query.includes("{")) {
      mapped = yield* searchNames(query.replaceAll("{", ""), limit, kind, imageOnly, evidence);
    }
    return { ...mapped, truncated: incompleteBeforeQuery || mapped.truncated };
  });

  const searchContents: WorkspaceSearchIndex["Service"]["searchContents"] = Effect.fn(
    "WorkspaceSearchIndex.searchContents",
  )(function* (input) {
    if (!(yield* isContentIndexReady())) {
      const gitResult = yield* searchContentsWithGit(input);
      if (Option.isSome(gitResult)) return gitResult.value;
      yield* waitForIndexReady(
        cwd,
        finder,
        ({ reason, cause }) =>
          new WorkspaceSearchIndexSearchFailed({
            cwd,
            queryLength: input.query.length,
            pageSize: input.limit,
            reason,
            cause,
          }),
      );
      contentIndexPending = false;
    }
    const wholeWordPrefilter = usesAsciiWholeWordPrefilter(input);
    // Use ASCII boundaries in fff so all non-ASCII neighbours remain candidates. JS
    // recomputes ranges with ASCII case folding and checks the actual Unicode boundaries.
    const word = wholeWordPrefilter
      ? new RegExp(input.query, input.caseSensitive ? "g" : "gi")
      : undefined;
    let { searchQuery, regexMode } = wholeWordPrefilter
      ? {
          searchQuery: `(?${input.caseSensitive ? "" : "i"}-u)\\b${input.query}\\b`,
          regexMode: true,
        }
      : buildContentSearchQuery(input);
    const rawPageSize = input.wholeWord
      ? Math.max(input.limit, CONTENT_SEARCH_MAX_MATCHES_PER_FILE)
      : input.limit;
    const grepPage = (
      cursor: GrepCursor | null,
      budget: { readonly timeBudgetMs: number; readonly enforceTimeBudget: true },
    ) =>
      runSearch(input.query, input.limit, "grep", () =>
        finder.grepAsync(searchQuery, {
          mode: regexMode ? "regex" : "plain",
          smartCase: !input.caseSensitive && !regexMode,
          maxMatchesPerFile: Math.min(CONTENT_SEARCH_MAX_MATCHES_PER_FILE, rawPageSize),
          pageSize: rawPageSize,
          cursor,
          ...budget,
        }),
      );
    return yield* collectContentSearchPages({
      limit: input.limit,
      searchPage: (cursor, budget) =>
        grepPage(cursor, budget).pipe(
          Effect.flatMap((result) => {
            const { regexFallbackError } = result;
            if (
              !regexMode ||
              regexFallbackError === undefined ||
              (input.caseSensitive && !wholeWordPrefilter)
            )
              return Effect.succeed(result);
            ({ searchQuery, regexMode } = buildContentSearchQuery({ ...input, useRegex: false }));
            // A failed internal prefilter is retried at the same cursor and does not turn
            // the user's literal search into a regex error.
            return grepPage(cursor, budget).pipe(
              Effect.map((literal) =>
                wholeWordPrefilter ? literal : { ...literal, regexFallbackError },
              ),
            );
          }),
        ),
      mapMatch: (match) => {
        if (isIgnoredPath(ignoredPaths, match.relativePath, hostIsWindows)) return undefined;
        const matchRanges = (
          word && regexMode
            ? findLiteralRanges(match.lineContent, word)
            : mapContentMatchRanges(match.lineContent, match.matchRanges)
        ).filter((range) => !input.wholeWord || isWholeWordRange(match.lineContent, range));
        if (input.wholeWord && matchRanges.length === 0) return undefined;
        return {
          path: toPosixPath(match.relativePath, hostIsWindows),
          lineNumber: match.lineNumber,
          lineContent: match.lineContent,
          matchRanges,
        };
      },
    });
  });

  return {
    list,
    refresh,
    refreshContent,
    search,
    searchContents,
    status: async () => {
      const progress = await finder.getScanProgressAsync();
      if (!progress.ok) throw new Error("Native scan state unavailable.");
      return {
        scanning: progress.value.isScanning,
        warmupComplete: progress.value.isWarmupComplete,
      };
    },
  };
});
