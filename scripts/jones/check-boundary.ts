// @effect-diagnostics nodeBuiltinImport:off - The synchronous compiler-host and Git inspection CLI uses Node filesystem and subprocess APIs.
import * as NodeCrypto from "node:crypto";
import * as NodeChildProcess from "node:child_process";
import * as NodeFS from "node:fs";
import * as NodePath from "node:path";
import * as NodeURL from "node:url";
import ts from "typescript-legacy";

export const categories = [
  "jones-owned",
  "hook",
  "accepted-shared",
  "jones-tests-in-upstream",
  "generated",
  "lock",
  "policy",
  "upstream-deleted",
] as const;
type Category = (typeof categories)[number];
type Edge = { from: string; to: string };
export type Fragment = {
  schema: "jones.boundary-fragment/v1";
  feature: string;
  jonesOwnedPaths: string[];
  hookPaths: string[];
  tombstones: string[];
  allowedImports: Edge[];
};
export type Inventory = {
  schema: "jones.boundary-inventory/v1";
  upstream: { repository: string; tag: string; commit: string };
  baseline: string;
  paths: Record<string, Category>;
  jonesOwnedPaths: string[];
  tombstones: string[];
  allowedImports: Edge[];
};
export type ResolutionCase = {
  from: string;
  sourceSha256: string;
  expression: string;
  reason: string;
  anchors: { path: string; sha256: string }[];
  targets: string[];
};
export function parseResolutionCases(value: unknown): ResolutionCase[] {
  const input = object(value, ["schema", "cases"]);
  if (input.schema !== "jones.boundary-resolutions/v1" || !Array.isArray(input.cases))
    throw new Error("Invalid resolution dispositions");
  const result = input.cases.map((entry) => {
    const item = object(entry, [
      "from",
      "sourceSha256",
      "expression",
      "reason",
      "anchors",
      "targets",
    ]);
    if (
      typeof item.sourceSha256 !== "string" ||
      !/^[a-f0-9]{64}$/.test(item.sourceSha256) ||
      typeof item.expression !== "string" ||
      !item.expression.trim() ||
      typeof item.reason !== "string" ||
      !item.reason.trim() ||
      !Array.isArray(item.anchors)
    )
      throw new Error("Invalid source-bound resolution case");
    const anchors = item.anchors.map((entry) => {
      const anchor = object(entry, ["path", "sha256"]);
      if (typeof anchor.sha256 !== "string" || !/^[a-f0-9]{64}$/.test(anchor.sha256))
        throw new Error("Invalid resolution anchor hash");
      return { path: safePath(anchor.path), sha256: anchor.sha256 };
    });
    return {
      from: safePath(item.from),
      sourceSha256: item.sourceSha256,
      expression: item.expression,
      reason: item.reason,
      anchors,
      targets: paths(item.targets),
    };
  });
  if (new Set(result.map((entry) => `${entry.from}\0${entry.expression}`)).size !== result.length)
    throw new Error("Duplicate resolution disposition");
  return result;
}
function sha256(text: string): string {
  return NodeCrypto.createHash("sha256").update(text).digest("hex");
}
export type Tree = ReadonlyMap<string, string>;
export type Violation = { code: string; path: string; detail: string };
const sourcePattern = /\.(?:[cm]?[jt]sx?)$/;

function object(value: unknown, keys: string[]): Record<string, unknown> {
  if (!value || typeof value !== "object" || Array.isArray(value))
    throw new Error("Expected object");
  const record = value as Record<string, unknown>;
  if (
    Object.keys(record).some((key) => !keys.includes(key)) ||
    keys.some((key) => !(key in record))
  )
    throw new Error(`Expected exactly: ${keys.join(", ")}`);
  return record;
}
export function safePath(value: unknown, directory = false): string {
  if (
    typeof value !== "string" ||
    !value ||
    value.startsWith("-") ||
    value.includes("\\") ||
    /[\x00-\x1f\x7f:*?#[\]{}]/.test(value) ||
    NodePath.posix.isAbsolute(value)
  )
    throw new Error(`Unsafe repository path: ${String(value)}`);
  const checked = directory && value.endsWith("/") ? value.slice(0, -1) : value;
  if (
    checked.split("/").some((part) => !part || part === "." || part === "..") ||
    (!directory && value.endsWith("/"))
  )
    throw new Error(`Unsafe repository path: ${value}`);
  return value;
}
function paths(value: unknown, directory = false): string[] {
  if (!Array.isArray(value)) throw new Error("Expected path array");
  const result = value.map((entry) => safePath(entry, directory));
  if (new Set(result).size !== result.length) throw new Error("Duplicate path");
  return result;
}
function ownedPaths(value: unknown): string[] {
  const result = paths(value, true);
  for (const entry of result) {
    if (
      entry.endsWith("/") &&
      !/^(?:apps\/[^/]+\/src\/(?:jones\/[^/]+|(?:components\/)?[^/]+)|packages\/[^/]+\/src\/(?:jones\/[^/]+|[^/]+)|scripts\/jones|apps\/desktop\/scripts\/ui-evidence)\/$/.test(
        entry,
      )
    )
      throw new Error(`Ownership directory is too broad: ${entry}`);
  }
  return result;
}
function edges(value: unknown): Edge[] {
  if (!Array.isArray(value)) throw new Error("Expected import edges");
  const result = value.map((entry) => {
    const edge = object(entry, ["from", "to"]);
    return { from: safePath(edge.from), to: safePath(edge.to) };
  });
  if (new Set(result.map((edge) => JSON.stringify(edge))).size !== result.length)
    throw new Error("Duplicate import edge");
  return result;
}
export function parseInventory(value: unknown): Inventory {
  const input = object(value, [
    "schema",
    "upstream",
    "baseline",
    "paths",
    "jonesOwnedPaths",
    "tombstones",
    "allowedImports",
  ]);
  const upstream = object(input.upstream, ["repository", "tag", "commit"]);
  if (
    input.schema !== "jones.boundary-inventory/v1" ||
    upstream.repository !== "pingdotgg/t3code" ||
    typeof upstream.tag !== "string" ||
    !/^v[\w.-]+$/.test(upstream.tag)
  )
    throw new Error("Invalid inventory provenance");
  if (
    typeof upstream.commit !== "string" ||
    !/^[a-f0-9]{40}$/.test(upstream.commit) ||
    typeof input.baseline !== "string" ||
    !/^[a-f0-9]{40}$/.test(input.baseline)
  )
    throw new Error("Expected full commit SHA");
  if (!input.paths || typeof input.paths !== "object" || Array.isArray(input.paths))
    throw new Error("Expected classification map");
  const classified: Record<string, Category> = {};
  for (const [name, category] of Object.entries(input.paths)) {
    safePath(name);
    if (!categories.includes(category as Category))
      throw new Error(`Invalid category: ${String(category)}`);
    classified[name] = category as Category;
  }
  return {
    schema: input.schema,
    upstream: { repository: upstream.repository, tag: upstream.tag, commit: upstream.commit },
    baseline: input.baseline,
    paths: classified,
    jonesOwnedPaths: ownedPaths(input.jonesOwnedPaths),
    tombstones: paths(input.tombstones),
    allowedImports: edges(input.allowedImports),
  };
}
export function parseFragment(value: unknown): Fragment {
  const input = object(value, [
    "schema",
    "feature",
    "jonesOwnedPaths",
    "hookPaths",
    "tombstones",
    "allowedImports",
  ]);
  if (
    input.schema !== "jones.boundary-fragment/v1" ||
    typeof input.feature !== "string" ||
    !/^[a-z][a-z0-9-]*$/.test(input.feature)
  )
    throw new Error("Invalid fragment identity");
  return {
    schema: input.schema,
    feature: input.feature,
    jonesOwnedPaths: ownedPaths(input.jonesOwnedPaths),
    hookPaths: paths(input.hookPaths),
    tombstones: paths(input.tombstones),
    allowedImports: edges(input.allowedImports),
  };
}
function owns(name: string, entries: string[]): boolean {
  return entries.some((entry) => (entry.endsWith("/") ? name.startsWith(entry) : name === entry));
}
export type ImportReference = {
  specifier: string | undefined;
  typeOnly: boolean;
  expression: string;
};
export function imports(name: string, text: string): ImportReference[] {
  const file = ts.createSourceFile(name, text, ts.ScriptTarget.Latest, true);
  const diagnostics = (file as ts.SourceFile & { parseDiagnostics: readonly ts.Diagnostic[] })
    .parseDiagnostics;
  if (diagnostics.length)
    throw new Error(
      `Cannot parse source ${name}: ${ts.flattenDiagnosticMessageText(diagnostics[0]!.messageText, "\n")}`,
    );
  const result: ImportReference[] = [];
  function add(node: ts.Node | undefined, typeOnly = false) {
    result.push({
      specifier: node && ts.isStringLiteralLike(node) ? node.text : undefined,
      typeOnly,
      expression: node?.getText(file) ?? "<missing>",
    });
  }
  function visit(node: ts.Node) {
    if (ts.isImportDeclaration(node)) {
      const clause = node.importClause;
      const bindings = clause?.namedBindings;
      add(
        node.moduleSpecifier,
        clause?.isTypeOnly === true ||
          (!!clause &&
            !clause.name &&
            !!bindings &&
            ts.isNamedImports(bindings) &&
            bindings.elements.length > 0 &&
            bindings.elements.every((entry) => entry.isTypeOnly)),
      );
    } else if (ts.isExportDeclaration(node) && node.moduleSpecifier)
      add(
        node.moduleSpecifier,
        node.isTypeOnly ||
          (!!node.exportClause &&
            ts.isNamedExports(node.exportClause) &&
            node.exportClause.elements.length > 0 &&
            node.exportClause.elements.every((entry) => entry.isTypeOnly)),
      );
    else if (
      ts.isImportEqualsDeclaration(node) &&
      ts.isExternalModuleReference(node.moduleReference)
    )
      add(node.moduleReference.expression, node.isTypeOnly);
    else if (ts.isImportTypeNode(node) && ts.isLiteralTypeNode(node.argument))
      add(node.argument.literal, true);
    else if (
      ts.isCallExpression(node) &&
      (node.expression.kind === ts.SyntaxKind.ImportKeyword ||
        (ts.isIdentifier(node.expression) && node.expression.text === "require"))
    )
      add(node.arguments[0]);
    ts.forEachChild(node, visit);
  }
  visit(file);
  return result;
}
function packageAliases(tree: Tree): Map<string, string> {
  const result = new Map<string, string>();
  for (const [name, text] of tree) {
    if (!/^(?:(?:apps|packages)\/[^/]+|apps\/mobile\/modules\/[^/]+)\/package\.json$/.test(name))
      continue;
    const json: unknown = JSON.parse(text);
    if (!json || typeof json !== "object") throw new Error(`Invalid package metadata: ${name}`);
    const metadata = json as { name?: unknown; exports?: unknown };
    if (
      typeof metadata.name !== "string" ||
      !metadata.exports ||
      typeof metadata.exports !== "object"
    )
      continue;
    const root = NodePath.posix.dirname(name);
    function target(value: unknown): string | undefined {
      if (typeof value === "string") return value;
      if (!value || typeof value !== "object") return undefined;
      const conditions = value as Record<string, unknown>;
      return target(conditions.types) ?? target(conditions.import) ?? target(conditions.default);
    }
    for (const [subpath, value] of Object.entries(metadata.exports)) {
      const entry = target(value);
      if (!entry || !entry.startsWith("./") || !subpath.startsWith(".")) continue;
      result.set(
        metadata.name + (subpath === "." ? "" : subpath.slice(1)),
        NodePath.posix.join(root, entry),
      );
    }
  }
  return result;
}
function resolveImport(
  from: string,
  specifier: string,
  tree: Tree,
  aliases: Map<string, string>,
): { target?: string; internal: boolean } {
  let candidate: string | undefined;
  const assetSpecifier = specifier.replace(/\?(?:url|raw|inline)(?:&no-inline)?$/, "");
  if (specifier.startsWith("."))
    candidate = NodePath.posix.join(NodePath.posix.dirname(from), assetSpecifier);
  else if (specifier.startsWith("~/") && /^apps\/[^/]+\/src\//.test(from))
    candidate = `${from.split("/").slice(0, 2).join("/")}/src/${specifier.slice(2)}`;
  else candidate = aliases.get(specifier);
  const internal =
    candidate !== undefined ||
    specifier.startsWith("@t3tools/") ||
    specifier.startsWith("~/") ||
    specifier.startsWith("@/") ||
    specifier.startsWith("#") ||
    specifier.startsWith("/");
  if (!candidate) return { internal };
  const stem = candidate.replace(/\.(?:[cm]?js|jsx)$/, "");
  const candidates = [
    candidate,
    ...[".ts", ".tsx", ".mts", ".cts", ".js", ".jsx", "/index.ts", "/index.tsx", "/index.js"].map(
      (extension) => stem + extension,
    ),
  ];
  const resolved = candidates.find((name) => tree.has(name));
  return resolved ? { target: resolved, internal } : { internal };
}
export function checkBoundary(
  inventory: Inventory,
  fragments: Fragment[],
  tree: Tree,
  upstreamPaths: ReadonlySet<string>,
  changedPaths: readonly string[],
  resolutionCases: ResolutionCase[] = [],
) {
  const violations: Violation[] = [];
  const classified = new Map<string, Category>();
  const owned = [...inventory.jonesOwnedPaths];
  const retired = new Set(inventory.tombstones);
  const allowed = new Set(inventory.allowedImports.map(({ from, to }) => `${from}\0${to}`));
  const hooks = new Set<string>();
  const features = new Set<string>();
  for (const fragment of fragments) {
    if (features.has(fragment.feature))
      throw new Error(`Duplicate feature fragment: ${fragment.feature}`);
    features.add(fragment.feature);
    for (const entry of fragment.jonesOwnedPaths) {
      if (owned.some((existing) => owns(entry, [existing]) || owns(existing, [entry])))
        throw new Error(`Overlapping ownership: ${entry}`);
      owned.push(entry);
    }
    for (const entry of fragment.hookPaths) {
      if (hooks.has(entry)) throw new Error(`Overlapping hook: ${entry}`);
      hooks.add(entry);
    }
    for (const entry of fragment.tombstones) retired.add(entry);
    for (const { from, to } of fragment.allowedImports) allowed.add(`${from}\0${to}`);
  }
  // Source ancestry wins over ownership overrides: an existing upstream file cannot become an exempt directory member.
  for (const name of upstreamPaths) {
    if (owns(name, owned)) throw new Error(`Jones ownership overlaps upstream source: ${name}`);
  }
  for (const entry of hooks)
    if (!upstreamPaths.has(entry) && inventory.paths[entry] !== "hook")
      throw new Error(`Hook is not upstream: ${entry}`);
  for (const edge of allowed) {
    const [from, to] = edge.split("\0");
    if (
      !from ||
      !to ||
      !(upstreamPaths.has(from) || inventory.paths[from] === "hook" || hooks.has(from)) ||
      !(owns(to, owned) || inventory.paths[to] === "jones-owned")
    )
      throw new Error(`Invalid boundary edge: ${edge.replace("\0", " -> ")}`);
  }
  for (const name of changedPaths) {
    safePath(name);
    const category = hooks.has(name)
      ? "hook"
      : (inventory.paths[name] ?? (owns(name, owned) ? "jones-owned" : undefined));
    if (category) classified.set(name, category);
    else
      violations.push({
        code: "unclassified-path",
        path: name,
        detail: "Changed path has no explicit classification",
      });
    if (category === "upstream-deleted" && tree.has(name))
      violations.push({
        code: "upstream-deleted-resurrected",
        path: name,
        detail: "Accepted upstream deletion is present again",
      });
  }
  for (const name of retired)
    if (tree.has(name))
      violations.push({
        code: "tombstone-resurrected",
        path: name,
        detail: "Retired path is present",
      });
  const aliases = packageAliases(tree);
  const acceptedUnsupported: {
    path: string;
    expression: string;
    reason: string;
    targets: string[];
  }[] = [];
  const dispositions = new Map<string, ResolutionCase>();
  for (const disposition of resolutionCases) {
    const text = tree.get(disposition.from);
    const stale =
      text === undefined ||
      sha256(text) !== disposition.sourceSha256 ||
      disposition.anchors.some((anchor) => {
        const content = tree.get(anchor.path);
        return content === undefined || sha256(content) !== anchor.sha256;
      });
    if (stale)
      violations.push({
        code: "stale-resolution-disposition",
        path: disposition.from,
        detail: disposition.expression,
      });
    else dispositions.set(`${disposition.from}\0${disposition.expression}`, disposition);
  }
  let scannedFiles = 0;
  for (const [name, text] of tree) {
    if (!sourcePattern.test(name) || !/^(?:apps|packages|scripts)\//.test(name)) continue;
    const isUpstream =
      upstreamPaths.has(name) || inventory.paths[name] === "hook" || hooks.has(name);
    const isJones = owns(name, owned) || inventory.paths[name] === "jones-owned";
    const contract =
      isJones && name.startsWith("packages/contracts/src/") && !/\.(?:test|cases)\./.test(name);
    if (!isUpstream && !contract) continue;
    scannedFiles++;
    function inspectTarget(target: string, reference: ImportReference) {
      if (
        isUpstream &&
        (owns(target, owned) || inventory.paths[target] === "jones-owned") &&
        !allowed.has(`${name}\0${target}`)
      )
        violations.push({ code: "unlisted-upstream-import", path: name, detail: target });
      if (contract && !reference.typeOnly && !target.startsWith("packages/contracts/src/"))
        violations.push({ code: "contract-runtime-import", path: name, detail: target });
    }
    for (const reference of imports(name, text)) {
      const resolved: { target?: string; internal: boolean } =
        reference.specifier === undefined
          ? { internal: true }
          : resolveImport(name, reference.specifier, tree, aliases);
      if (!resolved.target && resolved.internal) {
        const disposition = dispositions.get(`${name}\0${reference.expression}`);
        if (!disposition || contract) {
          violations.push({
            code: "unresolved-import",
            path: name,
            detail:
              reference.specifier ??
              "Computed dynamic import/require cannot be verified at this boundary",
          });
        } else {
          // An explicit unsupported loader is bound to all source bytes; this is a visible coverage disposition, not general expression evaluation.
          acceptedUnsupported.push({
            path: name,
            expression: reference.expression,
            reason: disposition.reason,
            targets: disposition.targets,
          });
          for (const target of disposition.targets) {
            if (!tree.has(target))
              violations.push({
                code: "unresolved-import",
                path: name,
                detail: `Disposition target missing: ${target}`,
              });
            else inspectTarget(target, reference);
          }
        }
        continue;
      }
      if (resolved.target) inspectTarget(resolved.target, reference);
      if (
        contract &&
        !reference.typeOnly &&
        reference.specifier &&
        (reference.specifier.startsWith("node:") ||
          /^@t3tools\/(?!contracts(?:\/|$))/.test(reference.specifier))
      )
        violations.push({
          code: "contract-runtime-import",
          path: name,
          detail: reference.specifier,
        });
    }
  }
  return {
    classified: Object.fromEntries([...classified].sort(([a], [b]) => a.localeCompare(b))),
    violations,
    acceptedUnsupported,
    scannedFiles,
    coverage:
      "TypeScript AST: import/export, import-equals, import types, literal dynamic import and bare require. Computed calls fail conservatively unless exact source/anchor hashes and expressions have visible unsupported-case dispositions; contracts cannot use these dispositions. Resolution covers relative files, Vite url/raw/inline/no-inline asset queries, app ~/ aliases, exact workspace and nested mobile package exports (types/import/default); other internal aliases fail conservatively. Does not follow eval, indirect loaders, wildcard exports, arbitrary tsconfig aliases or external package re-exports.",
  };
}
function git(root: string, args: string[]): string {
  return NodeChildProcess.execFileSync("git", ["-C", root, ...args], {
    encoding: "utf8",
    maxBuffer: 32 * 1024 * 1024,
  });
}
export function boundaryExit(strict: boolean, violations: readonly Violation[]): number {
  return strict && violations.length ? 1 : 0;
}
function regularPath(root: string, name: string): string {
  safePath(name);
  let full = root;
  for (const part of name.split("/")) {
    full = NodePath.join(full, part);
    if (NodeFS.lstatSync(full).isSymbolicLink())
      throw new Error(`Symlink source is unsupported: ${name}`);
  }
  if (!NodeFS.lstatSync(full).isFile()) throw new Error(`Source must be a regular file: ${name}`);
  return full;
}
function writeLine(value: string): void {
  process.stdout.write(`${value}\n`);
}
export function runCli(args: string[]): number {
  if (args.includes("--help")) {
    if (args.length !== 1) throw new Error("--help cannot be combined with options");
    writeLine(
      "Usage: node scripts/jones/check-boundary.ts [--strict] [--json]\nChecks the working tree against pinned source ancestry. Default is advisory (exit 0); strict violations exit 1; invalid inputs/incomplete reads exit 2. No revision or inventory overrides.",
    );
    return 0;
  }
  if (
    new Set(args).size !== args.length ||
    args.some((arg) => !["--strict", "--json"].includes(arg))
  )
    throw new Error("Unknown or duplicate option");
  const root = NodeURL.fileURLToPath(new URL("../../", import.meta.url));
  const startingHead = git(root, ["rev-parse", "HEAD"]).trim();
  const startingStatus = git(root, ["status", "--porcelain=v1", "-z"]);
  const inventory = parseInventory(
    JSON.parse(
      NodeFS.readFileSync(regularPath(root, "scripts/jones/boundary/inventory.json"), "utf8"),
    ),
  );
  git(root, ["merge-base", "--is-ancestor", inventory.upstream.commit, inventory.baseline]);
  git(root, ["merge-base", "--is-ancestor", inventory.baseline, "HEAD"]);
  const upstreamPaths = new Set(
    git(root, ["ls-tree", "-r", "--name-only", "-z", inventory.upstream.commit])
      .split("\0")
      .filter(Boolean),
  );
  let resolutionCases: ResolutionCase[] = [];
  try {
    resolutionCases = parseResolutionCases(
      JSON.parse(
        NodeFS.readFileSync(regularPath(root, "scripts/jones/boundary/resolutions.json"), "utf8"),
      ),
    );
  } catch (error) {
    if ((error as NodeJS.ErrnoException).code !== "ENOENT") throw error;
  }
  const requiredSnapshots = new Set(
    resolutionCases.flatMap((entry) => entry.anchors.map((anchor) => anchor.path)),
  );
  const files = git(root, ["ls-files", "-z", "--cached", "--others", "--exclude-standard"])
    .split("\0")
    .filter(Boolean);
  const tree = new Map<string, string>();
  for (const name of new Set(files)) {
    if (!/^(?:apps|packages|scripts)\//.test(name) && !requiredSnapshots.has(name)) continue;
    safePath(name);
    if (
      !sourcePattern.test(name) &&
      !/^(?:(?:apps|packages)\/[^/]+|apps\/mobile\/modules\/[^/]+)\/package\.json$/.test(name) &&
      !requiredSnapshots.has(name)
    )
      continue;
    const full = NodePath.join(root, name);
    let stat;
    try {
      stat = NodeFS.lstatSync(full);
    } catch (error) {
      if ((error as NodeJS.ErrnoException).code === "ENOENT") continue;
      throw error;
    }
    if (!stat.isFile()) throw new Error(`Source must be a regular file: ${name}`);
    tree.set(name, NodeFS.readFileSync(regularPath(root, name), "utf8"));
  }
  // Presence, including non-source tombstones, is independent of import scanning.
  for (const name of files) {
    if (tree.has(name)) continue;
    try {
      NodeFS.lstatSync(NodePath.join(root, name));
      tree.set(name, "");
    } catch (error) {
      if ((error as NodeJS.ErrnoException).code !== "ENOENT") throw error;
    }
  }
  const fragmentRoot = NodePath.join(root, "scripts/jones/boundary/fragments");
  let fragmentFiles: string[];
  try {
    fragmentFiles = NodeFS.readdirSync(fragmentRoot);
  } catch (error) {
    if ((error as NodeJS.ErrnoException).code !== "ENOENT") throw error;
    fragmentFiles = [];
  }
  const fragments = fragmentFiles
    .filter((name) => name.endsWith(".json"))
    .sort()
    .map((name) => {
      safePath(name);
      const full = regularPath(root, `scripts/jones/boundary/fragments/${name}`);
      if (!NodeFS.lstatSync(full).isFile())
        throw new Error(`Fragment must be a regular file: ${name}`);
      return parseFragment(JSON.parse(NodeFS.readFileSync(full, "utf8")));
    });
  const changed = new Set(
    [
      ...git(root, ["diff", "--name-only", "-z", inventory.upstream.commit, "--"]).split("\0"),
      ...git(root, ["ls-files", "--others", "--exclude-standard", "-z"]).split("\0"),
    ].filter(Boolean),
  );
  const result = checkBoundary(
    inventory,
    fragments,
    tree,
    upstreamPaths,
    [...changed],
    resolutionCases,
  );
  if (
    git(root, ["rev-parse", "HEAD"]).trim() !== startingHead ||
    git(root, ["status", "--porcelain=v1", "-z"]) !== startingStatus
  )
    throw new Error("Source identity/status changed during boundary scan; rerun on a stable tree");
  const report = {
    upstream: inventory.upstream,
    baseline: inventory.baseline,
    head: startingHead,
    surface: "working-tree",
    ...result,
  };
  if (args.includes("--json")) writeLine(JSON.stringify(report, null, 2));
  else {
    writeLine(
      `Classified ${Object.keys(result.classified).length} changed paths; scanned ${result.scannedFiles} source files; ${result.violations.length} violations; ${result.acceptedUnsupported.length} source-bound unsupported import dispositions. ${args.includes("--strict") ? "Strict" : "Advisory"} mode.`,
    );
    for (const violation of result.violations)
      writeLine(`${violation.code}: ${violation.path}: ${violation.detail}`);
    for (const disposition of result.acceptedUnsupported)
      writeLine(
        `accepted-unsupported: ${disposition.path}: ${disposition.expression}: ${disposition.reason}`,
      );
    writeLine(result.coverage);
  }
  return boundaryExit(args.includes("--strict"), result.violations);
}
if (
  process.argv[1] &&
  NodePath.resolve(process.argv[1]) === NodeURL.fileURLToPath(import.meta.url)
) {
  try {
    process.exitCode = runCli(process.argv.slice(2));
  } catch (error) {
    process.stderr.write(
      `Boundary check incomplete: ${error instanceof Error ? error.message : String(error)}\n`,
    );
    process.exitCode = 2;
  }
}
