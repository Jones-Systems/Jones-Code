// @effect-diagnostics nodeBuiltinImport:off -- Read-only analyzer boundary; no application modules or processes execute.
import * as NodeCrypto from "node:crypto";
import * as NodeFSP from "node:fs/promises";
import * as NodePath from "node:path";
import * as NodeProcess from "node:process";
import type { Preprocessor } from "knip";
import ts from "typescript-legacy";
import * as Identity from "../tsrs/identity.ts";
import { sourcePins, nativeImports } from "./foreignDeclarations.ts";

type Options = Parameters<Preprocessor>[0];
type Group = keyof typeof sourcePins;
interface ForeignOptions {
  readonly cwd: string;
  readonly issues: Pick<Options["issues"], "unlisted" | "binaries">;
  readonly counters: Pick<Options["counters"], "unlisted" | "binaries">;
}
interface Declaration {
  readonly group: Group;
  readonly category: "unlisted" | "binaries";
  readonly file: string;
  readonly symbol: string;
  readonly contract: string;
}
const declarations: readonly Declaration[] = [
  {
    group: "gjs",
    category: "unlisted",
    file: "apps/desktop/gnome-extension/captureFeedback.js",
    symbol: "gi",
    contract: "GJS introspection imports",
  },
  {
    group: "gjs",
    category: "unlisted",
    file: "apps/desktop/gnome-extension/captureFeedback.js",
    symbol: "resource",
    contract: "GNOME Shell resource imports",
  },
  {
    group: "gjs",
    category: "unlisted",
    file: "apps/desktop/gnome-extension/extension.js",
    symbol: "gi",
    contract: "GJS introspection imports",
  },
  {
    group: "gjs",
    category: "unlisted",
    file: "apps/desktop/gnome-extension/extension.js",
    symbol: "resource",
    contract: "GNOME Shell resource imports",
  },
  {
    group: "pgrep",
    category: "binaries",
    file: "apps/desktop/scripts/electron-launcher.test.mjs",
    symbol: "pgrep",
    contract: "captured-child native ERE fixture",
  },
  {
    group: "hyprctl",
    category: "binaries",
    file: "apps/desktop/src/app/DesktopPreReadyPlatform.ts",
    symbol: "hyprctl",
    contract: "bounded optional session version probe",
  },
  {
    group: "compiler",
    category: "unlisted",
    file: "apps/web/tsconfig.json",
    symbol: "@effect/language-service",
    contract: "verified native compiler configuration namespace",
  },
];
const equal = (actual: unknown, expected: unknown, label: string) => {
  if (JSON.stringify(actual) !== JSON.stringify(expected))
    throw new Error(`Foreign contract mismatch: ${label}`);
};
const hash = (source: string) => NodeCrypto.createHash("sha256").update(source).digest("hex");
const slash = (path: string) => path.split(NodePath.sep).join("/");

/** Pure validation port for negative fixtures; production always supplies actual read-only identity verification below. */
export async function inspectContracts(
  groups: readonly Group[],
  read: (file: string) => Promise<string>,
  verifyCompiler: () => Promise<{ readonly target: string; readonly artifactSHA256: string }>,
) {
  const sources: Array<{ file: string; sha256: string }> = [];
  for (const group of new Set(groups))
    for (const [file, expected] of Object.entries(sourcePins[group])) {
      const actual = hash(await read(file));
      equal(actual, expected, file);
      sources.push({ file, sha256: actual });
    }
  if (groups.includes("gjs")) {
    const base = "apps/desktop/gnome-extension/";
    const files = [
      "metadata.json",
      "extension.js",
      "captureService.js",
      "captureFeedback.js",
      "feedbackGeometry.js",
    ];
    const bundle: unknown = JSON.parse(await read(base + "bundle.json"));
    equal(bundle, { uuid: "snap-shot@t3.codes", files }, "packaged graph");
    const seen = new Set<string>();
    const visit = async (name: string): Promise<void> => {
      if (seen.has(name)) return;
      seen.add(name);
      if (!files.includes(name) || NodePath.posix.basename(name) !== name || !name.endsWith(".js"))
        throw new Error("Unpackaged GJS dependency");
      const source = ts.createSourceFile(
        name,
        await read(base + name),
        ts.ScriptTarget.Latest,
        true,
        ts.ScriptKind.JS,
      );
      // Full graph bytes are pinned as well: all import-clause combinations and non-import forms remain change guarded.
      const imports: string[] = [];
      for (const statement of source.statements) {
        if (ts.isExportDeclaration(statement) && statement.moduleSpecifier)
          throw new Error("Undeclared GJS export-from");
        if (!ts.isImportDeclaration(statement)) continue;
        if (!ts.isStringLiteral(statement.moduleSpecifier))
          throw new Error("Nonliteral GJS import");
        const uri = statement.moduleSpecifier.text;
        if (uri.startsWith("./")) {
          await visit(uri.slice(2));
          continue;
        }
        const clause = statement.importClause;
        const bindings: string[] = [];
        if (clause?.name) bindings.push(clause.name.text);
        if (clause?.namedBindings) {
          const value = clause.namedBindings;
          bindings.push(
            ts.isNamespaceImport(value)
              ? "*" + value.name.text
              : "{" + value.elements.map((element) => element.getText(source)).join(",") + "}",
          );
        }
        if (bindings.length !== 1 || (!uri.startsWith("gi://") && !uri.startsWith("resource:///")))
          throw new Error("Undeclared GJS loader/clause");
        imports.push(bindings[0] + "=" + uri);
      }
      const scan = (node: ts.Node): void => {
        if (
          ts.isCallExpression(node) &&
          (node.expression.kind === ts.SyntaxKind.ImportKeyword ||
            node.expression.getText(source) === "require")
        )
          throw new Error("Undeclared dynamic GJS loader");
        ts.forEachChild(node, scan);
      };
      scan(source);
      const expected =
        name === "extension.js"
          ? nativeImports["extension.js"]
          : name === "captureFeedback.js"
            ? nativeImports["captureFeedback.js"]
            : [];
      equal(imports, expected, "full native URI/binding " + name);
    };
    await visit("extension.js");
    equal([...seen].sort(), files.filter((file) => file.endsWith(".js")).sort(), "complete graph");
  }
  let compiler: { readonly target: string; readonly artifactSHA256: string } | undefined;
  if (groups.includes("compiler")) {
    compiler = await verifyCompiler();
    if (!compiler.target || !/^[a-f0-9]{64}$/.test(compiler.artifactSHA256))
      throw new Error("Missing verified native compiler identity");
  }
  return { sources, ...(compiler ? { compiler } : {}) };
}

/** All owning proofs and metadata are checked before this function mutates any Knip finding. */
export default async function checkedForeignBindings<T extends ForeignOptions>(
  options: T,
): Promise<T> {
  const matches: Array<{
    declaration: Declaration;
    key: string;
    issueKey: string;
    issue: Options["issues"]["unlisted"][string][string];
  }> = [];
  for (const declaration of declarations) {
    for (const [key, issues] of Object.entries(options.issues[declaration.category])) {
      if (
        slash(NodePath.relative(options.cwd, NodePath.resolve(options.cwd, key))) !==
        declaration.file
      )
        continue;
      for (const [issueKey, issue] of Object.entries(issues)) {
        if (issue.symbol !== declaration.symbol) continue;
        const expected = NodePath.resolve(options.cwd, declaration.file);
        const workspace = declaration.file.startsWith("apps/desktop/")
          ? "apps/desktop"
          : "apps/web";
        if (
          key !== declaration.file ||
          issueKey !== declaration.symbol ||
          issue.type !== declaration.category ||
          issue.workspace !== workspace ||
          issue.filePath !== expected ||
          issue.symbols !== undefined ||
          issue.parentSymbol !== undefined ||
          issue.isFixed ||
          issue.fixes.length !== 0 ||
          issue.specifier !== declaration.symbol
        )
          throw new Error(
            `Malformed foreign issue metadata: ${declaration.file}:${declaration.symbol}`,
          );
        if (
          (await NodeFSP.realpath(expected)) !== expected ||
          (await NodeFSP.realpath(NodePath.resolve(options.cwd, workspace))) !==
            NodePath.resolve(options.cwd, workspace)
        )
          throw new Error("Aliased foreign containing file/workspace");
        matches.push({ declaration, key, issueKey, issue });
      }
    }
  }
  if (matches.length === 0) return options;
  const root = NodePath.resolve(options.cwd);
  if ((await NodeFSP.realpath(root)) !== root) throw new Error("Aliased analyzer root");
  const read = async (file: string) => {
    const target = NodePath.resolve(root, file);
    if (!target.startsWith(root + NodePath.sep) || (await NodeFSP.realpath(target)) !== target)
      throw new Error("Foreign proof file escaped canonical root");
    return NodeFSP.readFile(target, "utf8");
  };
  const proof = await inspectContracts(
    matches.map(({ declaration }) => declaration.group),
    read,
    async () => {
      const target = Identity.hostTarget();
      if (!target)
        throw new Error(
          "Foreign compiler namespace needs supported official TSRS; fallback cannot qualify",
        );
      await Identity.verified(await Identity.resolveInstalled(root, target));
      return { target, artifactSHA256: Identity.TARGETS[target].patched };
    },
  );
  for (const category of ["unlisted", "binaries"] as const) {
    const count = Object.values(options.issues[category]).reduce(
      (total, issues) => total + Object.keys(issues).length,
      0,
    );
    if (!Number.isSafeInteger(options.counters[category]) || options.counters[category] !== count)
      throw new Error("Foreign issue counter mismatch");
  }
  for (const match of matches)
    if (options.issues[match.declaration.category][match.key]?.[match.issueKey] !== match.issue)
      throw new Error("Foreign findings changed during validation");
  for (const { declaration, key, issueKey } of matches) {
    const issues = options.issues[declaration.category][key]!;
    delete issues[issueKey];
    options.counters[declaration.category]--;
    if (Object.keys(issues).length === 0) delete options.issues[declaration.category][key];
  }
  NodeProcess.stderr.write(
    JSON.stringify({
      schema: "jones.checked-foreign-declarations/v1",
      classified: matches.map(({ declaration }) => declaration),
      ...proof,
      limits: [
        "checked declared foreign contracts, not npm implementations",
        "no GNOME/Hyprland native API or hardware execution proof",
      ],
    }) + "\n",
  );
  return options;
}
