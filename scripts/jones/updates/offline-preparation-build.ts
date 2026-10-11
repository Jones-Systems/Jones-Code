// @effect-diagnostics-next-line nodeBuiltinImport:off -- This build adapter observes and produces exact native artifact bytes before an application runtime exists.
import * as NodeFS from "node:fs";
// @effect-diagnostics-next-line nodeBuiltinImport:off -- Build commands use exact checkout or owned scratch paths with bounded output and duration.
import * as NodeChildProcess from "node:child_process";
// @effect-diagnostics-next-line nodeBuiltinImport:off -- Native SHA-256 binds downloaded, emitted and signed bytes without re-encoding them.
import * as NodeCrypto from "node:crypto";
// @effect-diagnostics-next-line nodeBuiltinImport:off -- The build owns its checkout/output paths, never application state.
import * as NodePath from "node:path";
import * as NodeModule from "node:module";
import * as NodeUtil from "node:util";
// @effect-diagnostics-next-line nodeBuiltinImport:off -- The sole download origin is the pinned official Node release over verified TLS.
import * as NodeHTTPS from "node:https";
import * as Schema from "effect/Schema";
import ts from "typescript-legacy";
import type { Plugin } from "vite-plus";

const encode = Schema.encodeSync(Schema.fromJsonString(Schema.Unknown));
const decode = Schema.decodeUnknownSync(Schema.fromJsonString(Schema.Unknown));
const nodeVersion = "26.8.2";
const archiveName = `node-v${nodeVersion}-darwin-arm64.tar.gz`;
const releaseOrigin = `https://nodejs.org/download/release/v${nodeVersion}/`;
const entry = "src/jones/updates/offlinePreparationEntry.ts";
const bundleName = "offlinePreparationEntry.mjs";
const executableName = "jones-offline-preparation";
const outRelative = "apps/server/dist-exe/preparation";
const releaseManifests = [
  "apps/server/package.json",
  "apps/desktop/package.json",
  "apps/web/package.json",
  "packages/contracts/package.json",
];
const sourceFiles = [
  "pnpm-lock.yaml",
  "pnpm-workspace.yaml",
  "package.json",
  ...releaseManifests,
  "apps/server/vite.config.ts",
  "vite.config.ts",
  "scripts/jones/updates/offline-preparation-build.ts",
  `apps/server/${entry}`,
  "apps/server/src/jones/updates/offlinePreparation.ts",
];
const maxBundleBytes = 32 * 1024 * 1024;
const maxRuntimeBytes = 256 * 1024 * 1024;
const forbiddenNames = new Set([
  "require",
  "createRequire",
  "eval",
  "Function",
  "dlopen",
  "getBuiltinModule",
  "binding",
  "_linkedBinding",
  "withSyntheticPreparationFixture",
  "initializeFixture",
]);
const forbiddenBuiltins = new Set([
  "module",
  "vm",
  "ffi",
  "worker_threads",
  "child_process",
  "process",
]);

const digest = (bytes: string | Uint8Array) =>
  NodeCrypto.createHash("sha256").update(bytes).digest("hex");

/** A closed SEA has no filesystem module resolver, including computed imports. */
export function verifyPreparationClosure(source: string): readonly string[] {
  if (Buffer.byteLength(source) > maxBundleBytes)
    throw new Error("Preparation bundle exceeds bound.");
  const file = ts.createSourceFile(
    bundleName,
    source,
    ts.ScriptTarget.Latest,
    true,
    ts.ScriptKind.JS,
  );
  const diagnostics = (file as ts.SourceFile & { parseDiagnostics: readonly ts.Diagnostic[] })
    .parseDiagnostics;
  if (diagnostics.length) throw new Error("Preparation bundle is not valid JavaScript.");
  const imports = new Set<string>();
  const inspectSpecifier = (node: ts.Node | undefined) => {
    if (!node || !ts.isStringLiteralLike(node)) throw new Error("Computed preparation loader.");
    const id = node.text;
    if (!NodeModule.isBuiltin(id) || forbiddenBuiltins.has(id.replace(/^node:/, "")))
      throw new Error(`Forbidden preparation import: ${id}`);
    imports.add(id);
  };
  const visit = (node: ts.Node): void => {
    if (ts.isImportDeclaration(node) || ts.isExportDeclaration(node)) {
      if (node.moduleSpecifier) inspectSpecifier(node.moduleSpecifier);
    }
    if (ts.isCallExpression(node) && node.expression.kind === ts.SyntaxKind.ImportKeyword) {
      if (node.arguments.length !== 1)
        throw new Error("Preparation import attributes are not admitted.");
      inspectSpecifier(node.arguments[0]);
    }
    if (
      (ts.isIdentifier(node) || ts.isStringLiteralLike(node)) &&
      (forbiddenNames.has(node.text) || /(?:^|_)require(?:_|$)/.test(node.text))
    )
      throw new Error(`Forbidden preparation loader or initializer: ${node.text}`);
    if (ts.isIdentifier(node) && ["process", "globalThis", "module"].includes(node.text)) {
      const parent = node.parent;
      if (
        !ts.isPropertyAccessExpression(parent) ||
        parent.expression !== node ||
        node.text !== "process" ||
        !["stdout", "argv", "exitCode"].includes(parent.name.text)
      )
        throw new Error("Unadmitted preparation runtime capability.");
    }
    if (ts.isStringLiteralLike(node) && /\bCREATE\s+TABLE\b/i.test(node.text))
      throw new Error("Synthetic or migration initializer survived preparation bundling.");
    ts.forEachChild(node, visit);
  };
  visit(file);
  return [...imports].sort();
}

export function preparationSeaConfig() {
  return {
    main: bundleName,
    mainFormat: "module",
    executable: "node",
    output: executableName,
    disableExperimentalSEAWarning: true,
    useSnapshot: false,
    useCodeCache: false,
    execArgv: [],
    execArgvExtension: "none",
  } as const;
}

export function verifyPreparationSeaConfig(value: unknown): void {
  if (encode(value) !== encode(preparationSeaConfig()))
    throw new Error("Preparation SEA configuration differs from the fixed build contract.");
}

export function verifyNodeArchive(checksums: string, bytes: Uint8Array): string {
  const matches = checksums.split(/\r?\n/).filter((line) => line.endsWith(`  ${archiveName}`));
  if (
    matches.length !== 1 ||
    matches[0]!.length !== 66 + archiveName.length ||
    !/^[a-f0-9]{64}  /.test(matches[0]!)
  )
    throw new Error("Missing or ambiguous official Node checksum.");
  const sha256 = digest(bytes);
  if (matches[0]!.slice(0, 64) !== sha256)
    throw new Error("Official Node archive checksum mismatch.");
  return sha256;
}

function command(
  file: string,
  args: readonly string[],
  cwd: string,
  maxBuffer = 1024 * 1024,
): Buffer {
  return NodeChildProcess.execFileSync(file, [...args], {
    cwd,
    maxBuffer,
    timeout: 120_000,
    killSignal: "SIGKILL",
    env: {
      PATH: "/usr/bin:/bin",
      HOME: cwd,
      TMPDIR: cwd,
      LC_ALL: "C",
      GIT_CONFIG_NOSYSTEM: "1",
      GIT_CONFIG_GLOBAL: "/dev/null",
    },
    stdio: ["ignore", "pipe", "pipe"],
  });
}

function readFile(file: string, limit: number): Buffer {
  const fd = NodeFS.openSync(file, NodeFS.constants.O_RDONLY | NodeFS.constants.O_NOFOLLOW);
  try {
    const before = NodeFS.fstatSync(fd);
    if (!before.isFile() || before.size > limit || before.nlink !== 1)
      throw new Error(`Unbounded or non-regular build member: ${NodePath.basename(file)}`);
    const buffer = Buffer.alloc(before.size + 1);
    let offset = 0;
    while (offset < buffer.length) {
      const count = NodeFS.readSync(fd, buffer, offset, buffer.length - offset, null);
      if (count === 0) break;
      offset += count;
    }
    const bytes = buffer.subarray(0, offset);
    const after = NodeFS.fstatSync(fd);
    if (
      bytes.length !== before.size ||
      after.size !== before.size ||
      after.mtimeMs !== before.mtimeMs
    )
      throw new Error("Build member changed while hashing.");
    return bytes;
  } finally {
    NodeFS.closeSync(fd);
  }
}

/** Only the existing release-stamp fields may differ from the committed source. */
export function verifyPreparationSourceOverlay(
  file: string,
  original: unknown,
  actual: unknown,
  identity: { sha: string; tree: string },
): string {
  if (!releaseManifests.includes(file)) throw new Error("Unadmitted source overlay.");
  const record = Schema.decodeUnknownSync(Schema.Record(Schema.String, Schema.Unknown));
  const before = { ...record(original) };
  const after = { ...record(actual) };
  const version = Schema.decodeUnknownSync(Schema.NonEmptyString)(after.version);
  if (["apps/server/package.json", "apps/desktop/package.json"].includes(file)) {
    if (
      after.jonesSource !== undefined &&
      !NodeUtil.isDeepStrictEqual(after.jonesSource, {
        repository: "Jones-Systems/Jones-Code",
        ...identity,
      })
    )
      throw new Error("Stamped source identity differs from observed Git source.");
    if (after.startupGateProtocol !== undefined && after.startupGateProtocol !== 1)
      throw new Error("Unadmitted startup metadata.");
    delete before.jonesSource;
    delete after.jonesSource;
    delete before.startupGateProtocol;
    delete after.startupGateProtocol;
  }
  delete before.version;
  delete after.version;
  if (!NodeUtil.isDeepStrictEqual(before, after))
    throw new Error("Preparation source has changes beyond release stamping.");
  return version;
}

function observeSource(root: string) {
  const git = (...args: string[]) => command("/usr/bin/git", args, root).toString("utf8").trim();
  if (
    NodeFS.realpathSync(git("rev-parse", "--show-toplevel")) !== root ||
    git("ls-files", "--others", "--exclude-standard") !== ""
  )
    throw new Error(
      "Preparation build requires an exact committed checkout without untracked source.",
    );
  const sha = git("rev-parse", "HEAD");
  const tree = git("rev-parse", "HEAD^{tree}");
  if (![sha, tree].every((value) => /^[a-f0-9]{40}$/.test(value)))
    throw new Error("Invalid source identity.");
  const changed = git("diff", "--name-only", "--no-renames", "HEAD", "--")
    .split("\n")
    .filter(Boolean);
  if (changed.some((file) => !releaseManifests.includes(file)))
    throw new Error("Preparation build source changed outside release stamping.");
  const versions = releaseManifests.map((file) =>
    verifyPreparationSourceOverlay(
      file,
      decode(git("show", `HEAD:${file}`)),
      decode(readFile(NodePath.join(root, file), 1024 * 1024).toString("utf8")),
      { sha, tree },
    ),
  );
  if (new Set(versions).size !== 1) throw new Error("Release manifest versions differ.");
  const hashes = Object.fromEntries(
    sourceFiles.map((name) => [name, digest(readFile(NodePath.join(root, name), 8 * 1024 * 1024))]),
  );
  return {
    repository: "Jones-Systems/Jones-Code",
    sha,
    tree,
    version: versions[0]!,
    hashes,
    buildOnlyOverlays: changed,
  };
}

function download(name: string, limit: number): Promise<Buffer> {
  if (name !== archiveName && name !== "SHASUMS256.txt")
    throw new Error("Unapproved Node download member.");
  return new Promise((resolve, reject) => {
    const request = NodeHTTPS.get(
      new URL(name, releaseOrigin),
      { timeout: 60_000, signal: AbortSignal.timeout(60_000) },
      (response) => {
        if (response.statusCode !== 200) {
          response.destroy();
          reject(new Error("Official Node download refused."));
          return;
        }
        const chunks: Buffer[] = [];
        let size = 0;
        response.on("data", (chunk: Buffer) => {
          size += chunk.length;
          if (size > limit) {
            response.destroy(new Error("Official Node download exceeds bound."));
            return;
          }
          chunks.push(chunk);
        });
        response.on("error", reject);
        response.on("end", () => resolve(Buffer.concat(chunks)));
      },
    );
    request.on("timeout", () => request.destroy(new Error("Official Node download timed out.")));
    request.on("error", reject);
  });
}

export function verifyPreparationHeldOutput(output: string): void {
  const status = Schema.decodeUnknownSync(
    Schema.Struct({
      status: Schema.Literal("held"),
      reason: Schema.Literal("maintenance-owner-binding-unqualified"),
      databaseOpened: Schema.Literal(false),
      productionInvocation: Schema.Literal(false),
      activation: Schema.Literal(false),
    }),
  )(decode(output));
  if (output !== `${encode(status)}\n`)
    throw new Error("Preparation entry output was not one exact held record.");
}

function verifyArgumentRefusal(executable: string, cwd: string): void {
  try {
    command(executable, ["--state", "/not-admitted", "--fixture"], cwd, 4096);
  } catch (cause) {
    if (typeof cause !== "object" || cause === null) throw cause;
    const error = cause as {
      status?: unknown;
      signal?: unknown;
      stdout?: unknown;
      stderr?: unknown;
    };
    if (
      error.status !== 64 ||
      error.signal !== null ||
      !Buffer.isBuffer(error.stdout) ||
      !Buffer.isBuffer(error.stderr) ||
      error.stderr.length !== 0
    )
      throw cause;
    verifyPreparationHeldOutput(error.stdout.toString("utf8"));
    return;
  }
  throw new Error("Preparation executable accepted application arguments.");
}

const HashRecord = Schema.Struct({
  sha256: Schema.String.check(Schema.isPattern(/^[a-f0-9]{64}$/)),
  size: Schema.Int.check(Schema.isGreaterThan(0)),
});
const ArtifactManifest = Schema.Struct({
  schemaVersion: Schema.Literal(1),
  observation: Schema.Literal("complete"),
  classification: Schema.Literal("held-entry-only"),
  preparationCapable: Schema.Literal(false),
  productionInvocation: Schema.Literal(false),
  source: Schema.Struct({
    repository: Schema.Literal("Jones-Systems/Jones-Code"),
    sha: Schema.String.check(Schema.isPattern(/^[a-f0-9]{40}$/)),
    tree: Schema.String.check(Schema.isPattern(/^[a-f0-9]{40}$/)),
    version: Schema.NonEmptyString,
    hashes: Schema.Record(Schema.String, Schema.String.check(Schema.isPattern(/^[a-f0-9]{64}$/))),
  }),
  entry: Schema.Literal(entry),
  dependencyInventory: Schema.Array(Schema.String),
  builtins: Schema.Array(Schema.String),
  bundle: HashRecord,
  seaConfig: Schema.Struct({ ...HashRecord.fields, value: Schema.Unknown }),
  upstream: Schema.Struct({
    archiveUrl: Schema.Literal(releaseOrigin + archiveName),
    checksumsUrl: Schema.Literal(releaseOrigin + "SHASUMS256.txt"),
    verification: Schema.Literal("official-https-checksum"),
    signedChecksumVerification: Schema.Literal(false),
    archive: HashRecord,
    checksumsSha256: Schema.String.check(Schema.isPattern(/^[a-f0-9]{64}$/)),
    runtime: Schema.Struct({
      ...HashRecord.fields,
      version: Schema.Literal(nodeVersion),
      platform: Schema.Literal("darwin"),
      arch: Schema.Literal("arm64"),
    }),
  }),
  executable: Schema.Struct({
    ...HashRecord.fields,
    name: Schema.Literal(executableName),
    signature: Schema.Literal("ad-hoc-verified"),
    heldOutputObserved: Schema.Literal(true),
    argumentRefusalObserved: Schema.Literal(true),
  }),
});

/** Re-observe final files; a JSON receipt alone is not artifact or execution authority. */
export function verifyPreparationArtifact(directory: string): void {
  verifyPreparationArtifactAt(directory, "provenance.json");
}

function verifyPreparationArtifactAt(
  directory: string,
  provenanceName: "provenance.json" | "provenance.complete.json",
): void {
  const manifest = Schema.decodeUnknownSync(ArtifactManifest)(
    decode(readFile(NodePath.join(directory, provenanceName), 4 * 1024 * 1024).toString("utf8")),
  );
  if (
    encode(Object.keys(manifest.source.hashes).sort()) !== encode([...sourceFiles].sort()) ||
    encode(manifest.dependencyInventory) !== encode([`apps/server/${entry}`]) ||
    manifest.builtins.length !== 0
  )
    throw new Error("Preparation provenance has incomplete source/dependency observations.");
  const inventory = [bundleName, executableName, "provenance.json", "sea-config.json"];
  if (provenanceName === "provenance.complete.json") inventory.push(provenanceName);
  if (encode(NodeFS.readdirSync(directory).sort()) !== encode(inventory.sort()))
    throw new Error("Unexpected final preparation inventory.");
  for (const [name, expected] of [
    [bundleName, manifest.bundle],
    ["sea-config.json", manifest.seaConfig],
    [executableName, manifest.executable],
  ] as const) {
    const bytes = readFile(NodePath.join(directory, name), maxRuntimeBytes);
    if (bytes.length !== expected.size || digest(bytes) !== expected.sha256)
      throw new Error(`Preparation artifact changed: ${name}`);
    if (
      name === bundleName &&
      encode(verifyPreparationClosure(bytes.toString("utf8"))) !== encode(manifest.builtins)
    )
      throw new Error("Recorded builtin inventory differs from the emitted bundle.");
    if (name === "sea-config.json") {
      verifyPreparationSeaConfig(decode(bytes.toString("utf8")));
      if (encode(decode(bytes.toString("utf8"))) !== encode(manifest.seaConfig.value))
        throw new Error("Recorded SEA config differs from bytes.");
    }
  }
}

/** Constructed only by the pack hook from its observed emitted graph. No caller-supplied provenance is admitted. */
export function offlinePreparationPack(rootPath: string) {
  const root = NodeFS.realpathSync(rootPath);
  const outDir = NodePath.join(root, outRelative);
  let source: ReturnType<typeof observeSource> | undefined;
  let emitted:
    | { sha256: string; size: number; builtins: readonly string[]; modules: readonly string[] }
    | undefined;
  const plugin: Plugin = {
    name: "jones:closed-offline-preparation",
    buildStart() {
      source = observeSource(root);
    },
    generateBundle(_options, bundle) {
      const outputs = Object.values(bundle);
      const chunk = outputs[0];
      if (
        outputs.length !== 1 ||
        !chunk ||
        chunk.type !== "chunk" ||
        chunk.fileName !== bundleName ||
        !chunk.isEntry ||
        chunk.dynamicImports.length ||
        chunk.map
      )
        throw new Error("Preparation requires exactly one entry and no chunks, maps or assets.");
      const builtins = verifyPreparationClosure(chunk.code);
      if (chunk.imports.some((id) => !NodeModule.isBuiltin(id)))
        throw new Error("Unbundled preparation dependency.");
      const modules = Object.entries(chunk.modules)
        .filter(([, info]) => info.renderedLength > 0)
        .map(([id]) => {
          const relative = NodePath.relative(root, id).replaceAll("\\", "/");
          if (
            relative.startsWith("../") ||
            NodePath.isAbsolute(relative) ||
            /(?:^|\/)(?:Migrations|providers?|fixtures?)(?:\/|$)|\.node$/.test(relative)
          )
            throw new Error(`Unadmitted preparation module: ${relative}`);
          return relative;
        })
        .sort();
      // This revision proves only the held entry. Broader preparation dependencies require a new reviewed inventory.
      if (encode(modules) !== encode([`apps/server/${entry}`]) || builtins.length !== 0)
        throw new Error("Unqualified dependency survived the held preparation entry build.");
      emitted = {
        sha256: digest(chunk.code),
        size: Buffer.byteLength(chunk.code),
        builtins,
        modules,
      };
    },
  };
  return {
    entry: { offlinePreparationEntry: entry },
    outDir,
    format: "esm" as const,
    platform: "node" as const,
    sourcemap: false,
    clean: true,
    dts: false,
    minify: false,
    outExtensions: () => ({ js: ".mjs" }),
    outputOptions: { codeSplitting: false },
    deps: {
      alwaysBundle: (id: string) => !NodeModule.isBuiltin(id),
      neverBundle: (id: string) => NodeModule.isBuiltin(id),
      onlyBundle: false,
    },
    plugins: [plugin],
    async onSuccess() {
      if (!source || !emitted)
        throw new Error("Missing observed preparation source or emitted graph.");
      if (encode(observeSource(root)) !== encode(source))
        throw new Error("Preparation source changed during bundling.");
      if (encode(NodeFS.readdirSync(outDir).sort()) !== encode([bundleName]))
        throw new Error("Unexpected preparation output inventory.");
      const bundle = readFile(NodePath.join(outDir, bundleName), maxBundleBytes);
      if (digest(bundle) !== emitted.sha256)
        throw new Error("Preparation bundle changed after closure verification.");
      // The native producer is intentionally host-specific; no host package installation or cross-runtime fallback.
      if (command("/usr/bin/uname", ["-sm"], root).toString("utf8").trim() !== "Darwin arm64")
        throw new Error("Preparation SEA production requires a native Darwin arm64 build host.");
      const provenancePath = NodePath.join(outDir, "provenance.json");
      NodeFS.writeFileSync(
        provenancePath,
        `${encode({ observation: "incomplete", classification: "unusable", productionInvocation: false })}\n`,
        { flag: "wx", mode: 0o600 },
      );
      const scratch = NodeFS.mkdtempSync(NodePath.join(outDir, ".build-"));
      let failure: unknown;
      let completed: { bytes: Buffer; configBytes: string; provenance: unknown } | undefined;
      try {
        const checksums = await download("SHASUMS256.txt", 128 * 1024);
        const archive = await download(archiveName, 128 * 1024 * 1024);
        const archiveHash = verifyNodeArchive(checksums.toString("utf8"), archive);
        const archivePath = NodePath.join(scratch, archiveName);
        NodeFS.writeFileSync(archivePath, archive, { flag: "wx", mode: 0o600 });
        const member = `node-v${nodeVersion}-darwin-arm64/bin/node`;
        if (
          command("/usr/bin/tar", ["-tzf", archivePath, member], scratch).toString("utf8") !==
          `${member}\n`
        )
          throw new Error("Official Node archive runtime member is not unique.");
        const runtime = command(
          "/usr/bin/tar",
          ["-xOzf", archivePath, member],
          scratch,
          maxRuntimeBytes,
        );
        const runtimePath = NodePath.join(scratch, "node");
        NodeFS.writeFileSync(runtimePath, runtime, { flag: "wx", mode: 0o500 });
        const runtimeHash = digest(readFile(runtimePath, maxRuntimeBytes));
        if (runtimeHash !== digest(runtime))
          throw new Error("Extracted Node runtime changed before observation.");
        if (
          command(runtimePath, ["--version"], scratch).toString("utf8").trim() !==
            `v${nodeVersion}` ||
          command("/usr/bin/lipo", ["-archs", runtimePath], scratch).toString("utf8").trim() !==
            "arm64"
        )
          throw new Error(
            "Observed Node runtime does not match the pinned version and architecture.",
          );
        NodeFS.writeFileSync(NodePath.join(scratch, bundleName), bundle, {
          flag: "wx",
          mode: 0o400,
        });
        const config = preparationSeaConfig();
        verifyPreparationSeaConfig(config);
        const configBytes = `${encode(config)}\n`;
        NodeFS.writeFileSync(NodePath.join(scratch, "sea-config.json"), configBytes, {
          flag: "wx",
          mode: 0o400,
        });
        // Node 26.8.2 documents direct SEA generation and module argv at:
        // https://nodejs.org/download/release/v26.8.2/docs/api/single-executable-applications.html
        command(runtimePath, ["--build-sea", "sea-config.json"], scratch);
        const executable = NodePath.join(scratch, executableName);
        command(
          "/usr/bin/codesign",
          ["--force", "--sign", "-", "--timestamp=none", executable],
          scratch,
        );
        command("/usr/bin/codesign", ["--verify", "--strict", executable], scratch);
        NodeFS.chmodSync(executable, 0o500);
        verifyPreparationHeldOutput(command(executable, [], scratch, 4096).toString("utf8"));
        verifyArgumentRefusal(executable, scratch);
        const finalBytes = readFile(executable, maxRuntimeBytes);
        if (digest(readFile(runtimePath, maxRuntimeBytes)) !== runtimeHash)
          throw new Error("Node build runtime changed during use.");
        if (encode(observeSource(root)) !== encode(source))
          throw new Error("Preparation source changed during native build.");
        const provenance = {
          schemaVersion: 1,
          classification: "held-entry-only",
          preparationCapable: false,
          productionInvocation: false,
          observation: "complete",
          source,
          entry,
          dependencyInventory: emitted.modules,
          builtins: emitted.builtins,
          bundle: { sha256: emitted.sha256, size: emitted.size },
          seaConfig: {
            value: config,
            sha256: digest(configBytes),
            size: Buffer.byteLength(configBytes),
          },
          upstream: {
            archiveUrl: releaseOrigin + archiveName,
            checksumsUrl: releaseOrigin + "SHASUMS256.txt",
            verification: "official-https-checksum",
            signedChecksumVerification: false,
            archive: { sha256: archiveHash, size: archive.length },
            checksumsSha256: digest(checksums),
            runtime: {
              version: nodeVersion,
              platform: "darwin",
              arch: "arm64",
              sha256: runtimeHash,
              size: runtime.length,
            },
          },
          executable: {
            name: executableName,
            sha256: digest(finalBytes),
            size: finalBytes.length,
            signature: "ad-hoc-verified",
            heldOutputObserved: true,
            argumentRefusalObserved: true,
          },
        };
        completed = { bytes: finalBytes, configBytes, provenance };
      } catch (cause) {
        failure = cause;
        throw cause;
      } finally {
        try {
          NodeFS.rmSync(scratch, { recursive: true, force: true });
        } catch (cleanup) {
          throw new AggregateError(
            failure === undefined ? [cleanup] : [failure, cleanup],
            `Preparation build cleanup failed: ${scratch}`,
          );
        }
      }
      if (!completed) throw new Error("Preparation build observation is incomplete.");
      NodeFS.writeFileSync(NodePath.join(outDir, executableName), completed.bytes, {
        flag: "wx",
        mode: 0o500,
      });
      NodeFS.writeFileSync(NodePath.join(outDir, "sea-config.json"), completed.configBytes, {
        flag: "wx",
        mode: 0o400,
      });
      // Failed cleanup or publication leaves the explicit unusable marker; it never becomes a complete receipt.
      const completePath = NodePath.join(outDir, "provenance.complete.json");
      NodeFS.writeFileSync(completePath, `${encode(completed.provenance)}\n`, {
        flag: "wx",
        mode: 0o400,
      });
      verifyPreparationArtifactAt(outDir, "provenance.complete.json");
      NodeFS.renameSync(completePath, provenancePath);
    },
  };
}
