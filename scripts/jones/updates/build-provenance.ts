// @effect-diagnostics nodeBuiltinImport:off - Build jobs stamp their explicit checkout through native Node adapters.
import * as NodeChildProcess from "node:child_process";
import * as NodeFS from "node:fs";
import * as NodePath from "node:path";
import * as NodeOS from "node:os";
import * as Schema from "effect/Schema";

const SourceHash = Schema.String.check(Schema.isPattern(/^[a-f0-9]{40}$/));
export const JonesBuildSource = Schema.Struct({
  repository: Schema.Literal("Jones-Systems/Jones-Code"),
  sha: SourceHash,
  tree: SourceHash,
});
export type JonesBuildSource = typeof JonesBuildSource.Type;
export const decodeJonesBuildSource = Schema.decodeUnknownEffect(JonesBuildSource);
const decodeBuildSource = Schema.decodeUnknownSync(JonesBuildSource);

export const JonesDesktopBuildMetadata = Schema.Union([
  Schema.Struct({
    jonesSource: JonesBuildSource,
    startupGateProtocol: Schema.Literal(1),
  }),
  Schema.Struct({
    jonesSource: Schema.optionalKey(JonesBuildSource),
    startupGateProtocol: Schema.optionalKey(Schema.Never),
  }),
]);
export const decodeJonesDesktopBuildMetadata = Schema.decodeUnknownEffect(JonesDesktopBuildMetadata);

/** An unsigned preview remains a manual Darwin build, never a signed release qualification. */
export function bundlesJonesNativeHelper(
  platform: "mac" | "linux" | "win",
  version: string,
  signed: boolean,
): boolean {
  return platform === "mac" && !signed && /-preview\.\d{8}\.\d+(?:\.\d+)?$/.test(version);
}

/** Stamp the committed tree independently of build-only package version changes. Importing has no effects. */
export function stampJonesBuildSource(input: {
  root: string;
  workflowSha?: string | undefined;
  githubEnvFile?: string | undefined;
}): JonesBuildSource {
  const root = NodeFS.realpathSync(input.root);
  const gitEnv = {
    ...Object.fromEntries(Object.entries(process.env).filter(([name]) => !name.startsWith("GIT_"))),
    GIT_CONFIG_NOSYSTEM: "1",
    GIT_CONFIG_GLOBAL: NodeOS.devNull,
  };
  const git = (args: readonly string[]) =>
    NodeChildProcess.execFileSync("git", [...args], {
      cwd: root,
      env: gitEnv,
      encoding: "utf8",
      maxBuffer: 16384,
      timeout: 10000,
      stdio: ["ignore", "pipe", "pipe"],
    }).trim();
  if (NodeFS.realpathSync(git(["rev-parse", "--show-toplevel"])) !== root)
    throw new Error("Jones build stamping requires the exact checkout root.");
  const identity = decodeBuildSource({
    repository: "Jones-Systems/Jones-Code",
    sha: git(["rev-parse", "HEAD"]),
    tree: git(["rev-parse", "HEAD^{tree}"]),
  });
  if (input.workflowSha && input.workflowSha !== identity.sha)
    throw new Error("Checkout does not match workflow source.");
  const manifests = ["apps/server/package.json", "apps/desktop/package.json"].map((relative) => {
    const file = NodePath.join(root, relative);
    const manifest = JSON.parse(NodeFS.readFileSync(file, "utf8")) as Record<string, unknown>;
    return { file, manifest: { ...manifest, jonesSource: identity } };
  });
  for (const { file, manifest } of manifests)
    NodeFS.writeFileSync(file, JSON.stringify(manifest, null, 2) + "\n");
  if (input.githubEnvFile !== undefined)
    NodeFS.appendFileSync(
      input.githubEnvFile,
      `JONES_SOURCE_SHA=${identity.sha}\nJONES_SOURCE_TREE=${identity.tree}\n`,
    );
  return identity;
}
