import * as NodeChildProcess from "node:child_process";
import * as NodeFS from "node:fs";
import * as NodePath from "node:path";
import * as NodeURL from "node:url";
import * as NodeUtil from "node:util";

// oxlint-disable-next-line t3code/no-global-process-runtime -- The Swift compiler targets the actual host; this script has no Effect runtime.
const hostArch = process.arch;
// oxlint-disable-next-line t3code/no-global-process-runtime -- The Speech framework exists only in the macOS SDK.
const hostPlatform = process.platform;

function resolveCompilerEnvironment() {
  const environment = process.env;
  const hasSpeechSdk = (env) => {
    const probe = NodeChildProcess.spawnSync("xcrun", ["--sdk", "macosx", "--show-sdk-version"], {
      env,
      encoding: "utf8",
    });
    return probe.status === 0 && Number.parseInt(probe.stdout, 10) >= 26;
  };
  if (hasSpeechSdk(environment)) return environment;
  // Honor an explicit override. Some hosted images default to Xcode 16 while also shipping Xcode 26.
  if (environment.DEVELOPER_DIR) return undefined;
  const applications = "/Applications";
  if (!NodeFS.existsSync(applications)) return undefined;
  const installedXcodes = NodeFS.readdirSync(applications)
    .filter((name) => /^Xcode(?:[_.-].+)?\.app$/.test(name))
    .sort((left, right) => right.localeCompare(left, undefined, { numeric: true }));
  for (const app of installedXcodes) {
    const env = {
      ...environment,
      DEVELOPER_DIR: NodePath.join(applications, app, "Contents", "Developer"),
    };
    if (hasSpeechSdk(env)) return env;
  }
  return undefined;
}

const { values } = NodeUtil.parseArgs({
  options: { output: { type: "string" }, arch: { type: "string", default: hostArch } },
});
// Packaging passes --output and must not ship without the helper; dev builds only lose the option.
const required = values.output !== undefined;

if (hostPlatform !== "darwin") {
  if (required) throw new Error("The Apple Speech helper can only be built on macOS.");
} else {
  const targets = { arm64: ["arm64"], x64: ["x86_64"], universal: ["arm64", "x86_64"] }[
    values.arch
  ];
  if (targets === undefined) throw new Error(`Unsupported macOS architecture: ${values.arch}`);
  const root = NodeURL.fileURLToPath(new URL("../../../native/apple-speech/", import.meta.url));
  const sources = ["AppleSpeech.swift", "AppleSpeechEngine.swift"].map((file) =>
    NodePath.resolve(root, file),
  );
  const output = values.output ?? NodePath.resolve(root, "build", values.arch, "t3-apple-speech");
  let current = false;
  try {
    current =
      NodeFS.statSync(output).mtimeMs >=
      Math.max(
        ...sources.map((source) => NodeFS.statSync(source).mtimeMs),
        NodeFS.statSync(NodeURL.fileURLToPath(import.meta.url)).mtimeMs,
      );
  } catch {}
  if (!current) {
    const env = resolveCompilerEnvironment();
    if (!env) {
      const message =
        "Building the Apple Speech helper requires the macOS 26 SDK (Xcode 26 or later).";
      if (required) throw new Error(message);
      console.warn(`${message} On-device Mac dictation is unavailable in this build.`);
      process.exit(0);
    }
    NodeFS.mkdirSync(NodePath.dirname(output), { recursive: true });
    const temporary = `${output}.${process.pid}.tmp`;
    const slices = targets.map((target) => `${temporary}.${target}`);
    try {
      targets.forEach((target, index) =>
        NodeChildProcess.execFileSync(
          "xcrun",
          [
            "--sdk",
            "macosx",
            "swiftc",
            "-O",
            "-swift-version",
            "5",
            "-parse-as-library",
            "-target",
            `${target}-apple-macos26.0`,
            ...sources,
            "-o",
            slices[index],
          ],
          { env, stdio: "inherit" },
        ),
      );
      NodeChildProcess.execFileSync("xcrun", ["lipo", "-create", ...slices, "-output", temporary], {
        env,
        stdio: "inherit",
      });
      NodeFS.renameSync(temporary, output);
    } catch (cause) {
      if (required) throw new Error("Failed to compile the Apple Speech helper.", { cause });
      console.warn(
        "Skipping the Apple Speech helper after a compiler failure; on-device Mac dictation is unavailable in this build.",
      );
    } finally {
      for (const file of [temporary, ...slices]) NodeFS.rmSync(file, { force: true });
    }
  }
}
