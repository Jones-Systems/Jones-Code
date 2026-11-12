import * as NodeChildProcess from "node:child_process";
import * as NodeModule from "node:module";
import * as NodePath from "node:path";
import * as NodeURL from "node:url";

export function resolvePlaywright(workspace) {
  const packageName = { "apps/web": "playwright", "apps/server": "playwright-core" }[workspace];
  if (!packageName) throw new Error(`Unsupported Chromium workspace: ${workspace}`);
  const require = NodeModule.createRequire(NodePath.resolve(workspace, "package.json"));
  const manifest = require.resolve(`${packageName}/package.json`);
  return {
    version: require(manifest).version,
    cli: NodePath.join(NodePath.dirname(manifest), "cli.js"),
    chromium: require(packageName).chromium,
  };
}

export async function ensureChromium({ chromium, installDependencies, report = console.error }) {
  const smoke = async () => {
    // Default headless launch selects the shell installed by --only-shell;
    // executablePath() can instead identify the uninstalled full browser.
    const browser = await chromium.launch({ headless: true, timeout: 30_000 });
    await browser.close();
  };
  try {
    await smoke();
  } catch (error) {
    report(error);
    await installDependencies();
    await smoke();
  }
}

if (
  process.argv[1] &&
  import.meta.url === NodeURL.pathToFileURL(NodePath.resolve(process.argv[1])).href
) {
  const [workspace, mode] = process.argv.slice(2);
  const playwright = resolvePlaywright(workspace);
  if (mode === "version") {
    console.log(playwright.version);
  } else if (mode === undefined) {
    await ensureChromium({
      chromium: playwright.chromium,
      installDependencies() {
        const result = NodeChildProcess.spawnSync(
          process.execPath,
          [playwright.cli, "install-deps", "chromium"],
          {
            stdio: "inherit",
          },
        );
        if (result.error) throw result.error;
        if (result.status !== 0) {
          throw new Error(
            `Chromium dependency installation failed (${result.signal ?? result.status})`,
          );
        }
      },
    });
  } else {
    throw new Error(`Unsupported Chromium setup mode: ${mode}`);
  }
}
