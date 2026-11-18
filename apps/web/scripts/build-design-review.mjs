import * as NodeChildProcess from "node:child_process";
import * as NodeFSP from "node:fs/promises";
import * as NodePath from "node:path";
import * as NodeURL from "node:url";
import react from "@vitejs/plugin-react";
import tailwind from "@tailwindcss/vite";
import { build } from "vite";

const webRoot = NodeURL.fileURLToPath(new URL("../", import.meta.url));
const repository = NodePath.resolve(webRoot, "../..");
const maximumPageBytes = 25 * 1024 * 1024;
const viewportHeight = /(?<![\w.+/])(\d+(?:\.\d+)?|\.\d+)[sld]?vh(?![\w-])/gi;

function git(...args) {
  return NodeChildProcess.execFileSync("git", args, { cwd: repository, encoding: "utf8" }).trim();
}

async function outputPath(filename) {
  const destination = NodePath.resolve(filename);
  const target = NodePath.join(
    await NodeFSP.realpath(NodePath.dirname(destination)),
    NodePath.basename(destination),
  );
  const fromRepository = NodePath.relative(await NodeFSP.realpath(repository), target);
  if (!NodePath.isAbsolute(fromRepository) && !fromRepository.startsWith(`..${NodePath.sep}`)) {
    throw new Error("Write the preview into an owned directory outside the checkout.");
  }
  return target;
}

function previewHtml(script, css) {
  // Give viewport-relative constraints a fixed reference height. The article viewer
  // must measure its content without the preview's layout feeding back into that measurement.
  const styles = css.replace(viewportHeight, (_, value) => `calc(${value} * 8px)`);
  const embeddedStyles = styles.replace(/<\/style/gi, "<\\/style");
  const embeddedScript = script.replace(/<\/script/gi, "<\\/script");
  return `<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <meta http-equiv="Content-Security-Policy" content="default-src 'none'; script-src 'unsafe-inline'; style-src 'unsafe-inline'; font-src data:; img-src data:; base-uri 'none'; form-action 'none'">
  <title>Jones Code design review</title>
  <style>${embeddedStyles}</style>
  <style>
    html, body, #root {
      height: auto !important;
      min-height: 0 !important;
      overflow: visible !important;
      background: transparent !important;
    }
  </style>
</head>
<body>
  <div id="root"></div>
  <script>${embeddedScript}</script>
</body>
</html>
`;
}

const [name, filename, ...extraArgs] = process.argv.slice(2);
if (
  extraArgs.length ||
  !/^[a-z][a-z0-9-]{0,63}$/.test(name ?? "") ||
  !filename?.endsWith(".html")
) {
  throw new Error(
    "Usage: node apps/web/scripts/build-design-review.mjs <preview-name> <new-output.html>",
  );
}
const destination = await outputPath(filename);
const source = git("rev-parse", "HEAD");
if (git("status", "--porcelain", "--untracked-files=normal")) {
  throw new Error(
    "Build a clean, committed candidate so the preview can identify its exact source.",
  );
}
const entry = NodePath.join(webRoot, "src", "jones", "designReview", `${name}-entry.tsx`);
const built = await build({
  configFile: false,
  envFile: false,
  envPrefix: [],
  root: webRoot,
  plugins: [react(), tailwind()],
  resolve: { tsconfigPaths: true, dedupe: ["react", "react-dom"] },
  define: {
    "import.meta.env.APP_SOURCE_SHA": JSON.stringify(source),
    // The hosted renderer mode skips connection discovery on the page origin.
    "import.meta.env.VITE_HOSTED_APP_CHANNEL": JSON.stringify("latest"),
    "process.env.NODE_ENV": JSON.stringify("production"),
  },
  build: {
    write: false,
    cssCodeSplit: false,
    lib: { entry, name: "JonesDesignReview", formats: ["iife"] },
  },
});
const result = Array.isArray(built) && built.length === 1 ? built[0] : built;
if (!result || !("output" in result)) {
  throw new Error("The preview must build one self-contained renderer.");
}

const [script] = result.output.filter((file) => file.type === "chunk");
const [styles] = result.output.filter(
  (file) => file.type === "asset" && file.fileName.endsWith(".css"),
);
if (
  result.output.length !== 2 ||
  !script ||
  script.imports.length ||
  // Rolldown can report an inlined dynamic import as a reference to its own chunk.
  script.dynamicImports.some((name) => name !== script.fileName) ||
  !styles ||
  typeof styles.source !== "string"
) {
  throw new Error(
    "The preview must contain one bundled script and one stylesheet with no extra files.",
  );
}
const html = previewHtml(script.code, styles.source);
if (Buffer.byteLength(html) > maximumPageBytes) {
  throw new Error("The preview exceeds the Jones Code HTML attachment size limit.");
}
if (
  git("rev-parse", "HEAD") !== source ||
  git("status", "--porcelain", "--untracked-files=normal")
) {
  throw new Error("The source changed during the preview build.");
}
await NodeFSP.writeFile(destination, html, { flag: "wx" });
console.log(`Built ${destination} from ${source}`);
