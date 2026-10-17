import * as NodePath from "node:path";
import * as NodeURL from "node:url";
import { defineConfig, mergeConfig, type Plugin } from "vite-plus";
import webConfig from "./vite.config";

export default defineConfig(async (environment) => {
  if (
    process.env.JONES_INSTALL_RENDERER_FIXTURE !== "1" ||
    environment.command !== "build" ||
    environment.mode !== "jones-install-fixture"
  ) {
    throw new Error("The install renderer fixture requires its explicit isolated build context.");
  }
  const outDir = process.env.JONES_INSTALL_FIXTURE_OUT;
  const sourceSha = process.env.JONES_INSTALL_FIXTURE_SOURCE_SHA;
  const sourceTree = process.env.JONES_INSTALL_FIXTURE_SOURCE_TREE;
  const webRoot = NodeURL.fileURLToPath(new URL(".", import.meta.url));
  const repositoryRoot = NodePath.resolve(webRoot, "../..");
  if (!outDir || !NodePath.isAbsolute(outDir)) {
    throw new Error("JONES_INSTALL_FIXTURE_OUT must name an owned absolute scratch directory.");
  }
  const relative = NodePath.relative(repositoryRoot, outDir);
  if (relative === "" || (!relative.startsWith(`..${NodePath.sep}`) && relative !== "..")) {
    throw new Error("Fixture output must be outside the source checkout.");
  }
  if (!sourceSha || !/^[a-f0-9]{40}$/.test(sourceSha)) {
    throw new Error("JONES_INSTALL_FIXTURE_SOURCE_SHA must bind the checked-out source revision.");
  }
  if (!sourceTree || !/^[a-f0-9]{40}$/.test(sourceTree)) {
    throw new Error("JONES_INSTALL_FIXTURE_SOURCE_TREE must bind the checked-out source tree.");
  }
  const manifest: Plugin = {
    name: "jones:install-renderer-fixture-manifest",
    generateBundle() {
      this.emitFile({
        type: "asset",
        fileName: "jones-install-fixture-manifest.json",
        source: JSON.stringify(
          {
            schema: "jones-install-renderer-fixture/v1",
            sourceSha,
            sourceTree,
            entry: "jones-install-fixture.html",
            boundary:
              "Production renderer components with synthetic desktop bridge; no Electron IPC or native installation",
          },
          null,
          2,
        ),
      });
    },
  };
  const base = typeof webConfig === "function" ? await webConfig(environment) : await webConfig;
  return mergeConfig(base, {
    root: webRoot,
    plugins: [manifest],
    base: "./",
    define: {
      "import.meta.env.JONES_INSTALL_FIXTURE_SOURCE_SHA": JSON.stringify(sourceSha),
      "import.meta.env.VITE_HTTP_URL": JSON.stringify(""),
      "import.meta.env.VITE_WS_URL": JSON.stringify(""),
      "import.meta.env.VITE_CLERK_PUBLISHABLE_KEY": JSON.stringify(""),
      "import.meta.env.VITE_T3CODE_RELAY_URL": JSON.stringify(""),
      "import.meta.env.VITE_RELAY_OTLP_TRACES_URL": JSON.stringify(""),
      "import.meta.env.VITE_RELAY_OTLP_TRACES_TOKEN": JSON.stringify(""),
    },
    build: {
      outDir,
      emptyOutDir: false,
      manifest: true,
      rolldownOptions: {
        input: NodeURL.fileURLToPath(new URL("./jones-install-fixture.html", import.meta.url)),
      },
    },
  });
});
