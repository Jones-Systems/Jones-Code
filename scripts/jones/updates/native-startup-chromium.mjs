import * as NodeAssert from "node:assert/strict";
import * as NodeFSP from "node:fs/promises";
import * as NodeModule from "node:module";
import * as NodeOS from "node:os";
import * as NodePath from "node:path";
import * as NodeURL from "node:url";
import { build } from "vite-plus/pack";
import desktopConfig from "../../../apps/desktop/vite.config.ts";
import {
  assertStartupRefused,
  cleanLaunchEnvironment,
  createNativeState,
  FixtureOwnership,
  leasePaths,
  runOwnedChild,
  snapshotTree,
  withExclusiveLeases,
} from "./native-startup-fixture.mjs";

NodeAssert.equal(
  // oxlint-disable-next-line t3code/no-global-process-runtime -- This standalone Chromium qualifier must select the real native Linux host before launching Electron.
  process.platform,
  "linux",
  "The synthetic Chromium fixture requires Linux/Xvfb; packaged macOS has a separate qualifier.",
);
const repository = NodeURL.fileURLToPath(new URL("../../../", import.meta.url));
const desktop = NodePath.join(repository, "apps", "desktop");
const require = NodeModule.createRequire(NodePath.join(desktop, "package.json"));
const expectedElectron = JSON.parse(
  await NodeFSP.readFile(NodePath.join(desktop, "package.json"), "utf8"),
).dependencies.electron;
NodeAssert.equal(require("electron/package.json").version, expectedElectron);
const ownership = new FixtureOwnership();
const temporary = await ownership.temporary(
  NodePath.join(NodeOS.tmpdir(), "jones-chromium-startup-"),
);
const controller = new AbortController();
const cancel = () => controller.abort();
process.on("SIGINT", cancel);
process.on("SIGTERM", cancel);
try {
  const root = await NodeFSP.realpath(temporary);
  const app = NodePath.join(root, "app");
  const home = NodePath.join(root, "state");
  const profile = NodePath.join(root, "profile");
  const bundle = NodePath.join(app, "dist-electron");
  await NodeFSP.mkdir(bundle, { recursive: true });
  const pack = desktopConfig.pack.find((entry) => entry.entry.includes("src/boot.ts"));
  NodeAssert.ok(pack);
  await build({
    ...pack,
    config: false,
    cwd: desktop,
    outDir: bundle,
    sourcemap: false,
    onSuccess: undefined,
    logLevel: "silent",
  });
  const metadata = {
    name: "jones-startup-fixture",
    version: "1.0.0",
    main: "entry.cjs",
    jonesSource: {
      repository: "Jones-Systems/Jones-Code",
      sha: "a".repeat(40),
      tree: "b".repeat(40),
    },
  };
  await NodeFSP.writeFile(NodePath.join(app, "package.json"), JSON.stringify(metadata));
  const active = await createNativeState(home, profile, metadata, ownership);
  const marker = NodePath.join(root, "loaded-after-refusal");
  for (const name of ["compileCache.cjs", "main.cjs"])
    await NodeFSP.writeFile(
      NodePath.join(bundle, name),
      `require('node:fs').writeFileSync(${JSON.stringify(marker)}, ${JSON.stringify(name)}); require('electron').app.exit(9);`,
    );
  // This fixture proves the production boot catch in a real Chromium process.
  // The Darwin selector is injected; default macOS profile routing is proved by
  // the separate unchanged-packaged-app qualifier, never by this fixture.
  await NodeFSP.writeFile(
    NodePath.join(app, "entry.cjs"),
    `
const { app } = require('electron');
if (process.versions.electron !== ${JSON.stringify(expectedElectron)}) app.exit(8);
Object.defineProperty(process, 'platform', {value: 'darwin'});
const getPath = app.getPath.bind(app);
app.getPath = (name) => name === 'home' ? ${JSON.stringify(root)} : name === 'appData' ? ${JSON.stringify(NodePath.join(root, "appData"))} : getPath(name);
require('./dist-electron/boot.cjs');
`,
  );
  const env = {
    ...cleanLaunchEnvironment(process.env, root),
    HOME: root,
    XDG_CONFIG_HOME: NodePath.join(root, "config"),
    XDG_CACHE_HOME: NodePath.join(root, "cache"),
    T3CODE_HOME: home,
    T3CODE_DESKTOP_USER_DATA_DIR: profile,
  };
  const launch = () =>
    runOwnedChild(require("electron"), ["--no-sandbox", app], { env, signal: controller.signal });
  const before = await snapshotTree(profile);
  assertStartupRefused(await launch(), /Unstarted native activation holds new writers/);
  const leases = leasePaths(active);
  for (const filename of leases) NodeAssert.ok((await NodeFSP.stat(filename)).isFile());
  await withExclusiveLeases(leases, async () => {
    assertStartupRefused(await launch(), /locked/);
  });
  await withExclusiveLeases(leases, async () => {});
  NodeAssert.deepEqual(await snapshotTree(profile), before);
  await NodeAssert.rejects(NodeFSP.stat(marker), { code: "ENOENT" });
  process.stdout.write(
    JSON.stringify({
      qualification: "synthetic-chromium-startup-refusal",
      electron: expectedElectron,
      scenarios: ["unresolved-activation", "exclusive-contention"],
      leasesReleased: true,
      profileUnchanged: true,
      defaultMacProfileQualified: false,
    }) + "\n",
  );
} finally {
  process.removeListener("SIGINT", cancel);
  process.removeListener("SIGTERM", cancel);
  await ownership.cleanup();
}
