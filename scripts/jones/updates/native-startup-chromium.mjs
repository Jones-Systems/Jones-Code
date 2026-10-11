import assert from "node:assert/strict";
import * as fs from "node:fs/promises";
import { createRequire } from "node:module";
import os from "node:os";
import path from "node:path";
import { fileURLToPath } from "node:url";
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

assert.equal(
  process.platform,
  "linux",
  "The synthetic Chromium fixture requires Linux/Xvfb; packaged macOS has a separate qualifier.",
);
const repository = fileURLToPath(new URL("../../../", import.meta.url));
const desktop = path.join(repository, "apps", "desktop");
const require = createRequire(path.join(desktop, "package.json"));
const expectedElectron = JSON.parse(await fs.readFile(path.join(desktop, "package.json"), "utf8"))
  .dependencies.electron;
assert.equal(require("electron/package.json").version, expectedElectron);
const ownership = new FixtureOwnership();
const temporary = await ownership.temporary(path.join(os.tmpdir(), "jones-chromium-startup-"));
const controller = new AbortController();
const cancel = () => controller.abort();
process.on("SIGINT", cancel);
process.on("SIGTERM", cancel);
try {
  const root = await fs.realpath(temporary);
  const app = path.join(root, "app");
  const home = path.join(root, "state");
  const profile = path.join(root, "profile");
  const bundle = path.join(app, "dist-electron");
  await fs.mkdir(bundle, { recursive: true });
  const pack = desktopConfig.pack.find((entry) => entry.entry.includes("src/boot.ts"));
  assert.ok(pack);
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
  await fs.writeFile(path.join(app, "package.json"), JSON.stringify(metadata));
  const active = await createNativeState(home, profile, metadata, ownership);
  const marker = path.join(root, "loaded-after-refusal");
  for (const name of ["compileCache.cjs", "main.cjs"])
    await fs.writeFile(
      path.join(bundle, name),
      `require('node:fs').writeFileSync(${JSON.stringify(marker)}, ${JSON.stringify(name)}); require('electron').app.exit(9);`,
    );
  // This fixture proves the production boot catch in a real Chromium process.
  // The Darwin selector is injected; default macOS profile routing is proved by
  // the separate unchanged-packaged-app qualifier, never by this fixture.
  await fs.writeFile(
    path.join(app, "entry.cjs"),
    `
const { app } = require('electron');
if (process.versions.electron !== ${JSON.stringify(expectedElectron)}) app.exit(8);
Object.defineProperty(process, 'platform', {value: 'darwin'});
const getPath = app.getPath.bind(app);
app.getPath = (name) => name === 'home' ? ${JSON.stringify(root)} : name === 'appData' ? ${JSON.stringify(path.join(root, "appData"))} : getPath(name);
require('./dist-electron/boot.cjs');
`,
  );
  const env = {
    ...cleanLaunchEnvironment(process.env, root),
    HOME: root,
    XDG_CONFIG_HOME: path.join(root, "config"),
    XDG_CACHE_HOME: path.join(root, "cache"),
    T3CODE_HOME: home,
    T3CODE_DESKTOP_USER_DATA_DIR: profile,
  };
  const launch = () =>
    runOwnedChild(require("electron"), ["--no-sandbox", app], { env, signal: controller.signal });
  const before = await snapshotTree(profile);
  assertStartupRefused(await launch(), /Unstarted native activation holds new writers/);
  const leases = leasePaths(active);
  for (const filename of leases) assert.ok((await fs.stat(filename)).isFile());
  await withExclusiveLeases(leases, async () => {
    assertStartupRefused(await launch(), /locked/);
  });
  await withExclusiveLeases(leases, async () => {});
  assert.deepEqual(await snapshotTree(profile), before);
  await assert.rejects(fs.stat(marker), { code: "ENOENT" });
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
