import assert from "node:assert/strict";
import { execFileSync, spawnSync } from "node:child_process";
import { createHash } from "node:crypto";
import { readFileSync } from "node:fs";
import { runInNewContext } from "node:vm";
import { test } from "node:test";
import { jonesNativeHelperSource } from "../../../apps/desktop/src/jones/updates/jonesNativeHelperSource.ts";
import { jonesPairedStateSource } from "../../../apps/desktop/src/jones/updates/jonesPairedStateSource.ts";
import {
  componentIdentifier,
  desktopAppId,
  nativePasskeysEnabled,
  requireProductionBuild,
  signingRequirement,
  JONES_MACOS_APP_ID,
} from "./policy.ts";

let helper = jonesNativeHelperSource;
if (process.env.JONES_SIGNING_TEST_REVISION) {
  const source = execFileSync(
    "git",
    [
      "show",
      `${process.env.JONES_SIGNING_TEST_REVISION}:apps/desktop/src/jones/updates/jonesNativeHelperSource.ts`,
    ],
    { encoding: "utf8" },
  );
  helper = source
    .split("String.raw`")[1]
    .split("`;")[0]
    .replace(/\$\{jonesPairedStateSource\.(\w+)\}/g, (_, key) => jonesPairedStateSource[key]);
}
const definitions = helper.split("\nparser = argparse.ArgumentParser()")[0];
console.log(`native helper SHA256 ${createHash("sha256").update(helper).digest("hex")}`);
for (const fault of [
  "valid",
  "tampered",
  "wrong-team",
  "adhoc",
  "wrong-id",
  "not-notarized",
  "rollback",
  "rollback-tampered",
]) {
  test(`native transaction refuses ${fault} before rename`, () => {
    const result = spawnSync(
      "python3",
      [
        "-c",
        `${definitions}
import tempfile
from types import SimpleNamespace
fault = sys.argv[1]
production_id = 'com.jonessystems.jonescode'
with tempfile.TemporaryDirectory(prefix='jones-signing-oracle-') as temporary:
    root = pathlib.Path(temporary)
    active, incoming, previous = (root / name for name in ('active.app', 'incoming.app', 'previous.app'))
    rollback = fault.startswith('rollback')
    for app in ((active, previous) if rollback else (active, incoming)):
        (app / 'Contents').mkdir(parents=True)
        identifier = 'com.wrong.app' if fault == 'wrong-id' and app == incoming else production_id
        with open(app / 'Contents/Info.plist', 'wb') as stream: plistlib.dump({'CFBundleIdentifier': identifier}, stream)
        (app / 'marker').write_text(app.name)
    expected = {'appPath': str(active), 'bundleIdentifier': production_id}
    calls = []
    def run(argv, **kwargs):
        calls.append(argv)
        if argv[0] == "/usr/bin/codesign":
            assert argv[argv.index("-R") + 1].startswith("="), "codesign -R treats unprefixed input as a file"
        target = previous if rollback else incoming
        failure = str(target) == argv[-1] and ((fault in ('tampered', 'wrong-team', 'adhoc', 'rollback-tampered') and argv[0] == '/usr/bin/codesign') or (fault == 'not-notarized' and 'stapler' in argv))
        return SimpleNamespace(returncode=1 if failure else 0)
    subprocess.run = run
    refused = False
    try:
        if rollback: restore_bundle(expected, incoming, previous)
        else: swap_bundle(expected, incoming, previous)
    except RuntimeError: refused = True
    if fault in ('valid', 'rollback'):
        assert not refused
        assert (active / 'marker').read_text() == ('previous.app' if rollback else 'incoming.app')
    else:
        assert refused, 'unsafe candidate reached bundle rename'
        assert (active / 'marker').read_text() == 'active.app'
    for argv in calls:
        if argv[0] == '/usr/bin/codesign':
            assert '--deep' in argv and '--strict' in argv
            assert '2CUPSQS986' in argv[argv.index('-R') + 1]
`,
        fault,
      ],
      { encoding: "utf8", timeout: 10_000 },
    );
    assert.equal(result.status, 0, result.stderr);
  });
}
test("production identity is explicit and native browser passkeys remain optional", () => {
  const baseline = {
    JONES_MACOS_RELEASE_MODE: "production",
    JONES_MACOS_PRODUCTION_ID: "com.jonessystems.jonescode",
    T3CODE_APPLE_TEAM_ID: "2CUPSQS986",
    CSC_KEYCHAIN: "/synthetic/keychain",
  };
  assert.equal(desktopAppId({}, "com.t3tools.t3code"), "com.t3tools.t3code");
  assert.equal(nativePasskeysEnabled(baseline), false);
  requireProductionBuild(baseline, { platform: "mac", signed: true });
  assert.throws(() => requireProductionBuild(baseline, { platform: "mac", signed: false }));
  assert.throws(() =>
    nativePasskeysEnabled({ ...baseline, T3CODE_MACOS_PROVISIONING_PROFILE: "/profile" }),
  );
  assert.throws(() =>
    nativePasskeysEnabled({ ...baseline, JONES_MACOS_NATIVE_PASSKEYS: "enabled" }),
  );
  assert.equal(
    componentIdentifier("/a/Jones.app", "/a/Jones.app/Contents/MacOS/Jones"),
    "com.jonessystems.jonescode",
  );
});

test("production build passes literal requirement source to codesign", () => {
  const source = readFileSync(new URL("./sign.ts", import.meta.url), "utf8");
  const call = source.match(/await run\("\/usr\/bin\/codesign", (\[[\s\S]*?\])\);/);
  assert.ok(call, "production codesign verification call is present");
  const args = runInNewContext(call[1], {
    signingRequirement,
    JONES_MACOS_APP_ID,
    options: { app: "/synthetic/Jones.app" },
  });
  assert.ok(
    args[args.indexOf("-R") + 1].startsWith("="),
    "codesign -R treats unprefixed input as a file",
  );
  assert.equal(args.at(-1), "/synthetic/Jones.app");
});
