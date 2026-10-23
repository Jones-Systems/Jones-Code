// @effect-diagnostics nodeBuiltinImport:off - Isolated synthetic Python transaction; signing commands are stubbed and never access a keychain.
import { spawnSync } from "node:child_process";
import { expect, it } from "vite-plus/test";
import { jonesNativeHelperSource } from "./jonesNativeHelperSource.ts";

const definitions = jonesNativeHelperSource.split("\nparser = argparse.ArgumentParser()")[0];

it.each(["valid", "wrong-team", "adhoc", "tampered", "not-notarized", "wrong-id", "legacy-bootstrap", "rollback", "rollback-tampered", "rollback-adhoc"])("native signing admission: %s", (scenario) => {
  const result = spawnSync("python3", ["-c", `${definitions}
import tempfile
from types import SimpleNamespace
scenario = sys.argv[1]
with tempfile.TemporaryDirectory(prefix='jones-signing-test-') as temporary:
    root = pathlib.Path(temporary)
    active, incoming, previous = (root / name for name in ('active.app', 'incoming.app', 'previous.app'))
    rollback = scenario.startswith('rollback')
    for app in ((active, previous) if rollback else (active, incoming)):
        (app / 'Contents').mkdir(parents=True)
        identifier = PRODUCTION_BUNDLE_ID
        if scenario == 'wrong-id' and app == incoming: identifier = 'com.other.app'
        if scenario == 'legacy-bootstrap' and app == active: identifier = 'com.t3tools.t3code'
        with open(app / 'Contents/Info.plist', 'wb') as stream: plistlib.dump({'CFBundleIdentifier': identifier}, stream)
        (app / 'marker').write_text(app.name)
    expected = {'appPath': str(active), 'bundleIdentifier': PRODUCTION_BUNDLE_ID}
    commands = []
    def command(argv, **kwargs):
        commands.append(argv)
        selected = str(previous if rollback else incoming) == argv[-1]
        invalid = selected and ((scenario in ('wrong-team', 'adhoc', 'tampered', 'rollback-tampered', 'rollback-adhoc') and argv[0] == '/usr/bin/codesign') or (scenario == 'not-notarized' and 'stapler' in argv))
        return SimpleNamespace(returncode=1 if invalid else 0)
    subprocess.run = command
    try:
        if rollback: restore_bundle(expected, incoming, previous)
        else: swap_bundle(expected, incoming, previous)
        assert scenario in ('valid', 'rollback'), 'invalid signing state reached swap'
        assert (active / 'marker').read_text() == ('previous.app' if rollback else 'incoming.app')
    except RuntimeError:
        assert scenario not in ('valid', 'rollback')
        assert (active / 'marker').read_text() == 'active.app', 'refusal moved active bundle'
    for command in commands:
        if command[0] == '/usr/bin/codesign':
            assert '--deep' in command and '--strict' in command
            requirement = command[command.index('-R') + 1]
            assert '2CUPSQS986' in requirement and PRODUCTION_BUNDLE_ID in requirement
            assert '1.2.840.113635.100.6.1.13' in requirement
`, scenario], { encoding: "utf8", timeout: 10_000 });
  expect(result.stderr).toBe("");
  expect(result.status).toBe(0);
});
