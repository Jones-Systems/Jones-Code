// @effect-diagnostics nodeBuiltinImport:off - These Python protocol tests create no application runtime or native admission evidence.
import * as NodeChildProcess from "node:child_process";
import { describe, expect, it } from "vite-plus/test";
import { jonesLegacyBootstrapSource } from "./jonesLegacyBootstrapSource.ts";
import { jonesLegacyMaintenanceSource } from "./jonesLegacyMaintenanceSource.ts";

const protocolFixture = String.raw`
import contextlib, io, json, pathlib, sys
payload = json.load(sys.stdin)
scope = {'__name__': 'maintenance_protocol_fixture'}
exec(compile(payload['source'], 'maintenance.py', 'exec'), scope)
fault = payload['case']
if fault == 'anchor':
    reads = []
    def forbidden(path, *args):
        reads.append(str(path))
        raise AssertionError('Invalid anchor reached a filesystem read')
    scope['read_owned'] = forbidden
    actual = sys.platform
    sys.platform = 'darwin'
    try:
        output = io.StringIO()
        with contextlib.redirect_stdout(output):
            code = scope['maintenance_main'](['cancel', '--manifest', '/fixture/private/state.sqlite'])
    finally: sys.platform = actual
    assert code == 1 and reads == []
    report = json.loads(output.getvalue())
    assert report['activationAllowed'] is False and report['stateAdmissionAllowed'] is False
elif fault == 'arguments':
    with contextlib.redirect_stderr(io.StringIO()):
        try: scope['maintenance_main'](['cancel', '--manifest', '/fixture/runtime/jones-active-install.json', '--selection', '/arbitrary'])
        except SystemExit as result: assert result.code == 2
        else: raise AssertionError('Cancel accepted selection authority')
elif fault == 'cursor':
    owner = scope['Maintenance'](pathlib.Path('/fixture/runtime/jones-active-install.json'), lambda: None)
    owner.original = {'appPath': '/fixture/Old.app', 'executablePath': '/fixture/Old.app/Contents/MacOS/Jones', 'generation': '1' * 64}
    owner.plan = {'maintenanceGeneration': '2' * 64}
    owner.gates = [{'path': '/fixture/Old.app/Contents/MacOS/Jones', 'app': '/fixture/Old.app'}, {'path': '/fixture/Retained.app/Contents/MacOS/Jones', 'app': '/fixture/Retained.app'}]
    owner.journal = {'phase': 'armed', 'done': [owner.gates[0]['path']], 'intent': None, 'cancel': None, 'armedBootSessionUuid': None}
    try: owner.validate_cursor()
    except scope['Refused']: pass
    else: raise AssertionError('Incomplete gates were accepted as armed')
elif fault == 'no-bytecode':
    assert sys.dont_write_bytecode is True
    assert payload['source'].index('sys.dont_write_bytecode = True') < payload['source'].index('import argparse')
elif fault == 'shim-bytes':
    active = {'appPath': '/fixture/Jones.app', 'executablePath': '/fixture/Jones.app/Contents/MacOS/Jones'}
    assert scope['expected_shim'](pathlib.Path('/fixture/home'), active).decode() == payload['expectedShim']
else: raise AssertionError('Unknown protocol fixture case')
print(json.dumps({'case': fault, 'passed': True}))
`;

describe("standalone pre-state maintenance protocol", () => {
  it("composes shared definitions while replacing only the checked entry", () => {
    const entry = "if __name__ == '__main__':\n    sys.exit(main())\n";
    expect(jonesLegacyBootstrapSource.endsWith(entry)).toBe(true);
    expect(
      jonesLegacyMaintenanceSource.startsWith(jonesLegacyBootstrapSource.slice(0, -entry.length)),
    ).toBe(true);
    expect(jonesLegacyMaintenanceSource.endsWith("    sys.exit(maintenance_main())\n")).toBe(true);
    expect(jonesLegacyMaintenanceSource).not.toContain("$" + "{");
    expect(jonesLegacyMaintenanceSource).not.toContain("`");
  });

  for (const scenario of ["anchor", "arguments", "cursor", "no-bytecode", "shim-bytes"]) {
    it(`enforces ${scenario} before granting any maintenance effect`, () => {
      const result = NodeChildProcess.spawnSync("python3", ["-I", "-B", "-c", protocolFixture], {
        input: JSON.stringify({
          source: jonesLegacyMaintenanceSource,
          case: scenario,
          expectedShim: [
            "#!/bin/sh",
            "# Written by T3 Code: runs the desktop app's bundled t3 CLI.",
            "export T3CODE_CLI_PATH='/fixture/home/bin/t3'",
            "home='/fixture/home'",
            'export T3CODE_HOME="${T3CODE_HOME:-$home}"',
            "export ELECTRON_RUN_AS_NODE=1",
            "app='/fixture/Jones.app/Contents/MacOS/Jones'",
            'if [ ! -x "$app" ]; then',
            '  echo "T3 Code has moved or been removed. Open the app once to update this command." >&2',
            "  exit 127",
            "fi",
            'exec "$app" \'/fixture/Jones.app/Contents/Resources/app.asar/apps/server/dist/bin.mjs\' "$@"',
            "",
          ].join("\n"),
        }),
        encoding: "utf8",
        timeout: 10000,
        maxBuffer: 1024 * 1024,
      });
      expect(result.error).toBeUndefined();
      expect(result.status, result.stderr).toBe(0);
      expect(JSON.parse(result.stdout)).toEqual({ case: scenario, passed: true });
    });
  }
});
