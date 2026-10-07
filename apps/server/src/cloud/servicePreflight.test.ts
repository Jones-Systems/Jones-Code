import { expect, it } from "@effect/vitest";

import {
  decodeServicePreflightResult,
  qualifiedServicePreflightFailure,
  runServicePreflight,
} from "./servicePreflight.ts";
import { SERVICE_LAUNCHER_PROTOCOL } from "./serviceProtocol.ts";

it.each([1, 2])("blocks legacy launcher protocol %i", (launcherProtocol) => {
  expect(
    runServicePreflight({
      databasePath: "/missing/state.sqlite",
      launcherProtocol,
      version: "1.2.3",
    }),
  ).toEqual({
    status: "blocked",
    version: "1.2.3",
    reason:
      "This release requires a newer T3 Code service launcher. Update it on the server machine.",
  });
});

it("accepts the current launcher protocol", () => {
  expect(
    runServicePreflight({
      databasePath: "/missing/state.sqlite",
      launcherProtocol: SERVICE_LAUNCHER_PROTOCOL,
      version: "1.2.3",
    }),
  ).toEqual({
    status: "ready",
    version: "1.2.3",
    launcherProtocol: SERVICE_LAUNCHER_PROTOCOL,
  });
});

it("advertises only the supplied bundled startup gate capability", () => {
  expect(runServicePreflight({
    databasePath: "/missing/state.sqlite", launcherProtocol: SERVICE_LAUNCHER_PROTOCOL,
    version: "1.2.3", startupGateProtocol: 1,
  })).toEqual({status: "ready", version: "1.2.3", launcherProtocol: SERVICE_LAUNCHER_PROTOCOL, startupGateProtocol: 1});
});

it("preserves ordinary preflight without inferring qualified support", () => {
  const ready = {status: "ready", version: "1.2.3", launcherProtocol: SERVICE_LAUNCHER_PROTOCOL};
  expect(decodeServicePreflightResult(ready)).toEqual(ready);
  expect(qualifiedServicePreflightFailure({code: 0, stdout: JSON.stringify(ready), version: "1.2.3"}))
    .toContain("startup-gate-unavailable");
});

it.each([null, undefined, 0, 2, "1", true])("rejects supplied malformed startup gate marker %s", (startupGateProtocol) => {
  expect(decodeServicePreflightResult({status: "ready", version: "1.2.3",
    launcherProtocol: SERVICE_LAUNCHER_PROTOCOL, startupGateProtocol})).toBeUndefined();
});

it("qualifies only an exact successful candidate with symmetric gate support", () => {
  const result = {status: "ready", version: "1.2.3", launcherProtocol: SERVICE_LAUNCHER_PROTOCOL, startupGateProtocol: 1};
  const stdout = JSON.stringify(result);
  expect(qualifiedServicePreflightFailure({code: 0, stdout, version: "1.2.3"})).toBeUndefined();
  for (const input of [
    {code: 1, stdout, version: "1.2.3"},
    {code: 0, stdout, version: "1.2.4"},
    {code: 0, stdout: "not JSON", version: "1.2.3"},
    {code: 0, stdout: JSON.stringify({...result, startupGateProtocol: 2}), version: "1.2.3"},
    {code: 0, stdout: JSON.stringify({...result, launcherProtocol: 3}), version: "1.2.3"},
  ]) expect(qualifiedServicePreflightFailure(input)).toContain("startup-gate-unavailable");
});
