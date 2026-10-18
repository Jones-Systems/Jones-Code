import { expect, it } from "vite-plus/test";
import {
  placementBootstrapArguments,
  readProcessPlacementBootstrap,
  validateProcessPlacementBootstrap,
} from "./processPlacementBootstrap.ts";
import { readProcessPlacementBinding } from "./processPlacement.ts";

const binding = {
  version: 1 as const,
  executablePath: "/usr/bin/python3.13",
  executableSha256: "a".repeat(64),
  scriptPath: "/opt/t3/bootstrap.py",
  scriptSha256: "b".repeat(64),
  policyPath: "/opt/t3/policy.json",
  policySha256: "c".repeat(64),
  helperPath: "/opt/t3/process-placement",
  helperSha256: "d".repeat(64),
};

it("keeps an absent external bootstrap disabled", () => {
  expect(readProcessPlacementBootstrap({})).toBeUndefined();
});
it("decodes only the explicit versioned external artifact identities", () => {
  expect(
    readProcessPlacementBootstrap({ T3_PROCESS_PLACEMENT_BOOTSTRAP: JSON.stringify(binding) }),
  ).toEqual(binding);
  expect(placementBootstrapArguments(binding).at(-1)).toBe("--");
});
it.each([
  "",
  "null",
  "{}",
  JSON.stringify({ ...binding, version: 2 }),
  JSON.stringify({ ...binding, extra: true }),
  JSON.stringify({ ...binding, executablePath: "python3" }),
  JSON.stringify({ ...binding, policySha256: "bad" }),
])("rejects invalid required external bootstrap bindings", (text) => {
  expect(() => readProcessPlacementBootstrap({ T3_PROCESS_PLACEMENT_BOOTSTRAP: text })).toThrow(
    "Process placement required",
  );
});
it("rejects a cold bootstrap without a ready binding at the parser boundary", () => {
  expect(() =>
    readProcessPlacementBinding({ T3_PROCESS_PLACEMENT_BOOTSTRAP: JSON.stringify(binding) }),
  ).toThrow("bootstrap must establish a ready binding");
});

it("honors the injected host when validating cold bootstrap artifacts", () => {
  expect(() => validateProcessPlacementBootstrap(binding, "darwin")).toThrow(
    "bootstrap requires Linux cgroup v2",
  );
});
