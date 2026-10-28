// @effect-diagnostics nodeBuiltinImport:off globalTimers:off globalDate:off -- Synthetic Node process, pipe and clock fixtures exercise the actual ownership boundary.
import { expect, it } from "vite-plus/test";
import { minimalWorkerEnvironment } from "./workerCommand.ts";
import { decodeBootstrap } from "./protocol.ts";

it("does not inherit provider credentials, arbitrary loader hooks or broad NODE_OPTIONS", () => {
  const input = { PATH: "/bin", HOME: "/synthetic", LANG: "en_US.UTF-8", NODE_OPTIONS: "--import=/untrusted/hook", ANTHROPIC_API_KEY: "fixture", OPENAI_API_KEY: "fixture", NODE_PATH: "/untrusted" };
  expect(minimalWorkerEnvironment(input, false, false)).toEqual({ PATH: "/bin", HOME: "/synthetic", LANG: "en_US.UTF-8" });
  expect(minimalWorkerEnvironment(input, true, false).NODE_OPTIONS).toBe("--max-old-space-size=256");
  expect(minimalWorkerEnvironment(input, false, true).ELECTRON_RUN_AS_NODE).toBe("1");
});

it("has no bootstrap option for arbitrary PID, module, command, native cap or weaker fallback", () => {
  const boot = { version: 1, authorityId: "authority", slotId: 0, slotGeneration: 1, nonce: "nonce", build: "a".repeat(64) };
  for (const extra of [{ pid: 1 }, { module: "x" }, { fallback: true }, { workers: 1 }]) expect(() => decodeBootstrap({ ...boot, ...extra })).toThrow();
});
