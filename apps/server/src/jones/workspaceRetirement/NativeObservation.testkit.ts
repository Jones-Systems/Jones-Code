// @effect-diagnostics nodeBuiltinImport:off -- Isolated native subprocess fixtures never contact a real service manager.
import * as NodeAssert from "node:assert/strict";
import * as NodeFSP from "node:fs/promises";
import * as NodePath from "node:path";
import * as NodeOS from "node:os";
import { git } from "./Filesystem.ts";
import { nativeServices, type ServiceIO } from "./NativeConsumers.ts";

async function fixture(run: (value: { base: string }) => Promise<void>) {
  const base = await NodeFSP.mkdtemp(NodePath.join(NodeOS.tmpdir(), "native-observation-"));
  try {
    await run({ base });
  } finally {
    await NodeFSP.rm(base, { recursive: true, force: true });
  }
}

export const nativeObservationCases: Record<string, () => Promise<void>> = {
  "loaded native service observations use bounded manager projections and retain unknown coverage":
    async () => {
      const calls: string[][] = [];
      const linux: ServiceIO = {
        uid: () => 501,
        command: async (_file, args) => {
          calls.push(args);
          return args.includes("list-units")
            ? "fixture.service loaded inactive dead Fixture\n"
            : "Id=fixture.service\nLoadState=loaded\nWorkingDirectory=/fixture/checkout\nRootDirectory=\nExecStart={ path=/outside/tool ; argv[]=/outside/tool ; }\nFragmentPath=\n";
        },
      };
      const result = await nativeServices("/fixture/checkout", "linux", linux);
      NodeAssert.equal(result.status, "complete");
      NodeAssert.deepEqual(result.paths, ["/fixture/checkout"]);
      NodeAssert.equal(
        calls.some((args) => args.some((a) => a.includes("Environment"))),
        false,
      );
      const darwin: ServiceIO = {
        uid: () => 501,
        command: async (_file, args) =>
          args[1]?.split("/").length === 2
            ? "domain = {\n\tservices = {\n\t\t0 0 fixture.service\n\t}\n}\n"
            : "fixture = {\n program = /outside/tool\n working directory = /fixture/checkout\n environment = {\n SECRET = never-returned\n }\n}\n",
      };
      const projected = await nativeServices("/fixture/checkout", "darwin", darwin);
      NodeAssert.equal(projected.status, "complete");
      NodeAssert.deepEqual(projected.paths, ["/fixture/checkout"]);
      NodeAssert.equal(JSON.stringify(projected).includes("never-returned"), false);
      const unavailable = await nativeServices("/fixture/checkout", "darwin", {
        ...darwin,
        command: async () => {
          throw Error("controlled manager unavailable");
        },
      });
      NodeAssert.equal(unavailable.status, "unavailable");
      NodeAssert.equal(unavailable.paths.length, 0);
    },
  "Git subprocess boundary accepts fractional budgets and rejects expired budgets": async () =>
    fixture(async ({ base }) => {
      NodeAssert.match(
        (await git(base, ["--version"], 1499.745857000351)).toString(),
        /^git version /,
      );
      for (const budget of [0, -1, NaN, Infinity])
        await NodeAssert.rejects(() => git(base, ["--version"], budget));
    }),
  "native service subprocess boundary accepts fractional budgets and rejects expired budgets":
    async () =>
      fixture(async ({ base }) => {
        await NodeFSP.writeFile(NodePath.join(base, "systemctl"), "#!/bin/sh\nprintf ''\n", {
          mode: 0o700,
        });
        const path = process.env.PATH;
        const now = Object.getOwnPropertyDescriptor(performance, "now");
        let calls = 0;
        try {
          process.env.PATH = base;
          Object.defineProperty(performance, "now", {
            configurable: true,
            value: () => (calls++ === 0 ? 0 : 3500.254142999649),
          });
          const result = await nativeServices("/fixture/checkout", "linux");
          NodeAssert.equal(result.status, "complete", JSON.stringify(result.limits));
          NodeAssert.deepEqual(result.paths, []);
          calls = 0;
          Object.defineProperty(performance, "now", {
            configurable: true,
            value: () => (calls++ === 0 ? 0 : 5001),
          });
          const expired = await nativeServices("/fixture/checkout", "linux");
          NodeAssert.equal(expired.status, "unavailable");
          NodeAssert.equal(expired.limits[0], "native_service_probe_budget_exceeded");
        } finally {
          if (path === undefined) delete process.env.PATH;
          else process.env.PATH = path;
          if (now) Object.defineProperty(performance, "now", now);
          else Reflect.deleteProperty(performance, "now");
        }
      }),
  "Linux native services distinguish absent placeholders from consumers and failed observations":
    async () => {
      const projection = (state: string, directory = "", id = "fixture.service") =>
        `Id=${id}\nLoadState=${state}\nWorkingDirectory=${directory}\nRootDirectory=\nExecStart=\nFragmentPath=\n`;
      for (const [listed, shown, expectedStatus, expectedPaths] of [
        ["loaded", projection("loaded", "/fixture/checkout"), "complete", ["/fixture/checkout"]],
        ["loaded", projection("loaded", "/outside"), "complete", []],
        ["not-found", projection("not-found"), "complete", []],
        ["loaded", projection("not-found"), "unavailable", []],
        ["not-found", projection("loaded"), "unavailable", []],
        ["not-found", projection("not-found", "/fixture/checkout"), "unavailable", []],
        [
          "not-found",
          projection("not-found").replace("FragmentPath=", "FragmentPath= "),
          "unavailable",
          [],
        ],
        ["not-found", projection("not-found", "", "other.service"), "unavailable", []],
        ["not-found", "LoadState=not-found\n", "unavailable", []],
        ["loaded", projection("loaded") + "LoadState=not-found\n", "unavailable", []],
        ["error", projection("error"), "unavailable", []],
        ["loaded", null, "unavailable", []],
      ] as const) {
        const result = await nativeServices("/fixture/checkout", "linux", {
          uid: () => 501,
          command: async (_file, args) => {
            if (args.includes("list-units"))
              return `fixture.service ${listed} inactive dead Fixture\n`;
            if (shown === null) throw Error("controlled manager read failure");
            return shown;
          },
        });
        NodeAssert.equal(result.status, expectedStatus, `${listed}: ${shown}`);
        NodeAssert.deepEqual(result.paths, expectedPaths);
      }
      const mixed = await nativeServices("/fixture/checkout", "linux", {
        uid: () => 501,
        command: async (_file, args) =>
          args.includes("list-units")
            ? "absent.service not-found inactive dead Absent\nfixture.service loaded inactive dead Fixture\n"
            : args.includes("absent.service")
              ? projection("not-found", "", "absent.service")
              : projection("loaded", "/fixture/checkout"),
      });
      NodeAssert.equal(mixed.status, "complete");
      NodeAssert.deepEqual(mixed.paths, ["/fixture/checkout"]);
      const failed = await nativeServices("/fixture/checkout", "linux", {
        uid: () => 501,
        command: async () => {
          throw Error("controlled enumeration failure");
        },
      });
      NodeAssert.equal(failed.status, "unavailable");
    },
};
