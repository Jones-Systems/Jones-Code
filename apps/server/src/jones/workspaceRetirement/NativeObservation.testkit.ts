// @effect-diagnostics nodeBuiltinImport:off -- Isolated native subprocess fixtures never contact a real service manager.
import * as NodeAssert from "node:assert/strict";
import * as NodeChildProcess from "node:child_process";
import * as NodeFSP from "node:fs/promises";
import * as NodePath from "node:path";
import * as NodeOS from "node:os";
import { nativeProcesses } from "./NativeObservations.ts";
import { policyIO } from "./LinuxInfrastructureEvidence.testkit.ts";
import { git } from "./Filesystem.ts";
import {
  cachedNativeServices,
  nativeServices,
  SERVICE_PROJECTION_REUSE_MS,
  type ServiceIO,
} from "./NativeConsumers.ts";

async function fixture(run: (value: { base: string }) => Promise<void>) {
  const base = await NodeFSP.mkdtemp(NodePath.join(NodeOS.tmpdir(), "native-observation-"));
  try {
    await run({ base });
  } finally {
    await NodeFSP.rm(base, { recursive: true, force: true });
  }
}

export const nativeObservationCases: Record<string, () => Promise<void>> = {
  "native Linux process adapter carries the captured anchor through ordinary observations and verification":
    async () => {
      const events: string[] = [];
      const result = await nativeProcesses(
        "/fixture",
        "linux",
        policyIO(undefined, {
          capture: async () => {
            events.push("capture");
            return {
              base: "/fixture/anchor",
              fd: 3,
              snapshot: "fixture",
              close: async () => {
                events.push("close");
              },
            };
          },
          readlink: async (path) => {
            NodeAssert.ok(path.startsWith("/fixture/anchor/"));
            events.push("readlink");
            throw Object.assign(Error(), { code: "EACCES" });
          },
          read: async (path) => {
            NodeAssert.ok(path.startsWith("/fixture/anchor/"));
            events.push("read");
            return "";
          },
          verifyInfrastructure: async (anchor) => {
            NodeAssert.equal(anchor.base, "/fixture/anchor");
            events.push("verify");
            return { kind: "verified", basisDigest: "a".repeat(64) };
          },
        }),
      );
      NodeAssert.equal(result.status, "partial");
      NodeAssert.equal(result.processPolicy?.effectiveStatus, "complete");
      NodeAssert.deepEqual(events, [
        "capture",
        "readlink",
        "readlink",
        "read",
        "read",
        "verify",
        "close",
      ]);
    },
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
  "service projection is captured once across targets and expires": async () => {
    let clock = 1_000_000,
      calls = 0,
      fail = false;
    const io: ServiceIO = {
      now: () => clock,
      uid: () => 501,
      command: async (_file, args) => {
        calls++;
        if (fail) throw Error("controlled manager unavailable");
        return args[1]?.split("/").length === 2
          ? "domain = {\n\tservices = {\n\t\t0 0 fixture.one\n\t\t0 0 fixture.two\n\t}\n}\n"
          : args[1]?.endsWith("fixture.one")
            ? "one = {\n working directory = /fixture/first\n}\n"
            : "two = {\n arguments = {\n /outside/tool\n --root=/fixture/second/src\n }\n}\n";
      },
    };
    const services = cachedNativeServices("darwin", io);
    const first = await services("/fixture/first");
    // Two domains, each listing two labels: six manager reads per capture.
    NodeAssert.equal(calls, 6);
    NodeAssert.equal(first.status, "complete");
    NodeAssert.equal(first.observedAt, 1_000_000);
    NodeAssert.deepEqual(first.paths, ["/fixture/first"]);
    clock += SERVICE_PROJECTION_REUSE_MS - 1;
    const [second, third] = await Promise.all([
      services("/fixture/second"),
      services("/fixture/third"),
    ]);
    NodeAssert.equal(calls, 6);
    // Reused evidence keeps its capture time, so consumer staleness still applies to it.
    NodeAssert.equal(second.observedAt, 1_000_000);
    NodeAssert.deepEqual(second.paths, ["/fixture/second"]);
    NodeAssert.deepEqual(third.paths, []);
    NodeAssert.equal(third.status, "complete");
    clock += 1;
    const expired = await services("/fixture/first");
    NodeAssert.equal(calls, 12);
    NodeAssert.equal(expired.observedAt, 1_000_000 + SERVICE_PROJECTION_REUSE_MS);
    // A clock moving backwards never reuses a capture.
    clock -= 1;
    await services("/fixture/first");
    NodeAssert.equal(calls, 18);
    // Failed captures are reported and never reused.
    clock += 60_000;
    fail = true;
    const failed = await services("/fixture/first");
    NodeAssert.equal(failed.status, "unavailable");
    NodeAssert.deepEqual(failed.paths, []);
    fail = false;
    const recovered = await services("/fixture/first");
    NodeAssert.equal(recovered.status, "complete");
    NodeAssert.deepEqual(recovered.paths, ["/fixture/first"]);
    NodeAssert.equal(calls, 25);
  },
};

// Real host observations: run only on the platform whose native tools they exercise.
export const darwinNativeObservationCases: Record<string, () => Promise<void>> = {
  "native Darwin process probe is complete and detects a child cwd": async () =>
    fixture(async ({ base }) => {
      const target = await NodeFSP.realpath(base);
      // The child execs into sleep without reaping its short-lived child, leaving a same-user
      // zombie: lsof reports nothing for it, yet signal 0 still finds its PID.
      const child = NodeChildProcess.spawn(
        "/bin/sh",
        ["-c", "/usr/bin/true & exec /bin/sleep 30"],
        {
          cwd: target,
          stdio: "ignore",
        },
      );
      try {
        await new Promise<void>((resolve, reject) => {
          child.once("spawn", resolve);
          child.once("error", reject);
        });
        const zombie = () =>
          new Promise<boolean>((resolve, reject) =>
            NodeChildProcess.execFile(
              "/bin/ps",
              ["-axo", "ppid=,stat="],
              { encoding: "utf8" },
              (error, stdout) =>
                error
                  ? reject(error)
                  : resolve(
                      stdout
                        .split("\n")
                        .map((row) => row.trim().split(/\s+/))
                        .some(
                          ([ppid, state]) => ppid === String(child.pid) && state?.startsWith("Z"),
                        ),
                    ),
            ),
          );
        // Each readback is one awaited ps run; the milestone is the observed zombie state.
        let readbacks = 0;
        while (!(await zombie())) NodeAssert.ok(++readbacks < 200, "fixture zombie did not appear");
        const probe = await nativeProcesses(target, "darwin");
        NodeAssert.equal(probe.status, "complete", JSON.stringify(probe.limits));
        NodeAssert.deepEqual(probe.paths, [target]);
        const clear = await nativeProcesses(NodePath.join(target, "absent"), "darwin");
        NodeAssert.equal(clear.status, "complete", JSON.stringify(clear.limits));
        NodeAssert.deepEqual(clear.paths, []);
      } finally {
        // Only the captured spawn PID of this fixture's own child is signalled.
        if (child.exitCode === null && child.signalCode === null) {
          const exited = new Promise((resolve) => child.once("exit", resolve));
          child.kill();
          await exited;
        }
      }
    }),
};
