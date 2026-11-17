// @effect-diagnostics nodeBuiltinImport:off -- Deterministic in-memory process fixtures use Node assertions and never inspect live processes.
import * as NodeAssert from "node:assert/strict";
import {
  consumerDecision,
  processProbe,
  type ProcessProbeIO,
  type Probe,
} from "./ConsumerProbe.ts";

const target = "/fixture/checkout";
const failure = (code: string) => Object.assign(Error("private argv/path/stderr"), { code });
function linux(overrides: Partial<ProcessProbeIO> = {}): ProcessProbeIO {
  return {
    command: async () => {
      throw Error("unexpected command");
    },
    list: async () => ["101", "102", "not-a-pid"],
    uid: () => 501,
    statUid: async () => 501,
    readlink: async () => "/outside",
    read: async () => "",
    ...overrides,
  };
}
// Node's execFile rejection shape for a child that ran to an exit status.
const exited = (status: number | null, stdout: string | undefined, extra: object = {}) =>
  Object.assign(Error("private argv/path/stderr"), {
    code: status,
    killed: false,
    signal: null,
    ...(stdout === undefined ? {} : { stdout }),
    ...extra,
  });
function darwin(
  lsof: () => Promise<string>,
  options: {
    alive?: (pid: string) => boolean;
    running?: ProcessProbeIO["running"];
    argv?: () => Promise<string>;
  } = {},
): ProcessProbeIO {
  return linux({
    command: async (_file, args) => {
      if (args[0] === "-axo") return "101 501\n102 501\n103 502";
      if (args.includes("-Fpn")) return lsof();
      return options.argv ? options.argv() : "";
    },
    ...(options.running
      ? { running: options.running }
      : options.alive
        ? { running: async (pids) => new Set(pids.filter(options.alive!)) }
        : {}),
  });
}
function decide(probe: Probe) {
  const clear: Probe = { observedAt: probe.observedAt, status: "complete", paths: [], limits: [] };
  return consumerDecision(
    target,
    {
      native: clear,
      services: clear,
      registry: clear,
      workloads: clear,
      obligations: clear,
      process: probe,
    },
    probe.observedAt,
  );
}
function has(probe: Probe, diagnostic: string) {
  NodeAssert.ok(probe.limits.includes(diagnostic), JSON.stringify(probe.limits));
}
function privateDiagnostics(probe: Probe) {
  NodeAssert.doesNotMatch(probe.limits.join("\n"), /private|\/fixture|\/proc|101|102|stderr/);
  NodeAssert.ok(probe.limits.length < 70);
}
export const processCoverageCases: Record<string, () => Promise<void>> = {
  "Linux coverage explains permission, unavailable and oversized reads while retaining active evidence":
    async () => {
      const probe = await processProbe(
        target,
        "linux",
        linux({
          list: async () => ["101", "102", "103", "104", "105"],
          readlink: async (path) => {
            if (path.endsWith("/cwd")) return target;
            throw failure("EACCES");
          },
          read: async (path) => {
            if (path.includes("/101/")) throw failure("EIO");
            if (path.includes("/102/")) throw Error("bound_exceeded");
            if (path.includes("/103/")) return "x".repeat(1024 * 1024 + 1);
            if (path.endsWith("/maps")) {
              if (path.includes("/105/")) return "x".repeat(2 * 1024 * 1024 + 1);
              throw failure("EPERM");
            }
            return target;
          },
        }),
      );
      NodeAssert.equal(probe.status, "partial");
      NodeAssert.deepEqual(probe.paths, [target]);
      has(probe, "process_probe_coverage:listed=5,attempted=5,same_user=5,other_user=0");
      has(probe, "process_probe_observation:stage=linux_exe,category=permission,count=5");
      has(probe, "process_probe_observation:stage=linux_cmdline,category=unavailable,count=1");
      has(probe, "process_probe_observation:stage=linux_cmdline,category=data_bound,count=2");
      has(probe, "process_probe_observation:stage=linux_maps,category=permission,count=1");
      has(probe, "process_probe_observation:stage=linux_maps,category=data_bound,count=2");
      has(probe, "process_probe_observation:stage=linux_maps,category=unavailable,count=1");
      const clear: Probe = {
        observedAt: probe.observedAt,
        status: "complete",
        paths: [],
        limits: [],
      };
      const decision = consumerDecision(
        target,
        {
          native: clear,
          services: clear,
          registry: clear,
          workloads: clear,
          obligations: clear,
          process: probe,
        },
        probe.observedAt,
      );
      NodeAssert.deepEqual(decision.holds, ["active_consumer"]);
      NodeAssert.deepEqual(decision.unknown, ["consumer_evidence_unavailable"]);
      NodeAssert.deepEqual(decision.coverage.process?.limits, probe.limits);
      privateDiagnostics(probe);
    },
  "Linux disappearance and other-user exclusions retain complete coverage; non-disappearance errors do not":
    async () => {
      for (const code of ["ENOENT", "ESRCH", "EPERM"]) {
        const probe = await processProbe(
          target,
          "linux",
          linux({
            statUid: async (path) => {
              if (path.endsWith("102")) return 502;
              throw failure(code);
            },
          }),
        );
        NodeAssert.equal(probe.status, code === "EPERM" ? "partial" : "complete");
        has(probe, "process_probe_coverage:listed=2,attempted=2,same_user=0,other_user=1");
        has(
          probe,
          `process_probe_observation:stage=linux_owner,category=${code === "EPERM" ? "permission" : "disappeared"},count=1`,
        );
        privateDiagnostics(probe);
      }
      const complete = await processProbe(target, "linux", linux({ read: async () => target }));
      NodeAssert.equal(complete.status, "complete");
      NodeAssert.deepEqual(complete.paths, [target]);
      has(complete, "process_probe_coverage:listed=2,attempted=2,same_user=2,other_user=0");
      const enumeration = await processProbe(
        target,
        "linux",
        linux({
          list: async () => {
            throw failure("EACCES");
          },
        }),
      );
      NodeAssert.equal(enumeration.status, "unavailable");
      has(
        enumeration,
        "process_probe_observation:stage=linux_enumerate,category=permission,count=1",
      );
      for (const code of ["ENOENT", "ESRCH"]) {
        const vanishedRead = await processProbe(
          target,
          "linux",
          linux({
            read: async () => {
              throw failure(code);
            },
          }),
        );
        NodeAssert.equal(vanishedRead.status, "complete");
        const links = await processProbe(
          target,
          "linux",
          linux({
            readlink: async () => {
              throw failure(code);
            },
          }),
        );
        NodeAssert.equal(links.status, code === "ENOENT" ? "complete" : "partial");
      }
    },
  "Both platforms distinguish elapsed budget from PID bound without inventing complete coverage":
    async () => {
      for (const platform of ["linux", "darwin"] as const) {
        let time = 0;
        const budget = await processProbe(
          target,
          platform,
          linux({
            now: () => time,
            list: async () => ["101", "102"],
            statUid: async () => {
              time = 5001;
              return 501;
            },
            read: async () => target,
            command: async (_file, args) => {
              if (args[0] === "-axo")
                return Array.from({ length: 65 }, (_, i) => `${i + 1} 501`).join("\n");
              time = 5001;
              return target;
            },
          }),
        );
        NodeAssert.equal(budget.status, "partial");
        NodeAssert.deepEqual(budget.paths, [target]);
        has(budget, "process_probe_observation:stage=scan,category=time_budget,count=1");
        const bounded = await processProbe(
          target,
          platform,
          linux({
            list: async () => Array.from({ length: 4097 }, (_, i) => String(i + 1)),
            command: async () => Array.from({ length: 4097 }, (_, i) => `${i + 1} 501`).join("\n"),
          }),
        );
        NodeAssert.equal(bounded.status, "unavailable");
        has(bounded, "process_probe_coverage:listed=4097,attempted=0,same_user=0,other_user=0");
        has(bounded, "process_probe_observation:stage=scan,category=pid_bound,count=1");
      }
    },
  "Darwin command stages retain partial observations and sanitize subprocess errors": async () => {
    const probe = await processProbe(
      target,
      "darwin",
      linux({
        command: async (_file, args) => {
          if (args[0] === "-axo") return "101 501\n102 502";
          if (args.includes("-Fpn")) throw failure("EACCES");
          return target;
        },
      }),
    );
    NodeAssert.equal(probe.status, "partial");
    NodeAssert.deepEqual(probe.paths, [target]);
    has(probe, "process_probe_observation:stage=darwin_lsof,category=permission,count=1");
    privateDiagnostics(probe);
    for (const [code, category] of [
      ["ETIMEDOUT", "timeout"],
      ["ERR_CHILD_PROCESS_STDIO_MAXBUFFER", "data_bound"],
      ["EIO", "unavailable"],
    ]) {
      const unavailable = await processProbe(
        target,
        "darwin",
        linux({
          command: async () => {
            throw failure(code!);
          },
        }),
      );
      NodeAssert.equal(unavailable.status, "unavailable");
      has(
        unavailable,
        `process_probe_observation:stage=darwin_enumerate,category=${category},count=1`,
      );
      privateDiagnostics(unavailable);
    }
    const killed = await processProbe(
      target,
      "darwin",
      linux({
        command: async (_file, args) => {
          if (args[0] === "-axo") return "101 501";
          if (args.includes("-Fpn")) return "n" + target;
          throw Object.assign(Error("private stderr"), { killed: true, signal: "SIGTERM" });
        },
      }),
    );
    NodeAssert.equal(killed.status, "partial");
    NodeAssert.deepEqual(killed.paths, [target]);
    has(killed, "process_probe_observation:stage=darwin_argv,category=timeout,count=1");
    const complete = await processProbe(
      target,
      "darwin",
      linux({ command: async (_file, args) => (args[0] === "-axo" ? "101 501" : "") }),
    );
    NodeAssert.equal(complete.status, "complete");
    NodeAssert.deepEqual(complete.paths, []);
  },
  "Darwin lsof exit 1 with a vanished pid stays complete and keeps observed paths": async () => {
    const checked: string[] = [];
    const alive = (pid: string) => {
      checked.push(pid);
      return pid !== "102";
    };
    const output = `p101\nfcwd\nn${target}/package\nftxt\nn/usr/lib/dyld\n`;
    const probe = await processProbe(
      target,
      "darwin",
      darwin(
        async () => {
          throw exited(1, output);
        },
        { alive },
      ),
    );
    NodeAssert.equal(probe.status, "complete", JSON.stringify(probe.limits));
    NodeAssert.deepEqual(probe.paths, [target + "/package"]);
    NodeAssert.deepEqual(checked, ["102"]);
    has(probe, "process_probe_coverage:listed=2,attempted=2,same_user=2,other_user=0");
    has(probe, "process_probe_observation:stage=darwin_lsof,category=disappeared,count=1");
    NodeAssert.deepEqual(decide(probe).holds, ["active_consumer"]);
    NodeAssert.deepEqual(decide(probe).unknown, []);
    privateDiagnostics(probe);
    // A clear batch with a vanished PID is complete evidence, not a hold.
    const clear = await processProbe(
      target,
      "darwin",
      darwin(
        async () => {
          throw exited(1, "p101\nfcwd\nn/outside\n");
        },
        { alive },
      ),
    );
    NodeAssert.equal(clear.status, "complete");
    NodeAssert.deepEqual(decide(clear).holds, []);
    NodeAssert.deepEqual(decide(clear).unknown, []);
    // ps exits 1 only when no listed PID matched; each must then be confirmed gone.
    const allGone = await processProbe(
      target,
      "darwin",
      darwin(
        async () => {
          throw exited(1, "");
        },
        {
          alive: () => false,
          argv: async () => {
            throw exited(1, "");
          },
        },
      ),
    );
    NodeAssert.equal(allGone.status, "complete", JSON.stringify(allGone.limits));
    has(allGone, "process_probe_observation:stage=darwin_lsof,category=disappeared,count=2");
    has(allGone, "process_probe_observation:stage=darwin_argv,category=disappeared,count=2");
  },
  "Darwin lsof exit 1 with a live unreported pid stays partial": async () => {
    const output = `p101\nfcwd\nn${target}\n`;
    const live = await processProbe(
      target,
      "darwin",
      darwin(
        async () => {
          throw exited(1, output);
        },
        { alive: () => true },
      ),
    );
    NodeAssert.equal(live.status, "partial");
    NodeAssert.deepEqual(live.paths, [target]);
    has(live, "process_probe_observation:stage=darwin_lsof,category=unavailable,count=1");
    NodeAssert.deepEqual(decide(live).unknown, ["consumer_evidence_unavailable"]);
    privateDiagnostics(live);
    // Without liveness evidence an unreported PID is never assumed gone.
    const unknownLiveness = await processProbe(
      target,
      "darwin",
      darwin(async () => {
        throw exited(1, output);
      }),
    );
    NodeAssert.equal(unknownLiveness.status, "partial");
    // A failed or unexpected process-state readback never confirms an exit.
    const failedReadback = await processProbe(
      target,
      "darwin",
      darwin(
        async () => {
          throw exited(1, output);
        },
        {
          running: async () => {
            throw failure("EIO");
          },
        },
      ),
    );
    NodeAssert.equal(failedReadback.status, "partial");
    has(failedReadback, "process_probe_observation:stage=darwin_lsof,category=unavailable,count=1");
    // Every other failure class stays incomplete, even when its stdout omits a vanished PID.
    for (const [rejection, category] of [
      [exited(1, "p101\np102\n"), "unavailable"],
      [exited(1, undefined), "unavailable"],
      [exited(2, output), "unavailable"],
      [exited(null, output, { killed: true, signal: "SIGTERM" }), "timeout"],
      [exited(null, output, { signal: "SIGKILL" }), "unavailable"],
      [exited(1, output, { killed: true }), "unavailable"],
      [exited(1, output, { signal: "SIGTERM" }), "unavailable"],
      [
        Object.assign(failure("ERR_CHILD_PROCESS_STDIO_MAXBUFFER"), { stdout: output }),
        "data_bound",
      ],
      [Object.assign(failure("ETIMEDOUT"), { stdout: output }), "timeout"],
    ] as const) {
      const probe = await processProbe(
        target,
        "darwin",
        darwin(
          async () => {
            throw rejection;
          },
          { alive: () => false },
        ),
      );
      NodeAssert.equal(probe.status, "partial", JSON.stringify(rejection));
      has(probe, `process_probe_observation:stage=darwin_lsof,category=${category},count=1`);
      NodeAssert.deepEqual(decide(probe).unknown, ["consumer_evidence_unavailable"]);
      privateDiagnostics(probe);
    }
    // A ps exit 1 with a live PID, with output, or by timeout is not a vanished batch.
    for (const [rejection, alive] of [
      [exited(1, ""), (pid: string) => pid === "101"],
      [exited(1, "node " + target), () => false],
      [exited(null, "", { killed: true, signal: "SIGTERM" }), () => false],
    ] as const) {
      const probe = await processProbe(
        target,
        "darwin",
        darwin(async () => "p101\np102\n", {
          alive,
          argv: async () => {
            throw rejection;
          },
        }),
      );
      NodeAssert.equal(probe.status, "partial", JSON.stringify(rejection));
      NodeAssert.deepEqual(decide(probe).unknown, ["consumer_evidence_unavailable"]);
    }
  },
};
