import { expect, it } from "vite-plus/test";
import {
  launcherHandoffProcessesQuiet,
  type LauncherHandoffProcessIO,
} from "./launcherHandoffProcesses.ts";

const program = "/service/runtime/launcher/t3-launcher";
const linux = (changes: Record<string, string> = {}): LauncherHandoffProcessIO => ({
  previousService: async () => "",
  managerSnapshot: async () =>
    "MainPID=10\nControlGroup=/user.slice/jones.service\nExecStart={ path=" +
    program +
    " ; argv[]=" +
    program +
    " __service-launcher ; }\n",
  platform: "linux",
  uid: 1000,
  readText: async (file) => {
    const values: Record<string, string> = {
      "/proc/10/cgroup": "0::/user.slice/jones.service\n",
      "/proc/11/cgroup": "0::/user.slice/jones.service\n",
      "/sys/fs/cgroup/user.slice/jones.service/cgroup.stat":
        "nr_descendants 0\nnr_dying_descendants 0\n",
      "/sys/fs/cgroup/user.slice/jones.service/cgroup.procs": "10\n11\n",
      ...changes,
    };
    if (values[file] === undefined) throw new Error("Unavailable synthetic inventory");
    return values[file];
  },
  darwinSnapshot: async () => {
    throw new Error("Not selected");
  },
});

it("admits only the two exact Linux manager members and refuses nested or unavailable groups", async () => {
  expect(await launcherHandoffProcessesQuiet(10, 11, program, linux())).toBe(true);
  for (const changes of [
    { "/sys/fs/cgroup/user.slice/jones.service/cgroup.procs": "10\n11\n12\n" },
    {
      "/sys/fs/cgroup/user.slice/jones.service/cgroup.stat":
        "nr_descendants 1\nnr_dying_descendants 0\n",
    },
    { "/sys/fs/cgroup/user.slice/jones.service/cgroup.stat": "" },
    { "/proc/11/cgroup": "0::/other.service\n" },
    { "/proc/10/cgroup": "0::/\n" },
    { "/proc/10/cgroup": "0::/user.slice/../outside\n" },
  ])
    expect(await launcherHandoffProcessesQuiet(10, 11, program, linux(changes))).toBe(false);
});

it("does not treat malformed Linux membership or a read failure as quiet", async () => {
  for (const contents of ["", "10\n11\ninvalid\n", "10\n", "10\n11\n1e3\n"])
    expect(
      await launcherHandoffProcessesQuiet(
        10,
        11,
        program,
        linux({
          "/sys/fs/cgroup/user.slice/jones.service/cgroup.procs": contents,
        }),
      ),
    ).toBe(false);
  const io = {
    ...linux(),
    readText: async () => {
      throw new Error("Unavailable");
    },
  };
  expect(await launcherHandoffProcessesQuiet(10, 11, program, io)).toBe(false);
});

const darwin = (text: string, exitedInventoryPid?: number): LauncherHandoffProcessIO => ({
  previousService: async () =>
    JSON.stringify({ unitContents: "<key>Label</key><string>com.jones.code</string>" }),
  managerSnapshot: async () => "pid = 10\nprogram = " + program + "\n",
  platform: "darwin",
  uid: 1000,
  readText: async () => {
    throw new Error("Not selected");
  },
  darwinSnapshot: async () => ({
    text,
    ...(exitedInventoryPid === undefined ? {} : { exitedInventoryPid }),
  }),
});
const pair = "10 1 10 1000\n11 10 10 1000\n";

it("admits the exact Darwin job group, excluding only its captured already-exited inventory child", async () => {
  expect(await launcherHandoffProcessesQuiet(10, 11, program, darwin(pair))).toBe(true);
  expect(
    await launcherHandoffProcessesQuiet(10, 11, program, darwin(pair + "12 10 10 1000\n", 12)),
  ).toBe(true);
  for (const text of [
    pair + "12 10 10 1000\n",
    pair + "12 11 12 1000\n",
    pair + "12 1 10 1000\n",
    "10 1 9 1000\n11 10 9 1000\n",
    "10 1 10 1000\n11 10 10 1001\n",
    pair + "bad\n",
    pair + "11 10 10 1000\n",
  ])
    expect(await launcherHandoffProcessesQuiet(10, 11, program, darwin(text))).toBe(false);
  expect(
    await launcherHandoffProcessesQuiet(10, 11, program, darwin(pair + "12 1 10 1000\n", 12)),
  ).toBe(false);
  expect(
    await launcherHandoffProcessesQuiet(10, 11, program, { ...linux(), platform: "unknown" }),
  ).toBe(false);
});

it("refuses a stale manager program or PID even when its process membership appears quiet", async () => {
  for (const io of [linux(), darwin(pair)]) {
    for (const managerSnapshot of [
      async () =>
        "MainPID=10\nControlGroup=/user.slice/jones.service\nExecStart={ path=/old/t3 ; }\n",
      async () => "pid = 10\nprogram = /old/t3\n",
      async () => "pid = 999\nprogram = " + program + "\n",
    ])
      expect(await launcherHandoffProcessesQuiet(10, 11, program, { ...io, managerSnapshot })).toBe(
        false,
      );
  }
});
