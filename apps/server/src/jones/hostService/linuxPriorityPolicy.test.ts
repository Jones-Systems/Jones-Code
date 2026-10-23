import { expect, it } from "vite-plus/test";
import {
  assertLinuxPriorityPolicy,
  assertNoLaterLinuxPriorityOverride,
  LINUX_PRIORITY_POLICY,
} from "./linuxPriorityPolicy.ts";

const policy = {
  uid: 0,
  gid: 0,
  mode: 0o100644,
  bytes: new TextEncoder().encode(LINUX_PRIORITY_POLICY),
};

it("accepts the exact root-owned policy with effective Nice=-15", () => {
  expect(() => assertLinuxPriorityPolicy(policy, "-15")).not.toThrow();
});

it.each([
  ["non-root owner", { uid: 1000 }, "-15"],
  ["non-root group", { gid: 1000 }, "-15"],
  ["writable mode", { mode: 0o100664 }, "-15"],
  ["symlink", { mode: 0o120644 }, "-15"],
  [
    "extra directive",
    { bytes: new TextEncoder().encode(`${LINUX_PRIORITY_POLICY}CPUWeight=1000\n`) },
    "-15",
  ],
  ["byte order mark", { bytes: new TextEncoder().encode(`\uFEFF${LINUX_PRIORITY_POLICY}`) }, "-15"],
  ["priority drift", {}, "0"],
  ["unknown effective priority", {}, ""],
] as const)("refuses %s", (_name, change, nice) => {
  expect(() => assertLinuxPriorityPolicy({ ...policy, ...change }, nice)).toThrow(
    /external administrator/,
  );
});

it("refuses later Nice directives even when their value matches", () => {
  expect(() =>
    assertNoLaterLinuxPriorityOverride(
      "jones-code.conf",
      new TextEncoder().encode("[Service]\nNice=-15\n"),
    ),
  ).toThrow(/later drop-in/);
  expect(() =>
    assertNoLaterLinuxPriorityOverride(
      "jones-code.conf",
      new TextEncoder().encode("[Service]\nEnvironment=FIXTURE=1\n"),
    ),
  ).not.toThrow();
});
