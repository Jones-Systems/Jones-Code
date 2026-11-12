"use strict";

const test = require("node:test");
const assert = require("node:assert/strict");
const {
  serviceAdapter,
  validateSystemdConfig,
  systemdWords,
  resolveServiceUser,
  proveDarwinImage,
} = require("./native.cjs");

const p = {
  uid: 1000,
  home: "/home/operator/.t3",
  service: {
    manager: "systemd-user",
    name: "t3code.service",
    configPath: "/home/operator/.config/systemd/user/t3code.service",
    dropIns: [],
  },
};
const release = { argv: ["/opt/runtime/t3", "--no-browser"] };
const contents =
  "[Service]\nExecStart=/opt/runtime/t3 --no-browser\nEnvironment=T3CODE_HOME=/home/operator/.t3\nKillMode=control-group\n";
const properties =
  "LoadState=loaded\nActiveState=inactive\nSubState=dead\nMainPID=0\nControlPID=0\nControlGroup=/user.slice/t3code.service\nFragmentPath=/home/operator/.config/systemd/user/t3code.service\nDropInPaths=\nNeedDaemonReload=no\nKillMode=control-group\n";

test("service config binds exact argv and home without executable hooks", () => {
  assert.doesNotThrow(() => validateSystemdConfig(contents, p, release));
  assert.throws(
    () => validateSystemdConfig(contents + "ExecStartPre=/bin/true\n", p, release),
    /Unsupported/,
  );
  assert.throws(() => validateSystemdConfig(contents.replace(".t3", ".other"), p, release), /home/);
  assert.throws(
    () => validateSystemdConfig(contents.replace("control-group", "process"), p, release),
    /complete process group/,
  );
  assert.deepEqual(systemdWords('"/opt/a b/t3" "argument value"'), [
    "/opt/a b/t3",
    "argument value",
  ]);
});

test("unaccounted drop-ins and manager lookup failure never prove service stopped", async () => {
  const unknown = serviceAdapter(p, async () => {
    throw new Error("manager unavailable");
  });
  await assert.rejects(unknown.observe(), /unavailable/);
  const drift = serviceAdapter(p, async () => ({
    stdout: properties.replace("DropInPaths=", "DropInPaths=/tmp/override.conf"),
    code: 0,
  }));
  await assert.rejects(drift.observe(), /unaccounted drop-ins/);
});

test("native manager mutation uses fixed commands and explicit elevation only", async () => {
  const commands = [];
  const run = async (file, args) => {
    commands.push([file, args]);
    return { stdout: properties, code: 0 };
  };
  const service = serviceAdapter(p, run);
  assert.equal((await service.observe()).stopped, true);
  await service.stop();
  await service.reload();
  await service.start();
  assert.deepEqual(commands.slice(1), [
    ["/usr/bin/systemctl", ["--user", "stop", "t3code.service"]],
    ["/usr/bin/systemctl", ["--user", "daemon-reload"]],
    ["/usr/bin/systemctl", ["--user", "start", "t3code.service"]],
  ]);
  const elevated = serviceAdapter(
    {
      ...p,
      service: { ...p.service, manager: "systemd-system", elevation: "sudo-noninteractive" },
    },
    run,
  );
  await elevated.stop();
  assert.deepEqual(commands.at(-1), [
    "/usr/bin/sudo",
    ["-n", "/usr/bin/systemctl", "stop", "t3code.service"],
  ]);
});

test("launchd bootout readback distinguishes absent service from manager failure", async () => {
  const plan = {
    ...p,
    service: {
      manager: "launchd",
      name: "com.example.t3",
      domain: "gui/1000",
      configPath: "/tmp/service.plist",
      dropIns: [],
    },
  };
  const absent = serviceAdapter(plan, async () => ({
    code: 113,
    stdout: "",
    stderr: 'Could not find service "com.example.t3" in domain',
  }));
  assert.equal((await absent.observe()).stopped, true);
  const unknown = serviceAdapter(plan, async () => ({
    code: 113,
    stdout: "",
    stderr: "Could not access domain",
  }));
  await assert.rejects(unknown.observe(), /unknown/);
});

test("ExecStart reset and environment replacement follow ordered systemd override semantics", () => {
  const original = contents
    .replace("/opt/runtime/t3", "/opt/old/t3")
    .replace("/home/operator/.t3", "/old/home");
  const override =
    "[Service]\nExecStart=\nExecStart=/opt/runtime/t3 --no-browser\nEnvironment=T3CODE_HOME=/home/operator/.t3\n";
  assert.doesNotThrow(() => validateSystemdConfig(original + override, p, release));
  assert.throws(
    () => validateSystemdConfig(original + override.replace("ExecStart=\n", ""), p, release),
    /Exactly one ExecStart/,
  );
  assert.throws(
    () =>
      validateSystemdConfig(
        contents + "[Service]\nEnvironment=\nEnvironment=OTHER=value\n",
        p,
        release,
      ),
    /home/,
  );
});

test("named service User must resolve to the exact requested uid", async () => {
  const calls = [];
  await resolveServiceUser("fixture-operator", 1000, async (file, args) => {
    calls.push([file, args]);
    return { stdout: "1000\n" };
  });
  assert.deepEqual(calls, [["/usr/bin/id", ["-u", "fixture-operator"]]]);
  await assert.rejects(
    resolveServiceUser("fixture-operator", 1000, async () => ({ stdout: "1001\n" })),
    /different uid/,
  );
  await assert.rejects(resolveServiceUser("-u", 1000), /Invalid named/);
});

test("Darwin mapped executable proof rejects a replaced inode even when the pathname matches", () => {
  const expected = { path: "/opt/t3", inode: 123n, device: 16777234n };
  const output =
    "p42\nftxt\nD0x1000012\ni123\nn/opt/t3\nftxt\nD0x1000012\ni456\nn/opt/library.dylib\n";
  assert.doesNotThrow(() => proveDarwinImage(output, expected));
  assert.throws(
    () => proveDarwinImage(output.replace("i123\n", "i999\n"), expected),
    /inode\/device/,
  );
  assert.throws(
    () => proveDarwinImage(output.replace("D0x1000012", "D0x1000013"), expected),
    /inode\/device/,
  );
});
