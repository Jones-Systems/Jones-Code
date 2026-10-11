// @effect-diagnostics nodeBuiltinImport:off
import * as NodeChildProcess from "node:child_process";
import * as NodeFSP from "node:fs/promises";
import * as NodeOS from "node:os";
import * as NodePath from "node:path";
import { expect, it } from "vite-plus/test";
import {
  QUALIFIED_WRITER_OBSERVER_PATH,
  QUALIFIED_WRITER_OBSERVER_SHA256,
} from "../cloud/qualifiedWriterObserver.ts";
import {
  OWNED_SYSTEM_DROPIN,
  parseRetainedServeCommand,
  parseSystemControlResult,
  renderOwnedSystemDropin,
  renderSystemBootstrapManifest,
  renderSystemServicePolicy,
  runSystemServiceControl,
  sha256,
  SYSTEM_SERVICE_CONTROL_PATH,
  SYSTEM_SERVICE_CONTROL_SHA256,
  SYSTEM_SERVICE_POLICY_PATH,
  systemControlArguments,
  systemUnitDigest,
  systemUnitRecord,
} from "./systemServiceControl.ts";

const worker = NodePath.join(import.meta.dirname, "systemServiceControl.pl");
const header = "jones-system-service-control 1";
const unit = "host-tooling-t3.service";
const retained = "/opt/retained/t3";
const launcherVersion = "0.0.46-preview.20261011.1";
const account = NodeOS.userInfo();
const controlGroup = "/hostdev.slice/host-tooling-t3.service";

function perl(args: readonly string[], env: Record<string, string>) {
  return new Promise<{ code: number | null; stdout: string; stderr: string }>((resolve) => {
    NodeChildProcess.execFile(
      "/usr/bin/perl",
      args,
      { timeout: 30_000, env },
      (error, stdout, stderr) =>
        resolve({
          code: error === null ? 0 : typeof error.code === "number" ? error.code : null,
          stdout,
          stderr,
        }),
    );
  });
}

// Loads the worker without its entry point, points its fixed roots at task
// scratch, accepts the test user as the owner of root files, and replaces only
// systemctl: `show` returns the scripted state, and a mutation may switch it.
const harness = `my ($worker, $state, $policy, $systemd, $archive, $proc, $cgroup, @operation) = @ARGV;
do $worker or die($@ || $!);
no warnings "redefine";
$main::POLICY_PATH = $policy;
$main::SYSTEMD_DIRECTORY = $systemd;
$main::ARCHIVE_DIRECTORY = $archive;
$main::PROC = $proc;
$main::CGROUP_ROOT = $cgroup;
$main::ROOT_UID = $<;
*main::trusted_self = sub { 1 };
*main::trusted_chain = sub { 1 };
*main::systemctl = sub {
  my @args = @_;
  open(my $log, ">>", "$state/calls") or die;
  print {$log} join(" ", @args), "\\n";
  close($log);
  if ($args[0] eq "show") {
    open(my $show, "<", "$state/show") or die;
    local $/;
    return (1, scalar <$show>);
  }
  return (0, "") if -e "$state/fail-$args[0]";
  rename("$state/after-$args[0]", "$state/show") if -e "$state/after-$args[0]";
  return (1, "");
};
# Only install-launcher's second scan names its stage. A scripted action then
# lands between the two scans: a foreign entry, or another inode at the stage.
my $preimage = \\&main::unit_preimage;
*main::unit_preimage = sub {
  my ($policy, $staged) = @_;
  if (defined $staged && open(my $inject, "<", "$state/inject")) {
    my $action = <$inject>;
    close($inject);
    unlink("$state/inject");
    chomp $action;
    my $directory = "$main::SYSTEMD_DIRECTORY/$policy->{unit}.d";
    my $target = $action eq "replace-stage" ? "$directory/$staged->{name}" : "$directory/$action";
    # Written aside and renamed in, so the replacement cannot reuse the stage's inode.
    open(my $file, ">", "$target.next") or die;
    print {$file} "[Service]\\n";
    close($file);
    chmod(0644, "$target.next");
    rename("$target.next", $target) or die;
  }
  return $preimage->(@_);
};
@ARGV = @operation;
run();`;

/** Hidden drop-in entries; in these fixtures only worker staging files are named so. */
const staged = async (directory: string) =>
  (await NodeFSP.readdir(directory)).filter((name) => name.startsWith(".")).sort();

interface ShowState {
  readonly dropins?: readonly string[];
  readonly active?: boolean;
  readonly exec?: string;
  readonly user?: string;
  readonly nnp?: string;
  readonly slice?: string;
  readonly workingDirectory?: string;
}

async function fixture(body: (f: Awaited<ReturnType<typeof createFixture>>) => Promise<void>) {
  const f = await createFixture();
  try {
    await body(f);
  } finally {
    await NodeFSP.rm(f.root, { recursive: true, force: true });
  }
}

async function createFixture() {
  const root = await NodeFSP.realpath(
    await NodeFSP.mkdtemp(NodePath.join(NodeOS.tmpdir(), "jones-system-control-test-")),
  );
  const directory = async (path: string) => {
    await NodeFSP.mkdir(path, { recursive: true });
    await NodeFSP.chmod(path, 0o755);
  };
  const file = async (path: string, text: string) => {
    await NodeFSP.writeFile(path, text);
    await NodeFSP.chmod(path, 0o644);
  };
  const home = NodePath.join(root, "home");
  const base = NodePath.join(home, ".t3");
  const state = NodePath.join(root, "state");
  const systemd = NodePath.join(root, "systemd");
  const dropins = NodePath.join(systemd, `${unit}.d`);
  const proc = NodePath.join(root, "proc");
  const cgroup = NodePath.join(root, "cgroup");
  const archive = NodePath.join(root, "var", "jones-code", "archive");
  for (const path of [
    base,
    state,
    dropins,
    NodePath.join(proc, "4242"),
    NodePath.join(cgroup, controlGroup),
  ])
    await directory(path);
  await directory(NodePath.join(root, "var"));
  const launcherEntry = NodePath.join(base, "runtime", "versions", launcherVersion, "t3");
  await directory(NodePath.dirname(launcherEntry));
  await file(launcherEntry, "#!/bin/sh\n");
  const policy = NodePath.join(root, "system-service.policy");
  const policyText = renderSystemServicePolicy({
    unit,
    user: account.username,
    uid: account.uid,
    gid: account.gid,
    base,
    slice: "hostdev.slice",
    retainedEntry: retained,
  });
  await file(policy, policyText);
  const fragment = NodePath.join(systemd, unit);
  await file(
    fragment,
    `[Unit]\nDescription=Host tooling T3\n\n[Service]\nUser=${account.username}\nNoNewPrivileges=yes\nExecStart=/opt/older/t3 serve\n`,
  );
  const serveCommand = `${retained} serve --host 127.0.0.1 --port 3773 --base-dir ${base}`;
  await file(
    NodePath.join(dropins, "jones-code.conf"),
    `[Service]\nExecStart=\nExecStart=${serveCommand}\nEnvironment=PRIVATE_TOKEN=do-not-print\n`,
  );
  await file(
    NodePath.join(proc, "4242", "stat"),
    `4242 (t3) S ${Array(18).fill("0").join(" ")} 777 0`,
  );
  await file(
    NodePath.join(proc, "4242", "status"),
    `Name:\tt3\nUid:\t${account.uid}\t${account.uid}\t${account.uid}\t${account.uid}\nGid:\t0\n`,
  );
  await file(NodePath.join(proc, "4242", "cgroup"), `0::${controlGroup}\n`);
  await file(NodePath.join(cgroup, controlGroup, "cgroup.procs"), "");
  const show = (input: ShowState = {}) =>
    [
      `FragmentPath=${fragment}`,
      `DropInPaths=${(input.dropins ?? ["jones-code.conf"]).map((name) => NodePath.join(dropins, name)).join(" ")}`,
      `User=${input.user ?? account.username}`,
      "Group=",
      `NoNewPrivileges=${input.nnp ?? "yes"}`,
      "KillMode=control-group",
      `Slice=${input.slice ?? "hostdev.slice"}`,
      `ControlGroup=${input.active === false ? "" : controlGroup}`,
      `ActiveState=${input.active === false ? "inactive" : "active"}`,
      `SubState=${input.active === false ? "dead" : "running"}`,
      `MainPID=${input.active === false ? 0 : 4242}`,
      `ExecStart={ path=${retained} ; argv[]=${input.exec ?? serveCommand} ; ignore_errors=no ; start_time=[n/a] }`,
      `WorkingDirectory=${input.workingDirectory ?? home}`,
      "",
    ].join("\n");
  const setShow = (input?: ShowState, after?: string) =>
    file(NodePath.join(state, after === undefined ? "show" : `after-${after}`), show(input));
  await setShow();
  const run = async (
    operation: readonly string[],
    env: Record<string, string> = { SUDO_UID: String(account.uid) },
  ) => {
    const result = await perl(
      ["-e", harness, worker, state, policy, systemd, archive, proc, cgroup, ...operation],
      env,
    );
    const calls = await NodeFSP.readFile(NodePath.join(state, "calls"), "utf8").catch(() => "");
    await NodeFSP.rm(NodePath.join(state, "calls"), { force: true });
    return { ...result, calls: calls.split("\n").filter((line) => line !== "") };
  };
  const record = async (label: string, path: string) =>
    systemUnitRecord(label, await NodeFSP.lstat(path), await NodeFSP.readFile(path));
  const digest = async () =>
    systemUnitDigest([
      await record("fragment", fragment),
      await record("dropin jones-code.conf", NodePath.join(dropins, "jones-code.conf")),
    ]);
  return {
    root,
    home,
    base,
    state,
    policy,
    policyText,
    fragment,
    dropins,
    archive,
    cgroup,
    launcherEntry,
    serveCommand,
    file,
    setShow,
    run,
    digest,
  };
}

// A worker run needs a non-root service user with a resolvable account.
const runnable = (account.uid ?? 0) > 0;

it.skipIf(!runnable)("inspects digests and metadata without emitting unit contents", () =>
  fixture(async (f) => {
    const result = await f.run(["inspect"]);
    expect(result.code).toBe(0);
    const parsed = parseSystemControlResult({ outcome: "exited", signal: null, ...result });
    expect(parsed).toMatchObject({
      status: "complete",
      inspection: {
        policySha256: sha256(f.policyText),
        unitDigest: await f.digest(),
        owned: "absent",
        activeState: "active",
        mainPid: 4242,
        mainBirth: "777",
        controlGroup,
        killMode: "control-group",
        exec: "retained",
        serve: { port: 3773, host: "127.0.0.1" },
      },
    });
    for (const secret of ["do-not-print", "argv", "serve --host", "PRIVATE_TOKEN"])
      expect(result.stdout).not.toContain(secret);
    expect(result.calls).toEqual([expect.stringMatching(/^show host-tooling-t3\.service /)]);
  }),
);

it.skipIf(!runnable)("refuses every request outside the closed grammar before systemctl", () =>
  fixture(async (f) => {
    const digest = await f.digest();
    for (const operation of [
      [],
      ["restart"],
      ["inspect", "--unit", "other.service"],
      ["stop", "--expect-unit", digest],
      [
        "stop",
        "--expect-owned",
        "absent",
        "--expect-unit",
        digest,
        "--expect-main-pid",
        "4242",
        "--expect-main-birth",
        "777",
      ],
      [
        "stop",
        "--expect-unit",
        digest,
        "--expect-owned",
        "absent",
        "--expect-main-pid",
        "0",
        "--expect-main-birth",
        "777",
      ],
      ["start", "--expect-unit", "A".repeat(64), "--expect-owned", "absent"],
      ["start", "--expect-unit", digest, "--expect-owned", "absent", "--extra", "x"],
      ["archive-launcher", "--expect-unit", digest, "--expect-owned", "absent"],
      [
        "install-launcher",
        "--expect-unit",
        digest,
        "--expect-owned",
        "absent",
        "--launcher-version",
        "latest",
      ],
      [
        "install-launcher",
        "--expect-unit",
        digest,
        "--expect-owned",
        "absent",
        "--launcher-version",
        `${launcherVersion}/../x`,
      ],
    ]) {
      const result = await f.run(operation);
      expect(result, operation.join(" ")).toMatchObject({
        code: 2,
        stdout: `${header}\nrejected arguments\n`,
        calls: [],
      });
    }
    for (const env of [{}, { SUDO_UID: "0" }, { SUDO_UID: String(account.uid + 1) }])
      expect((await f.run(["inspect"], env)).stdout).toBe(`${header}\nrejected caller\n`);
  }),
);

it.skipIf(!runnable)("accepts only the exact root policy grammar", () =>
  fixture(async (f) => {
    const valid = f.policyText;
    for (const text of [
      valid.replace("\n", "\nunit other.service\n"),
      valid.replace(/^user .*$/m, "user root"),
      valid.replace(/^base (.*)$/m, "base $1/../x"),
      valid.replace(/^slice .*$/m, "slice hostdev"),
      valid.replace(/^gid .*$/m, `gid ${account.gid + 1}`),
      valid.slice(0, -1),
      `${valid}extra value\n`,
      valid.replace(/^unit (.*)$/m, "unit host-%i.service"),
      valid.replace("jones-system-service-policy 1", "jones-system-service-policy 2"),
    ]) {
      await f.file(f.policy, text);
      expect((await f.run(["inspect"])).stdout, text).toMatch(
        new RegExp(`^${header}\\nrejected policy(-user)?\\n$`),
      );
    }
  }),
);

it.skipIf(!runnable)("binds only the retained serve command and the unit's own files", () =>
  fixture(async (f) => {
    const conf = NodePath.join(f.dropins, "jones-code.conf");
    const original = await NodeFSP.readFile(conf, "utf8");
    for (const [text, reason] of [
      [original.replace("ExecStart=/opt", "ExecStart=-/opt"), "unit-exec"],
      [original.replace("--port 3773", '--port "3773"'), "unit-exec"],
      [original.replace("--port 3773", "--port 3773 --auth-token x"), "unit-exec"],
      [original.replace("--port 3773", "--port %i"), "unit-exec"],
      [original.replace("ExecStart=\n", ""), "unit-exec"],
      [original.replace("--base-dir ", "--base-dir /other"), "unit-exec"],
      [`${original}ExecStartPre=/bin/true \\\n`, "unit-grammar"],
    ] as const) {
      await f.file(conf, text);
      expect((await f.run(["inspect"])).stdout, text).toBe(`${header}\nrejected ${reason}\n`);
    }
    await f.file(conf, original);
    await f.file(NodePath.join(f.dropins, "zzzzz-late.conf"), "[Service]\n");
    expect((await f.run(["inspect"])).stdout).toBe(`${header}\nrejected unit-precedence\n`);
    await NodeFSP.rm(NodePath.join(f.dropins, "zzzzz-late.conf"));
    await NodeFSP.chmod(conf, 0o664);
    expect((await f.run(["inspect"])).stdout).toBe(`${header}\nrejected unit-trust\n`);
    await NodeFSP.chmod(conf, 0o644);
    await NodeFSP.symlink(conf, NodePath.join(f.dropins, "linked.conf"));
    expect((await f.run(["inspect"])).stdout).toBe(`${header}\nrejected unit-trust\n`);
    await NodeFSP.rm(NodePath.join(f.dropins, "linked.conf"));
    for (const [state, reason] of [
      [{ user: "root" }, "unit-user"],
      [{ nnp: "no" }, "unit-hardening"],
      [{ slice: "system.slice" }, "unit-slice"],
      [{ dropins: ["jones-code.conf", "../../run/override.conf"] }, "unit-dropin-path"],
    ] as const) {
      await f.setShow(state);
      expect((await f.run(["inspect"])).stdout).toBe(`${header}\nrejected ${reason}\n`);
    }
  }),
);

it.skipIf(!runnable)(
  "binds the historical start form to the unit's WorkingDirectory in both parsers",
  () =>
    fixture(async (f) => {
      const conf = NodePath.join(f.dropins, "jones-code.conf");
      const parse = (result: Awaited<ReturnType<typeof f.run>>) =>
        parseSystemControlResult({ outcome: "exited", signal: null, ...result });
      const flags = `--mode web --host 127.0.0.1 --port 3773 --base-dir ${f.base}`;
      const start = `${retained} start ${flags} --no-browser ${f.home}`;
      const unitRunning = (command: string) =>
        f.file(
          conf,
          `[Service]\nExecStart=\nExecStart=${command}\nEnvironment=PRIVATE_TOKEN=do-not-print\n`,
        );
      // The TypeScript plan and the root worker judge the same command and directory.
      const judge = async (command: string, workingDirectory = f.home) => {
        await unitRunning(command);
        await f.setShow({ exec: command, workingDirectory });
        const worker = await f.run(["inspect"]);
        return {
          parser: parseRetainedServeCommand(command, retained, f.base, workingDirectory),
          worker: parse(worker),
          stdout: worker.stdout,
        };
      };

      const accepted = await judge(start);
      expect(accepted.parser).toEqual({
        port: 3773,
        host: "127.0.0.1",
        mode: "web",
        workingDirectory: f.home,
      });
      expect(accepted.worker).toMatchObject({
        status: "complete",
        inspection: { exec: "retained", serve: { port: 3773, host: "127.0.0.1", mode: "web" } },
      });
      for (const secret of [f.home, "no-browser", "do-not-print"])
        expect(accepted.stdout).not.toContain(secret);

      for (const command of [
        `${retained} start ${flags} --no-browser`,
        `${retained} start ${flags} --no-browser ${f.home} ${f.home}`,
        `${retained} start ${flags} ${f.home} --no-browser`,
        `${retained} start ${flags} ${f.home}`,
        `${retained} start ${flags} --no-browser ${f.home}/.`,
        `${retained} start ${flags} --no-browser ${f.home}/../home`,
        `${retained} start ${flags} --no-browser ${f.home}/`,
        `${retained} start ${flags} --no-browser home`,
        `${retained} start ${flags} --auth-token x --no-browser ${f.home}`,
        `${retained} start ${flags} --port 3773 --no-browser ${f.home}`,
        `${retained} start ${flags} --no-browser --no-browser ${f.home}`,
        `${retained} start ${flags} --no-browser true ${f.home}`,
        `${retained} start ${flags} --no-browser=true ${f.home}`,
        `${retained} start ${flags.replace("--mode web", "--mode desktop")} --no-browser ${f.home}`,
        `${retained} start ${flags.replace("--mode web ", "")} --no-browser ${f.home}`,
        `${retained} start ${flags.replace(f.base, "/elsewhere")} --no-browser ${f.home}`,
        `${retained} serve ${flags} --no-browser ${f.home}`,
        `/other/t3 start ${flags} --no-browser ${f.home}`,
      ]) {
        const refused = await judge(command);
        expect(refused.parser, command).toBeUndefined();
        expect(refused.stdout, command).toBe(`${header}\nrejected unit-exec\n`);
      }
      for (const workingDirectory of ["", `!${f.home}`, f.base, NodePath.join(f.root, "other")]) {
        const refused = await judge(start, workingDirectory);
        expect(refused.parser, workingDirectory).toBeUndefined();
        expect(refused.stdout, workingDirectory).toBe(
          `${header}\nrejected unit-working-directory\n`,
        );
      }
      // The serve grammar names no directory, so WorkingDirectory stays unbound for it.
      const serve = await judge(f.serveCommand, "");
      expect(serve.parser).toEqual({ port: 3773, host: "127.0.0.1" });
      expect(serve.worker).toMatchObject({ status: "complete", inspection: { exec: "retained" } });

      // Every later readback keeps the directory bound: the launcher inherits it.
      await unitRunning(start);
      const digest = await f.digest();
      const owned = NodePath.join(f.dropins, OWNED_SYSTEM_DROPIN);
      const expected = renderOwnedSystemDropin({
        base: f.base,
        launcherVersion,
        serve: { port: 3773, host: "127.0.0.1", mode: "web" },
      });
      const launcherExec = `${f.launcherEntry} __service-launcher`;
      const withOwned = ["jones-code.conf", OWNED_SYSTEM_DROPIN];
      const install = (expectOwned: string) => [
        "install-launcher",
        "--expect-unit",
        digest,
        "--expect-owned",
        expectOwned,
        "--launcher-version",
        launcherVersion,
      ];
      await f.setShow({ active: false, exec: start });
      await f.setShow(
        { active: false, dropins: withOwned, exec: launcherExec, workingDirectory: f.base },
        "daemon-reload",
      );
      expect(await f.run(install("absent"))).toMatchObject({
        code: 3,
        stdout: expect.stringMatching(/\nunknown unit-working-directory\n$/),
      });
      expect(await NodeFSP.readFile(owned, "utf8")).toBe(expected);
      await f.setShow({ active: false, dropins: withOwned, exec: launcherExec });
      await f.setShow({ active: false, dropins: withOwned, exec: launcherExec }, "daemon-reload");
      expect(parse(await f.run(install(sha256(expected))))).toMatchObject({
        status: "complete",
        inspection: {
          owned: sha256(expected),
          unitDigest: digest,
          exec: `launcher ${launcherVersion}`,
          serve: { port: 3773, host: "127.0.0.1", mode: "web" },
        },
      });
      expect(expected).toContain("Environment=T3CODE_MODE=web\n");
      expect(expected).not.toMatch(/WorkingDirectory|no-browser/);

      await f.setShow({ active: false, exec: start }, "daemon-reload");
      const archived = await f.run([
        "archive-launcher",
        "--expect-unit",
        digest,
        "--expect-owned",
        sha256(expected),
      ]);
      expect(parse(archived)).toMatchObject({
        status: "complete",
        inspection: { owned: "absent", exec: "retained" },
      });
      expect(await NodeFSP.readFile(conf, "utf8")).toContain(`ExecStart=${start}\n`);
    }),
);

it.skipIf(!runnable)(
  "stops only the bound main process and reports an uncertain stop as unknown",
  () =>
    fixture(async (f) => {
      const digest = await f.digest();
      const stop = (pid = "4242", birth = "777", unitDigest = digest) => [
        "stop",
        "--expect-unit",
        unitDigest,
        "--expect-owned",
        "absent",
        "--expect-main-pid",
        pid,
        "--expect-main-birth",
        birth,
      ];
      expect((await f.run(stop("4242", "777", "0".repeat(64)))).stdout).toBe(
        `${header}\nrejected unit-changed\n`,
      );
      for (const [pid, birth] of [
        ["4243", "777"],
        ["4242", "778"],
      ]) {
        const refused = await f.run(stop(pid, birth));
        expect(refused.stdout).toBe(`${header}\nrejected main-process\n`);
        expect(refused.calls.some((call) => call.startsWith("stop"))).toBe(false);
      }
      await f.file(NodePath.join(f.state, "fail-stop"), "");
      expect(await f.run(stop())).toMatchObject({
        code: 3,
        stdout: expect.stringMatching(/\nunknown stop\n$/),
      });
      await NodeFSP.rm(NodePath.join(f.state, "fail-stop"));
      // The unit stays active after the stop job: the effect is unknown.
      expect((await f.run(stop())).stdout).toMatch(/\nunknown stop-readback\n$/);
      await f.setShow({ active: false }, "stop");
      await f.file(NodePath.join(f.cgroup, controlGroup, "cgroup.procs"), "4242\n");
      expect((await f.run(stop())).stdout).toMatch(/\nunknown stop-readback\n$/);
      await f.setShow();
      await f.setShow({ active: false }, "stop");
      await f.file(NodePath.join(f.cgroup, controlGroup, "cgroup.procs"), "");
      const stopped = await f.run(stop());
      expect(stopped.calls).toContain(`stop ${unit}`);
      expect(
        parseSystemControlResult({ outcome: "exited", signal: null, ...stopped }),
      ).toMatchObject({
        status: "complete",
        inspection: { activeState: "inactive", mainPid: 0, unitDigest: digest },
      });
    }),
);

it.skipIf(!runnable)("writes, reloads and archives only the owned launcher drop-in", () =>
  fixture(async (f) => {
    const digest = await f.digest();
    const conf = NodePath.join(f.dropins, "jones-code.conf");
    const preserved = [await NodeFSP.readFile(f.fragment), await NodeFSP.readFile(conf)];
    const install = (owned = "absent") => [
      "install-launcher",
      "--expect-unit",
      digest,
      "--expect-owned",
      owned,
      "--launcher-version",
      launcherVersion,
    ];
    expect((await f.run(install())).stdout).toBe(`${header}\nrejected unit-active\n`);
    await f.setShow({ active: false });
    await NodeFSP.rm(f.launcherEntry);
    expect((await f.run(install())).stdout).toBe(`${header}\nrejected launcher-entry\n`);
    await f.file(f.launcherEntry, "#!/bin/sh\n");

    const expected = renderOwnedSystemDropin({
      base: f.base,
      launcherVersion,
      serve: { port: 3773, host: "127.0.0.1" },
    });
    const owned = NodePath.join(f.dropins, OWNED_SYSTEM_DROPIN);
    const launcherExec = `${f.launcherEntry} __service-launcher`;
    await f.file(NodePath.join(f.state, "fail-daemon-reload"), "");
    const uncertain = await f.run(install());
    expect(uncertain).toMatchObject({
      code: 3,
      stdout: expect.stringMatching(/\nunknown daemon-reload\n$/),
    });
    // The written evidence stays in place for reconciliation; no stage remains.
    expect(await NodeFSP.readFile(owned, "utf8")).toBe(expected);
    expect(await staged(f.dropins)).toEqual([]);
    await NodeFSP.rm(NodePath.join(f.state, "fail-daemon-reload"));
    expect((await f.run(install())).stdout).toBe(`${header}\nrejected owned-changed\n`);
    await NodeFSP.rm(owned);

    await f.setShow(
      { active: false, dropins: ["jones-code.conf", OWNED_SYSTEM_DROPIN], exec: launcherExec },
      "daemon-reload",
    );
    const installed = await f.run(install());
    expect(installed.calls).toContain("daemon-reload");
    expect(
      parseSystemControlResult({ outcome: "exited", signal: null, ...installed }),
    ).toMatchObject({
      status: "complete",
      inspection: {
        owned: sha256(expected),
        unitDigest: digest,
        exec: `launcher ${launcherVersion}`,
      },
    });
    expect(await NodeFSP.readFile(owned, "utf8")).toBe(expected);
    expect(await staged(f.dropins)).toEqual([]);
    expect([await NodeFSP.readFile(f.fragment), await NodeFSP.readFile(conf)]).toEqual(preserved);
    for (const line of expected.split("\n").filter((value) => value.includes("=")))
      expect(line).toMatch(/^(?:ExecStart=.*|Environment=T3CODE_(?:HOME|PORT|HOST|MODE)=.*)$/);

    await f.setShow({ active: false, exec: f.serveCommand }, "daemon-reload");
    const archived = await f.run([
      "archive-launcher",
      "--expect-unit",
      digest,
      "--expect-owned",
      sha256(expected),
    ]);
    expect(archived.stdout).toMatch(/\ncomplete\n$/);
    await expect(NodeFSP.lstat(owned)).rejects.toMatchObject({ code: "ENOENT" });
    const kept = await NodeFSP.readdir(f.archive);
    expect(kept).toEqual([expect.stringMatching(new RegExp(`^${sha256(expected)}-\\d+\\.conf$`))]);
    expect(await NodeFSP.readFile(NodePath.join(f.archive, kept[0]!), "utf8")).toBe(expected);
  }),
);

it.skipIf(!runnable)("removes only its own unpublished stage and keeps every foreign entry", () =>
  fixture(async (f) => {
    const digest = await f.digest();
    const install = (owned = "absent") => [
      "install-launcher",
      "--expect-unit",
      digest,
      "--expect-owned",
      owned,
      "--launcher-version",
      launcherVersion,
    ];
    const inject = (action: string) => f.file(NodePath.join(f.state, "inject"), `${action}\n`);
    const owned = NodePath.join(f.dropins, OWNED_SYSTEM_DROPIN);
    await f.setShow({ active: false });

    // A foreign entry appearing between the scans is refused and kept; the
    // stage this run created is removed, so the refusal is truthful.
    await inject("notes.txt");
    expect(await f.run(install())).toMatchObject({
      code: 2,
      stdout: `${header}\nrejected unit-dropin\n`,
    });
    expect(await NodeFSP.readFile(NodePath.join(f.dropins, "notes.txt"), "utf8")).toBe(
      "[Service]\n",
    );
    expect(await staged(f.dropins)).toEqual([]);
    await expect(NodeFSP.lstat(owned)).rejects.toMatchObject({ code: "ENOENT" });
    await NodeFSP.rm(NodePath.join(f.dropins, "notes.txt"));

    // A non-owned drop-in appearing between the scans fails the second compare.
    await inject("zz-extra.conf");
    expect(await f.run(install())).toMatchObject({
      code: 2,
      stdout: `${header}\nrejected unit-changed\n`,
    });
    expect(await NodeFSP.readFile(NodePath.join(f.dropins, "zz-extra.conf"), "utf8")).toBe(
      "[Service]\n",
    );
    expect(await staged(f.dropins)).toEqual([]);
    await NodeFSP.rm(NodePath.join(f.dropins, "zz-extra.conf"));

    // Another inode at the stage path is neither skipped nor removed: the
    // result is unknown and that file stays as evidence.
    await inject("replace-stage");
    expect(await f.run(install())).toMatchObject({
      code: 3,
      stdout: expect.stringMatching(/\nunknown created-cleanup\n$/),
    });
    const left = await staged(f.dropins);
    expect(left).toEqual([expect.stringMatching(/^\.zzzz-jones-qualified-launcher\.conf\.\d+$/)]);
    expect(await NodeFSP.readFile(NodePath.join(f.dropins, left[0]!), "utf8")).toBe("[Service]\n");
    // A stage left behind is refused by every later scan and preserved.
    expect((await f.run(["inspect"])).stdout).toBe(`${header}\nrejected unit-dropin\n`);
    expect((await f.run(install())).stdout).toBe(`${header}\nrejected unit-dropin\n`);
    expect(await staged(f.dropins)).toEqual(left);
    await NodeFSP.rm(NodePath.join(f.dropins, left[0]!));

    // Replacing an existing owned drop-in publishes by rename and leaves no stage.
    const previous = "# previous owned drop-in\n";
    await f.file(owned, previous);
    const withOwned = ["jones-code.conf", OWNED_SYSTEM_DROPIN];
    await f.setShow({ active: false, dropins: withOwned });
    await f.setShow(
      { active: false, dropins: withOwned, exec: `${f.launcherEntry} __service-launcher` },
      "daemon-reload",
    );
    const replaced = await f.run(install(sha256(previous)));
    expect(
      parseSystemControlResult({ outcome: "exited", signal: null, ...replaced }),
    ).toMatchObject({
      status: "complete",
      inspection: { unitDigest: digest, exec: `launcher ${launcherVersion}` },
    });
    expect(await NodeFSP.readFile(owned, "utf8")).toBe(
      renderOwnedSystemDropin({
        base: f.base,
        launcherVersion,
        serve: { port: 3773, host: "127.0.0.1" },
      }),
    );
    expect(await staged(f.dropins)).toEqual([]);
  }),
);

it.skipIf(!runnable)("starts only an inactive unit and reports an uncertain start as unknown", () =>
  fixture(async (f) => {
    const digest = await f.digest();
    const start = ["start", "--expect-unit", digest, "--expect-owned", "absent"];
    expect((await f.run(start)).stdout).toBe(`${header}\nrejected unit-active\n`);
    await f.setShow({ active: false });
    await f.file(NodePath.join(f.state, "fail-start"), "");
    expect((await f.run(start)).stdout).toMatch(/\nunknown start\n$/);
    await NodeFSP.rm(NodePath.join(f.state, "fail-start"));
    await f.setShow({}, "start");
    expect((await f.run(start)).stdout).toMatch(/\ncomplete\n$/);
  }),
);

it("trusts root files only through a real root-owned chain", async () => {
  const root = await NodeFSP.realpath(
    await NodeFSP.mkdtemp(NodePath.join(NodeOS.tmpdir(), "jones-system-control-trust-")),
  );
  try {
    const user = NodePath.join(root, "user-owned");
    await NodeFSP.writeFile(user, "x", { mode: 0o644 });
    const link = NodePath.join(root, "link");
    await NodeFSP.symlink("/usr/bin", link);
    const check = async (path: string) =>
      (
        await perl(
          [
            "-e",
            "do $ARGV[0] or die($@ || $!); print trusted_file($ARGV[1]) ? qq(trusted\\n) : qq(untrusted\\n);",
            worker,
            path,
          ],
          {},
        )
      ).stdout;
    expect(await check("/usr/bin/env")).toBe("trusted\n");
    expect(await check(user)).toBe("untrusted\n");
    expect(await check(NodePath.join(link, "env"))).toBe("untrusted\n");
  } finally {
    await NodeFSP.rm(root, { recursive: true, force: true });
  }
});

it("refuses the worker entry point without root before reading policy", async () => {
  if (process.getuid?.() === 0) return;
  expect(await perl(["-T", worker, "inspect"], {})).toEqual({
    code: 2,
    stdout: `${header}\nrejected not-root\n`,
    stderr: "",
  });
});

it("builds the exact worker argument shapes and refuses malformed requests", () => {
  const unitDigest = "a".repeat(64);
  expect(systemControlArguments({ operation: "inspect" })).toEqual(["inspect"]);
  expect(
    systemControlArguments({
      operation: "stop",
      unitDigest,
      owned: "absent",
      mainPid: 7,
      mainBirth: "9",
    }),
  ).toEqual([
    "stop",
    "--expect-unit",
    unitDigest,
    "--expect-owned",
    "absent",
    "--expect-main-pid",
    "7",
    "--expect-main-birth",
    "9",
  ]);
  for (const request of [
    { operation: "start", unitDigest: "x", owned: "absent" },
    { operation: "stop", unitDigest, owned: "absent", mainPid: 0, mainBirth: "9" },
    { operation: "install-launcher", unitDigest, owned: "absent", launcherVersion: "1.0.0" },
    { operation: "archive-launcher", unitDigest, owned: "absent" },
  ] as const)
    expect(() => systemControlArguments(request)).toThrow();
});

it("separates no-start, refusal and uncertain worker results", () => {
  const parse = (
    stdout: string,
    code: number | null,
    outcome: "exited" | "deadline" | "spawn-failed" = "exited",
  ) => parseSystemControlResult({ outcome, code, signal: null, stdout, stderr: "" });
  expect(parse("", 1)).toEqual({ status: "rejected", reason: "worker-not-started" });
  expect(parse("", null, "spawn-failed")).toEqual({
    status: "rejected",
    reason: "sudo-unavailable",
  });
  expect(parse(`${header}\n`, null, "deadline")).toEqual({ status: "unknown", reason: "deadline" });
  expect(parse(`${header}\nrejected unit-changed\n`, 2)).toEqual({
    status: "rejected",
    reason: "unit-changed",
  });
  expect(parse(`${header}\nunknown stop\n`, 3)).toEqual({ status: "unknown", reason: "stop" });
  for (const [stdout, code] of [
    [`${header}\nrejected unit-changed\n`, 0],
    [`${header}\n`, 0],
    [`${header}\npolicy x\ncomplete\n`, 0],
    [`${header}\nunknown stop`, 3],
    ["garbage\n", 0],
  ] as const)
    expect(parse(stdout, code), stdout).toEqual({ status: "unknown", reason: "malformed" });
});

it("runs the worker only through sudo -n with no environment or terminal", async () => {
  const root = await NodeFSP.realpath(
    await NodeFSP.mkdtemp(NodePath.join(NodeOS.tmpdir(), "jones-system-control-sudo-")),
  );
  try {
    const sudo = NodePath.join(root, "sudo");
    const record = NodePath.join(root, "record");
    await NodeFSP.writeFile(
      sudo,
      `#!/bin/sh\nprintf '%s\\n' "$@" > '${record}'\n/usr/bin/env >> '${record}'\nif [ -t 0 ]; then echo tty >> '${record}'; fi\nprintf '${header}\\nrejected caller\\n'\nexit 2\n`,
      { mode: 0o755 },
    );
    expect(await runSystemServiceControl({ operation: "inspect" }, { sudoPath: sudo })).toEqual({
      status: "rejected",
      reason: "caller",
    });
    const recorded = (await NodeFSP.readFile(record, "utf8")).split("\n");
    expect(recorded.slice(0, 4)).toEqual(["-n", "--", SYSTEM_SERVICE_CONTROL_PATH, "inspect"]);
    expect(
      recorded
        .slice(4)
        .filter(
          (line) =>
            line !== "" &&
            !line.startsWith("PWD=") &&
            !line.startsWith("SHLVL=") &&
            !line.startsWith("_="),
        ),
    ).toEqual([]);
  } finally {
    await NodeFSP.rm(root, { recursive: true, force: true });
  }
});

it("renders closed policy, owned drop-in and bootstrap manifest text", () => {
  const policy = {
    unit,
    user: "malcolmjones",
    uid: 1000,
    gid: 1000,
    base: "/home/malcolmjones/.t3",
    slice: "hostdev.slice",
    retainedEntry: "/home/malcolmjones/Jones-Code-Install/t3",
  };
  expect(renderSystemServicePolicy(policy)).toBe(
    "jones-system-service-policy 1\nunit host-tooling-t3.service\nuser malcolmjones\nuid 1000\ngid 1000\nbase /home/malcolmjones/.t3\nslice hostdev.slice\nretained-entry /home/malcolmjones/Jones-Code-Install/t3\n",
  );
  for (const change of [
    { user: "root" },
    { uid: 0 },
    { unit: "a@b.service" },
    { base: "/home/m/../x" },
    { slice: "hostdev" },
    { retainedEntry: "/home/m/t3 --x" },
    { user: "m\nunit x.service" },
  ])
    expect(() => renderSystemServicePolicy({ ...policy, ...change })).toThrow();
  const owned = renderOwnedSystemDropin({
    base: policy.base,
    launcherVersion,
    serve: { port: 3773 },
  });
  expect(owned.split("\n").filter((line) => /^[A-Za-z]+=/.test(line))).toEqual([
    "ExecStart=",
    `ExecStart=/home/malcolmjones/.t3/runtime/versions/${launcherVersion}/t3 __service-launcher`,
    "Environment=T3CODE_HOME=/home/malcolmjones/.t3",
    "Environment=T3CODE_PORT=3773",
  ]);
  expect(() =>
    renderOwnedSystemDropin({ base: policy.base, launcherVersion: "latest", serve: {} }),
  ).toThrow();
  expect(
    parseRetainedServeCommand(`${retained} serve --port 3773 --mode web`, retained, policy.base),
  ).toEqual({
    port: 3773,
    mode: "web",
  });
  for (const argv of [
    `${retained} start --port 3773`,
    `${retained} serve --port 3773 --port 3774`,
    `${retained} serve --port 99999`,
    `${retained} serve --base-dir /elsewhere`,
    `${retained} serve --token x`,
    `/other/t3 serve`,
  ])
    expect(parseRetainedServeCommand(argv, retained, policy.base), argv).toBeUndefined();
  const manifest = renderSystemBootstrapManifest(policy);
  expect(manifest.files.map((file) => [file.path, file.mode])).toEqual([
    [QUALIFIED_WRITER_OBSERVER_PATH, "0755"],
    [SYSTEM_SERVICE_CONTROL_PATH, "0755"],
    [SYSTEM_SERVICE_POLICY_PATH, "0644"],
    ["/etc/systemd/system/jones-qualified-writer-observer.socket", "0644"],
    ["/etc/systemd/system/jones-qualified-writer-observer@.service", "0644"],
  ]);
  expect(manifest.files[0]?.sha256).toBe(QUALIFIED_WRITER_OBSERVER_SHA256);
  expect(manifest.files[1]?.sha256).toBe(SYSTEM_SERVICE_CONTROL_SHA256);
  expect(manifest.enable).toEqual(["jones-qualified-writer-observer.socket"]);
});

it("accepts the exact observed historical start command only at its WorkingDirectory", () => {
  const entry =
    "/home/malcolmjones/Jones-Code-Install-20261008-911f6f14/t3-0.0.45-preview.20261008.37762212730.1-linux-x64/t3";
  const base = "/home/malcolmjones/.t3";
  const observed = `${entry} start --mode web --host 127.0.0.1 --port 3773 --base-dir ${base} --no-browser /home/malcolmjones`;
  const parsed = parseRetainedServeCommand(observed, entry, base, "/home/malcolmjones");
  expect(parsed).toEqual({
    port: 3773,
    host: "127.0.0.1",
    mode: "web",
    workingDirectory: "/home/malcolmjones",
  });
  for (const workingDirectory of [
    undefined,
    "",
    "/",
    base,
    "!/home/malcolmjones",
    "/home/malcolmjones/",
  ])
    expect(
      parseRetainedServeCommand(observed, entry, base, workingDirectory),
      String(workingDirectory),
    ).toBeUndefined();
  // The launcher drop-in keeps the bound loopback flags and names no directory.
  expect(renderOwnedSystemDropin({ base, launcherVersion, serve: parsed! })).toBe(
    renderOwnedSystemDropin({
      base,
      launcherVersion,
      serve: { port: 3773, host: "127.0.0.1", mode: "web" },
    }),
  );
});

it("keeps the installed worker bytes bound to this source", async () => {
  expect(sha256(await NodeFSP.readFile(worker))).toBe(SYSTEM_SERVICE_CONTROL_SHA256);
});
