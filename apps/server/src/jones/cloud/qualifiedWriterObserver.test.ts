// @effect-diagnostics nodeBuiltinImport:off
import { assert, it } from "@effect/vitest";
import * as NodeAssert from "node:assert/strict";
import * as NodeChildProcess from "node:child_process";
import * as NodeCrypto from "node:crypto";
import * as NodeFSP from "node:fs/promises";
import * as NodeNet from "node:net";
import * as NodeOS from "node:os";
import * as NodePath from "node:path";
import { QUALIFIED_STARTUP_STATE_FILES } from "./qualifiedQuiescence.ts";
import {
  evaluateWriterObservation,
  observeQualifiedWriters,
  parseWriterObserverResult,
  QUALIFIED_WRITER_OBSERVER_FILES,
  QUALIFIED_WRITER_OBSERVER_PATH,
  QUALIFIED_WRITER_OBSERVER_SHA256,
  renderWriterObserverSocketUnits,
} from "./qualifiedWriterObserver.ts";
import {
  QUALIFIED_WRITER_OBSERVER_SOCKET,
  type SocketPathMetadata,
} from "./qualifiedWriterObserverSocket.ts";

const helper = NodePath.join(import.meta.dirname, "qualifiedWriterObserver.pl");
const header = "jones-qualified-writer-observer 1";
// A root-owned regular file in a root-owned directory stands in for the
// installed helper; the synthetic sudo below never executes it.
const rootOwnedStandIn = "/usr/bin/env";

async function fixture(
  body: (input: {
    base: string;
    procRoot: string;
    files: { path: string; device: bigint; inode: bigint }[];
  }) => Promise<void>,
) {
  const base = await NodeFSP.realpath(
    await NodeFSP.mkdtemp(NodePath.join(NodeOS.tmpdir(), "jones-writer-observer-test-")),
  );
  try {
    await NodeFSP.mkdir(NodePath.join(base, "userdata"));
    const files = [];
    for (const name of ["statev2.sqlite", "settings.json"]) {
      const path = NodePath.join(base, "userdata", name);
      await NodeFSP.writeFile(path, `synthetic ${name}`);
      const stat = await NodeFSP.stat(path, { bigint: true });
      files.push({ path, device: stat.dev, inode: stat.ino });
    }
    const procRoot = NodePath.join(base, "proc");
    await NodeFSP.mkdir(NodePath.join(procRoot, "self"), { recursive: true });
    await NodeFSP.writeFile(NodePath.join(procRoot, "self", "status"), "NoNewPrivs:\t0\n");
    await body({ base, procRoot, files });
  } finally {
    await NodeFSP.rm(base, { recursive: true, force: true });
    await NodeAssert.rejects(NodeFSP.lstat(base), { code: "ENOENT" });
  }
}

async function syntheticProcess(
  procRoot: string,
  pid: number,
  options: {
    readonly birth?: string;
    readonly descriptors?: Record<string, { readonly target: string; readonly flags?: string }>;
    readonly maps?: string;
    readonly mapFiles?: Record<string, string>;
    readonly withoutTasks?: boolean;
  } = {},
) {
  const directory = NodePath.join(procRoot, String(pid));
  const task = NodePath.join(directory, "task", String(pid));
  await NodeFSP.mkdir(directory, { recursive: true });
  await NodeFSP.writeFile(
    NodePath.join(directory, "stat"),
    `${pid} (synthetic (worker)) S ${Array(18).fill("0").join(" ")} ${options.birth ?? "777"} 0`,
  );
  await NodeFSP.writeFile(NodePath.join(directory, "maps"), options.maps ?? "");
  if (options.withoutTasks) return;
  await NodeFSP.mkdir(NodePath.join(task, "fd"), { recursive: true });
  await NodeFSP.mkdir(NodePath.join(task, "fdinfo"));
  for (const [fd, descriptor] of Object.entries(options.descriptors ?? {})) {
    await NodeFSP.link(descriptor.target, NodePath.join(task, "fd", fd));
    if (descriptor.flags !== undefined)
      await NodeFSP.writeFile(
        NodePath.join(task, "fdinfo", fd),
        `pos:\t0\nflags:\t${descriptor.flags}\n`,
      );
  }
  await NodeFSP.mkdir(NodePath.join(directory, "map_files"));
  for (const [range, target] of Object.entries(options.mapFiles ?? {}))
    await NodeFSP.link(target, NodePath.join(directory, "map_files", range));
}

// Runs the helper's own observation against a synthetic /proc tree. The
// privileged entry point is not loaded: `run` executes only as the main script.
const observeHarness = `do $ARGV[0] or die($@ || $!);
my (undef, $proc, @specs) = @ARGV;
my @files = map { my ($index, $dev, $ino) = split /:/; +{ index => $index, dev => $dev, ino => $ino } } @specs;
my $output = eval { emit_observation(observe($proc, \\@files)) };
print defined $output ? $output : $@;`;

function perl(args: readonly string[], env?: Record<string, string>) {
  return new Promise<{ code: number | null; stdout: string; stderr: string }>((resolve) => {
    NodeChildProcess.execFile(
      "/usr/bin/perl",
      args,
      { timeout: 30_000, ...(env === undefined ? {} : { env }) },
      (error, stdout, stderr) =>
        resolve({
          code: error === null ? 0 : typeof error.code === "number" ? error.code : null,
          stdout,
          stderr,
        }),
    );
  });
}

const observeSynthetic = async (
  procRoot: string,
  files: readonly { device: bigint; inode: bigint }[],
) => {
  const result = await perl([
    "-e",
    observeHarness,
    helper,
    procRoot,
    ...files.map((file, index) => `${index === 0 ? 0 : 3}:${file.device}:${file.inode}`),
  ]);
  assert.equal(result.stderr, "");
  return result.stdout;
};

it("keeps one fixed state-file order in the launcher, client and root helper", async () => {
  assert.deepEqual(QUALIFIED_WRITER_OBSERVER_FILES, [
    "statev2.sqlite",
    "statev2.sqlite-wal",
    "statev2.sqlite-shm",
    ...QUALIFIED_STARTUP_STATE_FILES,
  ]);
  const source = await NodeFSP.readFile(helper, "utf8");
  const helperFiles = /^my @STATE_FILES = qw\(([^)]*)\);$/m.exec(source)?.[1]?.split(/\s+/);
  assert.deepEqual(helperFiles, [...QUALIFIED_WRITER_OBSERVER_FILES]);
  assert.match(source, /^#!\/usr\/bin\/perl -T\n/);
});

it("accepts only an exact, complete helper result", () => {
  const parse = (stdout: string, code = 0, stderr = "") =>
    parseWriterObserverResult({ code, signal: null, stdout, stderr });
  const complete = `${header}\nfile 0 10 20\nfile 3 10 21\nwriter 0 123 777 read-write\nwriter 3 456 9 shared-map\ncomplete 74\n`;
  assert.deepEqual(parse(complete), {
    status: "complete",
    files: [
      { index: 0, device: 10n, inode: 20n },
      { index: 3, device: 10n, inode: 21n },
    ],
    writers: [
      { index: 0, pid: 123, birth: "777", access: "read-write" },
      { index: 3, pid: 456, birth: "9", access: "shared-map" },
    ],
  });
  assert.deepEqual(parse(`${header}\nfile 0 10 20\nincomplete process-evidence\n`, 3), {
    status: "incomplete",
    reason: "process-evidence",
  });
  assert.deepEqual(parse(`${header}\nrejected not-root\n`, 2), {
    status: "rejected",
    reason: "not-root",
  });
  for (const [stdout, code, stderr] of [
    [complete, 1, ""],
    [complete, 0, "sudo: a password is required\n"],
    ["", 1, ""],
    [complete.replace(header, "jones-qualified-writer-observer 2"), 0, ""],
    [`${header}\nfile 0 10 20\nwriter 1 123 777 write\ncomplete 1\n`, 0, ""],
    [`${header}\nfile 3 10 20\nfile 0 10 21\ncomplete 1\n`, 0, ""],
    [
      `${header}\nfile 0 10 20\nwriter 0 123 777 write\nwriter 0 123 777 write\ncomplete 1\n`,
      0,
      "",
    ],
    [`${header}\nfile 0 10 20\nwriter 0 0 777 write\ncomplete 1\n`, 0, ""],
    [`${header}\nfile 0 10 20\nwriter 0 123 777 read\ncomplete 1\n`, 0, ""],
    [`${header}\nfile 0 10 20\nwriter 0 123 777 write\nincomplete deadline\n`, 3, ""],
    [`${header}\nincomplete deadline\n`, 0, ""],
    [`${header}\nfile 0 10 20\ncomplete 1`, 0, ""],
  ] as const)
    assert.equal(parse(stdout, code, stderr), undefined, stdout);
});

it("excludes only allowed writers whose process birth matches and binds every inode", () => {
  const files = [
    { path: "/home/user/.jones-code/userdata/statev2.sqlite", device: 10n, inode: 20n },
    { path: "/home/user/.jones-code/userdata/settings.json", device: 10n, inode: 21n },
  ];
  const result = {
    status: "complete" as const,
    files: [
      { index: 0, device: 10n, inode: 20n },
      { index: 3, device: 10n, inode: 21n },
    ],
    writers: [
      { index: 0, pid: 123, birth: "777", access: "read-write" as const },
      { index: 0, pid: 124, birth: "5", access: "read-write" as const },
      { index: 3, pid: 123, birth: "778", access: "write" as const },
    ],
  };
  assert.deepEqual(evaluateWriterObservation(result, files, new Map([[123, "777"]])), {
    status: "complete",
    writers: [
      { pid: 124, path: files[0]!.path },
      { pid: 123, path: files[1]!.path },
    ],
  });
  assert.deepEqual(
    evaluateWriterObservation(
      { ...result, files: [result.files[0]!, { index: 3, device: 10n, inode: 99n }] },
      files,
      new Map(),
    ),
    { status: "identity-changed" },
  );
  assert.deepEqual(
    evaluateWriterObservation({ ...result, files: [result.files[0]!] }, files, new Map()),
    { status: "identity-changed" },
  );
  assert.deepEqual(
    evaluateWriterObservation({ status: "incomplete", reason: "deadline" }, files, new Map()),
    { status: "unavailable" },
  );
});

it("runs only an installed root-owned helper and falls back only where it cannot elevate", async () => {
  await fixture(async ({ base, procRoot, files }) => {
    await syntheticProcess(procRoot, 123);
    const sudo = NodePath.join(base, "synthetic-sudo");
    const respond = async (output: string, code = 0) =>
      NodeFSP.writeFile(
        sudo,
        `#!/bin/sh\n/bin/cat <<'JONES_EOF'\n${output}\nJONES_EOF\nexit ${code}\n`,
        {
          mode: 0o755,
        },
      );
    // No observer socket exists here, so NoNewPrivs:1 keeps its unprivileged answer.
    const observe = (options: { helperPath?: string } = {}) =>
      observeQualifiedWriters(
        { files, allowedProcessIds: [123] },
        {
          helperPath: rootOwnedStandIn,
          sudoPath: sudo,
          procRoot,
          socketPath: NodePath.join(base, "absent.sock"),
          ...options,
        },
      );
    await respond(
      [
        header,
        `file 0 ${files[0]!.device} ${files[0]!.inode}`,
        `file 3 ${files[1]!.device} ${files[1]!.inode}`,
        "writer 0 123 777 read-write",
        "writer 3 456 1 write",
        "complete 2",
      ].join("\n"),
    );
    assert.deepEqual(await observe(), {
      status: "complete",
      writers: [{ pid: 456, path: files[1]!.path }],
    });
    // A sudo refusal is a misconfigured installed helper, never a fallback.
    await NodeFSP.writeFile(sudo, "#!/bin/sh\necho 'sudo: a password is required' >&2\nexit 1\n");
    assert.deepEqual(await observe(), { status: "unavailable" });
    assert.deepEqual(await observe({ helperPath: NodePath.join(base, "absent", "helper") }), {
      status: "unprivileged",
      reason: "not-installed",
    });
    // A helper the service user could replace is never run as root.
    assert.deepEqual(await observe({ helperPath: sudo }), { status: "unavailable" });
    await NodeFSP.writeFile(NodePath.join(procRoot, "self", "status"), "NoNewPrivs:\t1\n");
    assert.deepEqual(await observe(), { status: "unprivileged", reason: "no-new-privileges" });
    assert.deepEqual(await observe({ helperPath: NodePath.join(base, "absent", "helper") }), {
      status: "unprivileged",
      reason: "not-installed",
    });
    await NodeFSP.writeFile(NodePath.join(procRoot, "self", "status"), "NoNewPrivs:\t0\n");
    // An allowed process must be live and observable before the helper runs.
    await NodeFSP.rm(NodePath.join(procRoot, "123"), { recursive: true });
    assert.deepEqual(await observe(), { status: "unavailable" });
  });
});

it("reports descriptor and shared-mapping writers from the root helper's observation", async () => {
  await fixture(async ({ procRoot, files }) => {
    const [database, settings] = files;
    await syntheticProcess(procRoot, 123, {
      descriptors: {
        "5": { target: database!.path, flags: "0100002" },
        "6": { target: settings!.path, flags: "0100000" },
        "7": { target: settings!.path, flags: "02000001" },
      },
    });
    await syntheticProcess(procRoot, 124, {
      birth: "900",
      maps: `00007f00-00007f10 rw-s 00000000 fd:01 ${database!.inode} /synthetic/statev2.sqlite\n7f20-7f30 rw-p 00000000 fd:01 ${settings!.inode} /synthetic/settings.json\n7f40-7f50 r--s 00000000 fd:01 ${settings!.inode}\n`,
      mapFiles: { "7f00-7f10": database!.path, "7f20-7f30": settings!.path },
    });
    const unrelated = NodePath.join(NodePath.dirname(procRoot), "unrelated");
    await NodeFSP.writeFile(unrelated, "unrelated inode");
    await syntheticProcess(procRoot, 125, {
      descriptors: { "3": { target: unrelated, flags: "0100002" } },
    });
    assert.equal(
      await observeSynthetic(procRoot, files),
      "writer 0 123 777 read-write\nwriter 0 124 900 shared-map\nwriter 3 123 777 write\ncomplete 3\n",
    );
  });
});

it("ends the root helper's observation incomplete when present evidence is unreadable", async () => {
  await fixture(async ({ procRoot, files }) => {
    await syntheticProcess(procRoot, 123, { descriptors: { "5": { target: files[0]!.path } } });
    assert.equal(await observeSynthetic(procRoot, files), "incomplete descriptor-flags\n");
    await NodeFSP.rm(NodePath.join(procRoot, "123"), { recursive: true });
    await syntheticProcess(procRoot, 123, { withoutTasks: true });
    assert.equal(await observeSynthetic(procRoot, files), "incomplete process-evidence\n");
    await NodeFSP.rm(NodePath.join(procRoot, "123"), { recursive: true });
    await syntheticProcess(procRoot, 123, { maps: "not a mapping\n" });
    assert.equal(await observeSynthetic(procRoot, files), "incomplete mapping-format\n");
    await NodeFSP.rm(NodePath.join(procRoot, "123"), { recursive: true });
    // A listed process whose stat vanished holds nothing.
    await NodeFSP.mkdir(NodePath.join(procRoot, "126"));
    assert.equal(await observeSynthetic(procRoot, files), "complete 1\n");
  });
});

it("trusts the helper only through a real root-owned ancestor chain", async () => {
  await fixture(async ({ base, procRoot, files }) => {
    // Path resolution follows the user-owned link to root-owned /usr/bin, so
    // only the full-chain check sees an ancestor another user could replace.
    const link = NodePath.join(base, "link");
    await NodeFSP.symlink(NodePath.dirname(rootOwnedStandIn), link);
    const viaLink = NodePath.join(link, NodePath.basename(rootOwnedStandIn));
    const sudo = NodePath.join(base, "synthetic-sudo");
    await NodeFSP.writeFile(sudo, "#!/bin/sh\nexit 0\n", { mode: 0o755 });
    for (const helperPath of [viaLink, "/usr/bin/../bin/env", "usr/bin/env"])
      assert.deepEqual(
        await observeQualifiedWriters(
          { files, allowedProcessIds: [] },
          { helperPath, sudoPath: sudo, procRoot },
        ),
        { status: "unavailable" },
        helperPath,
      );
    // The privileged self-check applies the same chain rule to its own path.
    const selfCheck = async (path: string) =>
      (
        await perl([
          "-e",
          "do $ARGV[0] or die($@ || $!); $0 = $ARGV[1]; print trusted_self() ? qq(trusted\\n) : qq(untrusted\\n);",
          helper,
          path,
        ])
      ).stdout;
    assert.equal(await selfCheck(rootOwnedStandIn), "trusted\n");
    assert.equal(await selfCheck(viaLink), "untrusted\n");
    assert.equal(await selfCheck(files[1]!.path), "untrusted\n");
  });
});

it("ends a helper run that outlasts its alarm with a typed incomplete result", async () => {
  const result = await perl([
    "-e",
    'do $ARGV[0] or die($@ || $!); my ($code, $output) = bounded(1, sub { select(undef, undef, undef, 5); "late\\n" }); print "$code $output";',
    helper,
  ]);
  assert.deepEqual(result, { code: 0, stdout: "3 incomplete deadline\n", stderr: "" });
});

it("never reports complete after an inner eval swallowed the deadline alarm", async () => {
  // The alarm is delivered at once by signalling the helper process itself and
  // is caught by an inner eval, as observe_process's eval could when the
  // observed process also vanished; the work then claims completion.
  const swallowed = await perl([
    "-e",
    'do $ARGV[0] or die($@ || $!); my ($code, $output) = bounded(5, sub { eval { kill("ALRM", $$); my $spin = 0; $spin++ while $spin < 100000; 1 }; "complete 1\\n" }); print "$code $output";',
    helper,
  ]);
  assert.deepEqual(swallowed, { code: 0, stdout: "3 incomplete deadline\n", stderr: "" });
  await fixture(async ({ procRoot }) => {
    // Once expired, the next observation boundary ends the scan, including
    // for a listed process that has vanished.
    await NodeFSP.mkdir(NodePath.join(procRoot, "126"));
    const expired = await perl([
      "-e",
      "do $ARGV[0] or die($@ || $!); $main::DEADLINE_EXPIRED = 1; my $output = eval { emit_observation(observe($ARGV[1], [])) }; print defined $output ? $output : $@;",
      helper,
      procRoot,
    ]);
    assert.deepEqual(expired, { code: 0, stdout: "incomplete deadline\n", stderr: "" });
  });
});

it("refuses the privileged entry point without root before reading any state", async () => {
  if (process.getuid?.() === 0) return;
  const result = await perl(["-T", helper, "--base", "/synthetic/home"]);
  assert.deepEqual(result, { code: 2, stdout: `${header}\nrejected not-root\n`, stderr: "" });
  const socketMode = await perl(["-T", helper, "--socket", "--base", "/synthetic/home"], {});
  assert.deepEqual(socketMode, { code: 2, stdout: `${header}\nrejected not-root\n`, stderr: "" });
});

it("keeps the installed helper bytes bound to this source", async () => {
  const digest = NodeCrypto.createHash("sha256")
    .update(await NodeFSP.readFile(helper))
    .digest("hex");
  assert.equal(digest, QUALIFIED_WRITER_OBSERVER_SHA256);
});

// Runs the helper's request check after its root check, as the entry point
// would, with stdin and stdout set up the way the socket unit or sudo leaves
// them. Rejections are reported instead of exiting.
const callerHarness = `use Socket;
my ($helper, $stdin, @arguments) = @ARGV;
do $helper or die($@ || $!);
{ no warnings "redefine"; *main::reject = sub { die "rejected $_[0]\\n" }; }
open(my $report, ">&", \\*STDOUT) or die;
my ($here, $there);
if ($stdin eq "file") {
  open(STDIN, "<", $helper) or die;
} elsif ($stdin ne "inherited") {
  socketpair($here, $there, AF_UNIX, $stdin eq "datagram" ? SOCK_DGRAM : SOCK_STREAM, PF_UNSPEC) or die;
  open(STDIN, "<&", $here) or die;
  if ($stdin ne "split") { open(STDOUT, ">&", $here) or die; }
}
@ARGV = @arguments;
my @caller = eval { request_caller() };
print {$report} (@caller ? "caller @caller\\n" : $@);`;

// oxlint-disable-next-line t3code/no-global-process-runtime -- SO_PEERCRED exists only on a Linux host.
const linux = NodeOS.platform() === "linux";

it("takes the socket caller only from kernel peer credentials on the accepted stream", async () => {
  const request = async (
    stdin: "stream" | "datagram" | "split" | "file" | "inherited",
    args: readonly string[],
    env: Record<string, string>,
  ) => {
    const result = await perl(["-e", callerHarness, helper, stdin, ...args], env);
    assert.equal(result.stderr, "");
    return result.stdout;
  };
  const home = ["--base", "/synthetic/home"];
  // A socketpair's peer is this process, so the kernel reports the test's UID.
  // Without SO_PEERCRED, or for UID 0, the peer itself is refused.
  const uid = process.getuid?.();
  const peerReadable = linux && uid !== undefined && uid !== 0;
  const afterPeer = (expected: string) => (peerReadable ? expected : "rejected peer\n");
  assert.equal(
    await request("stream", ["--socket", ...home], {}),
    afterPeer(`caller ${uid} /synthetic/home\n`),
  );
  for (const env of [{ SUDO_UID: "1000" }, { SUDO_UID: "" }])
    assert.equal(await request("stream", ["--socket", ...home], env), "rejected caller\n");
  for (const args of [
    ["--socket", "--base"],
    ["--socket", "--base", "/synthetic/home", "/other"],
    ["--socket", "--home", "/synthetic/home"],
    ["--socket", "--socket", ...home],
  ])
    assert.equal(await request("stream", args, {}), "rejected arguments\n", args.join(" "));
  // Output must reach the same connected stream whose peer was identified.
  for (const stdin of ["file", "split", "datagram", "inherited"] as const)
    assert.equal(await request(stdin, ["--socket", ...home], {}), "rejected peer\n", stdin);
  for (const base of ["relative", "/synthetic/../home", "/synthetic/home/"])
    assert.equal(
      await request("stream", ["--socket", "--base", base], {}),
      afterPeer("rejected base\n"),
      base,
    );
  // The sudo request shape is unchanged.
  assert.equal(
    await request("inherited", home, { SUDO_UID: "1000" }),
    "caller 1000 /synthetic/home\n",
  );
  for (const env of [{}, { SUDO_UID: "0" }, { SUDO_UID: "01000" }, { SUDO_UID: "1000 " }])
    assert.equal(await request("inherited", home, env), "rejected caller\n", JSON.stringify(env));
  assert.equal(
    await request("inherited", [...home, "--socket"], { SUDO_UID: "1000" }),
    "rejected arguments\n",
  );
  assert.equal(
    await request("inherited", ["--base", "/synthetic/./home"], { SUDO_UID: "1000" }),
    "rejected base\n",
  );
});

const rootDirectory: SocketPathMetadata = {
  isDirectory: () => true,
  isSocket: () => false,
  isSymbolicLink: () => false,
  uid: 0,
  mode: 0o040755,
  dev: 1,
  ino: 1,
};

/** The socket fields of a real lstat result, without its prototype methods. */
const pick = (stat: SocketPathMetadata) => ({
  isSocket: () => true,
  isDirectory: () => false,
  uid: stat.uid,
  mode: stat.mode,
  dev: stat.dev,
  ino: stat.ino,
});

it("reads the observer socket only under NoNewPrivileges and never falls back from it", async () => {
  await fixture(async ({ base, procRoot, files }) => {
    await syntheticProcess(procRoot, 123);
    const status = NodePath.join(procRoot, "self", "status");
    await NodeFSP.writeFile(status, "NoNewPrivs:\t1\n");
    const socketPath = NodePath.join(base, "o.sock");
    let respond: (connection: NodeNet.Socket) => void = () => undefined;
    const requests: number[] = [];
    const server = NodeNet.createServer({ allowHalfOpen: true }, (connection) => {
      let received = 0;
      connection.on("error", () => undefined);
      connection.on("data", (bytes: Buffer) => {
        received += bytes.length;
      });
      connection.once("end", () => {
        requests.push(received);
        respond(connection);
      });
    });
    await new Promise<void>((resolve, reject) => {
      server.once("error", reject);
      server.listen(socketPath, () => resolve());
    });
    try {
      await NodeFSP.chmod(socketPath, 0o600);
      // Real metadata, with each real ancestor directory presented as root-owned.
      const trusted = async (path: string): Promise<SocketPathMetadata> => {
        const stat = await NodeFSP.lstat(path);
        return stat.isDirectory() ? rootDirectory : stat;
      };
      const observe = (
        options: Parameters<typeof observeQualifiedWriters>[1] = {},
        socketMetadata = trusted,
      ) =>
        observeQualifiedWriters(
          { files, allowedProcessIds: [123] },
          {
            helperPath: rootOwnedStandIn,
            // Never runs: a sudo attempt would fail to spawn and report unavailable.
            sudoPath: NodePath.join(base, "absent-sudo"),
            procRoot,
            socketPath,
            socketMetadata,
            deadlineMs: 5_000,
            ...options,
          },
        );
      const reply = (...lines: string[]) => {
        respond = (connection) => connection.end(`${lines.join("\n")}\n`);
      };
      const fileLines = [
        `file 0 ${files[0]!.device} ${files[0]!.inode}`,
        `file 3 ${files[1]!.device} ${files[1]!.inode}`,
      ];
      const complete = [
        header,
        ...fileLines,
        "writer 0 123 777 read-write",
        "writer 3 456 1 write",
        "complete 2",
      ];

      reply(...complete);
      assert.deepEqual(await observe(), {
        status: "complete",
        writers: [{ pid: 456, path: files[1]!.path }],
      });
      assert.deepEqual(requests, [0]);

      // The helper's own answer still binds every inode to this snapshot.
      reply(
        header,
        fileLines[0]!,
        `file 3 ${files[1]!.device} ${files[1]!.inode + 1n}`,
        "complete 2",
      );
      assert.deepEqual(await observe(), { status: "identity-changed" });

      for (const lines of [
        [header, "rejected base-owner"],
        [header, ...fileLines, "incomplete deadline"],
        [header, ...fileLines],
        [header, ...fileLines, "writer 3 456 1 write", "writer 3 456 1 write", "complete 2"],
        [...complete, "complete 2"],
      ]) {
        reply(...lines);
        assert.deepEqual(await observe(), { status: "unavailable" }, lines.join("|"));
      }
      respond = (connection) => connection.end("x".repeat(64 * 1024 + 1));
      assert.deepEqual(await observe(), { status: "unavailable" });
      respond = () => undefined;
      assert.deepEqual(await observe({ deadlineMs: 200 }), { status: "unavailable" });

      // The allowed trial's birth is still read before and after the answer.
      const setBirth = (birth: string) =>
        NodeFSP.writeFile(
          NodePath.join(procRoot, "123", "stat"),
          `123 (synthetic (worker)) S ${Array(18).fill("0").join(" ")} ${birth} 0`,
        );
      respond = (connection) =>
        void setBirth("778").then(() => connection.end(`${complete.join("\n")}\n`));
      assert.deepEqual(await observe(), { status: "unavailable" });
      await setBirth("777");

      // A socket replaced during the exchange voids its answer.
      reply(...complete);
      let reads = 0;
      const drifting = async (path: string): Promise<SocketPathMetadata> => {
        if (path !== socketPath) return trusted(path);
        const stat = await NodeFSP.lstat(path);
        return reads++ === 0 ? stat : { ...rootDirectory, ...pick(stat), ino: stat.ino + 1 };
      };
      assert.deepEqual(await observe({}, drifting), { status: "unavailable" });

      // A present socket that is untrusted, unreachable or whose helper is
      // missing reports unavailable rather than the unprivileged fallback.
      assert.deepEqual(await observe({}, (path) => NodeFSP.lstat(path)), {
        status: "unavailable",
      });
      assert.deepEqual(await observe({ helperPath: NodePath.join(base, "absent", "helper") }), {
        status: "unavailable",
      });
      const stale = NodePath.join(base, "stale.sock");
      const staleSocket = { ...pick(await NodeFSP.lstat(socketPath)), ino: 99 };
      assert.deepEqual(
        await observe({ socketPath: stale }, async (path) =>
          path === stale ? { ...rootDirectory, ...staleSocket } : rootDirectory,
        ),
        { status: "unavailable" },
      );

      // Only a missing socket keeps the unprivileged answer.
      const missing = NodePath.join(base, "missing.sock");
      assert.deepEqual(await observe({ socketPath: missing }), {
        status: "unprivileged",
        reason: "no-new-privileges",
      });
      assert.deepEqual(
        await observe({ socketPath: missing, helperPath: NodePath.join(base, "absent", "helper") }),
        { status: "unprivileged", reason: "not-installed" },
      );

      // Without NoNewPrivileges, or without evidence of it, the socket is not chosen.
      const before = requests.length;
      await NodeFSP.writeFile(status, "NoNewPrivs:\t0\n");
      assert.deepEqual(await observe(), { status: "unavailable" });
      await NodeFSP.rm(status);
      assert.deepEqual(await observe(), { status: "unavailable" });
      assert.equal(requests.length, before);
    } finally {
      await new Promise<void>((resolve) => server.close(() => resolve()));
    }
  });
});

it("renders closed root socket units for one approved user and home", () => {
  const units = renderWriterObserverSocketUnits({
    user: "malcolmjones",
    base: "/home/malcolmjones/.t3",
  });
  assert.equal(units.socket.name, "jones-qualified-writer-observer.socket");
  assert.equal(units.service.name, "jones-qualified-writer-observer@.service");
  const directives = (text: string) => text.split("\n").filter((line) => line.includes("="));
  const socket = directives(units.socket.text);
  const service = directives(units.service.text);
  for (const line of [
    `ListenStream=${QUALIFIED_WRITER_OBSERVER_SOCKET}`,
    "SocketUser=malcolmjones",
    "SocketMode=0600",
    "DirectoryMode=0755",
    "Accept=yes",
    "MaxConnections=4",
    "MaxConnectionsPerSource=4",
  ])
    assert.include(socket, line);
  assert.deepEqual(
    service.filter((line) => line.startsWith("ExecStart")),
    [`ExecStart=${QUALIFIED_WRITER_OBSERVER_PATH} --socket --base /home/malcolmjones/.t3`],
  );
  for (const line of [
    "StandardInput=socket",
    "StandardOutput=socket",
    "StandardError=journal",
    "Slice=system.slice",
    "RuntimeMaxSec=12",
    "TimeoutStopSec=3",
    "NoNewPrivileges=yes",
    "PrivateNetwork=yes",
  ])
    assert.include(service, line);
  for (const key of ["CPUQuota", "MemoryMax", "TasksMax"])
    assert.isTrue(
      service.some((line) => line.startsWith(`${key}=`)),
      key,
    );
  // Root with host PID visibility; no caller-selected identity or environment.
  for (const line of [...socket, ...service])
    assert.notMatch(
      line,
      /^(?:User|Group|DynamicUser|PrivatePIDs|ProtectProc|ProcSubset|PrivateUsers|CapabilityBoundingSet|AmbientCapabilities|Environment|EnvironmentFile|PassEnvironment)=|hostdev|SUDO_UID/,
      line,
    );
  for (const text of [units.socket.text, units.service.text])
    for (const character of text) {
      const code = character.charCodeAt(0);
      assert.isFalse((code < 0x20 && code !== 0x0a) || code === 0x7f, `control ${code}`);
    }

  for (const user of [
    "",
    "root",
    "Malcolm",
    "malcolm jones",
    "malcolm\nExecStartPre=/bin/sh",
    "m%h",
    "$USER",
    "-m",
    "m\tj",
    "a".repeat(33),
  ])
    NodeAssert.throws(
      () => renderWriterObserverSocketUnits({ user, base: "/home/m/.t3" }),
      /observer socket user/,
      JSON.stringify(user),
    );
  for (const base of [
    "",
    "/",
    "relative/.t3",
    "/home/m/",
    "/home//m",
    "/home/./m",
    "/home/../m",
    "/home/m j",
    "/home/m\nExecStartPre=/bin/sh",
    "/home/%h",
    "/home/$HOME",
    '/home/"m"',
    "/home/m\\x",
    "/home/m;x",
    `/${"a".repeat(256)}`,
  ])
    NodeAssert.throws(
      () => renderWriterObserverSocketUnits({ user: "m", base }),
      /observer home/,
      JSON.stringify(base),
    );
});
