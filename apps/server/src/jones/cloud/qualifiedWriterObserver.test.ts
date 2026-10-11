// @effect-diagnostics nodeBuiltinImport:off
import { assert, it } from "@effect/vitest";
import * as NodeAssert from "node:assert/strict";
import * as NodeChildProcess from "node:child_process";
import * as NodeFSP from "node:fs/promises";
import * as NodeOS from "node:os";
import * as NodePath from "node:path";
import { QUALIFIED_STARTUP_STATE_FILES } from "./qualifiedQuiescence.ts";
import {
  evaluateWriterObservation,
  observeQualifiedWriters,
  parseWriterObserverResult,
  QUALIFIED_WRITER_OBSERVER_FILES,
} from "./qualifiedWriterObserver.ts";

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

function perl(args: readonly string[]) {
  return new Promise<{ code: number | null; stdout: string; stderr: string }>((resolve) => {
    NodeChildProcess.execFile("/usr/bin/perl", args, { timeout: 30_000 }, (error, stdout, stderr) =>
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
    const observe = (options: { helperPath?: string } = {}) =>
      observeQualifiedWriters(
        { files, allowedProcessIds: [123] },
        { helperPath: rootOwnedStandIn, sudoPath: sudo, procRoot, ...options },
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

it("refuses the privileged entry point without root before reading any state", async () => {
  if (process.getuid?.() === 0) return;
  const result = await perl(["-T", helper, "--base", "/synthetic/home"]);
  assert.deepEqual(result, { code: 2, stdout: `${header}\nrejected not-root\n`, stderr: "" });
});
