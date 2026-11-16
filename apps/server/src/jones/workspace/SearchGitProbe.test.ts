import { expect, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as Deferred from "effect/Deferred";
import * as Fiber from "effect/Fiber";
import * as FileSystem from "effect/FileSystem";
import * as Path from "effect/Path";
import * as NodeServices from "@effect/platform-node/NodeServices";
import { ChildProcessSpawner } from "effect/process";
import * as VcsProcess from "../../vcs/VcsProcess.ts";
import { make, probeCommand, SearchProbeClock, type ProbeClock } from "./SearchGitProbe.ts";
import type { Bootstrap } from "../serverCompute/protocol.ts";

const boot: Bootstrap = { version: 1, build: "a".repeat(64), authorityId: "authority", slotId: 0, slotGeneration: 1, nonce: "nonce" };
const token = { authorityId: boot.authorityId, slotId: 0, slotGeneration: 1, keyId: "key", incarnation: 1 };
const owner = { kind: "foreground" as const, requestId: 1 };
const output = { exitCode: ChildProcessSpawner.ExitCode(0), stdout: "", stderr: "", stdoutTruncated: false, stderrTruncated: false };
function virtualClock() {
  let time = 0, serial = 0;
  const timers = new Map<number, { deadline: number; callback: () => void }>();
  const clock: ProbeClock = { now: () => time, schedule: (ms, callback) => {
    const id = ++serial;
    timers.set(id, { deadline: time + ms, callback });
    return () => { timers.delete(id); };
  } };
  return { clock, advance: (ms: number) => {
    time += ms;
    for (const [id, timer] of [...timers]) if (timer.deadline <= time) { timers.delete(id); timer.callback(); }
  } };
}
const fixture = (run: VcsProcess.VcsProcess["Service"]["run"]) => Effect.gen(function* () {
  const time = virtualClock();
  const fs = yield* FileSystem.FileSystem;
  const probe = yield* make.pipe(
    Effect.provideService(SearchProbeClock, time.clock),
    Effect.provideService(VcsProcess.VcsProcess, { run }),
    Effect.provideService(FileSystem.FileSystem, { ...fs, realPath: (p) => Effect.succeed(p) }),
  );
  yield* probe.bind({ bootstrap: boot, token, key: { cwd: "/workspace", variant: "paths" } });
  yield* probe.foregroundBegin({ token, requestId: 1, operation: "open", input: { cwd: "/workspace", variant: "paths" }, deadlineMonotonicMs: 30_000 });
  return { probe, ...time };
});

it("fixes all command and helper controls while preserving Git excludes", () => {
  const input = probeCommand({ kind: "grep", pattern: "x\ny", regexMode: true, ignoreCase: true, onlyMatching: true }, "/workspace", 1500);
  expect(input.command).toBe("git");
  expect(input.args).toEqual(["--no-pager", "--no-lazy-fetch", "--no-optional-locks", "-c", "core.fsmonitor=false",
    "-c", "core.quotePath=false", "-c", "grep.column=false", "-c", "grep.fullName=false", "-c", "submodule.recurse=false",
    "grep", "--untracked", "--no-textconv", "--no-recurse-submodules", "--threads=1", "--no-color", "-I", "-n", "-z", "-m", "100", "-o", "--column", "-i", "-P", "-e", "x\ny"]);
  expect(input.timeoutMs).toBe(1500);
  const ignored = probeCommand({ kind: "ignoredPaths" }, "/workspace", 30_000);
  expect(ignored.args).toContain("--exclude-standard");
  expect(ignored.timeoutMs).toBe(10_000);
  expect(ignored.args).not.toContain("core.excludesFile=");
});

it.effect("holds the per-slot admission charge through delivery acknowledgement", () =>
  Effect.scoped(Effect.gen(function* () {
    let launches = 0;
    const f = yield* fixture((input) => Effect.sync(() => { launches++; input.onSpawn?.(); return output; }));
    yield* f.probe.run(token, owner, 1, { kind: "ignoredPaths" });
    const second = yield* f.probe.run(token, owner, 2, { kind: "ignoredPaths" }).pipe(Effect.forkChild);
    yield* Effect.yieldNow;
    expect(launches).toBe(1);
    yield* f.probe.acknowledge(token, owner, 1);
    yield* Fiber.join(second);
    expect(launches).toBe(2);
    yield* f.probe.acknowledge(token, owner, 2);
  })).pipe(Effect.provide(NodeServices.layer)),
);

it.effect("refuses query rebinding and expired owner admission without a launch", () =>
  Effect.scoped(Effect.gen(function* () {
    let launches = 0;
    const f = yield* fixture(() => Effect.sync(() => { launches++; return output; }));
    yield* f.probe.foregroundEnd(token, 1);
    yield* f.probe.opened(token);
    yield* f.probe.foregroundBegin({ token, requestId: 2, operation: "searchContents",
      input: { query: "original", caseSensitive: true, useRegex: true }, deadlineMonotonicMs: 30_000 });
    const changed = yield* f.probe.run(token, { kind: "foreground", requestId: 2 }, 1,
      { kind: "grep", pattern: "different", regexMode: true, ignoreCase: false, onlyMatching: false }).pipe(Effect.flip);
    expect(changed.reason).toBe("protocol");
    f.advance(29_001);
    const expired = yield* f.probe.run(token, { kind: "foreground", requestId: 2 }, 2,
      { kind: "grep", pattern: "original", regexMode: true, ignoreCase: false, onlyMatching: false }).pipe(Effect.flip);
    expect(expired.reason).toBe("deadline");
    expect(launches).toBe(0);
  })).pipe(Effect.provide(NodeServices.layer)),
);

it.effect("fences auxiliary admission while cancelled ordinary cleanup remains unresolved", () =>
  Effect.scoped(Effect.gen(function* () {
    const release = yield* Deferred.make<void>();
    const started = yield* Deferred.make<void>();
    const f = yield* fixture((input) => Effect.gen(function* () {
      input.onSpawn?.();
      yield* Deferred.succeed(started, undefined);
      yield* Deferred.await(release).pipe(Effect.uninterruptible);
      return output;
    }));
    const running = yield* f.probe.run(token, owner, 1, { kind: "ignoredPaths" }).pipe(Effect.forkChild);
    yield* Deferred.await(started);
    yield* f.probe.cancel(token, owner, 1);
    yield* f.probe.acknowledge(token, owner, 1);
    f.advance(5001);
    const held = yield* f.probe.run(token, owner, 2, { kind: "ignoredPaths" }).pipe(Effect.flip);
    expect(held.reason).toBe("cleanup-unknown");
    yield* Deferred.succeed(release, undefined);
    yield* Fiber.await(running);
    // Only original scoped completion clears this fence. It is no OS exit receipt.
    const completed = yield* f.probe.run(token, owner, 3, { kind: "ignoredPaths" });
    expect(completed.exitCode).toBe(0);
    yield* f.probe.acknowledge(token, owner, 3);
  })).pipe(Effect.provide(NodeServices.layer)),
);

it.effect("uses real fixed Git operations without repository fsmonitor/textconv helpers", () =>
  Effect.scoped(Effect.gen(function* () {
    const fs = yield* FileSystem.FileSystem, path = yield* Path.Path;
    const directory = yield* fs.makeTempDirectoryScoped({ prefix: "jones-search-probe-" });
    const cwd = yield* fs.realPath(directory);
    const vcs = yield* VcsProcess.VcsProcess;
    yield* vcs.run({ operation: "probe.fixture.init", command: "git", args: ["init", "--quiet"], cwd });
    yield* fs.writeFileString(path.join(cwd, "visible.txt"), "probe-needle\n");
    yield* fs.writeFileString(path.join(cwd, "hidden.txt"), "probe-needle\n");
    yield* fs.writeFileString(path.join(cwd, ".git", "info", "exclude"), "hidden.txt\n");
    yield* vcs.run({ operation: "probe.fixture.add", command: "git", args: ["add", "visible.txt"], cwd });
    const helper = path.join(cwd, "helper.sh");
    yield* fs.writeFileString(helper, '#!/bin/sh\nprintf called >> "$0.called"\nprintf "probe-needle\\n"\n');
    yield* fs.chmod(helper, 0o700);
    yield* fs.writeFileString(path.join(cwd, ".gitattributes"), "visible.txt diff=probe\n");
    yield* fs.writeFileString(path.join(cwd, ".git", "config"),
      `[core]\nrepositoryformatversion=0\nbare=false\nfsmonitor=${JSON.stringify(helper)}\n[diff "probe"]\ntextconv=${JSON.stringify(helper)}\n`);
    const probe = yield* make;
    yield* probe.bind({ bootstrap: boot, token, key: { cwd, variant: "content" } });
    yield* probe.foregroundBegin({ token, requestId: 1, operation: "open", input: { cwd, variant: "content" },
      deadlineMonotonicMs: performance.now() + 30_000 });
    const ignored = yield* probe.run(token, owner, 1, { kind: "ignoredPaths" });
    expect(ignored.exitCode).toBe(0);
    expect(ignored.stdout.split("\0")).toContain("hidden.txt");
    yield* probe.acknowledge(token, owner, 1);
    yield* probe.opened(token);
    yield* probe.foregroundEnd(token, 1);
    const searchOwner = { kind: "foreground" as const, requestId: 2 };
    yield* probe.foregroundBegin({ token, requestId: 2, operation: "searchContents",
      input: { query: "probe-needle", caseSensitive: true, useRegex: false },
      deadlineMonotonicMs: performance.now() + 30_000 });
    const found = yield* probe.run(token, searchOwner, 2, { kind: "grep", pattern: "probe-needle",
      ignoreCase: false, regexMode: false, onlyMatching: false });
    expect(found.exitCode).toBe(0);
    expect(found.stdout).toContain("visible.txt");
    expect(found.stdout).not.toContain("hidden.txt");
    expect(yield* fs.exists(`${helper}.called`)).toBe(false);
    yield* probe.acknowledge(token, searchOwner, 2);
  })).pipe(Effect.provide(VcsProcess.layer), Effect.provide(NodeServices.layer)),
);
