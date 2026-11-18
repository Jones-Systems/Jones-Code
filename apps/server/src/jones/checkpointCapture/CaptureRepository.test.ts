import * as NodeServices from "@effect/platform-node/NodeServices";
import { assert, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Path from "effect/Path";
import { ChildProcessSpawner } from "effect/process";

import type * as VcsDriver from "../../vcs/VcsDriver.ts";
import * as CaptureRepository from "./CaptureRepository.ts";

it.effect.each([
  { label: "zero HEAD", stdout: `${"0".repeat(40)}\n` },
  { label: "invalid HEAD", stdout: "not-an-object\n" },
  { label: "unterminated HEAD", stdout: "1".repeat(40) },
  { label: "truncated HEAD", stdout: `${"1".repeat(40)}\n`, stdoutTruncated: true },
  { label: "invalid UTF-8 HEAD", stdout: `${"1".repeat(40)}\n`, stdoutInvalidUtf8: true },
])("fallback capture refuses $label in an actual linked classification", (sample) =>
  Effect.gen(function* () {
    const commands: string[] = [];
    const execute: VcsDriver.VcsDriver["Service"]["execute"] = (input) =>
      Effect.gen(function* () {
        const args = input.args.join(" ");
        commands.push(args);
        const stdout = input.args.includes("--path-format=absolute")
          ? "--path-format=absolute\n"
          : args === "rev-parse --git-common-dir"
            ? "/synthetic/common\n"
            : args === "rev-parse --git-dir"
              ? "/synthetic/common/worktrees/linked\n"
              : args === "rev-parse --verify --quiet HEAD"
                ? sample.stdout
                : undefined;
        if (stdout === undefined) return yield* Effect.die("Unexpected command after invalid HEAD");
        const head = args === "rev-parse --verify --quiet HEAD";
        return {
          exitCode: ChildProcessSpawner.ExitCode(0),
          stdout,
          stderr: "",
          stdoutTruncated: head && "stdoutTruncated" in sample && sample.stdoutTruncated === true,
          stdoutInvalidUtf8:
            head && "stdoutInvalidUtf8" in sample && sample.stdoutInvalidUtf8 === true,
          stderrTruncated: false,
        };
      });
    const capture = yield* CaptureRepository.make(execute);
    const result = yield* capture.resolveRepository("/synthetic/linked").pipe(Effect.result);
    assert.strictEqual(result._tag, "Failure");
    if (result._tag !== "Failure") return assert.fail("Expected invalid HEAD refusal");
    assert.strictEqual(result.failure._tag, "VcsRepositoryDetectionError");
    assert.deepEqual(commands.slice(1), [
      "rev-parse --git-common-dir",
      "rev-parse --git-dir",
      "rev-parse --verify --quiet HEAD",
    ]);
  }).pipe(Effect.provide(NodeServices.layer)),
);

it.effect.each([
  "registered",
  "foreign gitdir",
  "foreign superproject",
  "foreign worktree",
  "parent changed repository",
  "missing gitlink",
  "unmerged gitlink",
  "truncated registration",
  "invalid UTF-8 registration",
] as const)("parent continuation checks physical submodule custody: %s", (sample) =>
  Effect.gen(function* () {
    const fs = yield* FileSystem.FileSystem;
    const path = yield* Path.Path;
    const temporary = yield* fs.makeTempDirectoryScoped({ prefix: "t3-submodule-custody-" });
    // Use physical paths so the fixture also works through macOS's /var alias.
    const root = yield* fs.realPath(temporary);
    const parent = path.join(root, "parent");
    const child = path.join(parent, "library with spaces");
    const common = path.join(root, "repository.git");
    const parentGitDir = path.join(common, "worktrees", "parent");
    const moduleGitDir = path.join(parentGitDir, "modules", "library");
    const foreign = path.join(root, "foreign");
    for (const directory of [child, moduleGitDir, foreign])
      yield* fs.makeDirectory(directory, { recursive: true });
    const childGitDir = sample === "foreign gitdir" ? foreign : moduleGitDir;
    const head = "1".repeat(40);
    const commands: string[] = [];
    const execute: VcsDriver.VcsDriver["Service"]["execute"] = (input) =>
      Effect.sync(() => {
        const args = input.args.join(" ");
        commands.push(args);
        let stdout: string;
        if (input.args.includes("--path-format=absolute")) {
          const gitDir = input.cwd === parent ? parentGitDir : childGitDir;
          const gitCommonDir = input.cwd === parent ? common : childGitDir;
          stdout = `${gitCommonDir}\n${gitDir}\n${path.join(gitDir, "index")}\n${input.cwd}\n\n${head}\n`;
        } else if (args === "rev-parse --show-toplevel") {
          stdout = `${input.cwd === child && sample === "foreign worktree" ? foreign : input.cwd}\n`;
        } else if (args === "rev-parse --show-superproject-working-tree") {
          stdout = `${sample === "foreign superproject" ? foreign : parent}\n`;
        } else if (args === "rev-parse --git-dir") {
          stdout = `${sample === "parent changed repository" ? foreign : parentGitDir}\n`;
        } else if (input.args[0] === "ls-files") {
          stdout =
            sample === "missing gitlink"
              ? ""
              : `160000 ${head} ${sample === "unmerged gitlink" ? "1" : "0"}\tlibrary with spaces\0`;
        } else throw new Error("Unexpected custody probe");
        return {
          exitCode: ChildProcessSpawner.ExitCode(0),
          stdout,
          stderr: "",
          stdoutTruncated: input.args[0] === "ls-files" && sample === "truncated registration",
          stdoutInvalidUtf8:
            input.args[0] === "ls-files" && sample === "invalid UTF-8 registration",
          stderrTruncated: false,
        };
      });
    const capture = yield* CaptureRepository.make(execute);
    const repository = yield* capture.resolveRepository(parent);
    const result = yield* repository.resolveSubmodule(child).pipe(Effect.result);
    if (sample === "registered") {
      assert.strictEqual(result._tag, "Success");
      if (result._tag === "Success") assert.strictEqual(result.success.gitCommonDir, moduleGitDir);
      const direct = yield* capture.resolveRepository(child).pipe(Effect.result);
      assert.strictEqual(direct._tag, "Failure");
      if (direct._tag === "Failure")
        assert.strictEqual(direct.failure._tag, "VcsPrimaryCheckoutCheckpointError");
    } else {
      assert.strictEqual(result._tag, "Failure");
      if (result._tag === "Failure")
        assert.strictEqual(result.failure._tag, "VcsRepositoryDetectionError");
    }
    assert.isTrue(
      commands.every(
        (command) => command.startsWith("rev-parse ") || command.startsWith("ls-files "),
      ),
    );
  }).pipe(Effect.scoped, Effect.provide(NodeServices.layer)),
);
