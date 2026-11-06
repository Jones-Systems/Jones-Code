// @effect-diagnostics nodeBuiltinImport:off -- Disposable repositories exercise the real adapters without provider or app state.
import * as FS from "node:fs/promises";
import * as Path from "node:path";
import * as OS from "node:os";
import { execFile } from "node:child_process";
import { binding, attest, contentManifest, type Attestation } from "./OutputAttestation.ts";
import { canonical } from "./Filesystem.ts";
import { participantGuard } from "./ParticipantGuard.ts";
import { qualify, type QualificationInput } from "./Qualification.ts";
import type { ConsumerEvidence } from "./ConsumerProbe.ts";
import type { Preservation } from "./IgnoredClassifier.ts";

export type Recipe = Record<string, unknown>;
export function command(cwd: string, args: string[]) {
  return new Promise<string>((resolve, reject) =>
    execFile(
      "git",
      ["-C", cwd, ...args],
      {
        encoding: "utf8",
        timeout: 10000,
        maxBuffer: 1024 * 1024,
        env: {
          PATH: process.env.PATH,
          HOME: cwd,
          GIT_CONFIG_NOSYSTEM: "1",
          GIT_CONFIG_GLOBAL: "/dev/null",
          GIT_NO_LAZY_FETCH: "1",
          GIT_AUTHOR_NAME: "Fixture",
          GIT_AUTHOR_EMAIL: "fixture@example.invalid",
          GIT_COMMITTER_NAME: "Fixture",
          GIT_COMMITTER_EMAIL: "fixture@example.invalid",
          GIT_CONFIG_COUNT: "0",
        },
      },
      (error, stdout) => (error ? reject(error) : resolve(stdout.trim())),
    ),
  );
}
export function completeConsumers(now = Date.now()): ConsumerEvidence {
  const probe = () => ({
    observedAt: now,
    status: "complete" as const,
    paths: [],
    limits: ["synthetic supplied adapter"],
  });
  return {
    native: probe(),
    services: probe(),
    registry: probe(),
    workloads: probe(),
    obligations: probe(),
    process: probe(),
  };
}
export async function fixture<T>(
  run: (f: {
    base: string;
    repo: string;
    worktree: string;
    input: QualificationInput;
  }) => Promise<T>,
) {
  const base = await FS.realpath(await FS.mkdtemp(Path.join(OS.tmpdir(), "jones-swl-j2-")));
  try {
    const repo = Path.join(base, "repo"),
      worktree = Path.join(base, "checkout");
    await FS.mkdir(repo);
    await command(repo, ["init", "-b", "main"]);
    await FS.writeFile(Path.join(repo, "tracked"), "original\n");
    await FS.writeFile(
      Path.join(repo, ".gitignore"),
      "node_modules/\ninstall/\ncache/\n.venv/\n__pycache__/\n.DS_Store\nbuild/\nnotes/\n.t3/\n",
    );
    await command(repo, ["add", "."]);
    await command(repo, ["commit", "-m", "base"]);
    await command(repo, ["worktree", "add", "-b", "task", worktree]);
    const expected = await binding(worktree);
    return await run({
      base,
      repo,
      worktree,
      input: {
        worktree,
        expected,
        projectRoots: [repo],
        durableRepositories: [expected.git_common_dir],
        temporaryRoots: [],
        nativeRecreation: "supported",
        consumers: completeConsumers(),
      },
    });
  } finally {
    await FS.rm(base, { recursive: true, force: true });
  }
}
export async function executeRecipe(recipe: Recipe) {
  return fixture(async ({ base, repo, worktree, input }) => {
    const g = (args: string[]) => command(worktree, args);
    const write = async (relative: string, bytes: string | Buffer) => {
      const path = Path.join(worktree, relative);
      await FS.mkdir(Path.dirname(path), { recursive: true });
      await FS.writeFile(path, bytes);
      return path;
    };
    const commit = async () => {
      await g(["add", "."]);
      await g(["commit", "-m", "fixture change"]);
    };
    if (recipe.anchored === false) input.durableRepositories = [];
    if (recipe.head === "own_branch_only" || recipe.head === "squash_then_commits") {
      await write("tracked", "own branch\n");
      await commit();
      if (recipe.head === "squash_then_commits") {
        await command(repo, ["merge", "--squash", "task"]);
        await command(repo, ["commit", "-m", "squashed"]);
        await write("tracked", "follow-up\n");
        await commit();
      }
    }
    if (recipe.head === "detached_contained" || recipe.head === "detached_unique") {
      await g(["checkout", "--detach"]);
      if (recipe.head === "detached_unique") {
        await write("tracked", "detached\n");
        await commit();
      }
    }
    if (recipe.change === "tracked" || recipe.change === "staged") {
      await write("tracked", "edited\n");
      if (recipe.change === "staged") await g(["add", "tracked"]);
    }
    if (recipe.change === "config_hidden_untracked") {
      await g(["config", "status.showUntrackedFiles", "no"]);
      await write("untracked", "unique\n");
    }
    if (recipe.change === "assume_unchanged_modified") {
      await g(["update-index", "--assume-unchanged", "tracked"]);
      await write("tracked", "hidden edit\n");
    }
    if (recipe.change === "MERGE_HEAD")
      await FS.writeFile(
        Path.join(input.expected.git_dir, "MERGE_HEAD"),
        await g(["rev-parse", "HEAD"]),
      );
    if (recipe.change === "locked") await command(repo, ["worktree", "lock", worktree]);
    for (const name of (recipe.operation_files as string[]) ?? [])
      await FS.writeFile(Path.join(input.expected.git_dir, name), "stale\n");
    if (recipe.missing_path) {
      await FS.rename(worktree, Path.join(base, "missing"));
      return qualify(input);
    }
    if (recipe.reflog_tips) {
      const tree = await g(["rev-parse", "HEAD^{tree}"]),
        head = await g(["rev-parse", "HEAD"]);
      for (let i = 0; i < Number(recipe.reflog_tips); i++) {
        const tip = await g(["commit-tree", tree, "-p", head, "-m", "orphan " + i]);
        await g(["update-ref", "HEAD", tip]);
        await g(["update-ref", "HEAD", head]);
      }
    }
    for (const ref of (recipe.per_worktree_refs as string[]) ?? [])
      await g(["update-ref", ref, await g(["rev-parse", "HEAD"])]);
    if (recipe.primary_checkout) {
      input.worktree = repo;
      input.expected = await binding(repo);
      input.projectRoots = [];
    }
    if (recipe.contains_project_root)
      input.projectRoots = [Path.join(worktree, "registered-project")];
    if (recipe.tracked_lockfile) {
      await write("package.json", "{}\n");
      await write("pnpm-lock.yaml", "lockfileVersion: 9\n");
      await commit();
      await command(repo, ["update-ref", "refs/heads/main", await g(["rev-parse", "HEAD"])]);
    }
    if (recipe.tracked_source) {
      await write("module.py", "value = 'tracked source'\n");
      await commit();
      await command(repo, ["update-ref", "refs/heads/main", await g(["rev-parse", "HEAD"])]);
    }
    let root: string | undefined;
    if (typeof recipe.ignored === "string") {
      root = Path.join(worktree, recipe.ignored);
      if (recipe.ignored === ".DS_Store")
        await write(".DS_Store", Buffer.from([0, 0, 0, 1, ...Buffer.from("Bud1")]));
      else {
        await FS.mkdir(root, { recursive: true });
        await write(recipe.ignored + "/output", "original output\n");
        if (recipe.ignored === "cache" || recipe.cachedir_tag)
          await write(
            recipe.ignored + "/CACHEDIR.TAG",
            "Signature: 8a477f597d28d172789f06886806bc55\n",
          );
        if (recipe.pyvenv) await write(recipe.ignored + "/pyvenv.cfg", "fixture\n");
        if (recipe.ignored === "__pycache__") {
          if (recipe.valid_pyc) {
            const source = Path.join(base, "foreign.py"),
              bytecode = Path.join(root, "module.cpython-fixture.pyc");
            await FS.writeFile(
              source,
              recipe.bytecode_source === "different"
                ? "value = 'different source'\n"
                : "value = 'tracked source'\n",
            );
            await new Promise<void>((resolve, reject) =>
              execFile(
                process.platform === "darwin" ? "/usr/bin/python3" : "python3",
                [
                  "-B",
                  "-c",
                  "import py_compile,sys; py_compile.compile(sys.argv[1],cfile=sys.argv[2],dfile=sys.argv[3],doraise=True)",
                  source,
                  bytecode,
                  Path.join(worktree, "module.py"),
                ],
                { timeout: 5000, maxBuffer: 65536 },
                (error) => (error ? reject(error) : resolve()),
              ),
            );
          } else await write("__pycache__/module.pyc", Buffer.from("unattested bytecode"));
          if (recipe.foreign_file) await write("__pycache__/notes", "unique\n");
        }
        for (const [path, value] of Object.entries(
          (recipe.files as Record<string, string>) ?? {},
        )) {
          const file = await write(recipe.ignored + "/" + path, value);
          if (value === "mode-000") await FS.chmod(file, 0);
        }
      }
    }
    input.coverage = {
      classes: (recipe.coverage_classes as string[]) ?? [],
      exactRoots: ((recipe.coverage_roots as string[]) ?? []).map((p) => Path.join(worktree, p)),
    };
    if (root && recipe.attestation) {
      const record = await attest({
        worktree,
        root,
        producer: "fixture",
        status: "succeeded",
        participantGuard,
      });
      input.attestations = { [root]: record.sidecar };
      if (recipe.attestation === "unverifiable") {
        const value: Attestation = {
          ...record,
          roots: record.roots.map((r) => ({
            ...r,
            verification: "unverifiable",
            content_manifest_sha256: null,
          })),
          manifest: null,
        };
        await FS.writeFile(record.sidecar, canonical(value));
      }
    }
    if (root && recipe.mutation) {
      const file = Path.join(root, "output"),
        before = await FS.stat(file);
      if (recipe.mutation === "same_size_restored_mtime") {
        await FS.writeFile(file, "modified output\n");
        await FS.utimes(file, before.atime, before.mtime);
      }
      if (recipe.mutation === "added_file")
        await FS.writeFile(Path.join(root, "extra"), "addition\n");
      if (recipe.mutation === "edited_notes")
        await FS.writeFile(file, "unique handwritten notes\n");
    }
    if (root && recipe.nested_git) {
      const nested = Path.join(root, "nested");
      await FS.mkdir(nested);
      if (recipe.shared_common_store) {
        await command(nested, ["init", "-b", "main"]);
        await FS.writeFile(Path.join(nested, "file"), "fixture\n");
        await command(nested, ["add", "."]);
        await command(nested, ["commit", "-m", "base"]);
        await command(nested, [
          "worktree",
          "add",
          "-b",
          "linked",
          Path.join(base, "external-linked"),
        ]);
      } else if (recipe.nested_git === "gitfile")
        await FS.writeFile(Path.join(nested, ".git"), "gitdir: /nonexistent/fixture\n");
      else await FS.mkdir(Path.join(nested, ".git"));
    }
    if (recipe.submodule) {
      const submodule = Path.join(worktree, "submodule");
      await FS.mkdir(submodule);
      let head = await g(["rev-parse", "HEAD"]);
      if (recipe.submodule === "populated") {
        await command(submodule, ["init", "-b", "main"]);
        await FS.writeFile(Path.join(submodule, "file"), "retained\n");
        await command(submodule, ["add", "."]);
        await command(submodule, ["commit", "-m", "submodule"]);
        head = await command(submodule, ["rev-parse", "HEAD"]);
      }
      await g(["update-index", "--add", "--cacheinfo", "160000," + head + ",submodule"]);
      await g(["commit", "-m", "gitlink"]);
      await command(repo, ["update-ref", "refs/heads/main", await g(["rev-parse", "HEAD"])]);
    }
    if (recipe.admin_modules)
      await FS.mkdir(Path.join(input.expected.git_dir, "modules/retained"), { recursive: true });
    if (recipe.worktree_config) {
      await command(repo, ["config", "extensions.worktreeConfig", "true"]);
      for (const [key, value] of Object.entries(recipe.worktree_config as Record<string, string>))
        await g(["config", "--worktree", key, value]);
      if (recipe.sparse_patterns) {
        await FS.mkdir(Path.join(input.expected.git_dir, "info"), { recursive: true });
        await FS.writeFile(
          Path.join(input.expected.git_dir, "info/sparse-checkout"),
          (recipe.sparse_patterns as string[]).join("\n") + "\n",
        );
        await g(["read-tree", "-mu", "HEAD"]);
      }
    }
    if (recipe.alternates) {
      const alternate = Path.join(base, "temporary-objects");
      await FS.mkdir(alternate);
      await FS.writeFile(
        Path.join(input.expected.git_common_dir, "objects/info/alternates"),
        alternate + "\n",
      );
      input.temporaryRoots = [alternate];
    }
    if (recipe.preservation) {
      const source = await write("notes/evidence", "retained evidence\n");
      const sourceRoot = Path.dirname(source),
        destination = Path.join(base, "preserved");
      await FS.cp(sourceRoot, destination, { recursive: true });
      const content = await contentManifest(destination),
        info = await FS.stat(destination);
      const record: Preservation = {
        source: sourceRoot,
        destination,
        destinationIdentity: { dev: info.dev, ino: info.ino },
        manifestSha256: content.observation.content_manifest_sha256!,
        sourceManifestSha256: content.observation.content_manifest_sha256!,
        mode: recipe.preservation === "verified_move" ? "move" : "copy",
        retainedOwner: "fixture-owner",
        recordPath: Path.join(base, "preservation.json"),
      };
      if (record.mode === "move") await FS.rm(sourceRoot, { recursive: true });
      if (recipe.preservation === "copy_source_changed")
        await FS.writeFile(source, "changed evidence\n");
      await FS.writeFile(record.recordPath, canonical(record));
      input.preservations = [record];
    }
    if (recipe.legacy_mark || recipe.cleanup_candidate)
      await FS.writeFile(Path.join(input.expected.git_dir, "legacy-cleanup-mark"), "candidate\n");
    input.consumers = completeConsumers();
    if (recipe.obligation)
      input.consumers.obligations = { ...input.consumers.obligations, paths: [worktree] };
    return qualify(input);
  });
}
