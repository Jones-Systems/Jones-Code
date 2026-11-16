// @effect-diagnostics nodeBuiltinImport:off -- Verify raw original Git bytes and exact fixture filesystem changes outside the Effect runtime.
import * as NodeCrypto from "node:crypto";
import * as NodeFSP from "node:fs/promises";
import * as NodePath from "node:path";
import { describe, expect, it } from "vite-plus/test";
import {
  boundedGitEnvironment,
  githubIdentityFromOrigin,
  readCommittedDocument,
  type RawGitRunner,
} from "./rawGit.ts";
import { DocumentSectionRefusal } from "./pure.ts";
import { withRepository } from "./repository.testkit.ts";

describe("local exact Git blob reader", () => {
  it("reads literal paths and committed original bytes despite dirty worktree changes", async () => {
    await withRepository("## [one] Original\r\nα\r", async ({ root, selector, git }) => {
      const original = Buffer.from("## [one] Original\r\nα\r");
      await NodeFSP.writeFile(NodePath.join(root, selector.documentPath), "## [one] Dirty\n");
      const result = await readCommittedDocument(root, selector, new AbortController().signal);
      expect(Buffer.from(result.bytes)).toEqual(original);
      expect(result.blobOid).toBe(
        git("rev-parse", `${selector.commitOid}:${selector.documentPath}`),
      );
      expect(result.blobOid).toBe(
        NodeCrypto.createHash("sha1")
          .update(`blob ${original.byteLength}\0`)
          .update(original)
          .digest("hex"),
      );
    });
  });
  it("refuses subdirectories, multiple origins, foreign identity and commit/path ambiguity", async () => {
    await withRepository("## [one] A\n", async ({ root, selector, git }) => {
      const signal = new AbortController().signal;
      await NodeFSP.mkdir(NodePath.join(root, "sub"));
      await expect(
        readCommittedDocument(NodePath.join(root, "sub"), selector, signal),
      ).rejects.toMatchObject({ code: "repository_refused" });
      await expect(
        readCommittedDocument(
          root,
          { ...selector, repositoryIdentity: "github:Foreign/Repo" },
          signal,
        ),
      ).rejects.toMatchObject({ code: "identity_mismatch" });
      await expect(
        readCommittedDocument(root, { ...selector, commitOid: "1".repeat(40) }, signal),
      ).rejects.toMatchObject({ code: "git_failed" });
      await expect(
        readCommittedDocument(root, { ...selector, documentPath: "missing.md" }, signal),
      ).rejects.toMatchObject({ code: "path_refused" });
      git("config", "--add", "remote.origin.url", "ssh://git@github.com/Synthetic/Document-Test");
      await expect(readCommittedDocument(root, selector, signal)).rejects.toMatchObject({
        code: "origin_refused",
      });
    });
  });
  it("ignores replacement refs instead of reading a substitute commit or worktree", async () => {
    await withRepository("## [one] Original\n", async ({ root, selector, git }) => {
      await NodeFSP.writeFile(NodePath.join(root, selector.documentPath), "## [one] Replacement\n");
      git("add", "--", selector.documentPath);
      git("commit", "-m", "Synthetic replacement");
      git("replace", selector.commitOid, git("rev-parse", "HEAD"));
      const result = await readCommittedDocument(root, selector, new AbortController().signal);
      expect(Buffer.from(result.bytes).toString("utf8")).toBe("## [one] Original\n");
    });
  });
  it("refuses unsupported formats, foreign paths, truncation, corrupt bytes and oversized metadata", async () => {
    await withRepository("## [one] A\n", async ({ root, selector }) => {
      const bytes = Buffer.from("## [one] A\n");
      const blob = NodeCrypto.createHash("sha1")
        .update(`blob ${bytes.length}\0`)
        .update(bytes)
        .digest("hex");
      const signal = new AbortController().signal;
      const runner =
        (overrides: Record<string, Uint8Array | string>): RawGitRunner =>
        async (_root, args) => {
          const key =
            args[0] === "rev-parse" ? args[1]! : args[0] === "cat-file" ? args[1]! : args[0]!;
          const defaults: Record<string, string | Uint8Array> = {
            "--show-toplevel": `${root}\n`,
            "--show-object-format": "sha1\n",
            config: "git@github.com:Synthetic/Document-Test.git\0",
            "--verify": `${selector.commitOid}\n`,
            "ls-tree": `100644 blob ${blob}\t${selector.documentPath}\0`,
            "-s": `${bytes.length}\n`,
            blob: bytes,
          };
          const result = overrides[key] ?? defaults[key]!;
          return { exitCode: 0, stdout: typeof result === "string" ? Buffer.from(result) : result };
        };
      for (const [overrides, code] of [
        [{ "--show-object-format": "sha256\n" }, "unsupported_object_format"],
        [{ "--verify": `${"c".repeat(40)}\n` }, "commit_refused"],
        [{ "ls-tree": `100644 blob ${blob}\tforeign.md\0` }, "path_refused"],
        [{ "ls-tree": `120000 blob ${blob}\t${selector.documentPath}\0` }, "path_refused"],
        [{ blob: bytes.subarray(0, -1) }, "blob_refused"],
        [{ blob: Buffer.from("## [one] B\n") }, "blob_refused"],
        [{ "-s": "1048577\n" }, "document_too_large"],
        [{ blob: Buffer.alloc(1048577) }, "git_output_overflow"],
      ] as const)
        await expect(
          readCommittedDocument(root, selector, signal, runner(overrides)),
        ).rejects.toMatchObject({ code });
      const timedOut: RawGitRunner = async () => {
        throw new DocumentSectionRefusal("git_timeout");
      };
      await expect(readCommittedDocument(root, selector, signal, timedOut)).rejects.toMatchObject({
        code: "git_timeout",
      });
    });
  });
  it("allows only supported GitHub origins without normalizing ambiguous URL paths", () => {
    for (const origin of [
      "https://github.com/Owner/Repo.git",
      "git@github.com:Owner/Repo.git",
      "ssh://git@github.com:22/Owner/Repo",
    ])
      expect(githubIdentityFromOrigin(origin)).toBe("github:Owner/Repo");
    for (const origin of [
      "https://secret@github.com/Owner/Repo",
      "https://github.com:443/Owner/Repo",
      "https://github.com/a/../Owner/Repo",
      "https://github.com/Owner/Repo?secret",
      "https://github.com/Owner/Repo#fragment",
      "ssh://user@github.com/Owner/Repo",
      "https://foreign.example/Owner/Repo",
      "/private/local",
      "file:///private/local",
    ])
      expect(githubIdentityFromOrigin(origin)).toBeUndefined();
  });
  it("drops inherited Git overrides, executable injection and private environment variables", () => {
    const env = boundedGitEnvironment({
      PATH: "/usr/bin:/bin",
      GIT_DIR: "/private",
      GIT_WORK_TREE: "/private",
      GIT_CONFIG_COUNT: "1",
      GIT_CONFIG_KEY_0: "remote.origin.url",
      GIT_CONFIG_VALUE_0: "private",
      GIT_ALTERNATE_OBJECT_DIRECTORIES: "/private",
      GIT_CONFIG_PARAMETERS: "private",
      GIT_EXEC_PATH: "/private",
      GIT_TRACE: "/private",
      NODE_OPTIONS: "private",
      PRIVATE_TOKEN: "private",
    });
    expect(env.PATH).toBe("/usr/bin:/bin");
    expect(Object.values(env)).not.toContain("private");
    expect(env.GIT_NO_LAZY_FETCH).toBe("1");
    expect(env.GIT_NO_REPLACE_OBJECTS).toBe("1");
    expect(env.GIT_CONFIG_NOSYSTEM).toBe("1");
    expect(env.GIT_ALLOW_PROTOCOL).toBe("");
  });
});
