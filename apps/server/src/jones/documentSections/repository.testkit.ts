// @effect-diagnostics nodeBuiltinImport:off -- Test fixtures own a unique synthetic repository and its exact finally cleanup.
import { execFileSync } from "node:child_process";
import { mkdtemp, realpath, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import type { DocumentSectionsSelector } from "../../../../../packages/contracts/src/jones/documentSections.ts";
import { boundedGitEnvironment } from "./rawGit.ts";
import { sha256 } from "./pure.ts";

/** The calling test owns this unique synthetic repository until its finally cleanup. */
export async function withRepository<A>(
  text: string | Uint8Array,
  use: (fixture: {
    root: string;
    selector: DocumentSectionsSelector;
    git: (...args: string[]) => string;
  }) => Promise<A>,
): Promise<A> {
  const allocated = await mkdtemp(join(tmpdir(), "jones-document-sections-"));
  try {
    const root = await realpath(allocated);
    const git = (...args: string[]) =>
      execFileSync(
        "git",
        [
          "--no-replace-objects",
          "--literal-pathspecs",
          "-c",
          "core.autocrlf=false",
          "-c",
          `core.hooksPath=${process.platform === "win32" ? "NUL" : "/dev/null"}`,
          "-c",
          "user.name=Synthetic",
          "-c",
          "user.email=synthetic@example.invalid",
          "-C",
          root,
          ...args,
        ],
        {
          env: boundedGitEnvironment(process.env),
          encoding: "utf8",
          stdio: ["ignore", "pipe", "pipe"],
        },
      ).trim();
    git("init", "--object-format=sha1");
    git("remote", "add", "origin", "https://github.com/Synthetic/Document-Test.git");
    const bytes = typeof text === "string" ? Buffer.from(text) : text;
    await writeFile(join(root, "[literal].md"), bytes);
    git("add", "--", "[literal].md");
    git("commit", "-m", "Synthetic document");
    const selector: DocumentSectionsSelector = {
      repositoryIdentity: "github:Synthetic/Document-Test",
      commitOid: git("rev-parse", "HEAD"),
      documentPath: "[literal].md",
      documentSha256: sha256(bytes),
      sectionIds: ["one"],
    };
    return await use({ root, selector, git });
  } finally {
    await rm(allocated, { recursive: true, force: true });
  }
}
