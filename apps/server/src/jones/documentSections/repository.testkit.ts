// @effect-diagnostics nodeBuiltinImport:off -- Test fixtures own a unique synthetic repository and its exact finally cleanup.
import * as NodeChildProcess from "node:child_process";
import * as NodeFSP from "node:fs/promises";
import * as NodeOS from "node:os";
import * as NodePath from "node:path";
import type { DocumentSectionsSelector } from "../../../../../packages/contracts/src/jones/documentSections.ts";
import * as HostProcess from "@t3tools/shared/HostProcess";
import { boundedGitEnvironment } from "./rawGit.ts";
import { sha256 } from "./pure.ts";
import * as Effect from "effect/Effect";

async function makeRepository(text: string | Uint8Array) {
  const allocated = await NodeFSP.mkdtemp(
    NodePath.join(NodeOS.tmpdir(), "jones-document-sections-"),
  );
  let handedOff = false;
  try {
    const root = await NodeFSP.realpath(allocated);
    const git = (...args: string[]) =>
      NodeChildProcess.execFileSync(
        "git",
        [
          "--no-replace-objects",
          "--literal-pathspecs",
          "-c",
          "core.autocrlf=false",
          "-c",
          `core.hooksPath=${HostProcess.Platform.defaultValue() === "win32" ? "NUL" : "/dev/null"}`,
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
    await NodeFSP.writeFile(NodePath.join(root, "[literal].md"), bytes);
    git("add", "--", "[literal].md");
    git("commit", "-m", "Synthetic document");
    const selector: DocumentSectionsSelector = {
      repositoryIdentity: "github:Synthetic/Document-Test",
      commitOid: git("rev-parse", "HEAD"),
      documentPath: "[literal].md",
      documentSha256: sha256(bytes),
      sectionIds: ["one"],
    };
    handedOff = true;
    return {
      root,
      selector,
      git,
      dispose: () => NodeFSP.rm(allocated, { recursive: true, force: true }),
    };
  } finally {
    if (!handedOff) await NodeFSP.rm(allocated, { recursive: true, force: true });
  }
}

export async function withRepository<A>(
  text: string | Uint8Array,
  use: (fixture: Awaited<ReturnType<typeof makeRepository>>) => Promise<A>,
): Promise<A> {
  const fixture = await makeRepository(text);
  try {
    return await use(fixture);
  } finally {
    await fixture.dispose();
  }
}

export const withRepositoryEffect = <A, E, R>(
  text: string | Uint8Array,
  use: (fixture: Awaited<ReturnType<typeof makeRepository>>) => Effect.Effect<A, E, R>,
) =>
  Effect.acquireUseRelease(
    Effect.promise(() => makeRepository(text)),
    use,
    (fixture) => Effect.promise(() => fixture.dispose()),
  );
