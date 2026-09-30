import { HostProcessPlatform } from "@t3tools/shared/hostProcess";
import * as NodeAssert from "node:assert/strict";
// @effect-diagnostics-next-line nodeBuiltinImport:off - These cases need real Node temporary paths, symlinks, and mode bits to exercise filesystem safety.
import * as NodeFS from "node:fs";
import * as NodeOS from "node:os";
// @effect-diagnostics-next-line nodeBuiltinImport:off - The synchronous filesystem cases need native path semantics outside an Effect runtime.
import * as NodePath from "node:path";
import { openConversationLibrary } from "./open.ts";

async function owned(run: (path: string) => Promise<void>): Promise<void> {
  const path = NodeFS.mkdtempSync(NodePath.join(NodeOS.tmpdir(), "t3-library-open-"));
  try {
    await run(path);
  } finally {
    NodeFS.rmSync(path, { recursive: true, force: true });
  }
}

export const libraryOpenCases = [
  {
    name: "a missing read-only library creates no files",
    run: () =>
      owned(async (root) => {
        const store = await openConversationLibrary(root, false, 123);
        try {
          NodeAssert.deepEqual(store.execute({ kind: "accounts" }, false), {
            kind: "accounts",
            accounts: [],
          });
        } finally {
          store.close();
        }
        NodeAssert.equal(NodeFS.existsSync(NodePath.join(root, "conversation-library")), false);
      }),
  },
  {
    name: "creates a private independent library and reopens it read-only",
    run: () =>
      owned(async (root) => {
        NodeFS.writeFileSync(NodePath.join(root, "state.sqlite"), "not the library");
        let store = await openConversationLibrary(root, true, 123);
        try {
          store.execute({ kind: "createAccount", label: "Example", workspace: "Personal" }, true);
        } finally {
          store.close();
        }
        store = await openConversationLibrary(root, false, 456);
        try {
          const reply = store.execute({ kind: "accounts" }, false);
          NodeAssert.equal(reply.kind, "accounts");
          if (reply.kind === "accounts") NodeAssert.equal(reply.accounts.length, 1);
        } finally {
          store.close();
        }
        if (HostProcessPlatform.defaultValue() !== "win32") {
          NodeAssert.equal(
            NodeFS.statSync(NodePath.join(root, "conversation-library")).mode & 0o077,
            0,
          );
          NodeAssert.equal(
            NodeFS.statSync(NodePath.join(root, "conversation-library", "library.sqlite")).mode &
              0o077,
            0,
          );
        }
        NodeAssert.equal(NodeFS.statSync(NodePath.join(root, "state.sqlite")).size, 15);
      }),
  },
  {
    name: "rejects a symlinked library directory",
    run: () =>
      owned(async (root) => {
        const target = NodePath.join(root, "target");
        NodeFS.mkdirSync(target, { mode: 0o700 });
        NodeFS.symlinkSync(target, NodePath.join(root, "conversation-library"), "dir");
        await NodeAssert.rejects(openConversationLibrary(root, true, 123), /private/);
        NodeAssert.equal(NodeFS.existsSync(NodePath.join(target, "library.sqlite")), false);
      }),
  },
  {
    name: "rejects a symlinked database without changing its target",
    run: () =>
      owned(async (root) => {
        const directory = NodePath.join(root, "conversation-library");
        NodeFS.mkdirSync(directory, { mode: 0o700 });
        const target = NodePath.join(root, "unrelated");
        NodeFS.writeFileSync(target, "protected", { mode: 0o600 });
        NodeFS.symlinkSync(target, NodePath.join(directory, "library.sqlite"));
        await NodeAssert.rejects(openConversationLibrary(root, true, 123), /private/);
        NodeAssert.equal(NodeFS.statSync(target).size, 9);
      }),
  },
];
