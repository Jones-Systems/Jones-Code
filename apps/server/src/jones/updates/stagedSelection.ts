// @effect-diagnostics nodeBuiltinImport:off
// The host retains one selection per active native binding; restart cannot silently select another build.
import * as NodeCrypto from "node:crypto";
import * as NodeFS from "node:fs";
import * as NodeFSP from "node:fs/promises";
import * as NodeOS from "node:os";
import * as NodePath from "node:path";
import {
  currentQualifiedRuntimeBinding,
  decodeStagedQualifiedRuntime,
  verifyStagedQualifiedRuntime,
  type QualifiedRuntimeBinding,
  type StagedQualifiedRuntime,
} from "../cloud/qualifiedRuntime.ts";

function selectionPath(binding: QualifiedRuntimeBinding): string {
  const key = NodeCrypto.createHash("sha256").update(JSON.stringify(binding)).digest("hex");
  return NodePath.join(binding.baseDir, "runtime", "jones-updates", "selections", `${key}.json`);
}

async function readSelection(path: string): Promise<StagedQualifiedRuntime | undefined> {
  const file = await NodeFSP.open(
    path,
    NodeFS.constants.O_RDONLY | NodeFS.constants.O_NOFOLLOW,
  ).catch((error: NodeJS.ErrnoException) => {
    if (error.code === "ENOENT") return undefined;
    throw error;
  });
  if (file === undefined) return undefined;
  try {
    const stat = await file.stat();
    if (
      !stat.isFile() ||
      stat.size > 65536 ||
      (stat.mode & 0o077) !== 0 ||
      stat.uid !== NodeOS.userInfo().uid
    )
      throw new Error("An occupied staged selection has unknown ownership; it was preserved.");
    const selection = decodeStagedQualifiedRuntime(JSON.parse(await file.readFile("utf8")));
    if (selection === undefined)
      throw new Error("An occupied staged selection is invalid; it was preserved.");
    return selection;
  } finally {
    await file.close();
  }
}

export async function retainStagedSelection(selection: StagedQualifiedRuntime): Promise<void> {
  const path = selectionPath(selection.binding);
  await NodeFSP.mkdir(NodePath.dirname(path), { recursive: true, mode: 0o700 });
  const prior = await readSelection(path);
  if (prior !== undefined) {
    if (JSON.stringify(prior) !== JSON.stringify(selection))
      throw new Error(
        "This native binding already retains a different staged handle; it was preserved.",
      );
    return;
  }
  const file = await NodeFSP.open(path, "wx", 0o600);
  try {
    await file.writeFile(JSON.stringify(selection) + "\n");
    await file.sync();
  } finally {
    await file.close();
  }
  const parent = await NodeFSP.open(NodePath.dirname(path), "r");
  try {
    await parent.sync();
  } finally {
    await parent.close();
  }
}

export async function restoreStagedSelection(
  baseDir: string,
  activeVersion: string,
): Promise<StagedQualifiedRuntime | undefined> {
  const binding = await currentQualifiedRuntimeBinding(baseDir, activeVersion);
  const stored = await readSelection(selectionPath(binding));
  if (stored === undefined) return undefined;
  const verified = await verifyStagedQualifiedRuntime(baseDir, activeVersion, stored.stagedHandle);
  if (JSON.stringify(verified) !== JSON.stringify(stored))
    throw new Error("The retained staged selection changed; native installation is held.");
  return verified;
}

/** Retires only an exact unaccepted selection pointer; staged receipts and payload stay intact. */
export async function retireStagedSelection(
  baseDir: string,
  activeVersion: string,
  expected: {
    readonly environmentId: string;
    readonly expectedInstalledSource: string;
    readonly targetSource: string;
    readonly stagedHandle: string;
  },
): Promise<void> {
  const binding = await currentQualifiedRuntimeBinding(baseDir, activeVersion);
  if (
    binding.environmentId !== expected.environmentId ||
    binding.activeSourceSha !== expected.expectedInstalledSource
  )
    throw new Error("The running environment changed; the staged selection was preserved.");
  const path = selectionPath(binding);
  const stored = await readSelection(path);
  if (stored === undefined) return;
  if (
    JSON.stringify(stored.binding) !== JSON.stringify(binding) ||
    stored.stagedHandle !== expected.stagedHandle ||
    stored.receipt.sourceSha !== expected.targetSource
  )
    throw new Error("Another staged selection occupies this binding; it was preserved.");
  const verified = await verifyStagedQualifiedRuntime(baseDir, activeVersion, stored.stagedHandle);
  if (JSON.stringify(verified) !== JSON.stringify(stored))
    throw new Error("The retained stage changed; its pointer was preserved.");
  await NodeFSP.unlink(path);
  const parent = await NodeFSP.open(NodePath.dirname(path), "r");
  try {
    await parent.sync();
  } finally {
    await parent.close();
  }
}
