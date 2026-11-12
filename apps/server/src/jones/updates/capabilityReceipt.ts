// @effect-diagnostics nodeBuiltinImport:off
// Host adoption reads this receipt without opening the authenticated runtime store.
import * as NodeFS from "node:fs";
import * as NodeFSP from "node:fs/promises";
import * as NodePath from "node:path";
import * as NodeOS from "node:os";
import * as NodeCrypto from "node:crypto";

export interface JonesUpdateCapabilityReceipt {
  readonly schema: 1;
  readonly baseDir: string;
  readonly environmentId: string;
  readonly currentVersion: string;
  readonly processId: number;
  readonly capability: { readonly install: boolean };
  readonly qualifiedLauncher: boolean;
}

export async function publishJonesUpdateCapabilityReceipt(
  receipt: JonesUpdateCapabilityReceipt,
): Promise<void> {
  const directory = NodePath.join(receipt.baseDir, "runtime");
  const target = NodePath.join(directory, "jones-update-capability.json");
  await NodeFSP.mkdir(directory, { recursive: true, mode: 0o700 });
  const prior = await NodeFSP.open(
    target,
    NodeFS.constants.O_RDONLY | NodeFS.constants.O_NOFOLLOW,
  ).catch((cause: NodeJS.ErrnoException) => {
    if (cause.code === "ENOENT") return undefined;
    throw cause;
  });
  if (prior !== undefined) {
    try {
      const stat = await prior.stat();
      if (
        !stat.isFile() ||
        stat.uid !== NodeOS.userInfo().uid ||
        (stat.mode & 0o077) !== 0 ||
        stat.size > 4096
      )
        throw new Error("Update capability receipt has unknown ownership.");
      const previous = JSON.parse(await prior.readFile("utf8"));
      if (
        previous.schema !== 1 ||
        previous.baseDir !== receipt.baseDir ||
        previous.environmentId !== receipt.environmentId
      )
        throw new Error("Update capability receipt has a different native identity.");
    } finally {
      await prior.close();
    }
  }
  const scratch = NodePath.join(directory, `.jones-capability-${NodeCrypto.randomUUID()}`);
  const handle = await NodeFSP.open(scratch, "wx", 0o600);
  try {
    await handle.writeFile(`${JSON.stringify(receipt)}\n`);
    await handle.sync();
    await handle.close();
    await NodeFSP.rename(scratch, target);
  } finally {
    await handle.close();
    await NodeFSP.rm(scratch, { force: true });
  }
}
