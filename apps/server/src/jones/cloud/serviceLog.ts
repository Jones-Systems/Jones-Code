// @effect-diagnostics nodeBuiltinImport:off
// Native descriptors provide exclusive creation and no-follow append identity checks.
import * as NodeFS from "node:fs";
import * as NodeFSP from "node:fs/promises";

export async function prepareServiceLog(logPath: string): Promise<void> {
  const created = await NodeFSP.open(logPath, "wx", 0o600).catch((cause: unknown) => {
    if ((cause as NodeJS.ErrnoException).code === "EEXIST") return undefined;
    throw cause;
  });
  if (created !== undefined) {
    await created.close();
    return;
  }
  const existing = await NodeFSP.lstat(logPath);
  if (!existing.isFile()) throw new Error("Service log destination must be a regular file.");
}

export async function appendServiceLog(logPath: string, contents: string): Promise<void> {
  await prepareServiceLog(logPath);
  const expected = await NodeFSP.lstat(logPath);
  if (!expected.isFile()) throw new Error("Service log destination must be a regular file.");
  const handle = await NodeFSP.open(
    logPath,
    NodeFS.constants.O_WRONLY |
      NodeFS.constants.O_APPEND |
      NodeFS.constants.O_NOFOLLOW |
      NodeFS.constants.O_NONBLOCK,
  );
  try {
    const actual = await handle.stat();
    if (!actual.isFile() || actual.dev !== expected.dev || actual.ino !== expected.ino) {
      throw new Error("Service log destination changed before append.");
    }
    await handle.writeFile(contents);
  } finally {
    await handle.close();
  }
}
