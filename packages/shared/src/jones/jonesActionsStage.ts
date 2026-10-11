// @effect-diagnostics nodeBuiltinImport:off - Immutable updater staging and exclusive completion publication.
import * as NodeFSP from "node:fs/promises";
import * as NodePath from "node:path";

const HANDLE = /^[a-f0-9]{64}$/;
const ATTEMPT = /^stage-[a-zA-Z0-9]+$/;

function requireHandle(handle: string): void {
  if (!HANDLE.test(handle)) throw new Error("Invalid Jones stage handle.");
}

async function directory(path: string): Promise<string> {
  const resolved = NodePath.resolve(path);
  const info = await NodeFSP.lstat(resolved);
  if (!info.isDirectory() || (await NodeFSP.realpath(resolved)) !== resolved)
    throw new Error("The Jones stage directory is not a canonical directory.");
  return resolved;
}

async function childDirectory(root: string, name: string): Promise<string> {
  const child = NodePath.join(root, name);
  await NodeFSP.mkdir(child, { recursive: true, mode: 0o700 });
  return directory(child);
}

async function syncDirectory(path: string): Promise<void> {
  const stream = await NodeFSP.open(path, "r");
  try {
    await stream.sync();
  } finally {
    await stream.close();
  }
}

/** Accept legacy completed stages and immutable attempts; never follow an index outside its root. */
export async function requireJonesStageDirectory(
  root: string,
  handle: string,
  path: string,
): Promise<string> {
  requireHandle(handle);
  const canonicalRoot = await directory(root);
  const relative = NodePath.relative(canonicalRoot, path).split(NodePath.sep);
  if (
    !(
      (relative.length === 1 && relative[0] === handle) ||
      (relative.length === 3 &&
        relative[0] === "attempts" &&
        ATTEMPT.test(relative[1] ?? "") &&
        relative[2] === handle)
    )
  )
    throw new Error("The Jones stage is outside its qualified root.");
  return directory(path);
}

export async function findJonesCompletedStage(
  root: string,
  handle: string,
  receiptName: string,
): Promise<string | undefined> {
  requireHandle(handle);
  const canonicalRoot = await directory(root);
  const index = NodePath.join(canonicalRoot, "completed", `${handle}.json`);
  let info;
  try {
    info = await NodeFSP.lstat(index);
  } catch (cause) {
    if ((cause as NodeJS.ErrnoException).code !== "ENOENT") throw cause;
  }
  if (info !== undefined) {
    await directory(NodePath.dirname(index));
    if (!info.isFile() || info.size > 4096) throw new Error("Unknown Jones completion index.");
    const value: unknown = JSON.parse(await NodeFSP.readFile(index, "utf8"));
    if (
      value === null ||
      typeof value !== "object" ||
      !("schema" in value) ||
      value.schema !== 1 ||
      !("handle" in value) ||
      value.handle !== handle ||
      !("directory" in value) ||
      typeof value.directory !== "string"
    )
      throw new Error("Invalid Jones completion index.");
    return requireJonesStageDirectory(canonicalRoot, handle, value.directory);
  }
  const legacy = NodePath.join(canonicalRoot, handle);
  try {
    await requireJonesStageDirectory(canonicalRoot, handle, legacy);
    await NodeFSP.lstat(NodePath.join(legacy, receiptName));
    return legacy;
  } catch (cause) {
    // A legacy directory without a completion receipt is preserved, not overwritten or reused.
    if ((cause as NodeJS.ErrnoException).code === "ENOENT") return undefined;
    throw cause;
  }
}

export async function createJonesStageAttempt(root: string, handle: string) {
  requireHandle(handle);
  const canonicalRoot = await directory(root);
  const attempts = await childDirectory(canonicalRoot, "attempts");
  const attemptRoot = await NodeFSP.mkdtemp(NodePath.join(attempts, "stage-"));
  const path = NodePath.join(attemptRoot, handle);
  try {
    await NodeFSP.mkdir(path, { mode: 0o700 });
    return { directory: path, attemptRoot };
  } catch (cause) {
    try {
      await NodeFSP.rmdir(attemptRoot);
    } catch (cleanup) {
      throw new AggregateError([cause, cleanup], "Stage creation and cleanup failed.");
    }
    throw cause;
  }
}

/** The index is visible only as a complete file. A competing or unknown index is never replaced. */
export async function publishJonesCompletedStage(
  root: string,
  handle: string,
  path: string,
  receiptName: string,
): Promise<string> {
  const canonicalRoot = await directory(root);
  await requireJonesStageDirectory(canonicalRoot, handle, path);
  if (!(await NodeFSP.lstat(NodePath.join(path, receiptName))).isFile())
    throw new Error("The Jones stage has no ordinary completion receipt.");
  const completed = await childDirectory(canonicalRoot, "completed");
  const index = NodePath.join(completed, `${handle}.json`);
  const temporary = NodePath.join(NodePath.dirname(path), "completion.json");
  const file = await NodeFSP.open(temporary, "wx", 0o600);
  try {
    await file.writeFile(JSON.stringify({ schema: 1, handle, directory: path }) + "\n", "utf8");
    await file.sync();
  } finally {
    await file.close();
  }
  await syncDirectory(path);
  await syncDirectory(NodePath.dirname(path));
  await syncDirectory(NodePath.dirname(NodePath.dirname(path)));
  try {
    await NodeFSP.link(temporary, index);
  } catch (cause) {
    if ((cause as NodeJS.ErrnoException).code !== "EEXIST") throw cause;
  }
  await syncDirectory(completed);
  await syncDirectory(canonicalRoot);
  const selected = await findJonesCompletedStage(canonicalRoot, handle, receiptName);
  if (selected === undefined) throw new Error("Jones completion publication could not be read back.");
  return selected;
}
