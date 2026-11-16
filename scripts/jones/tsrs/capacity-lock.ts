// @effect-diagnostics nodeBuiltinImport:off -- Admission runs before compiler dependencies.
import * as Clock from "effect/Clock";
import * as Effect from "effect/Effect";
import * as NodeCrypto from "node:crypto";
import * as NodeFS from "node:fs";
import * as NodePath from "node:path";

type Phase = "admission" | "producer" | "closed";
type Owner = {
  schema: "jones-compiler-capacity/v1";
  pid: number;
  nonce: string;
  phase: Phase;
};

function absent(pid: number) {
  try {
    process.kill(pid, 0);
    return false;
  } catch (error) {
    if ((error as NodeJS.ErrnoException).code === "ESRCH") return true;
    throw error;
  }
}

function privateInfo(path: string, directory: boolean) {
  const info = NodeFS.lstatSync(path);
  if (
    info.uid !== process.getuid!() ||
    info.mode & 0o077 ||
    info.isSymbolicLink() ||
    (directory ? !info.isDirectory() : !info.isFile() || info.nlink !== 1 || info.size > 8192)
  )
    throw new Error(`Unsafe compiler lock metadata; preserved: ${path}`);
  return info;
}

function snapshot(lock: string) {
  const info = privateInfo(lock, true);
  const path = NodePath.join(lock, "owner.json");
  privateInfo(path, false);
  const text = NodeFS.readFileSync(path, "utf8");
  let value: unknown;
  try {
    value = JSON.parse(text);
  } catch (error) {
    throw new Error(`Invalid compiler lock owner; preserved: ${lock}`, { cause: error });
  }
  if (
    typeof value !== "object" ||
    value === null ||
    !("pid" in value) ||
    typeof value.pid !== "number" ||
    !Number.isSafeInteger(value.pid) ||
    value.pid < 1 ||
    !("nonce" in value) ||
    typeof value.nonce !== "string" ||
    !/^[a-f0-9-]{36}$/.test(value.nonce)
  )
    throw new Error(`Invalid compiler lock owner; preserved: ${lock}`);
  return { info, text, value: { ...value, pid: value.pid, nonce: value.nonce } };
}

function same(lock: string, previous: ReturnType<typeof snapshot>) {
  let current: ReturnType<typeof snapshot>;
  try {
    current = snapshot(lock);
  } catch (error) {
    if ((error as NodeJS.ErrnoException).code === "ENOENT") return false;
    throw error;
  }
  return (
    current.info.dev === previous.info.dev &&
    current.info.ino === previous.info.ino &&
    current.text === previous.text
  );
}

/** Only a recorded pre-producer or native-closed state permits abandoned recovery.
 * PID absence alone never proves that a producer (or its descendants) stopped.
 */
function recover(lock: string) {
  let previous: ReturnType<typeof snapshot>;
  try {
    previous = snapshot(lock);
  } catch (error) {
    // A creator may still be publishing its owner record, or a releaser removing it.
    if ((error as NodeJS.ErrnoException).code === "ENOENT") {
      let info;
      try {
        info = privateInfo(lock, true);
      } catch (missing) {
        if ((missing as NodeJS.ErrnoException).code === "ENOENT") return;
        throw missing;
      }
      // Age only ends the initialization grace period; it never permits recovery.
      if (Effect.runSync(Clock.currentTimeMillis) - info.mtimeMs > 2000)
        throw new Error(
          `Compiler lock owner record unavailable; preserved: ${lock}. Reconcile interrupted initialization/recovery.`,
          { cause: error },
        );
      return;
    }
    throw error;
  }
  const { value } = previous;
  if (!absent(value.pid)) return;
  if (
    !("schema" in value) ||
    value.schema !== "jones-compiler-capacity/v1" ||
    !("phase" in value) ||
    !["admission", "closed"].includes(String(value.phase))
  )
    throw new Error(
      `Abandoned compiler lock has no producer-closure proof; preserved: ${lock}. Owner PID ${value.pid}. Legacy/producer locks require an ownership and native-closure audit; do not delete them based only on PID absence.`,
    );
  const claim = NodePath.join(NodePath.dirname(lock), `reclaim-${value.nonce}`);
  // One exclusive claim for this generation. A crashed reclaimer leaves an
  // explicit hold, rather than letting a second reclaimer delete its successor.
  let fd: number;
  try {
    fd = NodeFS.openSync(claim, "wx", 0o600);
  } catch (error) {
    if ((error as NodeJS.ErrnoException).code === "EEXIST")
      throw new Error(`Compiler lock recovery already claimed; preserved: ${claim}`, {
        cause: error,
      });
    throw error;
  }
  NodeFS.closeSync(fd);
  try {
    if (!same(lock, previous)) return;
    if (NodeFS.readdirSync(lock).some((name) => name !== "owner.json"))
      throw new Error(`Compiler lock contains unrecognized recovery state; preserved: ${lock}`);
    NodeFS.unlinkSync(NodePath.join(lock, "owner.json"));
    NodeFS.rmdirSync(lock);
  } finally {
    NodeFS.unlinkSync(claim);
  }
}

export function tryCapacityLock(directory: string) {
  const lock = NodePath.join(directory, "build.lock");
  try {
    NodeFS.mkdirSync(lock, { mode: 0o700 });
  } catch (error) {
    if ((error as NodeJS.ErrnoException).code !== "EEXIST") throw error;
    recover(lock);
    return undefined;
  }
  const info = privateInfo(lock, true);
  let owner: Owner = {
    schema: "jones-compiler-capacity/v1",
    pid: process.pid,
    nonce: NodeCrypto.randomUUID(),
    phase: "admission",
  };
  const path = NodePath.join(lock, "owner.json");
  const temporary = NodePath.join(lock, `owner.next-${owner.nonce}`);
  let text = JSON.stringify(owner);
  NodeFS.writeFileSync(temporary, text, { flag: "wx", mode: 0o600 });
  NodeFS.renameSync(temporary, path);
  let held = true;
  const assertOwned = () => {
    if (!held || !same(lock, { info, text, value: owner }))
      throw new Error(`Compiler lock ownership changed; preserved: ${lock}`);
  };
  const update = (phase: Phase) => {
    assertOwned();
    const next = { ...owner, phase };
    const nextText = JSON.stringify(next);
    NodeFS.writeFileSync(temporary, nextText, { flag: "wx", mode: 0o600 });
    NodeFS.renameSync(temporary, path);
    owner = next;
    text = nextText;
  };
  return {
    lock,
    producer: () => update("producer"),
    /** Call only after native completion, or proof that no producer was started. */
    confirmClosure: () => update("closed"),
    release: () => {
      if (!held) return;
      assertOwned();
      NodeFS.unlinkSync(path);
      NodeFS.rmdirSync(lock);
      held = false;
    },
    get closed() {
      return owner.phase === "closed";
    },
  };
}
