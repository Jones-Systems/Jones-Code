// @effect-diagnostics nodeBuiltinImport:off - Detached launcher boundary uses Node built-ins.
/* oxlint-disable t3code/no-global-process-runtime -- Detached launcher has no Effect runtime. */
import * as Crypto from "node:crypto";
import * as FS from "node:fs/promises";
import * as Path from "node:path";
import { constants } from "node:fs";
import { withQualifiedRuntimeLock } from "./qualifiedRuntime.ts";

const hash = (value: string) => Crypto.createHash("sha256").update(value).digest("hex");
const absent = (error: unknown) => (error as NodeJS.ErrnoException).code === "ENOENT";

async function directory(path: string) {
  const info = await FS.lstat(path);
  if (!info.isDirectory() || info.uid !== process.getuid?.() || (await FS.realpath(path)) !== path)
    throw new Error("Unknown runtime retention directory; preserved.");
  return info;
}

async function record(path: string) {
  const file = await FS.open(path, constants.O_RDONLY | constants.O_NOFOLLOW);
  try {
    const info = await file.stat();
    if (!info.isFile() || info.uid !== process.getuid?.() || info.size > 1024 * 1024)
      throw new Error("Unknown runtime retention record; preserved.");
    const raw = await file.readFile("utf8");
    if (Buffer.byteLength(raw) > 1024 * 1024) throw new Error("Runtime record exceeds its bound.");
    return { value: JSON.parse(raw) as unknown, digest: hash(raw), dev: info.dev, ino: info.ino };
  } finally {
    await file.close();
  }
}

/** Caller owns service-launcher-lock; staging takes only the nested qualified-runtime-lock. */
export async function proposeRuntimeStagingRetention(baseDir: string): Promise<void> {
  await withQualifiedRuntimeLock(baseDir, "qualified-runtime-lock", async () => {
    const runtime = Path.join(baseDir, "runtime");
    await directory(runtime);
    const versions = Path.join(runtime, "versions");
    const generations: { version: string; dev: number; ino: number; receiptSha256: string | null }[] = [];
    try {
      await directory(versions);
      const names = (await FS.readdir(versions)).sort();
      if (names.length > 1000) throw new Error("Runtime generation inventory exceeds its bound.");
      for (const version of names) {
        const path = Path.join(versions, version);
        const info = await directory(path);
        const receipt = await record(Path.join(path, ".jones-runtime-receipt.json")).catch((error: unknown) => {
          if (absent(error)) return undefined;
          throw error;
        });
        generations.push({ version, dev: info.dev, ino: info.ino, receiptSha256: receipt?.digest ?? null });
      }
    } catch (error) {
      if (!absent(error)) throw error;
    }
    const references: { path: string; digest: string; versions: string[] }[] = [];
    let referenceEntries = 0;
    const add = (path: string, raw: string, digest: string) => {
      references.push({ path, digest, versions: generations.filter(({ version }) => raw.includes(version)).map(({ version }) => version) });
    };
    // These are the released producer/reference roots: service state, durable
    // operation history, qualified staged handles/selections and paired recovery.
    const scan = async (path: string, depth: number): Promise<void> => {
      await directory(path);
      const names = (await FS.readdir(path)).sort();
      referenceEntries += names.length;
      if (referenceEntries > 1000) throw new Error("Runtime reference inventory exceeds its bound.");
      for (const name of names) {
        const child = Path.join(path, name);
        const info = await FS.lstat(child);
        if (info.isSymbolicLink()) throw new Error("Unknown runtime reference link; preserved.");
        if (info.isDirectory()) {
          if (depth === 0) throw new Error("Runtime reference depth exceeds its bound.");
          await scan(child, depth - 1);
        } else if (name.endsWith(".json")) {
          const value = await record(child);
          add(child, JSON.stringify(value.value), value.digest);
        }
      }
    };
    const state = await record(Path.join(runtime, "service-state.json"));
    add("service-state.json", JSON.stringify(state.value), state.digest);
    const settings = await record(Path.join(baseDir, "userdata", "settings.json")).catch((error: unknown) => {
      if (absent(error)) return undefined;
      throw error;
    });
    const mode = (settings?.value as { storageCleanup?: { installRetention?: unknown } } | undefined)?.storageCleanup?.installRetention ?? "propose";
    if (mode !== "propose" && mode !== "retire") throw new Error("Unknown runtime retention mode; preserved.");
    for (const root of ["staged-updates", "jones-updates/selections", "jones-update-operations", "jones-update-history", "db-backup"]) {
      await scan(Path.join(runtime, root), 3).catch((error: unknown) => {
        if (!absent(error)) throw error;
      });
    }
    for (const name of ["current", "pending"]) {
      const path = Path.join(runtime, "launcher", name);
      const target = await FS.readlink(path).catch((error: unknown) => {
        if (absent(error)) return undefined;
        throw error;
      });
      if (target !== undefined) add(path, target, hash(target));
    }
    // The running launcher's own executable, retained service definitions and
    // external CLI consumers have no complete persisted consumer census. A
    // missing reference is therefore never evidence that a generation is unused.
    const snapshot = { generations, references, settingsSha256: settings?.digest ?? null };
    const plan = {
      protocol: 1,
      operation: "runtime-staging-retention",
      mode,
      closureGeneration: hash(JSON.stringify(snapshot)),
      ...snapshot,
      candidates: [],
      held: generations.map(({ version }) => ({ version, reason: "unknown-runtime-consumers" })),
    };
    const path = Path.join(runtime, "jones-runtime-retention-plan.json");
    const scratch = `${path}.${Crypto.randomUUID()}`;
    try {
      const file = await FS.open(scratch, "wx", 0o600);
      try {
        await file.writeFile(`${JSON.stringify(plan)}\n`);
        await file.sync();
      } finally { await file.close(); }
      await FS.rename(scratch, path);
      const parent = await FS.open(runtime, "r");
      try { await parent.sync(); } finally { await parent.close(); }
    } finally {
      await FS.unlink(scratch).catch((error: unknown) => { if (!absent(error)) throw error; });
    }
  });
}
