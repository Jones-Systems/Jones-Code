// @effect-diagnostics nodeBuiltinImport:off
// Durable receipts live outside userdata so a native rollback cannot erase dispatch history.
import * as Fs from "node:fs/promises";
import * as Path from "node:path";
import { randomUUID } from "node:crypto";
import { constants } from "node:fs";
import * as Schema from "effect/Schema";
import {
  FleetEnrollment,
  FleetHostOperation,
  FleetOperationId,
} from "@t3tools/contracts/jones/fleet-updates";
import { withQualifiedRuntimeLock } from "../cloud/qualifiedRuntime.ts";

export interface FleetHostStore {
  readonly readEnrollment: () => Promise<FleetEnrollment | null>;
  readonly editEnrollment: (
    change: (old: FleetEnrollment | null) => FleetEnrollment,
  ) => Promise<FleetEnrollment>;
  readonly readOperation: (id: string) => Promise<FleetHostOperation | null>;
  readonly editOperation: (
    id: string,
    change: (old: FleetHostOperation | null) => FleetHostOperation,
  ) => Promise<FleetHostOperation>;
}

const isEnrollment = Schema.is(FleetEnrollment);
const isOperation = Schema.is(FleetHostOperation);
const isOperationId = Schema.is(FleetOperationId);

async function ownedDirectory(path: string, privateMode: boolean) {
  await Fs.mkdir(path, { recursive: true, mode: 0o700 });
  const stat = await Fs.lstat(path);
  if (
    !stat.isDirectory() ||
    stat.isSymbolicLink() ||
    (stat.mode & (privateMode ? 0o077 : 0o022)) !== 0 ||
    (typeof process.getuid === "function" && stat.uid !== process.getuid())
  ) {
    throw new Error("Fleet receipt directory ownership is unknown.");
  }
  const parent = await Fs.open(Path.dirname(path), "r");
  try {
    await parent.sync();
  } finally {
    await parent.close();
  }
}

async function read<T>(path: string, valid: (value: unknown) => value is T): Promise<T | null> {
  const file = await Fs.open(path, constants.O_RDONLY | constants.O_NOFOLLOW).catch(
    (error: NodeJS.ErrnoException) => {
      if (error.code === "ENOENT") return null;
      throw error;
    },
  );
  if (file === null) return null;
  try {
    const stat = await file.stat();
    if (
      !stat.isFile() ||
      stat.nlink !== 1 ||
      stat.size > 64 * 1024 ||
      (stat.mode & 0o077) !== 0 ||
      (typeof process.getuid === "function" && stat.uid !== process.getuid())
    ) {
      throw new Error("Fleet receipt ownership or size is invalid.");
    }
    const value: unknown = JSON.parse(await file.readFile("utf8"));
    if (!valid(value))
      throw new Error("Fleet receipt is invalid; it cannot be replaced automatically.");
    return value;
  } finally {
    await file.close();
  }
}

async function replace(path: string, value: unknown) {
  const temporary = `${path}.${randomUUID()}.tmp`;
  const file = await Fs.open(temporary, "wx", 0o600);
  try {
    try {
      await file.writeFile(`${JSON.stringify(value)}\n`);
      await file.sync();
    } finally {
      await file.close();
    }
    await Fs.rename(temporary, path);
    const directory = await Fs.open(Path.dirname(path), "r");
    try {
      await directory.sync();
    } finally {
      await directory.close();
    }
  } finally {
    await Fs.unlink(temporary).catch((error: NodeJS.ErrnoException) => {
      if (error.code !== "ENOENT") throw error;
    });
  }
}

export function createFleetHostStore(baseDir: string): FleetHostStore {
  const runtime = Path.join(baseDir, "runtime");
  const directory = Path.join(runtime, "jones-fleet");
  const enrollment = Path.join(directory, "enrollment.json");
  const operationPath = (id: string) => {
    if (!isOperationId(id)) throw new Error("Invalid fleet operation identifier.");
    return Path.join(directory, `${id}.json`);
  };
  const locked = async <T>(operation: () => Promise<T>) => {
    await ownedDirectory(runtime, false);
    await ownedDirectory(directory, true);
    return withQualifiedRuntimeLock(baseDir, "jones-fleet", operation);
  };
  return {
    readEnrollment: () => locked(() => read(enrollment, isEnrollment)),
    editEnrollment: (change) =>
      locked(async () => {
        const next = change(await read(enrollment, isEnrollment));
        if (!isEnrollment(next)) throw new Error("Invalid fleet enrollment.");
        await replace(enrollment, next);
        return next;
      }),
    readOperation: (id) => locked(() => read(operationPath(id), isOperation)),
    editOperation: (id, change) =>
      locked(async () => {
        const path = operationPath(id);
        const next = change(await read(path, isOperation));
        if (!isOperation(next) || next.input.operationId !== id)
          throw new Error("Invalid fleet operation.");
        await replace(path, next);
        return next;
      }),
  };
}
