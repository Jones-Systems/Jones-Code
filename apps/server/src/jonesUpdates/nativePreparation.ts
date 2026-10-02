// @effect-diagnostics nodeBuiltinImport:off
// Native helper protocol owns durable files shared with the detached Python process.
import * as NodeFSP from "node:fs/promises";
import * as NodePath from "node:path";
import * as Schema from "effect/Schema";

const Manifest = Schema.Struct({
  protocol: Schema.Literal(1),
  owner: Schema.Literal("desktop"),
  home: Schema.String,
  databasePath: Schema.String,
  profile: Schema.String,
  environmentId: Schema.String,
  version: Schema.String,
});

const decodeManifest = Schema.decodeUnknownSync(Schema.fromJsonString(Manifest));

export async function prepareNativeContinuationReceipt(input: {
  home: string;
  databasePath: string;
  environmentId: string;
  version: string;
  handle: string;
  prepare: () => Promise<ReadonlyArray<string>>;
  clear: (threadIds: ReadonlyArray<string>) => Promise<void>;
}): Promise<void> {
  if (!/^[a-f0-9]{64}$/.test(input.handle)) throw new Error("Invalid staged handle.");
  const manifest = decodeManifest(
    await NodeFSP.readFile(
      NodePath.join(input.home, "runtime", "jones-active-install.json"),
      "utf8",
    ),
  );
  const home = await NodeFSP.realpath(input.home);
  const databasePath = await NodeFSP.realpath(input.databasePath);
  if (
    manifest.home !== home ||
    manifest.databasePath !== databasePath ||
    manifest.environmentId !== input.environmentId ||
    manifest.version !== input.version
  ) {
    throw new Error("The active native manifest changed before preparation.");
  }
  const directory = NodePath.join(home, "runtime", "jones-updates", "transactions", input.handle);
  const receiptPath = NodePath.join(directory, "continuation.json");
  const receipt = {
    protocol: 1,
    transactionId: input.handle,
    home,
    databasePath,
    profile: manifest.profile,
    environmentId: input.environmentId,
    prepared: true,
  };
  const encoded = JSON.stringify(receipt) + "\n";
  const prior = await NodeFSP.readFile(receiptPath, "utf8").catch(
    (error: NodeJS.ErrnoException) => {
      if (error.code === "ENOENT") return undefined;
      throw error;
    },
  );
  if (prior !== undefined) {
    if (prior !== encoded)
      throw new Error("An occupied continuation receipt belongs to a different installation.");
    return;
  }
  await NodeFSP.mkdir(directory, { recursive: true, mode: 0o700 });
  // A surviving reservation with no finished receipt is an unknown preparation effect.
  // Another process or a later retry cannot blindly mark or dispatch continuations again.
  const reservation = await NodeFSP.open(
    NodePath.join(directory, "prepare-intent.json"),
    "wx",
    0o600,
  );
  try {
    await reservation.writeFile(encoded);
    await reservation.sync();
  } finally {
    await reservation.close();
  }
  const parent = await NodeFSP.open(directory, "r");
  try {
    await parent.sync();
  } finally {
    await parent.close();
  }
  const marked = await input.prepare();
  try {
    const file = await NodeFSP.open(receiptPath, "wx", 0o600);
    try {
      await file.writeFile(encoded);
      await file.sync();
    } finally {
      await file.close();
    }
    const directoryFile = await NodeFSP.open(directory, "r");
    try {
      await directoryFile.sync();
    } finally {
      await directoryFile.close();
    }
  } catch (error) {
    await input.clear(marked);
    throw error;
  }
}
