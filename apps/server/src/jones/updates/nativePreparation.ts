// @effect-diagnostics nodeBuiltinImport:off
// Native helper protocol owns durable files shared with the detached Python process.
import * as NodeFSP from "node:fs/promises";
import * as NodePath from "node:path";
import * as NodeCrypto from "node:crypto";
import { isDeepStrictEqual } from "node:util";
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

async function boundedJson(path: string): Promise<{ raw: string; value: Record<string, unknown> }> {
  const info = await NodeFSP.lstat(path);
  if (!info.isFile() || info.isSymbolicLink() || info.size > 1024 * 1024)
    throw new Error("Native preparation metadata is not a bounded regular file.");
  const raw = await NodeFSP.readFile(path, "utf8");
  const value: unknown = JSON.parse(raw);
  if (value === null || typeof value !== "object" || Array.isArray(value))
    throw new Error("Native preparation metadata is not an object.");
  return { raw, value: value as Record<string, unknown> };
}

async function publish(path: string, value: Record<string, unknown>): Promise<void> {
  const file = await NodeFSP.open(path, "wx", 0o600);
  try {
    await file.writeFile(JSON.stringify(value) + "\n");
    await file.sync();
  } finally {
    await file.close();
  }
  const directory = await NodeFSP.open(NodePath.dirname(path), "r");
  try {
    await directory.sync();
  } finally {
    await directory.close();
  }
}

export async function prepareNativeContinuationReceipt(input: {
  home: string;
  databasePath: string;
  environmentId: string;
  version: string;
  handle: string;
  transactionId: string;
  prepare: () => Promise<ReadonlyArray<string>>;
  clear: (threadIds: ReadonlyArray<string>) => Promise<void>;
}): Promise<void> {
  if (!/^[a-f0-9]{64}$/.test(input.handle)) throw new Error("Invalid staged handle.");
  if (!/^[a-f0-9]{64}$/.test(input.transactionId))
    throw new Error("Invalid native transaction ID.");
  const manifestPath = NodePath.join(input.home, "runtime", "jones-active-install.json");
  const active = await boundedJson(manifestPath);
  const manifest = decodeManifest(active.raw);
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
  const directory = NodePath.join(
    home,
    "runtime",
    "jones-updates",
    "transactions",
    input.transactionId,
  );
  if ((await NodeFSP.realpath(directory)) !== directory)
    throw new Error("Native preparation transaction path is not canonical.");
  const claimPath = NodePath.join(directory, "prepare-intent.json");
  const claim = (await boundedJson(claimPath)).value;
  const selectionPath = claim.selectionPath;
  const selectionSha256 = claim.selectionSha256;
  if (
    typeof selectionPath !== "string" ||
    NodePath.dirname(selectionPath) !==
      NodePath.join(home, "runtime", "jones-updates", "staging") ||
    (await NodeFSP.realpath(selectionPath)) !== selectionPath ||
    typeof selectionSha256 !== "string" ||
    !/^[a-f0-9]{64}$/.test(selectionSha256)
  )
    throw new Error("Native preparation requires an exact selection claim.");
  const expectedClaim = {
    protocol: 1,
    preparationClaimProtocol: 2,
    stagedHandle: input.handle,
    transactionId: input.transactionId,
    selectionPath,
    selectionSha256,
    home,
    databasePath,
    profile: manifest.profile,
    environmentId: input.environmentId,
  };
  if (!isDeepStrictEqual(claim, expectedClaim))
    throw new Error("Native preparation requires the current helper claim protocol.");
  const selection = await boundedJson(selectionPath);
  const selectedApp = selection.value.app;
  if (
    NodeCrypto.createHash("sha256").update(selection.raw).digest("hex") !== selectionSha256 ||
    selection.value.schema !== 1 ||
    selection.value.source !== "jones-actions" ||
    selection.value.home !== home ||
    selection.value.profile !== manifest.profile ||
    selection.value.currentVersion !== input.version ||
    !isDeepStrictEqual(selection.value.active, active.value) ||
    selectedApp === null ||
    typeof selectedApp !== "object" ||
    Array.isArray(selectedApp) ||
    (selectedApp as Record<string, unknown>).handle !== input.handle
  )
    throw new Error("The claimed native selection changed before preparation.");
  const receiptPath = NodePath.join(directory, "continuation.json");
  const receipt = {
    ...expectedClaim,
    prepared: true,
  };
  const prior = await boundedJson(receiptPath).catch((error: NodeJS.ErrnoException) => {
    if (error.code === "ENOENT") return undefined;
    throw error;
  });
  if (prior !== undefined) {
    if (
      !isDeepStrictEqual(prior.value, receipt) ||
      !isDeepStrictEqual(
        (await boundedJson(NodePath.join(directory, "prepare-dispatched.json"))).value,
        expectedClaim,
      )
    )
      throw new Error("An occupied continuation receipt belongs to a different installation.");
    return;
  }
  if (
    (await boundedJson(manifestPath)).raw !== active.raw ||
    !isDeepStrictEqual((await boundedJson(claimPath)).value, expectedClaim)
  )
    throw new Error("The native preparation claim changed before dispatch.");
  // The helper's claim prevents discard. A separate durable dispatch record
  // makes a lost backend response an unknown effect, never a repeatable call.
  await publish(NodePath.join(directory, "prepare-dispatched.json"), expectedClaim);
  const marked = await input.prepare();
  try {
    await publish(receiptPath, receipt);
  } catch (error) {
    await input.clear(marked);
    throw error;
  }
}
