// @effect-diagnostics nodeBuiltinImport:off - Pure packaging of the standalone native updater tools.
import * as NodeCrypto from "node:crypto";
import { jonesLegacyBootstrapSource } from "../../../apps/desktop/src/jones/updates/jonesLegacyBootstrapSource.ts";
import { jonesNativeHelperSource } from "../../../apps/desktop/src/jones/updates/jonesNativeHelperSource.ts";

const isHash = (value: unknown, length: number): value is string =>
  typeof value === "string" && value.length === length && !/[^a-f0-9]/.test(value);

export interface JonesLegacyBootstrapBundleInput {
  readonly repository: "Jones-Systems/Jones-Code";
  readonly sourceSha: string;
  readonly sourceTree: string;
  readonly version: string;
}

export interface JonesLegacyBootstrapBundleFile<Content extends string | Uint8Array = string> {
  readonly relativePath: string;
  readonly content: Content;
  readonly mode: number;
}

export interface JonesLegacyBootstrapNativeInput {
  readonly identityNative: Uint8Array;
  readonly identityNativeSourceSha256: string;
  readonly maintenanceSource: string;
}

type TextBundleFile = Omit<JonesLegacyBootstrapBundleFile, "content"> & {
  readonly content: string;
};

/** Accept only a bounded, thin ARM64 executable with a complete load-command table. */
export function verifyJonesLegacyIdentityMachO(bytes: Uint8Array): void {
  const data = Buffer.from(bytes);
  const invalid = () => {
    throw new Error("Legacy identity helper must be a thin arm64 Mach-O executable.");
  };
  if (data.length < 32 || data.length > 4 * 1024 * 1024) invalid();
  if (
    data.readUInt32LE(0) !== 0xfeedfacf ||
    data.readUInt32LE(4) !== 0x0100000c ||
    data.readUInt32LE(8) !== 0 ||
    data.readUInt32LE(12) !== 2 ||
    data.readUInt32LE(28) !== 0
  )
    invalid();
  const count = data.readUInt32LE(16);
  const end = 32 + data.readUInt32LE(20);
  if (count < 1 || count > 512 || end > data.length) invalid();
  let offset = 32;
  let entry = false;
  let text = false;
  for (let index = 0; index < count; index++) {
    if (offset + 8 > end) invalid();
    const command = data.readUInt32LE(offset);
    const size = data.readUInt32LE(offset + 4);
    if (size < 8 || size % 8 !== 0 || offset + size > end) invalid();
    if (command === 0x80000028) {
      if (size !== 24 || data.readBigUInt64LE(offset + 8) >= BigInt(data.length)) invalid();
      entry = true;
    }
    if (command === 0x19) {
      if (size < 72 || size !== 72 + data.readUInt32LE(offset + 64) * 80) invalid();
      const fileOffset = data.readBigUInt64LE(offset + 40);
      const fileSize = data.readBigUInt64LE(offset + 48);
      if (fileOffset + fileSize > BigInt(data.length)) invalid();
      if (
        data
          .subarray(offset + 8, offset + 24)
          .toString("ascii")
          .replace(/\0+$/, "") === "__TEXT"
      )
        text = fileOffset === 0n && fileSize > 0n;
    }
    offset += size;
  }
  if (offset !== end || !entry || !text) invalid();
}

export function createJonesLegacyBootstrapBundle(
  input: JonesLegacyBootstrapBundleInput,
): ReadonlyArray<TextBundleFile>;
export function createJonesLegacyBootstrapBundle(
  input: JonesLegacyBootstrapBundleInput,
  native: JonesLegacyBootstrapNativeInput,
): ReadonlyArray<JonesLegacyBootstrapBundleFile<string | Uint8Array>>;
export function createJonesLegacyBootstrapBundle(
  input: JonesLegacyBootstrapBundleInput,
  native?: JonesLegacyBootstrapNativeInput,
): ReadonlyArray<JonesLegacyBootstrapBundleFile<string | Uint8Array>> {
  if (
    input.repository !== "Jones-Systems/Jones-Code" ||
    !isHash(input.sourceSha, 40) ||
    !isHash(input.sourceTree, 40) ||
    !/^\d+\.\d+\.\d+-preview\.\d{8}\.\d+(?:\.\d+)?$/.test(input.version) ||
    input.version.length > 128 ||
    /\s/.test(input.version)
  )
    throw new Error("Legacy bootstrap packaging requires exact Jones preview provenance.");
  const sha256 = (value: string | Uint8Array) =>
    NodeCrypto.createHash("sha256").update(value).digest("hex");
  if (native !== undefined) {
    verifyJonesLegacyIdentityMachO(native.identityNative);
    if (
      !isHash(native.identityNativeSourceSha256, 64) ||
      native.maintenanceSource.length === 0 ||
      Buffer.byteLength(native.maintenanceSource) > 1048576
    )
      throw new Error("Legacy maintenance packaging requires exact source bytes.");
  }
  return [
    {
      relativePath: "jones-bootstrap/bootstrap.py",
      content: jonesLegacyBootstrapSource,
      mode: 0o500,
    },
    { relativePath: "jones-bootstrap/executor.py", content: jonesNativeHelperSource, mode: 0o500 },
    {
      relativePath: "jones-bootstrap/BUNDLE.json",
      content:
        JSON.stringify({
          schema: native === undefined ? 1 : 2,
          repository: input.repository,
          sourceSha: input.sourceSha,
          sourceTree: input.sourceTree,
          version: input.version,
          bootstrapSha256: sha256(jonesLegacyBootstrapSource),
          executorSha256: sha256(jonesNativeHelperSource),
          ...(native === undefined
            ? {}
            : {
                maintenanceSha256: sha256(native.maintenanceSource),
                identityNativeSha256: sha256(native.identityNative),
                identityNativeSourceSha256: native.identityNativeSourceSha256,
                nativeArchitecture: "arm64",
              }),
        }) + "\n",
      mode: 0o400,
    },
    ...(native === undefined
      ? []
      : [
          {
            relativePath: "jones-bootstrap/maintenance.py",
            content: native.maintenanceSource,
            mode: 0o500,
          },
          {
            relativePath: "jones-bootstrap/identity-native",
            content: Uint8Array.from(native.identityNative),
            mode: 0o500,
          },
        ]),
  ];
}

export interface JonesLegacyBootstrapNativeProof extends JonesLegacyBootstrapBundleInput {
  readonly schema: 2;
  readonly bootstrapSha256: string;
  readonly executorSha256: string;
  readonly maintenanceSha256: string;
  readonly identityNativeSha256: string;
  readonly identityNativeSourceSha256: string;
  readonly nativeArchitecture: "arm64";
}

/** This record is supplied independently by the build, never read from the copied bundle. */
export function readJonesLegacyBootstrapNativeProof(
  raw: string,
  identity: JonesLegacyBootstrapBundleInput,
): JonesLegacyBootstrapNativeProof {
  if (Buffer.byteLength(raw) > 8192) throw new Error("Native build proof exceeds its bound.");
  const value: unknown = JSON.parse(raw);
  if (value === null || typeof value !== "object" || Array.isArray(value))
    throw new Error("Native build proof is not an object.");
  const proof = value as Record<string, unknown>;
  const hashes = [
    "bootstrapSha256",
    "executorSha256",
    "maintenanceSha256",
    "identityNativeSha256",
    "identityNativeSourceSha256",
  ];
  const keys = [
    "schema",
    "repository",
    "sourceSha",
    "sourceTree",
    "version",
    "nativeArchitecture",
    ...hashes,
  ];
  if (
    !isHash(identity.sourceSha, 40) ||
    !isHash(identity.sourceTree, 40) ||
    Object.keys(proof).sort().join(",") !== keys.sort().join(",") ||
    proof.schema !== 2 ||
    proof.nativeArchitecture !== "arm64" ||
    Object.entries(identity).some(([key, expected]) => proof[key] !== expected) ||
    hashes.some((key) => !isHash(proof[key], 64))
  )
    throw new Error("Native build proof does not match the exact source and architecture.");
  return proof as unknown as JonesLegacyBootstrapNativeProof;
}

export const JONES_PREPARATION_SOURCE_INPUTS = Object.freeze([
  "pnpm-lock.yaml",
  "pnpm-workspace.yaml",
  "package.json",
  "apps/server/package.json",
  "apps/desktop/package.json",
  "apps/web/package.json",
  "packages/contracts/package.json",
  "apps/server/vite.config.ts",
  "vite.config.ts",
  "scripts/jones/updates/offline-preparation-build.ts",
  "apps/server/src/jones/updates/offlinePreparationEntry.ts",
  "apps/server/src/jones/updates/offlinePreparation.ts",
  "apps/server/src/jones/updates/offlinePreparationCore.ts",
  "apps/server/src/jones/updates/offlinePreparationPlan.ts",
  "apps/server/src/jones/updates/offlinePreparationWorker.ts",
] as const);

export const JONES_PREPARATION_BOUNDS = Object.freeze({
  proofBytes: 64 * 1024,
  entryBytes: 32 * 1024 * 1024,
  seaConfigBytes: 16 * 1024,
  archiveBytes: 128 * 1024 * 1024,
  runtimeBytes: 256 * 1024 * 1024,
});

type PreparationSourceInput = (typeof JONES_PREPARATION_SOURCE_INPUTS)[number];
type PreparationHashRecord = { readonly sha256: string; readonly size: number };

export interface JonesLegacyBootstrapPreparation {
  readonly schema: 1;
  readonly protocol: 1;
  readonly provenanceSha256: string;
  readonly sourceInputHashes: Readonly<Record<PreparationSourceInput, string>>;
  readonly entry: PreparationHashRecord;
  readonly seaConfig: PreparationHashRecord;
  readonly executable: PreparationHashRecord & { readonly name: "jones-offline-preparation" };
  readonly node: {
    readonly version: "26.8.2";
    readonly platform: "darwin";
    readonly architecture: "arm64";
    readonly archiveSha256: string;
    readonly archiveBytes: number;
    readonly checksumsSha256: string;
    readonly runtimeSha256: string;
    readonly runtimeBytes: number;
  };
  readonly sqlite: { readonly version: string; readonly sourceId: string };
}

/** The producer supplies observed data after verifying its files; this shape grants no admission. */
export interface JonesPreparationArtifactEvidence {
  readonly schema: 1;
  readonly observation: "complete";
  readonly classification: "cold-preparation-worker";
  readonly source: JonesLegacyBootstrapBundleInput;
  readonly preparation: JonesLegacyBootstrapPreparation;
}

export interface JonesLegacyBootstrapPreparationProof extends Omit<
  JonesLegacyBootstrapNativeProof,
  "schema"
> {
  readonly schema: 3;
  readonly preparation: JonesLegacyBootstrapPreparation;
}

export interface JonesLegacyBootstrapPreparationBundle {
  readonly files: ReadonlyArray<JonesLegacyBootstrapBundleFile<string | Uint8Array>>;
  readonly runtime: {
    readonly kind: "streamed-file";
    readonly relativePath: "jones-bootstrap/jones-offline-preparation";
    readonly mode: 0o500;
    readonly sha256: string;
    readonly size: number;
    readonly maxBytes: number;
  };
}

function preparationRequire(condition: unknown): asserts condition {
  if (!condition) throw new Error("Preparation proof does not match the closed bundle contract.");
}

function preparationRecord(value: unknown, keys: readonly string[]): Record<string, unknown> {
  preparationRequire(value !== null && typeof value === "object" && !Array.isArray(value));
  const record = value as Record<string, unknown>;
  preparationRequire(
    Object.keys(record).length === keys.length && keys.every((key) => Object.hasOwn(record, key)),
  );
  return record;
}

function preparationHash(value: unknown): string {
  preparationRequire(isHash(value, 64));
  return value;
}

function preparationSize(value: unknown, maximum: number): number {
  preparationRequire(
    typeof value === "number" && Number.isSafeInteger(value) && value > 0 && value <= maximum,
  );
  return value;
}

function preparationHashRecord(value: unknown, maximum: number): PreparationHashRecord {
  const record = preparationRecord(value, ["sha256", "size"]);
  return Object.freeze({
    sha256: preparationHash(record.sha256),
    size: preparationSize(record.size, maximum),
  });
}

function preparationIdentity(value: unknown): JonesLegacyBootstrapBundleInput {
  const record = preparationRecord(value, ["repository", "sourceSha", "sourceTree", "version"]);
  preparationRequire(
    record.repository === "Jones-Systems/Jones-Code" &&
      isHash(record.sourceSha, 40) &&
      isHash(record.sourceTree, 40) &&
      typeof record.version === "string" &&
      record.version.length <= 128 &&
      /^\d+\.\d+\.\d+-preview\.\d{8}\.\d+(?:\.\d+)?$/.test(record.version) &&
      !/\s/.test(record.version),
  );
  return Object.freeze({
    repository: record.repository,
    sourceSha: record.sourceSha,
    sourceTree: record.sourceTree,
    version: record.version,
  });
}

const preparationSeaConfigBytes =
  JSON.stringify({
    main: "offlinePreparationEntry.mjs",
    mainFormat: "module",
    executable: "node",
    output: "jones-offline-preparation",
    disableExperimentalSEAWarning: true,
    useSnapshot: false,
    useCodeCache: false,
    execArgv: [],
    execArgvExtension: "none",
  }) + "\n";

function preparationData(value: unknown): JonesLegacyBootstrapPreparation {
  const record = preparationRecord(value, [
    "schema",
    "protocol",
    "provenanceSha256",
    "sourceInputHashes",
    "entry",
    "seaConfig",
    "executable",
    "node",
    "sqlite",
  ]);
  preparationRequire(record.schema === 1 && record.protocol === 1);
  const inputs = preparationRecord(record.sourceInputHashes, JONES_PREPARATION_SOURCE_INPUTS);
  const sourceInputHashes = Object.freeze(
    Object.fromEntries(
      JONES_PREPARATION_SOURCE_INPUTS.map((name) => [name, preparationHash(inputs[name])]),
    ),
  ) as Readonly<Record<PreparationSourceInput, string>>;
  const seaConfig = preparationHashRecord(
    record.seaConfig,
    JONES_PREPARATION_BOUNDS.seaConfigBytes,
  );
  preparationRequire(
    seaConfig.size === Buffer.byteLength(preparationSeaConfigBytes) &&
      seaConfig.sha256 ===
        NodeCrypto.createHash("sha256").update(preparationSeaConfigBytes).digest("hex"),
  );
  const executable = preparationRecord(record.executable, ["name", "sha256", "size"]);
  preparationRequire(executable.name === "jones-offline-preparation");
  const node = preparationRecord(record.node, [
    "version",
    "platform",
    "architecture",
    "archiveSha256",
    "archiveBytes",
    "checksumsSha256",
    "runtimeSha256",
    "runtimeBytes",
  ]);
  preparationRequire(
    node.version === "26.8.2" && node.platform === "darwin" && node.architecture === "arm64",
  );
  const sqlite = preparationRecord(record.sqlite, ["version", "sourceId"]);
  preparationRequire(
    typeof sqlite.version === "string" &&
      sqlite.version.length <= 11 &&
      /^(?:0|[1-9][0-9]{0,2})\.(?:0|[1-9][0-9]{0,2})\.(?:0|[1-9][0-9]{0,2})$/.test(
        sqlite.version,
      ) &&
      !/\s/.test(sqlite.version) &&
      typeof sqlite.sourceId === "string" &&
      sqlite.sourceId.length === 84 &&
      /^[0-9]{4}-[0-9]{2}-[0-9]{2} [0-9]{2}:[0-9]{2}:[0-9]{2} [a-f0-9]{64}$/.test(sqlite.sourceId),
  );
  const [year, month, day, hour, minute, second] = sqlite.sourceId
    .slice(0, 19)
    .split(/[- :]/)
    .map(Number);
  const leap = year! % 4 === 0 && (year! % 100 !== 0 || year! % 400 === 0);
  const days = [31, leap ? 29 : 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31];
  preparationRequire(
    month! >= 1 &&
      month! <= 12 &&
      day! >= 1 &&
      day! <= days[month! - 1]! &&
      hour! <= 23 &&
      minute! <= 59 &&
      second! <= 59,
  );
  return Object.freeze({
    schema: 1,
    protocol: 1,
    provenanceSha256: preparationHash(record.provenanceSha256),
    sourceInputHashes,
    entry: preparationHashRecord(record.entry, JONES_PREPARATION_BOUNDS.entryBytes),
    seaConfig,
    executable: Object.freeze({
      name: executable.name,
      sha256: preparationHash(executable.sha256),
      size: preparationSize(executable.size, JONES_PREPARATION_BOUNDS.runtimeBytes),
    }),
    node: Object.freeze({
      version: node.version,
      platform: node.platform,
      architecture: node.architecture,
      archiveSha256: preparationHash(node.archiveSha256),
      archiveBytes: preparationSize(node.archiveBytes, JONES_PREPARATION_BOUNDS.archiveBytes),
      checksumsSha256: preparationHash(node.checksumsSha256),
      runtimeSha256: preparationHash(node.runtimeSha256),
      runtimeBytes: preparationSize(node.runtimeBytes, JONES_PREPARATION_BOUNDS.runtimeBytes),
    }),
    sqlite: Object.freeze({ version: sqlite.version, sourceId: sqlite.sourceId }),
  });
}

function preparationJson(raw: string): unknown {
  preparationRequire(Buffer.byteLength(raw) <= JONES_PREPARATION_BOUNDS.proofBytes);
  const value: unknown = JSON.parse(raw);
  // Canonical bytes reject duplicate keys and alternate encodings before another decoder sees them.
  preparationRequire(raw === JSON.stringify(value) + "\n");
  return value;
}

function preparationEvidence(
  value: unknown,
  identity: JonesLegacyBootstrapBundleInput,
): JonesPreparationArtifactEvidence {
  const record = preparationRecord(value, [
    "schema",
    "observation",
    "classification",
    "source",
    "preparation",
  ]);
  preparationRequire(
    record.schema === 1 &&
      record.observation === "complete" &&
      record.classification === "cold-preparation-worker",
  );
  const expected = preparationIdentity(identity);
  const source = preparationIdentity(record.source);
  preparationRequire(
    source.repository === expected.repository &&
      source.sourceSha === expected.sourceSha &&
      source.sourceTree === expected.sourceTree &&
      source.version === expected.version,
  );
  return Object.freeze({
    schema: 1,
    observation: "complete",
    classification: "cold-preparation-worker",
    source,
    preparation: preparationData(record.preparation),
  });
}

/** Decode only data. The producer must separately observe SQLite and verify all artifact bytes. */
export function readJonesPreparationArtifactEvidence(
  raw: string,
  identity: JonesLegacyBootstrapBundleInput,
): JonesPreparationArtifactEvidence {
  return preparationEvidence(preparationJson(raw), identity);
}

/** Only schema 3 is accepted here; existing schema 2 callers retain their original decoder. */
export function readJonesLegacyBootstrapPreparationProof(
  raw: string,
  identity: JonesLegacyBootstrapBundleInput,
): JonesLegacyBootstrapPreparationProof {
  const record = preparationRecord(preparationJson(raw), [
    "schema",
    "repository",
    "sourceSha",
    "sourceTree",
    "version",
    "nativeArchitecture",
    "bootstrapSha256",
    "executorSha256",
    "maintenanceSha256",
    "identityNativeSha256",
    "identityNativeSourceSha256",
    "preparation",
  ]);
  preparationRequire(record.schema === 3);
  const { preparation, ...base } = record;
  const native = readJonesLegacyBootstrapNativeProof(
    JSON.stringify({ ...base, schema: 2 }),
    preparationIdentity(identity),
  );
  return Object.freeze({ ...native, schema: 3, preparation: preparationData(preparation) });
}

/** Describes a fixed streamed resource; it neither reads executable bytes nor admits their execution. */
export function createJonesLegacyBootstrapPreparationBundle(
  input: JonesLegacyBootstrapBundleInput,
  native: JonesLegacyBootstrapNativeInput,
  evidence: JonesPreparationArtifactEvidence,
): JonesLegacyBootstrapPreparationBundle {
  const observed = preparationEvidence(evidence, input);
  const files = createJonesLegacyBootstrapBundle(input, native);
  const base = readJonesLegacyBootstrapNativeProof(String(files[2]!.content), input);
  const manifest = JSON.stringify({ ...base, schema: 3, preparation: observed.preparation }) + "\n";
  readJonesLegacyBootstrapPreparationProof(manifest, input);
  return Object.freeze({
    files: Object.freeze(
      files.map((file) =>
        Object.freeze(
          file.relativePath === "jones-bootstrap/BUNDLE.json"
            ? { ...file, content: manifest }
            : file,
        ),
      ),
    ),
    runtime: Object.freeze({
      kind: "streamed-file",
      relativePath: "jones-bootstrap/jones-offline-preparation",
      mode: 0o500,
      sha256: observed.preparation.executable.sha256,
      size: observed.preparation.executable.size,
      maxBytes: JONES_PREPARATION_BOUNDS.runtimeBytes,
    }),
  });
}
