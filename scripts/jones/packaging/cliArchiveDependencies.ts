import * as Schema from "effect/Schema";
import { satisfiesSemverRange } from "@t3tools/shared/semver";
import { readPnpmProjectLockfile } from "@t3tools/shared/jones/packaging/pnpmProjectLockfile";

const Lockfile = Schema.Struct({
  lockfileVersion: Schema.Literals(["9.0"]),
  importers: Schema.Record(
    Schema.String,
    Schema.Struct({
      dependencies: Schema.optional(
        Schema.Record(
          Schema.String,
          Schema.Struct({ specifier: Schema.String, version: Schema.String }),
        ),
      ),
    }),
  ),
  patchedDependencies: Schema.Record(Schema.String, Schema.String),
  overrides: Schema.optional(Schema.Record(Schema.String, Schema.String)),
  snapshots: Schema.Record(Schema.String, Schema.Unknown),
});
const decodeLockfile = Schema.decodeUnknownSync(Lockfile);
const hasZodDependency = Schema.is(
  Schema.Struct({ dependencies: Schema.Struct({ zod: Schema.String }) }),
);

export function planCliArchiveDependencies(input: {
  readonly dependencies: Readonly<Record<string, string>>;
  readonly patchedDependencies: Readonly<Record<string, string>>;
  readonly lockfile: string;
}) {
  const lockfile = decodeLockfile(readPnpmProjectLockfile(input.lockfile));
  const importer = lockfile.importers["apps/server"]?.dependencies;
  if (!importer) throw new Error("CLI archive requires the committed apps/server lock importer.");

  const dependencies = { ...input.dependencies };
  const patchedDependencies: Record<string, string> = {};
  const patchedRoots = new Set<string>();
  for (const [patchKey, patchPath] of Object.entries(input.patchedDependencies)) {
    const separator = patchKey.lastIndexOf("@");
    const name = separator > 0 ? patchKey.slice(0, separator) : patchKey;
    if (!Object.hasOwn(dependencies, name)) continue;
    if (patchedRoots.has(name)) throw new Error(`Ambiguous CLI archive patches for ${name}.`);
    patchedRoots.add(name);

    const locked = importer[name];
    // pnpm records a direct override as the importer specifier. Keep the
    // manifest's baseline bound to that same exact patched version below.
    const override = lockfile.overrides?.[name];
    if (!locked || locked.specifier !== (override ?? input.dependencies[name])) {
      throw new Error(`Missing or stale CLI archive lock importer entry for ${name}.`);
    }
    // The reduced manifest has no lock seed. Cursor's zod peer can be reproduced
    // from a staged direct root. Other peer contexts need their own staging policy.
    const resolution =
      /^(\d+\.\d+\.\d+(?:-[\dA-Za-z.-]+)?)\(patch_hash=([a-f0-9]{64})\)(?:\(zod@(\d+\.\d+\.\d+)\))?$/.exec(
        locked.version,
      );
    const version = resolution?.[1];
    const patchHash = resolution?.[2];
    if (
      !version ||
      !patchHash ||
      (override !== undefined &&
        (override !== version ||
          ![version, `^${version}`, `~${version}`].includes(input.dependencies[name]!))) ||
      patchKey !== `${name}@${version}` ||
      lockfile.patchedDependencies[patchKey] !== patchHash ||
      !Object.hasOwn(lockfile.snapshots, `${name}@${locked.version}`) ||
      patchPath.length === 0
    ) {
      throw new Error(`Unsupported or mismatched CLI archive patch resolution for ${name}.`);
    }
    const zodPeer = resolution?.[3];
    if (zodPeer !== undefined) {
      const snapshot = lockfile.snapshots[`${name}@${locked.version}`];
      if (
        name !== "@cursor/sdk" ||
        !importer.zod ||
        importer.zod.specifier !== input.dependencies.zod ||
        importer.zod.version !== zodPeer ||
        !satisfiesSemverRange(zodPeer, importer.zod.specifier) ||
        (lockfile.overrides?.zod !== undefined && lockfile.overrides.zod !== zodPeer) ||
        !hasZodDependency(snapshot) ||
        snapshot.dependencies.zod !== zodPeer ||
        !Object.hasOwn(lockfile.snapshots, `zod@${zodPeer}`)
      ) {
        throw new Error(`Unsupported or mismatched CLI archive zod peer for ${name}.`);
      }
      dependencies.zod = zodPeer;
    }
    dependencies[name] = version;
    patchedDependencies[patchKey] = patchPath;
  }
  return { dependencies, patchedDependencies };
}
