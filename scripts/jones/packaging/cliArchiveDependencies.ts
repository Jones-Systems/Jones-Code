import * as Schema from "effect/Schema";
import { fromYaml } from "@t3tools/shared/schemaYaml";

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
  snapshots: Schema.Record(Schema.String, Schema.Unknown),
});
const decodeLockfile = Schema.decodeUnknownSync(fromYaml(Lockfile));

export function planCliArchiveDependencies(input: {
  readonly dependencies: Readonly<Record<string, string>>;
  readonly patchedDependencies: Readonly<Record<string, string>>;
  readonly lockfile: string;
}) {
  const lockfile = decodeLockfile(input.lockfile);
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
    if (!locked || locked.specifier !== input.dependencies[name]) {
      throw new Error(`Missing or stale CLI archive lock importer entry for ${name}.`);
    }
    // This reduced manifest has no lock seed: an exact patch must have an exact
    // root. Peer-qualified, aliased and non-registry resolutions need a separate
    // staging policy instead of silently discarding their resolution context.
    const resolution = /^(\d+\.\d+\.\d+(?:-[\dA-Za-z.-]+)?)\(patch_hash=([a-f0-9]{64})\)$/.exec(
      locked.version,
    );
    const version = resolution?.[1];
    const patchHash = resolution?.[2];
    if (
      !version ||
      !patchHash ||
      patchKey !== `${name}@${version}` ||
      lockfile.patchedDependencies[patchKey] !== patchHash ||
      !Object.hasOwn(lockfile.snapshots, `${name}@${locked.version}`) ||
      patchPath.length === 0
    ) {
      throw new Error(`Unsupported or mismatched CLI archive patch resolution for ${name}.`);
    }
    dependencies[name] = version;
    patchedDependencies[patchKey] = patchPath;
  }
  return { dependencies, patchedDependencies };
}
