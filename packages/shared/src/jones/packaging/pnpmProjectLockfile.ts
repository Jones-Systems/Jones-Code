import * as Schema from "effect/Schema";
import { parseAllDocuments } from "yaml";

const EnvironmentLockfile = Schema.Struct({
  lockfileVersion: Schema.Literals(["9.0"]),
  importers: Schema.Struct({
    ".": Schema.Struct({
      configDependencies: Schema.optional(Schema.Record(Schema.String, Schema.Unknown)),
      packageManagerDependencies: Schema.optional(
        Schema.Record(
          Schema.String,
          Schema.Struct({ specifier: Schema.String, version: Schema.String }),
        ),
      ),
    }),
  }),
  packages: Schema.Record(Schema.String, Schema.Unknown),
  snapshots: Schema.Record(Schema.String, Schema.Unknown),
});
const decodeEnvironmentLockfile = Schema.decodeUnknownSync(EnvironmentLockfile, {
  onExcessProperty: "error",
});

export function readPnpmProjectLockfile(source: string): unknown {
  const documents = parseAllDocuments(source);
  if (documents.some((document) => document.errors.length > 0)) {
    throw new Error("Invalid CLI archive lockfile YAML.");
  }
  if (documents.length === 1) return documents[0]!.toJS();
  if (documents.length !== 2) throw new Error("Unsupported CLI archive lockfile documents.");

  // pnpm's optional environment document precedes the workspace lock. Never
  // search arbitrary documents for a usable importer or combine their bindings.
  const environment = decodeEnvironmentLockfile(documents[0]!.toJS());
  const root = environment.importers["."];
  if (
    Object.keys(root.configDependencies ?? {}).length === 0 &&
    Object.keys(root.packageManagerDependencies ?? {}).length === 0
  ) {
    throw new Error("Missing CLI archive lockfile environment dependencies.");
  }
  return documents[1]!.toJS();
}
