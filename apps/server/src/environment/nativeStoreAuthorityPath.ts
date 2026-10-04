// @effect-diagnostics nodeBuiltinImport:off
import * as NodeFS from "node:fs";
import * as NodePath from "node:path";

const canonicalize = (input: string): string => {
  let candidate = NodePath.resolve(input);
  const missing: string[] = [];
  while (true) {
    try {
      return NodePath.join(NodeFS.realpathSync(candidate), ...missing);
    } catch (cause) {
      if (!(cause instanceof Error && "code" in cause && cause.code === "ENOENT")) throw cause;
      const parent = NodePath.dirname(candidate);
      if (parent === candidate) throw cause;
      missing.unshift(NodePath.basename(candidate));
      candidate = parent;
    }
  }
};

const contains = (root: string, candidate: string): boolean => {
  const relative = NodePath.relative(root, candidate);
  return (
    relative === "" ||
    (!NodePath.isAbsolute(relative) &&
      relative !== ".." &&
      !relative.startsWith(`..${NodePath.sep}`))
  );
};

export const validateNativeStoreAuthorityPath = (
  baseDir: string,
  authorityStateDir: string,
): void => {
  const authority = canonicalize(authorityStateDir);
  for (const protectedRoot of ["userdata", "dev", NodePath.join("runtime", "db-backup")]) {
    const root = canonicalize(NodePath.join(baseDir, protectedRoot));
    if (contains(root, authority) || contains(authority, root)) {
      throw new Error("Native authority directory overlaps application state or rollback backups.");
    }
  }
};

export const validateNativeStoreAuthorityDatabasePath = (
  baseDir: string,
  databasePath: string,
): void => {
  const legacyPath = NodePath.join(NodePath.resolve(baseDir), "userdata", "state.sqlite");
  if (
    typeof databasePath !== "string" ||
    !NodePath.isAbsolute(databasePath) ||
    NodePath.resolve(databasePath) !== legacyPath ||
    canonicalize(databasePath) !== NodePath.join(canonicalize(baseDir), "userdata", "state.sqlite")
  ) {
    throw new Error("Selected database requires separate native store qualification.");
  }
};
