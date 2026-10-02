// @effect-diagnostics nodeBuiltinImport:off -- This enrollment-local adapter constructs fixed file URIs and never opens SQLite.
import * as NodePath from "node:path";
import * as NodeURL from "node:url";

export type NativeEnrollmentSqliteAccess = "strict_inspection" | "operational_inspection" | "apply";
export interface NativeEnrollmentSqliteConfig {
  readonly filename: string;
  readonly readonly: boolean;
  readonly allowExtension: false;
}
export class NativeEnrollmentSqliteQualificationError extends Error {
  readonly code: "runtime_unqualified" | "database_unavailable";
  constructor(code: "runtime_unqualified" | "database_unavailable") {
    super(`Native enrollment SQLite qualification failed (${code}).`);
    this.name = "NativeEnrollmentSqliteQualificationError";
    this.code = code;
  }
}
export const isQualifiedNativeEnrollmentSqliteRuntime = (
  node: string | undefined,
  sqlite: string | undefined,
): boolean =>
  (node === "24.13.1" && sqlite === "3.51.2") ||
  (node === "24.19.0" && sqlite === "3.53.3") ||
  (node === "24.21.0" && sqlite === "3.53.4");

export const makeNativeEnrollmentSqliteConfig = (
  dbPath: string,
  access: NativeEnrollmentSqliteAccess,
): NativeEnrollmentSqliteConfig => {
  if (access !== "strict_inspection" && access !== "operational_inspection" && access !== "apply")
    throw new NativeEnrollmentSqliteQualificationError("runtime_unqualified");
  if (
    !NodePath.isAbsolute(dbPath) ||
    NodePath.resolve(dbPath) !== dbPath ||
    /[\r\n\0]/.test(dbPath)
  )
    throw new NativeEnrollmentSqliteQualificationError("database_unavailable");
  const versions: Readonly<Record<string, string | undefined>> = process.versions;
  if (!isQualifiedNativeEnrollmentSqliteRuntime(versions.node, versions.sqlite))
    throw new NativeEnrollmentSqliteQualificationError("runtime_unqualified");
  const readonly = access !== "apply";
  const uri = NodeURL.pathToFileURL(dbPath);
  // mode=rw cannot create a missing main file. mode=ro still permits SQLite WAL/SHM effects;
  // operational inspection and apply need a separate operation envelope covering those effects.
  uri.search = `mode=${readonly ? "ro" : "rw"}&cache=private&vfs=unix`;
  return { filename: uri.href, readonly, allowExtension: false };
};
