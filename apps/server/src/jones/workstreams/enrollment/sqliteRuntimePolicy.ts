export const isQualifiedNativeEnrollmentSqliteRuntime = (
  node: string | undefined,
  sqlite: string | undefined,
): boolean =>
  (node === "24.13.1" && sqlite === "3.51.2") ||
  (node === "24.19.0" && sqlite === "3.53.3") ||
  (node === "24.21.0" && sqlite === "3.53.4") ||
  (node === "24.22.0" && sqlite === "3.53.4") ||
  (node === "24.22.1" && sqlite === "3.53.4") ||
  (node === "26.8.2" && sqlite === "3.53.4") ||
  (node === "26.12.1" && sqlite === "3.54.0");
