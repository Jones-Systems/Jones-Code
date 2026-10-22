// @effect-diagnostics nodeBuiltinImport:off -- This test-only setup verifies the process before substituting candidate admission.
import * as NodeAssert from "node:assert/strict";
import { vi } from "vite-plus/test";

// Only this explicit qualification config loads the candidate policy. Production keeps
// its source-qualified allowlist, and no environment variable changes that policy.
vi.mock(
  "../../../apps/server/src/jones/workstreams/enrollment/sqliteRuntimePolicy.ts",
  async (importOriginal) => {
    const actual =
      await importOriginal<
        typeof import("../../../apps/server/src/jones/workstreams/enrollment/sqliteRuntimePolicy.ts")
      >();
    NodeAssert.equal(process.versions.node, "26.12.1");
    NodeAssert.equal(process.versions.sqlite, "3.54.0");
    process.stdout.write(
      `[native-enrollment-candidate] ${JSON.stringify({ node: process.versions.node, sqlite: process.versions.sqlite, productionAdmitted: actual.isQualifiedNativeEnrollmentSqliteRuntime(process.versions.node, process.versions.sqlite) })}\n`,
    );
    return {
      isQualifiedNativeEnrollmentSqliteRuntime: (
        node: string | undefined,
        sqlite: string | undefined,
      ) =>
        actual.isQualifiedNativeEnrollmentSqliteRuntime(node, sqlite) ||
        (node === "26.12.1" && sqlite === "3.54.0"),
    };
  },
);
