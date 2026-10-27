// @effect-diagnostics nodeBuiltinImport:off -- This test-only setup verifies the actual runtime and production admission.
import * as NodeAssert from "node:assert/strict";
import { isQualifiedNativeEnrollmentSqliteRuntime } from "../../../apps/server/src/jones/workstreams/enrollment/sqliteRuntimePolicy.ts";

// Exact-runtime coverage requires the real production policy; no test mock may
// supply this admission.
NodeAssert.equal(process.versions.node, "26.12.1");
NodeAssert.equal(process.versions.sqlite, "3.54.0");
const productionAdmitted = isQualifiedNativeEnrollmentSqliteRuntime(
  process.versions.node,
  process.versions.sqlite,
);
NodeAssert.equal(productionAdmitted, true);
process.stdout.write(
  `[native-enrollment-candidate] ${JSON.stringify({ node: process.versions.node, sqlite: process.versions.sqlite, productionAdmitted })}\n`,
);
