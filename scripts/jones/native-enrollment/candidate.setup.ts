// @effect-diagnostics nodeBuiltinImport:off -- This test-only setup verifies the actual runtime for retained SQLite and updater authority coverage.
import * as NodeAssert from "node:assert/strict";

// The retained updater authority and SQLite adapter are qualified on this exact runtime.
NodeAssert.equal(process.versions.node, "26.12.1");
NodeAssert.equal(process.versions.sqlite, "3.54.0");
process.stdout.write(
  `[sqlite-candidate] ${JSON.stringify({ node: process.versions.node, sqlite: process.versions.sqlite })}\n`,
);
