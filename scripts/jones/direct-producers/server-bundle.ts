#!/usr/bin/env node
import { assertManagedServerBundle } from "./server-pack-profile.ts";

// Refuse before loading the CLI, its dependencies or the bundler/configuration.
assertManagedServerBundle(process.argv.slice(2), process.env.T3CODE_PACK_EXE);
process.argv.splice(2, 0, "build-bundle");
await import("../../../apps/server/scripts/cli.ts");
