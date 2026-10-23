import * as NodeURL from "node:url";

import { runCli } from "./incrementalTypecheck.ts";

process.exitCode = await runCli(
  NodeURL.fileURLToPath(new URL("../../../", import.meta.url)),
  process.argv.slice(2),
);
