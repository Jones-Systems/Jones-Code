// Run only on an admitted host. This isolated helper changes only its own argv
// to use the production source identity resolver; no finder or server is started.
import * as NodePath from "node:path";
import * as NodeFSP from "node:fs/promises";
import * as NodeURL from "node:url";
import { currentBuildIdentity } from "../../../apps/server/src/jones/serverCompute/workerCommand.ts";
const [entryArg, output] = process.argv.slice(2);
if (!entryArg || !output || !NodePath.isAbsolute(output))
  throw new Error("Usage: source-command.mjs <absolute bin.ts> <absolute manifest.json>");
const entry = NodePath.resolve(entryArg);
if (!entry.endsWith("/apps/server/src/bin.ts")) throw new Error("Expected source bin entry");
const version = JSON.parse(
  await (
    await import("node:fs/promises")
  ).readFile(new URL("../../../package.json", import.meta.url), "utf8"),
).engines.node;
if (process.versions.node !== version) throw new Error(`Node ${version} required`);
process.argv[1] = entry;
const build = await currentBuildIdentity();
await NodeFSP.writeFile(
  output,
  JSON.stringify(
    {
      schema: "jones.server-parallelism.command/v1",
      runtimeForm: "source",
      file: process.execPath,
      args: ["--max-old-space-size=256", entry, "__jones-server-compute"],
      build,
      identityResolver: NodeURL.fileURLToPath(
        new URL("../../../apps/server/src/jones/serverCompute/workerCommand.ts", import.meta.url),
      ),
    },
    null,
    2,
  ) + "\n",
  { flag: "wx", mode: 0o600 },
);
