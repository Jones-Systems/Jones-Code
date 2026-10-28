import { writeFile, readFile } from "node:fs/promises";
import { isAbsolute } from "node:path";
import { measureSnapshotPipeline } from "../../../apps/server/src/jones/serverParallelism/snapshotQualification.ts";
const [output, count = "2000"] = process.argv.slice(2);
if (!output || !isAbsolute(output))
  throw new Error("Usage: snapshot.mjs <absolute report.json> [thread-count]");
const required = JSON.parse(
  await readFile(new URL("../../../package.json", import.meta.url), "utf8"),
).engines.node;
if (process.versions.node !== required) throw new Error(`Node ${required} required`);
await writeFile(
  output,
  JSON.stringify(await measureSnapshotPipeline(Number(count)), null, 2) + "\n",
  { flag: "wx", mode: 0o600 },
);
