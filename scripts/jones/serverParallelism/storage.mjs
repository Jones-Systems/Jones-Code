import * as NodeFSP from "node:fs/promises";
import * as NodePath from "node:path";
const directory = NodePath.resolve(process.argv[2] ?? ".");
const value = await NodeFSP.statfs(directory, { bigint: true }),
  available = value.bavail * value.bsize;
const required = 40n * 1024n ** 3n;
process.stdout.write(
  JSON.stringify({
    directory,
    availableBytes: String(available),
    requiredBytes: String(required),
    admitted: available >= required,
    meaning: "30 GiB reserve + 10 GiB estimated peak; fresh observation, not a reservation",
  }) + "\n",
);
if (available < required) process.exitCode = 1;
