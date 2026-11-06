// Produce the command manifest from a qualified installed layout. Does not run it.
import * as NodeFSP from "node:fs/promises";
import { executableIdentity, stampedIdentity } from "./commandIdentity.mjs";
import * as NodePath from "node:path";
const [runtimeForm, file, entry, output] = process.argv.slice(2);
if (
  !["bundle", "electron", "sea"].includes(runtimeForm) ||
  !file ||
  !entry ||
  !output ||
  !NodePath.isAbsolute(file) ||
  !NodePath.isAbsolute(output) ||
  (runtimeForm !== "sea" && !NodePath.isAbsolute(entry))
)
  throw new Error(
    "Usage: artifact-command.mjs <bundle|electron|sea> <absolute executable> <absolute entry or -> <absolute manifest.json>",
  );
await NodeFSP.access(file);
const input = await NodeFSP.readFile(runtimeForm === "sea" ? file : entry);
const build = runtimeForm === "sea" ? executableIdentity(input) : stampedIdentity(input);
if (!build || build === "f07a6bb84e82d6c130f80e7f7f398ea97a36ca2a6b5209ff71e56f613f0507c1")
  throw new Error("Missing complete-input build stamp");
await NodeFSP.writeFile(
  output,
  JSON.stringify(
    {
      schema: "jones.server-parallelism.command/v1",
      runtimeForm,
      file,
      args:
        runtimeForm === "sea"
          ? ["__jones-server-compute"]
          : ["--max-old-space-size=256", entry, "__jones-server-compute"],
      build,
    },
    null,
    2,
  ) + "\n",
  { flag: "wx", mode: 0o600 },
);
