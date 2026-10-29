import * as NodeFSP from "node:fs/promises";
import * as NodePath from "node:path";
import { runNativeQualification } from "../../../apps/server/src/jones/serverParallelism/nativeQualification.ts";
import { minimalWorkerEnvironment } from "../../../apps/server/src/jones/serverCompute/workerCommand.ts";
const [manifestPath, scratch, count = "2"] = process.argv.slice(2);
const manifest = JSON.parse(await NodeFSP.readFile(manifestPath, "utf8"));
let announced = false;
await runNativeQualification({
  count: count === "3" ? 3 : 2,
  directory: NodePath.join(scratch, "parent-loss"),
  command: {
    ...manifest,
    env: minimalWorkerEnvironment(
      process.env,
      manifest.runtimeForm === "sea",
      manifest.runtimeForm === "electron",
    ),
  },
  onProductiveOverlap: (pids) => {
    if (announced) return;
    announced = true;
    process.stdout.write(
      JSON.stringify({ type: "productive-overlap", pids, build: manifest.build }) + "\n",
    );
  },
});
// Reaching here means the outer test never killed this captured parent.
await NodeFSP.writeFile(NodePath.join(scratch, "parent-finished"), "finished\n", { flag: "wx" });
