// CI-only entry: no arbitrary PID, signal, eval, native-layout or observer privilege arguments.
import * as NodeFSP from "node:fs/promises";
import * as NodePath from "node:path";
import * as NodeOS from "node:os";
import { runNativeBlockCase } from "./native-block-case.mjs";
const [manifestPath, output, profile] = process.argv.slice(2);
if (
  process.argv.length !== 5 ||
  !NodePath.isAbsolute(manifestPath ?? "") ||
  !NodePath.isAbsolute(output ?? "") ||
  !["laptop", "vps"].includes(profile)
)
  throw new Error(
    "native-block-run.mjs <absolute manifest> <absolute evidence directory> <laptop|vps>",
  );
const scratch = await NodeFSP.realpath(
  await NodeFSP.mkdtemp(NodePath.join(NodeOS.tmpdir(), "jones-native-block-")),
);
const result = await runNativeBlockCase({ manifestPath, scratch, output, profile });
if (result.predicate !== true) process.exitCode = 1;
