// CI-only entry: no arbitrary PID, signal, eval, native-layout or observer privilege arguments.
import { mkdtemp, realpath } from "node:fs/promises";
import { join, isAbsolute } from "node:path";
import { tmpdir } from "node:os";
import { runNativeBlockCase } from "./native-block-case.mjs";
const [manifestPath, output, profile] = process.argv.slice(2);
if (process.argv.length !== 5 || !isAbsolute(manifestPath ?? "") || !isAbsolute(output ?? "") ||
    !["laptop", "vps"].includes(profile))
  throw new Error("native-block-run.mjs <absolute manifest> <absolute evidence directory> <laptop|vps>");
const scratch = await realpath(await mkdtemp(join(tmpdir(), "jones-native-block-")));
const result = await runNativeBlockCase({ manifestPath, scratch, output, profile });
if (result.predicate !== true) process.exitCode = 1;
