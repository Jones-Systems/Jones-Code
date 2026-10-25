import * as NodeURL from "node:url";
import { finish, runExperimental } from "./runner.mjs";

finish(
  await runExperimental(
    NodeURL.fileURLToPath(new URL("../../../", import.meta.url)),
    process.argv.slice(2),
  ),
);
