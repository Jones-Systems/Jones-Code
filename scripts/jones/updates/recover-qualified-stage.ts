// @effect-diagnostics nodeBuiltinImport:off - Source-run recovery CLI owns authenticated read-only GitHub API calls.
import * as NodeChildProcess from "node:child_process";
import * as NodeFSP from "node:fs/promises";
import * as NodeUtil from "node:util";
import {
  applyQualifiedStageRecovery,
  inspectQualifiedStageRecovery,
  type RecoveryApi,
  type RecoveryInput,
  type RecoveryPlan,
} from "../../../apps/server/src/jones/updates/recoverQualifiedStage.ts";

const execFile = NodeUtil.promisify(NodeChildProcess.execFile);
const api: RecoveryApi = {
  get: async (endpoint) => {
    let stdout: string;
    try {
      ({ stdout } = await execFile(
        "gh",
        ["api", "--hostname", "github.com", "--method", "GET", "--include", endpoint],
        { maxBuffer: 2 * 1024 * 1024 },
      ));
    } catch (cause) {
      const failed = cause as { stdout?: string };
      if (typeof failed.stdout !== "string")
        throw new Error("GitHub API invocation failed without a response.", { cause });
      stdout = failed.stdout;
    }
    const match = /^HTTP\/\S+ (\d{3})[^\r\n]*\r?\n[\s\S]*?\r?\n\r?\n([\s\S]*)$/.exec(stdout);
    if (match === null) throw new Error("GitHub API response status is unavailable.");
    return { status: Number(match[1]), body: JSON.parse(match[2]!) as unknown };
  },
};

const args = process.argv.slice(2);
if (args.length === 1 && args[0] === "--help") {
  process.stdout.write(
    "Usage: recover-qualified-stage.ts INPUT.json | --apply INSPECTED-PLAN.json\nInspect is read-only. Apply stages the already enrolled exact runtime only. Restart and Install are separate operations.\n",
  );
} else if (args.length === 1 && args[0] !== "--apply") {
  const input = JSON.parse(await NodeFSP.readFile(args[0]!, "utf8")) as RecoveryInput;
  process.stdout.write(
    JSON.stringify(await inspectQualifiedStageRecovery(input, api), null, 2) + "\n",
  );
} else if (args.length === 2 && args[0] === "--apply") {
  const plan = JSON.parse(await NodeFSP.readFile(args[1]!, "utf8")) as RecoveryPlan;
  process.stdout.write(
    JSON.stringify(await applyQualifiedStageRecovery(plan, api), null, 2) + "\n",
  );
} else {
  throw new Error(
    "Usage: recover-qualified-stage.ts INPUT.json | --apply INSPECTED-PLAN.json. This command stages only; restart and Install are separate operations.",
  );
}
