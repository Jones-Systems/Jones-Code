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
    const args = ["api", "--hostname", "github.com", "--method", "GET", "--include"];
    if (
      /^repos\/Jones-Systems\/Jones-Code\/compare\/[a-f0-9]{40}\.\.\.[a-f0-9]{40}$/.test(endpoint)
    ) {
      // Project before buffering: ancestry qualification needs no commit list or file diffs.
      args.push("--jq", "{status, merge_base_commit: {sha: .merge_base_commit.sha}}");
    }
    args.push(endpoint);
    const maxBuffer = 2 * 1024 * 1024;
    let stdout: string;
    let failedInvocation = false;
    try {
      ({ stdout } = await execFile("gh", args, { maxBuffer }));
    } catch (cause) {
      const failed = cause as {
        code?: unknown;
        stdout?: unknown;
        killed?: boolean;
        signal?: unknown;
      };
      if (
        failed.code !== 1 ||
        failed.killed === true ||
        failed.signal != null ||
        typeof failed.stdout !== "string"
      )
        // oxlint-disable-next-line preserve-caught-error -- Subprocess errors can retain unbounded stdout/stderr; CLI diagnostics must not disclose those bodies.
        throw new Error("GitHub API invocation failed or exceeded its response limit.");
      stdout = failed.stdout;
      failedInvocation = true;
    }
    if (Buffer.byteLength(stdout, "utf8") > maxBuffer)
      throw new Error("GitHub API response exceeded its response limit.");
    const match = /^HTTP\/\S+ (\d{3})[^\r\n]*\r?\n[\s\S]*?\r?\n\r?\n([\s\S]*)$/.exec(stdout);
    if (match === null) throw new Error("GitHub API response status is unavailable.");
    const status = Number(match[1]);
    if (failedInvocation && status !== 404)
      throw new Error("GitHub API invocation failed; its response cannot qualify recovery.");
    try {
      return { status, body: JSON.parse(match[2]!) as unknown };
    } catch {
      // JSON errors may include response text. Preserve only a bounded diagnostic at this CLI boundary.
      throw new Error("GitHub API response body is incomplete or invalid JSON.");
    }
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
