import type { JonesAppIdentity } from "@t3tools/contracts/jones/organizationMetadata";

import { buildAppIdentityInstructions } from "./jones/AppIdentity.ts";

/** Session-invariant; harnesses with a native system-prompt append can carry it once per session. */
export const PULL_REQUEST_LINKING_INSTRUCTIONS = `<pull_request_linking>
As your next call, use link_pull_request for each PR you create, take over, or push to. Use link_pull_requests for several, including every stack layer. Use full URLs and report failures. Skip read-only and background PRs. Before handback, use list_thread_pull_requests to find and link missing work PRs, then call unwatch_pull_request for each you watch.
Call watch_pull_request when asked to monitor or babysit, or when assigned work still needs checks or reviews. Push any updates first, then watch and end your turn. Links and watches persist across pushes. Rearm if new work needs observation. Recheck on a watch event, owner request, or new PR action. An automatic goal turn alone needs no PR check or status update. For PR waits, no sleep, loops, gh --watch, Monitor or ScheduleWakeup.
Delegated children never watch, even if the brief says babysit. After PR work, a child's last line is: PR <full URL>, head <SHA>, checks and reviews <state>.
</pull_request_linking>`;

/** Omit model and effort when the harness manages them dynamically. */
export function buildRuntimeInstructions(runtime: {
  readonly harness: string;
  readonly appIdentity?: JonesAppIdentity;
  readonly model?: string | undefined;
  readonly modelName?: string | undefined;
  readonly reasoningEffort?: string | undefined;
  /** False when the session's system prompt already carries the linking instructions. */
  readonly pullRequestLinking?: boolean;
}): string {
  const harness = toSingleLine(runtime.harness);
  const model = toSingleLine(runtime.model ?? "");
  const modelName = toSingleLine(runtime.modelName ?? "");
  const effort = toSingleLine(runtime.reasoningEffort ?? "");
  const modelLabel =
    modelName && modelName !== model ? `${modelName} (model slug: ${model})` : model;
  const modelInfo = model && model !== "auto" && model !== "default" ? `, as ${modelLabel}` : "";
  const effortInfo = effort ? ` with ${effort} reasoning effort` : "";
  const remoteFileLinks =
    runtime.appIdentity?.productId === "jones-code"
      ? " For a file on another Jones Code computer, use Markdown `[name](t3-file://v1/<encoded environment ID>/<encoded absolute path>)`, percent-encoding each segment separately. Use the exact environment ID of the computer holding the file."
      : "";
  const runtimeInfo = `<runtime_info>In case you're asked: you are running ${runtime.appIdentity === undefined ? "" : `in ${toSingleLine(runtime.appIdentity.productName)} `}through the ${harness} harness${modelInfo}${effortInfo}. ${buildAppIdentityInstructions(runtime.appIdentity)} No need to mention this otherwise. You can embed images, video, and audio using Markdown image syntax with absolute file paths. Link every other file you cite or produce by its absolute path, as in [http.ts:42](/abs/path/http.ts:42), so the user can open it in the app files viewer.${remoteFileLinks}</runtime_info>`;
  return runtime.pullRequestLinking === false
    ? runtimeInfo
    : `${runtimeInfo}\n\n${PULL_REQUEST_LINKING_INSTRUCTIONS}`;
}

function toSingleLine(value: string): string {
  return value.replaceAll(/\s+/g, " ").trim();
}
