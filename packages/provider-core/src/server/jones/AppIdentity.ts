import type { JonesAppIdentity } from "@t3tools/contracts/jones/organizationMetadata";

export function buildAppIdentityInstructions(identity?: JonesAppIdentity): string {
  if (identity === undefined)
    return "App identity and build source are unavailable. Do not infer product identity from the harness, workspace repository, or compatibility tool names.";
  const source = identity.source;
  const provenance =
    source === null
      ? "Build source commit is unavailable."
      : `Build source: ${source.repository}, commit ${source.sha}, tree ${source.tree}.`;
  return `App identity: Jones Code (product ID: jones-code; server version: ${identity.version}), a T3-derived app. ${provenance} This identifies the server running this thread; a connected desktop or mobile client may have a different version. The t3 executable, t3-code MCP namespace, and t3_code context keys are retained compatibility names, not the product identity. When available, get_invocation_context returns this server's appIdentity and serverVersion. If that field or tool is absent, identity is unknown; do not infer T3 Code from absence, a version pattern, or the workspace repository.`;
}
