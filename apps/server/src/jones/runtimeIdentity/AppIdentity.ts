import { JonesAppIdentity } from "@t3tools/contracts/jones/organizationMetadata";
import * as Schema from "effect/Schema";
import packageJson from "../../../package.json" with { type: "json" };

const isBuildSource = Schema.is(JonesAppIdentity.fields.source);

export function readAppIdentity(manifest: {
  readonly version: string;
  readonly jonesSource?: unknown;
}): JonesAppIdentity {
  const source = manifest.jonesSource;
  return {
    productId: "jones-code",
    productName: "Jones Code",
    version: manifest.version,
    source:
      source !== null && isBuildSource(source)
        ? { repository: source.repository, sha: source.sha, tree: source.tree }
        : null,
  };
}

export const APP_IDENTITY = readAppIdentity(packageJson);

export function buildAppIdentityInstructions(): string {
  const source = APP_IDENTITY.source;
  const provenance =
    source === null
      ? "Build source commit is unavailable."
      : `Build source: ${source.repository}, commit ${source.sha}, tree ${source.tree}.`;
  return `App identity: Jones Code (product ID: jones-code; server version: ${APP_IDENTITY.version}), a T3-derived app. ${provenance} This identifies the server running this thread; a connected desktop or mobile client may have a different version. The t3 executable, t3-code MCP namespace, and t3_code context keys are retained compatibility names, not the product identity. When available, get_invocation_context returns this server's appIdentity and serverVersion. If that field or tool is absent, identity is unknown; do not infer T3 Code from absence, a version pattern, or the workspace repository.`;
}
