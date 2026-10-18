import { buildAppIdentityInstructions as formatAppIdentityInstructions } from "@t3tools/provider-core/server/jones/AppIdentity";
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
  return formatAppIdentityInstructions(APP_IDENTITY);
}
