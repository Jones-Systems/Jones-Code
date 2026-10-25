import { findEnvironmentUpdate as findUpstreamEnvironmentUpdate } from "../../features/settings/environment-maintenance";
import {
  cliReleaseChannelOf,
  cliReleaseIndexPageUrl,
  newestCliReleaseVersion,
} from "@t3tools/shared/cliRelease";
import { compareSemverVersions } from "@t3tools/shared/semver";
import * as Schema from "effect/Schema";

const Release = Schema.Struct({
  tag_name: Schema.String,
  draft: Schema.optionalKey(Schema.Boolean),
});
const decodeReleases = Schema.decodeUnknownSync(Schema.Array(Release));

export async function findEnvironmentUpdate(
  currentVersion: string,
  signal: AbortSignal,
  upstreamEnvironment = false,
) {
  if (upstreamEnvironment) return findUpstreamEnvironmentUpdate(currentVersion, signal);
  const channel = cliReleaseChannelOf(currentVersion);
  if (channel === "preview") {
    throw new Error(
      "Jones previews use qualified main builds. Use the separate Download and Install actions for this environment.",
    );
  }
  for (let page = 1; ; page++) {
    const response = await fetch(cliReleaseIndexPageUrl(page), { signal });
    if (!response.ok) throw new Error(`Could not check releases (${response.status}). Try again.`);
    const releases = decodeReleases(await response.json());
    const version = newestCliReleaseVersion(releases, channel);
    if (version !== undefined) {
      return compareSemverVersions(version, currentVersion) > 0 ? version : null;
    }
    if (releases.length < 100) throw new Error(`No ${channel} release was found.`);
  }
}
