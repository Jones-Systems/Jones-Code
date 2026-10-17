import { compareSemverVersions, parseSemver } from "@t3tools/shared/semver";

const previewBuild = /-preview\.\d{8}\.\d+(?:\.\d+)?$/;

export function isJonesPreviewBuildPair(clientVersion: string, serverVersion: string): boolean {
  return previewBuild.test(clientVersion) && previewBuild.test(serverVersion);
}

export const JONES_CLIENT_UPDATE_HINT =
  "This server is newer than this client. Update this app or reload an updated web client.";

export interface JonesClientUpdateNotice {
  readonly kind: "server-newer" | "preview-source-difference";
  readonly clientVersion: string;
  readonly serverVersion: string;
  readonly clientSourceSha?: string;
  readonly serverSourceSha?: string;
  readonly hint: string;
}

function validSourceSha(value: string | undefined): string | undefined {
  return value && /^[a-f0-9]{40}$/i.test(value) ? value.toLowerCase() : undefined;
}

export function resolveJonesClientUpdateNotice(
  serverVersion: string | null | undefined,
  clientVersion: string,
  sources: {
    readonly clientSourceSha?: string | undefined;
    readonly serverSourceSha?: string | undefined;
  } = {},
): JonesClientUpdateNotice | null {
  const client = clientVersion.trim();
  const server = serverVersion?.trim();
  if (!client || !server) return null;
  const clientComparable = client.split("+")[0]!;
  const serverComparable = server.split("+")[0]!;
  const clientSemver = parseSemver(clientComparable);
  const serverSemver = parseSemver(serverComparable);
  if (!clientSemver || !serverSemver) return null;
  const clientSourceSha = validSourceSha(sources.clientSourceSha);
  const serverSourceSha = validSourceSha(sources.serverSourceSha);
  const identity = {
    clientVersion: client,
    serverVersion: server,
    ...(clientSourceSha ? { clientSourceSha } : {}),
    ...(serverSourceSha ? { serverSourceSha } : {}),
  };
  if (isJonesPreviewBuildPair(client, server)) {
    return clientSourceSha && serverSourceSha && clientSourceSha !== serverSourceSha
      ? {
          ...identity,
          kind: "preview-source-difference",
          hint: "Client and server use different Jones preview sources. Which source is newer is unknown.",
        }
      : null;
  }
  const bothNightly =
    clientSemver.prerelease[0] === "nightly" && serverSemver.prerelease[0] === "nightly";
  const core = (version: string) => version.replace(/[-+].*$/, "");
  if (
    compareSemverVersions(
      bothNightly ? serverComparable : core(server),
      bothNightly ? clientComparable : core(client),
    ) <= 0
  )
    return null;
  return { ...identity, kind: "server-newer", hint: JONES_CLIENT_UPDATE_HINT };
}
