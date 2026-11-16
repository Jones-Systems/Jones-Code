import { parseJonesBuildStartedAt } from "@t3tools/shared/jones/buildTime";
import { compareSemverVersions, parseSemver } from "@t3tools/shared/semver";

const previewBuild = /^(\d+\.\d+\.\d+)-preview\.(\d{8})\.(\d+)(?:\.(\d+))?(?:\+[\w.-]+)?$/;

/** Compare release, build date, workflow run and attempt; legacy builds imply attempt one. */
export function compareJonesPreviewVersions(
  clientVersion: string,
  serverVersion: string,
): number | null {
  const client = previewBuild.exec(clientVersion.trim());
  const server = previewBuild.exec(serverVersion.trim());
  if (!client || !server) return null;
  const comparable = (match: RegExpExecArray) =>
    `${match[1]}-preview.${match[2]}.${match[3]}.${match[4] ?? "1"}`;
  return compareSemverVersions(comparable(client), comparable(server));
}

export function isJonesPreviewBuildPair(clientVersion: string, serverVersion: string): boolean {
  return compareJonesPreviewVersions(clientVersion, serverVersion) !== null;
}

export const JONES_CLIENT_UPDATE_HINT =
  "This server is newer than this client. Update this app or reload an updated web client.";

export interface JonesClientUpdateNotice {
  readonly kind: "server-newer" | "client-newer" | "preview-identity-mismatch";
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
    readonly clientBuildStartedAt?: string | undefined;
    readonly serverBuildStartedAt?: string | undefined;
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
  const versionOrder = compareJonesPreviewVersions(client, server);
  const clientStartedAt = parseJonesBuildStartedAt(sources.clientBuildStartedAt);
  const serverStartedAt = parseJonesBuildStartedAt(sources.serverBuildStartedAt);
  const timeOrder =
    clientStartedAt && serverStartedAt
      ? Math.sign(Date.parse(clientStartedAt) - Date.parse(serverStartedAt))
      : 0;
  const previewOrder = versionOrder === null ? null : timeOrder || versionOrder;
  if (previewOrder !== null) {
    if (clientSourceSha && clientSourceSha === serverSourceSha) return null;
    if (previewOrder < 0)
      return { ...identity, kind: "server-newer", hint: JONES_CLIENT_UPDATE_HINT };
    if (previewOrder > 0)
      return {
        ...identity,
        kind: "client-newer",
        hint: "This app has a newer Jones preview build than this server. Update this server to catch up.",
      };
    return clientSourceSha && serverSourceSha && clientSourceSha !== serverSourceSha
      ? {
          ...identity,
          kind: "preview-identity-mismatch",
          hint: "Client and server report the same Jones preview version but different sources. Check the build identities before updating.",
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
