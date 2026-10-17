import { EnvironmentId } from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";
import * as Option from "effect/Option";

import {
  type ConnectionRegistration,
  type ConnectionRoute,
  ConnectionCredential,
  ConnectionProfile,
} from "../connection/catalog.ts";
import { type ConnectionTarget, PersistedConnectionTarget } from "../connection/model.ts";
import * as TokenStore from "../authorization/tokenStore.ts";
import { StoredGitHubRoutingPermission } from "../connection/githubRoutingPermissions.ts";

export const StoredConnectionCredential = Schema.Struct({
  connectionId: Schema.String,
  credential: ConnectionCredential,
});
export type StoredConnectionCredential = typeof StoredConnectionCredential.Type;

export const MAX_CONNECTION_COMPATIBILITY_HINTS = 128;
const MAX_COMPATIBILITY_ROUTES = 16;
const MAX_ROUTE_BINDING_LENGTH = 16_384;
const MAX_ROUTE_IDENTITY_LENGTH = 256;

/** Historical presentation only; this record supplies no connection or update authority. */
export const StoredConnectionCompatibilityHint = Schema.Struct({
  environmentId: EnvironmentId.check(Schema.isMaxLength(MAX_ROUTE_IDENTITY_LENGTH)),
  routeBinding: Schema.String.check(
    Schema.isMinLength(1),
    Schema.isMaxLength(MAX_ROUTE_BINDING_LENGTH),
  ),
  kind: Schema.Literal("outdated-update"),
});
export type StoredConnectionCompatibilityHint = typeof StoredConnectionCompatibilityHint.Type;

const decodeCompatibilityHint = Schema.decodeUnknownOption(StoredConnectionCompatibilityHint, {
  onExcessProperty: "error",
});

/** Invalid independent records are ignored without involving catalog recovery. */
export function decodeConnectionCompatibilityHints(
  value: unknown,
): ReadonlyArray<StoredConnectionCompatibilityHint> {
  if (!Array.isArray(value) || value.length > MAX_CONNECTION_COMPATIBILITY_HINTS) return [];
  const result: StoredConnectionCompatibilityHint[] = [];
  const seen = new Set<string>();
  for (const candidate of value) {
    const hint = Option.getOrNull(decodeCompatibilityHint(candidate));
    if (hint === null) continue;
    const key = JSON.stringify([hint.environmentId, hint.routeBinding]);
    if (seen.has(key)) continue;
    seen.add(key);
    result.push(hint);
  }
  return result;
}

function boundedRouteIdentity(value: string): boolean {
  return value.length > 0 && value.length <= MAX_ROUTE_IDENTITY_LENGTH;
}

function compatibilityEndpoint(value: string, protocols: ReadonlyArray<string>): string | null {
  if (value.length > 2_048) return null;
  try {
    const url = new URL(value);
    // Credentials and query/fragment payloads never become persisted hint identity.
    if (!protocols.includes(url.protocol) || url.username || url.password || url.search || url.hash)
      return null;
    return url.href;
  } catch {
    return null;
  }
}

/**
 * Binds saved routes in preference order, including their profile configuration.
 * Labels and credential contents are excluded; primary connections are not saved routes.
 */
export function connectionCompatibilityRouteBinding(
  routes: ReadonlyArray<ConnectionRoute>,
): string | null {
  if (routes.length === 0 || routes.length > MAX_COMPATIBILITY_ROUTES) return null;
  const environmentId = routes[0]!.target.environmentId;
  if (!boundedRouteIdentity(environmentId)) return null;
  const identities: unknown[] = [];
  const seen = new Set<string>();
  for (const { target, profile: routeProfile } of routes) {
    if (target.environmentId !== environmentId || target._tag === "PrimaryConnectionTarget")
      return null;
    const key = target._tag === "RelayConnectionTarget" ? target._tag : target.connectionId;
    if (!boundedRouteIdentity(key) || seen.has(key)) return null;
    seen.add(key);
    if (target._tag === "RelayConnectionTarget") {
      identities.push([target._tag]);
      continue;
    }
    const profile = Option.getOrNull(routeProfile);
    if (
      profile === null ||
      profile.connectionId !== target.connectionId ||
      profile.environmentId !== environmentId
    )
      return null;
    if (target._tag === "BearerConnectionTarget") {
      if (profile._tag !== "BearerConnectionProfile") return null;
      const http = compatibilityEndpoint(profile.httpBaseUrl, ["http:", "https:"]);
      const ws = compatibilityEndpoint(profile.wsBaseUrl, ["ws:", "wss:"]);
      if (http === null || ws === null) return null;
      identities.push([
        target._tag,
        target.connectionId,
        http,
        ws,
        profile.learned === true,
        profile.authorization ?? null,
      ]);
    } else {
      if (profile._tag !== "SshConnectionProfile") return null;
      const { alias, hostname, username, port } = profile.target;
      if (
        !boundedRouteIdentity(alias) ||
        !boundedRouteIdentity(hostname) ||
        (username !== null && !boundedRouteIdentity(username)) ||
        (port !== null && (!Number.isInteger(port) || port < 1 || port > 65_535))
      )
        return null;
      identities.push([target._tag, target.connectionId, alias, hostname, username, port]);
    }
  }
  const binding = JSON.stringify(["saved-routes-v1", environmentId, identities]);
  return binding.length <= MAX_ROUTE_BINDING_LENGTH ? binding : null;
}

export const ConnectionCatalogDocument = Schema.Struct({
  schemaVersion: Schema.Literal(1),
  targets: Schema.Array(PersistedConnectionTarget),
  profiles: Schema.Array(ConnectionProfile),
  credentials: Schema.Array(StoredConnectionCredential),
  remoteDpopTokens: Schema.Array(TokenStore.RemoteDpopAccessToken),
  githubRoutingPermissions: Schema.optionalKey(Schema.Array(StoredGitHubRoutingPermission)),
  // Saved environments the user switched off. They stay registered with their
  // credentials and cache but never connect until switched back on. Older
  // documents predate the key, so decoding defaults it to none.
  disabledEnvironmentIds: Schema.Array(EnvironmentId).pipe(
    Schema.withDecodingDefaultKey(Effect.succeed([])),
  ),
});
export type ConnectionCatalogDocument = typeof ConnectionCatalogDocument.Type;

export const EMPTY_CONNECTION_CATALOG_DOCUMENT: ConnectionCatalogDocument = Object.freeze({
  schemaVersion: 1,
  targets: [],
  profiles: [],
  credentials: [],
  remoteDpopTokens: [],
  disabledEnvironmentIds: [],
});

export function catalogCompatibilityRouteBinding(
  document: ConnectionCatalogDocument,
  environmentId: EnvironmentId,
): string | null {
  return connectionCompatibilityRouteBinding(
    catalogRoutes(document, environmentId).map((target) => ({
      target,
      profile:
        target._tag === "RelayConnectionTarget"
          ? Option.none()
          : Option.fromUndefinedOr(
              document.profiles.find((profile) => profile.connectionId === target.connectionId),
            ),
    })),
  );
}

/** Joins independent historical records to the current saved route configuration. */
export function listConnectionCompatibilityHints(
  document: ConnectionCatalogDocument,
  hints: unknown,
): ReadonlyArray<StoredConnectionCompatibilityHint> {
  return decodeConnectionCompatibilityHints(hints).filter(
    (hint) => catalogCompatibilityRouteBinding(document, hint.environmentId) === hint.routeBinding,
  );
}

/** Only the dedicated hint store is replaced; callers serialize its read and write. */
export function setConnectionCompatibilityHintInRecords(
  records: unknown,
  environmentId: EnvironmentId,
  routeBinding: string,
  kind: StoredConnectionCompatibilityHint["kind"] | null,
): ReadonlyArray<StoredConnectionCompatibilityHint> {
  const hint = Option.getOrNull(
    decodeCompatibilityHint({
      environmentId,
      routeBinding,
      kind: kind ?? "outdated-update",
    }),
  );
  if (hint === null) return decodeConnectionCompatibilityHints(records);
  const remaining = decodeConnectionCompatibilityHints(records).filter(
    (value) => value.environmentId !== environmentId || value.routeBinding !== routeBinding,
  );
  if (kind === null) return remaining;
  if (remaining.length >= MAX_CONNECTION_COMPATIBILITY_HINTS) return remaining;
  return [...remaining, hint];
}

export function putConnectionProfileInCatalog(
  document: ConnectionCatalogDocument,
  profile: ConnectionProfile,
): ConnectionCatalogDocument {
  return {
    ...document,
    profiles: replaceCatalogValue(document.profiles, (value) => value.connectionId, profile),
  };
}

export function removeConnectionProfileFromCatalog(
  document: ConnectionCatalogDocument,
  connectionId: string,
): ConnectionCatalogDocument {
  return {
    ...document,
    profiles: removeCatalogValue(document.profiles, (value) => value.connectionId, connectionId),
  };
}

export function replaceCatalogValue<A>(
  values: ReadonlyArray<A>,
  key: (value: A) => string,
  next: A,
): ReadonlyArray<A> {
  const nextKey = key(next);
  return [...values.filter((value) => key(value) !== nextKey), next];
}

export function removeCatalogValue<A>(
  values: ReadonlyArray<A>,
  key: (value: A) => string,
  removedKey: string,
): ReadonlyArray<A> {
  return values.filter((value) => key(value) !== removedKey);
}

function connectionIdOf(target: ConnectionTarget): string | null {
  switch (target._tag) {
    case "PrimaryConnectionTarget":
    case "RelayConnectionTarget":
      return null;
    case "BearerConnectionTarget":
    case "SshConnectionTarget":
      return target.connectionId;
  }
}

function routeKey(target: ConnectionTarget): string {
  return connectionIdOf(target) ?? target._tag;
}

function removeRouteMetadata(
  document: ConnectionCatalogDocument,
  removed: ReadonlyArray<ConnectionTarget>,
): ConnectionCatalogDocument {
  const connectionIds = new Set(removed.flatMap((target) => connectionIdOf(target) ?? []));
  const relayRemoved = removed.some((target) => target._tag === "RelayConnectionTarget");
  const environmentIds = new Set(removed.map((target) => target.environmentId));
  return {
    ...document,
    profiles: document.profiles.filter((value) => !connectionIds.has(value.connectionId)),
    credentials: document.credentials.filter((value) => !connectionIds.has(value.connectionId)),
    // The DPoP token belongs to the T3 Connect route.
    remoteDpopTokens: relayRemoved
      ? document.remoteDpopTokens.filter((value) => !environmentIds.has(value.environmentId))
      : document.remoteDpopTokens,
  };
}

/**
 * An environment's saved routes in preference order. Targets of one
 * environment keep their relative order in `targets`, so a document written
 * before routes existed is one environment with one route.
 */
export function catalogRoutes(
  document: ConnectionCatalogDocument,
  environmentId: EnvironmentId,
): ReadonlyArray<PersistedConnectionTarget> {
  return document.targets.filter((target) => target.environmentId === environmentId);
}

/**
 * Replaces an environment's routes with `routes`, preferred first. Records
 * owned by a dropped route go with it; the environment keeps its position in
 * the catalog. An empty list leaves the environment's other records in place;
 * use `removeConnectionFromCatalog` to forget the environment.
 */
export function setRoutesInCatalog(
  document: ConnectionCatalogDocument,
  environmentId: EnvironmentId,
  routes: ReadonlyArray<PersistedConnectionTarget>,
): ConnectionCatalogDocument {
  const kept = new Set(routes.map(routeKey));
  const dropped = catalogRoutes(document, environmentId).filter(
    (target) => !kept.has(routeKey(target)),
  );
  const firstIndex = document.targets.findIndex((target) => target.environmentId === environmentId);
  const others = document.targets.filter((target) => target.environmentId !== environmentId);
  const insertAt =
    firstIndex === -1
      ? others.length
      : document.targets.slice(0, firstIndex).filter((t) => t.environmentId !== environmentId)
          .length;
  return {
    ...removeRouteMetadata(document, dropped),
    targets: [...others.slice(0, insertAt), ...routes, ...others.slice(insertAt)],
  };
}

/** Saves one route of an environment, keeping its other routes. */
export function registerConnectionInCatalog(
  document: ConnectionCatalogDocument,
  registration: ConnectionRegistration,
  routes: ReadonlyArray<PersistedConnectionTarget> = [registration.target],
): ConnectionCatalogDocument {
  // Re-registering (for example editing a label or URL) keeps the disabled
  // flag; only `setConnectionEnabledInCatalog` or removal changes it.
  const next = setRoutesInCatalog(document, registration.target.environmentId, routes);

  switch (registration._tag) {
    case "RelayConnectionRegistration":
      return next;
    case "BearerConnectionRegistration":
      return {
        ...putConnectionProfileInCatalog(next, registration.profile),
        credentials: replaceCatalogValue(next.credentials, (value) => value.connectionId, {
          connectionId: registration.target.connectionId,
          credential: registration.credential,
        }),
      };
    case "SshConnectionRegistration":
      return putConnectionProfileInCatalog(next, registration.profile);
  }
}

/** Forgets an environment and every route it had. */
export function removeConnectionFromCatalog(
  document: ConnectionCatalogDocument,
  environmentId: EnvironmentId,
): ConnectionCatalogDocument {
  const next = setRoutesInCatalog(document, environmentId, []);
  return {
    ...next,
    remoteDpopTokens: removeCatalogValue(
      next.remoteDpopTokens,
      (value) => value.environmentId,
      environmentId,
    ),
    disabledEnvironmentIds: removeCatalogValue(
      next.disabledEnvironmentIds,
      (value) => value,
      environmentId,
    ),
    ...(next.githubRoutingPermissions === undefined
      ? {}
      : {
          githubRoutingPermissions: next.githubRoutingPermissions.filter(
            (permission) => permission.environmentId !== environmentId,
          ),
        }),
  };
}

/** Flips the disabled flag for a saved environment; unknown ids are ignored. */
export function setConnectionEnabledInCatalog(
  document: ConnectionCatalogDocument,
  environmentId: EnvironmentId,
  enabled: boolean,
): ConnectionCatalogDocument {
  const registered = document.targets.some((target) => target.environmentId === environmentId);
  const without = removeCatalogValue(
    document.disabledEnvironmentIds,
    (value) => value,
    environmentId,
  );
  return {
    ...document,
    disabledEnvironmentIds: registered && !enabled ? [...without, environmentId] : without,
  };
}

export function putRemoteDpopTokenInCatalog(
  document: ConnectionCatalogDocument,
  token: TokenStore.RemoteDpopAccessToken,
): ConnectionCatalogDocument {
  const registered = document.targets.some(
    (target) =>
      target._tag === "RelayConnectionTarget" && target.environmentId === token.environmentId,
  );
  if (!registered) {
    return document;
  }
  return {
    ...document,
    remoteDpopTokens: replaceCatalogValue(
      document.remoteDpopTokens,
      (value) => value.environmentId,
      token,
    ),
  };
}
