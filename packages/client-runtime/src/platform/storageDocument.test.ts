import { EnvironmentId } from "@t3tools/contracts";
import { describe, expect, it } from "@effect/vitest";
import * as Schema from "effect/Schema";
import * as Effect from "effect/Effect";
import * as Option from "effect/Option";

import * as TokenStore from "../authorization/tokenStore.ts";
import {
  BearerConnectionCredential,
  BearerConnectionProfile,
  BearerConnectionRegistration,
  RelayConnectionRegistration,
  SshConnectionProfile,
  SshConnectionRegistration,
} from "../connection/catalog.ts";
import {
  BearerConnectionTarget,
  ConnectionTransientError,
  PrimaryConnectionTarget,
  RelayConnectionTarget,
  SshConnectionTarget,
} from "../connection/model.ts";
import {
  GitHubRoutingPermissions,
  makeGitHubRoutingPermissions,
} from "../connection/githubRoutingPermissions.ts";
import {
  ConnectionCatalogDocument,
  EMPTY_CONNECTION_CATALOG_DOCUMENT,
  catalogRoutes,
  connectionCompatibilityRouteBinding,
  decodeConnectionCompatibilityHints,
  listConnectionCompatibilityHints,
  setConnectionCompatibilityHintInRecords,
  putConnectionProfileInCatalog,
  removeConnectionProfileFromCatalog,
  setRoutesInCatalog,
  putRemoteDpopTokenInCatalog,
  registerConnectionInCatalog,
  removeConnectionFromCatalog,
  setConnectionEnabledInCatalog,
} from "./storageDocument.ts";

const decodeConnectionCatalogDocument = Schema.decodeUnknownEffect(ConnectionCatalogDocument);

const ENVIRONMENT_ID = EnvironmentId.make("environment-1");
const decodeCatalogDocument = Schema.decodeUnknownSync(ConnectionCatalogDocument);

const RELAY_TARGET = new RelayConnectionTarget({
  environmentId: ENVIRONMENT_ID,
  label: "Remote",
});
const BEARER_TARGET = new BearerConnectionTarget({
  environmentId: ENVIRONMENT_ID,
  label: "Remote",
  connectionId: "bearer-1",
});
const BEARER_PROFILE = new BearerConnectionProfile({
  connectionId: BEARER_TARGET.connectionId,
  environmentId: ENVIRONMENT_ID,
  label: BEARER_TARGET.label,
  httpBaseUrl: "https://remote.example.test",
  wsBaseUrl: "wss://remote.example.test",
});
const BEARER_CREDENTIAL = new BearerConnectionCredential({
  token: "bearer-token",
});
const REMOTE_TOKEN = new TokenStore.RemoteDpopAccessToken({
  environmentId: ENVIRONMENT_ID,
  label: "Remote",
  endpoint: {
    httpBaseUrl: "https://remote.example.test",
    wsBaseUrl: "wss://remote.example.test",
    providerKind: "cloudflare_tunnel",
  },
  accessToken: "dpop-token",
  expiresAtEpochMs: 1_000_000,
  dpopThumbprint: "thumbprint",
});

describe("ConnectionCatalogDocument", () => {
  it.effect("persists explicit GitHub trust and forgets it when a connection is removed", () =>
    Effect.gen(function* () {
      let document = EMPTY_CONNECTION_CATALOG_DOCUMENT;
      const entry = { target: BEARER_TARGET, profile: Option.some(BEARER_PROFILE), enabled: true };
      const storage = {
        read: Effect.sync(() => document.githubRoutingPermissions ?? []),
        write: (githubRoutingPermissions: NonNullable<typeof document.githubRoutingPermissions>) =>
          Effect.gen(function* () {
            document = yield* decodeConnectionCatalogDocument({
              ...document,
              githubRoutingPermissions,
            }).pipe(Effect.orDie);
          }),
      };
      const permissions = yield* makeGitHubRoutingPermissions(storage);
      expect(yield* permissions.get(entry)).toBe("off");
      yield* permissions.set(entry, "read");
      const restarted = yield* makeGitHubRoutingPermissions(storage);
      expect(yield* restarted.get(entry)).toBe("read");

      const changedEndpoint = {
        ...entry,
        profile: Option.some(
          new BearerConnectionProfile({
            ...BEARER_PROFILE,
            httpBaseUrl: "https://different.example.test",
          }),
        ),
      };
      expect(yield* restarted.get(changedEndpoint)).toBe("off");
      expect(
        yield* restarted.get({
          ...entry,
          profile: Option.some(
            new BearerConnectionProfile({
              ...BEARER_PROFILE,
              wsBaseUrl: "wss://different.example.test",
            }),
          ),
        }),
      ).toBe("off");
      expect(
        yield* restarted.get({ target: RELAY_TARGET, profile: Option.none(), enabled: true }),
      ).toBe("off");
      yield* restarted.set(entry, "read-write");
      expect(yield* restarted.get(entry)).toBe("read-write");
      yield* restarted.forget(ENVIRONMENT_ID);
      expect(yield* restarted.get(entry)).toBe("off");
      const afterRemoval = yield* makeGitHubRoutingPermissions(storage);
      expect(yield* afterRemoval.get(entry)).toBe("off");

      yield* permissions.set(entry, "read");
      document = removeConnectionFromCatalog(document, ENVIRONMENT_ID);
      const afterCatalogRemoval = yield* makeGitHubRoutingPermissions(storage);
      expect(yield* afterCatalogRemoval.get(entry)).toBe("off");
    }),
  );

  it.effect("requires explicit trust for primary, relay, and SSH connections independently", () =>
    Effect.gen(function* () {
      const permissions = yield* makeGitHubRoutingPermissions({
        read: Effect.succeed([]),
        write: () => Effect.void,
      });
      const entries = [
        {
          target: new PrimaryConnectionTarget({
            environmentId: ENVIRONMENT_ID,
            label: "Local",
            httpBaseUrl: "http://localhost:3000",
            wsBaseUrl: "ws://localhost:3000",
          }),
          profile: Option.none(),
          enabled: true,
        },
        { target: RELAY_TARGET, profile: Option.none(), enabled: true },
        {
          enabled: true,
          target: new SshConnectionTarget({
            environmentId: ENVIRONMENT_ID,
            label: "SSH",
            connectionId: "ssh-1",
          }),
          profile: Option.some(
            new SshConnectionProfile({
              environmentId: ENVIRONMENT_ID,
              label: "SSH",
              connectionId: "ssh-1",
              target: { alias: "work", hostname: "work.example.test", username: "maria", port: 22 },
            }),
          ),
        },
      ];
      for (const entry of entries) {
        expect(yield* permissions.get(entry)).toBe("off");
        yield* permissions.set(entry, "read");
        expect(yield* permissions.get(entry)).toBe("read");
        yield* permissions.set(entry, "off");
        expect(yield* permissions.get(entry)).toBe("off");
      }
    }),
  );

  it.effect("does not enable GitHub routing when permission persistence fails", () =>
    Effect.gen(function* () {
      const entry = { target: BEARER_TARGET, profile: Option.some(BEARER_PROFILE), enabled: true };
      expect(yield* (yield* GitHubRoutingPermissions).get(entry)).toBe("off");
      const permissions = yield* makeGitHubRoutingPermissions({
        read: Effect.succeed([]),
        write: () =>
          Effect.fail(
            new ConnectionTransientError({
              reason: "remote-unavailable",
              detail: "storage unavailable",
            }),
          ),
      });
      yield* permissions.set(entry, "read-write").pipe(Effect.flip);
      expect(yield* permissions.get(entry)).toBe("off");
    }),
  );

  it.each([
    { name: "legacy", accountId: undefined },
    { name: "account-bound", accountId: "account-1" },
  ])("round-trips a catalog containing a $name DPoP token", ({ accountId }) => {
    const token = new TokenStore.RemoteDpopAccessToken({
      ...REMOTE_TOKEN,
      ...(accountId === undefined ? {} : { accountId }),
    });
    const document = {
      ...EMPTY_CONNECTION_CATALOG_DOCUMENT,
      targets: [RELAY_TARGET],
      remoteDpopTokens: [token],
    };
    const schema = Schema.fromJsonString(ConnectionCatalogDocument);
    const restored = Schema.decodeSync(schema)(Schema.encodeSync(schema)(document));

    expect(restored).toEqual(document);
    expect(restored.remoteDpopTokens[0]?.accountId).toBe(accountId);
  });

  it("registers a bearer connection as one catalog mutation", () => {
    const document = registerConnectionInCatalog(
      EMPTY_CONNECTION_CATALOG_DOCUMENT,
      new BearerConnectionRegistration({
        target: BEARER_TARGET,
        profile: BEARER_PROFILE,
        credential: BEARER_CREDENTIAL,
      }),
    );

    expect(document.targets).toEqual([BEARER_TARGET]);
    expect(document.profiles).toEqual([BEARER_PROFILE]);
    expect(document.credentials).toEqual([
      {
        connectionId: BEARER_TARGET.connectionId,
        credential: BEARER_CREDENTIAL,
      },
    ]);
  });

  it("replaces obsolete connection metadata without discarding a reusable DPoP token", () => {
    const bearer = registerConnectionInCatalog(
      {
        ...EMPTY_CONNECTION_CATALOG_DOCUMENT,
        remoteDpopTokens: [REMOTE_TOKEN],
      },
      new BearerConnectionRegistration({
        target: BEARER_TARGET,
        profile: BEARER_PROFILE,
        credential: BEARER_CREDENTIAL,
      }),
    );
    const relayTarget = new RelayConnectionTarget({
      environmentId: ENVIRONMENT_ID,
      label: "Remote",
    });
    const relay = registerConnectionInCatalog(
      bearer,
      new RelayConnectionRegistration({ target: relayTarget }),
    );

    expect(relay.targets).toEqual([relayTarget]);
    expect(relay.profiles).toEqual([]);
    expect(relay.credentials).toEqual([]);
    expect(relay.remoteDpopTokens).toEqual([REMOTE_TOKEN]);
  });

  it("removes every catalog record owned by an explicit disconnect", () => {
    const registered = registerConnectionInCatalog(
      {
        ...EMPTY_CONNECTION_CATALOG_DOCUMENT,
        remoteDpopTokens: [REMOTE_TOKEN],
      },
      new BearerConnectionRegistration({
        target: BEARER_TARGET,
        profile: BEARER_PROFILE,
        credential: BEARER_CREDENTIAL,
      }),
    );

    expect(removeConnectionFromCatalog(registered, ENVIRONMENT_ID)).toEqual(
      EMPTY_CONNECTION_CATALOG_DOCUMENT,
    );
  });

  it("stores a DPoP token for a registered relay environment", () => {
    const registered = registerConnectionInCatalog(
      EMPTY_CONNECTION_CATALOG_DOCUMENT,
      new RelayConnectionRegistration({ target: RELAY_TARGET }),
    );

    expect(putRemoteDpopTokenInCatalog(registered, REMOTE_TOKEN)).toEqual({
      ...registered,
      remoteDpopTokens: [REMOTE_TOKEN],
    });
  });

  it("ignores a late DPoP token after the environment is removed", () => {
    const registered = registerConnectionInCatalog(
      EMPTY_CONNECTION_CATALOG_DOCUMENT,
      new RelayConnectionRegistration({ target: RELAY_TARGET }),
    );
    const removed = removeConnectionFromCatalog(registered, ENVIRONMENT_ID);

    expect(putRemoteDpopTokenInCatalog(removed, REMOTE_TOKEN)).toEqual(
      EMPTY_CONNECTION_CATALOG_DOCUMENT,
    );
  });

  it("removes a DPoP token stored before the environment is removed", () => {
    const registered = registerConnectionInCatalog(
      EMPTY_CONNECTION_CATALOG_DOCUMENT,
      new RelayConnectionRegistration({ target: RELAY_TARGET }),
    );
    const refreshed = putRemoteDpopTokenInCatalog(registered, REMOTE_TOKEN);

    expect(removeConnectionFromCatalog(refreshed, ENVIRONMENT_ID)).toEqual(
      EMPTY_CONNECTION_CATALOG_DOCUMENT,
    );
  });

  it("does not store DPoP tokens for a different environment or connection kind", () => {
    const bearer = registerConnectionInCatalog(
      EMPTY_CONNECTION_CATALOG_DOCUMENT,
      new BearerConnectionRegistration({
        target: BEARER_TARGET,
        profile: BEARER_PROFILE,
        credential: BEARER_CREDENTIAL,
      }),
    );
    const otherRelay = registerConnectionInCatalog(
      EMPTY_CONNECTION_CATALOG_DOCUMENT,
      new RelayConnectionRegistration({
        target: new RelayConnectionTarget({
          environmentId: EnvironmentId.make("environment-2"),
          label: "Other relay",
        }),
      }),
    );

    expect(putRemoteDpopTokenInCatalog(bearer, REMOTE_TOKEN)).toBe(bearer);
    expect(putRemoteDpopTokenInCatalog(otherRelay, REMOTE_TOKEN)).toBe(otherRelay);
  });

  it("decodes a document written before the disabled list existed", () => {
    const decoded = decodeCatalogDocument({
      schemaVersion: 1,
      targets: [],
      profiles: [],
      credentials: [],
      remoteDpopTokens: [],
    });

    expect(decoded.disabledEnvironmentIds).toEqual([]);
  });

  it("switches a saved environment off and back on without touching its records", () => {
    const registered = registerConnectionInCatalog(
      EMPTY_CONNECTION_CATALOG_DOCUMENT,
      new BearerConnectionRegistration({
        target: BEARER_TARGET,
        profile: BEARER_PROFILE,
        credential: BEARER_CREDENTIAL,
      }),
    );

    const disabled = setConnectionEnabledInCatalog(registered, ENVIRONMENT_ID, false);
    expect(disabled.disabledEnvironmentIds).toEqual([ENVIRONMENT_ID]);
    expect(disabled.targets).toEqual(registered.targets);
    expect(disabled.credentials).toEqual(registered.credentials);
    expect(
      setConnectionEnabledInCatalog(disabled, ENVIRONMENT_ID, false).disabledEnvironmentIds,
    ).toEqual([ENVIRONMENT_ID]);

    expect(
      setConnectionEnabledInCatalog(disabled, ENVIRONMENT_ID, true).disabledEnvironmentIds,
    ).toEqual([]);
    expect(
      registerConnectionInCatalog(
        disabled,
        new BearerConnectionRegistration({
          target: BEARER_TARGET,
          profile: BEARER_PROFILE,
          credential: BEARER_CREDENTIAL,
        }),
      ).disabledEnvironmentIds,
    ).toEqual([ENVIRONMENT_ID]);
    expect(removeConnectionFromCatalog(disabled, ENVIRONMENT_ID).disabledEnvironmentIds).toEqual(
      [],
    );
  });

  it("persists the normalized SSH profile and its paired token beside its target", () => {
    const target = new SshConnectionTarget({
      environmentId: ENVIRONMENT_ID,
      label: "SSH",
      connectionId: "ssh-1",
    });
    const profile = new SshConnectionProfile({
      connectionId: target.connectionId,
      environmentId: target.environmentId,
      label: target.label,
      target: {
        alias: "devbox",
        hostname: "devbox.example.test",
        username: "developer",
        port: 22,
      },
    });
    const document = registerConnectionInCatalog(
      EMPTY_CONNECTION_CATALOG_DOCUMENT,
      new SshConnectionRegistration({ target, profile, credential: BEARER_CREDENTIAL }),
    );

    expect(document.targets).toEqual([target]);
    expect(document.profiles).toEqual([profile]);
    expect(document.credentials).toEqual([
      { connectionId: target.connectionId, credential: BEARER_CREDENTIAL },
    ]);
    // Removing the route forgets its token too.
    expect(removeConnectionFromCatalog(document, ENVIRONMENT_ID).credentials).toEqual([]);
  });

  it("keeps every route of an environment and drops only a removed route's records", () => {
    const withBearer = registerConnectionInCatalog(
      { ...EMPTY_CONNECTION_CATALOG_DOCUMENT, remoteDpopTokens: [REMOTE_TOKEN] },
      new BearerConnectionRegistration({
        target: BEARER_TARGET,
        profile: BEARER_PROFILE,
        credential: BEARER_CREDENTIAL,
      }),
    );
    const withRelay = registerConnectionInCatalog(
      withBearer,
      new RelayConnectionRegistration({ target: RELAY_TARGET }),
      [BEARER_TARGET, RELAY_TARGET],
    );
    expect(catalogRoutes(withRelay, ENVIRONMENT_ID)).toEqual([BEARER_TARGET, RELAY_TARGET]);
    expect(withRelay.credentials).toHaveLength(1);

    const relayOnly = setRoutesInCatalog(withRelay, ENVIRONMENT_ID, [RELAY_TARGET]);
    expect(relayOnly.targets).toEqual([RELAY_TARGET]);
    expect(relayOnly.profiles).toEqual([]);
    expect(relayOnly.credentials).toEqual([]);
    expect(relayOnly.remoteDpopTokens).toEqual([REMOTE_TOKEN]);

    const bearerOnly = setRoutesInCatalog(withRelay, ENVIRONMENT_ID, [BEARER_TARGET]);
    expect(bearerOnly.credentials).toHaveLength(1);
    expect(bearerOnly.remoteDpopTokens).toEqual([]);
  });

  it("keeps an environment's position in the catalog when its routes change", () => {
    const other = new RelayConnectionTarget({
      environmentId: EnvironmentId.make("environment-2"),
      label: "Other",
    });
    const document = {
      ...EMPTY_CONNECTION_CATALOG_DOCUMENT,
      targets: [RELAY_TARGET, other],
    };
    expect(
      setRoutesInCatalog(document, ENVIRONMENT_ID, [BEARER_TARGET, RELAY_TARGET]).targets,
    ).toEqual([BEARER_TARGET, RELAY_TARGET, other]);
  });
});

describe("saved compatibility hints", () => {
  const bearerRoute = { target: BEARER_TARGET, profile: Option.some(BEARER_PROFILE) };
  const relayRoute = { target: RELAY_TARGET, profile: Option.none() };
  const binding = connectionCompatibilityRouteBinding([bearerRoute])!;
  const hint = {
    environmentId: ENVIRONMENT_ID,
    routeBinding: binding,
    kind: "outdated-update",
  } as const;
  const registration = new BearerConnectionRegistration({
    target: BEARER_TARGET,
    profile: BEARER_PROFILE,
    credential: BEARER_CREDENTIAL,
  });
  const savedOff = setConnectionEnabledInCatalog(
    registerConnectionInCatalog(EMPTY_CONNECTION_CATALOG_DOCUMENT, registration),
    ENVIRONMENT_ID,
    false,
  );

  it("round-trips independent presentation without changing an Off catalog", () => {
    const records = setConnectionCompatibilityHintInRecords([], ENVIRONMENT_ID, binding, hint.kind);
    const restored = decodeConnectionCompatibilityHints(JSON.parse(JSON.stringify(records)));
    expect(listConnectionCompatibilityHints(savedOff, restored)).toEqual([hint]);
    expect(savedOff.disabledEnvironmentIds).toEqual([ENVIRONMENT_ID]);
    expect(Object.keys(restored[0]!).sort()).toEqual(["environmentId", "kind", "routeBinding"]);
    expect(JSON.stringify(restored)).not.toContain(BEARER_CREDENTIAL.token);
    expect(
      setConnectionCompatibilityHintInRecords(restored, ENVIRONMENT_ID, binding, null),
    ).toEqual([]);
    expect("compatibilityHints" in savedOff).toBe(false);
  });

  it("drops malformed hint records without changing a valid catalog", () => {
    const records = decodeConnectionCompatibilityHints([
      null,
      { ...hint, kind: "connected" },
      { ...hint, credential: { token: "secret" } },
      { ...hint, routeBinding: "x".repeat(16_385) },
      hint,
      hint,
    ]);
    expect(records).toEqual([hint]);
    for (const invalid of [null, {}, "bad", Array(129).fill(hint), undefined]) {
      expect(decodeConnectionCompatibilityHints(invalid)).toEqual([]);
      expect(decodeCatalogDocument({ ...savedOff, compatibilityHints: invalid })).toEqual(savedOff);
    }
  });

  it("joins only exact current routes and never reads an embedded hint fallback", () => {
    expect(
      listConnectionCompatibilityHints(savedOff, [
        { ...hint, routeBinding: "different-route" },
        { ...hint, environmentId: EnvironmentId.make("unknown") },
      ]),
    ).toEqual([]);
    const old = decodeCatalogDocument({ ...savedOff, compatibilityHints: [hint] });
    expect(listConnectionCompatibilityHints(old, [])).toEqual([]);
  });

  it("invalidates reorder, replacement, removal and dropped routes in the fresh join", () => {
    const pair = setRoutesInCatalog(savedOff, ENVIRONMENT_ID, [BEARER_TARGET, RELAY_TARGET]);
    const pairHint = {
      ...hint,
      routeBinding: connectionCompatibilityRouteBinding([bearerRoute, relayRoute])!,
    };
    expect(listConnectionCompatibilityHints(pair, [pairHint])).toEqual([pairHint]);
    for (const routes of [[RELAY_TARGET, BEARER_TARGET], [BEARER_TARGET], [RELAY_TARGET], []]) {
      expect(
        listConnectionCompatibilityHints(setRoutesInCatalog(pair, ENVIRONMENT_ID, routes), [
          pairHint,
        ]),
      ).toEqual([]);
    }
    expect(
      listConnectionCompatibilityHints(removeConnectionFromCatalog(savedOff, ENVIRONMENT_ID), [
        hint,
      ]),
    ).toEqual([]);
    const replacement = new BearerConnectionRegistration({
      ...registration,
      target: new BearerConnectionTarget({ ...BEARER_TARGET, connectionId: "replacement" }),
      profile: new BearerConnectionProfile({ ...BEARER_PROFILE, connectionId: "replacement" }),
    });
    expect(
      listConnectionCompatibilityHints(registerConnectionInCatalog(savedOff, replacement), [hint]),
    ).toEqual([]);
  });

  it("retains a new binding when an old binding is cleared", () => {
    const changed = new BearerConnectionProfile({
      ...BEARER_PROFILE,
      httpBaseUrl: "https://replacement.example.test",
    });
    const edited = putConnectionProfileInCatalog(savedOff, changed);
    expect(listConnectionCompatibilityHints(edited, [hint])).toEqual([]);
    expect(
      listConnectionCompatibilityHints(
        removeConnectionProfileFromCatalog(savedOff, BEARER_TARGET.connectionId),
        [hint],
      ),
    ).toEqual([]);
    const freshBinding = connectionCompatibilityRouteBinding([
      { target: BEARER_TARGET, profile: Option.some(changed) },
    ])!;
    const records = setConnectionCompatibilityHintInRecords(
      [hint],
      ENVIRONMENT_ID,
      freshBinding,
      hint.kind,
    );
    const cleared = setConnectionCompatibilityHintInRecords(records, ENVIRONMENT_ID, binding, null);
    expect(listConnectionCompatibilityHints(edited, cleared)).toEqual([
      { ...hint, routeBinding: freshBinding },
    ]);
    expect(edited.disabledEnvironmentIds).toEqual([ENVIRONMENT_ID]);
  });

  it("normalizes URL identity while excluding labels and credentials", () => {
    const renamed = new BearerConnectionProfile({
      ...BEARER_PROFILE,
      label: "Renamed",
      httpBaseUrl: "https://REMOTE.example.test:443/",
    });
    expect(
      connectionCompatibilityRouteBinding([
        {
          target: new BearerConnectionTarget({ ...BEARER_TARGET, label: "Renamed" }),
          profile: Option.some(renamed),
        },
      ]),
    ).toBe(binding);
    const refreshed = registerConnectionInCatalog(
      savedOff,
      new BearerConnectionRegistration({
        ...registration,
        profile: renamed,
        credential: new BearerConnectionCredential({ token: "replacement-secret" }),
      }),
    );
    expect(listConnectionCompatibilityHints(refreshed, [hint])).toEqual([hint]);
    expect(JSON.stringify(hint)).not.toContain("replacement-secret");
    for (const fields of [
      { wsBaseUrl: "wss://replacement.example.test" },
      { learned: true as const },
      { authorization: "t3-connect" as const },
    ]) {
      expect(
        connectionCompatibilityRouteBinding([
          {
            target: BEARER_TARGET,
            profile: Option.some(new BearerConnectionProfile({ ...BEARER_PROFILE, ...fields })),
          },
        ]),
      ).not.toBe(binding);
    }
  });

  it.each([
    "https://user:secret@example.test",
    "https://example.test?token=secret",
    "https://example.test#secret",
    "file:///tmp/not-an-endpoint",
    "not-a-url",
  ])("does not persist unsafe endpoint identity: %s", (httpBaseUrl) => {
    expect(
      connectionCompatibilityRouteBinding([
        {
          target: BEARER_TARGET,
          profile: Option.some(new BearerConnectionProfile({ ...BEARER_PROFILE, httpBaseUrl })),
        },
      ]),
    ).toBeNull();
  });

  it("binds every saved SSH configuration field", () => {
    const target = new SshConnectionTarget({
      environmentId: ENVIRONMENT_ID,
      label: "SSH",
      connectionId: "ssh-1",
    });
    const profile = new SshConnectionProfile({
      environmentId: ENVIRONMENT_ID,
      connectionId: target.connectionId,
      label: "SSH",
      target: { alias: "mini", hostname: "mini.example.test", username: "user", port: 22 },
    });
    const original = connectionCompatibilityRouteBinding([
      { target, profile: Option.some(profile) },
    ]);
    expect(original).not.toBeNull();
    for (const fields of [
      { alias: "other" },
      { hostname: "other.example.test" },
      { username: "other" },
      { port: 2222 },
    ]) {
      expect(
        connectionCompatibilityRouteBinding([
          {
            target,
            profile: Option.some(
              new SshConnectionProfile({ ...profile, target: { ...profile.target, ...fields } }),
            ),
          },
        ]),
      ).not.toBe(original);
    }
  });

  it("refuses missing, duplicate, primary, cross-environment and oversized route bindings", () => {
    expect(connectionCompatibilityRouteBinding([])).toBeNull();
    expect(
      connectionCompatibilityRouteBinding([{ target: BEARER_TARGET, profile: Option.none() }]),
    ).toBeNull();
    expect(connectionCompatibilityRouteBinding([bearerRoute, bearerRoute])).toBeNull();
    expect(connectionCompatibilityRouteBinding(Array(17).fill(relayRoute))).toBeNull();
    expect(
      connectionCompatibilityRouteBinding([
        bearerRoute,
        {
          target: new RelayConnectionTarget({
            environmentId: EnvironmentId.make("other"),
            label: "Other",
          }),
          profile: Option.none(),
        },
      ]),
    ).toBeNull();
    expect(
      connectionCompatibilityRouteBinding([
        {
          target: new PrimaryConnectionTarget({
            environmentId: ENVIRONMENT_ID,
            label: "Primary",
            httpBaseUrl: "http://localhost",
            wsBaseUrl: "ws://localhost",
          }),
          profile: Option.none(),
        },
      ]),
    ).toBeNull();
  });
});
