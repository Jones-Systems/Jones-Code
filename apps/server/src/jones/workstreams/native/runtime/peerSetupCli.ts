// @effect-diagnostics nodeBuiltinImport:off -- Explicit local CLI reads fixed existing owner paths and the server guard checks the actual Node socket.
// @effect-diagnostics effectFnOpportunity:off -- CLI/service callbacks are framework boundaries.
import * as NodeFS from "node:fs";
import * as NodePath from "node:path";
import * as NodeHttp from "node:http";
import * as NodeSqlite from "@t3tools/shared/nodeSqliteClient";
import * as NodeCrypto from "node:crypto";
import * as DateTime from "effect/DateTime";
import * as AuthRows from "../../../../persistence/AuthSessions.ts";
import {
  AuthAccessWriteScope,
  AuthSessionId,
  EnvironmentHttpBadRequestError,
} from "@t3tools/contracts";
import * as HttpClient from "effect/http/HttpClient";
import * as HttpClientRequest from "effect/http/HttpClientRequest";
import * as FetchHttpClient from "effect/http/FetchHttpClient";
import * as HttpServerRequest from "effect/http/HttpServerRequest";
import * as HttpServerResponse from "effect/http/HttpServerResponse";
import * as Stream from "effect/Stream";
import * as Duration from "effect/Duration";
import * as Option from "effect/Option";
import * as Sessions from "../../../../auth/SessionStore.ts";
import * as Secrets from "../../../../auth/ServerSecretStore.ts";
import { PersistedServerRuntimeState, isProcessAlive } from "../../../../serverRuntimeState.ts";
import { holdJonesServerNativeWriterFence } from "../../../updates/nativeWriterStartup.ts";
import { makeNativeEnrollmentSqliteConfig } from "../../enrollment/sqliteConnection.ts";
import { makeExistingEnrollmentSecretStore } from "../../enrollment/secretStore.ts";
import { annotateEnvironmentRequest, requireEnvironmentScope } from "../../../../auth/http.ts";
import type * as Setup from "./peerSetup.ts";
import * as W from "@t3tools/contracts/jones/workstreamsPeerSetup";
import * as Console from "effect/Console";
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";
import { Command, Flag } from "effect/cli";
import * as Path from "effect/Path";
import * as Layer from "effect/Layer";
import * as SqlClient from "effect/sql/SqlClient";
import { PeerSessionError } from "./peerSessionPins.ts";

const decodeOwnerRequest = Schema.decodeUnknownEffect(W.WorkstreamsPeerSetupOwnerRequest);
const decodeSetupResult = Schema.decodeUnknownEffect(W.WorkstreamsPeerSetupResult);
const decodeOwnerJson = Schema.decodeUnknownEffect(
  Schema.fromJsonString(W.WorkstreamsPeerSetupOwnerRequest),
);
const encodeSetupResultJson = Schema.encodeEffect(
  Schema.fromJsonString(W.WorkstreamsPeerSetupResult),
);
const encodeOwnerJson = Schema.encodeSync(
  Schema.fromJsonString(W.WorkstreamsPeerSetupOwnerRequest),
);
const decodeSetupResultJson = Schema.decodeUnknownEffect(
  Schema.fromJsonString(W.WorkstreamsPeerSetupResult),
);
const decodeRuntimeJson = Schema.decodeEffect(Schema.fromJsonString(PersistedServerRuntimeState));
const decodeDatabaseList = Schema.decodeUnknownEffect(
  Schema.Array(Schema.Struct({ name: Schema.String, file: Schema.String })),
);

/** Root supplies a fixed authenticated loopback/native-owner endpoint for the selected home.
 * No URL, tool name, callback, credentials, receipt or session-registration parameter exists. */
export class PeerSetupCliEndpoint extends Context.Service<
  PeerSetupCliEndpoint,
  {
    readonly run: (
      home: string,
      input: W.WorkstreamsPeerSetupOwnerRequest,
    ) => Effect.Effect<W.WorkstreamsPeerSetupResult, PeerSessionError>;
  }
>()("t3/jones/workstreams/native/runtime/peerSetupCli/PeerSetupCliEndpoint") {}
export const runPeerSetupCli = Effect.fn("PeerSetupCli.run")(function* (
  home: string,
  input: W.WorkstreamsPeerSetupOwnerRequest,
) {
  const path = yield* Path.Path;
  if (!path.isAbsolute(home) || path.resolve(home) !== home)
    return yield* new PeerSessionError({ code: "invalid_request" });
  const request = yield* decodeOwnerRequest(input).pipe(
    Effect.mapError(() => new PeerSessionError({ code: "invalid_request" })),
  );
  const result = yield* (yield* PeerSetupCliEndpoint).run(home, request);
  return yield* decodeSetupResult(result).pipe(
    Effect.mapError(() => new PeerSessionError({ code: "invalid_request" })),
  );
});
const common = {
  home: Flag.String("home"),
  setupId: Flag.String("setup-id"),
  registryId: Flag.String("registry-id"),
  ownerId: Flag.String("owner-id"),
  homeEnvironmentId: Flag.String("home-environment-id"),
  homeGeneration: Flag.Int("home-generation"),
  sourceEnvironmentId: Flag.String("source-environment-id"),
};
const address = (args: {
  setupId: string;
  registryId: string;
  ownerId: string;
  homeEnvironmentId: string;
  homeGeneration: number;
  sourceEnvironmentId: string;
}) => ({
  setup_id: args.setupId,
  source_environment_id: args.sourceEnvironmentId,
  expected_home: {
    registry_instance_id: args.registryId,
    owner_id: args.ownerId,
    home_environment_id: args.homeEnvironmentId,
    server_generation: args.homeGeneration,
  },
});
const print = (home: string, request: W.WorkstreamsPeerSetupOwnerRequest) =>
  runPeerSetupCli(home, request).pipe(
    Effect.flatMap((result) => Console.log(JSON.stringify(result))),
  );
const prepare = Command.make(
  "prepare",
  { ...common, lifetimeDays: Flag.Int("lifetime-days").pipe(Flag.withDefault(1)) },
  (args) =>
    print(args.home, { operation: "prepare", ...address(args), lifetime_days: args.lifetimeDays }),
);
const followup = (operation: "apply" | "status" | "resume") =>
  Command.make(
    operation,
    {
      ...common,
      requestSha256: Flag.String("request-sha256"),
    },
    (args) => print(args.home, { operation, ...address(args), request_sha256: args.requestSha256 }),
  );
/** @public Root registers under workstream; commands never open/migrate a database offline. */
export const command = Command.make("peer-setup").pipe(
  Command.withSubcommands([prepare, followup("apply"), followup("status"), followup("resume")]),
);

export const PEER_SETUP_OWNER_PATH = "/api/workstreams/native/peer-setup/v1/operations";
const unavailable = () => new PeerSessionError({ code: "unavailable" });
const forbidden = () =>
  new EnvironmentHttpBadRequestError({ message: "native_peer_setup_owner_unavailable" });
/** A separate fixed endpoint preserves the existing desktop-owner endpoint's boundary. */
export const assertLocalPeerSetupOwnerRequest = (
  request: HttpServerRequest.HttpServerRequest,
  principal: { readonly method: string; readonly scopes: ReadonlySet<string> },
): void => {
  if (
    !(request.source instanceof NodeHttp.IncomingMessage) ||
    !["127.0.0.1", "::1", "::ffff:127.0.0.1"].includes(request.source.socket.remoteAddress ?? "") ||
    request.method !== "POST" ||
    request.originalUrl !== PEER_SETUP_OWNER_PATH ||
    principal.method !== "bearer-access-token" ||
    !principal.scopes.has(AuthAccessWriteScope) ||
    Object.keys(request.headers).some(
      (key) =>
        key === "origin" ||
        key === "cookie" ||
        key === "forwarded" ||
        key === "x-real-ip" ||
        key.startsWith("x-forwarded-") ||
        key.startsWith("tailscale-"),
    )
  )
    throw forbidden();
};
const boundedText = <E>(stream: Stream.Stream<Uint8Array, E>) =>
  Stream.runFoldEffect(
    stream,
    () => new Uint8Array(),
    (prior, bytes) => {
      if (prior.byteLength + bytes.byteLength > 8192) return Effect.fail(unavailable());
      const next = new Uint8Array(prior.byteLength + bytes.byteLength);
      next.set(prior);
      next.set(bytes, prior.byteLength);
      return Effect.succeed(next);
    },
  ).pipe(
    Effect.flatMap((bytes) =>
      Effect.try({
        try: () => new TextDecoder("utf-8", { fatal: true }).decode(bytes),
        catch: unavailable,
      }),
    ),
    Effect.mapError(unavailable),
  );
export const readPeerSetupOwnerRequest = <E>(body: Stream.Stream<Uint8Array, E>) =>
  boundedText(body).pipe(Effect.flatMap(decodeOwnerJson), Effect.mapError(forbidden));
export const createPeerSetupOwnerHandler = (setup: Setup.PeerSetup["Service"] | undefined) =>
  Effect.fn("environment.workstreams.native.peerSetupOwner")(function* () {
    yield* annotateEnvironmentRequest("workstreams.native.peerSetupOwner");
    const principal = yield* requireEnvironmentScope(AuthAccessWriteScope);
    const request = yield* HttpServerRequest.HttpServerRequest;
    yield* Effect.try({
      try: () => assertLocalPeerSetupOwnerRequest(request, principal),
      catch: forbidden,
    });
    const operation = yield* readPeerSetupOwnerRequest(request.stream);
    if (!setup) return yield* forbidden();
    const result = yield* setup.ownerOperation(operation).pipe(Effect.mapError(forbidden));
    const output = yield* encodeSetupResultJson(result).pipe(Effect.mapError(forbidden));
    return HttpServerResponse.text(output, {
      headers: {
        "content-type": "application/json",
        "cache-control": "private, no-store",
        "x-content-type-options": "nosniff",
      },
    });
  });

/** Internal client capability: no URL/token is accepted on the CLI or setup wire.
 * The live binding below derives this one loopback origin from the selected private runtime file. */
export class PeerSetupCliSessions extends Context.Service<
  PeerSetupCliSessions,
  Pick<Sessions.SessionStore["Service"], "issue" | "revoke">
>()("t3/jones/workstreams/native/runtime/peerSetupCli/PeerSetupCliSessions") {}
export const makePeerSetupCliHttpEndpoint = Effect.fn("PeerSetupCli.http")(function* (
  selectedHome: string,
  origin: string,
) {
  const url = yield* Effect.try({ try: () => new URL(origin), catch: unavailable });
  if (
    url.protocol !== "http:" ||
    !["127.0.0.1", "[::1]"].includes(url.hostname) ||
    url.origin !== origin ||
    url.username ||
    url.password ||
    !url.port
  )
    return yield* unavailable();
  const sessions = yield* PeerSetupCliSessions,
    http = yield* HttpClient.HttpClient;
  return PeerSetupCliEndpoint.of({
    run: (home, request) => {
      if (home !== selectedHome) return Effect.fail(unavailable());
      return Effect.acquireUseRelease(
        sessions.issue({
          subject: "workstreams-peer-setup-cli",
          method: "bearer-access-token",
          scopes: [AuthAccessWriteScope],
          ttl: Duration.minutes(5),
          client: { deviceType: "bot", label: "workstreams peer setup CLI" },
        }),
        (issued) =>
          http
            .execute(
              HttpClientRequest.post(origin + PEER_SETUP_OWNER_PATH).pipe(
                HttpClientRequest.setHeader("authorization", "Bearer " + issued.token),
                HttpClientRequest.bodyText(encodeOwnerJson(request), "application/json"),
              ),
            )
            .pipe(
              Effect.flatMap((response) =>
                response.status === 200 ? boundedText(response.stream) : Effect.fail(unavailable()),
              ),
              Effect.flatMap(decodeSetupResultJson),
            ),
        (issued) => sessions.revoke(issued.sessionId).pipe(Effect.ignore),
      ).pipe(Effect.scoped, Effect.mapError(unavailable));
    },
  });
});
const safeLocalFile = (home: string, file: string, limit: number, read: boolean) => {
  if (
    !NodePath.isAbsolute(home) ||
    NodePath.resolve(home) !== home ||
    NodeFS.realpathSync(home) !== home ||
    !file.startsWith(home + NodePath.sep)
  )
    throw unavailable();
  let dir = home;
  for (const part of NodePath.relative(home, NodePath.dirname(file))
    .split(NodePath.sep)
    .filter(Boolean)) {
    const stat = NodeFS.lstatSync(dir);
    if (
      !stat.isDirectory() ||
      stat.isSymbolicLink() ||
      stat.uid !== process.getuid?.() ||
      stat.mode & 0o022
    )
      throw unavailable();
    dir = NodePath.join(dir, part);
  }
  const d = NodeFS.lstatSync(dir),
    stat = NodeFS.lstatSync(file);
  if (
    !d.isDirectory() ||
    d.isSymbolicLink() ||
    d.uid !== process.getuid?.() ||
    d.mode & 0o022 ||
    !stat.isFile() ||
    stat.isSymbolicLink() ||
    stat.uid !== process.getuid?.() ||
    stat.mode & 0o022 ||
    stat.nlink !== 1 ||
    stat.size > limit
  )
    throw unavailable();
  if (!read) return "";
  const fd = NodeFS.openSync(file, NodeFS.constants.O_RDONLY | NodeFS.constants.O_NOFOLLOW);
  try {
    const actual = NodeFS.fstatSync(fd);
    if (actual.dev !== stat.dev || actual.ino !== stat.ino || actual.size > limit)
      throw unavailable();
    const bytes = Buffer.alloc(limit + 1),
      count = NodeFS.readSync(fd, bytes, 0, bytes.length, 0);
    if (count > limit) throw unavailable();
    return new TextDecoder("utf-8", { fatal: true }).decode(bytes.subarray(0, count));
  } finally {
    NodeFS.closeSync(fd);
  }
};
/** Existing signing key is loaded before issuance writers. Reuses the production reservation signer. */
export const existingCliSessionsLayer = Layer.effect(
  PeerSetupCliSessions,
  Effect.gen(function* () {
    const sql = yield* SqlClient.SqlClient,
      rows = yield* AuthRows.AuthSessionRepository;
    // Reuse the existing signer and reservation matcher; no auth crypto is reimplemented.
    const materialize = yield* Sessions.makeReservedBearerSessionMaterializer;
    return PeerSetupCliSessions.of({
      issue: Effect.fn("PeerSetupCli.issue")(
        function* () {
          const now = DateTime.toUtc(yield* DateTime.now);
          const session: AuthRows.CreateAuthSessionInput = {
            sessionId: AuthSessionId.make(NodeCrypto.randomUUID()),
            subject: "workstreams-peer-setup-cli",
            method: "bearer-access-token",
            scopes: [AuthAccessWriteScope],
            client: {
              label: "workstreams peer setup CLI",
              deviceType: "bot",
              ipAddress: null,
              userAgent: null,
              os: null,
              browser: null,
            },
            issuedAt: now,
            expiresAt: DateTime.toUtc(DateTime.add(now, { minutes: 5 })),
          };
          return yield* sql.withTransaction(
            rows.createIfAbsent(session).pipe(Effect.andThen(materialize(session))),
          );
        },
        Effect.mapError((cause) => new Sessions.SessionCredentialIssueError({ cause })),
      ),
      revoke: (sessionId) =>
        Effect.flatMap(DateTime.now, (now) =>
          rows.revoke({ sessionId, revokedAt: DateTime.toUtc(now) }),
        ).pipe(
          Effect.as(true),
          Effect.mapError((cause) => new Sessions.SessionRevocationError({ sessionId, cause })),
        ),
    });
  }),
);
const runLive = Effect.fn("PeerSetupCli.live")(
  function* (home: string, input: W.WorkstreamsPeerSetupOwnerRequest) {
    const stateDir = NodePath.join(home, "userdata"),
      dbPath = NodePath.join(stateDir, "statev2.sqlite");
    const text = yield* Effect.try({
      try: () => safeLocalFile(home, NodePath.join(stateDir, "server-runtime.json"), 8192, true),
      catch: unavailable,
    });
    const runtime = yield* decodeRuntimeJson(text).pipe(Effect.mapError(unavailable));
    if (!isProcessAlive(runtime.pid) || runtime.port < 1 || runtime.port > 65535)
      return yield* unavailable();
    const databaseIdentity = yield* Effect.try({
      try: () => {
        // Existing owner identity and selected DB only; no configuration/bootstrap resolver or port allocator.
        if (!safeLocalFile(home, NodePath.join(stateDir, "environment-id"), 1024, true).trim())
          throw unavailable();
        safeLocalFile(home, dbPath, Number.MAX_SAFE_INTEGER, false);
        holdJonesServerNativeWriterFence(dbPath);
        const stat = NodeFS.lstatSync(dbPath);
        return { device: stat.dev, inode: stat.ino, uid: stat.uid, mode: stat.mode };
      },
      catch: unavailable,
    });
    const existing = makeExistingEnrollmentSecretStore({
      baseDir: home,
      secretsDir: NodePath.join(stateDir, "secrets"),
    });
    const key = yield* existing.get("server-signing-key").pipe(Effect.mapError(unavailable));
    if (Option.isNone(key) || key.value.byteLength !== 32) return yield* unavailable();
    const secrets = Layer.succeed(Secrets.ServerSecretStore, {
      ...existing,
      get: (name) =>
        name === "server-signing-key"
          ? Effect.succeedSome(Uint8Array.from(key.value))
          : existing.get(name),
    });
    const sqlite = yield* Effect.try({
      try: () => makeNativeEnrollmentSqliteConfig(dbPath, "apply"),
      catch: unavailable,
    });
    const selected = Layer.effectDiscard(
      Effect.gen(function* () {
        const sql = yield* SqlClient.SqlClient;
        const connection = yield* sql.reserve;
        const databases = yield* connection.execute("PRAGMA database_list", [], undefined);
        const rows = yield* decodeDatabaseList(databases);
        yield* Effect.try({
          try: () => {
            const stat = NodeFS.lstatSync(dbPath);
            if (
              rows.filter((row) => row.name === "main" && row.file === dbPath).length !== 1 ||
              stat.dev !== databaseIdentity.device ||
              stat.ino !== databaseIdentity.inode ||
              stat.uid !== databaseIdentity.uid ||
              stat.mode !== databaseIdentity.mode
            )
              throw unavailable();
            holdJonesServerNativeWriterFence(dbPath);
          },
          catch: unavailable,
        });
      }).pipe(Effect.scoped),
    );
    const sessionLayer = existingCliSessionsLayer.pipe(
      Layer.provideMerge(selected),
      Layer.provideMerge(AuthRows.layer),
      Layer.provideMerge(NodeSqlite.layer(sqlite)),
      Layer.provide(secrets),
    );
    return yield* Effect.flatMap(
      makePeerSetupCliHttpEndpoint(home, "http://127.0.0.1:" + runtime.port),
      (endpoint) => endpoint.run(home, input),
    ).pipe(
      Effect.provide(Layer.mergeAll(sessionLayer, FetchHttpClient.layer)),
      Effect.provideService(FetchHttpClient.RequestInit, { redirect: "error" }),
      Effect.scoped,
    );
  },
  (effect) => effect.pipe(Effect.mapError(unavailable)),
);
/** Explicit commands only. This binding never starts a runtime, creates a key, or migrates SQL. */
export const liveEndpointLayer = Layer.succeed(PeerSetupCliEndpoint, { run: runLive });
