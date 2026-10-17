// @effect-diagnostics nodeBuiltinImport:off -- Explicit local owner setup uses fixed private paths and the actual Node socket peer.
import * as NodeFS from "node:fs";
import * as NodePath from "node:path";
import * as NodeHttp from "node:http";
import {
  AuthAccessWriteScope,
  EnvironmentHttpBadRequestError,
  WorkstreamsNativeOwnerRequest,
  WorkstreamsNativeOwnerResponse,
} from "@t3tools/contracts";
import type { JonesNativeWriterAdmission } from "@t3tools/shared/jones/nativeWriterFence";
import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";
import * as Stream from "effect/Stream";
import type * as HttpServerRequest from "effect/http/HttpServerRequest";
import * as HttpServerResponse from "effect/http/HttpServerResponse";
import {
  initializeNativeStoreAuthority,
  NativeStoreAuthorityPersistenceError,
} from "../../../environment/nativeStoreAuthorityPersistence.ts";
import { prepareWorkstreamProviderRequest, runWorkstreamProviderCliOperation } from "./cli.ts";
import { makeTrustedNativeEnrollmentDesktopContext } from "./trustedCliContext.ts";

const unavailable = () =>
  new EnvironmentHttpBadRequestError({ message: "desktop_native_owner_unavailable" });
const path = "/api/workstreams/native/owner/v1/operations";
export const assertLocalDesktopOwnerRequest = (
  request: HttpServerRequest.HttpServerRequest,
  principal: { readonly method: string; readonly scopes: ReadonlySet<string> },
): void => {
  const source = request.source;
  if (
    !(source instanceof NodeHttp.IncomingMessage) ||
    !["127.0.0.1", "::1", "::ffff:127.0.0.1"].includes(source.socket.remoteAddress ?? "") ||
    request.originalUrl !== path ||
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
    throw unavailable();
};
const privateDirectory = (directory: string) => {
  try {
    NodeFS.mkdirSync(directory, { mode: 0o700 });
  } catch (error) {
    if (
      !(typeof error === "object" && error !== null && "code" in error && error.code === "EEXIST")
    )
      throw error;
  }
  const stat = NodeFS.lstatSync(directory);
  if (
    !stat.isDirectory() ||
    stat.isSymbolicLink() ||
    stat.uid !== process.getuid?.() ||
    (stat.mode & 0o077) !== 0 ||
    NodeFS.realpathSync(directory) !== directory
  )
    throw unavailable();
};
export interface DesktopNativeEnrollmentOwner extends JonesNativeWriterAdmission {
  readonly refresh: Effect.Effect<void, NativeStoreAuthorityPersistenceError>;
}
export const runDesktopNativeOwnerOperation = Effect.fn("runDesktopNativeOwnerOperation")(
  function* (
    request: WorkstreamsNativeOwnerRequest,
    owner: DesktopNativeEnrollmentOwner,
    location: {
      readonly baseDir: string;
      readonly authorityDir: string;
      readonly environmentId: string;
    },
  ) {
    yield* owner.refresh;
    yield* Effect.try({
      try: () => {
        owner.assertCurrent();
        if (
          location.baseDir !== owner.binding.home ||
          location.authorityDir !== NodePath.join(location.baseDir, "native-store-authority") ||
          location.environmentId !== owner.binding.environmentId
        )
          throw unavailable();
        const fd = NodeFS.openSync(
          NodePath.join(location.baseDir, "userdata", "environment-id"),
          NodeFS.constants.O_RDONLY | NodeFS.constants.O_NOFOLLOW,
        );
        try {
          const stat = NodeFS.fstatSync(fd);
          if (
            !stat.isFile() ||
            stat.uid !== process.getuid?.() ||
            (stat.mode & 0o022) !== 0 ||
            stat.size > 1024
          )
            throw unavailable();
          const bytes = Buffer.alloc(1025);
          const size = NodeFS.readSync(fd, bytes, 0, bytes.length, 0);
          if (
            size > 1024 ||
            new TextDecoder("utf-8", { fatal: true }).decode(bytes.subarray(0, size)).trim() !==
              location.environmentId
          )
            throw unavailable();
        } finally {
          NodeFS.closeSync(fd);
        }
      },
      catch: unavailable,
    });
    if (request.operation === "authority-enroll") {
      return yield* Effect.try({
        try: () => {
          const state = initializeNativeStoreAuthority(
            location.authorityDir,
            location.environmentId,
          );
          owner.assertCurrent();
          if (state.state !== "active" || state.transition_id !== null) throw unavailable();
          return {
            schema: "jones-code.workstreams-native-authority-enrolled/v1" as const,
            environment_id: state.environment_id,
            authority_namespace: state.authority_namespace,
            store_generation: state.store_generation,
          };
        },
        catch: unavailable,
      });
    }
    const directory = NodePath.join(location.baseDir, "workstreams", "native-enrollment");
    yield* Effect.try({
      try: () => {
        privateDirectory(NodePath.dirname(directory));
        privateDirectory(directory);
      },
      catch: unavailable,
    });
    const requestFile = NodePath.join(directory, `${request.enrollmentId}.request.json`);
    const credentialPath = NodePath.join(directory, `${request.enrollmentId}.credential.json`);
    const resolveContext = (
      input: { readonly baseDir: string; readonly authorityDir: string },
      access: "strict_inspection" | "operational_inspection" | "apply" = "strict_inspection",
    ) => makeTrustedNativeEnrollmentDesktopContext(input, access, owner);
    if (request.operation === "provider-prepare")
      return yield* prepareWorkstreamProviderRequest(
        {
          ...location,
          requestFile,
          ownerId: request.ownerId,
          principalId: request.principalId,
          enrollmentId: request.enrollmentId,
          registryOrigin: request.registryOrigin,
          lifetimeDays: request.lifetimeDays,
        },
        resolveContext,
      );
    const common = {
      baseDir: location.baseDir,
      authorityDir: location.authorityDir,
      requestFile,
      credentialPath,
    };
    return yield* runWorkstreamProviderCliOperation(
      request.operation === "provider-plan"
        ? { ...common, operation: "plan", sqliteSidecarEffects: "allow" }
        : request.operation === "provider-apply"
          ? { ...common, operation: "apply", expectedPriorSha256: request.expectedPriorSha256 }
          : {
              ...common,
              operation: "readback",
              expectedCredentialSha256: request.expectedCredentialSha256,
              sqliteSidecarEffects: "allow",
            },
      resolveContext,
    );
  },
);

export const readDesktopNativeOwnerRequest = <E>(
  body: Stream.Stream<Uint8Array, E>,
): Effect.Effect<WorkstreamsNativeOwnerRequest, EnvironmentHttpBadRequestError> =>
  Stream.runFoldEffect(
    body,
    () => ({ bytes: [] as number[], length: 0 }),
    (state, chunk) => {
      if (state.length + chunk.byteLength > 8192) return Effect.fail(unavailable());
      state.bytes.push(...chunk);
      state.length += chunk.byteLength;
      return Effect.succeed(state);
    },
  ).pipe(
    Effect.flatMap((state) =>
      Effect.try({
        try: () => new TextDecoder("utf-8", { fatal: true }).decode(new Uint8Array(state.bytes)),
        catch: unavailable,
      }),
    ),
    Effect.flatMap(
      Schema.decodeUnknownEffect(Schema.fromJsonString(WorkstreamsNativeOwnerRequest)),
    ),
    Effect.mapError(unavailable),
  );
export const renderDesktopNativeOwnerResponse = (response: WorkstreamsNativeOwnerResponse) =>
  HttpServerResponse.text(
    Schema.encodeSync(Schema.fromJsonString(WorkstreamsNativeOwnerResponse))(response),
    {
      headers: { "content-type": "application/json", "cache-control": "no-store" },
    },
  );
