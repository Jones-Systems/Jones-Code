// @effect-diagnostics nodeBuiltinImport:off -- Private publication requires no-follow, mode and no-clobber filesystem operations.
import * as NodeFSP from "node:fs/promises";
import * as NodeFS from "node:fs";
import * as NodePath from "node:path";
import * as NodeCrypto from "node:crypto";
import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";
import { sha256EnrollmentBytes } from "./request.ts";

export interface CredentialLocation {
  readonly baseDir: string;
  readonly credentialPath: string;
}
export interface CredentialFileObservation {
  readonly sha256: string | null;
}
export interface CredentialPublication {
  readonly state: "published" | "unchanged" | "conflict" | "unknown";
  readonly beforeSha256: string | null;
  readonly afterSha256: string | null;
}
export class NativeCredentialFileError extends Schema.TaggedError<NativeCredentialFileError>()(
  "NativeCredentialFileError",
  {
    code: Schema.Literals(["invalid_location", "unsafe_credential", "file_unavailable"]),
  },
) {}
export interface NativeCredentialWriter {
  readonly observe: () => Effect.Effect<CredentialFileObservation, NativeCredentialFileError>;
  readonly publish: (
    token: string,
    expectedBeforeSha256: string | null,
  ) => Effect.Effect<CredentialPublication, NativeCredentialFileError>;
}

const isMissing = (error: unknown) =>
  typeof error === "object" && error !== null && "code" in error && error.code === "ENOENT";
const isExisting = (error: unknown) =>
  typeof error === "object" && error !== null && "code" in error && error.code === "EEXIST";
const fail = (code: NativeCredentialFileError["code"]): never => {
  throw new NativeCredentialFileError({ code });
};
const credentialJson = Schema.fromJsonString(
  Schema.Struct({ authorization_header: Schema.String }),
);
const validateLocation = async (location: CredentialLocation) => {
  const { baseDir, credentialPath } = location;
  if (
    !NodePath.isAbsolute(baseDir) ||
    !NodePath.isAbsolute(credentialPath) ||
    NodePath.resolve(baseDir) !== baseDir ||
    NodePath.resolve(credentialPath) !== credentialPath ||
    !credentialPath.startsWith(`${baseDir}${NodePath.sep}`) ||
    /[\r\n\0]/.test(baseDir + credentialPath)
  )
    return fail("invalid_location");
  let current = NodePath.parse(credentialPath).root;
  const parts = NodePath.dirname(credentialPath)
    .slice(current.length)
    .split(NodePath.sep)
    .filter(Boolean);
  for (const part of parts) {
    current = NodePath.join(current, part);
    const stat = await NodeFSP.lstat(current);
    if (stat.isSymbolicLink() || !stat.isDirectory()) return fail("invalid_location");
    if (
      (current === baseDir || current.startsWith(`${baseDir}${NodePath.sep}`)) &&
      (stat.uid !== process.getuid?.() || (stat.mode & 0o022) !== 0)
    )
      return fail("invalid_location");
  }
};
const readCredential = async (location: CredentialLocation): Promise<CredentialFileObservation> => {
  await validateLocation(location);
  let handle;
  try {
    handle = await NodeFSP.open(
      location.credentialPath,
      NodeFS.constants.O_RDONLY | NodeFS.constants.O_NOFOLLOW,
    );
  } catch (error) {
    if (isMissing(error)) return { sha256: null };
    throw error;
  }
  try {
    const stat = await handle.stat();
    if (
      !stat.isFile() ||
      stat.uid !== process.getuid?.() ||
      (stat.mode & 0o7777) !== 0o600 ||
      stat.nlink !== 1 ||
      stat.size > 4096
    )
      return fail("unsafe_credential");
    const bytes = await handle.readFile();
    if (bytes.byteLength > 4096) return fail("unsafe_credential");
    const parsed = Schema.decodeUnknownSync(Schema.fromJsonString(Schema.Unknown))(
      new TextDecoder("utf-8", { fatal: true }).decode(bytes),
    );
    if (
      typeof parsed !== "object" ||
      parsed === null ||
      Array.isArray(parsed) ||
      Object.keys(parsed).length !== 1 ||
      !("authorization_header" in parsed) ||
      typeof parsed.authorization_header !== "string" ||
      !/^Bearer [A-Za-z0-9._~-]+$/.test(parsed.authorization_header)
    )
      return fail("unsafe_credential");
    return { sha256: sha256EnrollmentBytes(bytes) };
  } finally {
    await handle.close();
  }
};
const mapFileError = (error: unknown) =>
  Schema.is(NativeCredentialFileError)(error)
    ? error
    : new NativeCredentialFileError({ code: "file_unavailable" });

// Existing different bytes are preserved; replacing or restoring them requires a separate exact recovery operation.
export const makeNativeCredentialWriter = (
  location: CredentialLocation,
): NativeCredentialWriter => ({
  observe: () => Effect.tryPromise({ try: () => readCredential(location), catch: mapFileError }),
  publish: (token, expectedBeforeSha256) =>
    Effect.gen(function* () {
      if (
        !/^[A-Za-z0-9._~-]+$/.test(token) ||
        (expectedBeforeSha256 !== null && !/^[a-f0-9]{64}$/.test(expectedBeforeSha256))
      )
        return yield* new NativeCredentialFileError({ code: "unsafe_credential" });
      const content = yield* Schema.encodeEffect(credentialJson)({
        authorization_header: `Bearer ${token}`,
      }).pipe(Effect.mapError(() => new NativeCredentialFileError({ code: "unsafe_credential" })));
      return yield* Effect.tryPromise({
        try: async () => {
          const bytes = Buffer.from(content, "utf8");
          if (bytes.byteLength > 4096) return fail("unsafe_credential");
          const wanted = sha256EnrollmentBytes(bytes);
          const before = await readCredential(location);
          if (before.sha256 === wanted)
            return {
              state: "unchanged" as const,
              beforeSha256: before.sha256,
              afterSha256: before.sha256,
            };
          if (before.sha256 !== expectedBeforeSha256 || before.sha256 !== null)
            return {
              state: "conflict" as const,
              beforeSha256: before.sha256,
              afterSha256: before.sha256,
            };
          const stage = NodePath.join(
            NodePath.dirname(location.credentialPath),
            `.${NodePath.basename(location.credentialPath)}.enrollment-${NodeCrypto.randomUUID()}`,
          );
          let handle;
          let ownsStage = false;
          let linked = false;
          let cleanupFailed = false;
          try {
            handle = await NodeFSP.open(stage, "wx", 0o600);
            ownsStage = true;
            await handle.writeFile(bytes);
            await handle.sync();
            await handle.close();
            handle = undefined;
            await validateLocation(location);
            try {
              await NodeFSP.link(stage, location.credentialPath);
              linked = true;
            } catch (error) {
              if (!isExisting(error)) throw error;
            }
          } catch {
            // A filesystem response can be lost after publication; readback below decides whether exact bytes exist.
          } finally {
            if (handle)
              try {
                await handle.close();
              } catch {
                cleanupFailed = true;
              }
            if (ownsStage)
              try {
                await NodeFSP.unlink(stage);
              } catch (error) {
                if (!isMissing(error)) cleanupFailed = true;
              }
          }
          const after = await readCredential(location).catch(() => null);
          if (cleanupFailed || after === null)
            return {
              state: "unknown" as const,
              beforeSha256: before.sha256,
              afterSha256: after?.sha256 ?? null,
            };
          if (after.sha256 === wanted)
            return {
              state: linked ? ("published" as const) : ("unchanged" as const),
              beforeSha256: before.sha256,
              afterSha256: after.sha256,
            };
          return {
            state: after.sha256 === null ? ("unknown" as const) : ("conflict" as const),
            beforeSha256: before.sha256,
            afterSha256: after.sha256,
          };
        },
        catch: mapFileError,
      });
    }).pipe(Effect.uninterruptible),
});
