// @effect-diagnostics nodeBuiltinImport:off -- Enrollment reads one preexisting mode 0600 signing key without initializing the secret store.
import * as NodeFSP from "node:fs/promises";
import * as NodeFS from "node:fs";
import * as NodePath from "node:path";
import * as Effect from "effect/Effect";
import * as Option from "effect/Option";
import {
  ServerSecretStore,
  SecretStorePersistError,
  SecretStoreReadError,
  SecretStoreRemoveError,
  SecretStoreRandomGenerationError,
} from "../../auth/ServerSecretStore.ts";

export interface ExistingEnrollmentSecretLocation {
  readonly baseDir: string;
  readonly secretsDir: string;
}
const resource = "native enrollment signing key";
const readError = () =>
  new SecretStoreReadError({ resource, cause: "existing_signing_key_unavailable" });

// ServerSecretStore resolves `${name}.bin`; SessionStore's signing-secret name is server-signing-key.
export const makeExistingEnrollmentSecretStore = (
  location: ExistingEnrollmentSecretLocation,
): ServerSecretStore["Service"] =>
  ServerSecretStore.of({
    get: (name) =>
      Effect.tryPromise({
        try: async () => {
          if (
            name !== "server-signing-key" ||
            !NodePath.isAbsolute(location.baseDir) ||
            !NodePath.isAbsolute(location.secretsDir) ||
            NodePath.resolve(location.baseDir) !== location.baseDir ||
            NodePath.resolve(location.secretsDir) !== location.secretsDir ||
            !location.secretsDir.startsWith(`${location.baseDir}${NodePath.sep}`) ||
            /[\r\n\0]/.test(location.baseDir + location.secretsDir)
          )
            throw readError();
          let current = NodePath.parse(location.secretsDir).root;
          for (const component of location.secretsDir
            .slice(current.length)
            .split(NodePath.sep)
            .filter(Boolean)) {
            current = NodePath.join(current, component);
            const stat = await NodeFSP.lstat(current);
            if (stat.isSymbolicLink() || !stat.isDirectory()) throw readError();
            if (
              (current === location.baseDir ||
                current.startsWith(`${location.baseDir}${NodePath.sep}`)) &&
              (stat.uid !== process.getuid?.() || (stat.mode & 0o022) !== 0)
            )
              throw readError();
          }
          const handle = await NodeFSP.open(
            NodePath.join(location.secretsDir, "server-signing-key.bin"),
            NodeFS.constants.O_RDONLY | NodeFS.constants.O_NOFOLLOW,
          );
          try {
            const stat = await handle.stat();
            if (
              !stat.isFile() ||
              stat.uid !== process.getuid?.() ||
              (stat.mode & 0o7777) !== 0o600 ||
              stat.nlink !== 1 ||
              stat.size !== 32
            )
              throw readError();
            const bytes = await handle.readFile();
            if (bytes.byteLength !== 32) throw readError();
            return Option.some<Uint8Array>(new Uint8Array(bytes));
          } finally {
            await handle.close();
          }
        },
        catch: readError,
      }),
    set: () =>
      Effect.fail(
        new SecretStorePersistError({ resource, cause: "enrollment_secret_store_read_only" }),
      ),
    create: () =>
      Effect.fail(
        new SecretStorePersistError({ resource, cause: "enrollment_secret_store_read_only" }),
      ),
    getOrCreateRandom: () =>
      Effect.fail(
        new SecretStoreRandomGenerationError({
          resource,
          cause: "enrollment_secret_store_read_only",
        }),
      ),
    remove: () =>
      Effect.fail(
        new SecretStoreRemoveError({ resource, cause: "enrollment_secret_store_read_only" }),
      ),
  });
