// @effect-diagnostics nodeBuiltinImport:off -- Fixed private no-follow files retain encrypted setup bytes before effects.
import * as NodeFS from "node:fs";
import * as NodePath from "node:path";
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Schema from "effect/Schema";
import * as W from "@t3tools/contracts/jones/workstreamsPeerSetup";
import { canonicalWorkstreamsSnapshotJson as canonical } from "@t3tools/contracts/jones/workstreamsSnapshot";
import * as Envelope from "../crypto/envelope.ts";

const decodeIdentity = Schema.decodeUnknownEffect(W.WorkstreamsPeerSetupIdentity);

export class SetupRequestFileError extends Schema.TaggedError<SetupRequestFileError>()(
  "SetupRequestFileError",
  {
    code: Schema.Literals(["unsafe_file", "unavailable", "invalid_request", "conflict"]),
  },
) {
  override get message() {
    return "Peer setup request file refused: " + this.code + ".";
  }
}
export type SetupAttemptPhase = "owner_apply" | "home_prepare" | "home_bind" | "member_apply";
export class PeerSetupRequestFiles extends Context.Service<
  PeerSetupRequestFiles,
  {
    readonly read: (
      address: W.WorkstreamsPeerSetupAddress,
    ) => Effect.Effect<string | null, SetupRequestFileError>;
    readonly retain: (body: string) => Effect.Effect<void, SetupRequestFileError>;
    readonly attempted: (
      identity: W.WorkstreamsPeerSetupIdentity,
      phase: SetupAttemptPhase,
    ) => Effect.Effect<boolean, SetupRequestFileError>;
    readonly begin: (
      identity: W.WorkstreamsPeerSetupIdentity,
      phase: SetupAttemptPhase,
    ) => Effect.Effect<boolean, SetupRequestFileError>;
  }
>()("t3/jones/workstreams/native/runtime/peerSetupRequestFile/PeerSetupRequestFiles") {}
const missing = (e: unknown) =>
  typeof e === "object" && e !== null && "code" in e && e.code === "ENOENT";
const isError = Schema.is(SetupRequestFileError);
const failure = (e: unknown) =>
  isError(e) ? e : new SetupRequestFileError({ code: "unavailable" });
const unsafe = () => {
  throw new SetupRequestFileError({ code: "unsafe_file" });
};
const safeDirectory = (path: string, privateDirectory = false) => {
  const s = NodeFS.lstatSync(path);
  if (
    !s.isDirectory() ||
    s.isSymbolicLink() ||
    s.uid !== process.getuid?.() ||
    (s.mode & (privateDirectory ? 0o077 : 0o022)) !== 0 ||
    NodeFS.realpathSync(path) !== path
  )
    unsafe();
};
const syncDirectory = (directory: string) => {
  const fd = NodeFS.openSync(
    directory,
    NodeFS.constants.O_RDONLY | NodeFS.constants.O_DIRECTORY | NodeFS.constants.O_NOFOLLOW,
  );
  try {
    NodeFS.fsyncSync(fd);
  } finally {
    NodeFS.closeSync(fd);
  }
};
const location = (home: string, setupId: string, create: boolean, phase?: SetupAttemptPhase) => {
  if (
    !NodePath.isAbsolute(home) ||
    NodePath.resolve(home) !== home ||
    !/^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$/.test(setupId)
  )
    unsafe();
  safeDirectory(home);
  let dir = home;
  for (const leaf of ["workstreams", "native-peer-setup"]) {
    dir = NodePath.join(dir, leaf);
    try {
      safeDirectory(dir, leaf === "native-peer-setup");
    } catch (e) {
      if (!missing(e) || !create) throw e;
      try {
        NodeFS.mkdirSync(dir, { mode: 0o700 });
      } catch (race) {
        if (
          !(typeof race === "object" && race !== null && "code" in race && race.code === "EEXIST")
        )
          throw race;
      }
      safeDirectory(dir, leaf === "native-peer-setup");
      syncDirectory(NodePath.dirname(dir));
    }
  }
  if (
    phase !== undefined &&
    !["owner_apply", "home_prepare", "home_bind", "member_apply"].includes(phase)
  )
    unsafe();
  return NodePath.join(
    dir,
    setupId + (phase === undefined ? ".request.enc" : "." + phase + ".attempt.enc"),
  );
};
const readFile = (home: string, id: string, phase?: SetupAttemptPhase) => {
  let fd: number;
  try {
    fd = NodeFS.openSync(
      location(home, id, false, phase),
      NodeFS.constants.O_RDONLY | NodeFS.constants.O_NOFOLLOW,
    );
  } catch (e) {
    if (missing(e)) return null;
    throw e;
  }
  try {
    const s = NodeFS.fstatSync(fd);
    if (
      !s.isFile() ||
      s.uid !== process.getuid?.() ||
      (s.mode & 0o7777) !== 0o600 ||
      s.nlink !== 1 ||
      s.size > 1_100_000
    )
      unsafe();
    const bytes = Buffer.alloc(1_100_001);
    const count = NodeFS.readSync(fd, bytes, 0, bytes.length, 0);
    if (count > 1_100_000) unsafe();
    return new TextDecoder("utf-8", { fatal: true }).decode(bytes.subarray(0, count));
  } finally {
    NodeFS.closeSync(fd);
  }
};
const aad = (r: W.WorkstreamsPeerSetupAddress) => ({
  ...r.expected_home,
  key_scope: "local" as const,
  purpose: "peer_setup_request",
  record_id: r.setup_id,
  revision: 1,
});
export const layer = (home: string) =>
  Layer.effect(
    PeerSetupRequestFiles,
    Effect.gen(function* () {
      const envelope = yield* Envelope.WorkstreamsEnvelope;
      const read = Effect.fn("PeerSetupRequestFiles.read")(function* (
        address: W.WorkstreamsPeerSetupAddress,
      ) {
        const encoded = yield* Effect.try({
          try: () => readFile(home, address.setup_id),
          catch: failure,
        });
        if (encoded === null) return null;
        const bytes = yield* envelope.open(aad(address), encoded).pipe(Effect.mapError(failure));
        const request = yield* Effect.try({
          try: () => W.decodeWorkstreamsPeerSetupRequest(bytes),
          catch: () => new SetupRequestFileError({ code: "invalid_request" }),
        });
        if (
          request.setup_id !== address.setup_id ||
          request.source_environment_id !== address.source_environment_id ||
          canonical(request.expected_home) !== canonical(address.expected_home)
        )
          return yield* new SetupRequestFileError({ code: "conflict" });
        return bytes;
      });
      const retain = Effect.fn("PeerSetupRequestFiles.retain")(
        function* (body: string) {
          const r = yield* Effect.try({
            try: () => W.decodeWorkstreamsPeerSetupRequest(body),
            catch: () => new SetupRequestFileError({ code: "invalid_request" }),
          });
          const before = yield* read(r);
          if (before !== null) {
            if (before !== body) return yield* new SetupRequestFileError({ code: "conflict" });
            return;
          }
          const encoded = yield* envelope.seal(aad(r), body).pipe(Effect.mapError(failure));
          yield* Effect.try({
            try: () => {
              const file = location(home, r.setup_id, true);
              let fd: number;
              try {
                fd = NodeFS.openSync(
                  file,
                  NodeFS.constants.O_WRONLY |
                    NodeFS.constants.O_CREAT |
                    NodeFS.constants.O_EXCL |
                    NodeFS.constants.O_NOFOLLOW,
                  0o600,
                );
              } catch (e) {
                if (typeof e === "object" && e !== null && "code" in e && e.code === "EEXIST")
                  return;
                throw e;
              }
              try {
                NodeFS.writeFileSync(fd, encoded, "utf8");
                NodeFS.fsyncSync(fd);
              } finally {
                NodeFS.closeSync(fd);
              }
              const directory = NodeFS.openSync(
                NodePath.dirname(file),
                NodeFS.constants.O_RDONLY |
                  NodeFS.constants.O_DIRECTORY |
                  NodeFS.constants.O_NOFOLLOW,
              );
              try {
                NodeFS.fsyncSync(directory);
              } finally {
                NodeFS.closeSync(directory);
              }
            },
            catch: failure,
          });
          if ((yield* read(r)) !== body)
            return yield* new SetupRequestFileError({ code: "conflict" });
        },
        (effect) => effect.pipe(Effect.uninterruptible),
      );
      const attempted = Effect.fn("PeerSetupRequestFiles.attempted")(function* (
        id: W.WorkstreamsPeerSetupIdentity,
        phase: SetupAttemptPhase,
      ) {
        const selected = yield* decodeIdentity(id).pipe(
          Effect.mapError(() => new SetupRequestFileError({ code: "invalid_request" })),
        );
        const stored = yield* Effect.try({
          try: () => readFile(home, selected.setup_id, phase),
          catch: failure,
        });
        if (stored === null) return false;
        const bytes = yield* envelope
          .open({ ...aad(selected), purpose: "peer_setup_attempt:" + phase }, stored)
          .pipe(Effect.mapError(failure));
        if (bytes !== canonical(selected))
          return yield* new SetupRequestFileError({ code: "conflict" });
        return true;
      });
      const begin = Effect.fn("PeerSetupRequestFiles.begin")(
        function* (id: W.WorkstreamsPeerSetupIdentity, phase: SetupAttemptPhase) {
          const selected = yield* decodeIdentity(id).pipe(
            Effect.mapError(() => new SetupRequestFileError({ code: "invalid_request" })),
          );
          if (yield* attempted(selected, phase)) return false;
          const encoded = yield* envelope
            .seal({ ...aad(selected), purpose: "peer_setup_attempt:" + phase }, canonical(selected))
            .pipe(Effect.mapError(failure));
          const created = yield* Effect.try({
            try: () => {
              const file = location(home, selected.setup_id, true, phase);
              let fd: number;
              try {
                fd = NodeFS.openSync(
                  file,
                  NodeFS.constants.O_WRONLY |
                    NodeFS.constants.O_CREAT |
                    NodeFS.constants.O_EXCL |
                    NodeFS.constants.O_NOFOLLOW,
                  0o600,
                );
              } catch (e) {
                if (typeof e === "object" && e !== null && "code" in e && e.code === "EEXIST")
                  return false;
                throw e;
              }
              try {
                NodeFS.writeFileSync(fd, encoded, "utf8");
                NodeFS.fsyncSync(fd);
              } finally {
                NodeFS.closeSync(fd);
              }
              const directory = NodeFS.openSync(
                NodePath.dirname(file),
                NodeFS.constants.O_RDONLY |
                  NodeFS.constants.O_DIRECTORY |
                  NodeFS.constants.O_NOFOLLOW,
              );
              try {
                NodeFS.fsyncSync(directory);
              } finally {
                NodeFS.closeSync(directory);
              }
              return true;
            },
            catch: failure,
          });
          if (!(yield* attempted(selected, phase)))
            return yield* new SetupRequestFileError({ code: "unavailable" });
          return created;
        },
        (effect) => effect.pipe(Effect.uninterruptible),
      );
      return PeerSetupRequestFiles.of({ read, retain, attempted, begin });
    }),
  );
