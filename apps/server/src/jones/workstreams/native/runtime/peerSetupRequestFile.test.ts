// @effect-diagnostics nodeBuiltinImport:off -- Filesystem fixtures are synthetic and retained for parent cleanup.
import * as NodeFS from "node:fs";
import * as NodeOS from "node:os";
import * as NodePath from "node:path";
import { expect, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Files from "./peerSetupRequestFile.ts";
import * as Envelope from "../crypto/envelope.ts";
const address = {
  setup_id: "setup:test",
  source_environment_id: "member:test",
  expected_home: {
    registry_instance_id: "11111111-1111-4111-8111-111111111111",
    owner_id: "owner:test",
    home_environment_id: "home:test",
    server_generation: 1,
  },
};
const crypto = Layer.succeed(Envelope.WorkstreamsEnvelope, {
  seal: () => Effect.die("invalid inputs cannot encrypt"),
  open: () => Effect.die("unsafe inputs cannot decrypt"),
});
it.effect("inspection creates nothing and unsafe paths/modes fail before decryption", () =>
  Effect.gen(function* () {
    const home = NodeFS.mkdtempSync(NodePath.join(NodeOS.tmpdir(), "nw-request-file-"));
    const service = yield* Files.PeerSetupRequestFiles.pipe(
      Effect.provide(Files.layer(home).pipe(Layer.provide(crypto))),
    );
    expect(yield* service.read(address)).toBeNull();
    expect(NodeFS.readdirSync(home)).toEqual([]);
    expect(
      (yield* service.read({ ...address, setup_id: "../escape" }).pipe(Effect.result))._tag,
    ).toBe("Failure");
    expect(
      (yield* service.retain('{"receipt":"forged","bearer":"sentinel"}').pipe(Effect.result))._tag,
    ).toBe("Failure");
    NodeFS.mkdirSync(NodePath.join(home, "workstreams"), { mode: 0o700 });
    NodeFS.mkdirSync(NodePath.join(home, "workstreams", "native-peer-setup"), { mode: 0o700 });
    NodeFS.writeFileSync(
      NodePath.join(home, "workstreams", "native-peer-setup", address.setup_id + ".request.enc"),
      "unsafe",
      { mode: 0o644 },
    );
    expect(yield* service.read(address).pipe(Effect.result)).toMatchObject({
      _tag: "Failure",
      failure: { code: "unsafe_file" },
    });
  }),
);
