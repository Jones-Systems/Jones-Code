import * as NodeServices from "@effect/platform-node/NodeServices";
import { assert, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Path from "effect/Path";

import { SERVICE_LAUNCHER_PROTOCOL } from "../../cloud/serviceProtocol.ts";
import { classifyJonesManagedRuntime, jonesUninstallRefusal } from "./managedRuntime.ts";

const STABLE = "0.0.44";
const PREVIEW = "0.0.44-preview.20261002.101.2";

it.layer(NodeServices.layer)("Jones managed runtime detection", (it) => {
  it.effect("reads the same evidence the version-only update guard refuses on", () =>
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem;
      const path = yield* Path.Path;
      const root = yield* fs.makeTempDirectoryScoped({ prefix: "t3-jones-managed-" });
      const runtime = path.join(root, "runtime");
      const state = path.join(runtime, "service-state.json");
      const receipt = (version: string) =>
        path.join(runtime, "versions", version, ".jones-runtime-receipt.json");
      const writeState = (activeVersion: string) =>
        fs.writeFileString(
          state,
          `{"protocol":${SERVICE_LAUNCHER_PROTOCOL},"activeVersion":"${activeVersion}"}`,
        );

      // A home with no runtime directory at all is ordinary.
      assert.equal(yield* classifyJonesManagedRuntime(root, STABLE), "unmanaged");
      yield* fs.makeDirectory(path.join(runtime, "versions", STABLE), { recursive: true });
      yield* writeState(STABLE);
      assert.equal(yield* classifyJonesManagedRuntime(root, STABLE), "unmanaged");

      // A preview caller or preview service state is Jones-built.
      assert.equal(yield* classifyJonesManagedRuntime(root, PREVIEW), "managed");
      yield* writeState(PREVIEW);
      assert.equal(yield* classifyJonesManagedRuntime(root, STABLE), "managed");

      // A receipt on the requested version, or on the service's active version.
      yield* writeState(STABLE);
      yield* fs.writeFileString(receipt(STABLE), "{}");
      assert.equal(yield* classifyJonesManagedRuntime(root, STABLE), "managed");
      assert.equal(yield* classifyJonesManagedRuntime(root, "0.0.1"), "managed");
      yield* fs.remove(receipt(STABLE));

      // The admitted-install record alone is enough.
      yield* fs.writeFileString(path.join(runtime, "jones-active-install.json"), "{}");
      assert.equal(yield* classifyJonesManagedRuntime(root, STABLE), "managed");

      // Unreadable state fails closed and outranks every other finding.
      yield* fs.writeFileString(state, "{not json");
      assert.equal(yield* classifyJonesManagedRuntime(root, STABLE), "unreadable-service-state");
      yield* fs.remove(path.join(runtime, "jones-active-install.json"));
      assert.equal(yield* classifyJonesManagedRuntime(root, STABLE), "unreadable-service-state");
    }).pipe(Effect.scoped),
  );

  it.effect("names the host updater, the records at risk and that nothing was removed", () =>
    Effect.gen(function* () {
      const path = yield* Path.Path;
      for (const management of ["managed", "unreadable-service-state"] as const) {
        const message = jonesUninstallRefusal(path, "/home/t3", management);
        assert.include(message, "Jones Code runtime managed by its host updater");
        assert.include(message, "Jones install and recovery records under");
        assert.include(message, path.join("/home/t3", "runtime"));
        assert.include(message, "nothing was removed");
      }
    }),
  );
});
