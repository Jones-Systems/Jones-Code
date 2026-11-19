import * as NodeServices from "@effect/platform-node/NodeServices";
import { expect, it } from "@effect/vitest";
import * as HostProcess from "@t3tools/shared/HostProcess";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Path from "effect/Path";
import * as HttpClient from "effect/http/HttpClient";
import * as HttpClientResponse from "effect/http/HttpClientResponse";
import { makeClaudeMaintenanceResolution } from "../../provider/Drivers/ClaudeMaintenance.ts";

const fixture = Effect.fn("AuthFleetClaudeMaintenance.test.fixture")(function* () {
  const fs = yield* FileSystem.FileSystem;
  const path = yield* Path.Path;
  const home = yield* fs.makeTempDirectoryScoped();
  const native = path.join(home, ".local/bin/claude");
  const version = path.join(home, ".local/share/claude/versions/2.1.327");
  const tooling = path.join(home, ".local/share/host-tooling");
  const binding = path.join(tooling, "runtime/auth-fleet-binding.json");
  const config = path.join(home, ".claude");
  const launchers = {
    default: path.join(tooling, "auth-fleet/bin/claude-fleet-default"),
    jace: path.join(tooling, "auth-fleet/bin/claude-fleet-jace"),
  };
  const writeExecutable = Effect.fn("AuthFleetClaudeMaintenance.test.writeExecutable")(function* (
    file: string,
  ) {
    yield* fs.makeDirectory(path.dirname(file), { recursive: true });
    yield* fs.writeFileString(file, "#!/bin/sh\nexit 1\n");
    yield* fs.chmod(file, 0o700);
  });
  yield* writeExecutable(version);
  yield* fs.makeDirectory(path.dirname(native), { recursive: true });
  yield* fs.symlink(version, native);
  yield* writeExecutable(launchers.default);
  yield* writeExecutable(launchers.jace);
  yield* fs.makeDirectory(config);
  yield* fs.writeFileString(path.join(config, "settings.json"), '{"autoUpdatesChannel":"stable"}');
  yield* fs.makeDirectory(path.dirname(binding), { recursive: true });
  const writeBinding = (jaceNative = native) =>
    fs
      .writeFileString(
        binding,
        JSON.stringify({
          schema: "host-tooling.auth-fleet-binding/v1",
          home,
          accounts: [
            { provider: "claude", alias: "default", native_cli: native },
            { provider: "claude", alias: "jace", native_cli: jaceNative },
          ],
        }),
      )
      .pipe(Effect.andThen(fs.chmod(binding, 0o600)));
  yield* writeBinding();

  const requests: string[] = [];
  const http = HttpClient.make((request) => {
    requests.push(request.url);
    return Effect.succeed(HttpClientResponse.fromWeb(request, new Response("2.1.330")));
  });
  const env = { HOME: home, PATH: "" };
  const resolve = (binaryPath: string, platform: "linux" | "darwin" = "linux") =>
    makeClaudeMaintenanceResolution({
      binaryPath,
      env: { ...env, CLAUDE_CONFIG_DIR: path.join(home, "account-config") },
    }).pipe(
      Effect.provideService(HostProcess.Environment, env),
      Effect.provideService(HostProcess.Platform, platform),
      Effect.provideService(HttpClient.HttpClient, http),
    );

  return {
    fs,
    path,
    home,
    native,
    version,
    launchers,
    resolve,
    requests,
    writeBinding,
    writeExecutable,
  };
});

it.layer(NodeServices.layer)("auth-fleet Claude maintenance", (it) => {
  it.effect.each(["darwin", "linux"] as const)(
    "advertises one shared native update for both accounts on %s",
    (platform) =>
      Effect.gen(function* () {
        const f = yield* fixture();
        const defaultResolve = yield* f.resolve(f.launchers.default, platform);
        const jaceResolve = yield* f.resolve(f.launchers.jace, platform);
        const first = yield* defaultResolve();
        const second = yield* jaceResolve();
        expect(first.update).toMatchObject({
          executable: f.native,
          args: ["update"],
          lockKey: "claudeAgent-native",
          env: { HOME: f.home },
        });
        expect(first.update?.env?.CLAUDE_CONFIG_DIR).toBeUndefined();
        expect(second.update).toEqual(first.update);
        expect(yield* first.lookupLatestVersion!).toBe("2.1.330");
        expect(f.requests).toEqual(["https://downloads.claude.ai/claude-code-releases/stable"]);

        yield* f.writeBinding(f.version);
        expect((yield* jaceResolve({ fresh: true })).update).toBeNull();
        expect((yield* defaultResolve({ fresh: true })).update).toEqual(first.update);
      }),
  );

  it.effect("keeps an unrelated wrapper or non-native install manual-only", () =>
    Effect.gen(function* () {
      const f = yield* fixture();
      const wrapper = f.path.join(f.home, "other/claude-fleet-default");
      yield* f.writeExecutable(wrapper);
      const unrelated = yield* f.resolve(wrapper);
      expect((yield* unrelated()).update).toBeNull();

      yield* f.fs.remove(f.native);
      yield* f.writeExecutable(f.native);
      const unverified = yield* f.resolve(f.launchers.default);
      expect((yield* unverified()).update).toBeNull();
    }),
  );
});
