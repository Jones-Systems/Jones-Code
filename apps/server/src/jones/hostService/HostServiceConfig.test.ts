import * as NodeServices from "@effect/platform-node/NodeServices";
import { expect, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Path from "effect/Path";

import * as HostServiceConfig from "./HostServiceConfig.ts";

const testLayer = HostServiceConfig.layer.pipe(Layer.provideMerge(NodeServices.layer));

it.layer(testLayer)("Jones host service config", (it) => {
  it.effect("round trips the stable config and produces loopback service environment", () =>
    Effect.gen(function* () {
      const service = yield* HostServiceConfig.HostServiceConfig;
      const fs = yield* FileSystem.FileSystem;
      const path = yield* Path.Path;
      const baseDir = yield* fs.makeTempDirectoryScoped({ prefix: "jones-host-config-" });
      expect(Option.isNone(yield* service.read(baseDir))).toBe(true);
      const config = { schema: 1, port: 4321, tailscaleServePort: 8443 } as const;
      yield* service.write(baseDir, config);
      expect(Option.getOrUndefined(yield* service.read(baseDir))).toEqual(config);
      expect(HostServiceConfig.configEnvironment(config)).toEqual({
        T3CODE_HOST: "127.0.0.1",
        T3CODE_PORT: "4321",
        T3CODE_TAILSCALE_SERVE: "false",
      });
      const configPath = path.join(baseDir, "jones", "host-service.json");
      const first = yield* fs.readFileString(configPath);
      yield* service.write(baseDir, config);
      expect(yield* fs.readFileString(configPath)).toBe(first);
      expect(yield* fs.readDirectory(path.dirname(configPath))).toEqual(["host-service.json"]);
    }),
  );

  it.effect(
    "reads schema 1 without a route claim and rejects malformed versions and TCP ports",
    () =>
      Effect.gen(function* () {
        const service = yield* HostServiceConfig.HostServiceConfig;
        const fs = yield* FileSystem.FileSystem;
        const path = yield* Path.Path;
        const baseDir = yield* fs.makeTempDirectoryScoped({ prefix: "jones-host-config-" });
        yield* service.write(baseDir, { schema: 1, port: 65535 });
        expect(Option.getOrUndefined(yield* service.read(baseDir))).toEqual({
          schema: 1,
          port: 65535,
        });
        const filePath = path.join(baseDir, "jones", "host-service.json");
        for (const text of [
          "{",
          '{"schema":2,"port":4321}',
          '{"schema":1,"port":0}',
          '{"schema":1,"port":65536}',
          '{"schema":1,"port":1.5}',
          '{"schema":1,"port":"4321"}',
          '{"schema":1,"port":4321,"tailscaleServePort":0}',
        ]) {
          yield* fs.writeFileString(filePath, text);
          expect(yield* service.read(baseDir).pipe(Effect.flip)).toMatchObject({
            _tag: "HostServiceConfigError",
            operation: "decode",
          });
          expect(yield* fs.readFileString(filePath)).toBe(text);
        }
      }),
  );

  it.effect("rejects relative bases before any read or write", () =>
    Effect.gen(function* () {
      const service = yield* HostServiceConfig.HostServiceConfig;
      expect(yield* service.read("relative/base").pipe(Effect.flip)).toMatchObject({
        _tag: "HostServiceConfigError",
        operation: "validate-base-dir",
      });
      expect(
        yield* service.write("relative/base", { schema: 1, port: 4321 }).pipe(Effect.flip),
      ).toMatchObject({
        _tag: "HostServiceConfigError",
        operation: "validate-base-dir",
      });
    }),
  );

  it.effect("rejects invalid writes without replacing the last config", () =>
    Effect.gen(function* () {
      const service = yield* HostServiceConfig.HostServiceConfig;
      const fs = yield* FileSystem.FileSystem;
      const baseDir = yield* fs.makeTempDirectoryScoped({ prefix: "jones-host-config-" });
      yield* service.write(baseDir, { schema: 1, port: 4321 });
      expect(yield* service.write(baseDir, { schema: 1, port: 0 }).pipe(Effect.flip)).toMatchObject(
        {
          _tag: "HostServiceConfigError",
          operation: "decode",
        },
      );
      expect(Option.getOrUndefined(yield* service.read(baseDir))).toEqual({
        schema: 1,
        port: 4321,
      });
    }),
  );

  it.effect("cleans destination-local scratch on rename failure and preserves existing bytes", () =>
    Effect.gen(function* () {
      const service = yield* HostServiceConfig.HostServiceConfig;
      const fs = yield* FileSystem.FileSystem;
      const path = yield* Path.Path;
      const baseDir = yield* fs.makeTempDirectoryScoped({ prefix: "jones-host-config-" });
      const configPath = path.join(baseDir, "jones", "host-service.json");
      yield* fs.makeDirectory(configPath, { recursive: true });
      yield* fs.writeFileString(path.join(configPath, "occupied"), "owned-before");
      expect(
        yield* service.write(baseDir, { schema: 1, port: 4321 }).pipe(Effect.flip),
      ).toMatchObject({
        _tag: "HostServiceConfigError",
        operation: "write",
      });
      expect(yield* fs.readDirectory(path.dirname(configPath))).toEqual(["host-service.json"]);
      expect(yield* fs.readFileString(path.join(configPath, "occupied"))).toBe("owned-before");
    }),
  );
});
