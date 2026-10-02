import * as NodeServices from "@effect/platform-node/NodeServices";
import { assert, describe, it } from "@effect/vitest";
import * as Duration from "effect/Duration";
import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";
import * as FileSystem from "effect/FileSystem";
import * as Option from "effect/Option";
import * as Path from "effect/Path";
import { TestClock } from "effect/testing";
import { HttpClient, HttpClientResponse } from "effect/unstable/http";
import { cliReleaseIndexPageUrl } from "@t3tools/shared/cliRelease";
import {
  HostProcessEnvironment,
  HostProcessInvokedAs,
  HostProcessPlatform,
  HostProcessWorkingDirectory,
} from "@t3tools/shared/hostProcess";

import { repointLauncher, resolveLauncherPath, resolveNewestVersion } from "./update.ts";

it.layer(NodeServices.layer)("t3 update launcher", (it) => {
  it.effect("repoints a symlink that lives in a runtime versions tree", () =>
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem;
      const path = yield* Path.Path;
      const root = yield* fs.makeTempDirectoryScoped({ prefix: "t3-update-" });
      const oldExe = path.join(root, "runtime/versions/1.0.0/t3");
      const newExe = path.join(root, "runtime/versions/2.0.0/t3");
      const launcher = path.join(root, "bin/t3");
      for (const file of [oldExe, newExe]) {
        yield* fs.makeDirectory(path.dirname(file), { recursive: true });
        yield* fs.writeFileString(file, "");
      }
      yield* fs.makeDirectory(path.dirname(launcher), { recursive: true });
      yield* fs.symlink(oldExe, launcher);

      const repointed = yield* repointLauncher({
        launchedAs: launcher,
        versionsDir: path.join(root, "runtime/versions"),
        targetEntryPath: newExe,
      });

      assert.deepStrictEqual(Option.getOrUndefined(repointed), launcher);
      assert.equal(yield* fs.readLink(launcher), newExe);
    }).pipe(Effect.scoped, Effect.provideService(HostProcessPlatform, "linux")),
  );

  it.effect("leaves a plain copy or a foreign symlink alone", () =>
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem;
      const path = yield* Path.Path;
      const root = yield* fs.makeTempDirectoryScoped({ prefix: "t3-update-" });
      const newExe = path.join(root, "runtime/versions/2.0.0/t3");
      const copy = path.join(root, "copy/t3");
      const foreign = path.join(root, "foreign/t3");
      const elsewhere = path.join(root, "elsewhere/t3");
      // Another install's versions tree: same shape, different home.
      const otherHome = path.join(root, "other/runtime/versions/1.0.0/t3");
      const otherLauncher = path.join(root, "other/bin/t3");
      for (const file of [newExe, copy, elsewhere, otherHome]) {
        yield* fs.makeDirectory(path.dirname(file), { recursive: true });
        yield* fs.writeFileString(file, "");
      }
      yield* fs.makeDirectory(path.dirname(foreign), { recursive: true });
      yield* fs.symlink(elsewhere, foreign);
      yield* fs.makeDirectory(path.dirname(otherLauncher), { recursive: true });
      yield* fs.symlink(otherHome, otherLauncher);

      for (const launchedAs of [copy, foreign, otherLauncher, undefined]) {
        const repointed = yield* repointLauncher({
          launchedAs,
          versionsDir: path.join(root, "runtime/versions"),
          targetEntryPath: newExe,
        });
        assert.equal(repointed._tag, "None", launchedAs ?? "undefined");
      }
      assert.equal(yield* fs.readLink(foreign), elsewhere);
      assert.equal(yield* fs.readLink(otherLauncher), otherHome);
    }).pipe(Effect.scoped, Effect.provideService(HostProcessPlatform, "linux")),
  );

  it.effect("finds the launcher a bare command name resolved to on PATH", () =>
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem;
      const path = yield* Path.Path;
      const root = yield* fs.makeTempDirectoryScoped({ prefix: "t3-update-" });
      const launcher = path.join(root, "bin/t3");
      yield* fs.makeDirectory(path.dirname(launcher), { recursive: true });
      yield* fs.writeFileString(launcher, "");

      const bare = yield* resolveLauncherPath.pipe(
        Effect.provideService(HostProcessInvokedAs, "t3"),
        Effect.provideService(HostProcessEnvironment, {
          PATH: `${path.join(root, "missing")}:${path.join(root, "bin")}`,
        }),
        Effect.provideService(HostProcessWorkingDirectory, root),
      );
      const relative = yield* resolveLauncherPath.pipe(
        Effect.provideService(HostProcessInvokedAs, "./bin/t3"),
        Effect.provideService(HostProcessEnvironment, { PATH: "" }),
        Effect.provideService(HostProcessWorkingDirectory, root),
      );
      const absent = yield* resolveLauncherPath.pipe(
        Effect.provideService(HostProcessInvokedAs, "t3"),
        Effect.provideService(HostProcessEnvironment, { PATH: path.join(root, "missing") }),
        Effect.provideService(HostProcessWorkingDirectory, root),
      );

      assert.equal(bare, launcher);
      assert.equal(relative, launcher);
      assert.equal(absent, undefined);
    }).pipe(Effect.scoped, Effect.provideService(HostProcessPlatform, "linux")),
  );
});

describe("t3 update release source", () => {
  it.effect("walks only the configured source pages to find a stable release", () =>
    Effect.gen(function* () {
      const requests: string[] = [];
      const client = HttpClient.make((request) => {
        requests.push(request.url);
        const releases =
          requests.length === 1
            ? [{ tag_name: "v2.0.0-nightly.20261001.1" }, { tag_name: "v2.0.0-preview.20261001.1" }]
            : [{ tag_name: "v1.9.0", draft: true }, { tag_name: "v1.8.0" }];
        return Effect.succeed(
          HttpClientResponse.fromWeb(request, new Response(JSON.stringify(releases))),
        );
      });
      const version = yield* resolveNewestVersion("stable").pipe(
        Effect.provideService(HttpClient.HttpClient, client),
      );
      assert.equal(version, "1.8.0");
      assert.deepStrictEqual(requests, [cliReleaseIndexPageUrl(1), cliReleaseIndexPageUrl(2)]);
    }),
  );

  it.effect.each(["nightly", "preview"] as const)(
    "selects the %s channel from the configured source",
    (channel) =>
      Effect.gen(function* () {
        const requests: string[] = [];
        const client = HttpClient.make((request) => {
          requests.push(request.url);
          return Effect.succeed(
            HttpClientResponse.fromWeb(
              request,
              new Response(
                JSON.stringify([
                  { tag_name: "v2.0.0" },
                  { tag_name: "v2.0.0-nightly.20261001.1" },
                  { tag_name: "v2.0.0-preview.20261001.1" },
                ]),
              ),
            ),
          );
        });
        const version = yield* resolveNewestVersion(channel).pipe(
          Effect.provideService(HttpClient.HttpClient, client),
        );
        assert.equal(version, `2.0.0-${channel}.20261001.1`);
        assert.deepStrictEqual(requests, [cliReleaseIndexPageUrl(1)]);
      }),
  );

  it.effect.each([404, 503])(
    "names the configured source when unavailable (%s), without fallback",
    (status) =>
      Effect.gen(function* () {
        const requests: string[] = [];
        const client = HttpClient.make((request) => {
          requests.push(request.url);
          return Effect.succeed(HttpClientResponse.fromWeb(request, new Response("", { status })));
        });
        const error = yield* resolveNewestVersion("stable").pipe(
          Effect.provideService(HttpClient.HttpClient, client),
          Effect.flip,
        );
        assert.equal(error.reason, `Could not list t3 releases from ${cliReleaseIndexPageUrl(1)}.`);
        assert.deepStrictEqual(requests, [cliReleaseIndexPageUrl(1)]);
      }),
  );

  it.effect("names the configured source on timeout, without fallback", () =>
    Effect.gen(function* () {
      const requests: string[] = [];
      const client = HttpClient.make((request) => {
        requests.push(request.url);
        return Effect.never;
      });
      const lookup = yield* resolveNewestVersion("stable").pipe(
        Effect.provideService(HttpClient.HttpClient, client),
        Effect.flip,
        Effect.forkChild,
      );
      yield* TestClock.adjust(Duration.seconds(30));
      const error = yield* Fiber.join(lookup);
      assert.equal(
        error.reason,
        `Timed out listing t3 releases from ${cliReleaseIndexPageUrl(1)}.`,
      );
      assert.deepStrictEqual(requests, [cliReleaseIndexPageUrl(1)]);
    }),
  );

  it.effect.each(["not json", "{}", '[{"draft":false}]'])(
    "names the configured source for malformed index %s, without fallback",
    (body) =>
      Effect.gen(function* () {
        const requests: string[] = [];
        const client = HttpClient.make((request) => {
          requests.push(request.url);
          return Effect.succeed(HttpClientResponse.fromWeb(request, new Response(body)));
        });
        const error = yield* resolveNewestVersion("stable").pipe(
          Effect.provideService(HttpClient.HttpClient, client),
          Effect.flip,
        );
        assert.equal(
          error.reason,
          `The t3 release index from ${cliReleaseIndexPageUrl(1)} had an unexpected shape.`,
        );
        assert.deepStrictEqual(requests, [cliReleaseIndexPageUrl(1)]);
      }),
  );

  it.effect("names the configured source when the channel is unpublished, without fallback", () =>
    Effect.gen(function* () {
      const requests: string[] = [];
      const client = HttpClient.make((request) => {
        requests.push(request.url);
        return Effect.succeed(HttpClientResponse.fromWeb(request, new Response("[]")));
      });
      const error = yield* resolveNewestVersion("preview").pipe(
        Effect.provideService(HttpClient.HttpClient, client),
        Effect.flip,
      );
      assert.equal(
        error.reason,
        `No published preview release was found in ${cliReleaseIndexPageUrl(1)}.`,
      );
      assert.deepStrictEqual(requests, [cliReleaseIndexPageUrl(1)]);
    }),
  );

  it.effect(
    "bounds pagination to ten configured source pages when the channel is unpublished",
    () =>
      Effect.gen(function* () {
        const requests: string[] = [];
        const client = HttpClient.make((request) => {
          requests.push(request.url);
          return Effect.succeed(
            HttpClientResponse.fromWeb(
              request,
              new Response(JSON.stringify([{ tag_name: "v2.0.0-nightly.20261001.1" }])),
            ),
          );
        });
        const error = yield* resolveNewestVersion("stable").pipe(
          Effect.provideService(HttpClient.HttpClient, client),
          Effect.flip,
        );
        assert.equal(
          error.reason,
          `No published stable release was found in ${cliReleaseIndexPageUrl(1)}.`,
        );
        assert.deepStrictEqual(
          requests,
          Array.from({ length: 10 }, (_, index) => cliReleaseIndexPageUrl(index + 1)),
        );
      }),
  );
});
