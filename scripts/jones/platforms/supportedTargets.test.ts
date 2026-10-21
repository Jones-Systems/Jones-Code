import * as NodeServices from "@effect/platform-node/NodeServices";
import { describe, expect, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Path from "effect/Path";
import * as Schema from "effect/Schema";
import { fromYaml } from "@t3tools/shared/schemaYaml";
import rootPackageJson from "../../../package.json" with { type: "json" };
import { requireJonesBuildTarget, UnsupportedJonesBuildTargetError } from "./supportedTargets.ts";

const allowedTargets = [
  { platform: "mac", arch: "arm64" },
  { platform: "linux", arch: "arm64" },
  { platform: "linux", arch: "x64" },
] as const;
const rejectedTargets = [
  { platform: "win", arch: "arm64" },
  { platform: "win", arch: "x64" },
  { platform: "mac", arch: "x64" },
  { platform: "mac", arch: "universal" },
] as const;

const Workflow = Schema.Struct({
  jobs: Schema.Record(
    Schema.String,
    Schema.Struct({
      "runs-on": Schema.optional(Schema.Union([Schema.String, Schema.Array(Schema.String)])),
      steps: Schema.optional(Schema.Array(Schema.Struct({ run: Schema.optional(Schema.String) }))),
    }),
  ),
});

describe("Jones packaging targets", () => {
  it.effect.each(allowedTargets)("preserves $platform/$arch options and identity", (target) =>
    Effect.gen(function* () {
      const input = { ...target, outputDir: "synthetic-output", version: "1.2.3" };
      const result = yield* requireJonesBuildTarget(input);
      expect(result).toBe(input);
      expect(result.outputDir).toBe("synthetic-output");
      expect(result.version).toBe("1.2.3");
    }),
  );

  it.effect.each(rejectedTargets)("rejects $platform/$arch before build effects", (target) =>
    Effect.gen(function* () {
      let built = false;
      const error = yield* requireJonesBuildTarget(target).pipe(
        Effect.flatMap(() =>
          Effect.sync(() => {
            built = true;
          }),
        ),
        Effect.flip,
      );
      expect(built).toBe(false);
      expect(error).toBeInstanceOf(UnsupportedJonesBuildTargetError);
      expect(error.platform).toBe(target.platform);
      expect(error.arch).toBe(target.arch);
      expect(error.message).toContain("--platform mac --arch arm64");
      expect(error.message).toContain("--platform linux --arch arm64|x64");
    }),
  );

  it("does not advertise unsupported package shortcuts", () => {
    for (const name of [
      "dist:desktop:dmg:x64",
      "dist:desktop:win",
      "dist:desktop:win:arm64",
      "dist:desktop:win:x64",
    ]) {
      expect(rootPackageJson.scripts).not.toHaveProperty(name);
    }
    expect(rootPackageJson.scripts["dist:desktop:dmg:arm64"]).toContain("--arch arm64");
    expect(rootPackageJson.scripts["dist:desktop:linux"]).toContain("--arch x64");
  });

  it.effect("keeps Windows runners and explicit Intel Mac packaging out of workflows", () =>
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem;
      const path = yield* Path.Path;
      const directory = yield* path.fromFileUrl(
        new URL("../../../.github/workflows/", import.meta.url),
      );
      expect(yield* fs.exists(path.join(directory, "windows-tests.yml"))).toBe(false);
      for (const name of (yield* fs.readDirectory(directory)).filter((name) =>
        /\.ya?ml$/.test(name),
      )) {
        const workflow = yield* Schema.decodeEffect(fromYaml(Workflow))(
          yield* fs.readFileString(path.join(directory, name)),
        );
        for (const job of Object.values(workflow.jobs)) {
          const runners =
            typeof job["runs-on"] === "string" ? [job["runs-on"]] : (job["runs-on"] ?? []);
          expect(
            runners.some((runner) => /windows/i.test(runner)),
            name,
          ).toBe(false);
          for (const step of job.steps ?? []) {
            const run = step.run ?? "";
            expect(run, name).not.toMatch(/dist:desktop:(?:win|dmg:x64)/);
            for (const line of run.split("\n")) {
              if (/--platform(?:=|\s+)mac\b/.test(line)) {
                expect(line, name).not.toMatch(/--arch(?:=|\s+)(?:x64|universal)\b/);
              }
              expect(line, name).not.toMatch(/--platform(?:=|\s+)win\b/);
            }
          }
        }
      }
    }).pipe(Effect.provide(NodeServices.layer)),
  );
});
