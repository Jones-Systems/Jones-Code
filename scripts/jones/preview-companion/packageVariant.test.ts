import { describe, expect, it } from "@effect/vitest";
import * as ConfigProvider from "effect/ConfigProvider";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Path from "effect/Path";
import { HostProcessArchitecture, HostProcessPlatform } from "@t3tools/shared/hostProcess";
import { createBuildConfig, resolveBuildOptions } from "../../build-desktop-artifact.ts";
import { companionPackageMetadata } from "./packageVariant.ts";
import { isCompanionPackage } from "@t3tools/shared/jones/previewCompanionProduct";

const layer = Layer.mergeAll(
  Path.layer,
  ConfigProvider.layer(
    ConfigProvider.fromEnv({
      env: {
        GITHUB_REPOSITORY: "synthetic/example",
        T3CODE_DESKTOP_UPDATE_REPOSITORY: "synthetic/example",
      },
    }),
  ),
);

describe("dedicated companion artifact", () => {
  it.effect.each(["mac", "linux", "win"] as const)(
    "isolates %s identity and disables inferred feeds/protocols",
    (platform) =>
      Effect.gen(function* () {
        const config = yield* createBuildConfig(
          platform,
          platform === "mac" ? "dmg" : platform === "linux" ? "AppImage" : "nsis",
          "1.0.0",
          false,
          true,
          3000,
          {
            entitlementsPath: "/synthetic/ordinary.plist",
            provisioningProfilePath: "/synthetic/ordinary.mobileprovision",
          },
          false,
          "arm64",
          "preview-companion",
        ).pipe(Effect.provide(layer));
        expect(config).toMatchObject({
          appId: "com.jonessystems.jonespreviewcompanion",
          productName: "Jones Preview Companion",
          artifactName: "Jones-Preview-Companion-${version}-${arch}.${ext}",
          publish: null,
          protocols: [],
        });
        expect(config[platform]).toMatchObject({ publish: null, protocols: [] });
        if (platform === "mac") {
          expect(config.mac).not.toHaveProperty("provisioningProfile");
          expect(config.mac).not.toHaveProperty("entitlements");
          expect(config.dmg).toMatchObject({ title: "Jones Preview Companion 1.0.0 Installer" });
        }
        if (platform === "linux") {
          expect(config.linux).toMatchObject({
            executableName: "jones-preview-companion",
            desktop: { entry: { StartupWMClass: "jones-preview-companion" } },
          });
          expect(config.deb).toMatchObject({ fpm: [] });
        }
      }),
  );

  it.effect("leaves ordinary desktop metadata and updates intact", () =>
    Effect.gen(function* () {
      const config = yield* createBuildConfig(
        "mac",
        "dmg",
        "1.0.0",
        false,
        false,
        undefined,
        undefined,
      ).pipe(Effect.provide(layer));
      expect(config).toMatchObject({
        appId: "com.t3tools.t3code",
        productName: "Jones Code",
        artifactName: "T3-Code-${version}-${arch}.${ext}",
      });
      expect(config.publish).toEqual([
        expect.objectContaining({ provider: "github", owner: "synthetic", repo: "example" }),
      ]);
      expect(config.mac).toMatchObject({
        protocols: [{ name: "T3 Code", schemes: ["t3code", "t3code-dev"] }],
      });
      expect(companionPackageMetadata()).toEqual({});
    }),
  );

  it.effect("passes the fixed CLI variant to staging and produces a boot-readable marker", () =>
    Effect.gen(function* () {
      const resolved = yield* resolveBuildOptions({
        variant: Option.some("preview-companion"),
        platform: Option.some("mac"),
        target: Option.some("dmg"),
        arch: Option.some("arm64"),
        buildVersion: Option.none(),
        outputDir: Option.none(),
        skipBuild: Option.none(),
        keepStage: Option.none(),
        signed: Option.none(),
        verbose: Option.none(),
        mockUpdates: Option.none(),
        mockUpdateServerPort: Option.none(),
        wslRuntime: Option.none(),
      }).pipe(
        Effect.provide(layer),
        Effect.provideService(HostProcessPlatform, "darwin"),
        Effect.provideService(HostProcessArchitecture, "arm64"),
      );
      expect(resolved.variant).toBe("preview-companion");
      const metadata = companionPackageMetadata(resolved.variant);
      expect(metadata).toMatchObject({
        name: "jones-preview-companion",
        productName: "Jones Preview Companion",
        jonesDesktopProduct: "preview-companion",
      });
      expect(isCompanionPackage(metadata)).toBe(true);
    }),
  );
});
