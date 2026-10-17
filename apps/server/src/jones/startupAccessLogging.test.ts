import { expect, it } from "@effect/vitest";
import { DEFAULT_SIGNAL_EXPORT } from "@t3tools/shared/observability";
import * as OtelEnvironment from "@t3tools/shared/otelEnvironment";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Logger from "effect/Logger";
import * as Path from "effect/Path";
import * as References from "effect/References";

import * as ServerConfig from "../config.ts";
import * as ExternalLauncher from "../process/externalLauncher.ts";
import {
  formatHeadlessStartupOutput,
  logStartupPairingIssued,
  maybeOpenBrowser,
} from "./startupAccessLogging.ts";

const browserConfigLayer = (noBrowser: boolean) =>
  Layer.effect(
    ServerConfig.ServerConfig,
    Effect.gen(function* () {
      const baseDir = "/synthetic-startup-logging";
      return ServerConfig.make({
        ...(yield* ServerConfig.deriveServerPaths(baseDir, undefined)),
        baseDir,
        cwd: baseDir,
        logLevel: "Info",
        traceMinLevel: "Info",
        traceTimingEnabled: false,
        traceBatchWindowMs: 200,
        traceMaxBytes: 1024,
        traceMaxFiles: 1,
        otlpTracesUrl: undefined,
        otlpMetricsUrl: undefined,
        otlpLogsUrl: undefined,
        otlpTracesExport: DEFAULT_SIGNAL_EXPORT,
        otlpMetricsExport: DEFAULT_SIGNAL_EXPORT,
        otlpLogsExport: DEFAULT_SIGNAL_EXPORT,
        otelEnvironment: OtelEnvironment.none,
        staticDir: undefined,
        devAllowedOrigins: [],
        desktopBootstrapToken: undefined,
        logWebSocketEvents: false,
        tailscaleServeEnabled: false,
        tailscaleServePort: 443,
        mode: "web",
        host: "localhost",
        port: 3773,
        devUrl: undefined,
        noBrowser,
        startupPresentation: "browser",
        autoBootstrapProjectFromCwd: false,
      });
    }),
  ).pipe(Layer.provide(Path.layer));

it("prints only the origin when a connection URL contains credential-bearing components", () => {
  const output = formatHeadlessStartupOutput(
    "https://SYNTHETIC_USER:SYNTHETIC_PASSWORD@example.invalid/private/SYNTHETIC_PATH?token=SYNTHETIC_QUERY#token=SYNTHETIC_FRAGMENT",
  );

  expect(output).toContain("Connection string: https://example.invalid");
  expect(output).not.toContain("SYNTHETIC_");
  expect(output).not.toContain("token=");
});

it("does not echo malformed connection input", () => {
  expect(formatHeadlessStartupOutput("SYNTHETIC_INVALID_TOKEN")).toContain(
    "Connection string: unavailable",
  );
  expect(formatHeadlessStartupOutput("SYNTHETIC_INVALID_TOKEN")).not.toContain("SYNTHETIC_");
});

it.effect.each(["success", "failure", "disabled"] as const)(
  "keeps startup logs credential-free when browser launch is %s",
  (outcome) => {
    const target = "http://localhost:3773/pair#token=SYNTHETIC_BROWSER_CREDENTIAL";
    const launched: string[] = [];
    const logs: unknown[] = [];
    const logger = Logger.make(({ fiber, message }) => {
      logs.push({ message, annotations: fiber.getRef(References.CurrentLogAnnotations) });
    });

    return Effect.gen(function* () {
      yield* logStartupPairingIssued;
      yield* maybeOpenBrowser(target);

      expect(launched).toEqual(outcome === "disabled" ? [] : [target]);
      const output = JSON.stringify(logs);
      expect(output).toContain("Startup pairing credential issued");
      expect(output).toContain("t3 pair");
      expect(output).not.toContain("SYNTHETIC_");
      expect(output).not.toContain("#token=");
      expect(output).not.toContain("pairingUrl");
      expect(output.includes("browser auto-open unavailable")).toBe(outcome === "failure");
    }).pipe(
      Effect.provide(
        Layer.mergeAll(
          Logger.layer([logger], { mergeWithExisting: false }),
          browserConfigLayer(outcome === "disabled"),
          Layer.mock(ExternalLauncher.ExternalLauncher)({
            launchBrowser: (url) => {
              launched.push(url);
              return outcome === "failure"
                ? Effect.fail(
                    new ExternalLauncher.ExternalLauncherBrowserSpawnError({
                      target: url,
                      command: "SYNTHETIC_COMMAND",
                      args: [url],
                      cause: new Error(`SYNTHETIC_FAILURE ${url}`),
                    }),
                  )
                : Effect.void;
            },
          }),
        ),
      ),
    );
  },
);
