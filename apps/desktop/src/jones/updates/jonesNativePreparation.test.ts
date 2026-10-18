// @effect-diagnostics nodeBuiltinImport:off -- The fixture owns one loopback listener and closes it after the request.
import * as NodeHttp from "node:http";
import { it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as TestClock from "effect/testing/TestClock";
import { expect } from "vite-plus/test";
import {
  DESKTOP_BOOTSTRAP_TOKEN_WINDOW_MS,
  currentDesktopBootstrapToken,
  isValidDesktopBootstrapToken,
} from "@t3tools/shared/desktopBootstrapToken";
import {
  jonesNativeBootstrapCredential,
  jonesPreparationText,
  prepareJonesNativeInstall,
} from "./jonesNativePreparation.ts";

const active = {
  protocol: 1,
  owner: "desktop",
  generation: "previous",
  transactionId: "bootstrap",
  home: "/fixture",
  databasePath: "/fixture/userdata/statev2.sqlite",
  profile: "/fixture/profile",
  environmentId: "fixture-environment",
  appPath: "/fixture/Jones.app",
  executablePath: "/fixture/Jones.app/Jones",
  version: "1.0.0",
  sourceSha: "c".repeat(40),
  sourceTree: "d".repeat(40),
} as const;

async function withBackend(
  respond: (path: string | undefined) => { status: number; body: unknown },
  body: (listener: string, paths: Array<string | undefined>) => Promise<void>,
) {
  const paths: Array<string | undefined> = [];
  const server = NodeHttp.createServer((request, response) => {
    request.resume();
    request.on("end", () => {
      paths.push(request.url);
      const reply = respond(request.url);
      response.statusCode = reply.status;
      response.setHeader("content-type", "application/json");
      response.end(JSON.stringify(reply.body));
    });
  });
  try {
    await new Promise<void>((resolve, reject) => {
      server.once("error", reject);
      server.listen(0, "127.0.0.1", resolve);
    });
    const address = server.address();
    if (address === null || typeof address === "string")
      throw new Error("Missing fixture listener.");
    await body(`http://127.0.0.1:${address.port}`, paths);
  } finally {
    await new Promise<void>((resolve) => {
      server.close(() => resolve());
      server.closeAllConnections();
    });
  }
}

it.effect("derives the rotating credential a long-running backend still accepts", () =>
  Effect.gen(function* () {
    const secret = "fixture-secret";
    const launchedAt = Date.UTC(2026, 9, 17, 15, 37, 39);
    const installAt = Date.UTC(2026, 9, 18, 14, 39, 28);
    const frozen = currentDesktopBootstrapToken(secret, launchedAt);
    expect(installAt - launchedAt).toBeLessThan(2 * DESKTOP_BOOTSTRAP_TOKEN_WINDOW_MS);
    // The token frozen into the launch config has already left the accepted windows.
    expect(isValidDesktopBootstrapToken(secret, frozen, installAt)).toBe(false);
    yield* TestClock.setTime(installAt);
    const credential = yield* jonesNativeBootstrapCredential({
      desktopBootstrapToken: frozen,
      desktopBootstrapSecret: secret,
    });
    expect(isValidDesktopBootstrapToken(secret, credential, installAt)).toBe(true);
    expect(yield* jonesNativeBootstrapCredential({ desktopBootstrapToken: "legacy" })).toBe(
      "legacy",
    );
  }),
);

it("names the refused step without returning response bodies", async () =>
  withBackend(
    () => ({ status: 401, body: { error: "invalid_grant", detail: "private-detail" } }),
    async (listener, paths) => {
      await expect(
        prepareJonesNativeInstall({
          listener,
          bootstrapToken: "fixture-bootstrap",
          stagedHandle: "a".repeat(64),
          transactionId: "b".repeat(64),
          active,
        }),
      ).rejects.toThrow(/^Native preparation authorization was refused \(HTTP 401\)\.$/);
      expect(paths).toEqual(["/oauth/token"]);
    },
  ));

it("returns the backend state that explains a refused preparation", async () =>
  withBackend(
    (path) => ({
      status: 200,
      body:
        path === "/oauth/token"
          ? { access_token: "fixture-access" }
          : { phase: "blocked", message: "Native preparation was refused: fixture." },
    }),
    async (listener) => {
      expect(
        await prepareJonesNativeInstall({
          listener,
          bootstrapToken: "fixture-bootstrap",
          stagedHandle: "a".repeat(64),
          transactionId: "b".repeat(64),
          active,
        }),
      ).toEqual({ phase: "blocked", message: "Native preparation was refused: fixture." });
      expect(jonesPreparationText("line\nbreak\u0000")).toBe("line break");
    },
  ));

it("sends the durable attempt independently of the staged artifact to the native backend", async () => {
  const requests: Array<{
    path: string | undefined;
    body: string;
    authorization: string | undefined;
  }> = [];
  const server = NodeHttp.createServer((request, response) => {
    let body = "";
    request.setEncoding("utf8");
    request.on("data", (chunk: string) => {
      body += chunk;
    });
    request.on("end", () => {
      requests.push({ path: request.url, body, authorization: request.headers.authorization });
      response.setHeader("content-type", "application/json");
      response.end(
        JSON.stringify(request.url === "/oauth/token" ? { access_token: "fixture-access" } : {}),
      );
    });
  });
  try {
    await new Promise<void>((resolve, reject) => {
      server.once("error", reject);
      server.listen(0, "127.0.0.1", resolve);
    });
    const address = server.address();
    if (address === null || typeof address === "string")
      throw new Error("Missing fixture listener.");
    await prepareJonesNativeInstall({
      listener: `http://127.0.0.1:${address.port}`,
      bootstrapToken: "fixture-bootstrap",
      stagedHandle: "a".repeat(64),
      transactionId: "b".repeat(64),
      active: {
        protocol: 1,
        owner: "desktop",
        generation: "previous",
        transactionId: "bootstrap",
        home: "/fixture",
        databasePath: "/fixture/userdata/statev2.sqlite",
        profile: "/fixture/profile",
        environmentId: "fixture-environment",
        appPath: "/fixture/Jones.app",
        executablePath: "/fixture/Jones.app/Jones",
        version: "1.0.0",
        sourceSha: "c".repeat(40),
        sourceTree: "d".repeat(40),
      },
    });
    expect(requests.map((request) => request.path)).toEqual([
      "/oauth/token",
      "/api/jones-updates/prepare-native",
    ]);
    expect(JSON.parse(requests[1]!.body)).toEqual({
      stagedHandle: "a".repeat(64),
      transactionId: "b".repeat(64),
      environmentId: "fixture-environment",
      currentVersion: "1.0.0",
    });
    expect(requests[1]!.authorization).toBe("Bearer fixture-access");
  } finally {
    await new Promise<void>((resolve) => {
      server.close(() => resolve());
      server.closeAllConnections();
    });
  }
});
