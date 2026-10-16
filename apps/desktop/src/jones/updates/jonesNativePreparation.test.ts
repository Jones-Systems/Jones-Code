// @effect-diagnostics nodeBuiltinImport:off -- The fixture owns one loopback listener and closes it after the request.
import * as NodeHttp from "node:http";
import { expect, it } from "vite-plus/test";
import { prepareJonesNativeInstall } from "./jonesNativePreparation.ts";

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
