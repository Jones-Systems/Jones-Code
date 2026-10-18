// @effect-diagnostics nodeBuiltinImport:off - Bounded loopback-only native preparation transport.
import * as NodeHttp from "node:http";
import * as Schema from "effect/Schema";
import type { JonesActiveInstall } from "./jonesActivation.ts";

const decodeAccessToken = Schema.decodeUnknownSync(Schema.Struct({ access_token: Schema.String }));

function post(url: URL, contentType: string, body: string, bearer?: string): Promise<unknown> {
  if (url.protocol !== "http:" || !["127.0.0.1", "[::1]", "localhost"].includes(url.hostname)) {
    throw new Error("Native preparation requires the exact loopback listener.");
  }
  return new Promise((resolve, reject) => {
    const request = NodeHttp.request(
      url,
      {
        method: "POST",
        headers: {
          "content-type": contentType,
          "content-length": Buffer.byteLength(body),
          ...(bearer === undefined ? {} : { authorization: `Bearer ${bearer}` }),
        },
      },
      (response) => {
        let raw = "";
        response.setEncoding("utf8");
        response.on("data", (chunk: string) => {
          raw += chunk;
          if (raw.length > 65536)
            request.destroy(new Error("Native preparation response exceeded its bound."));
        });
        response.once("error", () => reject(new Error("Native preparation transport failed.")));
        response.once("end", () => {
          if (response.statusCode !== 200) {
            reject(new Error("Native preparation was refused."));
            return;
          }
          try {
            resolve(JSON.parse(raw));
          } catch {
            reject(new Error("Native preparation response was invalid."));
          }
        });
      },
    );
    request.setTimeout(30000, () =>
      request.destroy(new Error("Native preparation did not complete.")),
    );
    request.once("error", () => reject(new Error("Native preparation transport failed.")));
    request.end(body);
  });
}

/** The bootstrap token and access token remain in memory and are sent only to the local backend. */
export async function prepareJonesNativeInstall(input: {
  listener: string;
  bootstrapToken: string;
  stagedHandle: string;
  transactionId: string;
  active: JonesActiveInstall;
}): Promise<void> {
  const form = new URLSearchParams({
    grant_type: "urn:ietf:params:oauth:grant-type:token-exchange",
    subject_token_type: "urn:t3:params:oauth:token-type:environment-bootstrap",
    requested_token_type: "urn:ietf:params:oauth:token-type:access_token",
    subject_token: input.bootstrapToken,
    scope: "orchestration:operate",
  });
  const token = decodeAccessToken(
    await post(
      new URL("/oauth/token", input.listener),
      "application/x-www-form-urlencoded",
      form.toString(),
    ),
  );
  await post(
    new URL("/api/jones-updates/prepare-native", input.listener),
    "application/json",
    JSON.stringify({
      stagedHandle: input.stagedHandle,
      transactionId: input.transactionId,
      environmentId: input.active.environmentId,
      currentVersion: input.active.version,
    }),
    token.access_token,
  );
}
