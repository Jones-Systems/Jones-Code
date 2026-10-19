// @effect-diagnostics nodeBuiltinImport:off - Bounded loopback-only native preparation transport.
import * as NodeHttp from "node:http";
import * as Clock from "effect/Clock";
import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";
import { currentDesktopBootstrapToken } from "@t3tools/shared/desktopBootstrapToken";
import type { JonesActiveInstall } from "./jonesActivation.ts";

const decodeAccessToken = Schema.decodeUnknownSync(Schema.Struct({ access_token: Schema.String }));
const decodePreparationState = Schema.decodeUnknownSync(
  Schema.Struct({
    phase: Schema.optionalKey(Schema.String),
    message: Schema.optionalKey(Schema.String),
  }),
);

/**
 * A backend launched with the desktop secret accepts only the current rotating
 * token. The token frozen into its launch config expires after one or two
 * 12-hour windows, so a long-running app would otherwise be refused here.
 */
export const jonesNativeBootstrapCredential = (bootstrap: {
  readonly desktopBootstrapToken: string;
  readonly desktopBootstrapSecret?: string;
}): Effect.Effect<string> => {
  const secret = bootstrap.desktopBootstrapSecret;
  return secret === undefined
    ? Effect.succeed(bootstrap.desktopBootstrapToken)
    : Effect.map(Clock.currentTimeMillis, (nowMs) => currentDesktopBootstrapToken(secret, nowMs));
};

/** Bounded printable text for update state; the backend's refusal messages are fixed strings. */
export function jonesPreparationText(value: string): string {
  const text = value.replace(/[^\x20-\x7e]/g, " ").trim();
  return text.length > 240 ? `${text.slice(0, 239)}…` : text;
}

function errorCode(cause: unknown): string {
  return typeof cause === "object" &&
    cause !== null &&
    "code" in cause &&
    typeof cause.code === "string" &&
    /^[A-Z0-9_]{1,32}$/.test(cause.code)
    ? cause.code
    : "unknown";
}

function post(
  step: string,
  url: URL,
  contentType: string,
  body: string,
  bearer?: string,
): Promise<unknown> {
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
            request.destroy(Object.assign(new Error(), { code: "RESPONSE_BOUND" }));
        });
        response.once("error", (cause) =>
          reject(new Error(`Native ${step} transport failed (${errorCode(cause)}).`)),
        );
        response.once("end", () => {
          if (response.statusCode !== 200) {
            reject(new Error(`Native ${step} was refused (HTTP ${response.statusCode}).`));
            return;
          }
          try {
            resolve(JSON.parse(raw));
          } catch {
            reject(new Error(`Native ${step} response was invalid.`));
          }
        });
      },
    );
    request.setTimeout(30000, () =>
      request.destroy(Object.assign(new Error(), { code: "TIMEOUT" })),
    );
    request.once("error", (cause) =>
      reject(new Error(`Native ${step} transport failed (${errorCode(cause)}).`)),
    );
    request.end(body);
  });
}

/**
 * The bootstrap token and access token remain in memory and are sent only to the local backend.
 * Returns the backend's state so a missing receipt can be explained by its refusal message.
 */
export async function prepareJonesNativeInstall(input: {
  listener: string;
  bootstrapToken: string;
  stagedHandle: string;
  transactionId: string;
  active: JonesActiveInstall;
}): Promise<{ readonly phase?: string; readonly message?: string }> {
  const form = new URLSearchParams({
    grant_type: "urn:ietf:params:oauth:grant-type:token-exchange",
    subject_token_type: "urn:t3:params:oauth:token-type:environment-bootstrap",
    requested_token_type: "urn:ietf:params:oauth:token-type:access_token",
    subject_token: input.bootstrapToken,
    scope: "orchestration:operate",
  });
  const token = decodeAccessToken(
    await post(
      "preparation authorization",
      new URL("/oauth/token", input.listener),
      "application/x-www-form-urlencoded",
      form.toString(),
    ),
  );
  const state = await post(
    "preparation",
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
  try {
    return decodePreparationState(state);
  } catch {
    return {};
  }
}
