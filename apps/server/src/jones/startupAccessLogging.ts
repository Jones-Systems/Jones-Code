import * as Effect from "effect/Effect";

import * as ServerConfig from "../config.ts";
import * as ExternalLauncher from "../process/externalLauncher.ts";

const pairingIssuedMessage = "Startup pairing credential issued; credential output withheld.";
const pairingHint =
  "Run `t3 pair` on this host (add --base-dir for a non-default home) to print a pairing link.";

// Startup output can be captured even from a terminal. Credentials belong only
// in the browser launch or an explicit pairing command, never startup logs.
export const logStartupPairingIssued = Effect.logInfo(pairingIssuedMessage, {
  hint: pairingHint,
});

export const formatHeadlessStartupOutput = (connectionString: string): string => {
  let origin = "unavailable";
  try {
    origin = new URL(connectionString).origin;
  } catch {
    // Invalid input must not be echoed into a persistent log.
  }
  return [
    "T3 Code server is ready.",
    `Connection string: ${origin}`,
    pairingIssuedMessage,
    pairingHint,
    "",
  ].join("\n");
};

export const maybeOpenBrowser = Effect.fn("startupAccessLogging.maybeOpenBrowser")(function* (
  target: string,
) {
  const serverConfig = yield* ServerConfig.ServerConfig;
  if (serverConfig.noBrowser) {
    return;
  }
  const externalLauncher = yield* ExternalLauncher.ExternalLauncher;

  yield* externalLauncher.launchBrowser(target).pipe(
    // Nested launcher failures retain the credential-bearing target before the catch runs.
    Effect.withTracerEnabled(false),
    Effect.catch(() =>
      Effect.logInfo("browser auto-open unavailable", {
        hint: pairingHint,
      }),
    ),
  );
});
