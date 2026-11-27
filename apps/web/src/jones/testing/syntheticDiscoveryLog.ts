type ConsoleLog = (...args: unknown[]) => void;

const warningPrefix = /^\[\d{2}:\d{2}:\d{2}\.\d{3}\] WARN \(#\d+\):$/;

/**
 * The browser runner serves the app on loopback without a server descriptor.
 * Report the first failed discovery on each page and fold its identical retries.
 */
export function syntheticDiscoveryLog(log: ConsoleLog, origin: string): ConsoleLog {
  const url = new URL("/.well-known/t3/environment", origin);
  if (url.protocol !== "http:" || !["localhost", "127.0.0.1", "[::1]"].includes(url.hostname)) {
    return log;
  }

  const expectedDetail = `Remote environment endpoint returned an invalid response from ${url.href}.`;
  let reported = false;

  return (...args) => {
    const [prefix, message, context] = args;
    if (
      args.length === 3 &&
      typeof prefix === "string" &&
      warningPrefix.test(prefix) &&
      message === "Could not discover the primary environment." &&
      typeof context === "object" &&
      context !== null &&
      "error" in context &&
      Object.keys(context).length === 1
    ) {
      const { error } = context;
      if (
        typeof error === "object" &&
        error !== null &&
        "_tag" in error &&
        error._tag === "ConnectionTransientError" &&
        "reason" in error &&
        error.reason === "remote-unavailable" &&
        "detail" in error &&
        error.detail === expectedDetail &&
        "hop" in error &&
        error.hop === "server" &&
        "via" in error &&
        error.via === "direct"
      ) {
        if (reported) return;
        reported = true;
      }
    }
    log(...args);
  };
}
