import type * as Cause from "effect/Cause";
import * as Logger from "effect/Logger";
import * as References from "effect/References";

interface CapturedLog {
  readonly level: string;
  readonly message: ReadonlyArray<unknown>;
  readonly cause: Cause.Cause<unknown>;
  readonly annotations: Readonly<Record<string, unknown>>;
}

/** Collects every log record emitted while `layer` is provided, in emission order. */
export const captureLogs = () => {
  const logs: Array<CapturedLog> = [];
  const logger = Logger.make(({ fiber, message, logLevel, cause }) => {
    logs.push({
      level: logLevel,
      message: Array.isArray(message) ? message : [message],
      cause,
      annotations: fiber.getRef(References.CurrentLogAnnotations),
    });
  });
  return {
    logs,
    layer: Logger.layer([logger], { mergeWithExisting: false }),
    /** Records at one level, for example `"Warn"`. */
    at: (level: string) => logs.filter((entry) => entry.level === level),
  };
};
