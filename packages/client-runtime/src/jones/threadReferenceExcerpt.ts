import type { ThreadId } from "@t3tools/contracts";
import type { ThreadReferenceExcerpt } from "@t3tools/contracts/jones/threadReferenceExcerpt";
import { ThreadReferenceExcerptResult } from "@t3tools/contracts/jones/threadReferenceExcerptRpc";
import * as Schema from "effect/Schema";
import * as Effect from "effect/Effect";
import * as DateTime from "effect/DateTime";

const decodeResult = Schema.decodeUnknownSync(ThreadReferenceExcerptResult);

function failureReason(
  error: unknown,
): "source_disconnected" | "unsupported_source_server" | "invalid_response" {
  let current = error;
  for (let depth = 0; depth < 5; depth++) {
    if (typeof current === "string")
      return /unknown (rpc )?method|method (not found|not supported)|unknown.*request|unknown.*tag/i.test(
        current,
      )
        ? "unsupported_source_server"
        : "invalid_response";
    if (current === null || typeof current !== "object") break;
    const tag = "_tag" in current ? current._tag : undefined;
    if (tag === "EnvironmentRpcUnavailableError") return "source_disconnected";
    const message =
      "message" in current && typeof current.message === "string" ? current.message : "";
    if (
      /unknown (rpc )?method|method (not found|not supported)|unknown.*request|unknown.*tag/i.test(
        message,
      )
    )
      return "unsupported_source_server";
    current = "cause" in current ? current.cause : undefined;
  }
  return "invalid_response";
}

export function captureThreadReferenceExcerpt(input: {
  readonly threadId: ThreadId;
  readonly request: () => Promise<unknown>;
}): Promise<ThreadReferenceExcerpt> {
  return Effect.runPromise(
    Effect.gen(function* () {
      const capturedAt = DateTime.formatIso(yield* DateTime.now);
      const unavailable = (
        reason: Extract<ThreadReferenceExcerpt, { status: "unavailable" }>["reason"],
      ): ThreadReferenceExcerpt => ({ status: "unavailable", capturedAt, reason });
      return yield* Effect.tryPromise({ try: input.request, catch: failureReason }).pipe(
        Effect.map((value) => {
          try {
            const result = decodeResult(value);
            return result.threadId === input.threadId
              ? result.excerpt
              : unavailable("invalid_response");
          } catch {
            return unavailable("invalid_response");
          }
        }),
        Effect.catch((reason) => Effect.succeed(unavailable(reason))),
        Effect.timeoutOrElse({
          duration: "8 seconds",
          orElse: () => Effect.succeed(unavailable("timeout")),
        }),
      );
    }),
  );
}
