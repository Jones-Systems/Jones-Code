import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";
import { ChildProcessSpawner } from "effect/process";

import {
  disableTailscaleServe,
  ensureTailscaleServe,
  readTailscaleServeConfigJson,
} from "../tailscale.ts";

export interface ServeMappingInput {
  readonly servePort: number;
  readonly expectedTarget: string;
}

export type ServeMappingState =
  | { readonly _tag: "absent" }
  | { readonly _tag: "exact"; readonly target: string }
  | {
      readonly _tag: "conflicting";
      readonly reason:
        | "other-target"
        | "extra-handlers"
        | "tcp-forward"
        | "funnel"
        | "foreground"
        | "non-web-tcp";
      readonly observedTarget?: string;
    }
  | {
      readonly _tag: "unknown";
      readonly reason: "status-failed" | "empty-output" | "decode-failed" | "unrecognized-shape";
    };

const isRecord = (value: unknown): value is Record<string, unknown> =>
  typeof value === "object" && value !== null && !Array.isArray(value);
const hasContent = (value: unknown): boolean =>
  value !== undefined &&
  value !== null &&
  value !== false &&
  value !== "" &&
  (typeof value !== "object" || Object.keys(value).length > 0);
const unknownShape = (): ServeMappingState => ({ _tag: "unknown", reason: "unrecognized-shape" });
const decodeServeJson = Schema.decodeUnknownSync(Schema.fromJsonString(Schema.Unknown));
const portMatches = (key: string, port: number): boolean => key.endsWith(`:${String(port)}`);

/** Fixtures follow ipn.ServeConfig; an unfamiliar populated shape must never permit a write. */
export function classifyServeConfig(json: string, input: ServeMappingInput): ServeMappingState {
  if (json.trim().length === 0) return { _tag: "unknown", reason: "empty-output" };
  let config: unknown;
  try {
    config = decodeServeJson(json);
  } catch {
    return { _tag: "unknown", reason: "decode-failed" };
  }
  if (config === null) return { _tag: "absent" };
  if (!isRecord(config)) return unknownShape();
  const knownKeys = new Set(["TCP", "Web", "AllowFunnel", "Foreground", "Services"]);
  if (Object.entries(config).some(([key, value]) => !knownKeys.has(key) && hasContent(value))) {
    return unknownShape();
  }
  // Service-specific Serve configs use another ownership namespace; do not guess at its scope.
  if (hasContent(config.Services)) return unknownShape();
  for (const key of ["TCP", "Web", "AllowFunnel", "Foreground"]) {
    if (config[key] !== undefined && config[key] !== null && !isRecord(config[key]))
      return unknownShape();
  }
  const tcp = isRecord(config.TCP) ? config.TCP : {};
  const web = isRecord(config.Web) ? config.Web : {};
  const funnel = isRecord(config.AllowFunnel) ? config.AllowFunnel : {};
  const foreground = isRecord(config.Foreground) ? config.Foreground : {};
  if (
    Object.entries(funnel).some(
      ([key, value]) => portMatches(key, input.servePort) && hasContent(value),
    )
  ) {
    return { _tag: "conflicting", reason: "funnel" };
  }
  for (const value of Object.values(foreground)) {
    if (!isRecord(value)) return unknownShape();
    const state = classifyServeConfig(JSON.stringify(value), input);
    if (state._tag === "unknown") return state;
    if (state._tag !== "absent") return { _tag: "conflicting", reason: "foreground" };
  }
  const tcpEntry = tcp[String(input.servePort)];
  const webEntries = Object.entries(web).filter(([key]) => portMatches(key, input.servePort));
  if (tcpEntry === undefined && webEntries.length === 0) return { _tag: "absent" };
  if (!isRecord(tcpEntry)) return { _tag: "conflicting", reason: "non-web-tcp" };
  if (hasContent(tcpEntry.TCPForward)) return { _tag: "conflicting", reason: "tcp-forward" };
  if (tcpEntry.HTTPS !== true || Object.keys(tcpEntry).some((key) => key !== "HTTPS")) {
    return { _tag: "conflicting", reason: "non-web-tcp" };
  }
  if (webEntries.length === 0) return { _tag: "conflicting", reason: "non-web-tcp" };
  let otherTarget: string | undefined;
  for (const [, entry] of webEntries) {
    if (!isRecord(entry) || Object.keys(entry).some((key) => key !== "Handlers"))
      return unknownShape();
    const handlers = entry.Handlers;
    if (!isRecord(handlers)) return unknownShape();
    if (Object.keys(handlers).length !== 1 || handlers["/"] === undefined) {
      return { _tag: "conflicting", reason: "extra-handlers" };
    }
    const handler = handlers["/"];
    if (
      !isRecord(handler) ||
      Object.keys(handler).length !== 1 ||
      typeof handler.Proxy !== "string"
    ) {
      return unknownShape();
    }
    if (handler.Proxy !== input.expectedTarget) otherTarget = handler.Proxy;
  }
  return otherTarget !== undefined
    ? { _tag: "conflicting", reason: "other-target", observedTarget: otherTarget }
    : { _tag: "exact", target: input.expectedTarget };
}

export const queryServeMapping = (
  input: ServeMappingInput,
): Effect.Effect<ServeMappingState, never, ChildProcessSpawner.ChildProcessSpawner> =>
  readTailscaleServeConfigJson.pipe(
    Effect.map((json) => classifyServeConfig(json, input)),
    Effect.catch(() =>
      Effect.succeed<ServeMappingState>({ _tag: "unknown", reason: "status-failed" }),
    ),
  );

export function decidePairWrite(
  state: ServeMappingState,
  sameEnvironmentProbe: boolean,
): "write" | "reuse" | { readonly refuse: ServeMappingState } {
  if (state._tag === "absent") return "write";
  if (state._tag === "exact") return "reuse";
  if (state._tag === "conflicting" && state.reason === "other-target" && sameEnvironmentProbe)
    return "write";
  return { refuse: state };
}

const writeTarget = (input: ServeMappingInput) => {
  const target = new URL(input.expectedTarget);
  return ensureTailscaleServe({
    localPort: Number(target.port || "80"),
    localHost: target.hostname,
    servePort: input.servePort,
  });
};

export const acquireServeMapping = (input: ServeMappingInput) =>
  Effect.gen(function* () {
    const state = yield* queryServeMapping(input);
    if (state._tag === "exact") return { created: false } as const;
    if (state._tag !== "absent") return { skipped: state } as const;
    const written = yield* writeTarget(input).pipe(
      Effect.as(true),
      Effect.catch(() => Effect.succeed(false)),
    );
    if (!written) return { skipped: { _tag: "unknown", reason: "status-failed" } } as const;
    const readback = yield* queryServeMapping(input);
    return readback._tag === "exact"
      ? ({ created: true } as const)
      : ({ skipped: { _tag: "unknown", reason: "status-failed" } } as const);
  });

/** The CLI has no CAS: rechecking narrows the race but cannot make removal atomic. */
export const releaseServeMapping = (
  input: ServeMappingInput & { readonly created: boolean },
): Effect.Effect<
  "disabled" | "skipped" | "unknown",
  never,
  ChildProcessSpawner.ChildProcessSpawner
> =>
  Effect.gen(function* () {
    if (!input.created) return "skipped";
    const state = yield* queryServeMapping(input);
    if (state._tag === "unknown") return "unknown";
    if (state._tag !== "exact") return "skipped";
    const disabled = yield* disableTailscaleServe({ servePort: input.servePort }).pipe(
      Effect.as(true),
      Effect.catch(() => Effect.succeed(false)),
    );
    const readback = yield* queryServeMapping(input);
    return disabled && readback._tag === "absent" ? "disabled" : "unknown";
  });
