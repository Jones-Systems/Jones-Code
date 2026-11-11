// @effect-diagnostics nodeBuiltinImport:off -- Opt-in development process and its loopback client; never imported by the ordinary CLI.
import * as NodeFS from "node:fs";
import * as NodePath from "node:path";
import * as NodeURL from "node:url";
import * as NodeRuntime from "@effect/platform-node/NodeRuntime";
import * as NodeServices from "@effect/platform-node/NodeServices";
import * as NodeSocket from "@effect/platform-node/NodeSocket";
import {
  OrchestrationV2ThreadLaunchInput,
  OrchestrationV2Command,
  ORCHESTRATION_PROTOCOL_QUERY_PARAM,
  ORCHESTRATION_PROTOCOL_VERSION,
  ProjectSearchEntriesInput,
  ServerSettingsPatch,
  TerminalAttachInput,
  TerminalCloseInput,
  TerminalWriteInput,
  WsRpcGroup,
} from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Redacted from "effect/Redacted";
import * as Schema from "effect/Schema";
import * as RpcClient from "effect/rpc/RpcClient";
import * as RpcSerialization from "effect/rpc/RpcSerialization";
import * as Socket from "effect/socket/Socket";
import * as OtelEnvironment from "@t3tools/shared/otelEnvironment";
import { DEFAULT_SIGNAL_EXPORT } from "@t3tools/shared/observability";
import * as ServerConfig from "../../src/config.ts";
import { Configuration } from "../../src/jones/workspaceLifecycle/Configuration.ts";
import { resolveServerParallelismProfile } from "../../src/jones/serverParallelism/config.ts";
import { runServer } from "../../src/server.ts";

const Binding = Schema.Struct({
  schema: Schema.Literal("jones.workspace-lifecycle-dev.instance/v1"),
  sourceRoot: Schema.String,
  root: Schema.String,
  home: Schema.String,
  projectRoot: Schema.String,
  environmentId: Schema.String,
  credential: Schema.String,
  workspaceLifecycle: Configuration,
});

const decodeBinding = Schema.decodeUnknownEffect(Schema.fromJsonString(Binding), { onExcessProperty: "error" });
const decodeLaunch = Schema.decodeUnknownEffect(OrchestrationV2ThreadLaunchInput);
const decodeCommand = Schema.decodeUnknownEffect(OrchestrationV2Command);
const decodeAttach = Schema.decodeUnknownEffect(TerminalAttachInput);
const decodeWrite = Schema.decodeUnknownEffect(TerminalWriteInput);
const decodeClose = Schema.decodeUnknownEffect(TerminalCloseInput);
const decodeSettings = Schema.decodeUnknownEffect(ServerSettingsPatch);
const decodeSearch = Schema.decodeUnknownEffect(ProjectSearchEntriesInput);

/** Uses the production workers, adapters, terminal, MCP and settlement services. */
const serve = (raw: unknown) =>
  Effect.gen(function* () {
    const binding = yield* decodeBinding(raw);
    // oxlint-disable-next-line t3code/no-global-process-runtime -- Qualification requires the actual native platform.
    if (process.versions.node !== "26.12.1" || process.platform !== "darwin" || process.arch !== "arm64")
      return yield* Effect.die(new Error("qualified_apple_silicon_node_required"));
    const canonical = (path: string) => NodeFS.realpathSync(path) === path;
    const inside = (parent: string, child: string) => child.startsWith(parent + NodePath.sep);
    if (
      ![binding.root, binding.home, binding.projectRoot, binding.sourceRoot].every(canonical) ||
      process.cwd() !== binding.sourceRoot ||
      process.env.HOME !== binding.home ||
      !inside(binding.root, binding.home) ||
      !inside(binding.root, binding.projectRoot) ||
      binding.workspaceLifecycle.environmentId !== binding.environmentId ||
      binding.workspaceLifecycle.projects.length !== 1 ||
      binding.workspaceLifecycle.projects[0]?.projectRoot !== binding.projectRoot ||
      binding.credential.length < 32
    ) return yield* Effect.die(new Error("isolated_instance_binding_mismatch"));

    const baseDir = NodePath.join(binding.root, "jones-home");
    const paths = yield* ServerConfig.deriveServerPaths(baseDir, undefined, { baseDirIsExplicit: true });
    if (NodeFS.readFileSync(paths.environmentIdPath, "utf8").trim() !== binding.environmentId)
      return yield* Effect.die(new Error("persisted_environment_identity_mismatch"));
    yield* ServerConfig.ensureServerDirectories(paths);
    const config: ServerConfig.ServerConfig["Service"] = ServerConfig.make({
      ...paths,
      baseDir,
      cwd: binding.projectRoot,
      workspaceLifecycle: binding.workspaceLifecycle,
      serverParallelism: resolveServerParallelismProfile("laptop"),
      logLevel: "Error",
      traceMinLevel: "Error",
      traceTimingEnabled: false,
      traceBatchWindowMs: 200,
      traceMaxBytes: 65536,
      traceMaxFiles: 1,
      otlpTracesUrl: undefined,
      otlpMetricsUrl: undefined,
      otlpLogsUrl: undefined,
      otlpTracesExport: DEFAULT_SIGNAL_EXPORT,
      otlpMetricsExport: DEFAULT_SIGNAL_EXPORT,
      otlpLogsExport: DEFAULT_SIGNAL_EXPORT,
      otelEnvironment: OtelEnvironment.none,
      mode: "web",
      port: 0,
      host: "127.0.0.1",
      staticDir: undefined,
      // Reusable development authentication is local to this disposable instance.
      devUrl: new URL("http://127.0.0.1:5173"),
      devAuthToken: Redacted.make(binding.credential),
      devAllowedOrigins: [],
      noBrowser: true,
      startupPresentation: "headless",
      desktopBootstrapToken: undefined,
      shellEnvironmentPrepared: true,
      autoBootstrapProjectFromCwd: false,
      logWebSocketEvents: false,
      tailscaleServeEnabled: false,
      tailscaleServePort: 443,
    });
    // The native source worker resolver hashes bin.ts and its worker closure and
    // dispatches __jones-server-compute there. The proof entry is not a CLI clone.
    process.argv[1] = NodePath.join(binding.sourceRoot, "apps/server/src/bin.ts");
    return yield* runServer.pipe(Effect.provide(ServerConfig.layer(config)));
  }).pipe(Effect.provide(NodeServices.layer));

// Reuse the production RPC schemas and socket codec. No test-only routes or
// in-process service substitutes are added to runServer.
export const developmentRpc = async (
  origin: string,
  credential: string,
  operation: "launch" | "dispatch" | "terminal-attach" | "terminal-write" | "terminal-close" | "settings" | "search",
  input: unknown,
) => {
  const url = new URL("/ws", origin);
  if (url.hostname !== "127.0.0.1" || url.protocol !== "http:")
    throw new Error("loopback_required");
  url.protocol = "ws:";
  url.searchParams.set(ORCHESTRATION_PROTOCOL_QUERY_PARAM, String(ORCHESTRATION_PROTOCOL_VERSION));
  const protocol = RpcClient.layerProtocolSocket().pipe(
    Layer.provide(Socket.layerWebSocket(url.toString(), { openTimeout: "10 seconds" }).pipe(
      Layer.provide(Layer.succeed(Socket.WebSocketConstructor, (address, protocols) =>
        new NodeSocket.NodeWS.WebSocket(address, protocols as string | string[] | undefined, {
          headers: { authorization: `Bearer ${credential}` },
          maxPayload: 1024 * 1024,
        }) as unknown as globalThis.WebSocket,
      )),
    )),
    Layer.provide(RpcSerialization.layerJson),
  );
  return Effect.runPromise(Effect.scoped(Effect.gen(function* () {
    const client = yield* RpcClient.make(WsRpcGroup);
    switch (operation) {
      case "launch":
        return yield* client["orchestration.launchThread"](yield* decodeLaunch(input));
      case "dispatch":
        return yield* client["orchestration.dispatchCommand"](yield* decodeCommand(input));
      case "terminal-attach":
        return yield* client["terminal.attach"](yield* decodeAttach(input));
      case "terminal-write":
        return yield* client["terminal.write"](yield* decodeWrite(input));
      case "terminal-close":
        return yield* client["terminal.close"](yield* decodeClose(input));
      case "settings":
        return yield* client["server.updateSettings"]({ patch: yield* decodeSettings(input) });
      case "search":
        return yield* client["projects.searchEntries"](yield* decodeSearch(input));
    }
  })).pipe(Effect.provide(protocol), Effect.timeout("15 seconds")));
};

if (process.argv[1] === NodeURL.fileURLToPath(import.meta.url)) {
  const path = process.argv[2];
  if (!path || process.argv.length !== 3 || !NodePath.isAbsolute(path))
    throw new Error("Use workspaceLifecycleDev.node-test.mjs --live --operator /absolute/bindings.json");
  const stat = NodeFS.lstatSync(path);
  if (!stat.isFile() || stat.isSymbolicLink() || stat.size > 32768 || (stat.mode & 0o077) !== 0)
    throw new Error("private_bounded_instance_binding_required");
  process.on("message", (message: unknown) => {
    if (message === "inspect-native-workers") {
      const report = process.report?.getReport();
      const workerThreads = typeof report === "object" && report !== null && "workers" in report && Array.isArray(report.workers)
        ? report.workers.length : null;
      process.send?.({ schema: "jones.workspace-lifecycle-dev.workers/v1", pid: process.pid, workerThreads });
    }
  });
  NodeRuntime.runMain(serve(NodeFS.readFileSync(path, "utf8")));
}
