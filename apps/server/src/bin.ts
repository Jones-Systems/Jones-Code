/**
 * Every ACP agent spawns `t3 acp-mcp-bridge` while opening its session, and terminal-fallback
 * agents run `t3 acp-mcp-call` per tool call, so their startup sits on first-message latency.
 * They, Codex's managed-token helper, native restore, and update preparation maintenance dispatch
 * before the full CLI module graph; other commands defer to ./binCli.ts.
 */
import { isEntrypoint } from "./entrypoint.ts";
import { enableCliCompileCache } from "./cliCompileCache.ts";

if (
  isEntrypoint({
    moduleUrl: import.meta.url,
    entryPath: process.argv[1],
    runtimeMain: import.meta.main,
  })
) {
  const command = process.argv[2];
  if (command === "__jones-reconcile-preparation") {
    const { runNativeEffectsRecoveryCommand } =
      await import("./jones/updates/preparation/nativeEffectsRecovery.ts");
    await runNativeEffectsRecoveryCommand(process.argv.slice(3));
  } else if (command === "__jones-server-compute") {
    // Closed private protocol: never fall through to CLI/server initialization,
    // including when bootstrap validation or native loading rejects the command.
    const { runServerComputeWorkerCommand } = await import("./jones/serverCompute/worker.ts");
    await runServerComputeWorkerCommand(process.argv.slice(3));
  } else {
    enableCliCompileCache();
    const { ignoreMacosTypeOfServiceEinval } = await import("./outboundHttp.ts");
    ignoreMacosTypeOfServiceEinval();
    if (command === "__document-text") {
      const { extractDocumentText } = await import("./provider/documentTextWorker.ts");
      try {
        process.stdout.write(await extractDocumentText(process.argv[3]!, process.argv[4]!));
      } catch (error) {
        if (error instanceof Error && "code" in error && /^E[A-Z0-9]+$/.test(String(error.code)))
          throw error;
        process.exitCode = 2;
      }
    } else if (command === "__managed-chatgpt-token") {
      const { runManagedChatGptTokenCli } = await import("./cli/managedChatGptToken.ts");
      await runManagedChatGptTokenCli(process.argv.slice(3));
    } else if (command === "acp-mcp-bridge" || command === "acp-mcp-call") {
      const { runAcpMcpCliFastPath } = await import("./mcp/AcpMcpStdioBridge.ts");
      await runAcpMcpCliFastPath(command, process.argv.slice(3));
    } else if (command === "__jones-preparation-worker") {
      const { runPreparationWorkerCommand } = await import("./jones/updates/preparation/worker.ts");
      await runPreparationWorkerCommand(process.argv.slice(3));
    } else if (command === "__jones-native-authority-transition") {
      const { runNativeAuthorityTransition } =
        await import("./jones/workstreams/nativeAuthorityTransition.ts");
      await runNativeAuthorityTransition();
    } else {
      // Windows desktop launches without stdin; attach before reading, except on fast paths using pipes.
      const { attachConsoleStdin } = await import("./cli/consoleStdin.ts");
      attachConsoleStdin();
      const importStart = performance.now();
      const { runCli } = await import("./binCli.ts");
      if (process.env.T3CODE_STARTUP_PROFILE_DIR) {
        performance.measure("server.import.binCli", { start: importStart });
      }
      runCli();
    }
  }
}
