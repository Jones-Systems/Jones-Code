// @effect-diagnostics nodeBuiltinImport:off -- Target-local read-only service manager metadata; raw argv/definitions are transient and never logged or persisted.
import * as NodeChildProcess from "node:child_process";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import type * as SqlClient from "effect/sql/SqlClient";
import { inside, nativePlatform } from "./Filesystem.ts";
import type { Probe } from "./ConsumerProbe.ts";

export interface ServiceIO {
  uid: () => number | undefined;
  command: (file: string, args: string[], remainingMs: number) => Promise<string>;
}
const nativeIO: ServiceIO = {
  // oxlint-disable-next-line t3code/no-global-process-runtime -- Native service domains belong to the real server UID, never an injected client platform.
  uid: () => process.getuid?.(),
  command: (file, args, remainingMs) =>
    new Promise((resolve, reject) =>
      NodeChildProcess.execFile(
        file,
        args,
        {
          timeout: Math.max(1, Math.min(1500, remainingMs)),
          maxBuffer: 2 * 1024 * 1024,
          encoding: "utf8",
        },
        (error, out) =>
          error ? reject(new Error("native_service_manager_readback_unavailable")) : resolve(out),
      ),
    ),
};
function uses(text: string, target: string) {
  return text.split(/[\s\0"'{};=]+/).some((p) => inside(p, target));
}
function launchdServices(text: string) {
  const start = text.indexOf("services = {");
  if (start < 0) throw new Error("launchd_loaded_services_projection_missing");
  const end = text.indexOf("\n\t}", start);
  if (end < 0) throw new Error("launchd_loaded_services_projection_incomplete");
  return text
    .slice(start + "services = {".length, end)
    .trim()
    .split("\n")
    .filter(Boolean)
    .map((line) => {
      const fields = line.trim().split(/\s+/),
        label = fields.at(-1)!;
      if (fields.length < 3 || !/^[A-Za-z0-9_.-]+$/.test(label))
        throw new Error("launchd_loaded_services_projection_malformed");
      return label;
    });
}
function launchdPaths(text: string) {
  const selected: string[] = [];
  let argumentsBlock = false;
  for (const line of text.split("\n")) {
    const trimmed = line.trim();
    if (/^arguments = \{$/.test(trimmed)) {
      argumentsBlock = true;
      continue;
    }
    if (argumentsBlock) {
      if (trimmed === "}") argumentsBlock = false;
      else selected.push(trimmed);
      continue;
    }
    if (/^(program|working directory|path) = /.test(trimmed))
      selected.push(trimmed.slice(trimmed.indexOf(" = ") + 3));
  }
  // Environment blocks are deliberately excluded. No raw definition survives this call.
  if (argumentsBlock) throw new Error("launchd_service_projection_incomplete");
  return selected.join("\n");
}
export async function nativeServices(
  target: string,
  platform: NodeJS.Platform = nativePlatform,
  io: ServiceIO = nativeIO,
): Promise<Probe> {
  const observedAt = DateTime.toEpochMillis(DateTime.nowUnsafe()),
    started = performance.now(),
    paths = new Set<string>();
  let count = 0;
  const run = (file: string, args: string[]) => {
    const remaining = 5000 - (performance.now() - started);
    if (remaining <= 0) throw new Error("native_service_probe_budget_exceeded");
    return io.command(file, args, remaining);
  };
  try {
    if (platform === "linux") {
      const units = (
        await run("systemctl", [
          "--user",
          "list-units",
          "--all",
          "--type=service",
          "--plain",
          "--no-legend",
          "--no-pager",
        ])
      )
        .trim()
        .split("\n")
        .filter(Boolean)
        .map((line) => line.trim().split(/\s+/)[0]!);
      if (
        units.length > 2000 ||
        units.some((unit) => !unit.endsWith(".service") || !/^[A-Za-z0-9_.@\\:-]+$/.test(unit))
      )
        throw new Error("native_service_unit_bound_or_identity");
      for (const unit of units) {
        // No Environment or arbitrary unit content is requested.
        const projection = await run("systemctl", [
          "--user",
          "show",
          unit,
          "--no-pager",
          "--property=Id,LoadState,WorkingDirectory,RootDirectory,ExecStart,FragmentPath",
        ]);
        if (!projection.includes("LoadState=loaded"))
          throw new Error("native_service_load_state_changed");
        if (uses(projection, target)) paths.add(target);
        count++;
      }
    } else if (platform === "darwin") {
      const uid = io.uid();
      if (uid === undefined) throw new Error("native_service_uid_unavailable");
      for (const domain of [`user/${uid}`, `gui/${uid}`]) {
        const labels = launchdServices(await run("/bin/launchctl", ["print", domain]));
        if (count + labels.length > 2000) throw new Error("native_service_unit_bound_exceeded");
        for (const label of labels) {
          const projection = launchdPaths(
            await run("/bin/launchctl", ["print", `${domain}/${label}`]),
          );
          if (uses(projection, target)) paths.add(target);
          count++;
        }
      }
    } else throw new Error("native_service_manager_unsupported");
    return {
      observedAt,
      status: "complete",
      paths: [...paths],
      limits: [
        "same-user loaded service domains only; other-user/root and remote services excluded",
      ],
    };
  } catch (cause) {
    return {
      observedAt,
      status: count ? "partial" : "unavailable",
      paths: [...paths],
      limits: [
        cause instanceof Error ? cause.message : "native_service_manager_observation_unavailable",
        "Re-observe this target's same-user loaded service definitions through its owning service manager; do not stop or rewrite services.",
      ],
    };
  }
}

export const nativeWorkloads = (sql: SqlClient.SqlClient, worktree: string) =>
  Effect.gen(function* () {
    // Read identifiers/status/bindings only: no prompts, transcript or task payload is loaded.
    const runs = yield* sql<{
      run_id: string;
    }>`SELECT r.run_id FROM orchestration_v2_projection_runs r JOIN orchestration_v2_projection_threads t ON t.thread_id=r.thread_id WHERE json_extract(t.payload_json,'$.worktreePath')=${worktree} AND r.status NOT IN ('completed','failed','interrupted','cancelled','rolled_back') LIMIT 201`;
    const tasks = yield* sql<{
      task_id: string;
    }>`SELECT s.task_id FROM scheduled_tasks s LEFT JOIN orchestration_v2_projection_threads t ON t.thread_id=s.thread_id WHERE (s.enabled=1 OR s.last_run_status='running' OR EXISTS (SELECT 1 FROM jones_scheduled_webhook_pending_runs p WHERE p.task_id=s.task_id)) AND (json_extract(s.workspace_strategy_json,'$.worktreePath')=${worktree} OR json_extract(t.payload_json,'$.worktreePath')=${worktree}) LIMIT 201`;
    const pending = yield* sql<{
      delivery_id: string;
    }>`SELECT delivery_id FROM scheduled_task_webhook_pending WHERE state IN ('pending','dispatching','running') AND json_extract(task_json,'$.workspaceStrategy.type')='existing_worktree' AND json_extract(task_json,'$.workspaceStrategy.worktreePath')=${worktree} LIMIT 201`;
    return {
      observedAt: DateTime.toEpochMillis(DateTime.nowUnsafe()),
      status:
        runs.length > 200 || tasks.length > 200 || pending.length > 200
          ? ("partial" as const)
          : ("complete" as const),
      paths: runs.length || tasks.length || pending.length ? [worktree] : [],
      limits: [
        "Jones-owned bound runs, queued turns, schedules and pending webhook deliveries; native processes and services are observed separately",
      ],
    } satisfies Probe;
  });
