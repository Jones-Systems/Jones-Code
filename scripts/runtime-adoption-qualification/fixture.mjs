import * as NodeFSP from "node:fs/promises";
import * as NodePath from "node:path";
import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";
import * as SqlClient from "effect/unstable/sql/SqlClient";
import * as NodeServices from "@effect/platform-node/NodeServices";
import * as NodeSqliteClient from "@t3tools/shared/nodeSqliteClient";
import { deriveServerPaths } from "../../apps/server/src/config.ts";
import { makeSqlitePersistenceLive } from "../../apps/server/src/persistence/Layers/Sqlite.ts";
import { runMigrations } from "../../apps/server/src/persistence/Migrations.ts";
import { resolveAttachmentPathById } from "../../apps/server/src/attachmentStore.ts";
import { sha256File } from "./support.mjs";
import { DEFAULT_SERVER_SETTINGS, ServerSettings } from "../../packages/contracts/src/settings.ts";
import { KeybindingsConfig } from "../../packages/contracts/src/keybindings.ts";

const attachmentBytes = Buffer.from(
  "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mP8/x8AAwMCAO+a4WQAAAAASUVORK5CYII=",
  "base64",
);

const tableKeys = {
  projection_projects: "project_id",
  projection_threads: "thread_id",
  projection_thread_messages: "message_id",
  projection_turns: "row_id",
  projection_thread_sessions: "thread_id",
  provider_session_runtime: "thread_id",
};

export function withDatabaseEffect(fixture, { startup = false } = {}, effect) {
  const database = startup
    ? makeSqlitePersistenceLive(fixture.paths.dbPath)
    : NodeSqliteClient.layer({ filename: fixture.paths.dbPath });
  return Effect.scoped(effect.pipe(Effect.provide(database), Effect.provide(NodeServices.layer)));
}

export function withDatabase(fixture, options = {}, effect) {
  return Effect.runPromise(withDatabaseEffect(fixture, options, effect));
}

const insert = (sql, table, row) =>
  Effect.gen(function* () {
    const columns = yield* sql.unsafe(`PRAGMA table_info(${table})`);
    for (const key of Object.keys(row)) {
      if (!columns.some((column) => column.name === key))
        throw new Error(`Unknown fixture column ${table}.${key}`);
    }
    for (const column of columns) {
      if (column.notnull && column.dflt_value === null && !column.pk && !(column.name in row)) {
        throw new Error(`Unpopulated required fixture column ${table}.${column.name}`);
      }
    }
    const keys = Object.keys(row);
    yield* sql.unsafe(
      `INSERT INTO ${table} (${keys.join(",")}) VALUES (${keys.map(() => "?").join(",")})`,
      Object.values(row),
    );
  });

export async function makePopulatedFixture({
  baseDir,
  throughMigration = 53,
  environmentId = "8e933cd0-2a98-4615-a8be-7990056f8801",
}) {
  if (![53, 54].includes(throughMigration))
    throw new Error("Fixture supports historical upstream 53 or 54 only");
  baseDir = NodePath.resolve(baseDir);
  const paths = await Effect.runPromise(
    deriveServerPaths(baseDir, undefined).pipe(Effect.provide(NodeServices.layer)),
  );
  await NodeFSP.mkdir(paths.stateDir, { recursive: true });
  const expected = { environmentId, throughMigration, files: [], identities: [] };
  const fixture = { baseDir, paths, expected };
  const put = async (filename, content) => {
    await NodeFSP.mkdir(NodePath.dirname(filename), { recursive: true });
    await NodeFSP.writeFile(filename, content);
    expected.files.push(filename);
  };
  const settings = Schema.encodeSync(ServerSettings)({
    ...DEFAULT_SERVER_SETTINGS,
    providerInstances: {
      "codex-qualification-instance": {
        driver: "codex",
        displayName: "Synthetic Codex",
        config: {},
      },
      "claudeAgent-qualification-instance": {
        driver: "claudeAgent",
        displayName: "Synthetic Claude",
        config: {},
      },
    },
  });
  const keybindings = Schema.decodeUnknownSync(KeybindingsConfig)([
    { key: "mod+shift+k", command: "thread.stop" },
  ]);
  await put(paths.settingsPath, JSON.stringify(settings));
  await put(paths.keybindingsConfigPath, JSON.stringify(keybindings));
  await put(paths.environmentIdPath, environmentId);
  const now = "2026-09-30T12:00:00.000Z";
  const seeds = [];
  for (const [index, provider] of ["codex", "claudeAgent"].entries()) {
    const suffix = index + 1;
    const projectId = `synthetic-project-${suffix}`;
    const threadId = `canonical-thread-${suffix}`;
    const nativeThreadId = `native-${provider}-thread-${suffix}`;
    const providerInstanceId = `${provider}-qualification-instance`;
    const workspace = NodePath.join(baseDir, "synthetic-projects", String(suffix));
    const worktree = NodePath.join(paths.worktreesDir, `synthetic-${suffix}`);
    const attachmentId = `${threadId}-00000000-0000-4000-8000-00000000000${suffix}-png`;
    await put(NodePath.join(workspace, "fixture.txt"), `synthetic project ${suffix}\n`);
    await put(NodePath.join(worktree, "fixture.txt"), `synthetic worktree ${suffix}\n`);
    await put(NodePath.join(paths.attachmentsDir, `${attachmentId}.png`), attachmentBytes);
    const durableSessionId =
      provider === "claudeAgent" ? "550e8400-e29b-41d4-a716-446655440000" : null;
    const identity = {
      projectId,
      threadId,
      nativeThreadId,
      providerInstanceId,
      durableSessionId,
      attachmentId,
      workspace,
      worktree,
    };
    expected.identities.push(identity);
    seeds.push({ ...identity, provider, suffix });
  }
  await withDatabase(
    fixture,
    {},
    Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;
      yield* runMigrations({ toMigrationInclusive: throughMigration });
      for (const seed of seeds) {
        const {
          projectId,
          threadId,
          nativeThreadId,
          providerInstanceId,
          durableSessionId,
          attachmentId,
          workspace,
          worktree,
          provider,
          suffix,
        } = seed;
        const turnId = `synthetic-turn-${suffix}`;
        const messageId = `synthetic-message-${suffix}`;
        const model = provider === "codex" ? "gpt-5.4" : "claude-sonnet-4-6";
        yield* insert(sql, "projection_projects", {
          project_id: projectId,
          title: `Synthetic project ${suffix}`,
          workspace_root: workspace,
          scripts_json: "[]",
          created_at: now,
          updated_at: now,
        });
        yield* insert(sql, "projection_threads", {
          thread_id: threadId,
          project_id: projectId,
          title: `Synthetic thread ${suffix}`,
          model_selection_json: JSON.stringify({ instanceId: providerInstanceId, model }),
          branch: `synthetic-${suffix}`,
          worktree_path: worktree,
          latest_turn_id: turnId,
          created_at: now,
          updated_at: now,
        });
        yield* insert(sql, "projection_thread_messages", {
          message_id: messageId,
          thread_id: threadId,
          turn_id: turnId,
          role: "user",
          text: `Synthetic conversation ${suffix}`,
          is_streaming: 0,
          created_at: now,
          updated_at: now,
          attachments_json: JSON.stringify([
            {
              type: "image",
              id: attachmentId,
              name: "fixture.png",
              mimeType: "image/png",
              sizeBytes: attachmentBytes.length,
            },
          ]),
        });
        yield* insert(sql, "projection_turns", {
          thread_id: threadId,
          turn_id: turnId,
          pending_message_id: messageId,
          state: "completed",
          requested_at: now,
          started_at: now,
          completed_at: now,
          checkpoint_files_json: "[]",
        });
        yield* insert(sql, "projection_thread_sessions", {
          thread_id: threadId,
          status: "idle",
          provider_name: provider,
          provider_instance_id: providerInstanceId,
          provider_session_id: `native-session-${suffix}`,
          provider_thread_id: nativeThreadId,
          updated_at: now,
        });
        yield* insert(sql, "provider_session_runtime", {
          thread_id: threadId,
          provider_name: provider,
          provider_instance_id: providerInstanceId,
          adapter_key: provider,
          status: "stopped",
          last_seen_at: now,
          resume_cursor_json: JSON.stringify(
            provider === "claudeAgent"
              ? {
                  threadId: nativeThreadId,
                  resume: durableSessionId,
                  resumeSessionAt: "synthetic-stale-assistant",
                  turnCount: 3,
                }
              : { threadId: nativeThreadId },
          ),
          runtime_payload_json: JSON.stringify({ synthetic: true }),
        });
      }
    }),
  );
  expected.files.sort();
  return fixture;
}

async function exists(filename, directory = false) {
  try {
    const info = await NodeFSP.stat(filename);
    return directory ? info.isDirectory() : info.isFile();
  } catch (error) {
    if (error.code === "ENOENT") return false;
    throw error;
  }
}

export async function readFixture(fixture) {
  const database = await withDatabase(
    fixture,
    {},
    Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;
      const tables = {};
      for (const [table, key] of Object.entries(tableKeys)) {
        const rows = yield* sql.unsafe(`SELECT * FROM ${table} ORDER BY ${key}`);
        tables[table] = rows.map((row) => ({ ...row }));
      }
      const upstream =
        yield* sql`SELECT migration_id, name FROM effect_sql_migrations ORDER BY migration_id`;
      const forkExists =
        yield* sql`SELECT name FROM sqlite_master WHERE type = 'table' AND name = 'jones_sql_migrations'`;
      const fork = forkExists.length
        ? yield* sql`SELECT migration_id, name FROM jones_sql_migrations ORDER BY migration_id`
        : [];
      return {
        tables,
        ledgers: {
          upstream: upstream.map((row) => ({ ...row })),
          fork: fork.map((row) => ({ ...row })),
        },
      };
    }),
  );
  const errors = [];
  const files = await Promise.all(
    fixture.expected.files.map(async (filename) => {
      const present = await exists(filename);
      if (!present) errors.push(`Missing fixture file: ${filename}`);
      return { path: filename, sha256: present ? await sha256File(filename) : null };
    }),
  );
  const projects = await Promise.all(
    database.tables.projection_projects.map(async (row) => {
      const present = await exists(row.workspace_root, true);
      if (!present) errors.push(`Broken project path: ${row.project_id}`);
      return { projectId: row.project_id, path: row.workspace_root, exists: present };
    }),
  );
  const worktrees = await Promise.all(
    database.tables.projection_threads.map(async (row) => {
      if (
        !database.tables.projection_projects.some(
          (project) => project.project_id === row.project_id,
        )
      )
        errors.push(`Broken thread project: ${row.thread_id}`);
      const present = row.worktree_path ? await exists(row.worktree_path, true) : true;
      if (!present) errors.push(`Broken worktree path: ${row.thread_id}`);
      return { threadId: row.thread_id, path: row.worktree_path, exists: present };
    }),
  );
  const attachments = [];
  for (const row of database.tables.projection_thread_messages) {
    if (!database.tables.projection_threads.some((thread) => thread.thread_id === row.thread_id))
      errors.push(`Broken message thread: ${row.message_id}`);
    for (const attachment of JSON.parse(row.attachments_json ?? "[]")) {
      const filename = resolveAttachmentPathById({
        attachmentsDir: fixture.paths.attachmentsDir,
        attachmentId: attachment.id,
      });
      if (!filename) errors.push(`Missing attachment: ${attachment.id}`);
      attachments.push({
        messageId: row.message_id,
        id: attachment.id,
        path: filename,
        exists: filename !== null,
      });
    }
  }
  if (
    (await NodeFSP.readFile(fixture.paths.environmentIdPath, "utf8")) !==
    fixture.expected.environmentId
  )
    errors.push("Environment ID changed");
  return {
    ...database,
    files,
    references: { valid: errors.length === 0, errors, projects, worktrees, attachments },
  };
}

export async function snapshotFixture(fixture, destination) {
  destination = NodePath.resolve(destination);
  await NodeFSP.mkdir(NodePath.dirname(destination), { recursive: true });
  try {
    await NodeFSP.access(destination);
    throw new Error("Snapshot destination already exists");
  } catch (error) {
    if (error.code !== "ENOENT") throw error;
  }
  await withDatabase(
    fixture,
    {},
    Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;
      yield* sql.unsafe("VACUUM INTO ?", [destination]);
    }),
  );
  return { path: destination, sha256: await sha256File(destination) };
}
