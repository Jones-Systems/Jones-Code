import {
  GitCommandError,
  isRuntimeModeAbove,
  linkOriginIsFenced,
  managedPolicyDeniedMessage,
  managedRuntimeModeDenial,
  type OrchestrationV2LinkOrigin,
  type ProjectId,
  type ProjectTrackedFolder,
  type ProviderInteractionMode,
  type RuntimeMode,
  TASK_FOLDER_DEFAULT_REPOSITORIES_MAX,
  type TaskFolder,
  TaskFolderError,
  type TaskFolderRepository,
  type ThreadId,
  type VcsCreateWorktreeInput,
  type VcsCreateWorktreeResult,
  type VcsRemoveWorktreeInput,
  type WorkspaceLayoutCommandsResult,
  type WorktreeLayoutCommandKind,
  T3_PROJECT_FILE_NAME,
} from "@t3tools/contracts";
import { resolveProjectSettings } from "@t3tools/shared/projectSettings";
import { flattenTemporaryWorktreeBranchName, isTemporaryWorktreeBranch } from "@t3tools/shared/git";
import * as HostProcess from "@t3tools/shared/HostProcess";
import { windowsSystem32Path } from "@t3tools/shared/windowsSystem";
import * as KeyedLock from "@t3tools/shared/KeyedLock";
import { parseT3ProjectFile } from "@t3tools/shared/t3ProjectFile";
import * as Context from "effect/Context";
import * as Digest from "effect/crypto/Digest";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Hex from "effect/encoding/Hex";
import * as FileSystem from "effect/FileSystem";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Path from "effect/Path";
import type * as PlatformError from "effect/PlatformError";
import * as Schema from "effect/Schema";

import * as ServerConfig from "../config.ts";
import * as GitWorkflowService from "../git/GitWorkflowService.ts";
import * as TaskFolderTeardown from "../jones/taskFolderTeardown/Teardown.ts";
import * as ProjectionStore from "../orchestration-v2/ProjectionStore.ts";
import * as ProjectStore from "../orchestration-v2/ProjectStore.ts";
import * as ManagedPolicy from "../policy/ManagedPolicy.ts";
import * as ServerSettings from "../serverSettings.ts";
import * as GitVcsDriver from "../vcs/GitVcsDriver.ts";
import * as VcsProcess from "../vcs/VcsProcess.ts";
import {
  removeEmptyDirectory,
  type RemoveEmptyDirectoryError,
} from "../vcs/removeEmptyDirectory.ts";
import { resolveWorktreesDirectory } from "../worktreesDirectory.ts";
import { isWithinRoot } from "./KnownWorkspaces.ts";
import * as LayoutCommandApprovals from "./LayoutCommandApprovals.ts";
import * as TaskFolderStore from "./TaskFolderStore.ts";
import { projectFolders, parseGitmodulesPaths } from "./WorkspaceFolders.ts";
import { withWorkspaceLease } from "./workspaceLease.ts";

// Each worktree is a checkout: enough at once to finish four repositories together, few enough
// to leave Git's server-wide process slots to running threads.
const REPOSITORY_CONCURRENCY = 4;
const LAYOUT_COMMAND_TIMEOUT_MS = 600_000;
const BRANCH_NAME_ATTEMPTS = 50;

/** What a new task folder will hold, decided before anything is created. */
export interface TaskFolderPlan {
  readonly projectId: ProjectId;
  readonly projectRoot: string;
  readonly realProjectRoot: string;
  readonly projectIsRepository: boolean;
  readonly anchor: string;
  /** The tracked Git folders that get a worktree. */
  readonly repositories: ReadonlyArray<ProjectTrackedFolder>;
  /** Every tracked Git folder. None is linked into a plain project folder: a link is the original. */
  readonly trackedRepositories: ReadonlyArray<string>;
  readonly realTrackedRepositories: ReadonlyMap<string, string>;
}

interface TaskFolderRemoval {
  readonly removed: ReadonlyArray<string>;
  readonly kept: ReadonlyArray<{ readonly path: string; readonly reason: string }>;
}

/** One checkout of a task folder, as removal sees it. */
interface TaskFolderCheckout {
  readonly worktreePath: string;
  /** The checkout this is a worktree of. */
  readonly repositoryPath: string;
}

export class TaskFolders extends Context.Service<
  TaskFolders,
  {
    /**
     * Null when the project tracks no Git folder: its worktree is then a single checkout, made as
     * it always was. Omitted `repositories` selects every tracked Git folder up to
     * `TASK_FOLDER_DEFAULT_REPOSITORIES_MAX`, and none beyond that.
     */
    readonly plan: (input: {
      readonly projectId: ProjectId;
      readonly repositories?: ReadonlyArray<{ readonly path: string }> | undefined;
    }) => Effect.Effect<TaskFolderPlan | null, TaskFolderError>;
    /**
     * Creates the folder and its worktrees on one branch. Failed creations retain any
     * checkouts whose identity or safe removal could not be proved.
     */
    readonly create: (input: {
      readonly plan: TaskFolderPlan;
      readonly branch: string;
      /** Where the project folder's branch starts; unused when it is not a repository. */
      readonly projectStart: { readonly refName: string; readonly baseRefName: string };
      /**
       * The branch the thread starts from. A repository that has it starts there; one that does
       * not starts from its own default branch.
       */
      readonly baseBranch?: string | undefined;
      readonly startFromOrigin: boolean;
      /**
       * Whether the project's own layout command may run for whoever asks. A project that
       * declares one gets no worktree without it: the default layout is not what it asked for.
       */
      readonly projectCommands: ProjectCommandsNow;
      readonly projectProgress?: GitVcsDriver.CreateWorktreeProgress;
      /** As soon as the folder is recorded, so a cancelled caller knows what to remove. */
      readonly onClaimed?: (taskFolder: TaskFolder, branch: string) => Effect.Effect<void>;
      readonly onRepository?: (progress: {
        readonly name: string;
        readonly done: number;
        readonly total: number;
      }) => Effect.Effect<void>;
    }) => Effect.Effect<
      {
        readonly taskFolder: TaskFolder;
        readonly branch: string;
        /** The project's own command made the checkouts, from whatever start it chose. */
        readonly laidOutByProject: boolean;
        /** A repository's worktree starts from the commit just fetched from its remote. */
        readonly repositoriesStartedFromOrigin: boolean;
      },
      TaskFolderError
    >;
    /**
     * The worktree an existing thread asks for in its project folder: a task folder with the
     * default repositories when the project there tracks Git folders. Null otherwise, and for a
     * request that names its path or checks out an existing branch, which stay a single worktree.
     */
    readonly createForProjectFolder: (
      input: VcsCreateWorktreeInput,
      options: { readonly projectCommands: ProjectCommandsNow },
    ) => Effect.Effect<VcsCreateWorktreeResult | null, TaskFolderError>;
    /**
     * Gives a tracked Git folder its worktree in a thread's task folder, on the thread's branch.
     * `startFromOrigin` defaults to the project's setting for new worktrees.
     */
    readonly addRepository: (input: {
      readonly threadId: ThreadId;
      readonly repositoryPath: string;
      readonly startFromOrigin?: boolean | undefined;
    }) => Effect.Effect<TaskFolder, TaskFolderError>;
    /** The task folder that is, or contains, `target`. */
    readonly find: (target: string) => Effect.Effect<TaskFolder | null, TaskFolderError>;
    /**
     * Removes repository worktrees, then the project folder, then the links and the folder.
     * `inspect` may keep a checkout with a reason. Forced removal requires a separate
     * preservation owner. `forget` drops the record, shrinking it to what was kept.
     */
    readonly remove: (input: {
      readonly root: string;
      readonly force: boolean;
      readonly forget: boolean;
      /** Anything but permitted leaves the project's teardown command out. */
      readonly projectCommands: ProjectCommandsNow;
      readonly deleteBranch?: string | undefined;
      readonly ownerThreadId?: ThreadId | undefined;
      readonly inspect?: (
        checkout: TaskFolderCheckout,
      ) => Effect.Effect<{ readonly keep: string | null; readonly force: boolean }>;
    }) => Effect.Effect<TaskFolderRemoval, TaskFolderError>;
    /**
     * Removes the task folder `path` is the root or the project folder of, for good. False when
     * it is neither, and a failure naming what could not be removed.
     */
    readonly removeIfTaskFolder: (input: {
      readonly path: string;
      readonly force: boolean;
      readonly projectCommands: ProjectCommandsNow;
    }) => Effect.Effect<boolean, TaskFolderError>;
    /** Resolves a manual worktree removal to one exact task folder or standalone checkout. */
    readonly removeForProjectFolder: (
      input: VcsRemoveWorktreeInput,
      projectCommands: ProjectCommandsNow,
    ) => Effect.Effect<void, GitCommandError | TaskFolderError>;
    /** Keeps a registered project checkout and the Git store it relies on. */
    readonly registeredPathReason: (
      target: string,
    ) => Effect.Effect<string | null, TaskFolderError>;
    /** Renames the branch in every checkout to one name free in all of them. */
    readonly renameBranch: (input: {
      readonly root: string;
      readonly oldBranch: string;
      readonly newBranch: string;
      readonly exactName?: boolean;
    }) => Effect.Effect<{ readonly branch: string }, TaskFolderError>;
    /**
     * Verifies every checkout before reuse. Missing members require recovery evidence that
     * binds the whole folder to the original operation.
     */
    readonly restore: <E>(input: {
      readonly taskFolder: TaskFolder;
      readonly projectCommands: ProjectCommandsNow;
      /** The thread's branch, for a project that lays its folder out with its own command. */
      readonly branch: string;
      readonly recreate: (checkout: TaskFolderCheckout) => Effect.Effect<void, E>;
    }) => Effect.Effect<void, TaskFolderError | E>;
    readonly assertReady: (input: {
      readonly taskFolder: TaskFolder;
      readonly branch: string;
    }) => Effect.Effect<void, TaskFolderError>;
    /** The project's layout commands as its `t3.json` holds them now, and whether each is approved. */
    readonly layoutCommands: (
      projectId: ProjectId,
    ) => Effect.Effect<WorkspaceLayoutCommandsResult, TaskFolderError>;
  }
>()("t3/workspace/TaskFolders") {}

const isTaskFolderError = Schema.is(TaskFolderError);
const isGitCommandError = Schema.is(GitCommandError);

/**
 * A caller's answer for who asked. Whether a person approved the command's text and whether
 * policy allows it are not the caller's to answer: this service checks both at every run.
 */
export type ProjectCommands = "permitted" | "request-not-permitted" | "agent-below-full-access";

/**
 * The answer as a caller hands it over: settled when it cannot change while the request lasts,
 * or one to ask again each time a command's turn comes, as for a thread whose modes can change.
 */
export type ProjectCommandsNow = ProjectCommands | Effect.Effect<ProjectCommands>;

/**
 * A layout command is the project owner's own code. A link paired below full access grants T3's
 * tools only (see `mcp/linkOrigin.ts`), so work it started never runs one.
 */
export const projectCommandsForLink = (
  origin: OrchestrationV2LinkOrigin | undefined,
): ProjectCommands =>
  origin === undefined || !linkOriginIsFenced(origin) ? "permitted" : "request-not-permitted";

/**
 * For a worktree an agent asked for. Below Full access the agent's own commands wait for a
 * person, so the project's do not run for it either, approved or not. `maxRuntimeMode` is the
 * administrator's limit: a thread recorded above it starts no work in that mode, so it is not in
 * Full access here whatever it records.
 */
export const projectCommandsForAgent = (
  origin: OrchestrationV2LinkOrigin | undefined,
  modes: {
    readonly runtimeMode: RuntimeMode;
    readonly interactionMode: ProviderInteractionMode;
  },
  maxRuntimeMode: RuntimeMode | undefined,
): ProjectCommands => {
  const link = projectCommandsForLink(origin);
  if (link !== "permitted") return link;
  return modes.runtimeMode === "full-access" &&
    !isRuntimeModeAbove(modes.runtimeMode, maxRuntimeMode) &&
    modes.interactionMode === "default"
    ? "permitted"
    : "agent-below-full-access";
};

/** Only `layoutCommand` makes one, so nothing runs that was not checked there. */
const cleared: unique symbol = Symbol("t3/workspace/TaskFolders/cleared");
interface ClearedLayoutCommand {
  readonly [cleared]: true;
  readonly text: string;
  /** The same check again, for the moment the command's turn comes. */
  readonly recheck: Effect.Effect<
    { readonly command: ClearedLayoutCommand } | { readonly refusal: string } | null
  >;
}

const layoutRefusedDetail = (refusal: string) =>
  `This project lays out its worktrees with its own command (worktreeLayout in ${T3_PROJECT_FILE_NAME}), ${refusal}`;

const failureDetail = (cause: unknown) =>
  typeof cause === "object" &&
  cause !== null &&
  "detail" in cause &&
  typeof cause.detail === "string"
    ? cause.detail
    : cause instanceof Error
      ? cause.message
      : String(cause);

/** The deepest folder that holds every one of `folders`, or null when they share none. */
export const commonAncestor = (path: Path.Path, folders: ReadonlyArray<string>) => {
  let ancestor = folders[0]!;
  for (const folder of folders.slice(1)) {
    while (!isWithinRoot(path, folder, ancestor)) {
      const parent = path.dirname(ancestor);
      // Two Windows drives meet nowhere, and a drive root is its own parent.
      if (parent === ancestor) return null;
      ancestor = parent;
    }
  }
  return ancestor;
};

/**
 * The folder a task folder mirrors: the deepest one holding the project and every tracked
 * repository that shares a root with it. The rest are `unreachable` and cannot be laid out.
 */
export const layoutAnchor = (
  path: Path.Path,
  projectRoot: string,
  repositories: ReadonlyArray<string>,
) => {
  let anchor = projectRoot;
  const unreachable: string[] = [];
  for (const repository of repositories) {
    const shared = commonAncestor(path, [anchor, repository]);
    if (shared === null) unreachable.push(repository);
    else anchor = shared;
  }
  return { anchor, unreachable };
};

const make = Effect.gen(function* () {
  const fileSystem = yield* FileSystem.FileSystem;
  const path = yield* Path.Path;
  const digest = yield* Digest.Digest;
  const config = yield* ServerConfig.ServerConfig;
  const settings = yield* ServerSettings.ServerSettingsService;
  const projects = yield* ProjectStore.ProjectStoreV2;
  const git = yield* GitWorkflowService.GitWorkflowService;
  const gitDriver = yield* GitVcsDriver.GitVcsDriver;
  const store = yield* TaskFolderStore.TaskFolderStore;
  const processes = yield* VcsProcess.VcsProcess;
  const hostEnvironment = yield* HostProcess.Environment;
  const hostPlatform = yield* HostProcess.Platform;
  const onWindows = yield* HostProcess.isWindows;
  const projections = yield* ProjectionStore.ProjectionStoreV2;
  const approvals = yield* LayoutCommandApprovals.LayoutCommandApprovals;
  const managedPolicy = yield* ManagedPolicy.ManagedPolicy;
  const layoutCommandsOffBy = ManagedPolicy.deniedBy(managedPolicy, "worktreeLayoutCommands");
  // Set when the administrator's limit leaves no thread in Full access: "Example Corp limits…".
  const fullAccessOff = managedRuntimeModeDenial(managedPolicy.summary, "full-access");

  const exists = (target: string) =>
    fileSystem.exists(target).pipe(Effect.orElseSucceed(() => false));
  const lstatIfPresent = (target: string) =>
    fileSystem
      .lstat(target)
      .pipe(Effect.catchReason("PlatformError", "NotFound", () => Effect.succeed(null)));
  const canonicalLocation = (target: string): Effect.Effect<string, PlatformError.PlatformError> =>
    fileSystem.realPath(target).pipe(
      Effect.catchReason("PlatformError", "NotFound", (_reason, cause) =>
        Effect.gen(function* () {
          const parent = path.dirname(target);
          // A dangling link is not a missing path and cannot establish identity.
          if (parent === target || (yield* lstatIfPresent(target)) !== null) return yield* cause;
          return path.join(yield* canonicalLocation(parent), path.basename(target));
        }),
      ),
    );
  const samePath = (left: string, right: string) => path.relative(left, right) === "";
  const isRepository = (folder: string) => exists(path.join(folder, ".git"));
  const occupied = Effect.fnUntraced(function* (target: string) {
    if (!(yield* exists(target))) return false;
    const isDirectory = (yield* fileSystem.stat(target).pipe(Effect.option)).pipe(
      Option.exists((info) => info.type === "Directory"),
    );
    if (!isDirectory) return true;
    return (
      (yield* fileSystem.readDirectory(target).pipe(Effect.orElseSucceed(() => [""]))).length > 0
    );
  });
  const trackedFolders = (project: {
    readonly workspaceRoot: string;
    readonly trackedFolders: ReadonlyArray<ProjectTrackedFolder>;
  }) =>
    projectFolders(project).pipe(
      Effect.provideService(FileSystem.FileSystem, fileSystem),
      Effect.provideService(Path.Path, path),
    );
  // The repository that pins a submodule owns its checkout: Git, a hook or a setup script puts it
  // in that repository's worktree, so it never gets one of its own here.
  const isSubmodule = Effect.fnUntraced(function* (folder: string) {
    for (
      let owner = path.dirname(folder);
      owner !== path.dirname(owner);
      owner = path.dirname(owner)
    ) {
      if (!(yield* isRepository(owner))) continue;
      const declared = yield* fileSystem
        .readFileString(path.join(owner, ".gitmodules"))
        .pipe(Effect.orElseSucceed(() => ""));
      return parseGitmodulesPaths(declared).includes(
        path.relative(owner, folder).split(path.sep).join("/"),
      );
    }
    return false;
  });
  const fail =
    (operation: TaskFolderError["operation"], prefix: string) =>
    (cause: unknown): TaskFolderError =>
      new TaskFolderError({ operation, detail: `${prefix}: ${failureDetail(cause)}`, cause });

  // A project that lays its own thread folder out says so in its `t3.json`.
  const projectFile = (projectRoot: string) =>
    fileSystem.readFileString(path.join(projectRoot, T3_PROJECT_FILE_NAME)).pipe(
      Effect.map(parseT3ProjectFile),
      Effect.orElseSucceed(() => null),
    );
  const worktreeLayout = (projectRoot: string) =>
    projectFile(projectRoot).pipe(Effect.map((config) => config?.worktreeLayout ?? null));
  /**
   * The project's command for `kind` as its file holds it now: cleared to run, or the reason it
   * stays unrun, worded to end a sentence about the command. Null when the project declares none.
   * The text checked is the text run, so a file that changes in between changes nothing.
   * Untraced: every caller is a traced operation, and its span covers these reads.
   */
  const layoutCommand: (
    project: {
      readonly projectId: ProjectId;
      readonly projectRoot: string;
      readonly realProjectRoot?: string;
    },
    kind: WorktreeLayoutCommandKind,
    projectCommands: ProjectCommandsNow,
  ) => ClearedLayoutCommand["recheck"] = Effect.fnUntraced(
    function* (project, kind, projectCommands) {
      const text = (yield* worktreeLayout(project.projectRoot))?.[kind];
      if (text === undefined) return null;
      const refuse = (refusal: string) => ({ refusal }) as const;
      const registered = yield* projects
        .get(project.projectId)
        .pipe(Effect.orElseSucceed(Option.none));
      const realProjectRoot = yield* fileSystem.realPath(project.projectRoot).pipe(Effect.option);
      if (
        Option.isNone(registered) ||
        !samePath(registered.value.workspaceRoot, project.projectRoot) ||
        Option.isNone(realProjectRoot) ||
        (project.realProjectRoot !== undefined &&
          !samePath(realProjectRoot.value, project.realProjectRoot))
      )
        return refuse("whose registered project folder could not be verified.");
      const asked = typeof projectCommands === "string" ? projectCommands : yield* projectCommands;
      if (asked === "request-not-permitted")
        return refuse("which this request is not permitted to run.");
      if (asked === "agent-below-full-access")
        return refuse(
          `which an agent can only have run from a thread in Full access. ${
            fullAccessOff === undefined
              ? "Switch the calling thread to Full access, or start the worktree thread yourself."
              : `${fullAccessOff} Start the worktree thread yourself.`
          }`,
        );
      if (layoutCommandsOffBy !== undefined)
        return refuse(
          `and ${managedPolicyDeniedMessage(layoutCommandsOffBy, "project worktree layout commands")}`,
        );
      const approved = yield* (
        approvals
          .isApproved({ projectId: project.projectId, kind, command: text })
          // An approval that cannot be read is not one.
          .pipe(
            Effect.catch((cause) =>
              Effect.logWarning(cause.detail, { cause }).pipe(Effect.as(false)),
            ),
          )
      );
      if (!approved)
        return refuse(
          "which nobody has approved as it is written now. Review and approve it in the web or desktop app, under Worktree layout in the project's settings or when starting a worktree thread there.",
        );
      const command: ClearedLayoutCommand = {
        [cleared]: true,
        text,
        recheck: Effect.suspend(() =>
          layoutCommand(
            { ...project, realProjectRoot: realProjectRoot.value },
            kind,
            projectCommands,
          ),
        ),
      };
      return { command } as const;
    },
  );
  // One at a time per project: the commands share the project folder, and its workspace files.
  const layoutLocks = yield* KeyedLock.make<string>();
  const layoutShell = onWindows ? windowsSystem32Path(hostEnvironment, "cmd.exe") : "sh";
  const recheckLayoutCommand = Effect.fnUntraced(function* (
    operation: TaskFolderError["operation"],
    command: ClearedLayoutCommand,
  ) {
    const now = yield* command.recheck;
    if (now === null || !("command" in now) || now.command.text !== command.text) {
      return yield* new TaskFolderError({
        operation,
        detail: layoutRefusedDetail(
          now !== null && "refusal" in now
            ? now.refusal
            : "which changed while this request waited for its turn. Try again.",
        ),
      });
    }
    return now.command;
  });
  const invokeLayoutCommand = (
    operation: TaskFolderError["operation"],
    command: ClearedLayoutCommand,
    taskFolder: Pick<TaskFolder, "root" | "projectRoot" | "projectPath">,
    branch: string,
    baseBranch: string | undefined,
    repositories: ReadonlyArray<string>,
  ) =>
    processes
      .run({
        operation: "TaskFolders.worktreeLayout",
        command: layoutShell,
        args: onWindows ? ["/d", "/s", "/c", command.text] : ["-c", command.text],
        cwd: taskFolder.projectRoot,
        env: {
          ...hostEnvironment,
          // A `GIT_DIR` the server inherited would point the command's Git at another repository.
          ...VcsProcess.unsetInheritedRepositoryEnv(hostEnvironment, hostPlatform),
          T3_PROJECT_ROOT: taskFolder.projectRoot,
          T3_WORKTREE_ROOT: taskFolder.root,
          T3_WORKTREE_PROJECT_PATH: taskFolder.projectPath,
          T3_WORKTREE_BRANCH: branch,
          T3_WORKTREE_BASE: baseBranch ?? "",
          T3_WORKTREE_REPOSITORIES: repositories.join("\n"),
        },
        timeoutMs: LAYOUT_COMMAND_TIMEOUT_MS,
      })
      .pipe(Effect.mapError(fail(operation, "The project's worktree layout command failed")));
  const runLayoutCommand = (
    operation: TaskFolderError["operation"],
    command: ClearedLayoutCommand,
    taskFolder: Pick<TaskFolder, "root" | "projectRoot" | "projectPath">,
    branch: string,
    baseBranch: string | undefined,
    repositories: ReadonlyArray<string>,
    /**
     * What the caller does once the command's turn has come, before the check below. It answers
     * with what to note when the command is cleared to start, or null when the command is no
     * longer wanted: it is then left unrun.
     */
    inItsTurn: Effect.Effect<(() => void) | null, TaskFolderError> = Effect.succeed(() => {}),
  ) =>
    layoutLocks.withLock(
      taskFolder.projectRoot,
      Effect.gen(function* () {
        const noteStart = yield* inItsTurn;
        if (noteStart === null) return null;
        // The wait for a turn can be long. Recheck the caller, file, and approval in its turn.
        const now = yield* recheckLayoutCommand(operation, command);
        noteStart();
        return yield* invokeLayoutCommand(
          operation,
          now,
          taskFolder,
          branch,
          baseBranch,
          repositories,
        );
      }),
    );

  /** Where a folder's `.git` file points, or null when `.git` is a directory or absent. */
  const gitDirPointer = Effect.fnUntraced(function* (folder: string) {
    const pointer = yield* fileSystem
      .readFileString(path.join(folder, ".git"))
      .pipe(Effect.orElseSucceed(() => ""));
    const gitDir = /^gitdir:\s*(.+?)\s*$/m.exec(pointer)?.[1];
    return gitDir === undefined ? null : path.resolve(folder, gitDir);
  });
  // A repository that is itself a linked worktree registers worktrees under the Git dir it shares.
  const commonGitDir = Effect.fnUntraced(function* (repositoryPath: string) {
    const gitDir = yield* gitDirPointer(repositoryPath);
    if (gitDir === null) return path.join(repositoryPath, ".git");
    const common = (yield* fileSystem
      .readFileString(path.join(gitDir, "commondir"))
      .pipe(Effect.orElseSucceed(() => ""))).trim();
    return common === "" ? gitDir : path.resolve(gitDir, common);
  });

  // What a layout command leaves must be a checkout this service can later remove, restore and
  // rename: a worktree linked to the repository, on the thread's branch. Null when it is.
  /** The checkout's own Git directory when it is a worktree linked to its repository. */
  const linkedGitDir = Effect.fnUntraced(
    function* (checkout: TaskFolderCheckout) {
      const { worktreePath, repositoryPath } = checkout;
      if ((yield* lstatIfPresent(worktreePath))?.type !== "Directory") return null;
      const marker = path.join(worktreePath, ".git");
      if ((yield* lstatIfPresent(marker))?.type !== "File") return null;
      const pointer = yield* gitDirPointer(worktreePath);
      if (pointer === null) return null;
      const admin = yield* fileSystem.realPath(pointer);
      const common = yield* fileSystem.realPath(yield* commonGitDir(repositoryPath));
      if (!samePath(path.dirname(admin), path.join(common, "worktrees"))) return null;
      const registered = (yield* fileSystem.readFileString(path.join(admin, "gitdir"))).trim();
      const recordedCommon = (yield* fileSystem.readFileString(
        path.join(admin, "commondir"),
      )).trim();
      return registered !== "" &&
        recordedCommon !== "" &&
        samePath(
          yield* fileSystem.realPath(path.resolve(admin, registered)),
          yield* fileSystem.realPath(marker),
        ) &&
        samePath(yield* fileSystem.realPath(path.resolve(admin, recordedCommon)), common)
        ? admin
        : null;
    },
    Effect.orElseSucceed(() => null),
  );
  const unadoptable = Effect.fnUntraced(function* (checkout: TaskFolderCheckout, branch: string) {
    const resolved = yield* linkedGitDir(checkout);
    if (resolved === null)
      return `${checkout.worktreePath} is not a Git worktree of ${checkout.repositoryPath}`;
    const head = (yield* fileSystem
      .readFileString(path.join(resolved, "HEAD"))
      .pipe(Effect.orElseSucceed(() => ""))).trim();
    return head === `ref: refs/heads/${branch}`
      ? null
      : `${checkout.worktreePath} is not on the branch ${branch}`;
  });

  const mapped = (taskFolder: Pick<TaskFolder, "root" | "anchor">, source: string) =>
    path.join(taskFolder.root, path.relative(taskFolder.anchor, source));
  const depth = (target: string) => path.resolve(target).split(path.sep).length;

  const readProject = Effect.fn("TaskFolders.readProject")(function* (
    operation: TaskFolderError["operation"],
    projectId: ProjectId,
  ) {
    const row = yield* projects
      .get(projectId)
      .pipe(Effect.mapError(fail(operation, "Could not read the project")));
    if (Option.isNone(row)) {
      return yield* new TaskFolderError({
        operation,
        detail: `Project ${projectId} was not found.`,
      });
    }
    return row.value;
  });

  const registeredPathReason = Effect.fn("TaskFolders.registeredPathReason")(
    function* (target: string) {
      const named = path.resolve(target);
      const real = yield* canonicalLocation(named);
      const contains = (candidate: string) =>
        isWithinRoot(path, candidate, named) || isWithinRoot(path, candidate, real);
      for (const project of yield* projects.listShells()) {
        const roots = [
          project.workspaceRoot,
          ...(project.trackedFolders ?? []).map((folder) => folder.path),
        ];
        for (const root of roots) {
          const projectPath = path.resolve(root);
          const projectReal = yield* canonicalLocation(projectPath);
          if (contains(projectPath) || contains(projectReal)) return "contains a project checkout";
          const marker = yield* lstatIfPresent(path.join(projectReal, ".git"));
          if (marker === null) continue;
          const common = yield* canonicalLocation(yield* commonGitDir(projectReal));
          if (contains(common)) return "contains a registered project's Git common directory";
        }
      }
      return null;
    },
    Effect.mapError(fail("remove", "Could not verify the registered project paths")),
  );

  const removalBlocked = Effect.fnUntraced(
    function* (target: string, ownerThreadId?: ThreadId) {
      const registered = yield* registeredPathReason(target);
      if (registered !== null) return registered;
      const named = path.resolve(target);
      const real = yield* canonicalLocation(named);
      const overlaps = (candidate: string) =>
        [named, real].some(
          (root) => isWithinRoot(path, candidate, root) || isWithinRoot(path, root, candidate),
        );
      for (const binding of yield* projections.getThreadWorktreePaths({ includeArchived: true })) {
        if (binding.threadId === ownerThreadId) continue;
        const candidate = path.resolve(binding.worktreePath);
        if (overlaps(candidate) || overlaps(yield* canonicalLocation(candidate)))
          return "another thread has a workspace binding here";
      }
      return null;
    },
    Effect.mapError((cause) =>
      isTaskFolderError(cause)
        ? cause
        : fail("remove", "Could not verify workspace bindings")(cause),
    ),
  );

  // A layout, an existing checkout, or a symlinked parent must not redirect a member outside
  // the task folder. Inspect the named path without following any link below the recorded root.
  const assertDestination = Effect.fnUntraced(function* (
    root: string,
    target: string,
    operation: TaskFolderError["operation"],
  ) {
    const unsafe = () =>
      new TaskFolderError({
        operation,
        detail: `Could not verify the task folder destination ${target}.`,
      });
    if (!isWithinRoot(path, target, root)) return yield* unsafe();
    for (let current = target; ; current = path.dirname(current)) {
      const info = yield* lstatIfPresent(current).pipe(Effect.mapError(unsafe));
      if (info !== null && info.type !== "Directory") return yield* unsafe();
      if (samePath(current, root)) return;
    }
  });

  const plan: TaskFolders["Service"]["plan"] = Effect.fn("TaskFolders.plan")(function* (input) {
    const project = yield* readProject("plan", input.projectId);
    const projectRoot = path.resolve(project.workspaceRoot);
    const classified = yield* Effect.forEach(
      yield* trackedFolders(project),
      (folder) => {
        const folderPath = path.resolve(folder.path);
        return Effect.all([isRepository(folderPath), isSubmodule(folderPath)]).pipe(
          Effect.map(([git, submodule]) => ({
            name: folder.name,
            path: folderPath,
            git,
            submodule,
          })),
        );
      },
      { concurrency: 8 },
    );
    const tracked = classified.filter(
      (folder) => folder.git && !folder.submodule && folder.path !== projectRoot,
    );
    // A project that lays its own thread folder out gets one even with nothing else to check out.
    // An explicit choice still goes through the checks below, so it is refused, not dropped.
    if (
      tracked.length === 0 &&
      (input.repositories ?? []).length === 0 &&
      (yield* worktreeLayout(projectRoot)) === null
    )
      return null;
    // A repository on another Windows drive has no place in the folder; only choosing it fails.
    const { anchor, unreachable } = layoutAnchor(
      path,
      projectRoot,
      tracked.map((folder) => folder.path),
    );
    const reachable = tracked.filter((folder) => !unreachable.includes(folder.path));
    let repositories: ReadonlyArray<ProjectTrackedFolder>;
    if (input.repositories === undefined) {
      repositories = reachable.length <= TASK_FOLDER_DEFAULT_REPOSITORIES_MAX ? reachable : [];
    } else {
      const selected = new Map<string, ProjectTrackedFolder>();
      for (const requested of input.repositories) {
        const requestedPath = path.resolve(requested.path);
        const folder = classified.find((candidate) => candidate.path === requestedPath);
        // A submodule comes with its repository's worktree; clients cannot tell one apart.
        if (folder?.submodule === true) continue;
        if (folder === undefined || !folder.git || folder.path === projectRoot) {
          return yield* new TaskFolderError({
            operation: "plan",
            detail:
              folder === undefined
                ? `"${requested.path}" is not a tracked folder of project "${project.title}".`
                : `"${requested.path}" is not a Git repository, so it cannot get a worktree.`,
          });
        }
        if (unreachable.includes(folder.path)) {
          return yield* new TaskFolderError({
            operation: "plan",
            detail: `"${folder.path}" shares no common root with the project folder ${projectRoot}, so the two cannot be laid out in one worktree folder.`,
          });
        }
        selected.set(folder.path, folder);
      }
      repositories = [...selected.values()];
    }
    // A repository with no commit yet has nothing to branch from, so it is left out.
    repositories = yield* Effect.filter(
      repositories,
      (folder) =>
        git.hasCommit({ cwd: folder.path, refName: "HEAD" }).pipe(Effect.orElseSucceed(() => true)),
      { concurrency: REPOSITORY_CONCURRENCY },
    );
    const realTrackedRepositories = new Map(
      yield* Effect.forEach(reachable, (folder) =>
        fileSystem.realPath(folder.path).pipe(
          Effect.map((resolved) => [folder.path, resolved] as const),
          Effect.mapError(fail("plan", "Could not verify a tracked repository")),
        ),
      ),
    );
    return {
      projectId: project.projectId,
      projectRoot,
      realProjectRoot: yield* fileSystem
        .realPath(projectRoot)
        .pipe(Effect.mapError(fail("plan", "Could not verify the project folder"))),
      projectIsRepository: yield* isRepository(projectRoot),
      anchor,
      repositories: repositories.map(({ name, path: folderPath }) => ({ name, path: folderPath })),
      trackedRepositories: reachable.map((folder) => folder.path),
      realTrackedRepositories,
    };
  });

  const branchNames = (cwds: ReadonlyArray<string>) =>
    Effect.forEach(cwds, (cwd) => git.listLocalBranchNames(cwd), {
      concurrency: REPOSITORY_CONCURRENCY,
    });
  // Git stores branches as files, so a name also collides with one it nests under or contains.
  const collides = (existing: string, candidate: string) =>
    existing === candidate ||
    existing.startsWith(`${candidate}/`) ||
    candidate.startsWith(`${existing}/`);

  const freeBranchName = (
    operation: TaskFolderError["operation"],
    branch: string,
    names: ReadonlyArray<ReadonlyArray<string>>,
  ) => {
    for (let attempt = 1; attempt <= BRANCH_NAME_ATTEMPTS; attempt++) {
      const candidate = attempt === 1 ? branch : `${branch}-${attempt}`;
      if (!names.some((list) => list.some((existing) => collides(existing, candidate))))
        return Effect.succeed(candidate);
    }
    return Effect.fail(
      new TaskFolderError({
        operation,
        detail: `No branch name based on "${branch}" is free in every repository.`,
      }),
    );
  };

  /**
   * Where a repository's branch starts: `baseBranch` when the repository has it, else its
   * default branch. The remote commit of either when starting from origin.
   */
  const repositoryStart = Effect.fn("TaskFolders.repositoryStart")(function* (
    cwd: string,
    startFromOrigin: boolean,
    baseBranch?: string,
  ) {
    // Clients send `HEAD` when no base was chosen; as a name it would match `origin/HEAD`.
    const wanted = baseBranch?.replace(/^refs\/heads\//, "").replace(/^origin\//, "");
    const defaultBranch =
      wanted !== undefined &&
      wanted !== "" &&
      wanted !== "HEAD" &&
      ((yield* git.hasCommit({ cwd, refName: `refs/heads/${wanted}` })) ||
        (yield* git.hasCommit({ cwd, refName: `refs/remotes/origin/${wanted}` })))
        ? wanted
        : ((yield* gitDriver.resolveDefaultBranchName(cwd, "origin")) ??
          (yield* git.currentBranch(cwd)));
    let baseRef = "HEAD";
    if (defaultBranch !== null) {
      if (yield* git.hasCommit({ cwd, refName: `refs/heads/${defaultBranch}` }))
        baseRef = defaultBranch;
      else if (yield* git.hasCommit({ cwd, refName: `refs/remotes/origin/${defaultBranch}` }))
        baseRef = `origin/${defaultBranch}`;
    }
    const remoteBase =
      startFromOrigin && baseRef !== "HEAD"
        ? yield* git.fetchWorktreeBaseCommit({ cwd, refName: baseRef })
        : null;
    return remoteBase === null
      ? { refName: baseRef, baseRefName: baseRef }
      : { refName: remoteBase.commitSha, baseRefName: `refs/remotes/${remoteBase.remoteRefName}` };
  });

  /**
   * Stands in for a folder that is not a repository: every entry links to the original, so
   * nothing is copied, except the folders that lead to a tracked repository, which are real so a
   * worktree can sit in them.
   */
  const linkFolder: (
    source: string,
    target: string,
    trackedRepositories: ReadonlyArray<string>,
    skip?: string,
  ) => Effect.Effect<void, PlatformError.PlatformError> = Effect.fnUntraced(
    function* (source, target, trackedRepositories, skip) {
      yield* fileSystem.makeDirectory(target, { recursive: true });
      for (const name of yield* fileSystem.readDirectory(source)) {
        const entry = path.join(source, name);
        const link = path.join(target, name);
        if (name === ".git" || name === skip || trackedRepositories.includes(entry)) continue;
        // A repository discovered since the plan must not become a writable link to its source.
        if (yield* isRepository(entry)) continue;
        if (yield* exists(link)) continue;
        if (trackedRepositories.some((repository) => isWithinRoot(path, repository, entry))) {
          yield* linkFolder(entry, link, trackedRepositories);
        } else {
          yield* fileSystem.symlink(entry, link);
        }
      }
    },
  );

  /**
   * Makes room for a worktree at `relative` inside a linked folder: a link on the way becomes a
   * real folder linking its other entries. Anything there that this service did not link stays.
   */
  const openLinkedPath = Effect.fnUntraced(function* (
    source: string,
    target: string,
    relative: string,
  ) {
    const segments = relative.split(path.sep);
    for (const [index, segment] of segments.entries()) {
      source = path.join(source, segment);
      target = path.join(target, segment);
      const last = index === segments.length - 1;
      const link = yield* fileSystem.readLink(target).pipe(Effect.option);
      if (Option.isSome(link)) {
        if (path.resolve(path.dirname(target), link.value) !== source) return false;
        yield* fileSystem.remove(target);
        if (!last) yield* linkFolder(source, target, [], segments[index + 1]);
      } else if (!last) {
        yield* fileSystem.makeDirectory(target, { recursive: true });
      }
    }
    return true;
  });

  const isProjectLink = Effect.fnUntraced(function* (taskFolder: TaskFolder, target: string) {
    const link = yield* fileSystem.readLink(target);
    return samePath(
      path.resolve(path.dirname(target), link),
      path.resolve(taskFolder.anchor, path.relative(taskFolder.root, target)),
    );
  });

  /** Removes only links to the original project and directories they leave empty. */
  const removeLinks: (
    taskFolder: TaskFolder,
    target: string,
  ) => Effect.Effect<boolean, PlatformError.PlatformError | RemoveEmptyDirectoryError> =
    Effect.fnUntraced(function* (taskFolder, target) {
      const info = yield* lstatIfPresent(target);
      if (info === null) return true;
      if (info.type === "SymbolicLink") {
        if (!(yield* isProjectLink(taskFolder, target))) return false;
        yield* fileSystem.remove(target);
        return true;
      }
      if (info.type !== "Directory") return false;
      if (yield* isRepository(target)) return false;
      let empty = true;
      for (const name of yield* fileSystem.readDirectory(target)) {
        if (!(yield* removeLinks(taskFolder, path.join(target, name)))) empty = false;
      }
      return empty && (yield* removeEmptyDirectory(target));
    });

  /** Checks the scaffolding before unlinking it or handing the whole folder to teardown. */
  const holdsOnlyLinks: (
    taskFolder: TaskFolder,
    target: string,
    checked: ReadonlySet<string>,
  ) => Effect.Effect<boolean, PlatformError.PlatformError> = Effect.fnUntraced(
    function* (taskFolder, target, checked) {
      if (checked.has(target)) return true;
      const info = yield* lstatIfPresent(target);
      if (info === null) return true;
      if (info.type === "SymbolicLink") return yield* isProjectLink(taskFolder, target);
      if (info.type !== "Directory") return false;
      if (yield* isRepository(target)) return false;
      for (const name of yield* fileSystem.readDirectory(target)) {
        if (!(yield* holdsOnlyLinks(taskFolder, path.join(target, name), checked))) return false;
      }
      return true;
    },
  );

  const allocateRoot = Effect.fn("TaskFolders.allocateRoot")(function* (
    taskPlan: TaskFolderPlan,
    branch: string,
  ) {
    const parent = resolveWorktreesDirectory(
      (yield* settings.getSettings.pipe(Effect.orElseSucceed(() => ({ worktreesDirectory: "" }))))
        .worktreesDirectory,
      config.worktreesDir,
      path,
      yield* HostProcess.HomeDirectory,
    );
    if (parent === null) {
      return yield* new TaskFolderError({
        operation: "create",
        detail:
          "The worktree location must be an absolute folder on this machine, not a drive root. Change it in Settings → Worktrees.",
      });
    }
    const hash = yield* digest.digest(
      Digest.sha256,
      new TextEncoder().encode(`${taskPlan.projectId}\0${taskPlan.anchor}\0${branch}`),
    );
    // UTF-8 bytes bound UTF-16 length too, leaving room for files on Windows.
    let prefix = "";
    for (const character of branch.replace(/\//g, "-")) {
      if (Buffer.byteLength(prefix + character) > 23) break;
      prefix += character;
    }
    const base = path.join(
      parent,
      path.basename(taskPlan.anchor) || "workspace",
      `${prefix}-${Hex.encode(hash).slice(0, 16)}`,
    );
    const allocateFailed = fail("create", "Could not create the worktree folder");
    const destination = yield* canonicalLocation(base).pipe(Effect.mapError(allocateFailed));
    for (const source of [taskPlan.realProjectRoot, ...taskPlan.trackedRepositories]) {
      const real = yield* canonicalLocation(source).pipe(Effect.mapError(allocateFailed));
      if (isWithinRoot(path, destination, real)) {
        return yield* new TaskFolderError({
          operation: "create",
          detail: "The task folder would be inside one of its source folders.",
        });
      }
    }
    yield* fileSystem
      .makeDirectory(path.dirname(base), { recursive: true })
      .pipe(Effect.mapError(allocateFailed));
    // Made here, not just found free: two creations on one branch must not share a folder.
    for (let suffix = 1; ; suffix++) {
      const root = suffix === 1 ? base : `${base}-${suffix}`;
      const claimed = yield* fileSystem.makeDirectory(root).pipe(
        Effect.as(true),
        Effect.catchIf(
          (cause) => cause.reason._tag === "AlreadyExists",
          () => Effect.succeed(false),
        ),
        Effect.mapError(allocateFailed),
      );
      if (claimed) return root;
    }
  });

  const checkoutRemovalReason = Effect.fnUntraced(function* (checkout: TaskFolderCheckout) {
    if ((yield* linkedGitDir(checkout)) === null) return "checkout identity changed";
    if ((yield* git.currentBranch(checkout.worktreePath)) === null) return "has a detached HEAD";
    const status = yield* gitDriver.execute({
      operation: "TaskFolders.checkRemoval",
      cwd: checkout.worktreePath,
      args: [
        "--no-optional-locks",
        "status",
        "--porcelain=v1",
        "--untracked-files=all",
        "--ignored=matching",
        "--ignore-submodules=none",
      ],
    });
    return status.stdout.length === 0 ? null : "has local files or changes without preservation";
  });

  const removeCheckout = Effect.fnUntraced(function* (checkout: TaskFolderCheckout) {
    yield* git.removeWorktree({
      cwd: checkout.repositoryPath,
      path: checkout.worktreePath,
      force: false,
    });
  });
  const hasWorktreeRegistration = Effect.fnUntraced(function* (checkout: TaskFolderCheckout) {
    const real = yield* canonicalLocation(checkout.worktreePath);
    for (const registered of yield* gitDriver.listWorktreePaths(checkout.repositoryPath)) {
      if (samePath(yield* canonicalLocation(path.resolve(registered)), real)) return true;
    }
    return false;
  });

  const teardownReceipts = store.teardownReceipts;
  const teardown = yield* TaskFolderTeardown.make({
    receipts: teardownReceipts,
    linkedGitDir,
    commonGitDir,
    canonicalLocation,
    assertDestination: (root, target) => assertDestination(root, target, "remove"),
    hasRegistration: (checkout) =>
      hasWorktreeRegistration(checkout).pipe(
        Effect.mapError(fail("remove", "Could not verify the worktree registration")),
      ),
  });

  // A failed creation confers no authority to run the project's teardown command.
  const neverLaidOut = new Set<string>();

  const removeLocked = Effect.fn("TaskFolders.remove")(
    function* (
      taskFolder: TaskFolder,
      input: Omit<Parameters<TaskFolders["Service"]["remove"]>[0], "root">,
      keepBranchIn: ReadonlySet<string> = new Set(),
      laidOut = !neverLaidOut.has(taskFolder.root),
      expectedBranch = input.deleteBranch,
    ) {
      const removed: string[] = [];
      const kept: Array<{ path: string; reason: string }> = [];
      let remaining = taskFolder.repositories;
      const holdFolder = (reason: string): TaskFolderRemoval => ({
        removed,
        kept: [{ path: taskFolder.root, reason }],
      });
      const blocked = yield* removalBlocked(taskFolder.root, input.ownerThreadId);
      if (blocked !== null) return holdFolder(blocked);

      const projectCheckout = taskFolder.projectIsWorktree
        ? { worktreePath: taskFolder.projectPath, repositoryPath: taskFolder.projectRoot }
        : null;
      const checkouts = [
        ...taskFolder.repositories,
        ...(projectCheckout === null ? [] : [projectCheckout]),
      ];
      for (const checkout of checkouts)
        yield* assertDestination(taskFolder.root, checkout.worktreePath, "remove");
      yield* assertDestination(taskFolder.root, taskFolder.projectPath, "remove");

      const verdict = Effect.fnUntraced(function* (checkout: TaskFolderCheckout) {
        const keep = input.inspect === undefined ? null : (yield* input.inspect(checkout)).keep;
        return keep ?? (yield* checkoutRemovalReason(checkout));
      });
      const plan = yield* teardown.prepare({
        folder: taskFolder,
        checkouts,
        ownerThreadId: input.ownerThreadId,
        expectedBranch,
        inTurn: (effect) => layoutLocks.withLock(taskFolder.projectRoot, effect),
        approvedCommand: Effect.gen(function* () {
          if (!laidOut || checkouts.length === 0) return null;
          const layout = yield* layoutCommand(taskFolder, "remove", input.projectCommands);
          if (layout === null || "refusal" in layout) return null;
          const recheck = recheckLayoutCommand("remove", layout.command);
          const branch = input.deleteBranch ?? "";
          return {
            commandHash: yield* LayoutCommandApprovals.commandHash(digest, layout.command.text),
            shell: layoutShell,
            branch,
            recheck,
            run: recheck.pipe(
              Effect.flatMap((now) =>
                invokeLayoutCommand(
                  "remove",
                  now,
                  taskFolder,
                  branch,
                  undefined,
                  taskFolder.repositories.map((repository) => repository.repositoryPath),
                ),
              ),
              Effect.map((result) => result.exitCode),
            ),
          };
        }),
        // A layout command can touch the entire folder. Recheck custody, the links and
        // every checkout after any wait, before the command is allowed to start.
        allGo: Effect.gen(function* () {
          if ((yield* removalBlocked(taskFolder.root, input.ownerThreadId)) !== null) return false;
          yield* assertDestination(taskFolder.root, taskFolder.projectPath, "remove");
          const checked = new Set<string>();
          for (const checkout of checkouts) {
            yield* assertDestination(taskFolder.root, checkout.worktreePath, "remove");
            if ((yield* lstatIfPresent(checkout.worktreePath)) === null) return false;
            if ((yield* verdict(checkout)) !== null) return false;
            checked.add(checkout.worktreePath);
          }
          return yield* holdsOnlyLinks(taskFolder, taskFolder.root, checked);
        }).pipe(Effect.orElseSucceed(() => false)),
      });
      if ("held" in plan) return holdFolder(plan.held);
      const captured = plan.capture;
      const verifyCleanupOwner =
        captured === null
          ? Effect.void
          : teardown.assertOwner(captured, input.ownerThreadId, "cleanup");

      const removeOne = Effect.fnUntraced(function* (checkout: TaskFolderCheckout) {
        const target = checkout.worktreePath;
        return yield* Effect.gen(function* () {
          yield* assertDestination(taskFolder.root, target, "remove");
          const original = captured?.checkouts.find((entry) => entry.worktreePath === target);
          if (captured !== null && original === undefined)
            return yield* TaskFolderTeardown.ownerChanged();
          if ((yield* lstatIfPresent(target)) === null) {
            // An absent path is not a receipt for an absent Git registration.
            if (original !== undefined && (yield* teardown.assertCheckout(original, true)))
              return yield* TaskFolderTeardown.ownerChanged();
            if (yield* hasWorktreeRegistration(checkout)) {
              kept.push({
                path: target,
                reason: "worktree registration requires reconciliation",
              });
              return false;
            }
            yield* verifyCleanupOwner;
            return true;
          }
          const keep =
            (yield* verdict(checkout)) ??
            (yield* removalBlocked(taskFolder.root, input.ownerThreadId));
          if (keep !== null) {
            kept.push({ path: target, reason: keep });
            return false;
          }
          const deleteBranch = input.deleteBranch;
          if (
            expectedBranch !== undefined &&
            (yield* git.currentBranch(target)) !== expectedBranch
          ) {
            kept.push({ path: target, reason: "the worktree branch changed" });
            return false;
          }
          yield* verifyCleanupOwner;
          if (original !== undefined) yield* teardown.assertCheckout(original, false);
          yield* removeCheckout(checkout);
          if ((yield* lstatIfPresent(target)) !== null) {
            kept.push({ path: target, reason: "files remain after Git removed the worktree" });
            return false;
          }
          removed.push(target);
          if (deleteBranch !== undefined && !keepBranchIn.has(checkout.repositoryPath))
            yield* git
              .deleteLocalBranch({
                cwd: checkout.repositoryPath,
                refName: deleteBranch,
                force: false,
              })
              .pipe(Effect.ignoreCause({ log: true }));
          return true;
        }).pipe(
          Effect.catch((cause) =>
            Effect.sync(() => {
              kept.push({ path: target, reason: failureDetail(cause) });
              return false;
            }),
          ),
        );
      });

      // Children first: a worktree nested in another must go before the one that holds it.
      for (const repository of taskFolder.repositories.toSorted(
        (left, right) => depth(right.worktreePath) - depth(left.worktreePath),
      )) {
        if (kept.some((entry) => isWithinRoot(path, entry.path, repository.worktreePath))) {
          kept.push({ path: repository.worktreePath, reason: "holds a repository that was kept" });
          continue;
        }
        if (!(yield* removeOne(repository))) continue;
        remaining = remaining.filter((entry) => entry.worktreePath !== repository.worktreePath);
        if (input.forget) yield* store.put({ ...taskFolder, repositories: remaining });
      }
      const projectPath = taskFolder.projectPath;
      const projectHoldsKept = kept.some((entry) => isWithinRoot(path, entry.path, projectPath));
      if (projectCheckout !== null) {
        if (!projectHoldsKept) yield* removeOne(projectCheckout);
        else kept.push({ path: projectPath, reason: "holds a repository that was kept" });
      } else if (!projectHoldsKept) {
        const blockedNow = yield* removalBlocked(taskFolder.root, input.ownerThreadId);
        if (blockedNow !== null) return holdFolder(blockedNow);
        yield* verifyCleanupOwner;
        const projectGone = yield* holdsOnlyLinks(taskFolder, projectPath, new Set()).pipe(
          Effect.flatMap((onlyLinks) =>
            onlyLinks ? removeLinks(taskFolder, projectPath) : Effect.succeed(false),
          ),
          Effect.orElseSucceed(() => false),
        );
        if (projectGone) removed.push(projectPath);
        else kept.push({ path: projectPath, reason: "has files that exist only in this folder" });
      }
      if (kept.length === 0) {
        const blockedNow = yield* removalBlocked(taskFolder.root, input.ownerThreadId);
        if (blockedNow !== null) return holdFolder(blockedNow);
        yield* verifyCleanupOwner;
        const rootGone = yield* removeLinks(taskFolder, taskFolder.root).pipe(
          Effect.orElseSucceed(() => false),
        );
        if (!rootGone) {
          kept.push({ path: taskFolder.root, reason: "has files that exist only in this folder" });
        } else if (input.forget) {
          yield* store.remove(taskFolder.root);
          neverLaidOut.delete(taskFolder.root);
        }
      }
      return { removed, kept } satisfies TaskFolderRemoval;
    },
    Effect.mapError((cause) =>
      isTaskFolderError(cause)
        ? cause
        : fail("remove", "Could not verify task folder removal")(cause),
    ),
  );

  const requireTaskFolder = Effect.fnUntraced(function* (
    operation: TaskFolderError["operation"],
    target: string,
  ) {
    const taskFolder = yield* store.findContaining(target);
    if (taskFolder === null) {
      return yield* new TaskFolderError({
        operation,
        detail: `"${target}" is not inside a task folder.`,
      });
    }
    return taskFolder;
  });
  // The same lease storage cleanup and file rewind take: the thread's worktree path.
  const locked = <A, E>(taskFolder: Pick<TaskFolder, "projectPath">, effect: Effect.Effect<A, E>) =>
    withWorkspaceLease(path.resolve(taskFolder.projectPath), effect);

  const createRepositoryWorktree = Effect.fnUntraced(function* (
    taskFolder: TaskFolder,
    repository: ProjectTrackedFolder,
    branch: string,
    startFromOrigin: boolean,
    baseBranch: string | undefined,
    verifyRepository: (repositoryPath: string) => Effect.Effect<void, TaskFolderError>,
    onAttempt: (checkout: TaskFolderRepository) => void,
  ) {
    const worktreePath = mapped(taskFolder, repository.path);
    const checkout = { repositoryPath: repository.path, worktreePath, name: repository.name };
    onAttempt(checkout);
    yield* verifyRepository(repository.path);
    yield* assertDestination(taskFolder.root, worktreePath, "create");
    if (yield* occupied(worktreePath)) {
      const reason = yield* unadoptable(checkout, branch);
      if (reason !== null)
        return yield* new TaskFolderError({ operation: "create", detail: reason });
      return { repository: checkout, fromOrigin: false };
    }
    yield* fileSystem.makeDirectory(path.dirname(worktreePath), { recursive: true });
    const start = yield* repositoryStart(repository.path, startFromOrigin, baseBranch);
    yield* verifyRepository(repository.path);
    yield* assertDestination(taskFolder.root, worktreePath, "create");
    yield* git.createWorktree({
      cwd: repository.path,
      refName: start.refName,
      newRefName: branch,
      baseRefName: start.baseRefName,
      path: worktreePath,
    });
    return {
      repository: checkout,
      fromOrigin: start.refName !== start.baseRefName,
    };
  });

  const create: TaskFolders["Service"]["create"] = Effect.fn("TaskFolders.create")(
    function* (input) {
      const taskPlan = input.plan;
      const verifyProject = Effect.gen(function* () {
        const project = yield* readProject("create", taskPlan.projectId);
        const real = yield* fileSystem
          .realPath(project.workspaceRoot)
          .pipe(Effect.mapError(fail("create", "Could not verify the project folder")));
        if (
          !samePath(project.workspaceRoot, taskPlan.projectRoot) ||
          !samePath(real, taskPlan.realProjectRoot)
        )
          return yield* new TaskFolderError({
            operation: "create",
            detail: "The project folder changed during preparation.",
          });
      });
      const verifyRepository = Effect.fnUntraced(function* (source: string) {
        const real = yield* fileSystem
          .realPath(source)
          .pipe(Effect.mapError(fail("create", "Could not verify the tracked repository")));
        if (real !== taskPlan.realTrackedRepositories.get(source))
          return yield* new TaskFolderError({
            operation: "create",
            detail: `The tracked repository ${source} changed during preparation.`,
          });
      });
      const verifySources = verifyProject.pipe(
        Effect.andThen(
          Effect.forEach(taskPlan.trackedRepositories, verifyRepository, { discard: true }),
        ),
      );
      yield* verifySources;
      const layout = yield* layoutCommand(taskPlan, "create", input.projectCommands);
      if (layout !== null && "refusal" in layout)
        return yield* new TaskFolderError({
          operation: "create",
          detail: layoutRefusedDetail(layout.refusal),
        });
      const listFailed = fail("create", "Could not list branches");
      const names = yield* branchNames(
        taskPlan.repositories.map((repository) => repository.path),
      ).pipe(Effect.mapError(listFailed));
      const projectNames = taskPlan.projectIsRepository
        ? yield* git.listLocalBranchNames(taskPlan.projectRoot).pipe(Effect.mapError(listFailed))
        : [];
      // A temporary name nests under its prefix, which a repository may hold as a branch.
      const requested =
        isTemporaryWorktreeBranch(input.branch) &&
        [...names, projectNames].some((list) =>
          list.some((existing) => input.branch.startsWith(`${existing}/`)),
        )
          ? flattenTemporaryWorktreeBranchName(input.branch)
          : input.branch;
      // A name the project folder already has is Git's to refuse, as for a single worktree.
      const branch = projectNames.some((existing) => collides(existing, requested))
        ? requested
        : yield* freeBranchName("create", requested, [...names, projectNames]);
      const root = yield* allocateRoot(taskPlan, branch);
      const empty: TaskFolder = {
        root,
        projectId: taskPlan.projectId,
        projectRoot: taskPlan.projectRoot,
        anchor: taskPlan.anchor,
        projectPath: path.join(root, path.relative(taskPlan.anchor, taskPlan.projectRoot)),
        projectIsWorktree: taskPlan.projectIsRepository,
        repositories: [],
      };
      const created: TaskFolderRepository[] = [];
      const attempted = new Map<string, TaskFolderRepository>();
      let repositoriesStartedFromOrigin = false;
      let layoutStarted = false;
      const current = (): TaskFolder => ({ ...empty, repositories: [...created] });
      const checkedOutFrom = [
        ...(taskPlan.projectIsRepository ? [taskPlan.projectRoot] : []),
        ...taskPlan.trackedRepositories,
      ];
      // A layout command may check out a branch that was already there; a rollback leaves it.
      const hadBranch = new Set<string>();
      for (const [index, repository] of taskPlan.repositories.entries()) {
        if (names[index]!.includes(branch)) hadBranch.add(repository.path);
      }
      if (projectNames.includes(branch)) hadBranch.add(taskPlan.projectRoot);
      return yield* Effect.gen(function* () {
        // Record the intended members before Git runs. A restarted consumer must see any
        // member still missing rather than mistake a partially filled row for a finished folder.
        yield* store.put({
          ...empty,
          repositories: taskPlan.repositories.map((repository) => ({
            repositoryPath: repository.path,
            worktreePath: mapped(empty, repository.path),
            name: repository.name,
          })),
        });
        if (input.onClaimed !== undefined) yield* input.onClaimed(empty, branch);
        yield* verifySources;
        if (layout !== null) {
          const ran = yield* Effect.exit(
            runLayoutCommand(
              "create",
              layout.command,
              empty,
              branch,
              input.baseBranch,
              taskPlan.repositories.map((repository) => repository.path),
              // Listed only now: a branch that appeared during the wait is not this attempt's to
              // delete. Noted only once the command is cleared: one refused after its wait made
              // nothing to roll back.
              verifySources.pipe(Effect.andThen(branchNames(checkedOutFrom))).pipe(
                Effect.mapError(listFailed),
                Effect.map((before) => () => {
                  layoutStarted = true;
                  for (const [index, repositoryPath] of checkedOutFrom.entries()) {
                    if (before[index]!.includes(branch)) hadBranch.add(repositoryPath);
                  }
                }),
              ),
            ),
          );
          // Whatever the command checked out where a repository belongs is the thread's. Adopted
          // even when the command failed, so the rollback removes it instead of leaking it.
          const refused: string[] = [];
          for (const repositoryPath of taskPlan.trackedRepositories) {
            const checkout = { repositoryPath, worktreePath: mapped(empty, repositoryPath) };
            if (!(yield* isRepository(checkout.worktreePath))) continue;
            yield* assertDestination(root, checkout.worktreePath, "create");
            const reason = yield* unadoptable(checkout, branch);
            if (reason === null) created.push({ ...checkout, name: path.basename(repositoryPath) });
            else refused.push(reason);
          }
          if (Exit.isFailure(ran)) return yield* Effect.failCause(ran.cause);
          yield* assertDestination(root, empty.projectPath, "create");
          if (!(yield* exists(empty.projectPath))) {
            return yield* new TaskFolderError({
              operation: "create",
              detail: `The project's worktree layout command left no folder at ${empty.projectPath}.`,
            });
          }
          if (taskPlan.projectIsRepository && (yield* isRepository(empty.projectPath))) {
            const reason = yield* unadoptable(
              { repositoryPath: taskPlan.projectRoot, worktreePath: empty.projectPath },
              branch,
            );
            if (reason !== null) refused.push(reason);
          }
          if (refused.length > 0) {
            return yield* new TaskFolderError({
              operation: "create",
              detail: `The project's worktree layout command must make each checkout with git worktree add on the thread's branch: ${refused.join("; ")}.`,
            });
          }
          const taskFolder: TaskFolder = {
            ...current(),
            projectIsWorktree:
              taskPlan.projectIsRepository && (yield* isRepository(empty.projectPath)),
          };
          yield* store.put(taskFolder);
          return {
            taskFolder,
            branch,
            laidOutByProject: true,
            repositoriesStartedFromOrigin: false,
          };
        }
        if ((yield* worktreeLayout(taskPlan.projectRoot))?.create !== undefined)
          return yield* new TaskFolderError({
            operation: "create",
            detail:
              "The project added a worktree layout command during preparation. Retry with its current approval.",
          });
        if (taskPlan.projectIsRepository) {
          yield* assertDestination(root, empty.projectPath, "create");
          yield* fileSystem.makeDirectory(path.dirname(empty.projectPath), { recursive: true });
          yield* git.createWorktree(
            {
              cwd: taskPlan.projectRoot,
              refName: input.projectStart.refName,
              newRefName: branch,
              baseRefName: input.projectStart.baseRefName,
              path: empty.projectPath,
            },
            input.projectProgress === undefined ? {} : { progress: input.projectProgress },
          );
        } else {
          yield* linkFolder(taskPlan.projectRoot, empty.projectPath, taskPlan.trackedRepositories);
        }
        let finished = 0;
        // A repository inside another waits for the one that holds it.
        const levels = Map.groupBy(
          taskPlan.repositories,
          (repository) =>
            taskPlan.repositories.filter(
              (other) =>
                other.path !== repository.path && isWithinRoot(path, repository.path, other.path),
            ).length,
        );
        for (const level of [...levels.keys()].toSorted((left, right) => left - right)) {
          // Every checkout runs to its end: one stopped halfway is registered in its repository
          // without being known here, so nothing would remove it.
          const exits = yield* Effect.forEach(
            levels.get(level)!,
            (repository) =>
              createRepositoryWorktree(
                empty,
                repository,
                branch,
                input.startFromOrigin,
                input.baseBranch,
                verifyRepository,
                (checkout) => attempted.set(checkout.worktreePath, checkout),
              ).pipe(
                Effect.tap((worktree) =>
                  Effect.suspend(() => {
                    finished += 1;
                    created.push(worktree.repository);
                    repositoriesStartedFromOrigin ||= worktree.fromOrigin;
                    return (
                      input.onRepository?.({
                        name: repository.name,
                        done: finished,
                        total: taskPlan.repositories.length,
                      }) ?? Effect.void
                    );
                  }),
                ),
                Effect.mapError(fail("create", `Could not create the ${repository.name} worktree`)),
                Effect.exit,
              ),
            { concurrency: REPOSITORY_CONCURRENCY },
          ).pipe(Effect.uninterruptible);
          const failed = exits.find(Exit.isFailure);
          if (failed !== undefined) return yield* Effect.failCause(failed.cause);
        }
        const taskFolder = current();
        yield* store.put(taskFolder);
        return { taskFolder, branch, laidOutByProject: false, repositoriesStartedFromOrigin };
      }).pipe(
        Effect.onError(() =>
          Effect.gen(function* () {
            neverLaidOut.add(root);
            const candidates = layoutStarted
              ? taskPlan.trackedRepositories.map((repositoryPath) => ({
                  repositoryPath,
                  worktreePath: mapped(empty, repositoryPath),
                  name: path.basename(repositoryPath),
                }))
              : [...attempted.values()];
            for (const checkout of candidates) {
              if (!created.some((entry) => entry.worktreePath === checkout.worktreePath))
                created.push(checkout);
            }
            // Keep attempted members discoverable until their worktree registrations are reconciled.
            yield* store.put(current());
            yield* verifySources;
            const { kept } = yield* locked(
              empty,
              removeLocked(
                current(),
                {
                  force: false,
                  forget: true,
                  ...(layout === null ? { deleteBranch: branch } : {}),
                  projectCommands: "request-not-permitted",
                },
                hadBranch,
                false,
                branch,
              ),
            );
            if (kept.length > 0)
              yield* Effect.logWarning("A failed task folder needs reconciliation", { root, kept });
          }).pipe(Effect.ignoreCause({ log: true })),
        ),
        Effect.mapError((cause) =>
          isTaskFolderError(cause)
            ? cause
            : fail("create", "Could not create the worktrees")(cause),
        ),
      );
    },
  );

  /** The project's setting for where new worktrees start. */
  const startsFromOrigin = (operation: TaskFolderError["operation"], projectId: ProjectId) =>
    settings.getSettings.pipe(
      Effect.mapError(fail(operation, "Could not read settings")),
      Effect.map(
        (current) =>
          resolveProjectSettings(current, projectId).settings.newWorktreesStartFromOrigin,
      ),
    );

  const createForProjectFolder: TaskFolders["Service"]["createForProjectFolder"] = Effect.fn(
    "TaskFolders.createForProjectFolder",
  )(function* (input, { projectCommands }) {
    if (input.path !== null || input.newRefName === undefined) return null;
    const cwd = path.resolve(input.cwd);
    const project = yield* projects
      .findActiveByWorkspaceRoot(cwd)
      .pipe(Effect.mapError(fail("plan", "Could not read the project")));
    let projectId: ProjectId;
    if (Option.isSome(project)) projectId = project.value.projectId;
    else {
      const matching = yield* Effect.gen(function* () {
        const real = yield* canonicalLocation(cwd);
        return yield* Effect.filter(yield* projects.listShells(), (shell) =>
          canonicalLocation(path.resolve(shell.workspaceRoot)).pipe(
            Effect.map((root) => samePath(root, real)),
          ),
        );
      }).pipe(Effect.mapError(fail("plan", "Could not verify the project's workspace root")));
      if (matching.length === 0) return null;
      if (matching.length > 1)
        return yield* new TaskFolderError({
          operation: "plan",
          detail: "Several projects match the worktree source.",
        });
      projectId = matching[0]!.id;
    }
    const taskPlan = yield* plan({ projectId });
    if (taskPlan === null) return null;
    const { taskFolder, branch } = yield* create({
      plan: taskPlan,
      branch: input.newRefName,
      projectStart: { refName: input.refName, baseRefName: input.baseRefName ?? input.refName },
      baseBranch: input.baseRefName ?? input.refName,
      startFromOrigin: yield* startsFromOrigin("create", projectId),
      projectCommands,
    });
    return { worktree: { path: taskFolder.projectPath, refName: branch } };
  });

  const addRepository: TaskFolders["Service"]["addRepository"] = Effect.fn(
    "TaskFolders.addRepository",
  )(function* (input) {
    const refuse = (detail: string) => new TaskFolderError({ operation: "add-repository", detail });
    const thread = yield* projections
      .getThreadShell(input.threadId)
      .pipe(Effect.mapError(fail("add-repository", "Could not read the thread")));
    if (
      thread === null ||
      thread.deletedAt != null ||
      thread.archivedAt != null ||
      thread.worktreePath === null ||
      thread.branch === null
    )
      return yield* refuse("This thread does not work in a worktree.");
    const recordedBranch = thread.branch;
    const startFromOrigin =
      input.startFromOrigin ?? (yield* startsFromOrigin("add-repository", thread.projectId));
    const found = yield* requireTaskFolder("add-repository", thread.worktreePath);
    return yield* locked(
      found,
      Effect.gen(function* () {
        // Read again under the lease: another add may have landed.
        const taskFolder = yield* requireTaskFolder("add-repository", found.root);
        const live = yield* projections
          .getThreadShell(input.threadId)
          .pipe(Effect.mapError(fail("add-repository", "Could not recheck the thread")));
        if (
          live === null ||
          live.deletedAt != null ||
          live.archivedAt != null ||
          live.worktreePath !== thread.worktreePath ||
          live.branch !== recordedBranch ||
          live.projectId !== taskFolder.projectId
        )
          return yield* refuse("The thread's task folder binding changed.");
        const protectedPath = yield* registeredPathReason(taskFolder.root);
        if (protectedPath !== null) return yield* refuse(protectedPath);
        // The thread's record lags a rename of its branch, so a checkout says which one it is.
        const checkedOut = taskFolder.projectIsWorktree
          ? taskFolder.projectPath
          : taskFolder.repositories[0]?.worktreePath;
        const branch =
          checkedOut === undefined
            ? recordedBranch
            : yield* git
                .currentBranch(checkedOut)
                .pipe(
                  Effect.mapError(fail("add-repository", "Could not verify the worktree branch")),
                );
        if (branch === null) return yield* refuse("The worktree no longer has a branch.");
        yield* assertReady({ taskFolder, branch });
        const project = yield* readProject("add-repository", taskFolder.projectId);
        if (!samePath(project.workspaceRoot, taskFolder.projectRoot))
          return yield* refuse("The project checkout changed since the task folder was created.");
        const repositoryPath = path.resolve(input.repositoryPath);
        const folder = (yield* trackedFolders(project)).find(
          (candidate) => path.resolve(candidate.path) === repositoryPath,
        );
        if (folder === undefined)
          return yield* refuse(`"${input.repositoryPath}" is not a tracked folder of the project.`);
        if (!(yield* isRepository(repositoryPath)))
          return yield* refuse(`"${input.repositoryPath}" is not a Git repository.`);
        if (yield* isSubmodule(repositoryPath))
          return yield* refuse(`${folder.name} is a submodule, so it comes with its repository.`);
        if (taskFolder.repositories.some((entry) => entry.repositoryPath === repositoryPath))
          return yield* refuse(`${folder.name} already has a worktree in this thread.`);
        if (!isWithinRoot(path, repositoryPath, taskFolder.anchor))
          return yield* refuse(
            `${folder.name} lies outside the folders this thread's worktree was laid out for. Start a new worktree thread to include it.`,
          );
        const worktreePath = mapped(taskFolder, repositoryPath);
        const verifyPath = fail("add-repository", "Could not verify the repository source");
        const realRepository = yield* fileSystem
          .realPath(repositoryPath)
          .pipe(Effect.mapError(verifyPath));
        const realRoot = yield* fileSystem
          .realPath(taskFolder.root)
          .pipe(Effect.mapError(verifyPath));
        if (isWithinRoot(path, realRepository, realRoot))
          return yield* refuse("The repository source is inside this task folder.");
        const existing = (yield* git
          .listLocalBranchNames(repositoryPath)
          .pipe(Effect.mapError(fail("add-repository", "Could not list branches")))).includes(
          branch,
        );
        const start = existing
          ? null
          : yield* repositoryStart(repositoryPath, startFromOrigin).pipe(
              Effect.mapError(fail("add-repository", "Could not resolve the repository base")),
            );
        const refName = start?.refName ?? branch;
        if (
          !(yield* git
            .hasCommit({ cwd: repositoryPath, refName })
            .pipe(Effect.mapError(verifyPath)))
        )
          return yield* refuse(`The repository ${folder.name} has no commit at its worktree base.`);
        yield* assertDestination(taskFolder.root, taskFolder.projectPath, "add-repository");
        const insideLinkedProject =
          !taskFolder.projectIsWorktree &&
          isWithinRoot(path, repositoryPath, taskFolder.projectRoot) &&
          repositoryPath !== taskFolder.projectRoot;
        const wasLinked =
          insideLinkedProject &&
          Option.isSome(yield* fileSystem.readLink(worktreePath).pipe(Effect.option));
        const opened = insideLinkedProject
          ? yield* openLinkedPath(
              taskFolder.projectRoot,
              taskFolder.projectPath,
              path.relative(taskFolder.projectRoot, repositoryPath),
            ).pipe(Effect.mapError(fail("add-repository", "Could not prepare the folder")))
          : true;
        if (!opened || (yield* occupied(worktreePath)))
          return yield* refuse(`${worktreePath} already holds files, so no worktree can go there.`);
        yield* assertDestination(taskFolder.root, worktreePath, "add-repository");
        // The link the worktree took the place of comes back when the worktree does not stay.
        const relink = wasLinked
          ? // Git may or may not have left an empty folder where the link was.
            removeEmptyDirectory(worktreePath).pipe(
              Effect.ignore,
              Effect.andThen(fileSystem.symlink(repositoryPath, worktreePath)),
              Effect.ignoreCause({ log: true }),
            )
          : Effect.void;
        let issued = false;
        const checkout = { repositoryPath, worktreePath, name: folder.name };
        const updated: TaskFolder = {
          ...taskFolder,
          repositories: [...taskFolder.repositories, checkout],
        };
        yield* Effect.gen(function* () {
          yield* fileSystem.makeDirectory(path.dirname(worktreePath), { recursive: true });
          yield* assertDestination(taskFolder.root, worktreePath, "add-repository");
          if (!samePath(realRepository, yield* fileSystem.realPath(repositoryPath)))
            return yield* refuse("The repository source changed.");
          yield* store.put(updated);
          issued = true;
          yield* git.createWorktree({
            cwd: repositoryPath,
            refName,
            path: worktreePath,
            ...(start === null ? {} : { newRefName: branch, baseRefName: start.baseRefName }),
          });
          const reason = yield* unadoptable(checkout, branch);
          if (reason !== null) return yield* refuse(reason);
        }).pipe(
          Effect.onError(() => (issued ? Effect.void : relink)),
          Effect.mapError(fail("add-repository", `Could not create the ${folder.name} worktree`)),
        );
        return updated;
      }),
    );
  });

  const remove: TaskFolders["Service"]["remove"] = Effect.fnUntraced(function* (input) {
    const taskFolder = yield* store.findContaining(input.root);
    if (taskFolder === null || taskFolder.root !== input.root) return { removed: [], kept: [] };
    const owner = yield* teardownReceipts.readOwner(input.root);
    const changed = () =>
      new TaskFolderError({
        operation: "remove",
        detail: "The task folder binding changed.",
      });
    if (owner === null || !TaskFolderTeardown.sameFolder(owner.folder, taskFolder))
      return yield* changed();
    return yield* locked(
      taskFolder,
      Effect.gen(function* () {
        const current = yield* teardownReceipts.readOwner(input.root);
        if (current === null || !TaskFolderTeardown.sameOwner(owner, current))
          return yield* changed();
        return yield* removeLocked(current.folder, input);
      }),
    );
  });

  const removeIfTaskFolder: TaskFolders["Service"]["removeIfTaskFolder"] = Effect.fn(
    "TaskFolders.removeIfTaskFolder",
  )(function* (input) {
    const taskFolder = yield* store.findContaining(input.path);
    if (taskFolder === null) return false;
    const unsafePath = fail("remove", "Could not verify the task folder removal target");
    const target = yield* canonicalLocation(path.resolve(input.path)).pipe(
      Effect.mapError(unsafePath),
    );
    if (
      !samePath(
        target,
        yield* canonicalLocation(taskFolder.root).pipe(Effect.mapError(unsafePath)),
      ) &&
      !samePath(
        target,
        yield* canonicalLocation(taskFolder.projectPath).pipe(Effect.mapError(unsafePath)),
      )
    ) {
      return yield* new TaskFolderError({
        operation: "remove",
        detail:
          "This checkout is inside a task folder. Remove the task folder through its project path.",
      });
    }
    const { kept } = yield* remove({
      root: taskFolder.root,
      force: input.force,
      forget: true,
      projectCommands: input.projectCommands,
    });
    if (kept.length > 0) {
      return yield* new TaskFolderError({
        operation: "remove",
        detail: `Kept ${kept.map((entry) => `${entry.path} (${entry.reason})`).join(", ")}.`,
      });
    }
    return true;
  });

  const removeForProjectFolder: TaskFolders["Service"]["removeForProjectFolder"] = Effect.fn(
    "TaskFolders.removeForProjectFolder",
  )(function* (input, projectCommands) {
    const target = path.resolve(input.cwd, input.path);
    if (yield* removeIfTaskFolder({ path: target, force: input.force === true, projectCommands }))
      return;
    yield* withWorkspaceLease(
      target,
      Effect.gen(function* () {
        const block = yield* removalBlocked(target);
        if (block !== null)
          return yield* new TaskFolderError({ operation: "remove", detail: block });
        const real = yield* canonicalLocation(target).pipe(
          Effect.mapError(fail("remove", "Could not verify the removal target")),
        );
        for (const folder of yield* store.list) {
          const folderRoot = yield* canonicalLocation(folder.root).pipe(
            Effect.mapError(fail("remove", "Could not verify the task folder root")),
          );
          if (isWithinRoot(path, folderRoot, real) || isWithinRoot(path, real, folderRoot))
            return yield* new TaskFolderError({
              operation: "remove",
              detail: "This checkout overlaps a recorded task folder.",
            });
        }
        const repositoryPath = yield* git.repositoryRoot(input.cwd);
        if (repositoryPath === null)
          return yield* new TaskFolderError({
            operation: "remove",
            detail: "The removal request has no Git repository.",
          });
        const checkout = { repositoryPath, worktreePath: target };
        const reason = yield* checkoutRemovalReason(checkout);
        if (reason !== null)
          return yield* new TaskFolderError({ operation: "remove", detail: reason });
        yield* removeCheckout(checkout);
      }).pipe(
        Effect.mapError((cause) =>
          isTaskFolderError(cause) || isGitCommandError(cause)
            ? cause
            : fail("remove", "Could not verify the checkout")(cause),
        ),
      ),
    );
  });

  const renameBranch: TaskFolders["Service"]["renameBranch"] = Effect.fn(
    "TaskFolders.renameBranch",
  )(function* (input) {
    const taskFolder = yield* requireTaskFolder("create", input.root);
    return yield* locked(
      taskFolder,
      Effect.gen(function* () {
        yield* assertReady({ taskFolder, branch: input.oldBranch });
        const cwds = [
          ...(taskFolder.projectIsWorktree ? [taskFolder.projectPath] : []),
          ...taskFolder.repositories.map((repository) => repository.worktreePath),
        ];
        if (cwds.length === 0 || input.oldBranch === input.newBranch)
          return { branch: input.oldBranch };
        const renameFailed = fail("create", "Could not rename the branch");
        const branch =
          input.exactName === true
            ? input.newBranch
            : yield* freeBranchName(
                "create",
                input.newBranch,
                (yield* branchNames(cwds).pipe(Effect.mapError(renameFailed))).map((list) =>
                  list.filter((existing) => existing !== input.oldBranch),
                ),
              );
        const renamed: string[] = [];
        yield* Effect.forEach(
          cwds,
          (cwd) =>
            git
              .renameBranch({ cwd, oldBranch: input.oldBranch, newBranch: branch, exactName: true })
              .pipe(Effect.tap(() => Effect.sync(() => renamed.push(cwd)))),
          { discard: true },
        ).pipe(
          // One name everywhere or the old one everywhere.
          Effect.onError(() =>
            Effect.forEach(
              renamed,
              (cwd) =>
                git
                  .renameBranch({
                    cwd,
                    oldBranch: branch,
                    newBranch: input.oldBranch,
                    exactName: true,
                  })
                  .pipe(Effect.ignoreCause({ log: true })),
              { discard: true },
            ),
          ),
          Effect.mapError(renameFailed),
        );
        return { branch };
      }),
    );
  });

  const assertReady: TaskFolders["Service"]["assertReady"] = Effect.fn("TaskFolders.assertReady")(
    function* ({ taskFolder: expected, branch }) {
      yield* locked(
        expected,
        Effect.gen(function* () {
          const taskFolder = yield* requireTaskFolder("create", expected.root);
          const refuse = (detail: string) => new TaskFolderError({ operation: "create", detail });
          if (
            taskFolder.root !== expected.root ||
            taskFolder.projectId !== expected.projectId ||
            taskFolder.projectPath !== expected.projectPath ||
            taskFolder.projectRoot !== expected.projectRoot ||
            taskFolder.projectIsWorktree !== expected.projectIsWorktree ||
            taskFolder.repositories.length !== expected.repositories.length ||
            taskFolder.repositories.some(
              (repository, index) =>
                repository.worktreePath !== expected.repositories[index]?.worktreePath ||
                repository.repositoryPath !== expected.repositories[index]?.repositoryPath,
            )
          )
            return yield* refuse("The task folder binding changed during recovery.");
          const project = yield* readProject("create", taskFolder.projectId);
          if (!samePath(project.workspaceRoot, taskFolder.projectRoot))
            return yield* refuse("The task folder's project checkout changed during recovery.");
          yield* assertDestination(taskFolder.root, taskFolder.projectPath, "create");
          if ((yield* lstatIfPresent(taskFolder.projectPath)) === null)
            return yield* refuse(
              "Task folder recovery is held because the project folder is missing.",
            );
          const checkouts = [
            ...taskFolder.repositories,
            ...(taskFolder.projectIsWorktree
              ? [{ repositoryPath: taskFolder.projectRoot, worktreePath: taskFolder.projectPath }]
              : []),
          ];
          for (const checkout of checkouts) {
            yield* assertDestination(taskFolder.root, checkout.worktreePath, "create");
            const reason = yield* unadoptable(checkout, branch);
            if (reason !== null) return yield* refuse(`Task folder recovery is held: ${reason}.`);
          }
        }).pipe(
          Effect.mapError((cause) =>
            isTaskFolderError(cause)
              ? cause
              : fail("create", "Could not verify task folder recovery")(cause),
          ),
        ),
      );
    },
  );

  const layoutCommands: TaskFolders["Service"]["layoutCommands"] = Effect.fn(
    "TaskFolders.layoutCommands",
  )(function* (projectId) {
    const project = yield* readProject("read", projectId);
    const layout = yield* worktreeLayout(path.resolve(project.workspaceRoot));
    const declared = (["create", "remove"] as const).flatMap((kind) => {
      const command = layout?.[kind];
      return command === undefined ? [] : [{ kind, command }];
    });
    return {
      commands: yield* Effect.forEach(declared, ({ kind, command }) =>
        approvals
          .isApproved({ projectId, kind, command })
          .pipe(Effect.map((approved) => ({ kind, command, approved }))),
      ),
    };
  });

  return TaskFolders.of({
    plan,
    create,
    createForProjectFolder,
    addRepository,
    find: store.findContaining,
    remove,
    removeIfTaskFolder,
    removeForProjectFolder,
    registeredPathReason,
    renameBranch,
    restore: (input) => assertReady(input),
    assertReady,
    layoutCommands,
  });
});

export const layer = Layer.effect(TaskFolders, make);
