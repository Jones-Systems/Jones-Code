import { type TaskFolder, TaskFolderError, type ThreadId } from "@t3tools/contracts";
import { importedHistoryCanonicalJson as canonical } from "@t3tools/shared/jones/importedHistoryCanonical";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Option from "effect/Option";
import * as Path from "effect/Path";
import type * as PlatformError from "effect/PlatformError";

import * as ProjectStore from "../../orchestration-v2/ProjectStore.ts";
import { captureRuntimeUseWorkspace } from "../cleanup/RuntimeUseWorkspace.ts";
import {
  type Capture,
  type Checkout,
  type Directory,
  type FolderOwner,
  ownerMatches,
  type ReceiptStore,
} from "./ReceiptStore.ts";

type CheckoutPaths = Pick<Checkout, "worktreePath" | "repositoryPath">;
export const ownerChanged = (cause?: unknown) =>
  new TaskFolderError({ operation: "remove", detail: "The layout teardown owner changed.", cause });
const unequal = (left: unknown, right: unknown) => canonical(left) !== canonical(right);
export const sameFolder = (left: TaskFolder, right: TaskFolder) => !unequal(left, right);
export const sameOwner = (left: FolderOwner, right: FolderOwner) => !unequal(left, right);

interface LayoutCommand {
  readonly commandHash: string;
  readonly shell: string;
  readonly branch: string;
  readonly recheck: Effect.Effect<unknown, TaskFolderError>;
  readonly run: Effect.Effect<number, TaskFolderError>;
}

type Preparation = { readonly capture: Capture | null } | { readonly held: string };

export const make = Effect.fnUntraced(function* (input: {
  readonly receipts: ReceiptStore;
  readonly linkedGitDir: (checkout: CheckoutPaths) => Effect.Effect<string | null>;
  readonly commonGitDir: (repository: string) => Effect.Effect<string>;
  readonly canonicalLocation: (path: string) => Effect.Effect<string, PlatformError.PlatformError>;
  readonly assertDestination: (root: string, path: string) => Effect.Effect<void, TaskFolderError>;
  readonly hasRegistration: (checkout: CheckoutPaths) => Effect.Effect<boolean, TaskFolderError>;
}) {
  const fileSystem = yield* FileSystem.FileSystem;
  const path = yield* Path.Path;
  const projects = yield* ProjectStore.ProjectStoreV2;
  const { receipts } = input;

  const directory = Effect.fnUntraced(function* (target: string) {
    const real = yield* fileSystem.realPath(target);
    const { canonicalPath, directoryIdentity } = yield* captureRuntimeUseWorkspace(real);
    if (BigInt(directoryIdentity.birth) <= 0n || (yield* fileSystem.realPath(target)) !== real)
      return yield* ownerChanged();
    return { canonicalPath, directoryIdentity };
  }, Effect.mapError(ownerChanged));

  const present = (target: string) =>
    fileSystem.lstat(target).pipe(
      Effect.map(() => true),
      Effect.catchReason("PlatformError", "NotFound", () => Effect.succeed(false)),
      Effect.mapError(ownerChanged),
    );

  const projectBirth = Effect.fnUntraced(function* ({ folder }: FolderOwner) {
    const project = yield* projects.get(folder.projectId).pipe(Effect.mapError(ownerChanged));
    if (
      Option.isNone(project) ||
      path.relative(project.value.workspaceRoot, folder.projectRoot) !== ""
    )
      return yield* ownerChanged();
    return project.value.createdAt;
  });

  const checkoutIdentity = Effect.fnUntraced(function* (checkout: CheckoutPaths) {
    const admin = yield* input.linkedGitDir(checkout);
    if (admin === null) return yield* ownerChanged();
    const head = (yield* fileSystem
      .readFileString(path.join(admin, "HEAD"))
      .pipe(Effect.mapError(ownerChanged))).trim();
    if (!head.startsWith("ref: refs/heads/")) return yield* ownerChanged();
    return {
      worktreePath: checkout.worktreePath,
      repositoryPath: checkout.repositoryPath,
      directory: yield* directory(checkout.worktreePath),
      repository: yield* directory(checkout.repositoryPath),
      admin: yield* directory(admin),
      common: yield* directory(yield* input.commonGitDir(checkout.repositoryPath)),
      head,
    } satisfies Checkout;
  });

  const capture = Effect.fn("TaskFolderTeardown.capture")(function* (
    owner: FolderOwner,
    checkouts: ReadonlyArray<CheckoutPaths>,
    command: Pick<Capture, "commandHash" | "shell" | "branch" | "ownerThreadId">,
  ) {
    const { folder } = owner;
    const root = yield* directory(folder.root);
    if (root.canonicalPath !== owner.realRoot) return yield* ownerChanged();
    return {
      version: 1,
      owner,
      projectCreatedAt: yield* projectBirth(owner),
      root,
      projectRoot: yield* directory(folder.projectRoot),
      projectPath: yield* directory(folder.projectPath),
      checkouts: yield* Effect.forEach(checkouts, checkoutIdentity),
      ...command,
    } satisfies Capture;
  });

  // Missing targets are allowed only after a proved exit. For a missing worktree,
  // TaskFolders still verifies that Git's registration is gone before forgetting it.
  const assertDirectory = Effect.fnUntraced(function* (
    target: string,
    expected: Directory,
    allowMissing: boolean,
  ) {
    if (yield* present(target)) {
      if (unequal(yield* directory(target), expected)) return yield* ownerChanged();
    } else if (
      !allowMissing ||
      (yield* input.canonicalLocation(target).pipe(Effect.mapError(ownerChanged))) !==
        expected.canonicalPath
    ) {
      return yield* ownerChanged();
    }
  });

  const assertCheckout = Effect.fn("TaskFolderTeardown.assertCheckout")(function* (
    original: Checkout,
    allowMissing: boolean,
  ) {
    if (yield* present(original.worktreePath)) {
      if (unequal(yield* checkoutIdentity(original), original)) return yield* ownerChanged();
      return true;
    }
    yield* assertDirectory(original.worktreePath, original.directory, allowMissing);
    yield* assertDirectory(original.repositoryPath, original.repository, false);
    yield* assertDirectory(
      yield* input.commonGitDir(original.repositoryPath),
      original.common,
      false,
    );
    return false;
  });

  const assertOwner = Effect.fn("TaskFolderTeardown.assertOwner")(function* (
    original: Capture,
    ownerThreadId: ThreadId | undefined,
    phase: "issue" | "cleanup",
  ) {
    const { folder } = original.owner;
    const current = yield* receipts.readOwner(folder.root);
    if (
      (ownerThreadId ?? null) !== original.ownerThreadId ||
      current === null ||
      (phase === "issue"
        ? unequal(current, original.owner)
        : !ownerMatches(current, original.owner)) ||
      (yield* projectBirth(original.owner)) !== original.projectCreatedAt
    )
      return yield* ownerChanged();
    yield* assertDirectory(folder.projectRoot, original.projectRoot, false);
    yield* assertDirectory(folder.root, original.root, phase === "cleanup");
    yield* assertDirectory(folder.projectPath, original.projectPath, phase === "cleanup");
    return current;
  });

  const assertGroup = Effect.fn("TaskFolderTeardown.assertGroup")(function* (
    original: Capture,
    ownerThreadId: ThreadId | undefined,
    phase: "issue" | "cleanup",
  ) {
    const current = yield* assertOwner(original, ownerThreadId, phase);
    const recorded = new Set(current.folder.repositories.map((member) => member.worktreePath));
    if (current.folder.projectIsWorktree) recorded.add(current.folder.projectPath);
    yield* input.assertDestination(current.folder.root, current.folder.projectPath);
    for (const member of original.checkouts) {
      yield* input.assertDestination(current.folder.root, member.worktreePath);
      const present = yield* assertCheckout(member, phase === "cleanup");
      if (!recorded.has(member.worktreePath) && (present || (yield* input.hasRegistration(member))))
        return yield* ownerChanged();
    }
  });

  // A committed claim without a recorded zero exit is unknown, including
  // interruption before the process's start or during its completion delivery.
  const issue = Effect.fn("TaskFolderTeardown.issue")(function* (
    snapshot: Capture,
    run: Effect.Effect<number, TaskFolderError>,
  ) {
    yield* Effect.uninterruptibleMask((restore) =>
      Effect.gen(function* () {
        const claimId = yield* receipts.claim(snapshot);
        if ((yield* restore(run)) !== 0)
          return yield* new TaskFolderError({
            operation: "remove",
            detail: "The layout teardown did not report a successful exit.",
          });
        yield* receipts.succeed(snapshot.owner.folder.root, claimId);
      }),
    );
  });

  const prepare = Effect.fn("TaskFolderTeardown.prepare")(function* (request: {
    readonly folder: TaskFolder;
    readonly checkouts: ReadonlyArray<CheckoutPaths>;
    readonly ownerThreadId: ThreadId | undefined;
    readonly expectedBranch: string | undefined;
    readonly approvedCommand: Effect.Effect<LayoutCommand | null, TaskFolderError>;
    readonly allGo: Effect.Effect<boolean>;
    readonly inTurn: <A>(
      effect: Effect.Effect<A, TaskFolderError>,
    ) => Effect.Effect<A, TaskFolderError>;
  }): Effect.fn.Return<Preparation, TaskFolderError> {
    const { folder, checkouts, ownerThreadId, expectedBranch, approvedCommand, allGo, inTurn } =
      request;
    const owner = yield* receipts.readOwner(folder.root);
    if (owner === null || !sameFolder(folder, owner.folder))
      return { held: "task folder binding changed" };
    // A permission or command change does not resolve a previously claimed effect.
    const previous = yield* receipts.read(owner);
    const unknown = { held: "layout teardown outcome requires reconciliation" };
    if (previous?.outcome === "unknown") return unknown;

    let snapshot: Capture;
    if (previous !== null) {
      snapshot = previous.capture;
    } else {
      const command = yield* approvedCommand;
      if (command === null || !(yield* allGo)) return { capture: null };
      if (!(yield* receipts.available))
        return { held: "layout teardown receipt storage is unavailable" };
      snapshot = yield* capture(owner, checkouts, {
        commandHash: command.commandHash,
        shell: command.shell,
        branch: command.branch,
        ownerThreadId: ownerThreadId ?? null,
      });
      if (
        expectedBranch !== undefined &&
        snapshot.checkouts.some((member) => member.head !== `ref: refs/heads/${expectedBranch}`)
      )
        return { held: "the worktree branch changed" };

      const result = yield* inTurn(
        Effect.gen(function* () {
          yield* assertGroup(snapshot, ownerThreadId, "issue");
          if (!(yield* allGo)) return false;
          yield* command.recheck;
          yield* issue(
            snapshot,
            Effect.gen(function* () {
              if (!(yield* allGo)) return yield* ownerChanged();
              yield* assertGroup(snapshot, ownerThreadId, "issue");
              return yield* command.run;
            }),
          );
          return true;
        }),
      ).pipe(Effect.result);
      if (result._tag === "Failure") return unknown;
      if (!result.success) return { capture: null };
    }

    const verified = yield* assertGroup(snapshot, ownerThreadId, "cleanup").pipe(Effect.result);
    return verified._tag === "Success"
      ? { capture: snapshot }
      : { held: "layout teardown owner changed" };
  });

  return { prepare, assertOwner, assertCheckout };
});
