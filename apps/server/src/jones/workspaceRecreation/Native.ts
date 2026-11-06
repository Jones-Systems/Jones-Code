// @effect-diagnostics nodeBuiltinImport:off -- Exact native Git recreation and its disposable conformance fixture own and await each subprocess.
import * as FSP from "node:fs/promises";
import * as Path from "node:path";
import * as OS from "node:os";
import * as Crypto from "node:crypto";
import { canonical, exists, git, readBounded, sha256 } from "../workspaceRetirement/Filesystem.ts";
import type { Binding } from "../workspaceRetirement/OutputAttestation.ts";

export type HoldReason =
  | "branch_changed"
  | "branch_in_use"
  | "stale_registration"
  | "unknown"
  | "native_recreation_unsupported";
export class RecreationHold extends Error {
  readonly _tag = "RecreationHold";
  readonly reason: HoldReason;
  readonly target: string;
  readonly owner: string;
  readonly missingFact: string;
  readonly nextAction: string;
  constructor(
    reason: HoldReason,
    target: string,
    owner: string,
    missingFact: string,
    nextAction: string,
    options?: ErrorOptions,
  ) {
    super(
      `Checkout recreation held (${reason}): ${target}; owner ${owner}; ${missingFact}. ${nextAction}`,
      options,
    );
    this.reason = reason;
    this.target = target;
    this.owner = owner;
    this.missingFact = missingFact;
    this.nextAction = nextAction;
  }
}
export interface RecreationSource {
  readonly id: string;
  readonly incarnation: string;
  readonly binding: Binding;
  readonly head: string;
  readonly branch: string | null;
  readonly capturedConfig?: Readonly<Record<string, string>>;
  readonly sparsePatterns?: string;
}
export interface RecreationInput {
  readonly cwd: string;
  readonly source: RecreationSource;
  readonly owner: string;
  readonly alternatePath: string;
  readonly detached?: boolean;
  readonly nativeGuarded?: boolean;
}
export interface RecreationResult {
  readonly path: string;
  readonly branch: string | null;
  readonly head: string;
  readonly breadcrumb: string;
  readonly predecessor: string;
  readonly journal: string;
}
interface Registration {
  path: string;
  head: string;
  branch: string | null;
  locked: boolean;
}
async function registrations(cwd: string): Promise<Registration[]> {
  const raw = (await git(cwd, ["worktree", "list", "--porcelain", "-z"])).toString();
  return raw
    .split("\0\0")
    .filter(Boolean)
    .map((block) => {
      const fields = block.split("\0");
      return {
        path: fields.find((f) => f.startsWith("worktree "))?.slice(9) ?? "",
        head: fields.find((f) => f.startsWith("HEAD "))?.slice(5) ?? "",
        branch: fields.find((f) => f.startsWith("branch "))?.slice(7) ?? null,
        locked: fields.some((f) => f === "locked" || f.startsWith("locked ")),
      };
    });
}
const oid = (head: string) => /^(?:[0-9a-f]{40}|[0-9a-f]{64})$/.test(head);
async function commit(cwd: string, ref: string) {
  try {
    return (await git(cwd, ["rev-parse", "--verify", "--end-of-options", `${ref}^{commit}`]))
      .toString()
      .trim();
  } catch {
    return null;
  }
}
async function branchCommit(cwd: string, ref: string) {
  try {
    await git(cwd, ["show-ref", "--verify", "--quiet", ref]);
  } catch (cause) {
    if ((cause as { code?: unknown }).code === 1) return null;
    throw cause;
  }
  const head = await commit(cwd, ref);
  if (head === null) throw new Error("Existing branch commit is unavailable");
  return head;
}
async function durableFile(path: string, value: unknown, exclusive = false, literal = false) {
  const file = await FSP.open(path, exclusive ? "wx" : "w", 0o600);
  try {
    await file.writeFile(literal ? String(value) : canonical(value) + "\n");
    await file.sync();
  } finally {
    await file.close();
  }
  const directory = await FSP.open(Path.dirname(path), "r");
  try {
    await directory.sync();
  } finally {
    await directory.close();
  }
}

export async function missingSource(cwd: string, path: string): Promise<RecreationSource> {
  const hold = (fact: string) =>
    new RecreationHold(
      "unknown",
      path,
      "missing checkout",
      fact,
      "Recover the exact deleted checkout HEAD and registration before recreating this target.",
    );
  if (await exists(path)) throw hold("checkout is present");
  const entry = (await registrations(cwd)).find((r) => r.path === path);
  const common = await FSP.realpath(
    (await git(cwd, ["rev-parse", "--path-format=absolute", "--git-common-dir"])).toString().trim(),
  );
  if (!entry) {
    const pointer = Path.join(
      common,
      "workspace-lifecycle",
      "recreations",
      `target-${sha256(path)}.json`,
    );
    if (await exists(pointer)) {
      const retained = JSON.parse(
        (await readBounded(pointer, 65536)).toString(),
      ) as RecreationSource;
      if (
        retained.binding.path === path &&
        retained.binding.git_common_dir === common &&
        oid(retained.head)
      )
        return retained;
    }
    throw hold("neither terminal receipt nor exact stale registration is available");
  }
  for (const name of await FSP.readdir(Path.join(common, "worktrees"))) {
    const admin = Path.join(common, "worktrees", name);
    if (
      (await readBounded(Path.join(admin, "gitdir"), 65536)).toString().trim() !==
      Path.join(path, ".git")
    )
      continue;
    const stat = await FSP.lstat(admin);
    const log = (await readBounded(Path.join(admin, "logs/HEAD"), 4 * 1024 * 1024)).toString();
    const head = log.trim().split("\n").at(-1)?.split(" ")[1];
    if (!head || !oid(head))
      throw hold("deleted checkout HEAD has no retained native reflog observation");
    const capturedConfig: Record<string, string> = {};
    const config = Path.join(admin, "config.worktree");
    if (await exists(config))
      for (const pair of (await git(cwd, ["config", "--null", "--file", config, "--list"]))
        .toString()
        .split("\0")
        .filter(Boolean)) {
        const [key, ...value] = pair.split("\n");
        if (
          !["core.sparsecheckout", "core.sparsecheckoutcone", "index.sparse"].includes(
            key!.toLowerCase(),
          )
        )
          throw hold("deleted checkout worktree configuration cannot be safely reapplied");
        capturedConfig[key!] = value.join("\n");
      }
    const patterns = Path.join(admin, "info/sparse-checkout");
    const sparsePatterns = (await exists(patterns))
      ? (await readBounded(patterns, 1024 * 1024)).toString()
      : undefined;
    // The branch can have moved since deletion; its current tip is never substituted for this HEAD.
    return {
      id: `missing:${sha256(canonical({ admin, dev: stat.dev, ino: stat.ino, log }))}`,
      incarnation: `missing:${sha256(canonical({ admin, dev: stat.dev, ino: stat.ino }))}`,
      binding: {
        path,
        dev: 0,
        ino: 0,
        git_dir: admin,
        git_dir_dev: stat.dev,
        git_dir_ino: stat.ino,
        git_common_dir: common,
        admin_id: name,
        breadcrumb_id: null,
      },
      head,
      branch: entry.branch,
      capturedConfig,
      ...(sparsePatterns === undefined ? {} : { sparsePatterns }),
    };
  }
  throw hold("registration admin directory unavailable");
}

// Probe this Git executable's exact unforced missing-path operation, never the owner's repository.
let conformance: Promise<boolean> | undefined;
async function probeMissingRemoval() {
  const root = await FSP.realpath(
    await FSP.mkdtemp(Path.join(OS.tmpdir(), "jones-recreation-conformance-")),
  );
  const identity = await FSP.lstat(root);
  try {
    const repo = Path.join(root, "repo"),
      first = Path.join(root, "first"),
      other = Path.join(root, "other");
    await FSP.mkdir(repo);
    await git(repo, ["init", "--quiet"]);
    await git(repo, [
      "-c",
      "user.name=Fixture",
      "-c",
      "user.email=fixture@example.invalid",
      "commit",
      "--allow-empty",
      "-qm",
      "fixture",
    ]);
    await git(repo, ["worktree", "add", "--detach", first, "HEAD"]);
    await git(repo, ["worktree", "add", "--detach", other, "HEAD"]);
    await FSP.rm(first, { recursive: true });
    await FSP.rm(other, { recursive: true });
    const before = await registrations(repo);
    await git(repo, ["worktree", "remove", "--", first]);
    const after = await registrations(repo);
    return canonical(after) === canonical(before.filter((r) => r.path !== first));
  } catch {
    return false;
  } finally {
    const current = await FSP.lstat(root);
    if (!current.isDirectory() || current.dev !== identity.dev || current.ino !== identity.ino)
      throw new Error(`Conformance scratch identity changed: ${root}`);
    await FSP.rm(root, { recursive: true, force: true });
    if (await exists(root)) throw new Error(`Conformance scratch cleanup unavailable: ${root}`);
  }
}

async function removeStale(input: RecreationInput) {
  const { source, owner, cwd } = input,
    target = source.binding.path;
  const hold = (fact: string, cause?: unknown) =>
    new RecreationHold(
      "stale_registration",
      target,
      owner,
      fact,
      "Preserve this checkout's exact registration and refs; qualify exact missing-path removal on this Git version before retrying.",
      { cause },
    );
  const before = await registrations(cwd),
    entry = before.find((r) => r.path === target);
  if (!entry) return;
  if (await exists(target)) throw hold("the registered path is occupied");
  if (entry.locked) throw hold("the exact registration is locked");
  const admin = source.binding.git_dir;
  if (Path.dirname(admin) !== Path.join(source.binding.git_common_dir, "worktrees"))
    throw hold("admin identity unavailable");
  const stat = await FSP.lstat(admin);
  if (
    !stat.isDirectory() ||
    stat.dev !== source.binding.git_dir_dev ||
    stat.ino !== source.binding.git_dir_ino
  )
    throw hold("admin identity changed");
  if (await exists(Path.join(admin, "modules")))
    throw hold("submodule admin data requires its native owner");
  if (
    (await readBounded(Path.join(admin, "gitdir"), 65536)).toString().trim() !==
    Path.join(target, ".git")
  )
    throw hold("registration does not name the exact missing path");
  const logs = new Map<string, Buffer>();
  let logBytes = 0,
    logEntries = 0;
  async function collectLogs(directory: string) {
    if (!(await exists(directory))) return;
    if (!(await FSP.lstat(directory)).isDirectory())
      throw hold("reflog directory identity unavailable");
    for (const name of await FSP.readdir(directory)) {
      if (++logEntries > 10000) throw hold("reflog entry bound exceeded");
      const path = Path.join(directory, name),
        stat = await FSP.lstat(path);
      if (stat.isDirectory()) await collectLogs(path);
      else if (stat.isFile()) {
        const log = await readBounded(path, 4 * 1024 * 1024 - logBytes);
        logBytes += log.length;
        logs.set(path, log);
      } else throw hold("reflog contains unsupported entry");
    }
  }
  await collectLogs(Path.join(admin, "logs"));
  const tips = new Set([source.head, entry.head]);
  for (const log of logs.values())
    for (const line of log.toString().split("\n").filter(Boolean)) {
      const fields = line.split(" ");
      if (!oid(fields[0]!) || !oid(fields[1]!)) throw hold("HEAD reflog could not be decoded");
      for (const head of fields.slice(0, 2)) if (!/^0+$/.test(head!)) tips.add(head!);
    }
  for (const head of tips) {
    if (!oid(head) || (await commit(cwd, head)) !== head)
      throw hold("reflog commit object unavailable");
    const ref = `refs/jones/workspace-recreation/${sha256(source.id)}/${head}`;
    const current = await commit(cwd, ref);
    if (current !== null && current !== head) throw hold("preservation ref changed");
    if (current === null) await git(cwd, ["update-ref", ref, head, "0".repeat(head.length)]);
    if ((await commit(cwd, ref)) !== head) throw hold("preservation ref readback unavailable");
  }
  if (!(await (conformance ??= probeMissingRemoval())))
    throw hold("this Git has no proved exact missing-path removal support");
  const now = await FSP.lstat(admin);
  const originalLogs = new Map(logs);
  logs.clear();
  logBytes = 0;
  logEntries = 0;
  await collectLogs(Path.join(admin, "logs"));
  if (
    (await exists(target)) ||
    now.dev !== stat.dev ||
    now.ino !== stat.ino ||
    logs.size !== originalLogs.size ||
    [...originalLogs].some(([path, bytes]) => !logs.get(path)?.equals(bytes)) ||
    canonical(await registrations(cwd)) !== canonical(before)
  )
    throw hold("registration changed after preservation");
  let failure: unknown;
  try {
    await git(cwd, ["worktree", "remove", "--", target]);
  } catch (cause) {
    failure = cause;
  }
  // A lost command response is reconciled through readback, never repeated or replaced with prune.
  if (
    (await exists(target)) ||
    (await exists(admin)) ||
    canonical(await registrations(cwd)) !== canonical(before.filter((r) => r.path !== target))
  )
    throw new RecreationHold(
      "unknown",
      target,
      owner,
      "exact removal postcondition unavailable",
      "Reconcile this original registration removal through readback; do not repeat removal.",
      { cause: failure },
    );
}

export async function readBreadcrumb(gitDirectory: string): Promise<RecreationResult | null> {
  const path = Path.join(gitDirectory, "workspace-recreation.json");
  if (!(await exists(path))) return null;
  const value = JSON.parse((await readBounded(path, 65536)).toString()) as RecreationResult;
  if (
    !value ||
    typeof value.breadcrumb !== "string" ||
    !value.breadcrumb ||
    !oid(value.head) ||
    typeof value.predecessor !== "string" ||
    typeof value.path !== "string"
  )
    throw new Error("Invalid checkout recreation breadcrumb");
  return value;
}

const journalPath = (source: RecreationSource) =>
  Path.join(
    source.binding.git_common_dir,
    "workspace-lifecycle",
    "recreations",
    sha256(source.id) + ".json",
  );

async function createdReadback(cwd: string, result: RecreationResult) {
  const reg = (await registrations(cwd)).find((r) => r.path === result.path);
  if (reg && reg.head === result.head && reg.branch === result.branch) {
    const admin = (await git(result.path, ["rev-parse", "--absolute-git-dir"]))
      .toString()
      .trim();
    if (canonical(await readBreadcrumb(admin)) === canonical(result)) return result;
  }
  return null;
}

// A present directory is not completion evidence for an earlier native add/population attempt.
export async function knownRecreation(cwd: string, path: string): Promise<RecreationResult | null> {
  const repository = (await exists(Path.join(cwd, ".git"))) ? cwd : path;
  if (!(await exists(Path.join(repository, ".git")))) return null;
  const common = await FSP.realpath(
    (await git(repository, ["rev-parse", "--path-format=absolute", "--git-common-dir"]))
      .toString()
      .trim(),
  );
  const pointer = Path.join(
    common,
    "workspace-lifecycle",
    "recreations",
    `target-${sha256(path)}.json`,
  );
  if (!(await exists(pointer))) return null;
  const source: RecreationSource = JSON.parse((await readBounded(pointer, 65536)).toString());
  const journal = journalPath(source);
  const hold = () =>
    new RecreationHold(
      "unknown",
      path,
      "known checkout recreation",
      `prior recreation requires readback: ${journal}`,
      "Reconcile this original recreation journal before provider use; do not repeat creation.",
    );
  if (
    source.binding.path !== path ||
    source.binding.git_common_dir !== common ||
    !oid(source.head)
  )
    throw hold();
  const prior = JSON.parse((await readBounded(journal, 65536)).toString());
  if (canonical(prior.source) !== canonical(source)) throw hold();
  if (prior.result?.path !== path) return null;
  if (prior.state === "held") return null;
  if (
    prior.state === "created" &&
    prior.result.journal === journal &&
    prior.result.predecessor === source.incarnation &&
    prior.result.head === source.head
  ) {
    const result = await createdReadback(repository, prior.result);
    if (result !== null) return result;
  }
  throw hold();
}
export async function plannedPath(
  source: RecreationSource,
  alternatePath: string,
): Promise<string> {
  const journal = journalPath(source);
  const journalExists = await exists(journal);
  if (journalExists) {
    const prior = JSON.parse((await readBounded(journal, 65536)).toString());
    if (typeof prior.result?.path !== "string" || !Path.isAbsolute(prior.result.path))
      throw new Error("Invalid recreation journal path");
    if (prior.state !== "held") return prior.result.path;
  }
  return (await exists(source.binding.path)) ? alternatePath : source.binding.path;
}
export async function generationInvalidated(result: RecreationResult) {
  const path = result.journal + ".invalidated";
  return (
    (await exists(path)) &&
    (await readBounded(path, 4096)).toString() === canonical(result.breadcrumb) + "\n"
  );
}
export async function acknowledgeInvalidation(result: RecreationResult) {
  if (!(await generationInvalidated(result)))
    await durableFile(result.journal + ".invalidated", result.breadcrumb, true);
}

async function recreateNative(input: RecreationInput): Promise<RecreationResult> {
  const { source, cwd, owner } = input,
    saved = source.binding.path;
  const hold = (reason: HoldReason, fact: string, cause?: unknown) =>
    new RecreationHold(
      reason,
      saved,
      owner,
      fact,
      reason === "branch_changed"
        ? "Keep the current branch; explicitly request detached recreation at this receipt HEAD."
        : reason === "branch_in_use"
          ? "Release the branch through its current checkout owner, then retry this turn."
          : "Reconcile this exact receipt and recreation journal before another recreation attempt.",
      { cause },
    );
  if (input.nativeGuarded)
    throw hold(
      "native_recreation_unsupported",
      "native verified checkouts cannot use ordinary recreation",
    );
  if (
    !oid(source.head) ||
    !Path.isAbsolute(saved) ||
    Path.normalize(saved) !== saved ||
    !Path.isAbsolute(input.alternatePath) ||
    Path.normalize(input.alternatePath) !== input.alternatePath ||
    (await FSP.realpath(
      (await git(cwd, ["rev-parse", "--path-format=absolute", "--git-common-dir"]))
        .toString()
        .trim(),
    )) !== source.binding.git_common_dir
  )
    throw hold("unknown", "receipt common-store identity unavailable");
  const directory = Path.join(source.binding.git_common_dir, "workspace-lifecycle", "recreations");
  await FSP.mkdir(directory, { recursive: true });
  const journal = journalPath(source);
  const journalExists = await exists(journal);
  if (journalExists) {
    const prior = JSON.parse((await readBounded(journal, 65536)).toString());
    if (canonical(prior.source) !== canonical(source))
      throw hold("unknown", "receipt source changed after recreation intent");
    if (prior.state === "created") {
      const result = prior.result as RecreationResult;
      const observed = await createdReadback(cwd, result);
      if (observed !== null) return observed;
    }
    if (prior.state !== "held")
      throw hold("unknown", `prior recreation requires readback: ${journal}`);
  }
  if ((await commit(cwd, source.head)) !== source.head)
    throw hold("unknown", "receipt HEAD object unavailable");
  let branch = input.detached ? null : source.branch;
  if (branch !== null) {
    if (!branch.startsWith("refs/heads/"))
      throw hold("unknown", "receipt branch is not a local branch");
    const current = await branchCommit(cwd, branch);
    if (current === null) branch = null;
    else if (current !== source.head)
      throw hold("branch_changed", `saved ${source.head}; current ${current}`);
  }
  const path = (await exists(saved)) ? input.alternatePath : saved;
  if (await exists(path)) throw hold("unknown", "selected managed path is occupied");
  if (
    branch !== null &&
    (await registrations(cwd)).some((r) => r.branch === branch && r.path !== saved)
  )
    throw hold("branch_in_use", `branch ${branch} is registered in another checkout`);
  const result: RecreationResult = {
    path,
    branch,
    head: source.head,
    breadcrumb: Crypto.randomUUID(),
    predecessor: source.incarnation,
    journal,
  };
  await durableFile(journal, { state: "intent", source, owner, result }, !journalExists);
  await durableFile(Path.join(directory, `target-${sha256(saved)}.json`), source);
  try {
    if (!(await exists(saved))) await removeStale(input);
    if (branch !== null && (await registrations(cwd)).some((r) => r.branch === branch))
      throw hold("branch_in_use", `branch ${branch} is registered in another checkout`);
    if (branch !== null && (await branchCommit(cwd, branch)) !== source.head)
      throw hold("branch_changed", "branch moved before worktree add");
    const config = source.capturedConfig ?? {};
    if (
      Object.keys(config).some(
        (key) =>
          !["core.sparsecheckout", "core.sparsecheckoutcone", "index.sparse"].includes(
            key.toLowerCase(),
          ),
      )
    )
      throw hold("unknown", "receipt has unsupported worktree configuration");
    // No reset, -B, --force, or branch creation. Detached add never attaches a moved branch.
    await git(cwd, [
      "worktree",
      "add",
      "--no-checkout",
      ...(branch === null ? ["--detach"] : []),
      "--",
      path,
      branch === null ? source.head : branch.slice(11),
    ]);
    const admin = (await git(path, ["rev-parse", "--absolute-git-dir"])).toString().trim();
    if ((await git(path, ["rev-parse", "HEAD"])).toString().trim() !== source.head)
      throw hold("unknown", "new checkout HEAD changed during creation");
    for (const [key, value] of Object.entries(config))
      await git(path, ["config", "--file", Path.join(admin, "config.worktree"), key, value]);
    if (source.sparsePatterns !== undefined) {
      await FSP.mkdir(Path.join(admin, "info"), { recursive: true });
      await durableFile(
        Path.join(admin, "info/sparse-checkout"),
        source.sparsePatterns,
        false,
        true,
      );
    }
    await git(path, ["-c", "advice.detachedHead=false", "read-tree", "-mu", "HEAD"]);
    const reg = (await registrations(cwd)).find((r) => r.path === path);
    if (!reg || reg.head !== source.head || reg.branch !== branch)
      throw hold("unknown", "new checkout registration changed");
    await durableFile(Path.join(admin, "workspace-recreation.json"), result, true);
    await durableFile(journal, { state: "created", source, owner, result });
    return result;
  } catch (cause) {
    // These pre-add holds created no checkout. Preserved refs and exact stale removal remain recorded by this source journal.
    if (
      cause instanceof RecreationHold &&
      ["branch_changed", "branch_in_use", "stale_registration"].includes(cause.reason)
    ) {
      await durableFile(journal, { state: "held", source, owner, result, reason: cause.reason });
      throw cause;
    }
    throw hold("unknown", `recreation effect requires exact readback: ${journal}`, cause);
  }
}

export async function recreate(input: RecreationInput): Promise<RecreationResult> {
  let lock: Awaited<ReturnType<typeof FSP.open>> | undefined;
  let lockPath: string | undefined;
  let failure: unknown;
  try {
    if (!input.nativeGuarded && !input.detached && input.source.branch !== null) {
      const common = await FSP.realpath(
        (await git(input.cwd, ["rev-parse", "--path-format=absolute", "--git-common-dir"]))
          .toString()
          .trim(),
      );
      if (
        common !== input.source.binding.git_common_dir ||
        !input.source.branch.startsWith("refs/heads/")
      )
        throw new Error("Receipt branch/common-store identity unavailable");
      await git(input.cwd, ["check-ref-format", input.source.branch]);
      lockPath = Path.join(common, input.source.branch + ".lock");
      await FSP.mkdir(Path.dirname(lockPath), { recursive: true });
      // Hold Git's create-only ref lock through HEAD validation and add. Never rename it onto the branch.
      lock = await FSP.open(lockPath, "wx", 0o600);
    }
    return await recreateNative(input);
  } catch (cause) {
    failure = cause;
    if (cause instanceof RecreationHold) throw cause;
    throw new RecreationHold(
      "unknown",
      input.source.binding.path,
      input.owner,
      "native recreation observation unavailable",
      "Reconcile this exact receipt, registration and journal before retrying.",
      { cause },
    );
  } finally {
    if (lock !== undefined && lockPath !== undefined) {
      try {
        const original = await lock.stat(),
          current = await FSP.lstat(lockPath);
        if (current.dev !== original.dev || current.ino !== original.ino || !current.isFile())
          throw new Error("Recreation branch lock owner changed");
        await FSP.unlink(lockPath);
      } catch (cleanup) {
        throw new AggregateError(
          failure === undefined ? [cleanup] : [failure, cleanup],
          "Recreation branch lock cleanup failed",
        );
      } finally {
        await lock.close();
      }
    }
  }
}
