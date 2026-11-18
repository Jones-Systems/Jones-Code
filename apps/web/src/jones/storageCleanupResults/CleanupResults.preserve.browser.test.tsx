import type { StorageCleanupReport, ThreadId } from "@t3tools/contracts";
import { expect, it, vi } from "vite-plus/test";
import { render } from "../../../test/render";
import { CleanupResults } from "./CleanupResults";

type Entry = StorageCleanupReport["entries"][number];
type Retirement = NonNullable<Entry["retirement"]>;

const sha = "0123456789abcdef0123456789abcdef01234567";

const entry = (retirement: Partial<Retirement>, patch: Partial<Entry> = {}): Entry => ({
  kind: "worktree",
  outcome: "kept",
  reason: "Retirement proposed; confirmation is required.",
  path: "/work/feature-one",
  projectPath: "/project",
  threadId: "synthetic-thread" as ThreadId,
  threadTitle: "Feature one",
  bytes: null,
  files: null,
  retirement: {
    state: "held",
    reasonCodes: ["dirty_tracked", "untracked"],
    receiptId: null,
    fingerprint: "fingerprint-1",
    paths: [],
    details: [],
    outputObligations: [],
    ...retirement,
  },
  ...patch,
});

const preservable = (remote?: string | null): Retirement["recoverability"] => ({
  verdict: "retirable_after_preserve",
  regenerable: [],
  preserve: {
    files: 12,
    deletions: 0,
    bytes: 3 * 1024 ** 2,
    ...(remote === undefined ? {} : { remote }),
    excluded: [],
    excludedBytes: 0,
  },
});

const renderEntries = (entries: Entry[], onRetire?: (entry: Entry) => void) =>
  render(
    <CleanupResults
      selectedEnvironmentIds={["env"]}
      reports={[
        {
          environmentId: "env",
          environmentLabel: "Mini",
          report: {
            trigger: "manual",
            operation: "propose",
            startedAt: "2026-11-17T12:00:00.000Z",
            finishedAt: "2026-11-17T12:01:00.000Z",
            entries,
            counts: { removed: 0, kept: entries.length, failed: 0 },
            omittedCount: 0,
            bytesFreed: 0,
          },
          ...(onRetire ? { onRetire } : {}),
        },
      ]}
    />,
  );

const retireButton = (screen: Awaited<ReturnType<typeof renderEntries>>) =>
  screen.getByRole("button", { name: "Retire", exact: true });

it("states what Retire saves, where, and the verified force before the click, with a remote", async () => {
  const proposal = entry({ recoverability: preservable("origin") });
  const onRetire = vi.fn();
  await using screen = await renderEntries([proposal], onRetire);
  const copy = screen.container.querySelector('[data-testid="retirement-proposal-copy"]');
  expect(copy?.getAttribute("data-kind")).toBe("preserve");
  expect(copy?.textContent).toContain("12 files (3.00 MB) to a new preserve/… branch");
  expect(copy?.textContent).toContain('pushed to the remote "origin"');
  expect(copy?.textContent).toContain("--force only after the saved content is verified to match");
  // The untracked-files setting note no longer applies when a branch carries them.
  expect(screen.container.textContent).not.toContain("Keep worktrees with local changes");
  await retireButton(screen).click();
  expect(onRetire).toHaveBeenCalledWith(proposal);
});

it("states local-only preservation when the repository has no remote", async () => {
  await using screen = await renderEntries([entry({ recoverability: preservable(null) })], vi.fn());
  const text = screen.container.querySelector(
    '[data-testid="retirement-proposal-copy"]',
  )?.textContent;
  expect(text).toContain("no remote, so the branch stays local-only");
  expect(text).toContain("--force only after the saved content is verified");
  await expect.element(retireButton(screen)).toBeInTheDocument();
});

it("lists excluded paths with human reasons and offers no Retire", async () => {
  const held = entry({
    recoverability: {
      verdict: "held",
      regenerable: [],
      preserve: {
        files: 4,
        deletions: 0,
        bytes: 100,
        excluded: [
          { path: ".env.local", reason: "sensitive_name" },
          { path: "vendor/lib", reason: "nested_repository" },
          { path: "notes.txt", reason: "secret_pattern:github_token" },
        ],
        excludedBytes: 5,
      },
    },
  });
  await using screen = await renderEntries([held], vi.fn());
  const text = screen.container.textContent ?? "";
  expect(text).toContain("Held: 3 files cannot be saved to a branch");
  expect(text).toContain(".env.local");
  expect(text).toContain("Name looks like a secret or credential file");
  expect(text).toContain("Contains its own Git repository");
  expect(text).toContain("Content matches a secret pattern (github_token)");
  expect(text).toContain("Needs attention");
  await expect
    .element(screen.getByRole("button", { name: "Retire", exact: true }))
    .not.toBeInTheDocument();
});

it("shows a recreatable target as safe to retire with nothing lost", async () => {
  const safe = entry({
    state: "proposed",
    reasonCodes: [],
    recoverability: { verdict: "recreatable", regenerable: ["/work/feature-one/node_modules"] },
  });
  await using screen = await renderEntries([safe], vi.fn());
  const copy = screen.container.querySelector('[data-testid="retirement-proposal-copy"]');
  expect(copy?.getAttribute("data-kind")).toBe("recreatable");
  expect(copy?.textContent).toContain("Safe to retire");
  expect(copy?.textContent).toContain("Nothing is lost");
  await expect.element(retireButton(screen)).toBeInTheDocument();
});

it("keeps the reason-code rule for a server that sends no recoverability", async () => {
  const legacy = entry({ reasonCodes: ["dirty_tracked"], recoverability: undefined });
  await using screen = await renderEntries([legacy], vi.fn());
  expect(screen.container.querySelector('[data-testid="retirement-proposal-copy"]')).toBeNull();
  await expect
    .element(screen.getByRole("button", { name: "Retire", exact: true }))
    .not.toBeInTheDocument();
});

const retired = (preserve: NonNullable<Retirement["preserve"]>) =>
  entry(
    {
      state: "retired",
      reasonCodes: [],
      receiptId: "receipt-1",
      recoverability: preservable("origin"),
      preserve,
    },
    { outcome: "removed", reason: "retired", bytes: 4096 },
  );

it("shows a remote-verified preserve receipt with its restore command and forced removal", async () => {
  const restore = "git worktree add /work/feature-one preserve/20261117/mini/feature-one";
  await using screen = await renderEntries([
    retired({
      branch: "preserve/20261117/mini/feature-one",
      sha,
      remote: "origin",
      verification: "remote_verified",
      excluded: [],
      restore,
      forced: true,
      readback: { local: "verified", remote: "verified" },
    }),
  ]);
  const receipt = screen.container.querySelector('[data-testid="retirement-preserve-receipt"]');
  const text = receipt?.textContent ?? "";
  expect(text).toContain("preserve/20261117/mini/feature-one");
  expect(text).toContain(sha.slice(0, 10));
  expect(text).not.toContain(sha);
  expect(text).toContain("Remote-verified");
  expect(text).toContain("Removed with --force after verification");
  expect(text).toContain("Remote: origin");
  expect(text).toContain("Local branch verified · remote verified");
  expect(text).toContain(restore);
  expect(text).toContain("never deleted automatically");
  // A finished retirement no longer reads as a proposal.
  expect(screen.container.querySelector('[data-testid="retirement-proposal-copy"]')).toBeNull();
  await expect
    .element(screen.getByRole("button", { name: "Copy restore command" }))
    .toBeInTheDocument();
});

it("shows a local-only receipt without force and with a failed readback", async () => {
  await using screen = await renderEntries([
    retired({
      branch: "preserve/20261117/mini/feature-one",
      sha,
      remote: null,
      verification: "local_only",
      excluded: [],
      restore: "git switch -c feature-one preserve/20261117/mini/feature-one",
      forced: false,
      readback: { local: "failed", remote: "not_applicable" },
    }),
  ]);
  const text =
    screen.container.querySelector('[data-testid="retirement-preserve-receipt"]')?.textContent ??
    "";
  expect(text).toContain("Local-only");
  expect(text).toContain("Removed without --force");
  expect(text).toContain("this machine's repository holds the only copy");
  expect(text).toContain("Local branch FAILED · remote not applicable (not pushed)");
});

it("labels every preserve reason code a held retirement reports", async () => {
  const codes = [
    "preserve_commit_failed",
    "preserve_index_isolation_unverified",
    "preserve_branch_unavailable",
    "preserve_remote_unavailable",
    "preserve_store_not_durable",
    "preserve_exclusions_changed",
    "preserve_selection_changed",
    "preserve_tree_mismatch",
    "preserve_readback_failed",
    "preserve_remote_readback_failed",
    "preserve_path_unrepresentable",
    "interrupted_preserved_path_missing",
  ];
  await using screen = await renderEntries(
    codes.map((code, index) =>
      entry({ reasonCodes: [code], fingerprint: null }, { path: `/work/held-${index}` }),
    ),
  );
  // Each category shows five rows at a time; open them all.
  await screen.getByRole("button", { name: "Show all" }).click();
  for (const code of codes) {
    const label = screen.container.querySelector(`[data-reason-code="${code}"]`);
    expect(label?.textContent, code).toBeTruthy();
    expect(label?.textContent, code).not.toContain(code);
  }
});
