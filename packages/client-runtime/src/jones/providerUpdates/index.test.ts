import { describe, expect, it } from "vite-plus/test";
import { ProviderDriverKind, ProviderInstanceId, type ServerProvider } from "@t3tools/contracts";

import {
  canOneClickUpdateProviderCandidate,
  collectProviderUpdateCandidates,
  hasOneClickUpdateProviderCandidate,
  providerUpdateNotificationKey,
  type ProviderUpdateCandidate,
} from "./index.ts";

const checkedAt = "2026-10-16T10:00:00.000Z";
const laterCheckedAt = "2026-10-16T10:01:00.000Z";

function provider(id: string, overrides: Partial<ServerProvider> = {}): ProviderUpdateCandidate {
  return {
    driver: ProviderDriverKind.make("codex"),
    instanceId: ProviderInstanceId.make(id),
    enabled: true,
    installed: true,
    version: "1.0.0",
    status: "ready",
    auth: { status: "authenticated" },
    checkedAt,
    models: [],
    slashCommands: [],
    skills: [],
    versionAdvisory: {
      status: "behind_latest",
      currentVersion: "1.0.0",
      latestVersion: "1.1.0",
      canUpdate: true,
      updateCommand: "npm install -g provider",
      checkedAt,
      message: "Update available.",
    },
    ...overrides,
  } as ProviderUpdateCandidate;
}

function targets(providers: ReadonlyArray<ServerProvider>) {
  return collectProviderUpdateCandidates(providers)
    .filter((candidate) => canOneClickUpdateProviderCandidate(candidate, providers))
    .map((candidate) => candidate.instanceId);
}

describe("provider update instance selection", () => {
  it.each(["npm install -g provider", "bun add -g provider"])(
    "keeps both instances when the sibling command is %s",
    (updateCommand) => {
      const first = provider("personal");
      const second = provider("work", {
        versionAdvisory: { ...first.versionAdvisory, updateCommand },
      });
      expect(targets([first, second, first])).toEqual(["personal", "work"]);
    },
  );

  it.each(["manual", "disabled", "broken", "unsupported", "queued", "running"] as const)(
    "excludes only the %s sibling",
    (guard) => {
      const valid = provider("valid");
      const sibling = provider("sibling", {
        ...(guard === "manual"
          ? {
              versionAdvisory: { ...valid.versionAdvisory, canUpdate: false, updateCommand: null },
            }
          : {}),
        ...(guard === "disabled" ? { enabled: false } : {}),
        ...(guard === "broken" || guard === "unsupported"
          ? {
              compatibilityAdvisory: {
                status: "supported",
                latestVersionStatus: guard,
                message: null,
                recommendedRange: null,
                recommendedVersion: null,
              },
            }
          : {}),
        ...(guard === "queued" || guard === "running"
          ? {
              updateState: {
                status: guard,
                startedAt: checkedAt,
                finishedAt: null,
                message: "Updating provider.",
                output: null,
              },
            }
          : {}),
      });
      expect(targets([valid, sibling])).toEqual(["valid"]);
      expect(targets([sibling, valid])).toEqual(["valid"]);
    },
  );

  it("does not offer an update when the command is absent", () => {
    const snapshot = provider("manual");
    expect(
      targets([
        {
          ...snapshot,
          versionAdvisory: { ...snapshot.versionAdvisory, updateCommand: null },
        },
      ]),
    ).toEqual([]);
  });

  it.each([false, true])("uses the newest snapshot before filtering (reverse=%s)", (reverse) => {
    const old = provider("personal");
    const current: ServerProvider = {
      ...old,
      checkedAt: laterCheckedAt,
      versionAdvisory: { ...old.versionAdvisory, status: "current" },
    };
    const snapshots = reverse ? [old, current] : [current, old];
    expect(targets(snapshots)).toEqual([]);
    expect(canOneClickUpdateProviderCandidate(old, snapshots)).toBe(false);
  });

  it("uses the newest busy state even when the caller retains an older candidate", () => {
    const old = provider("personal");
    const busy = provider("personal", {
      checkedAt: laterCheckedAt,
      updateState: {
        status: "running",
        startedAt: checkedAt,
        finishedAt: null,
        message: "Updating provider.",
        output: null,
      },
    });
    expect(hasOneClickUpdateProviderCandidate(old, [old, busy])).toBe(true);
    expect(canOneClickUpdateProviderCandidate(old, [old, busy])).toBe(false);
  });

  it("does not borrow eligibility from another instance", () => {
    expect(canOneClickUpdateProviderCandidate(provider("missing"), [provider("other")])).toBe(
      false,
    );
  });

  it("tracks membership and action availability without depending on snapshot order", () => {
    const first = provider("personal");
    const second = provider("work");
    expect(providerUpdateNotificationKey([first, second])).toBe(
      providerUpdateNotificationKey([second, first, first]),
    );
    expect(providerUpdateNotificationKey([first])).not.toBe(
      providerUpdateNotificationKey([second]),
    );
    expect(providerUpdateNotificationKey([first])).not.toBe(
      providerUpdateNotificationKey([
        { ...first, versionAdvisory: { ...first.versionAdvisory, canUpdate: false } },
      ]),
    );
  });
});
