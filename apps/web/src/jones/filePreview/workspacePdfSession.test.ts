import { describe, expect, it } from "vite-plus/test";
import {
  createWorkspacePdfSession,
  clampWorkspacePdfPosition,
  initialWorkspacePdfPosition,
} from "./workspacePdfSession";

describe("workspace PDF resource lifetime", () => {
  it("pins a grant per revision and renews an explicit attempt even for the same URL", () => {
    const session = createWorkspacePdfSession("environment/thread/draft-cwd/path");
    session.selectRevision("one");
    const first = session.load("original");
    expect(session.load("renewed")).toBe(first);
    expect(session.beginRetry(first)("original")).toBe(true);
    const retried = session.load("ignored renewal");
    expect(retried.src).toBe("original");
    expect(retried.generation).toBeGreaterThan(first.generation);
    expect(first.retryAttempt).toBe(0);
    expect(retried.retryAttempt).toBe(1);
    expect(session.beginRetry(first)("late retry")).toBe(false);
  });
  it("withholds unavailable authorization and rejects old revision, replaced and disposed completions", () => {
    const old = createWorkspacePdfSession("old-resource");
    old.selectRevision("one");
    const first = old.load("old-grant");
    old.selectRevision("two");
    expect(old.beginRetry(first)("late")).toBe(false);
    const next = old.load("current-grant");
    const pending = old.beginRetry(next);
    old.dispose();
    old.activate();
    expect(pending("disposed lifetime")).toBe(false);
    old.dispose();
    expect(old.beginRetry(next)("late after disposal")).toBe(false);
    const replacement = createWorkspacePdfSession("other-cwd/same-path");
    replacement.selectRevision("two");
    expect(replacement.beginRetry(next)("foreign")).toBe(false);
    expect(replacement.readPosition()).toEqual(initialWorkspacePdfPosition);
  });
  it("retains a synchronous outgoing snapshot but fences it after the new document publishes its clamp", () => {
    const session = createWorkspacePdfSession("same-resource");
    session.selectRevision("one");
    const first = session.load("one");
    session.remember(first, { page: 2, offset: 600, left: 80, zoomPercent: 300 });
    session.selectRevision("two");
    const next = session.load("two");
    session.remember(first, { page: 2, offset: 610, left: 80, zoomPercent: 300 });
    expect(session.readPosition().offset).toBe(610);
    const clamped = clampWorkspacePdfPosition(session.readPosition(), [140], 20);
    session.remember(next, clamped);
    expect(session.readPosition()).toEqual({ page: 0, offset: 139, left: 20, zoomPercent: 300 });
    session.remember(first, { page: 2, offset: 620, left: 80, zoomPercent: 300 });
    expect(session.readPosition()).toEqual(clamped);
    session.selectRevision("three");
    const final = session.load("three");
    expect(clampWorkspacePdfPosition(session.readPosition(), [140, 792, 792], 100).page).toBe(0);
    session.remember(final, { page: 0, offset: 0, left: 0, zoomPercent: 110 });
    expect(session.readPosition().zoomPercent).toBe(110);
  });
});
