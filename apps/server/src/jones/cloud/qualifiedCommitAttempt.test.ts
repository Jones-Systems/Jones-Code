import { expect, it } from "@effect/vitest";
import {
  describeErrorChain,
  launcherDriftNotice,
  launcherFailureMessage,
  QualifiedCommitFailure,
  qualifiedRollbackRefusal,
} from "./qualifiedCommitAttempt.ts";

const previous = "11111111-1111-4111-8111-111111111111";
const current = "22222222-2222-4222-8222-222222222222";

it("allows later pre-commit rollback after an earlier update's pointer write", () => {
  expect(
    qualifiedRollbackRefusal(
      { updateId: previous, step: "committed-message" },
      current,
      "candidate-exited:1",
    ),
  ).toBeUndefined();
  expect(qualifiedRollbackRefusal(undefined, current, "prepared-timeout")).toBeUndefined();
});

it.each(["pointer-write", "post-write-liveness", "committed-message"] as const)(
  "retains the matching update after %s and names its failure",
  (step) => {
    const attempt = { updateId: current, step };
    expect(qualifiedRollbackRefusal(attempt, current, "candidate-exited:1")).toBe(
      `Qualified pointer commit may have occurred (update ${current}, step ${step}); refused rollback for candidate-exited:1; paired state is retained for reconciliation.`,
    );
    const cause = Object.assign(new Error("private path and token=secret"), { code: "EIO" });
    const failure = new QualifiedCommitFailure(attempt, cause);
    expect(failure.cause).toBe(cause);
    expect(launcherFailureMessage(failure)).toBe(
      `Qualified pointer commit failed at step ${step} (update ${current}): EIO; paired state is retained for reconciliation.`,
    );
    expect(
      launcherFailureMessage(
        new Error("Launcher shutdown evidence could not be reconciled.", { cause: failure }),
      ),
    ).toContain(failure.message);
  },
);

it("bounds cyclic causes and withholds messages, stacks, arbitrary codes and objects", () => {
  const error = Object.assign(new Error("https://user:password@host?token=secret"), {
    code: "EIO",
    path: "/private",
  });
  error.cause = error;
  expect(describeErrorChain(error)).toBe(
    "EIO <- caused by: EIO <- caused by: EIO <- caused by: EIO",
  );
  expect(describeErrorChain(Object.assign(new Error("secret"), { code: "SECRET" }))).toBe(
    "Error (details withheld)",
  );
  expect(describeErrorChain({ toString: () => "secret" })).toBe(
    "non-Error failure (details withheld)",
  );
  expect(describeErrorChain(undefined)).toBe("unknown failure");
});

it("preserves existing top-level diagnostics and safely summarizes nested errors", () => {
  expect(launcherFailureMessage(new Error("T3CODE_HOME is required."))).toBe(
    "T3CODE_HOME is required.",
  );
  expect(launcherFailureMessage("plain failure")).toBe("plain failure");
  expect(
    launcherFailureMessage(
      new Error("Cannot start launcher", {
        cause: Object.assign(new Error("token=secret"), { code: "ENOSPC" }),
      }),
    ),
  ).toBe("Cannot start launcher <- caused by: ENOSPC");
});

it("names version drift without attempting to refresh or treating equal versions as drift", () => {
  expect(launcherDriftNotice("0.0.45-preview.1.1", "0.0.45-preview.1.1")).toBeUndefined();
  const notice = launcherDriftNotice("0.0.45-preview.20261011.1.1", "0.0.45-preview.20261016.2.1");
  expect(notice).toContain(
    "Launcher 0.0.45-preview.20261011.1.1 is supervising server 0.0.45-preview.20261016.2.1.",
  );
  expect(notice).toContain("t3 jones host adopt");
  expect(notice?.endsWith("\n")).toBe(true);
});
