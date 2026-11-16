import { expect, it } from "vite-plus/test";
import { DisplacedAnswerLedger } from "./displacedAnswer";

it("restores the exact ordinary draft and preserves later answer displacements", () => {
  const ledger = new DisplacedAnswerLedger();
  ledger.record("draft", "request:q1", "answer", "ordinary  ", "ordinary\n\nanswer");
  ledger.record("draft", "request:q2", "later", "ordinary\n\nanswer", "ordinary\n\nanswer\n\nlater");
  expect(ledger.withdraw("draft", "request:q1", "answer", "ordinary\n\nanswer\n\nlater")).toBe("ordinary\n\nlater");
  expect(ledger.withdraw("draft", "request:q2", "later", "ordinary\n\nlater")).toBe("ordinary  ");
});

it("does not remove equal text from another route, question or edited ordinary draft", () => {
  const ledger = new DisplacedAnswerLedger();
  ledger.record("draft", "request:q1", "answer", "ordinary", "ordinary\n\nanswer");
  expect(ledger.withdraw("other", "request:q1", "answer", "ordinary\n\nanswer")).toBe("ordinary\n\nanswer");
  expect(ledger.withdraw("draft", "other:q1", "answer", "ordinary\n\nanswer")).toBe("ordinary\n\nanswer");
  expect(ledger.withdraw("draft", "request:q1", "answer", "edited\n\nanswer")).toBe("edited\n\nanswer");
  ledger.record("draft", "request:q2", "later", "edited\n\nanswer", "edited\n\nanswer\n\nlater");
  expect(ledger.withdraw("draft", "request:q1", "answer", "edited\n\nanswer\n\nlater")).toBe("edited\n\nanswer\n\nlater");
});
