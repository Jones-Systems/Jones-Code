import { carryDisplacedCustomAnswerIntoPrompt } from "../../pendingUserInput";

interface Displacement {
  draft: string;
  key: string;
  text: string;
  before: string;
  after: string;
}

// Exact draft snapshots fence undo from equal text typed later by the user.
export class DisplacedAnswerLedger {
  private entries: Displacement[] = [];

  record(draft: string, key: string, text: string, before: string, after: string) {
    this.entries.push({ draft, key, text, before, after });
  }

  withdraw(draft: string, key: string, answer: string, prompt: string): string {
    const index = this.entries.findLastIndex(
      (entry) => entry.draft === draft && entry.key === key && entry.text.trim() === answer.trim(),
    );
    const entry = this.entries[index];
    if (!entry) return prompt;
    const later = this.entries.slice(index + 1).filter((item) => item.draft === draft);
    let expected = entry.after;
    for (const item of later) {
      if (item.before !== expected) return prompt;
      expected = item.after;
    }
    if (prompt !== expected) return prompt;
    let restored = entry.before;
    for (const item of later) {
      item.before = restored;
      restored = carryDisplacedCustomAnswerIntoPrompt(restored, item.text);
      item.after = restored;
    }
    this.entries.splice(index, 1);
    return restored;
  }
}
