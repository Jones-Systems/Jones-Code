import { useGT } from "gt-react";
import type {
  AccountRoutingChoice,
  AccountRoutingFamily,
  ComposerAccountRoutingPicker,
} from "./composerAccountRouting";

export function useAccountRoutingLabel() {
  const gt = useGT();
  return (family: AccountRoutingFamily | "unavailable") =>
    family === "codex"
      ? gt("Codex Auto")
      : family === "claudeAgent"
        ? gt("Claude Auto")
        : gt("Auto account");
}

export function AccountRoutingChoices({
  choices,
  browsing,
  onSelect,
}: {
  choices: ComposerAccountRoutingPicker["choices"];
  browsing: AccountRoutingFamily | null;
  onSelect: (choice: AccountRoutingChoice) => void;
}) {
  const label = useAccountRoutingLabel();
  if (choices.length === 0) return null;
  return (
    <div className="flex flex-wrap gap-1 px-1.5 pb-1.5">
      {choices.map((choice) => (
        <button
          key={choice.family}
          type="button"
          aria-pressed={choice.family === browsing}
          className="rounded-md px-2 py-1 text-xs text-muted-foreground hover:bg-accent hover:text-foreground focus-visible:outline-2 focus-visible:outline-primary aria-pressed:bg-accent aria-pressed:text-foreground"
          onClick={() => onSelect(choice)}
        >
          {label(choice.family)}
        </button>
      ))}
    </div>
  );
}
