import { CopyIcon, RefreshCwIcon, RotateCcwIcon } from "lucide-react";
import { useId, useState, useSyncExternalStore, type ReactNode } from "react";
import { Button } from "../../components/ui/button";
import { Textarea } from "../../components/ui/textarea";
import { Toggle, ToggleGroup } from "../../components/ui/toggle-group";
import { blockedEffectsSnapshot, subscribeBlockedEffects } from "./isolation";
import { formatDesignReviewSource, type DesignReviewSource } from "./reviewSource";

/**
 * Shared chrome for in-thread design reviews: a header, the review's own sections, and a footer
 * that names the source commit, the real components shown, and the isolation in force.
 */
export function ReviewSurface(props: {
  readonly title: string;
  readonly summary: ReactNode;
  readonly source: DesignReviewSource;
  readonly components: ReadonlyArray<string>;
  readonly children: ReactNode;
}) {
  return (
    // The attachment page may be transparent and a review can switch theme, so the surface paints its
    // own background. The frame grows to fit its content, so nothing here sizes from the viewport.
    <div className="bg-background text-foreground">
      <main className="mx-auto flex w-full max-w-3xl flex-col gap-5 px-4 py-5 sm:px-6">
        <header className="flex flex-col gap-1">
          <h1 className="text-base font-semibold">{props.title}</h1>
          <p className="text-sm text-muted-foreground">{props.summary}</p>
        </header>
        {props.children}
        <ReviewProvenance source={props.source} components={props.components} />
      </main>
    </div>
  );
}

function ReviewProvenance(props: {
  readonly source: DesignReviewSource;
  readonly components: ReadonlyArray<string>;
}) {
  const blocked = useSyncExternalStore(subscribeBlockedEffects, blockedEffectsSnapshot);
  return (
    <footer className="flex flex-col gap-1 border-t border-border/60 pt-3 text-xs text-muted-foreground">
      <p>Real product components: {props.components.join(", ")}.</p>
      <p className="break-all">
        Source: <span className="font-mono">{formatDesignReviewSource(props.source)}</span>
        {props.source.builtAt ? ` (built ${props.source.builtAt})` : null}
      </p>
      <p>
        Isolated preview: synthetic data, no network, temporary storage.
        {blocked.length > 0
          ? ` Blocked ${blocked.length} external ${blocked.length === 1 ? "request" : "requests"}.`
          : null}
      </p>
    </footer>
  );
}

/** One labelled scenario or variant axis, as a segmented single choice. */
export function ReviewChoice<Value extends string>(props: {
  readonly label: string;
  readonly options: ReadonlyArray<{ readonly value: Value; readonly label: string }>;
  readonly value: Value;
  readonly onChange: (value: Value) => void;
}) {
  const labelId = useId();
  return (
    <div className="flex min-w-0 flex-col gap-1.5">
      <span id={labelId} className="text-xs font-medium text-muted-foreground">
        {props.label}
      </span>
      <ToggleGroup
        aria-labelledby={labelId}
        variant="segmented"
        className="max-w-full flex-wrap"
        value={[props.value]}
        onValueChange={(next) => {
          const option = props.options.find((candidate) => candidate.value === next[0]);
          if (option) props.onChange(option.value);
        }}
      >
        {props.options.map((option) => (
          <Toggle key={option.value} value={option.value}>
            {option.label}
          </Toggle>
        ))}
      </ToggleGroup>
    </div>
  );
}

/** One side of a comparison. A review lays out two in a `ReviewComparison`. */
export function ReviewPane(props: {
  readonly label: string;
  readonly detail: ReactNode;
  readonly children: ReactNode;
}) {
  return (
    <figure className="m-0 flex min-w-0 flex-col gap-2 rounded-lg border border-border/60 p-3">
      <figcaption className="flex items-baseline justify-between gap-2 text-xs">
        <span className="font-medium">{props.label}</span>
        <span className="tabular-nums text-muted-foreground">{props.detail}</span>
      </figcaption>
      {props.children}
    </figure>
  );
}

export function ReviewComparison(props: { readonly label: string; readonly children: ReactNode }) {
  return (
    <section aria-label={props.label} className="grid gap-3 sm:grid-cols-2">
      {props.children}
    </section>
  );
}

type CopyOutcome = {
  readonly key: string;
  readonly status: "copied" | "blocked";
  readonly text: string;
};

/**
 * Reset, copy, and restart. `copyText` runs when the reviewer copies, so it can read the rendered
 * page; `copyKey` identifies the current choice, and a copy result for an older choice is hidden.
 */
export function ReviewActions(props: {
  readonly resetLabel: string;
  readonly onReset: () => void;
  readonly onRestart: () => void;
  readonly copyKey: string;
  readonly copyText: () => string;
}) {
  const [outcome, setOutcome] = useState<CopyOutcome | null>(null);
  const current = outcome?.key === props.copyKey ? outcome : null;
  const copy = () => {
    const key = props.copyKey;
    const text = props.copyText();
    const blocked = () => setOutcome({ key, status: "blocked", text });
    if (!("clipboard" in navigator) || typeof navigator.clipboard.writeText !== "function") {
      blocked();
      return;
    }
    navigator.clipboard
      .writeText(text)
      .then(() => setOutcome({ key, status: "copied", text }), blocked);
  };
  return (
    <section aria-label="Review actions" className="flex flex-col gap-2">
      <div className="flex flex-wrap gap-2">
        <Button variant="default" size="sm" onClick={copy}>
          <CopyIcon aria-hidden />
          Copy choice
        </Button>
        <Button variant="outline" size="sm" onClick={props.onReset}>
          <RotateCcwIcon aria-hidden />
          {props.resetLabel}
        </Button>
        <Button variant="ghost" size="sm" onClick={props.onRestart}>
          <RefreshCwIcon aria-hidden />
          Restart review
        </Button>
      </div>
      <p role="status" className="min-h-4 text-xs text-muted-foreground">
        {current?.status === "copied"
          ? "Copied your choice to the clipboard."
          : current?.status === "blocked"
            ? "This view cannot use the clipboard. The text below is selected; copy it with Cmd+C or Ctrl+C."
            : ""}
      </p>
      {current?.status === "blocked" ? (
        <Textarea
          aria-label="Choice to copy"
          size="sm"
          readOnly
          rows={6}
          value={current.text}
          autoFocus
          onFocus={(event) => event.currentTarget.select()}
        />
      ) : null}
    </section>
  );
}
