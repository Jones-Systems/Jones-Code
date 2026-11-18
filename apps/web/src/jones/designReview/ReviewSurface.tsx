import { CopyIcon, RefreshCwIcon, RotateCcwIcon } from "lucide-react";
import { useId, useState, useSyncExternalStore, type ReactNode } from "react";
import { Button } from "../../components/ui/button";
import { Textarea } from "../../components/ui/textarea";
import { Toggle, ToggleGroup } from "../../components/ui/toggle-group";
import { blockedEffectsSnapshot, subscribeBlockedEffects } from "./isolation";
import { formatDesignReviewSource, type DesignReviewSource } from "./reviewSource";
import "./reviewSurface.css";

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
    <main className="flex min-w-0 flex-col gap-3">
      <header data-design-review-chrome className="grid gap-1">
        <h1 className="text-sm font-semibold">{props.title}</h1>
        <p className="text-sm text-muted-foreground">{props.summary}</p>
      </header>
      {props.children}
      <ReviewProvenance source={props.source} components={props.components} />
    </main>
  );
}

function ReviewProvenance(props: {
  readonly source: DesignReviewSource;
  readonly components: ReadonlyArray<string>;
}) {
  const blocked = useSyncExternalStore(subscribeBlockedEffects, blockedEffectsSnapshot);
  return (
    <details data-design-review-chrome className="text-xs text-muted-foreground">
      <summary className="w-fit cursor-pointer">Build details</summary>
      <div className="mt-1 grid gap-1.5">
        <p>Real product components: {props.components.join(", ")}.</p>
        <p className="break-all font-mono">{formatDesignReviewSource(props.source)}</p>
        <p>
          Synthetic data and temporary choices.
          {blocked.length > 0
            ? ` Blocked ${blocked.length} external ${blocked.length === 1 ? "request" : "requests"}.`
            : null}
        </p>
      </div>
    </details>
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
    <div className="grid min-w-0 grid-cols-[4.25rem_minmax(0,1fr)] items-center gap-2">
      <span id={labelId} className="text-xs font-medium text-muted-foreground">
        {props.label}
      </span>
      <ToggleGroup
        aria-labelledby={labelId}
        variant="default"
        size="compact"
        className="max-w-full flex-wrap"
        value={[props.value]}
        onValueChange={(next) => {
          const option = props.options.find((candidate) => candidate.value === next[0]);
          if (option) props.onChange(option.value);
        }}
      >
        {props.options.map((option) => (
          <Toggle key={option.value} value={option.value} variant="pill">
            {option.label}
          </Toggle>
        ))}
      </ToggleGroup>
    </div>
  );
}

/** A single frame keeps the product surface visually distinct from the walkthrough controls. */
export function ReviewWindow(props: { readonly caption: string; readonly children: ReactNode }) {
  return (
    <figure className="m-0 grid min-w-0 gap-1.5">
      <figcaption data-design-review-chrome className="text-xs text-muted-foreground">
        {props.caption}
      </figcaption>
      <div className="overflow-hidden rounded-lg border border-border bg-background text-foreground">
        <div className="flex items-center gap-2 border-b border-border bg-card px-3 py-1.5 text-xs font-medium">
          <span aria-hidden="true" className="flex gap-1">
            <span className="size-1.5 rounded-full bg-muted-foreground/25" />
            <span className="size-1.5 rounded-full bg-muted-foreground/25" />
            <span className="size-1.5 rounded-full bg-muted-foreground/25" />
          </span>
          Jones Code
        </div>
        {props.children}
      </div>
    </figure>
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
    <section aria-label="Review actions" data-design-review-chrome className="flex flex-col gap-1">
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
