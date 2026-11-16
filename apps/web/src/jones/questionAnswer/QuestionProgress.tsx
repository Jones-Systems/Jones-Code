export function QuestionProgress({
  index,
  answered,
  questionIds,
  disabled,
  onSelect,
}: {
  index: number;
  answered: ReadonlyArray<boolean>;
  questionIds: ReadonlyArray<string>;
  disabled: boolean;
  onSelect?: ((index: number) => void) | undefined;
}) {
  if (answered.length < 2) return null;
  return (
    <div className="flex shrink-0 items-center gap-0.5" aria-label="Question progress">
      {answered.map((complete, i) =>
        onSelect ? (
          <button
            key={questionIds[i]}
            type="button"
            disabled={disabled}
            aria-label={`Question ${i + 1} of ${answered.length}, ${complete ? "answered" : "unanswered"}`}
            aria-current={i === index ? "step" : undefined}
            className="flex size-6 items-center justify-center rounded focus-visible:outline-2 focus-visible:-outline-offset-2 focus-visible:outline-ring disabled:opacity-50"
            onClick={() => onSelect(i)}
          >
            <span
              aria-hidden
              className={`size-1.5 rounded-full ${i === index ? "bg-primary ring-2 ring-primary/20" : complete ? "bg-foreground/60" : "bg-muted-foreground/25"}`}
            />
          </button>
        ) : (
          <span
            key={questionIds[i]}
            aria-hidden
            className={`mx-1 size-1.5 rounded-full ${i === index ? "bg-primary" : complete ? "bg-foreground/60" : "bg-muted-foreground/25"}`}
          />
        ),
      )}
      <span className="ms-1 text-xs tabular-nums text-muted-foreground">
        {index + 1}/{answered.length}
      </span>
    </div>
  );
}
