import { useId } from "react";
import { THREAD_TITLE_BRIGHTNESS_BOOST } from "./model";

export function ThreadTitleBrightnessSlider({
  value,
  onChange,
}: {
  value: number;
  onChange: (value: number) => void;
}) {
  const id = useId();
  return (
    <div className="w-full min-w-0">
      <label htmlFor={id} className="flex justify-between gap-2 text-xs">
        <span>Title brightness</span>
        <output>{value}%</output>
      </label>
      <input
        id={id}
        className="h-6 w-full cursor-pointer touch-none accent-primary"
        type="range"
        min={THREAD_TITLE_BRIGHTNESS_BOOST.min}
        max={THREAD_TITLE_BRIGHTNESS_BOOST.max}
        step={1}
        aria-label="Sidebar thread title brightness"
        value={value}
        aria-valuetext={
          value === 0
            ? "Original colors"
            : value === 100
              ? "Highest contrast"
              : `${value}% toward highest contrast`
        }
        onInput={(event) => onChange(event.currentTarget.valueAsNumber)}
      />
      <div className="flex justify-between text-xs text-muted-foreground">
        <span>0% · Original</span>
        <span>100% · Highest contrast</span>
      </div>
    </div>
  );
}
