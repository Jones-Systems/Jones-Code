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
        className="w-full cursor-pointer"
        type="range"
        min={THREAD_TITLE_BRIGHTNESS_BOOST.min}
        max={THREAD_TITLE_BRIGHTNESS_BOOST.max}
        step={5}
        aria-label="Sidebar thread title brightness"
        value={value}
        aria-valuetext={
          value === 0
            ? "Current colours"
            : value === 100
              ? "Brightest: receded titles at full foreground"
              : `${value}% toward full foreground`
        }
        onChange={(event) => onChange(Number(event.target.value))}
      />
      <div className="flex justify-between text-xs text-muted-foreground">
        <span>Current</span>
        <span>Brighter</span>
      </div>
    </div>
  );
}
