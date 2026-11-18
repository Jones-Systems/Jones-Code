import { Menu } from "@base-ui/react/menu";
import { PERSONAL_THREAD_LIMITS, type PersonalThreadLimit } from "./personalThreadWindow";

/** Compact effort-style choices, retaining the menu's radio keyboard semantics. */
export function PersonalWorkstreamVisibleThreads({
  limit,
  onChange,
}: {
  limit: PersonalThreadLimit;
  onChange: (limit: PersonalThreadLimit) => void;
}) {
  return (
    <Menu.RadioGroup
      value={String(limit)}
      className="mr-2 flex items-center gap-0.5"
      aria-label="Default visible threads"
    >
      {PERSONAL_THREAD_LIMITS.map((value, index) => (
        <span key={value} className="contents">
          {index > 0 ? (
            <span aria-hidden className="text-muted-foreground/60">
              ·
            </span>
          ) : null}
          <Menu.RadioItem
            value={String(value)}
            closeOnClick={false}
            className="flex h-6 min-w-6 cursor-pointer items-center justify-center rounded-md px-1.5 text-xs text-muted-foreground outline-none hover:text-foreground data-checked:bg-input/72 data-checked:text-foreground focus-visible:ring-2 focus-visible:ring-inset focus-visible:ring-ring"
            onClick={() => onChange(value)}
          >
            {value}
          </Menu.RadioItem>
        </span>
      ))}
    </Menu.RadioGroup>
  );
}
