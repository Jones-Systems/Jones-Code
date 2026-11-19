import { useRef, type MouseEvent } from "react";

export const DOUBLE_CLICK_WINDOW_MS = 250;

export function useTimedClick(scope?: string) {
  const sequence = useRef<{
    target: HTMLElement;
    scope: string | undefined;
    at: number;
    consumed: boolean;
  } | null>(null);

  return (event: MouseEvent<HTMLElement>, onSingle: () => void, onDouble?: () => void) => {
    if (event.detail === 0 || !onDouble) {
      sequence.current = null;
      onSingle();
      return;
    }

    const previous = sequence.current;
    const rapid =
      previous?.target === event.currentTarget &&
      previous.scope === scope &&
      event.timeStamp >= previous.at &&
      event.timeStamp - previous.at <= DOUBLE_CLICK_WINDOW_MS;
    sequence.current = {
      target: event.currentTarget,
      scope,
      at: event.timeStamp,
      consumed: rapid,
    };
    if (!rapid) onSingle();
    else if (!previous.consumed) onDouble();
  };
}
