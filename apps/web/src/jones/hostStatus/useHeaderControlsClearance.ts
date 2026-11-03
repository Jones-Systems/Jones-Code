import { useLayoutEffect, useState, type RefObject } from "react";
import { observeResize } from "../../lib/observeResize";

export function useHeaderControlsClearance(
  headerRef: RefObject<HTMLElement | null>,
  controls: HTMLElement | null,
  minimum: number,
) {
  const [clearance, setClearance] = useState(minimum);
  // oxlint-disable-next-line t3code/react-effect-budget -- Reserve the live fixed controls' overlap before fitting host readings; own resize subscriptions until either element changes.
  useLayoutEffect(() => {
    const header = headerRef.current;
    if (!header || !controls) {
      setClearance(minimum);
      return;
    }
    const measure = () => {
      const content = header.getBoundingClientRect();
      const buttons = controls.getBoundingClientRect();
      const overlaps =
        buttons.width > 0 &&
        buttons.top < content.bottom &&
        buttons.bottom > content.top &&
        buttons.right > content.left &&
        buttons.left < content.right;
      setClearance(
        Math.min(content.width, Math.max(minimum, overlaps ? content.right - buttons.left + 8 : 0)),
      );
    };
    measure();
    const stop = observeResize([header, controls], measure);
    window.addEventListener("resize", measure);
    return () => {
      stop();
      window.removeEventListener("resize", measure);
    };
  }, [headerRef, controls, minimum]);
  return clearance;
}
