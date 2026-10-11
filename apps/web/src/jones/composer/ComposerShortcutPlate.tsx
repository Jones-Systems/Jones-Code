import type { ReactNode } from "react";

export function ComposerShortcutPlate({
  children,
  visible,
}: {
  children: ReactNode;
  visible: boolean;
}) {
  return (
    <div className="relative isolate" style={{ visibility: visible ? "visible" : "hidden" }}>
      <div
        aria-hidden
        data-composer-shortcut-plate="true"
        className="pointer-events-none absolute -inset-x-2 -top-1.5 -bottom-[100vh] -z-10 bg-background"
      />
      {children}
    </div>
  );
}
