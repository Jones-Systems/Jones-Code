import type { ReactNode, Ref } from "react";

export function ComposerShortcutRails({
  bandRef,
  visible,
  height,
  accounts,
  effort,
}: {
  bandRef: Ref<HTMLDivElement>;
  visible: boolean;
  height: number;
  accounts: ReactNode;
  effort: ReactNode;
}) {
  return (
    <div
      ref={bandRef}
      data-chat-composer-shortcut-rails="true"
      aria-hidden={!visible || undefined}
      inert={!visible || undefined}
      className="relative w-full"
      style={{ height }}
    >
      <div
        data-chat-composer-shortcut-content="true"
        className="flex min-w-0 flex-wrap items-end justify-between gap-x-4 gap-y-2"
      >
        <div className="pointer-events-auto min-w-0 max-w-full shrink-0">
          <ComposerShortcutPlate visible={visible}>{accounts}</ComposerShortcutPlate>
        </div>
        <div className="pointer-events-auto w-max min-w-0 max-w-full shrink-0">
          {effort ? (
            <ComposerShortcutPlate visible={visible}>{effort}</ComposerShortcutPlate>
          ) : null}
        </div>
      </div>
    </div>
  );
}

function ComposerShortcutPlate({ children, visible }: { children: ReactNode; visible: boolean }) {
  return (
    <div className="relative isolate" style={{ visibility: visible ? "visible" : "hidden" }}>
      <div
        aria-hidden
        data-composer-shortcut-plate="true"
        className="pointer-events-none absolute inset-0 -z-10 bg-background"
      />
      {children}
    </div>
  );
}
