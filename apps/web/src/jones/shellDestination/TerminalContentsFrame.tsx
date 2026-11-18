import type { ReactNode } from "react";
import { MessageCopyButton } from "../../components/chat/MessageCopyButton";

export function TerminalCopyButton({
  getText,
  size = "icon-xs",
}: {
  getText: () => string;
  size?: "icon-xs" | "icon-micro";
}) {
  return (
    <MessageCopyButton text={getText} label="Copy terminal contents" size={size} variant="ghost" />
  );
}

export function TerminalContentsFrame({
  children,
  getText,
}: {
  children: ReactNode;
  getText: () => string;
}) {
  return (
    <div className="flex h-full flex-col bg-(--terminal-background)">
      {children}
      <div className="flex shrink-0 justify-end px-2 pb-1">
        <TerminalCopyButton getText={getText} />
      </div>
    </div>
  );
}
