import type { ReactNode } from "react";
import { MessageCopyButton } from "../../components/chat/MessageCopyButton";

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
        <MessageCopyButton
          text={getText}
          label="Copy terminal contents"
          size="icon-xs"
          variant="ghost"
        />
      </div>
    </div>
  );
}
