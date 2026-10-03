import { useState } from "react";
import { useBlocker } from "@tanstack/react-router";
import { isElectron } from "../../env";
import { WorkspacePageHeader } from "../WorkspacePageHeader";
import { WorkspacePageContainer } from "../WorkspacePageContainer";
import { SidebarInset } from "../ui/sidebar";
import { Button } from "../ui/button";
import { Alert, AlertAction, AlertDescription, AlertTitle } from "../ui/alert";
import { createMockWorkQueueSource } from "./mockWorkQueue";
import { WorkQueuePanel } from "./WorkQueuePanel";

export function WorkQueuePage() {
  const [source] = useState(createMockWorkQueueSource);
  const [dirty, setDirty] = useState(false);
  const blocker = useBlocker({
    shouldBlockFn: () => dirty,
    enableBeforeUnload: () => dirty,
    withResolver: true,
  });
  return (
    <SidebarInset className="h-dvh overflow-hidden">
      <WorkspacePageHeader electron={isElectron}>
        <span className="text-sm font-medium">Submitted work</span>
      </WorkspacePageHeader>
      <div className="min-h-0 flex-1 overflow-y-auto">
        <WorkspacePageContainer width="expanded">
          {blocker.status === "blocked" && (
            <Alert variant="warning">
              <AlertTitle>Leave this preview?</AlertTitle>
              <AlertDescription>Your unsaved draft will be discarded.</AlertDescription>
              <AlertAction>
                <Button variant="outline" onClick={() => blocker.reset()}>
                  Keep editing
                </Button>
                <Button onClick={() => blocker.proceed()}>Discard changes and leave</Button>
              </AlertAction>
            </Alert>
          )}
          <WorkQueuePanel source={source} onDirtyChange={setDirty} />
        </WorkspacePageContainer>
      </div>
    </SidebarInset>
  );
}
