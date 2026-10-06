import { useEffect, useState } from "react";
import { Button } from "../ui/button";
import { Alert, AlertAction, AlertDescription, AlertTitle } from "../ui/alert";
import { createMockWorkQueueSource } from "./mockWorkQueue";
import { WorkQueuePanel } from "./WorkQueuePanel";

export function WorkQueuePreview() {
  const [source] = useState(createMockWorkQueueSource);
  const [dirty, setDirty] = useState(false);
  const [leaving, setLeaving] = useState(false);

  useEffect(() => {
    if (!dirty) return;
    const beforeUnload = (event: BeforeUnloadEvent) => {
      event.preventDefault();
      event.returnValue = "";
    };
    window.addEventListener("beforeunload", beforeUnload);
    return () => window.removeEventListener("beforeunload", beforeUnload);
  }, [dirty]);

  useEffect(() => {
    if (leaving && !dirty) window.location.assign("/welcome");
  }, [leaving, dirty]);

  return (
    <div className="h-dvh overflow-y-auto bg-background text-foreground">
      <header className="flex items-center justify-between gap-4 border-b border-border px-6 py-4">
        <span className="text-sm font-medium">Submitted work preview</span>
        <Button variant="outline" onClick={() => setLeaving(true)}>
          Connect a device
        </Button>
      </header>
      <main className="mx-auto flex max-w-6xl flex-col gap-6 px-4 py-6 sm:px-6">
        {leaving && dirty && (
          <Alert variant="warning">
            <AlertTitle>Leave this preview?</AlertTitle>
            <AlertDescription>Your unsaved draft will be discarded.</AlertDescription>
            <AlertAction>
              <Button variant="outline" onClick={() => setLeaving(false)}>
                Keep editing
              </Button>
              <Button onClick={() => setDirty(false)}>Discard changes and leave</Button>
            </AlertAction>
          </Alert>
        )}
        <WorkQueuePanel source={source} onDirtyChange={setDirty} />
      </main>
    </div>
  );
}
