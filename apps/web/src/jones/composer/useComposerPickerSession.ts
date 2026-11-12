import { useState } from "react";

/** A dismissed popup starts in Favorites; model and effort changes keep the current popup intact. */
export function useComposerPickerSession(open: boolean): number {
  const [session, setSession] = useState({ open, generation: 0 });
  if (session.open !== open) {
    setSession({ open, generation: session.generation + (open ? 1 : 0) });
  }
  return session.generation;
}
