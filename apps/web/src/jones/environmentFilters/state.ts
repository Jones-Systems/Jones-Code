import { Atom } from "effect/reactivity";
import { DEFAULT_SIDEBAR_ENVIRONMENT_FILTERS } from "./model";

// Keep this renderer-session preference when route changes unmount the sidebar.
export const sidebarEnvironmentFiltersAtom = Atom.make(DEFAULT_SIDEBAR_ENVIRONMENT_FILTERS).pipe(
  Atom.keepAlive,
);
