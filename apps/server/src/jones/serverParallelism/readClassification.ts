import { withReadPriority } from "@t3tools/shared/jones/sqliteReadPriority";

// Apply at the executing read body, after queue/fiber boundaries. Wrapping an
// entire daemon would also demote Stop and runtime-response scheduling probes.
export const backgroundRead = withReadPriority("background");
export const interactiveRead = withReadPriority("interactive");
