import * as Effect from "effect/Effect";

// Unreleased sync builds used this identity to copy Jones peer links into upstream's link
// table. Jones must not write upstream rows, so the identity is kept with no effect: databases
// that already recorded it still start, and links stay in the Jones table. Rows such a build
// copied are left unread; removing them requires a separately authorized operation.
export default Effect.void;
