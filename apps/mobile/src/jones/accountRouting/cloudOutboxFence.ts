import type { EnvironmentId } from "@t3tools/contracts";
import { Atom } from "effect/reactivity";

import type { QueuedThreadMessage } from "../../state/thread-outbox-model";

export interface ArchivedOutboxRow {
  readonly message: QueuedThreadMessage;
  readonly revision: number;
}

export const cloudOutboxArchiveFencesAtom = Atom.make<
  ReadonlyMap<EnvironmentId, ReadonlyArray<ArchivedOutboxRow>>
>(new Map()).pipe(Atom.keepAlive);
