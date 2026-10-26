export type DomainIdKind =
  | "workstream"
  | "native_reference"
  | "membership"
  | "declaration"
  | "edge"
  | "event";
export interface DomainIdSource {
  next(kind: DomainIdKind): string;
}

/** Allocation is supplied by the transaction, never by the decider. */
export function allocatedIds(
  values: Readonly<Record<DomainIdKind, readonly string[]>>,
): DomainIdSource {
  const queues = new Map(Object.entries(values).map(([kind, ids]) => [kind, [...ids]]));
  return {
    next(kind) {
      const id = queues.get(kind)?.shift();
      if (!id || !/^[a-zA-Z0-9][a-zA-Z0-9._:-]{0,127}$/.test(id))
        throw new Error("command_id_allocation_exhausted");
      return id;
    },
  };
}
