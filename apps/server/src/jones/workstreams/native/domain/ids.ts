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
