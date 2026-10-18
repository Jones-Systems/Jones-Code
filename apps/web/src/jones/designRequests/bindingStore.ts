import {
  DesignRequestBindings,
  type DesignRequestBinding,
} from "@t3tools/contracts/jones/designRequests";
import { designRequestNonce } from "@t3tools/client-runtime/jones/design-requests";
import * as Schema from "effect/Schema";

/** Client-local pairings. They name an existing workstream; the connector never creates one. */
export const DESIGN_REQUEST_BINDINGS_KEY = "jones-design-request-bindings/v1";

const decodeBindings = Schema.decodeUnknownOption(DesignRequestBindings, {
  onExcessProperty: "error",
});

export type DesignRequestBindingStorage = Pick<Storage, "getItem" | "setItem">;

export function readDesignRequestBindings(
  storage: DesignRequestBindingStorage,
): readonly DesignRequestBinding[] {
  let raw: string | null;
  try {
    raw = storage.getItem(DESIGN_REQUEST_BINDINGS_KEY);
  } catch {
    return [];
  }
  if (raw === null) return [];
  let parsed: unknown;
  try {
    parsed = JSON.parse(raw);
  } catch {
    return [];
  }
  const decoded = decodeBindings(parsed);
  return decoded._tag === "Some" ? decoded.value.bindings : [];
}

export const findDesignRequestBinding = (
  bindings: readonly DesignRequestBinding[],
  galleryOrigin: string,
  projectKey: string,
): DesignRequestBinding | null =>
  bindings.find(
    (binding) => binding.galleryOrigin === galleryOrigin && binding.projectKey === projectKey,
  ) ?? null;

function write(storage: DesignRequestBindingStorage, bindings: readonly DesignRequestBinding[]) {
  storage.setItem(DESIGN_REQUEST_BINDINGS_KEY, JSON.stringify({ version: 1, bindings }));
}

/** Pair one gallery origin and project to an existing workstream, replacing any earlier pairing. */
export function pairDesignRequestBinding(
  storage: DesignRequestBindingStorage,
  input: {
    readonly galleryOrigin: string;
    readonly projectKey: string;
    readonly workstreamId: string;
  },
  newId: () => string = designRequestNonce,
): DesignRequestBinding {
  const binding: DesignRequestBinding = { bindingId: newId(), ...input };
  const others = readDesignRequestBindings(storage).filter(
    (item) => item.galleryOrigin !== input.galleryOrigin || item.projectKey !== input.projectKey,
  );
  write(storage, [...others, binding].slice(-200));
  return binding;
}

export function removeDesignRequestBinding(
  storage: DesignRequestBindingStorage,
  bindingId: string,
) {
  write(
    storage,
    readDesignRequestBindings(storage).filter((item) => item.bindingId !== bindingId),
  );
}
