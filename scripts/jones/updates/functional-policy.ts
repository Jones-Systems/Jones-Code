export interface FunctionalPreparationGraphBinding {
  readonly policySha256: string;
  readonly graphSha256: string;
  readonly entrySha256: string;
}

/** No captured graph has a positive policy yet; observation alone cannot qualify the producer. */
export function verifyFunctionalPreparationGraph(_input: {
  readonly repoRoot: string;
  readonly reportBytes: Uint8Array;
  readonly entryBytes: Uint8Array;
}): FunctionalPreparationGraphBinding {
  throw new Error("The functional preparation graph policy is unqualified.");
}
