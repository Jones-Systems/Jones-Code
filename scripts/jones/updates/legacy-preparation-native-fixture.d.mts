/** Data validation and observation only; these exports grant no producer or cold-owner authority. */
export interface PreparationNativeInput {
  readonly schema: 1;
  readonly classification: "synthetic-preparation-native-input";
  readonly sourceSha: string;
  readonly sourceTree: string;
  readonly nativeSourceSha256: string;
  readonly qualifierSourceHashes: Readonly<Record<string, string>>;
  readonly files: Readonly<
    Record<
      "executable" | "node" | "native" | "entry" | "seaConfig" | "provenance",
      {
        readonly path: string;
        readonly sha256: string;
        readonly size: number;
      }
    >
  >;
  readonly preparation: {
    readonly schema: 1;
    readonly protocol: 1;
    readonly sourceInputHashes: Readonly<Record<string, string>>;
    readonly executable: { readonly name: string; readonly sha256: string; readonly size: number };
    readonly entry: { readonly sha256: string; readonly size: number };
    readonly seaConfig: { readonly sha256: string; readonly size: number };
    readonly provenanceSha256: string;
    readonly node: {
      readonly version: "26.8.2";
      readonly platform: "darwin";
      readonly architecture: "arm64";
      readonly runtimeSha256: string;
      readonly runtimeBytes: number;
    };
    readonly sqlite: { readonly version: string; readonly sourceId: string };
  };
}
export const preparationNativeCases: readonly string[];
export const preparationNativeSourceInputs: readonly string[];
export const preparationNativeQualifierSourceInputs: readonly string[];
export function validatePreparationNativeInput(input: unknown): PreparationNativeInput;
export function validatePreparationNativeResult(result: unknown, input: unknown): void;
export function readPreparationNativeQualifierSourceHashes(
  directory?: string,
  signal?: AbortSignal,
): Promise<Readonly<Record<string, string>>>;
export function runLegacyPreparationNativeFixture(options?: {
  readonly input?: unknown;
  readonly signal?: AbortSignal;
  readonly temporaryDirectory?: string;
}): Promise<unknown>;
export class PreparationNativeUnavailable extends Error {
  constructor(reason: string);
  readonly code: "JONES_PREPARATION_QUALIFICATION_UNAVAILABLE";
}

export function validatePreparationNativeFailure(value: unknown): Readonly<Record<string, unknown>>;
export function publishPreparationNativeFailure(
  root: string,
  primary: unknown,
  completedCases?: number,
  options?: { readonly signal?: AbortSignal; readonly publicationTimeoutMs?: number },
): Promise<
  Error & {
    readonly failureEvidencePath: string;
    readonly failureEvidence: Readonly<Record<string, unknown>>;
  }
>;
