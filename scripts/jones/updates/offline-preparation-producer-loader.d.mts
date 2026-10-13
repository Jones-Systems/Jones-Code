import type { QualifiedPreparationStageResult } from "./offline-preparation-build.ts";
import type { JonesLegacyBootstrapBundleInput } from "./legacy-bootstrap-bundle.ts";

interface CaptureFileIdentity {
  readonly device: string;
  readonly inode: string;
  readonly uid: number;
}

/** Diagnostic observations only; the copied tree has already been removed on successful return. */
interface HeldPreparationProducerCapture {
  readonly status: "held";
  readonly reason: "producer-execution-closure-unqualified";
  readonly imported: false;
  readonly qualification: "unqualified";
  readonly completeDependencyClosure: false;
  readonly loadedCollectorBytesAttested: false;
  readonly sourceRoot: string;
  readonly sourceIdentity: CaptureFileIdentity;
  readonly reportedGitIdentity: {
    readonly repository: "Jones-Systems/Jones-Code";
    readonly sha: string;
    readonly tree: string;
    readonly attested: false;
  };
  readonly codeRoot: string;
  readonly codeIdentity: CaptureFileIdentity;
  readonly stageRoot: string;
  readonly stageIdentity: CaptureFileIdentity;
  readonly files: readonly {
    readonly sourcePath: string;
    readonly capturedPath: string;
    readonly size: number;
    readonly sha256: string;
    readonly sourceIdentity: CaptureFileIdentity;
    readonly capturedIdentity: CaptureFileIdentity;
  }[];
  readonly cleanup: { readonly disposition: "removed"; readonly root: string };
}

export function capturePreparationProducer(options: {
  readonly sourceRoot: string;
  readonly stageRoot: string;
  readonly signal?: AbortSignal;
}): Promise<HeldPreparationProducerCapture>;

/** Fixed reviewed capture/build/stage operation; returns evidence, never a reusable capability. */
export function buildAndStagePreparationProducer(options: {
  readonly sourceRoot: string;
  readonly stageRoot: string;
  readonly expectedSource: JonesLegacyBootstrapBundleInput;
  readonly stageResourcesDir: string;
  readonly signal?: AbortSignal;
}): Promise<
  QualifiedPreparationStageResult & {
    readonly handleConsumption: { readonly recreatedRejected: 4; readonly consumedRejected: true };
    readonly cleanup: { readonly disposition: "removed"; readonly root: string };
  }
>;
