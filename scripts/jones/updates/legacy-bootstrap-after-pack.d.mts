import type {
  JonesLegacyBootstrapBundleInput,
  JonesLegacyBootstrapNativeProof,
  JonesLegacyBootstrapPreparationProof,
  JonesLegacyBootstrapPreparationBundle,
} from "./legacy-bootstrap-bundle.ts";

export function readPackagedBootstrapProof(
  raw: string,
  identity: JonesLegacyBootstrapBundleInput,
): JonesLegacyBootstrapNativeProof | JonesLegacyBootstrapPreparationProof;

export function verifyJonesPreparationRuntime(
  filename: string,
  runtime: JonesLegacyBootstrapPreparationBundle["runtime"],
): Promise<void>;

export function copyJonesPreparationRuntime(
  source: string,
  destination: string,
  runtime: JonesLegacyBootstrapPreparationBundle["runtime"],
): Promise<void>;
