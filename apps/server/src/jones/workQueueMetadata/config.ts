// @effect-diagnostics nodeBuiltinImport:off - Server-only configuration must validate absolute paths using the same native path semantics as the artifact reader.
import { WorkQueueMetadataSource } from "@t3tools/contracts";
import * as Schema from "effect/Schema";
import * as NodePath from "node:path";

export type WorkQueueMetadataConfig =
  | { readonly status: "unconfigured" | "invalid" }
  | {
      readonly status: "configured";
      readonly path: string;
      readonly source: WorkQueueMetadataSource;
      readonly maxAgeMs: number;
    };

const decodeSource = Schema.decodeUnknownSync(WorkQueueMetadataSource);

export function workQueueMetadataConfig(
  env: Readonly<Record<string, string | undefined>>,
): WorkQueueMetadataConfig {
  const names = [
    "PATH",
    "QUEUE_ID",
    "HOST_ID",
    "ENVIRONMENT_REF",
    "EXPORTER_INSTANCE_ID",
    "MAX_AGE_MS",
  ] as const;
  const values = names.map((name) => env[`T3CODE_WORK_QUEUE_METADATA_${name}`]);
  if (values.every((value) => value === undefined)) return { status: "unconfigured" };
  const [path, queue_id, host_id, environment_ref, exporter_instance_id, age] = values;
  try {
    if (!path || !NodePath.isAbsolute(path) || (age !== undefined && !/^[1-9][0-9]*$/.test(age)))
      return { status: "invalid" };
    const maxAgeMs = age === undefined ? 30_000 : Number(age);
    if (!Number.isSafeInteger(maxAgeMs) || maxAgeMs < 1 || maxAgeMs > 86_400_000)
      return { status: "invalid" };
    const source = decodeSource({
      queue_id,
      host_id,
      environment_ref,
      exporter_instance_id,
    });
    return { status: "configured", path, source, maxAgeMs };
  } catch {
    return { status: "invalid" };
  }
}
