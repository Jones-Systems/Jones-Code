import * as Context from "effect/Context";
import type { WorkstreamsNativeRegistry } from "@t3tools/contracts/jones/workstreamsNative";
import type { WorkstreamOperationOrigin } from "@t3tools/contracts/jones/workstreamOperations";

/** Startup selection is independent of persisted binding presence. */
export class NativeHome extends Context.Service<
  NativeHome,
  WorkstreamsNativeRegistry & {
    readonly served_by_environment_id: string;
  }
>()("t3/jones/workstreams/native/mode/authorityMode/NativeHome") {}

/** Only an authenticated entry boundary supplies this captured admission, after checking caller limits. */
export interface AdmittedCaller {
  readonly principal_id: string;
  readonly source_environment_id: string;
  readonly origin: WorkstreamOperationOrigin;
  readonly binding_version: number;
}
