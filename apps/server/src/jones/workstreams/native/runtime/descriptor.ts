import type { ExecutionEnvironmentDescriptor } from "@t3tools/contracts";
import type { WorkstreamsNativeCapability } from "@t3tools/contracts/jones/workstreamsNative";
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Option from "effect/Option";

/** Protocol metadata from the selected application graph, never caller or home authority. */
export class NativeDescriptor extends Context.Service<
  NativeDescriptor,
  { readonly read: Effect.Effect<WorkstreamsNativeCapability | undefined> }
>()("t3/jones/workstreams/native/runtime/descriptor/NativeDescriptor") {}

/** Resolve at request time: ServerEnvironment is built before native runtime selection. */
export const annotateDescriptor = Effect.fn("NativeDescriptor.annotateDescriptor")(function* (
  descriptor: ExecutionEnvironmentDescriptor,
) {
  const publisher = yield* Effect.serviceOption(NativeDescriptor);
  if (Option.isNone(publisher)) return descriptor;
  const capability = yield* publisher.value.read;
  return capability === undefined
    ? descriptor
    : {
        ...descriptor,
        capabilities: { ...descriptor.capabilities, workstreamsNative: capability },
      };
});
