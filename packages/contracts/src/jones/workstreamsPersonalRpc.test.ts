import * as Schema from "effect/Schema";
import { describe, expect, it } from "vite-plus/test";
import { ExecutionEnvironmentCapabilities } from "../environment.ts";
import { WsRpcGroup } from "../rpc.ts";
import { ThreadRegistryComposedThread } from "../threadRegistry.ts";
import {
  AssignPersonalWorkstreamRpc,
  PutPersonalWorkstreamRpc,
  SubscribePersonalWorkstreamsRpc,
  WorkstreamsPersonalError,
} from "./workstreamsPersonalRpc.ts";

const decodeSubscribe = Schema.decodeSync(SubscribePersonalWorkstreamsRpc.payloadSchema);
const decodeAssign = Schema.decodeSync(AssignPersonalWorkstreamRpc.payloadSchema);
const isAssign = Schema.is(AssignPersonalWorkstreamRpc.payloadSchema);
const isPut = Schema.is(PutPersonalWorkstreamRpc.payloadSchema);
const decodeError = Schema.decodeUnknownSync(WorkstreamsPersonalError);
const encodeError = Schema.encodeSync(WorkstreamsPersonalError);
const decodeCapabilities = Schema.decodeUnknownSync(ExecutionEnvironmentCapabilities);
const decodeThread = Schema.decodeSync(ThreadRegistryComposedThread);
const isThread = Schema.is(ThreadRegistryComposedThread);

describe("personal Workstream RPC compatibility", () => {
  it("registers all three public methods and requires assignment revisions", () => {
    expect(WsRpcGroup.requests.get("workstreamsPersonal.subscribe")).toBeDefined();
    expect(WsRpcGroup.requests.get("workstreamsPersonal.put")).toBeDefined();
    expect(WsRpcGroup.requests.get("workstreamsPersonal.assign")).toBeDefined();
    expect(decodeSubscribe({})).toEqual({});
    const input = { threadId: "thread-1", workstream: null, expectedRevision: 0 };
    expect(decodeAssign(input)).toEqual(input);
    expect(isAssign({ ...input, expectedRevision: -1 })).toBe(false);
    expect(isPut({ workstream: null })).toBe(false);
  });

  it("carries the current assignment on a precondition failure", () => {
    const error = {
      _tag: "WorkstreamsPersonalError",
      code: "precondition_failed",
      currentAssignment: {
        threadId: "thread-1",
        workstreamId: "other-group",
        revision: 3,
        updatedAt: "2026-11-12T12:00:00.000Z",
      },
    };
    const decoded = decodeError(error);
    expect(encodeError(decoded)).toEqual(error);
  });

  it("keeps the capability absent on old servers and gates it to version one", () => {
    const decode = decodeCapabilities;
    expect(decode({}).workstreamsPersonal).toBeUndefined();
    expect(decode({ workstreamsPersonal: { version: 1 } }).workstreamsPersonal).toEqual({
      version: 1,
    });
    expect(() => decode({ workstreamsPersonal: { version: 2 } })).toThrow();
  });
});

describe("Voice personal membership compatibility", () => {
  const thread = {
    thread_key: "thread-1",
    registration: null,
    summary: null,
    activity: null,
    associations: [],
    freshness: {},
    native_memberships: [],
  };

  it("decodes an older composed thread without a personal membership envelope", () => {
    const decoded = decodeThread(thread);
    expect(decoded).toEqual(thread);
    expect(decoded).not.toHaveProperty("personal_memberships");
  });

  it("carries personal association and revision without a native placement proof", () => {
    const current = {
      ...thread,
      personal_memberships: [
        {
          workstream_ref: "native:legacy:group-1",
          workstream_id: "legacy:group-1",
          name: "Example",
          environment_id: "environment-1",
          thread_id: "thread-1",
          revision: 1,
        },
      ],
    };
    expect(decodeThread(current)).toEqual(current);
    expect(
      isThread({
        ...current,
        personal_memberships: [{ ...current.personal_memberships[0], revision: -1 }],
      }),
    ).toBe(false);
    expect(
      decodeThread({
        ...thread,
        native_memberships: [{ workstream_ref: "native:legacy", placement: { historical: true } }],
      }).native_memberships,
    ).toEqual([{ workstream_ref: "native:legacy", placement: { historical: true } }]);
  });
});
