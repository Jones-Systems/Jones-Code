import { expect, it } from "vitest";
import type { RemoteAdmissionRecord } from "@t3tools/contracts";
import { originalReturnBinding } from "./returnBinding.ts";
const envelope:RemoteAdmissionRecord['envelope']={identity:{clientId:'receiver-session',linkId:'receiver-session'},operation:'t3_thread_launch',
  target:{projectId:'project',threadId:null,runtimeMode:'auto',interactionMode:'default'},receiverPolicy:{enabled:false,revision:'initial',modelSelection:null},
  payload:{params:{clientRequestId:'forwarded-key',rootObservationKey:'untrusted',returnRouteKey:'untrusted'},
    invocation:{environmentId:'receiver',requestNamespace:'untrusted:namespace',client:{sessionId:'receiver-session',linked:true,
      remoteOrigin:{linkSessionId:'receiver-session',thread:{threadId:'trusted-origin'}}}}}};
it('derives the exact origin root from receiver identity and forwarded key; ignores caller roots/namespaces',() => {
  expect(originalReturnBinding({envelope})).toEqual({rootObservationKey:'origin:receiver:forwarded-key',returnRouteKey:'origin:receiver:forwarded-key',originThreadId:'trusted-origin'});
});
it('never binds a root from a foreign session or non-linked metadata',() => {
  expect(originalReturnBinding({envelope:{...envelope,identity:{clientId:'different',linkId:'different'}}})).toBeNull();
  expect(originalReturnBinding({envelope:{...envelope,payload:{params:{clientRequestId:'key'},invocation:{environmentId:'receiver',client:{sessionId:'receiver-session',linked:false}}}}})).toBeNull();
});
