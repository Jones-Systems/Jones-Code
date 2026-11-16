import * as Context from "effect/Context";
import * as Layer from "effect/Layer";
/** Shared independently of intake construction so receipt readers never depend on intake. */
export class ReceiverClaimTracker extends Context.Service<ReceiverClaimTracker, {
  readonly has:(id:string) => boolean;readonly add:(id:string) => void;readonly delete:(id:string) => void;
}>()('jones/remoteAdmissionIntegration/ReceiverClaimTracker') {}
export const claimTrackingLayer=Layer.sync(ReceiverClaimTracker,() => {
  const claims=new Set<string>();return {has:id => claims.has(id),add:id => {claims.add(id);},delete:id => {claims.delete(id);}};
});
