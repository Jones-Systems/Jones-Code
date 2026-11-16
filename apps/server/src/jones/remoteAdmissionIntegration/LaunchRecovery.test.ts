import { describe, expect, it } from "vitest";
import { classifyLaunchEvidence, recoveryIdentities, type LaunchEvidenceInput } from "./LaunchRecovery.ts";
const ids = recoveryIdentities("receiver-session", "request");
const base = (): LaunchEvidenceInput => ({ keyed:true,abandonedClaim:true,readsComplete:true,liveClaim:false,
  expectedProjectId:"project",initialMessageRequested:true,observedAt:"2026-11-16T00:00:00.000Z",
  hashed:{commandId:ids.hashed,creation:null,message:null,shell:null,claimMatches:true},
  legacy:{commandId:ids.legacy,creation:null,message:null,shell:null,claimMatches:true} });
const accepted = () => { const input=base(); return {...input,hashed:{...input.hashed,
  creation:{commandId:ids.hashed,threadId:ids.hashed,commandType:"thread.create",status:"accepted" as const}}}; };
describe("receipt-grounded receiver recovery", () => {
  it("uses the receiver namespace and exact initial message command", () => {
    expect(ids.legacy).toBe("mcp:launch:client%3Areceiver-session:request");
    expect(classifyLaunchEvidence(accepted()).evidence.initialMessageCommandId).toBe(`${ids.hashed}:initial-message`);
  });
  it("missing projection confirms creation but never deletion", () => {
    const result=classifyLaunchEvidence(accepted()).result;
    expect(result.outcome).toBe("created"); expect(result.stages.availability).toBe("unknown");
  });
  it("absence proves no thread only for a complete abandoned claim", () => {
    expect(classifyLaunchEvidence(base()).result.outcome).toBe("not_created");
    for (const override of [{abandonedClaim:false},{readsComplete:false},{keyed:false},{liveClaim:true}])
      expect(classifyLaunchEvidence({...base(),...override}).result.outcome).toBe("unknown");
  });
  it("contradictions beat positive evidence", () => {
    const input=accepted();
    expect(classifyLaunchEvidence({...input,legacy:{...input.legacy,shell:{id:ids.legacy,projectId:"project",deletedAt:null}}}).result.outcome).toBe("unknown");
    expect(classifyLaunchEvidence({...input,hashed:{...input.hashed,claimMatches:false}}).result.outcome).toBe("unknown");
    expect(classifyLaunchEvidence({...input,hashed:{...input.hashed,shell:{id:ids.hashed,projectId:"wrong",deletedAt:null}}}).result.outcome).toBe("unknown");
  });
  it("positive deletedAt is distinct from absence", () => {
    const input=accepted(); const result=classifyLaunchEvidence({...input,hashed:{...input.hashed,
      shell:{id:ids.hashed,projectId:"project",deletedAt:"2026-11-16T01:00:00Z"}}}).result;
    expect(result.outcome).toBe("created");expect(result.stages.availability).toBe("deleted");
  });
  it("message receipt without creation is unknown", () => {
    const input=base();expect(classifyLaunchEvidence({...input,hashed:{...input.hashed,message:{commandId:`${ids.hashed}:initial-message`,
      threadId:ids.hashed,commandType:"message.dispatch",status:"accepted"}}}).result.outcome).toBe("unknown");
  });
});
