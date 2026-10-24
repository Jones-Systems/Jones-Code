/** Test-only controls across the web/server TypeScript project boundary. */
export interface WireFixture {
  readonly origin: string;
  readonly close: () => Promise<void>;
  readonly control: {
    readonly observations: {
      accepted: number;
      rejected: number;
      submissions: number;
      capabilities: number;
      receiptReads: number;
      refusals: number;
      commandIds: string[];
    };
    readonly authority: { authorization_revision: number };
    readonly faults: {
      changeGrantBeforeSubmit: boolean;
      commandResponse: "valid" | "missing" | "invalid" | "replayed" | "lost";
      receiptResponse: "valid" | "missing";
    };
  };
}
export function startWireFixture(): Promise<WireFixture>;
