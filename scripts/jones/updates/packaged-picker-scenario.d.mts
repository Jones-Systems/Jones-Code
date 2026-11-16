import type {
  JonesActionsTransport,
  JonesActionsCandidate,
} from "@t3tools/shared/jones/jonesActions";
export const labels: Readonly<Record<string, string>>;
export const bridgeMembers: string[];
type Job = { kind: "prepare"; token: string; candidate: JonesActionsCandidate };
export function createPickerFixture(
  installedSource: string,
  onPrepare?: (job: Job) => void,
): {
  transport: JonesActionsTransport;
  worker: {
    run: (job: unknown, onVerifying?: () => void | Promise<void>) => Promise<never>;
    cancel: (token: string) => Promise<"not-running">;
    reaped: (token: string) => boolean;
    heldWorker: () => undefined;
    close: () => Promise<void>;
  };
  calls: string[];
  jobs: Job[];
  downloads: unknown[];
  builds: unknown[];
  setMode: (mode: string) => void;
  release: () => void;
  rebuild: () => void;
};
export function createPickerObservations(): {
  notify: () => void;
  wait: <T>(predicate: (value: T) => boolean, read: () => Promise<T>) => Promise<T>;
};
export function runPickerScenario(
  phase: string,
  host: unknown,
  fixture: ReturnType<typeof createPickerFixture>,
): Promise<{ phase: string; checks: Array<{ id: string; status: "pass" }> }>;
