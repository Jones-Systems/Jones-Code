import type {
  EnvironmentId,
  TokenAccountingMetric,
  TokenAccountingReadInput,
  TokenAccountingReadResult,
  TokenAccountingUnavailable,
} from "@t3tools/contracts";
import type { AtomCommandResult } from "@t3tools/client-runtime/state/runtime";
import { useLayoutEffect, useRef, useState } from "react";

export interface AccountingEnvironment {
  readonly environmentId: EnvironmentId;
  readonly label: string;
  readonly primary: boolean;
  readonly connected: boolean;
  readonly supported: boolean;
}

export function selectAccountingEnvironment(
  environments: readonly AccountingEnvironment[],
  selected: EnvironmentId | null,
): AccountingEnvironment | null {
  const eligible = environments.filter(
    (environment) => environment.connected && environment.supported,
  );
  return (
    eligible.find((environment) => environment.environmentId === selected) ??
    eligible.find((environment) => environment.primary) ??
    (eligible.length === 1 ? eligible[0]! : null)
  );
}

export function formatAccountingCount(value: number): string {
  if (value >= 1e9)
    return `${(value / 1e9).toLocaleString("en-US", { maximumFractionDigits: 1 })}B`;
  if (value >= 1e6)
    return `${(value / 1e6).toLocaleString("en-US", { maximumFractionDigits: 2 })}M`;
  return value.toLocaleString("en-US");
}

export function formatAccountingMetric(metric: TokenAccountingMetric): string {
  if (metric.total !== null) return formatAccountingCount(metric.total);
  if (metric.known_sum !== null)
    return `Known: ${formatAccountingCount(metric.known_sum)} · total unknown`;
  return "Unknown";
}

export const ACCOUNTING_TRANSPORT_ERROR =
  "The saved report could not be read. Try again when connected.";

const UNAVAILABLE_MESSAGES: Record<TokenAccountingUnavailable["reason"], string> = {
  reader_unconfigured: "A saved accounting reader has not been configured for this environment.",
  report_unconfigured: "A saved accounting report has not been selected for this environment.",
  host_binding_unverified:
    "The saved accounting reader is unavailable until its host binding is verified.",
  configured_report_missing: "The configured saved report is missing.",
  report_invalid: "The saved report did not pass validation.",
  report_identity_mismatch: "The saved report did not pass its identity check.",
  configured_report_id_mismatch: "The saved report does not match the configured report ID.",
  projection_invalid: "The saved report could not be represented as a valid accounting summary.",
  report_schema_unsupported: "This saved report format is not supported.",
  identity_algorithm_unsupported: "This saved report identity format is not supported.",
  input_too_large: "The saved report exceeds the reader size limit.",
  projection_too_large: "The saved accounting summary exceeds the response size limit.",
  collection_limit_exceeded: "The saved report exceeds the supported collection limits.",
  reader_timeout: "The saved accounting reader did not finish in time.",
  reader_failed: "The saved accounting reader could not read the report.",
};

export function accountingUnavailableMessage(result: TokenAccountingUnavailable): string {
  return UNAVAILABLE_MESSAGES[result.reason];
}

export interface AccountingReadTarget {
  readonly environmentId: EnvironmentId;
  readonly generation: number;
}

type AccountingRead = (target: {
  readonly environmentId: EnvironmentId;
  readonly input: TokenAccountingReadInput;
}) => Promise<AtomCommandResult<TokenAccountingReadResult, unknown>>;

type AccountingReadState =
  | { readonly phase: "idle" | "reading" | "error" }
  | { readonly phase: "observed"; readonly result: TokenAccountingReadResult };

export function useSavedTokenAccounting(target: AccountingReadTarget | null, read: AccountingRead) {
  const key = target === null ? null : `${target.environmentId}:${target.generation}`;
  const [state, setState] = useState<AccountingReadState>({ phase: "idle" });
  const lifetime = useRef({ key: null as string | null, attempt: 0, reading: false });

  useLayoutEffect(() => {
    lifetime.current.key = key;
    lifetime.current.attempt += 1;
    lifetime.current.reading = false;
    setState({ phase: "idle" });
    return () => {
      // A result belongs to one connection generation and one mounted panel.
      lifetime.current.key = null;
      lifetime.current.attempt += 1;
      lifetime.current.reading = false;
    };
  }, [key]);

  async function load() {
    if (target === null || lifetime.current.key !== key || lifetime.current.reading) return;
    const attempt = ++lifetime.current.attempt;
    lifetime.current.reading = true;
    setState({ phase: "reading" });
    const current = () => lifetime.current.key === key && lifetime.current.attempt === attempt;
    try {
      const result = await read({ environmentId: target.environmentId, input: {} });
      if (!current()) return;
      setState(
        result._tag === "Success"
          ? { phase: "observed", result: result.value }
          : { phase: "error" },
      );
    } catch {
      if (current()) setState({ phase: "error" });
    } finally {
      if (current()) lifetime.current.reading = false;
    }
  }

  return { state, load };
}
