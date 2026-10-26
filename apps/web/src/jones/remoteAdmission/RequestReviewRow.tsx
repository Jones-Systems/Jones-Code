import type {
  EnvironmentId,
  RemoteAdmissionDecision,
  RemoteAdmissionRecord,
} from "@t3tools/contracts";
import { useClientSettings } from "../../hooks/useSettings";
import { RequestCard } from "./RequestCard";
import { admissionPresentation } from "./presentation";
import { useRequestTarget } from "./requestTarget";

export function RequestReviewRow({
  environmentId,
  record,
  policyRevision,
  blocked,
  onDecide,
}: {
  readonly environmentId: EnvironmentId;
  readonly record: RemoteAdmissionRecord;
  readonly policyRevision: string;
  readonly blocked: string | null;
  readonly onDecide: (input: RemoteAdmissionDecision) => void;
}) {
  const target = useRequestTarget(environmentId, record);
  const timestampFormat = useClientSettings((settings) => settings.timestampFormat);
  return (
    <RequestCard
      record={record}
      view={admissionPresentation(record)}
      target={target}
      timestampFormat={timestampFormat}
      policyRevision={policyRevision}
      blocked={blocked}
      onDecide={onDecide}
    />
  );
}
