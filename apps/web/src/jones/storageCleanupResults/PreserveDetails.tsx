import type { WorkspaceRetirementOutcome } from "@t3tools/contracts";
import { Badge } from "../../components/ui/badge";
import { Button } from "../../components/ui/button";
import { namedClipboardTarget, useCopyToClipboard } from "../../hooks/useCopyToClipboard";
import {
  proposalCopy,
  readbackLabel,
  retirementReasonLabel,
  shortSha,
  verificationLabel,
} from "./retirementCopy";

type Retirement = WorkspaceRetirementOutcome;

/** What Retire will do, stated before the click: a preserve branch, its remote, and the forced removal. */
export function ProposalRecoverability({ retirement }: { retirement: Retirement }) {
  const copy = proposalCopy(retirement.recoverability);
  if (!copy) return null;
  return (
    <span className="block space-y-1" data-testid="retirement-proposal-copy" data-kind={copy.kind}>
      {copy.lines.map((line) => (
        <span key={line} className="block">
          {line}
        </span>
      ))}
      {copy.kind === "excluded" && (
        <span className="block">
          {copy.excluded.map(({ path, label }) => (
            <span key={path} className="block">
              <code className="text-foreground/80">{path}</code>: {label}
            </span>
          ))}
        </span>
      )}
    </span>
  );
}

/** Human labels for the preserve reason codes a held or failed retirement reports. */
export function ReasonLabels({ codes }: { codes: readonly string[] }) {
  const labelled = codes.flatMap((code) => {
    const label = retirementReasonLabel(code);
    return label ? [{ code, label }] : [];
  });
  if (labelled.length === 0) return null;
  return (
    <span className="block space-y-1">
      {labelled.map(({ code, label }) => (
        <span key={code} className="block" data-reason-code={code}>
          {label}
        </span>
      ))}
    </span>
  );
}

/** The branch that kept this checkout's uncertain content, with the command that restores it. */
export function PreserveReceipt({ preserve }: { preserve: NonNullable<Retirement["preserve"]> }) {
  const { copyToClipboard, isCopied } = useCopyToClipboard({
    target: namedClipboardTarget("restore command"),
    timeout: 1_200,
  });
  return (
    <span
      className="block space-y-1 border-l border-border/60 pl-3"
      data-testid="retirement-preserve-receipt"
    >
      <span className="flex flex-wrap items-center gap-2">
        <span className="font-medium text-foreground/80">Saved to branch</span>
        <code className="text-foreground/80">{preserve.branch}</code>
        <code>{shortSha(preserve.sha)}</code>
        <Badge variant={preserve.verification === "remote_verified" ? "success" : "warning"}>
          {verificationLabel(preserve.verification)}
        </Badge>
        <Badge variant={preserve.forced ? "warning" : "outline"}>
          {preserve.forced ? "Removed with --force after verification" : "Removed without --force"}
        </Badge>
      </span>
      <span className="block">
        Remote:{" "}
        {preserve.remote === null
          ? "none: this machine's repository holds the only copy"
          : preserve.remote}
      </span>
      <span className="block">Readback: {readbackLabel(preserve.readback)}</span>
      <span className="block">To restore this checkout:</span>
      <span className="flex flex-wrap items-center gap-2">
        <code className="select-text text-foreground/80">{preserve.restore}</code>
        <Button variant="outline" size="compact" onClick={() => copyToClipboard(preserve.restore)}>
          {isCopied ? "Copied" : "Copy restore command"}
        </Button>
      </span>
      {preserve.excluded.length > 0 && (
        <span className="block">
          Not saved: {preserve.excluded.map(({ path }) => path).join(", ")}
        </span>
      )}
      <span className="block">Preserve branches are never deleted automatically.</span>
    </span>
  );
}
