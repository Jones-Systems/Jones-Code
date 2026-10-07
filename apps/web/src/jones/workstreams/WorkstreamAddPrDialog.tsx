import { workstreamBindingKey } from "@t3tools/client-runtime/state/workstreams";
import { useEffect, useRef, useState } from "react";
import {
  WorkstreamActionError,
  workstreamFailureMessage,
  type WorkstreamReferenceController,
} from "./workstreamActionSnapshot";
import { Button } from "../../components/ui/button";
import {
  Dialog,
  DialogDescription,
  DialogFooter,
  DialogHeader,
  DialogPanel,
  DialogPopup,
  DialogTitle,
} from "../../components/ui/dialog";
import { Input } from "../../components/ui/input";
import { canEditWorkstreams } from "../../components/workstreams/nativeWorkstreamActions";
import { parseWorkstreamPrUrl, prepareWorkstreamPr } from "./workstreamReferenceActions";

export function WorkstreamAddPrDialog(props: {
  readonly controller: WorkstreamReferenceController;
  readonly workstreamId: string;
  readonly open: boolean;
  readonly onOpenChange: (open: boolean) => void;
  readonly onLinked: () => void;
  readonly commandId: () => Promise<string>;
}) {
  const [url, setUrl] = useState("");
  const [prepared, setPrepared] = useState(false);
  const [pending, setPending] = useState<"register" | "verify" | "retry" | null>(null);
  const [needsReconciliation, setNeedsReconciliation] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const busy = useRef(false);
  const input = useRef<HTMLInputElement | null>(null);
  const request = useRef<AbortController | null>(null);
  const session = useRef(0);
  const binding = props.controller.data
    ? workstreamBindingKey({ ...props.controller.data.binding, registryVersion: 0 })
    : null;
  useEffect(() => {
    session.current += 1;
    request.current?.abort();
    setUrl("");
    setPrepared(false);
    setNeedsReconciliation(false);
    setError(null);
    setPending(null);
    busy.current = false;
    if (props.open) input.current?.focus();
    return () => {
      session.current += 1;
      request.current?.abort();
    };
  }, [props.open, props.workstreamId, binding]);
  let valid = false;
  try {
    parseWorkstreamPrUrl(url);
    valid = true;
  } catch {}
  const available =
    canEditWorkstreams(props.controller.data) &&
    !props.controller.loading &&
    props.controller.registrationContext?.state === "ready" &&
    props.controller.registrationContext.sources.filter((source) => source.provider === "github")
      .length === 1;
  const run = (phase: "register" | "verify" | "retry") => {
    if (busy.current || (phase !== "retry" && (needsReconciliation || !available || !valid))) return;
    busy.current = true;
    setPending(phase);
    setError(null);
    const started = session.current;
    const abort = new AbortController();
    request.current = abort;
    void (async () => {
      if (phase === "retry") {
        await props.controller.retry();
        if (started === session.current && !abort.signal.aborted) setNeedsReconciliation(false);
        return;
      }
      await prepareWorkstreamPr({
        controller: props.controller,
        url,
        workstreamId: props.workstreamId,
        commandId: props.commandId,
        verify: phase === "verify",
        signal: abort.signal,
      });
      if (started !== session.current || abort.signal.aborted) return;
      if (phase === "register") setPrepared(true);
      else {
        props.onLinked();
        props.onOpenChange(false);
      }
    })()
      .catch((cause: unknown) => {
        if (started === session.current && !abort.signal.aborted) {
          if (!(cause instanceof WorkstreamActionError) || cause.reason === "unknown")
            setNeedsReconciliation(true);
          setError(workstreamFailureMessage(cause));
        }
      })
      .finally(() => {
        if (started === session.current) {
          busy.current = false;
          setPending(null);
        }
      });
  };
  return (
    <Dialog
      open={props.open}
      onOpenChange={(open) => {
        if (!busy.current) props.onOpenChange(open);
      }}
    >
      <DialogPopup showCloseButton={pending === null}>
        <DialogHeader>
          <DialogTitle>Add PR reference</DialogTitle>
          <DialogDescription>
            Register a GitHub pull request, then verify it to link it to this Workstream.
          </DialogDescription>
        </DialogHeader>
        <DialogPanel>
          <Input
            ref={input}
            aria-label="GitHub pull request URL"
            placeholder="https://github.com/owner/repository/pull/42"
            value={url}
            disabled={pending !== null || prepared || needsReconciliation}
            onChange={(event) => {
              setUrl(event.target.value);
              setError(null);
            }}
            onKeyDown={(event) => {
              if (event.key === "Enter") {
                event.preventDefault();
                run(prepared ? "verify" : "register");
              }
            }}
          />
          {!available ? (
            <p className="pt-2 text-xs text-muted-foreground">
              GitHub reference preparation is unavailable until its source is activated.
            </p>
          ) : null}
          {prepared ? (
            <p role="status" className="pt-2 text-xs text-muted-foreground">
              Reference prepared. Verify PR to link it.
            </p>
          ) : null}
          {pending ? (
            <p role="status" className="pt-2 text-xs text-muted-foreground">
              {pending === "register"
                ? "Registering PR reference…"
                : pending === "verify"
                  ? "Verifying and linking PR…"
                  : "Reloading metadata and checking existing commands…"}
            </p>
          ) : null}
          {error ? (
            <p role="alert" className="pt-2 text-xs text-destructive">
              {error}{" "}
              <button type="button" disabled={pending !== null} onClick={() => run("retry")}>
                Retry
              </button>
            </p>
          ) : null}
        </DialogPanel>
        <DialogFooter>
          <Button
            type="button"
            size="sm"
            variant="outline"
            disabled={pending !== null}
            onClick={() => props.onOpenChange(false)}
          >
            Cancel
          </Button>
          <Button
            type="button"
            size="sm"
            disabled={pending !== null || needsReconciliation || !available || !valid}
            onClick={() => run(prepared ? "verify" : "register")}
          >
            {prepared ? "Verify PR" : "Add reference"}
          </Button>
        </DialogFooter>
      </DialogPopup>
    </Dialog>
  );
}
