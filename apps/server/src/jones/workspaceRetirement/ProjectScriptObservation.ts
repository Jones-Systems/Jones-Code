// @effect-diagnostics nodeBuiltinImport:off -- A bounded optional terminal observer; command issuance remains the native terminal owner's effect.
import { scheduler } from "node:timers/promises";
import * as NodeCrypto from "node:crypto";
import type { TerminalWriteInput } from "@t3tools/contracts";
import type { PtyProcess } from "@t3tools/shared/PtyAdapter";
import type { DutyIdentity } from "./DutyIdentity.ts";
import {
  completionSentinel,
  completionSentinelPattern,
  stripTerminalControl,
  wrapCommandForCompletion,
  type CompletionShell,
} from "./ScriptCompletionProtocol.ts";
import {
  completionObservation,
  recordCompletion,
  type CaptureResult,
  type CompletionObservation,
} from "./CompletionObservation.ts";

type Descriptor = NonNullable<TerminalWriteInput["observation"]>;
interface Invocation {
  signature: string;
  nativeError?: unknown;
  hasNativeError: boolean;
  entered: boolean;
  completed: boolean;
  detach: () => void;
}
interface NativeTerminal {
  process: Pick<PtyProcess, "pid" | "onData" | "onExit">;
  threadId: string;
  terminalId: string;
  launchCwd: string;
  observedCwd: string | null;
  shell: string | null | undefined;
  physical: DutyIdentity | undefined;
  isCurrent: () => boolean;
}
export interface ObserverOptions {
  capture?: (observation: CompletionObservation) => CaptureResult;
  diagnostic?: (reason: string) => void;
  nonce?: () => string;
}
function shellOf(name: string | null | undefined): CompletionShell | null {
  const shell = name?.split(/[\\/]/).at(-1)?.toLowerCase();
  if (shell === "fish") return "fish";
  if (shell === "pwsh" || shell === "powershell" || shell === "pwsh.exe") return "powershell";
  return ["sh", "bash", "zsh", "dash", "ksh"].includes(shell ?? "") ? "posix" : null;
}
function validDescriptor(value: Descriptor) {
  return (
    /^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i.test(
      value.invocationId,
    ) &&
    [value.projectId, value.scriptId].every(
      (v) => typeof v === "string" && v.trim().length > 0 && v.length <= 256,
    )
  );
}

export function makeProjectScriptObserver(options: ObserverOptions = {}) {
  const capture = options.capture ?? recordCompletion;
  const states = new WeakMap<
    object,
    { seen: Map<string, Invocation>; active: Invocation | null }
  >();
  const diagnostic = (reason: string) => {
    try {
      options.diagnostic?.(reason);
    } catch {
      /* Optional diagnostics cannot affect native work. */
    }
  };
  return {
    write(input: TerminalWriteInput, terminal: NativeTerminal, submit: (data: string) => void) {
      const descriptor = input.observation;
      if (!descriptor) return submit(input.data);
      let state = states.get(terminal.process);
      if (!state) {
        state = { seen: new Map(), active: null };
        states.set(terminal.process, state);
      }
      // This hashes command bytes solely for invocation identity, never a file,
      // output tree, allocation baseline or producer proof. No command is retained.
      let signature: string;
      try {
        signature = NodeCrypto.createHash("sha256")
          .update(JSON.stringify([descriptor, input.data]))
          .digest("hex");
      } catch {
        diagnostic("observer_identity_unavailable; ordinary write remains unobserved");
        return submit(input.data);
      }
      const prior = state.seen.get(descriptor.invocationId);
      if (prior) {
        if (prior.signature !== signature) throw new Error("project_script_invocation_changed");
        if (prior.hasNativeError) throw prior.nativeError;
        // An accepted/uncertain issuance is never sent a second time, including
        // while awaiting a late marker after a lost response.
        return;
      }
      // No backlog: at most one listener pair and 4KiB of partial output per
      // terminal. Keep 256 compact issuance receipts through this native lifecycle.
      if (state.seen.size >= 256) {
        diagnostic("observer_history_bound; ordinary write remains unobserved");
        return submit(input.data);
      }
      const invocation: Invocation = {
        signature,
        hasNativeError: false,
        entered: false,
        completed: false,
        detach: () => {},
      };
      state.seen.set(descriptor.invocationId, invocation);
      const issue = (data: string) => {
        invocation.entered = true;
        try {
          submit(data);
        } catch (error) {
          invocation.nativeError = error;
          invocation.hasNativeError = true;
          diagnostic("write_issuance_unknown; no replay");
          throw error;
        }
      };
      const shell = shellOf(terminal.shell);
      const physical = terminal.physical;
      if (
        !validDescriptor(descriptor) ||
        state.active !== null ||
        shell === null ||
        physical?.status !== "bound" ||
        terminal.observedCwd === null ||
        !input.data.endsWith("\r") ||
        !terminal.isCurrent()
      ) {
        diagnostic("observer_unavailable; ordinary write remains unobserved");
        return issue(input.data);
      }
      let data = input.data;
      let unsubscribeData = () => {},
        unsubscribeExit = () => {};
      let deadline: AbortController | undefined;
      const detach = () => {
        deadline?.abort();
        deadline = undefined;
        try {
          unsubscribeData();
        } catch {
          /* A broken observer unsubscribe cannot alter native completion. */
        }
        try {
          unsubscribeExit();
        } catch {
          /* Keep the native owner independent. */
        }
        if (state.active === invocation) state.active = null;
      };
      invocation.detach = detach;
      try {
        const nonce = (options.nonce ?? (() => NodeCrypto.randomBytes(16).toString("hex")))();
        if (!/^[a-f0-9]{32}$/.test(nonce)) throw new Error("observer_nonce_unavailable");
        const hidden = input.data.startsWith(" ");
        const body = input.data.slice(hidden ? 1 : 0, -1);
        data = `${hidden ? " " : ""}${wrapCommandForCompletion(body, shell, completionSentinel(nonce), true)}\r`;
        if (data.length > 65536 || Buffer.byteLength(data) > 65536)
          throw new Error("observer_wrapper_bound");
        const marker = completionSentinelPattern(nonce);
        let partial = "";
        const finish = (exitCode: number | null, reason: string) => {
          if (invocation.completed) return;
          invocation.completed = true;
          detach();
          if (exitCode === null || !invocation.entered || !terminal.isCurrent()) {
            diagnostic(reason);
            return;
          }
          try {
            const result = capture(
              completionObservation({
                producer: "jones-project-script",
                owner: terminal.threadId,
                worktree: physical.binding.path,
                status: exitCode === 0 ? "succeeded" : "failed",
                roots: [],
                lineage: {
                  threadId: terminal.threadId,
                  terminalId: terminal.terminalId,
                  terminalPid: String(terminal.process.pid),
                  launchCwd: terminal.launchCwd,
                  observedCwd: terminal.observedCwd!,
                  invocationId: descriptor.invocationId,
                  requestedProjectId: descriptor.projectId,
                  requestedScriptId: descriptor.scriptId,
                  gitDirectory: physical.binding.gitDirectory,
                  gitCommonDirectory: physical.binding.commonDirectory,
                  rootDevice: physical.binding.dev,
                  rootInode: physical.binding.ino,
                  exitCode: String(exitCode),
                },
                omittedEnrichment: [
                  "output_roots_not_declared",
                  "shell_cwd_observed_before_write_only",
                ],
              }),
            );
            if (result.status !== "recorded")
              diagnostic(result.reason ?? "completion_capture_unavailable");
          } catch {
            diagnostic("completion_capture_unavailable");
          }
        };
        state.active = invocation;
        unsubscribeData = terminal.process.onData((chunk) => {
          if (invocation.completed) return;
          if (chunk.length > 65536) {
            finish(null, "observer_output_bound; observation_unknown");
            return;
          }
          if (!terminal.isCurrent()) {
            finish(null, "terminal_lifecycle_changed");
            return;
          }
          const lines = (partial + chunk).split(/\r\n|\r|\n/);
          partial = (lines.pop() ?? "").slice(-4096);
          for (const line of lines) {
            const cleaned = stripTerminalControl(line).trim();
            const matched = marker.exec(cleaned);
            // Typed/echoed wrappers contain the token, but never a line equal to
            // the actual exit marker. Arbitrary output is not persisted.
            if (!matched || matched.index !== 0 || matched[0] !== cleaned) continue;
            const exit = Number(matched[1]);
            if (Number.isSafeInteger(exit)) finish(exit, "exit_observed");
          }
        });
        unsubscribeExit = terminal.process.onExit(() =>
          finish(null, "terminal_exited_without_command_marker; observation_unknown"),
        );
        if (invocation.completed) {
          detach();
          data = input.data;
        } else {
          deadline = new AbortController();
          // This native timer is owned by the observer. Abort synchronously clears
          // it on every detach; the rejection branch drains cancellation quietly.
          const signal = deadline.signal;
          void scheduler.wait(10 * 60 * 1000, { signal, ref: false }).then(
            () => finish(null, "observer_timeout; observation_unknown"),
            () => {
              if (!signal.aborted)
                finish(null, "observer_timer_unavailable; observation_unknown");
            },
          );
        }
      } catch {
        detach();
        diagnostic("observer_prewrite_unavailable; ordinary write remains unobserved");
        return issue(input.data);
      }
      // No fallback surrounds this call: a throw may follow accepted bytes.
      return issue(data);
    },
    close(process: object) {
      const state = states.get(process);
      if (!state) return;
      state.active?.detach();
      state.active = null;
      diagnostic("terminal_closed; unfinished observation_unknown");
    },
  };
}
