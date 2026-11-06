// Shared nonce/quoting protocol. Setup keeps its existing isolated POSIX scope;
// explicit terminal actions preserve the current shell's state.
const COMPLETION_SENTINEL_PREFIX = "__T3_SETUP_DONE__";
export type CompletionShell = "posix" | "fish" | "powershell";

export function completionSentinel(token: string): string {
  return `${COMPLETION_SENTINEL_PREFIX}_${token}:`;
}

export function completionSentinelPattern(token: string): RegExp {
  return new RegExp(`${COMPLETION_SENTINEL_PREFIX}_${token}:(-?\\d+)`);
}

export function stripTerminalControl(text: string): string {
  return text
    .replace(
      /\x1b\[[0-9;?]*[ -/]*[@-~]|\x1b\][^\x07\x1b]*(?:\x07|\x1b\\)|\x1b[()][A-Za-z0-9]|\x1b[=>]/g,
      "",
    )
    .replace(/[\x00-\x08\x0b\x0c\x0e-\x1f\x7f]/g, "");
}

// A block with its own closing line protects the sentinel from trailing comments, heredocs and scripts reading stdin.
export function wrapCommandForCompletion(
  command: string,
  shell: CompletionShell,
  sentinel: string,
  preserveShellState = false,
): string {
  const body = command.replace(/\r?\n/g, "\r");
  switch (shell) {
    case "powershell":
      return `$global:LASTEXITCODE = $null; ${preserveShellState ? "." : "&"} {\r${body}\r}; if ($null -ne $LASTEXITCODE) { $__t3c = $LASTEXITCODE } elseif ($?) { $__t3c = 0 } else { $__t3c = 1 }; Write-Host "${sentinel}$__t3c"`;
    case "fish":
      return `begin\r${body}\rend; printf '\\n${sentinel}%s\\n' $status`;
    case "posix":
      return `${preserveShellState ? "{ " : "( "}${body}\r${preserveShellState ? "}" : ")"}; printf '\\n${sentinel}%s\\n' "$?"`;
  }
}

