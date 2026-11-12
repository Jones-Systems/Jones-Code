/** Preserve Jones's bounded CLI failure contract, including exit-zero error
 * envelopes. The caller supplies the existing upstream stdout parser. */
export function claudeCommandFailure(input: {
  readonly stdout: string;
  readonly stderr: string;
  readonly exitCode: number;
  readonly reportedError: string | undefined;
}): string | undefined {
  if (input.exitCode === 0 && input.reportedError === undefined) return undefined;
  const detail = input.reportedError ?? (input.stderr.trim() || input.stdout.trim());
  return detail.length > 0
    ? `Claude CLI command failed: ${detail.slice(0, 2_000)}`
    : `Claude CLI command failed with code ${input.exitCode}.`;
}
