function trackedFiles() {
  return [
    { auto: true as const },
    { pattern: "!*.tsbuildinfo", base: "package" as const },
    { pattern: "!node_modules/.cache/jones/tsrs/patch.lock", base: "workspace" as const },
    { pattern: "!node_modules/.cache/jones/tsrs/patch.reclaim-*", base: "workspace" as const },
  ];
}

export function typecheckTask(command: string) {
  return {
    command,
    cache: { input: trackedFiles(), output: trackedFiles() },
  };
}
