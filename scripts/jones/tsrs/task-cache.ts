function trackedFiles() {
  return [
    { auto: true as const },
    { pattern: "!*.tsbuildinfo", base: "package" as const },
    { pattern: "!**/.jones-tsrs-*.tsbuildinfo", base: "workspace" as const },
    { pattern: "!node_modules/.cache/jones/tsrs/build-info/v1/**", base: "workspace" as const },
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
