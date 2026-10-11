export function offlinePreparationStatus() {
  return {
    status: "held",
    reason: "maintenance-owner-binding-unqualified",
    databaseOpened: false,
    productionInvocation: false,
    activation: false,
  } as const;
}

/** This entry has no argument, path, fixture or maintenance-authority interface. */
export function offlinePreparationEntryOutput(): string {
  // @effect-diagnostics-next-line preferSchemaOverJson:off -- This fixed native entry serializes only its closed held-status literal, without loading the application or schema runtime.
  return `${JSON.stringify(offlinePreparationStatus())}\n`;
}

// SEA module entries expose import.meta.main without filesystem entry detection.
if (import.meta.main) {
  // Node SEA preserves argv[0]/argv[1] for the executable; user arguments start at index 2.
  if (process.argv.length !== 2) process.exitCode = 64;
  process.stdout.write(offlinePreparationEntryOutput());
}
