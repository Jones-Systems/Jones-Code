// Installed updaters accept only the established runtime roots. Keep bundled
// system libraries inside node_modules so those updaters can stage new builds.
const directory = "node_modules/@jones-code/runtime-libs";

export const linuxLibatomicLayout = {
  directory,
  file: `${directory}/libatomic.so.1`,
  runpath: `$ORIGIN/${directory}`,
} as const;
