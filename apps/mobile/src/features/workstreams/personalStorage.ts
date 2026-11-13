import { writeFileAtomically } from "../../lib/atomic-file";

const DIRECTORY = "workstreams-personal";
const writes = new Map<string, Promise<void>>();

async function storageFile(key: string) {
  const { Directory, File, Paths } = await import("expo-file-system");
  const directory = new Directory(Paths.document, DIRECTORY);
  directory.create({ idempotent: true, intermediates: true });
  return new File(directory, `${encodeURIComponent(key)}.json`);
}

export const personalMobileStorage = {
  getItem: async (key: string): Promise<string | null> => {
    await writes.get(key)?.catch(() => {});
    const file = await storageFile(key);
    return file.exists ? file.text() : null;
  },
  setItem: (key: string, value: string): Promise<void> => {
    // Preserve observation order when native storage finishes writes asynchronously.
    const current = (writes.get(key) ?? Promise.resolve())
      .catch(() => {})
      .then(async () => writeFileAtomically(await storageFile(key), value));
    writes.set(key, current);
    const release = () => {
      if (writes.get(key) === current) writes.delete(key);
    };
    void current.then(release, release);
    return current;
  },
};
