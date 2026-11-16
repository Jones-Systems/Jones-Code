export function createMemoryIpc(): {
  installCalls: () => number;
  main: {
    removeHandler: (channel: string) => void;
    handle: (
      channel: string,
      listener: (event: { sender: { id: number } }, raw: unknown) => unknown | Promise<unknown>,
    ) => void;
    removeAllListeners: (channel: string) => void;
    on: (channel: string, listener: (event: { returnValue: unknown }) => void) => void;
  };
  renderer: {
    invoke: (channel: string, ...args: unknown[]) => Promise<unknown>;
    sendSync: (channel: string) => unknown;
    on: (channel: string, listener: (event: unknown, value: unknown) => void) => void;
    removeListener: (channel: string, listener: (event: unknown, value: unknown) => void) => void;
  };
  send: (channel: string, value: unknown) => Promise<void>;
  channels: () => { invoke: string[]; sync: string[] };
};
