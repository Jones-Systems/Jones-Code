// Local source-test transport only. Packaged verification uses Electron's native hop.
export function createMemoryIpc() {
  const handles = new Map(),
    sync = new Map(),
    listeners = new Map();
  let installCalls = 0;
  return {
    installCalls: () => installCalls,
    main: {
      removeHandler: (channel) => handles.delete(channel),
      handle: (channel, listener) => handles.set(channel, listener),
      removeAllListeners: (channel) => sync.delete(channel),
      on: (channel, listener) => sync.set(channel, listener),
    },
    renderer: {
      invoke: async (channel, ...args) => {
        if (channel === "desktop:update-install") installCalls++;
        if (!handles.has(channel)) throw new Error(`Unregistered channel: ${channel}`);
        return structuredClone(
          await handles.get(channel)({ sender: { id: 1 } }, structuredClone(args[0])),
        );
      },
      sendSync: (channel) => {
        const event = { returnValue: undefined };
        sync.get(channel)?.(event);
        return structuredClone(event.returnValue);
      },
      on: (channel, listener) => {
        const list = listeners.get(channel) ?? new Set();
        list.add(listener);
        listeners.set(channel, list);
      },
      removeListener: (channel, listener) => listeners.get(channel)?.delete(listener),
    },
    send: async (channel, value) => {
      for (const listener of listeners.get(channel) ?? []) listener({}, structuredClone(value));
    },
    channels: () => ({ invoke: [...handles.keys()], sync: [...sync.keys()] }),
  };
}
