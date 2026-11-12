import type {
  DesktopCompanionBridge,
  DesktopCompanionState,
  DesktopCompanionTicketRequest,
  DesktopCompanionTicketResponse,
} from "@t3tools/contracts";

export function subscribeCompanionController(input: {
  readonly bridge: DesktopCompanionBridge;
  readonly state: (state: DesktopCompanionState | null) => void;
  readonly ticket: (
    request: DesktopCompanionTicketRequest,
  ) => Promise<DesktopCompanionTicketResponse["result"]>;
  readonly notice: Parameters<DesktopCompanionBridge["onNotice"]>[0];
}): () => void {
  let live = true;
  let stateVersion = 0;
  let ticketVersion = 0;
  const configKey = (state: DesktopCompanionState | null) => JSON.stringify(state?.config);
  let current: DesktopCompanionState | null = null;
  const update = (state: DesktopCompanionState) => {
    if (!live) return;
    stateVersion++;
    current = state;
    input.state(state);
  };
  const unstate = input.bridge.onState(update);
  const unticket = input.bridge.onTicketRequest((request) => {
    const config = current?.config;
    const key = configKey(current);
    const version = ++ticketVersion;
    if (!live || !config?.enabled || config.environmentId !== request.environmentId) return;
    void input
      .ticket(request)
      .then(async (result) => {
        if (!live || configKey(current) !== key || ticketVersion !== version) return;
        await input.bridge.completeTicket({ ...request, result });
      })
      .catch(() => undefined);
  });
  const unnotice = input.bridge.onNotice(input.notice);

  void input.bridge
    .getState()
    .then((state) => {
      if (live && stateVersion === 0) update(state);
    })
    .catch(() => undefined)
    .finally(() => {
      if (live) void input.bridge.setTicketProviderReady(true).catch(() => undefined);
    });
  return () => {
    live = false;
    unstate();
    unticket();
    unnotice();
    input.state(null);
    void input.bridge.setTicketProviderReady(false).catch(() => undefined);
  };
}
