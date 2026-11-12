import { Tooltip, TooltipTrigger, TooltipPopup } from "../../components/ui/tooltip";
import { clientServerBuildLabels, type ClientServerBuilds } from "./buildPairs";

export function ClientServerBuildLabels({ builds }: { readonly builds: ClientServerBuilds }) {
  const { clientLabel, serverLabel } = clientServerBuildLabels(builds);
  const clientDetail = `${clientLabel.detail}\n${builds.clientVersion}\nsource ${builds.clientSourceSha ?? "not recorded"}`;
  const serverDetail = `${serverLabel.detail}\n${builds.serverVersion}\nsource ${builds.serverSourceSha ?? "not recorded"}`;
  return (
    <>
      Client{" "}
      <Tooltip>
        <TooltipTrigger render={<span tabIndex={0} aria-label={clientDetail} />}>
          {clientLabel.text}
        </TooltipTrigger>
        <TooltipPopup side="top">{clientDetail}</TooltipPopup>
      </Tooltip>
      {"; Server "}
      <Tooltip>
        <TooltipTrigger render={<span tabIndex={0} aria-label={serverDetail} />}>
          {serverLabel.text}
        </TooltipTrigger>
        <TooltipPopup side="top">{serverDetail}</TooltipPopup>
      </Tooltip>
    </>
  );
}
