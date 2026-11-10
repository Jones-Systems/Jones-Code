import * as Wire from "@t3tools/contracts";

export const workstreamCapabilities = (
  context: Wire.WorkstreamReadContext,
  permissions: Wire.WorkstreamCapabilities["permissions"],
): Wire.WorkstreamCapabilities => ({
  contract_family: Wire.WORKSTREAM_CONTRACT_FAMILY,
  contract_version: Wire.WORKSTREAM_CONTRACT_VERSION,
  manifest_sha256: Wire.WORKSTREAM_CONTRACT_MANIFEST_SHA256,
  context,
  permissions,
  max_request_bytes: 32768,
  max_response_bytes: 1048576,
  max_json_depth: 10,
  max_page_items: 100,
  max_pr_response_bytes: 262144,
  max_pr_request_seconds: 15,
  cursor_ttl_seconds: 900,
});
