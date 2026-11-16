# Host status navigation

[Map index and shared recipes](README.md). The shared web/desktop chat header
uses [GlobalHostStatusIndicators](../../apps/web/src/jones/hostStatus/GlobalHostStatusIndicators.tsx)
for both draft and server routes. Its selected gateway and retained samples
belong to the app session, independently of the displayed thread environment.
The [thread/chat map](thread-chat.md) owns other entry paths.

## Journey and focused evidence

Use an isolated app with a configured telemetry gateway or task-owned synthetic
`/api/host-status` responses. After all four chips show readings, delay the next
response and switch between a server thread and a draft using the sidebar and
**New thread**. Fresh values should remain visible during the pending response;
navigation within the polling interval should not issue another telemetry
request. Also hide/show the tab within that interval, open/close a chat panel,
and navigate to Settings and back. Check source removal separately: it must
discard retained readings. Hold a response beyond the sample's 30-second
deadline to verify that retained values become unavailable rather than staying
apparently current. Restore the fixture route and remove only task-owned response
overrides when finished.

Verified on **2026-10-20** in the shared web renderer: server-thread to draft
navigation, the details panel toggle, and Settings/back retained all four
readings without a gray frame. The first thread-to-draft switch added no HTTP
request. The isolated fixture supplied four synthetic host samples with a
1.2-second response delay; a DOM mutation observer checked the chips during
navigation. Browser viewport was 1280×800, device scale 2, light theme. The
candidate was served by `vp run dev` from base
`ae750c43299f6d66a7406abd890143d26b1f15f3`; the combined Git diff of the three
telemetry source files against that base has SHA-256
`5032ad7944f70da7190090682cc38bc6508f50707d8f8f123c307dec86fe150c`.
The change PR retains before/after images and recordings with the final commit.
This source/dev-server correspondence is not packaged-build attestation.
Visibility, expiry, source removal and cancellation have focused automated
coverage; native shells, real cross-host transports, and other entry paths
retain their separate verification needs.
