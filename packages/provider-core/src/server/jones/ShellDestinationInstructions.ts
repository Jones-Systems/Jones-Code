import { JONES_SHELL_LANGUAGE } from "@t3tools/contracts/jones/shellDestinationFence";

export const JONES_SHELL_DESTINATION_INSTRUCTIONS: string = `
### Commands for a specific environment

For an interactive command the owner should run on a particular environment, use the dedicated \`${JONES_SHELL_LANGUAGE}\` fenced language. Discover your own exact environment ID with \`get_invocation_context\` and peer IDs with \`t3_environment_links\`. Never guess IDs, use an ID prefix, or resolve a target from a label. Labels are display hints only.

The opening fence requires \`environment=<exact ID>\` exactly once. Only optional \`label\` and \`title\` are also supported, each at most once, as space-separated \`key=value\` options. Values containing spaces or quotes must be single- or double-quoted; escape the enclosing quote or a backslash with a backslash. Unicode labels, apostrophes, and dots are allowed in properly quoted values. Unknown, duplicate, malformed, or control-containing metadata is rejected. Use an exact, nonblank environment ID with no surrounding whitespace (at most 1024 characters); all metadata together is limited to 4096 characters.

In this example, REPLACE_WITH_DISCOVERED_ENVIRONMENT_ID is a placeholder, not a real environment ID. Replace it with the exact discovered ID before offering a runnable command:

\`\`\`${JONES_SHELL_LANGUAGE} environment=REPLACE_WITH_DISCOVERED_ENVIRONMENT_ID label="Destination machine" title="Check working directory"
pwd
\`\`\`

Never put target metadata on a \`bash\`, \`sh\`, or plain \`shell\` fence: older clients can execute those commands on the source environment despite the metadata. The dedicated \`${JONES_SHELL_LANGUAGE}\` language keeps older clients from treating the block as a runnable shell command. If the target ID is unknown, use a \`text\` fence and name the intended machine in prose.

Each \`${JONES_SHELL_LANGUAGE}\` block supports one interactive command line. If a working directory is needed, show an explicit \`cd\` in that command line; do not imply the destination inherits the source workspace. The client needs a connection to the destination and \`terminal:operate\` permission there. Do not use a command block to bypass approval. Native terminal prompts remain owner interaction; emitting a block gives the agent no permission and does not answer those prompts. Source delivery does not install this feature on a running client or server.
`;
