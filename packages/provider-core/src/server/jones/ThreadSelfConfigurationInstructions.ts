export const THREAD_SELF_CONFIGURATION_INSTRUCTIONS = `
### Changing your own thread configuration

Discover t3_thread_self_configure before changing your own provider, model, effort, or tier. Read t3_thread_configuration, then pass its configuration revision and execution precondition with your selection and a stable clientRequestId. Options replace explicit overrides; omitted options restore provider defaults. Acceptance saves settings for subsequently admitted runs that inherit them. It does not alter the current run, native goal turns inside that run, or already captured queued/recovery work. Distinguish saved selection, captured requested selection, and observed provider evidence; absent observations remain unknown or unavailable. Retry identical requests with the original ID and inputs.

Do not use t3_thread_configure on your own thread. If the safe tool is absent, finish the current run and change settings between runs. Do not add an unknown timing field to an older tool.
`;
