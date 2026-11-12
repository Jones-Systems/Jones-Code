# Files in question answers

When an agent asks a question that accepts a custom answer, use **Attach files** or paste an image into the answer field. On mobile, use the attachment button to choose photos or files. You can send a file by itself, with a selected option, or with a typed answer.

Each question keeps its own attachments as you move between questions. Your normal prompt draft stays separate. Wait for uploads to finish before submitting; retry or remove any failed upload. A failed response keeps the draft available to try again.

Files upload to the environment running the thread, including remote environments. The agent receives paths to those saved files with your answer and can open them with its available tools. Questions that only accept predefined choices do not offer attachments. Older servers need updating before this option appears.

Submitted attachments remain in the thread history. Reverting a thread removes attachments belonging to discarded answers; deleting the thread uses the same file cleanup as other attachments. You can attach up to 100 files across one set of answers, using the same file and total image size limits as the normal composer.

On web and desktop, you can also drop a sidebar thread or paste a copied thread reference into
a written answer. These references carry the thread title, source environment, and a bounded
excerpt captured when attached: up to three recent exchanges with tool activity and six earlier
input/reply pairs. Long text and tool output are shortened with omissions marked; reasoning and
file contents are excluded. No transcript file is uploaded. The excerpt is copied into your
answer and shares the same size limits as [thread context in the composer](./composer.md#context-in-your-message).

If the source is disconnected or does not support excerpts, the reference keeps its identity.
The answering agent can read additional history on demand. For another environment, its tool
must support the source environment ID and that source must be linked and accessible.
