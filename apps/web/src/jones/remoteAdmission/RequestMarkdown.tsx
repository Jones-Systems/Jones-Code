import ReactMarkdown, { type Components } from "react-markdown";
import remarkGfm from "remark-gfm";

const allowedElements = [
  "p",
  "br",
  "strong",
  "em",
  "del",
  "code",
  "pre",
  "blockquote",
  "ul",
  "ol",
  "li",
  "hr",
  "table",
  "thead",
  "tbody",
  "tr",
  "th",
  "td",
  "h1",
  "h2",
  "h3",
  "h4",
  "h5",
  "h6",
  "a",
  "img",
  "input",
];

const components: Components = {
  a: ({ children: text, href }) => (
    <>
      {String(text) === href ? (
        text
      ) : (
        <>
          {text}
          {href ? ` (${href})` : ""}
        </>
      )}
    </>
  ),
  img: ({ alt, src }) => (
    <>
      [image: {alt ?? ""}] ({typeof src === "string" ? src : ""})
    </>
  ),
  input: ({ checked }) => (
    <span>
      {checked ? "☑" : "☐"}
      <span className="sr-only">{checked ? "Done" : "Not done"}</span>
    </span>
  ),
  h1: ({ children: text }) => <p className="font-semibold">{text}</p>,
  h2: ({ children: text }) => <p className="font-semibold">{text}</p>,
  h3: ({ children: text }) => <p className="font-semibold">{text}</p>,
  h4: ({ children: text }) => <p className="font-semibold">{text}</p>,
  h5: ({ children: text }) => <p className="font-semibold">{text}</p>,
  h6: ({ children: text }) => <p className="font-semibold">{text}</p>,
  pre: ({ children: text }) => <pre>{text}</pre>,
  code: ({ children: text }) => <code>{text}</code>,
};

export function RequestMarkdown({ children }: { readonly children: string }) {
  return (
    <div className="chat-markdown max-h-60 min-w-0 overflow-auto overscroll-contain wrap-anywhere [&_pre]:whitespace-pre-wrap [&_pre]:wrap-anywhere [&_table]:table-fixed [&_table]:w-full">
      <ReactMarkdown
        remarkPlugins={[remarkGfm]}
        allowedElements={allowedElements}
        unwrapDisallowed
        urlTransform={(url) => url}
        components={components}
      >
        {children}
      </ReactMarkdown>
    </div>
  );
}
