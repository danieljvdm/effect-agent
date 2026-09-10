import { Streamdown, type StreamdownProps } from "streamdown";

import { safeTravelUrl } from "../travel-content";
import { TravelImage } from "./travel/photo-gallery";

export const safeMessageUrl = (url: string | undefined) =>
  typeof url === "string" && /^\/trips\/[a-zA-Z0-9-]{1,80}\/[1-9]\d{0,8}$/.test(url)
    ? url
    : safeTravelUrl(url);

const components: StreamdownProps["components"] = {
  strong: "strong",
  em: "em",
  a: ({ href, children }) => {
    const url = safeMessageUrl(href);

    return url ? (
      <a href={url} target="_blank" rel="noopener noreferrer">
        {children}
      </a>
    ) : (
      <span>{children}</span>
    );
  },
  img: ({ src, alt }) =>
    typeof src === "string" && safeTravelUrl(src) ? (
      <span className="message-photo">
        <TravelImage key={src} src={src} alt={alt || "Photo from the linked source"} />
      </span>
    ) : null,
};

/** No raw HTML, embedded frames, or executable links from model output. */
export function MessageText({
  text,
  streaming = false,
}: {
  readonly text: string;
  readonly streaming?: boolean;
}) {
  return (
    <Streamdown
      className="message-markdown"
      mode={streaming ? "streaming" : "static"}
      isAnimating={streaming}
      parseIncompleteMarkdown={streaming}
      controls={false}
      skipHtml
      rehypePlugins={[]}
      allowedElements={[
        "p",
        "a",
        "img",
        "strong",
        "em",
        "del",
        "h1",
        "h2",
        "h3",
        "h4",
        "h5",
        "h6",
        "ul",
        "ol",
        "li",
        "blockquote",
        "pre",
        "code",
        "hr",
        "br",
        "table",
        "thead",
        "tbody",
        "tr",
        "th",
        "td",
      ]}
      urlTransform={(url) => safeMessageUrl(url)}
      components={components}
    >
      {text}
    </Streamdown>
  );
}
