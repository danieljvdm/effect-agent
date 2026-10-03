/// <reference lib="dom" />

import type { Control } from "effect-agent/browser-use";

/** Executed in the isolated page realm; the page cannot forge the reference registry. */
export const inspectDom = (
  selector: string | undefined,
  prefix: string,
  maximum: number,
  optionFilter: string | undefined,
) => {
  const registry = new Map<string, Element>();

  Reflect.set(globalThis, "@effect-agent/native-browser", registry);
  const roots: Array<Document | ShadowRoot> = [document];
  const visited = new Set<Node>();
  let discoveredElements = 0;
  const candidates: Array<HTMLElement> = [];
  let scanned = 0;
  let truncated = false;
  let text = "";

  const controlSelector =
    'button,a[href],input,textarea,select,label,[contenteditable="true"],[role="button"],[role="link"],[role="checkbox"],[role="radio"],[role="switch"],[role="tab"],[role="option"],[role="combobox"],[role="textbox"],[role="menuitem"],[role="scrollbar"]';

  const filter: NodeFilter = {
    acceptNode(node) {
      if (!(node instanceof Element)) return NodeFilter.FILTER_ACCEPT;
      const style = getComputedStyle(node);

      return node.matches('script,style,template,option,optgroup,[inert],[aria-hidden="true"]') ||
        style.display === "none" ||
        style.opacity === "0" ||
        style.contentVisibility === "hidden"
        ? NodeFilter.FILTER_REJECT
        : NodeFilter.FILTER_ACCEPT;
    },
  };

  for (let index = 0; index < roots.length && !truncated; index++) {
    const root = roots[index];

    if (root === undefined) break;

    // Narrow inspection starts at matching roots, so unrelated page content
    // cannot exhaust its budget. Shadow-host discovery has a separate bound.
    if (selector !== undefined) {
      const discovery = document.createTreeWalker(root, NodeFilter.SHOW_ELEMENT, filter);
      let node = discovery.nextNode();

      while (node !== null) {
        if (++discoveredElements > 10_000) {
          truncated = true;
          break;
        }
        if (node instanceof Element && node.shadowRoot !== null) roots.push(node.shadowRoot);
        node = discovery.nextNode();
      }
    }
    const matches = selector === undefined ? [root] : Array.from(root.querySelectorAll(selector));
    const scopes = matches.slice(0, 256);
    const nodes: Array<Element> = [];

    truncated ||= matches.length > scopes.length;
    for (const scope of scopes) {
      if (scope instanceof Element && filter.acceptNode(scope) === NodeFilter.FILTER_REJECT)
        continue;

      const walker = document.createTreeWalker(
        scope,
        NodeFilter.SHOW_ELEMENT | NodeFilter.SHOW_TEXT,
        filter,
      );

      let node: Node | null = scope instanceof Element ? scope : walker.nextNode();

      while (node !== null) {
        if (visited.has(node)) {
          node = walker.nextNode();
          continue;
        }
        visited.add(node);
        if (++scanned > 10_000) {
          truncated = true;
          break;
        }
        if (node instanceof Element) {
          nodes.push(node);
          if (selector === undefined && node.shadowRoot !== null) roots.push(node.shadowRoot);
        } else if (node instanceof Text) {
          const parent = node.parentElement;

          // Select options have their own bounded lookup; collapsed option text
          // must not bury the current form's values and validation messages.
          if (
            parent instanceof HTMLElement &&
            parent.closest('select,script,style,[inert],[aria-hidden="true"]') === null &&
            parent.checkVisibility({ checkOpacity: true, checkVisibilityCSS: true })
          ) {
            const value = node.textContent?.replace(/\s+/g, " ").trim() ?? "";

            if (value.length > 0) {
              const room = Math.max(0, 12_000 - text.length);

              text += `${value.slice(0, room)}\n`;
              truncated ||= value.length >= room;
            }
          }
        }
        node = walker.nextNode();
      }
      if (scanned > 10_000) break;
    }
    for (const node of nodes) {
      if (selector !== undefined && !node.matches(selector) && node.closest(selector) === null)
        continue;
      if (!(node instanceof HTMLElement)) continue;

      const scrollable =
        node.scrollHeight > node.clientHeight &&
        ["auto", "scroll"].includes(getComputedStyle(node).overflowY);

      if (!node.matches(controlSelector) && !scrollable) continue;
      if (
        !node.checkVisibility({ checkOpacity: true, checkVisibilityCSS: true }) ||
        node.closest('[inert],[aria-hidden="true"]') !== null
      )
        continue;
      const modal = document.querySelector("dialog:modal");

      if (modal !== null && !modal.contains(node) && node.getRootNode() === document) continue;
      if (candidates.length === maximum) {
        truncated = true;
        break;
      }
      candidates.push(node);
    }
  }

  const controls = candidates.map((node, index) => {
    const ref = `${prefix}-${index}`;
    const tag = node.tagName.toLowerCase();
    const associated = node instanceof HTMLLabelElement ? (node.control ?? node) : node;
    const type = associated instanceof HTMLInputElement ? associated.type : "";

    const nativeField =
      associated instanceof HTMLInputElement ||
      associated instanceof HTMLTextAreaElement ||
      associated instanceof HTMLSelectElement;

    const labels = nativeField ? associated.labels : null;

    const labelledBy = (node.getAttribute("aria-labelledby") ?? "")
      .split(/\s+/)
      .map((id) =>
        node.getRootNode() instanceof Document || node.getRootNode() instanceof ShadowRoot
          ? (Reflect.get(node.getRootNode(), "getElementById").call(node.getRootNode(), id)
              ?.textContent ?? "")
          : "",
      )
      .join(" ");

    const role = node.getAttribute("role");

    const kind =
      tag === "label" &&
      associated instanceof HTMLInputElement &&
      ["checkbox", "radio"].includes(type)
        ? type
        : (role ?? (type === "checkbox" || type === "radio" ? type : tag === "a" ? "link" : tag));

    const name = (
      node.getAttribute("aria-label") ||
      labelledBy.trim() ||
      (labels === null
        ? ""
        : Array.from(labels)
            .map((label) => label.textContent)
            .join(" ")) ||
      (nativeField
        ? node.getAttribute("placeholder")
        : node instanceof HTMLElement
          ? node.innerText
          : "") ||
      ""
    )
      .replace(/\s+/g, " ")
      .trim()
      .slice(0, 300);

    const allOptions = node instanceof HTMLSelectElement ? Array.from(node.options) : [];

    const matchingOptions = allOptions.flatMap((option, index) =>
      option.selected ||
      optionFilter === undefined ||
      `${option.label}\n${option.value}`.toLowerCase().includes(optionFilter.toLowerCase())
        ? [{ option, index }]
        : [],
    );

    // Keep selected state even when it falls beyond the bounded lookup window.
    const selectedOptions = matchingOptions.filter(({ option }) => option.selected);

    const visibleOptions = [
      ...selectedOptions.slice(0, 256),
      ...matchingOptions
        .filter(({ option }) => !option.selected)
        .slice(0, Math.max(0, 256 - selectedOptions.length)),
    ];

    if (matchingOptions.length > visibleOptions.length) truncated = true;

    const attributes = Object.fromEntries(
      [
        "id",
        "name",
        "type",
        "role",
        "tabindex",
        "placeholder",
        "aria-label",
        "aria-labelledby",
        "aria-expanded",
        "aria-selected",
        "aria-controls",
        "href",
      ].flatMap((key) => {
        const value = node.getAttribute(key);

        return value === null ? [] : [[key, value.slice(0, 1_024)]];
      }),
    );

    registry.set(ref, node);
    const pointerEvents = getComputedStyle(node).pointerEvents?.slice(0, 64);

    return {
      ref,
      kind,
      name,
      attributes,
      ...(pointerEvents ? { pointerEvents } : {}),
      value:
        nativeField && type !== "password"
          ? associated.value.slice(0, 4_096)
          : node.isContentEditable
            ? node.innerText.slice(0, 4_096)
            : "",
      options: visibleOptions.map(({ option }) => option.value.slice(0, 4_096)),
      ...(node instanceof HTMLSelectElement
        ? {
            optionCount: allOptions.length,
            optionDetails: visibleOptions.map(({ option, index }) => ({
              index,
              value: option.value.slice(0, 4096),
              label: option.label.slice(0, 300),
              disabled:
                option.disabled ||
                (option.parentElement instanceof HTMLOptGroupElement &&
                  option.parentElement.disabled),
              selected: option.selected,
            })),
          }
        : {}),
      disabled:
        associated.matches(":disabled") || associated.getAttribute("aria-disabled") === "true",
      editable:
        (node instanceof HTMLInputElement &&
          !node.readOnly &&
          !["password", "file", "checkbox", "radio", "hidden", "submit", "button"].includes(
            node.type,
          )) ||
        (node instanceof HTMLTextAreaElement && !node.readOnly) ||
        (node.isContentEditable && node.getAttribute("aria-readonly") !== "true"),
      ...(associated instanceof HTMLInputElement && (type === "checkbox" || type === "radio")
        ? { checked: associated.checked }
        : {}),
    };
  });

  return {
    text: text.slice(0, 12_000),
    controls,
    readyState: document.readyState,
    truncated: truncated || text.length > 12_000,
  };
};

/** Current state after standard DOM scrolling. Pointer actions also require a hit test;
 * keyboard actions verify native focus at dispatch instead. Never dispatches input.
 */
export const checkDom = (
  node: Element,
  expected: typeof Control.Type,
  scroll: boolean,
  pointer: boolean,
) => {
  if (!node.isConnected || !(node instanceof HTMLElement)) return false;
  if (scroll) node.scrollIntoView({ block: "center", inline: "center", behavior: "instant" });
  const associated = node instanceof HTMLLabelElement ? (node.control ?? node) : node;
  const type = associated instanceof HTMLInputElement ? associated.type : "";

  if (
    associated instanceof HTMLSelectElement &&
    expected.optionDetails !== undefined &&
    ((expected.optionCount === undefined ? expected.optionDetails.length : expected.optionCount) !==
      associated.options.length ||
      expected.optionDetails.some((option, index) => {
        const current = associated.options.item(option.index ?? index);

        return (
          current === null ||
          current.value !== option.value ||
          current.label.slice(0, 300) !== option.label ||
          current.selected !== option.selected ||
          (current.disabled ||
            (current.parentElement instanceof HTMLOptGroupElement &&
              current.parentElement.disabled)) !== option.disabled
        );
      }))
  )
    return false;
  if (node.isContentEditable && node.innerText.slice(0, 4_096) !== expected.value) return false;

  const kind =
    node instanceof HTMLLabelElement &&
    associated instanceof HTMLInputElement &&
    ["checkbox", "radio"].includes(type)
      ? type
      : (node.getAttribute("role") ??
        (type === "checkbox" || type === "radio"
          ? type
          : node.tagName === "A"
            ? "link"
            : node.tagName.toLowerCase()));

  if (
    kind !== expected.kind ||
    associated.matches(":disabled") ||
    associated.getAttribute("aria-disabled") === "true" ||
    node.closest('[inert],[aria-hidden="true"]') !== null ||
    !node.checkVisibility({ checkOpacity: true, checkVisibilityCSS: true })
  )
    return false;
  if (
    (associated instanceof HTMLInputElement ||
      associated instanceof HTMLTextAreaElement ||
      associated instanceof HTMLSelectElement) &&
    type !== "password" &&
    associated.value.slice(0, 4_096) !== expected.value
  )
    return false;
  if (
    !(
      associated instanceof HTMLInputElement ||
      associated instanceof HTMLTextAreaElement ||
      associated instanceof HTMLSelectElement
    ) &&
    !node.isContentEditable &&
    !node.hasAttribute("aria-label") &&
    !node.hasAttribute("aria-labelledby") &&
    node.innerText.replace(/\s+/g, " ").trim().slice(0, 300) !== expected.name
  )
    return false;
  if (
    expected.checked !== undefined &&
    (!(associated instanceof HTMLInputElement) || associated.checked !== expected.checked)
  )
    return false;
  if (
    Object.entries(expected.attributes ?? {}).some(
      ([key, value]) => (node.getAttribute(key) ?? "").slice(0, 1_024) !== value,
    )
  )
    return false;
  const ancestors = new Set<Node>();
  let ancestor: Node | null = node;

  while (ancestor !== null) {
    ancestors.add(ancestor);
    ancestor = ancestor instanceof ShadowRoot ? ancestor.host : ancestor.parentNode;
  }

  const modalSelector =
    'dialog:modal,[role="dialog"][aria-modal="true"],[role="alertdialog"][aria-modal="true"]';

  const roots = new Set([document, node.getRootNode()]);

  for (const root of roots) {
    if (!(root instanceof Document || root instanceof ShadowRoot)) continue;
    for (const modal of root.querySelectorAll(modalSelector)) {
      if (
        modal instanceof HTMLElement &&
        modal.checkVisibility({ checkOpacity: true, checkVisibilityCSS: true }) &&
        !ancestors.has(modal)
      )
        return false;
    }
  }
  const rect = node.getBoundingClientRect();

  const x =
    Math.max(0, rect.left) + (Math.min(innerWidth, rect.right) - Math.max(0, rect.left)) / 2;

  const y =
    Math.max(0, rect.top) + (Math.min(innerHeight, rect.bottom) - Math.max(0, rect.top)) / 2;

  let hit = document.elementFromPoint(x, y);

  while (hit?.shadowRoot !== null && hit?.shadowRoot !== undefined) {
    const next = hit.shadowRoot.elementFromPoint(x, y);

    if (next === null || next === hit) break;
    hit = next;
  }

  // Keyboard-only overlays can sit above sibling card content. Require that
  // containing element to remain unobstructed, then verify native focus at dispatch.
  // This never permits a pointer click through the overlay.
  return rect.width > 0 &&
    rect.height > 0 &&
    node.isConnected &&
    (hit === node ||
      (hit !== null &&
        (node.contains(hit) ||
          (!pointer &&
            node.tabIndex >= 0 &&
            getComputedStyle(node).pointerEvents === "none" &&
            node.parentElement?.contains(hit)))))
    ? { x, y }
    : false;
};

/** Translate a checked child point and refuse input through hidden/covered frame owners. */
export const checkFrameDom = (node: Element, point: { x: number; y: number }, scroll: boolean) => {
  if (
    !(node instanceof HTMLElement) ||
    !node.isConnected ||
    node.closest('[inert],[aria-hidden="true"]') !== null ||
    !node.checkVisibility({ checkOpacity: true, checkVisibilityCSS: true })
  )
    return false;
  if (scroll) node.scrollIntoView({ block: "center", inline: "center", behavior: "instant" });
  const style = getComputedStyle(node);

  // A transformed coordinate space needs a native geometry adapter; never guess.
  let ancestor: Element | null = node;

  while (ancestor !== null) {
    const current = getComputedStyle(ancestor);

    if (current.transform !== "none" || (current.zoom !== "1" && current.zoom !== "normal"))
      return false;
    ancestor = ancestor.parentElement;
  }
  const rect = node.getBoundingClientRect();
  const x = rect.left + parseFloat(style.borderLeftWidth) + parseFloat(style.paddingLeft) + point.x;
  const y = rect.top + parseFloat(style.borderTopWidth) + parseFloat(style.paddingTop) + point.y;
  const hit = document.elementFromPoint(x, y);

  return hit === node ? { x, y } : false;
};

export const waitDom = (selector: string, state: string, text: string | undefined) => {
  const roots: Array<Document | ShadowRoot> = [document];
  let scanned = 0;

  for (let index = 0; index < roots.length; index++) {
    const root = roots[index];

    if (root === undefined) break;

    const walker = document.createTreeWalker(root, NodeFilter.SHOW_ELEMENT, {
      acceptNode(node) {
        if (!(node instanceof Element)) return NodeFilter.FILTER_ACCEPT;
        const style = getComputedStyle(node);

        return node.matches('script,style,template,option,optgroup,[inert],[aria-hidden="true"]') ||
          style.display === "none" ||
          style.opacity === "0" ||
          style.contentVisibility === "hidden"
          ? NodeFilter.FILTER_REJECT
          : NodeFilter.FILTER_ACCEPT;
      },
    });

    let node = walker.nextNode();

    while (node !== null) {
      if (++scanned > 10_000) return false;
      if (node instanceof Element) {
        if (node.shadowRoot !== null) roots.push(node.shadowRoot);
        if (
          node instanceof HTMLElement &&
          node.matches(selector) &&
          node.checkVisibility({ checkOpacity: true, checkVisibilityCSS: true })
        ) {
          if (state === "hidden") return false;
          if (
            state === "visible" ||
            (state === "enabled" &&
              !node.matches(":disabled") &&
              node.getAttribute("aria-disabled") !== "true") ||
            (state === "text" && text !== undefined && node.innerText.includes(text))
          )
            return true;
        }
      }
      node = walker.nextNode();
    }
  }

  return state === "hidden";
};
