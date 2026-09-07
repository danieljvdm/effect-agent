// This fixed program runs in an isolated world. Pages cannot replace its native DOM methods.
// Handles retain actual nodes; a selector is never reconstructed from an opaque reference.
export const maxAttributeLength = 2048;

export const inspectFrame = `(() => {
  const doc = document;
  const forms = [];
  const isChoice = el => el instanceof HTMLInputElement && ['radio','checkbox'].includes(el.type);
  const presented = el => {
    if (el.closest('[hidden],[aria-hidden="true"],[inert]') ||
      ['hidden','collapse'].includes(getComputedStyle(el).visibility)) return false;
    for (let parent = el; parent; parent = parent.parentElement) {
      if (getComputedStyle(parent).opacity === '0') return false;
    }
    return true;
  };
  const hasLayout = el => presented(el) &&
    [...el.getClientRects()].some(rect => rect.width > 0 && rect.height > 0);
  const available = el => el.type !== 'hidden' && !el.closest('[hidden],[aria-hidden="true"],[inert]') &&
    (hasLayout(el) || (isChoice(el) && [...(el.labels ?? [])].some(hasLayout)));
  const elements = [...doc.querySelectorAll('input,textarea,select,button,a[href]')].filter(available).slice(0, 65);
  const labelText = el => {
    const label = el.labels?.[0]?.cloneNode(true);
    label?.querySelectorAll('input,textarea,select,script,style,noscript').forEach(child => child.remove());
    return label?.textContent ?? el.getAttribute('aria-label');
  };
  const describe = (el) => {
    if (doc !== document || !el.isConnected || el.ownerDocument !== doc || !available(el)) return null;
    const form = el.form ?? null;
    let formIndex = forms.indexOf(form);
    if (formIndex < 0) { formIndex = forms.length; forms.push(form); }
    const action = form ? (el.hasAttribute('formaction') ? el.formAction : form.action || doc.URL) : doc.URL;
    const method = form ? (el.hasAttribute('formmethod') ? el.formMethod : form.method) : '';
    const enctype = form?.enctype ?? '';
    const name = el.name ?? '';
    const completion = el.getAttribute('autocomplete') ?? '';
    const inputType = el.type ?? '';
    const choiceValue = isChoice(el) ? el.value : '';
    // Reject before parsing, fingerprinting or CDP transfer. Truncation could hide a target change.
    if ([action, method, enctype, name, completion, inputType, choiceValue].some(value => value.length > ${maxAttributeLength})) return null;
    const type = inputType.toLowerCase();
    const autocomplete = completion.trim().toLowerCase().split(/\\s+/).at(-1);
    const cardRoles = { 'cc-name':'card-name', 'cc-number':'card-number', 'cc-exp':'card-expiry',
      'cc-exp-month':'card-expiry-month', 'cc-exp-year':'card-expiry-year', 'cc-csc':'card-security-code' };
    let role = 'unsupported';
    const nativeField = el instanceof HTMLInputElement || el instanceof HTMLSelectElement || el instanceof HTMLTextAreaElement;
    if (nativeField && !['submit','button'].includes(type) && !el.matches(':disabled') && !el.readOnly && !(el instanceof HTMLSelectElement && el.multiple)) {
      if (isChoice(el)) role = type;
      else if (type === 'password' || ['current-password','new-password','one-time-code'].includes(autocomplete)) {
        if (type === 'password' && form) role = 'password';
      } else if (['text','email','tel','number','month','search','url','date','time','week','datetime-local','textarea',''].includes(type) || el instanceof HTMLSelectElement) {
        if (cardRoles[autocomplete]) { if (form) role = cardRoles[autocomplete]; }
        else if (autocomplete === 'username' || autocomplete === 'email') { if (form) role = 'username'; }
        else if (['text','email'].includes(type) && form && form.querySelector('input[type="password"]')) role = 'username';
        else if (el instanceof HTMLSelectElement) { if (!el.multiple) role = 'select'; }
        else role = 'text';
      }
    } else if (el instanceof HTMLButtonElement || (el instanceof HTMLInputElement && ['submit','button'].includes(type))) {
      if (!el.matches(':disabled')) role = type === 'submit' && form ? 'submit' : type === 'button' ? 'button' : 'unsupported';
    } else if (el instanceof HTMLAnchorElement) role = 'link';
    const fingerprint = JSON.stringify([role, action, method, enctype, name, completion, type, choiceValue]);
    return { role, formIndex, action, fingerprint, ...(isChoice(el) ? {checked: el.checked} : {}),
      label: (labelText(el) ?? (nativeField ? '' : el.textContent) ?? '').slice(0,200) };
  };
  const expose = ({role, formIndex, action, label, checked}) =>
    ({role, formIndex, action, label, ...(checked === undefined ? {} : {checked})});
  const original = elements.map(describe);
  const validate = (index) => {
    const current = describe(elements[index]);
    if (!current || !original[index] || current.fingerprint !== original[index].fingerprint ||
      current.formIndex !== original[index].formIndex) return null;
    return expose(current);
  };
  return {
    doc, elements, original: original.map(current => current && expose(current)), validate,
    text: () => {
      if (doc !== document) return null;
      if (!doc.body) return '';
      const walker = doc.createTreeWalker(doc.body, NodeFilter.SHOW_TEXT);
      const range = doc.createRange();
      let text = '';
      for (let node = walker.nextNode(); node && text.length < 65536; node = walker.nextNode()) {
        const parent = node.parentElement;
        if (!parent || parent.closest('input,textarea,select,script,style,noscript,iframe,object,embed')) continue;
        if (!presented(parent)) continue;
        range.selectNodeContents(node);
        if (![...range.getClientRects()].some(rect => rect.width > 0 && rect.height > 0)) continue;
        text += (node.textContent ?? '').replace(/\\s+/g, ' ') + ' ';
      }
      return text.slice(0,65536);
    },
    fill: (index, role, value) => {
      const current = validate(index);
      if (!current || current.role !== role) return false;
      const el = elements[index];
      if (role === 'select') {
        const options = [...el.options].filter(option => !option.matches(':disabled'));
        const values = options.filter(option => option.value === value);
        const matches = values.length > 0 ? values : options.filter(option => option.textContent?.trim() === value);
        if (matches.length !== 1) return 'unsupported';
        value = matches[0].value;
        if ([...el.options].filter(option => option.value === value).length !== 1) return 'unsupported';
      }
      let prototype = Object.getPrototypeOf(el);
      let setter;
      while (prototype && !setter) { setter = Object.getOwnPropertyDescriptor(prototype,'value')?.set; prototype = Object.getPrototypeOf(prototype); }
      if (!setter) return false;
      // Validate and mutate in one isolated-world task, without focusing first and running
      // page handlers between validation and assignment. Native setters support controlled inputs.
      setter.call(el, value);
      if (el.value !== value) return 'unsupported';
      el.dispatchEvent(new Event('input', {bubbles:true}));
      el.dispatchEvent(new Event('change', {bubbles:true}));
      return true;
    },
    click: (index) => {
      const current = validate(index);
      if (!current || !['button','submit','link','radio','checkbox'].includes(current.role)) return false;
      if (current.role === 'submit' && elements[index].form && !elements[index].form.matches(':valid')) return 'needs-attention';
      elements[index].click();
      return true;
    }
  };
})()`;
