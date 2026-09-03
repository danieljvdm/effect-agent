// This fixed program runs in an isolated world. Pages cannot replace its native DOM methods.
// Handles retain actual nodes; a selector is never reconstructed from an opaque reference.
export const maxAttributeLength = 2048;

export const inspectFrame = `(() => {
  const doc = document;
  const forms = [];
  const elements = [...doc.querySelectorAll('input,select,button,a[href]')].slice(0, 65);
  const describe = (el) => {
    if (doc !== document || !el.isConnected || el.ownerDocument !== doc) return null;
    const form = el.form ?? null;
    let formIndex = forms.indexOf(form);
    if (formIndex < 0) { formIndex = forms.length; forms.push(form); }
    const action = form ? (el.hasAttribute('formaction') ? el.formAction : form.action || doc.URL) : doc.URL;
    const method = form ? (el.hasAttribute('formmethod') ? el.formMethod : form.method) : '';
    const enctype = form?.enctype ?? '';
    const name = el.name ?? '';
    const completion = el.getAttribute('autocomplete') ?? '';
    const inputType = el.type ?? '';
    // Reject before parsing, fingerprinting or CDP transfer. Truncation could hide a target change.
    if ([action, method, enctype, name, completion, inputType].some(value => value.length > ${maxAttributeLength})) return null;
    const type = inputType.toLowerCase();
    const autocomplete = completion.trim().toLowerCase().split(/\\s+/).at(-1);
    const cardRoles = { 'cc-name':'card-name', 'cc-number':'card-number', 'cc-exp':'card-expiry',
      'cc-exp-month':'card-expiry-month', 'cc-exp-year':'card-expiry-year', 'cc-csc':'card-security-code' };
    let role = 'unsupported';
    const nativeField = el instanceof HTMLInputElement || el instanceof HTMLSelectElement;
    if (nativeField && form && !['submit','button'].includes(type) && !el.disabled && !el.readOnly && el.getClientRects().length > 0) {
      if (el instanceof HTMLInputElement && type === 'password') role = 'password';
      else if (['text','email','tel','number','month',''].includes(type) || el instanceof HTMLSelectElement) {
        if (cardRoles[autocomplete]) role = cardRoles[autocomplete];
        else if (autocomplete === 'username' || autocomplete === 'email') role = 'username';
        else if (['text','email'].includes(type) && form && form.querySelector('input[type="password"]')) role = 'username';
      }
    } else if (el instanceof HTMLButtonElement || (el instanceof HTMLInputElement && ['submit','button'].includes(type))) {
      if (!el.disabled) role = type === 'submit' && form ? 'submit' : type === 'button' ? 'button' : 'unsupported';
    } else if (el instanceof HTMLAnchorElement) role = 'link';
    const fingerprint = JSON.stringify([role, action, method, enctype, name, completion, type]);
    return { role, formIndex, action, fingerprint,
      label: (el.labels?.[0]?.textContent ?? el.getAttribute('aria-label') ?? el.textContent ?? '').slice(0,200) };
  };
  const expose = ({role, formIndex, action, label}) => ({role, formIndex, action, label});
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
      const clone = doc.body?.cloneNode(true);
      clone?.querySelectorAll('input,textarea,select,script,style,noscript,iframe,object,embed').forEach(el => el.remove());
      return (clone?.textContent ?? '').slice(0,65536);
    },
    fill: (index, role, value) => {
      const current = validate(index);
      if (!current || current.role !== role) return false;
      const el = elements[index];
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
      if (!current || !['button','submit','link'].includes(current.role)) return false;
      if (current.role === 'submit' && elements[index].form && !elements[index].form.matches(':valid')) return 'needs-attention';
      elements[index].click();
      return true;
    }
  };
})()`;
