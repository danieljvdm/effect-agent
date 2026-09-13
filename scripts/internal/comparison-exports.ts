/** Add renamed paths only to disposable comparison manifests; published APIs stay canonical. */
export const comparisonExports = (original: Readonly<Record<string, string>> = {}) => {
  const exports = { ...original };

  for (const [key, target] of Object.entries(original)) {
    const canonical = key
      .replace(/([A-Z]+)([A-Z][a-z])/g, "$1-$2")
      .replace(/([a-z0-9])([A-Z])/g, "$1-$2")
      .toLowerCase();

    if (exports[canonical] === undefined) exports[canonical] = target;
  }

  return exports;
};
