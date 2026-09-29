/** Sentry-style instrumentation preserves storage identity but wraps SQL on every access. */
export const instrumentedStorage = <Storage extends { readonly sql: object }>(
  storage: Storage,
): Storage =>
  new Proxy(storage, {
    get(target, property) {
      if (property === "sql")
        return new Proxy(target.sql, {
          get(sql, key) {
            const value = Reflect.get(sql, key, sql);

            return typeof value === "function" ? value.bind(sql) : value;
          },
        });
      const value = Reflect.get(target, property, target);

      return typeof value === "function" ? value.bind(target) : value;
    },
  });
