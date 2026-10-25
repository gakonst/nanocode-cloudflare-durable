if (self !== top || /\bTwitter(Android| for iP)/.test(navigator.userAgent)) document.documentElement.setAttribute("data-xframe", "");

{
  const define = (name, value) => {
    try {
      Object.defineProperty(window, name, { configurable: true, value });
    } catch {}
  };

  for (const name of ["localStorage", "sessionStorage"]) {
    try {
      window[name].getItem("");
    } catch {
      const items = new Map();
      define(name, {
        clear: () => items.clear(),
        getItem: (key) => (items.has(String(key)) ? items.get(String(key)) : null),
        key: (index) => [...items.keys()][index] ?? null,
        get length() {
          return items.size;
        },
        removeItem: (key) => void items.delete(String(key)),
        setItem: (key, value) => void items.set(String(key), String(value)),
      });
    }
  }

  try {
    if (!window.indexedDB) define("indexedDB", undefined);
  } catch {
    define("indexedDB", undefined);
  }
}
