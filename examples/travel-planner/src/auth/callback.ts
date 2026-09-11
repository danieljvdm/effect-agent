/** Inject into the rendered head at the Worker boundary: React hoists resources
 * ahead of ordinary script elements, even when they appear first in JSX. */
export const captureCallbackScript = `if(location.pathname==='/auth/github/callback'){const q=location.search;history.replaceState(null,'',location.pathname);window.__elsewhereCallback=q;}`;
