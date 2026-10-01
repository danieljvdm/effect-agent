declare namespace Cloudflare {
  interface Env {
    readonly BROWSER: BrowserRun;
    readonly CHECKOUTS: DurableObjectNamespace;
    readonly CHECKOUT_TOKEN: string;
    readonly CHECKOUT_PASSWORD: string;
    readonly OPENAI_API_KEY: string;
    readonly CHECKOUT_MODEL: string;
    readonly CLOUDFLARE_ACCOUNT_ID: string;
    readonly BROWSER_RENDERING_API_TOKEN: string;
  }
}
