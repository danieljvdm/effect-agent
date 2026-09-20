import { savedAddress, type ShopState } from "./checkout-contract.ts";
import { quote } from "./checkout-store.ts";

const escape = (value: string) =>
  value
    .replaceAll("&", "&amp;")
    .replaceAll('"', "&quot;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;");

const money = (cents: number) => `$${(cents / 100).toFixed(2)}`;

const input = (name: string, label: string, value = "", extra = "") =>
  `<label>${label}<input name="${name}" value="${escape(value)}" ${extra}></label>`;

const hidden = (name: string, value: string) =>
  `<input type="hidden" name="${name}" value="${escape(value)}">`;

const select = (name: string, label: string, values: ReadonlyArray<string>, selected: string) =>
  `<label>${label}<select name="${name}">${values.map((value) => `<option${value === selected ? " selected" : ""}>${value}</option>`).join("")}</select></label>`;

export const document = (
  title: string,
  body: string,
  script = "",
) => `<!doctype html><html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><title>${escape(title)}</title><style>
*{box-sizing:border-box}body{font:16px/1.5 system-ui;margin:0;color:#20312c;background:#f6f6f2}header{padding:20px 6%;border-bottom:1px solid #ccd4cd;background:white;display:flex;justify-content:space-between}main{max-width:960px;margin:32px auto;padding:0 24px}h1{font-size:32px}h2{font-size:22px}a{color:#215d47}article,section,aside{background:white;border:1px solid #d8ded8;border-radius:8px;padding:24px;margin:16px 0}.columns{display:grid;grid-template-columns:3fr 2fr;gap:24px}label{display:block;margin:14px 0}input,select{display:block;width:100%;padding:12px;border:1px solid #a2b3a9;border-radius:5px;font:inherit}input[type=radio]{display:inline;width:auto}button,.button{display:inline-block;padding:12px 22px;border:0;border-radius:5px;background:#205b46;color:white;font:inherit;cursor:pointer;margin:8px 8px 8px 0}button:disabled{opacity:.5}.wallet{background:#592ff4}.error{color:#a42722;background:#fff1f0;padding:12px}.muted{color:#53665c}.total{font-weight:bold;font-size:21px}iframe{width:100%;height:420px;border:0}footer{padding:24px;text-align:center;font-size:13px;color:#53665c}@media(max-width:650px){.columns{display:block}}
</style></head><body><header><strong>Fieldwork Supply</strong><span>Secure test checkout</span></header><main>${body}</main><footer>Controlled checkout fixture • Dummy accounts and fake money only • No fulfillment</footer>${script ? `<script>${script}</script>` : ""}</body></html>`;

const summary = (state: ShopState) => {
  const total = quote(state);

  return `<aside><h2>Order summary</h2>${state.cart ? `<p>${state.cart.product === "everyday-shirt" ? "Everyday Shirt" : "Canvas Bag"} · ${state.cart.color} · ${state.cart.size} · Quantity ${state.cart.quantity}</p>` : "Your cart is empty"}${total ? `<p>Subtotal ${money(total.subtotal)}</p><p>Shipping (${total.shipping}) ${money(total.shippingCents)}</p><p>Tax ${money(total.tax)}</p><p class="total">Total ${money(total.total)} USD</p>` : "<p>Shipping and tax calculated at checkout.</p>"}</aside>`;
};

export const shopPage = (
  state: ShopState,
  origin: string,
  processor: string,
  page: string,
  error = "",
) => {
  const base = `${origin}/s/${state.key}`;

  const form = (action: string, fields: string, submit: string) =>
    `<form method="post" action="${base}/${action}">${fields}<button>${submit}</button></form>`;

  const alert = error ? `<p role="alert" class="error">${escape(error)}</p>` : "";
  const nav = `<nav><a href="${base}/">Shop</a> · <a href="${base}/cart">Cart</a> · <a href="${base}/login">${state.authenticated ? "Alex's account" : "Sign in"}</a> · <a href="${base}/orders">Order history</a></nav>`;
  const providerUrl = (kind: string) => `${processor}/${kind}?merchant=${encodeURIComponent(base)}`;
  let body: string;
  let script = "";

  if (page === "login")
    body = `<h1>Sign in</h1>${alert}${form("login", input("email", "Email", "", 'type="email" autocomplete="username" required') + input("password", "Password", "", 'type="password" autocomplete="current-password" required'), "Sign in")}`;
  else if (page === "product")
    body = `<h1>Everyday Shirt</h1><p>Soft cotton, relaxed fit. $34.00 USD</p><p>Choose your color and size. Four units available per variant.</p>${alert}${form("cart", hidden("product", "everyday-shirt") + select("color", "Color", ["red", "blue"], "red") + select("size", "Size", ["S", "M", "L"], "S") + input("quantity", "Quantity", "1", 'type="number" min="1" max="4" required'), "Add to cart")}`;
  else if (page === "cart")
    body = `<h1>Your cart</h1>${alert}${summary(state)}${state.cart ? `${form("cart", hidden("product", state.cart.product) + select("color", "Color", ["red", "blue"], state.cart.color) + select("size", "Size", ["S", "M", "L"], state.cart.size) + input("quantity", "Quantity", String(state.cart.quantity), 'type="number" min="1" max="4"'), "Update cart")}<a class="button" href="${state.flow === "accelerated" ? providerUrl("wallet") : `${base}/address`}">${state.flow === "accelerated" ? "Express wallet checkout" : "Checkout"}</a>` : `<a href="${base}/product">Continue shopping</a>`}`;
  else if (page === "address") {
    const address = state.address ?? {
      ...savedAddress,
      postalCode: state.scenario === "correction" ? "9410" : "94107",
    };

    body = `<h1>Delivery address</h1><p>1. Information → 2. Shipping → 3. Payment</p>${alert}${form("address", input("name", "Full name", address.name, 'autocomplete="name" required') + input("line1", "Street address", address.line1, 'autocomplete="address-line1" required') + input("city", "City", address.city, 'autocomplete="address-level2" required') + input("region", "State", address.region, 'autocomplete="address-level1" required') + input("postalCode", "ZIP code", address.postalCode, 'autocomplete="postal-code" required') + hidden("country", "US"), "Continue to shipping")}`;
  } else if (page === "shipping") {
    body = `<h1>Shipping method</h1>${alert}<div class="columns"><section>${form("shipping", `<label><input type="radio" name="shipping" value="standard"${state.shipping === "standard" ? " checked" : ""}>Standard — $5.00 · 5–7 business days</label><label><input type="radio" name="shipping" value="express"${state.shipping === "express" ? " checked" : ""}>Express — $15.00 · 1–2 business days</label><p id="estimate" aria-live="polite">Choose shipping to update the total.</p>`, "Continue to payment")}<a href="${base}/address">Edit address</a></section>${summary(state)}</div>`;
    const standard = money(quote({ ...state, shipping: "standard" })?.total ?? 0);
    const express = money(quote({ ...state, shipping: "express" })?.total ?? 0);

    script = `document.querySelectorAll('[name="shipping"]').forEach(r=>r.addEventListener('change',()=>{const e=document.querySelector('#estimate');e.textContent='Updating…';setTimeout(()=>{e.textContent=r.value==='standard'?'Total with standard shipping: ${standard} USD':'Total with express shipping: ${express} USD'},350)}));`;
  } else if (page === "payment") {
    body = `<h1>Payment</h1>${alert}<div class="columns"><section>${state.payment ? `<p>Payment method: ${state.payment === "saved" ? "Saved Visa •••• 4242" : state.payment === "primary" ? "Visa •••• 4242" : "Mastercard •••• 4444"}</p><a class="button" href="${base}/review">Review order</a><details><summary>Use another card</summary><div id="card-slot"></div></details>` : `<p id="loading" aria-live="polite">Loading secure payment fields…</p><div id="card-slot"></div>`}</section>${summary(state)}</div>`;
    script = `setTimeout(()=>{document.querySelector('#loading')?.remove();const f=document.createElement('iframe');f.title='Secure card payment';f.src=${JSON.stringify(providerUrl("card"))};document.querySelector('#card-slot').append(f)},400);addEventListener('message',e=>{if(e.origin===${JSON.stringify(processor)}&&e.data==='card-ready')location.href=${JSON.stringify(`${base}/review`)}});`;
  } else if (page === "review") {
    body = `<h1>Review your order</h1>${alert}<div class="columns"><section><h2>Ship to</h2><p>${escape(state.address?.name ?? "")}<br>${escape(state.address?.line1 ?? "")}<br>${escape(state.address?.city ?? "")} ${escape(state.address?.region ?? "")} ${escape(state.address?.postalCode ?? "")}</p><a href="${base}/address">Edit address</a><p>${state.payment === "backup" ? "Mastercard •••• 4444" : "Visa •••• 4242"}</p><a href="${base}/payment">Change payment</a>${form("pay", "", `Place order · ${money(quote(state)?.total ?? 0)}`)}</section>${summary(state)}</div>`;
    script = `document.querySelector('form').addEventListener('submit',e=>{e.currentTarget.querySelector('button').disabled=true});`;
  } else if (page === "orders") {
    body = `<h1>Order history</h1>${state.orders.map((order) => `<article><h2>Order ${escape(order.orderId)}</h2><p>Payment received · ${money(order.quote.total)} ${order.quote.currency}</p><p>${order.quote.cart.quantity} Everyday Shirt · ${order.quote.cart.color} · ${order.quote.cart.size}</p><p>${order.quote.shipping} shipping to ${escape(order.quote.address.line1)}, ${escape(order.quote.address.postalCode)}</p></article>`).join("") || "<p>No orders yet.</p>"}`;
  } else if (page === "ambiguous") {
    body = `<h1>We couldn't display your confirmation</h1><p role="alert">Your payment may have been processed. Do not submit again. Check your <a href="${base}/orders">order history</a> before taking further action.</p>`;
  } else
    body = `<h1>Everyday essentials</h1><p>Thoughtful basics for wherever the day takes you.</p><div class="columns"><article><h2><a href="${base}/product">Everyday Shirt</a></h2><p>Blue or red · S / M / L</p><strong>$34.00</strong></article><article><h2>Canvas Bag</h2><p>Natural cotton tote</p><strong>$22.00</strong></article></div>`;

  return document("Fieldwork Supply", nav + body, script);
};

/** Independent origin. Forms submit through ordinary HTTPS; no server-side purchase shortcut. */
export const processorPage = (url: URL) => {
  const merchant = new URL(url.searchParams.get("merchant") ?? "https://invalid.invalid");

  if (
    merchant.protocol !== "https:" ||
    merchant.username ||
    merchant.password ||
    !/^\/s\/[a-z0-9-]{1,80}$/.test(merchant.pathname) ||
    merchant.search ||
    merchant.hash
  )
    return null;
  const target = escape(merchant.href);

  if (url.pathname === "/wallet")
    return document(
      "Express wallet · Test mode",
      `<h1>Express wallet</h1><p>Continue with your saved shipping and payment details.</p><form action="${target}/wallet" method="post">${input("email", "Email address", "", 'type="email" autocomplete="email" required')}<button class="wallet">Continue</button></form>`,
    );
  if (url.pathname === "/verify")
    return document(
      "Confirm it's you · Test mode",
      `<h1>Confirm it's you</h1><p>Enter the six-digit code sent to your test phone. This controlled fixture uses 246810.</p><form action="${target}/verify" method="post">${input("code", "Verification code", "", 'inputmode="numeric" autocomplete="one-time-code" pattern="[0-9]{6}" required')}<button class="wallet">Verify and use saved details</button></form>`,
    );
  if (url.pathname === "/card")
    return document(
      "Secure payment · Test mode",
      `<h2>Card information</h2><form action="${target}/card" method="post">${input("name", "Name on card", "", 'autocomplete="cc-name" required')}${input("number", "Card number", "", 'autocomplete="cc-number" inputmode="numeric" required')}${input("expiry", "Expiry (MM/YY)", "", 'autocomplete="cc-exp" required')}${input("cvc", "Security code", "", 'autocomplete="cc-csc" inputmode="numeric" required')}<button>Use this card</button></form>`,
    );
  if (url.pathname === "/ready")
    return document(
      "Card saved",
      "<p>Card ready. Continue reviewing your order.</p>",
      `parent.postMessage('card-ready',${JSON.stringify(merchant.origin)});`,
    );

  return null;
};
