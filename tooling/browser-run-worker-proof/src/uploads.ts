import { BrowserRunInteractiveHost } from "@effect-agent/platform-cloudflare/interactive-browser";
import { Effect, Encoding, Schema } from "effect";
import {
  BrowserNavigateRequest,
  BrowserReadTextRequest,
  BrowserSelectFileRequest,
  InteractiveBrowserPolicy,
} from "effect-agent/interactive-browser";

// A small, valid synthetic PDF. The receiver computes its own SHA-256 over the
// multipart file bytes; a successful selector action alone cannot pass this proof.
const fixture =
  "JVBERi0xLjcKJYGBgYEKCjYgMCBvYmoKPDwKL0ZpbHRlciAvRmxhdGVEZWNvZGUKL0xlbmd0aCAxMTMKPj4Kc3RyZWFtCnicK+RyCuEyUADBonQufY/UnLLUkszkRF1zA0sLEwsDcwtLBSMThZA0LhDpw2UIVgohQ3K5bEyNzS3NXM1NzCzMTIGkpZmxkYG5qbmBmbOZm5mhmQmQZ2BuBGS7mZnZKYRkcYVocbmGcAVyAQCRnhqWCmVuZHN0cmVhbQplbmRvYmoKCjcgMCBvYmoKPDwKL0ZpbHRlciAvRmxhdGVEZWNvZGUKL1R5cGUgL09ialN0bQovTiA1Ci9GaXJzdCAyNgovTGVuZ3RoIDM1OAo+PgpzdHJlYW0KeJzVUl1LwzAUfc+vuI/6IEnTrh8yBtvaKshQNkFRfMjaMCojkTaV+e+9N+0cexCfbTgk996T5KT3BCBAQhRBCEkKEUxCCRNIggCmU8Yfvz408Ae10x3jd03dwStyBKzhjfGl7Y2DgM1m7MRdKqf2dseGTRAQ+ch4aG3dV7qFaVmUpRCJECKOELEQMsd5icgQEmOsyRTXiCQagbkkFCKcY60cECfDHqp77mTcX+CM3Jg4+cCN0iH+uZfuKoYz5F96shnjK1vnymm4yK+lkLEIcPjv5RJ/R6uVs//3cV5/Y82vLzzrM7WXmtxq8oDvMl/rzvZthW0nXmmxQotbvf/UrqnUVSKyFHUmaYYeG43Bn++377ryVAqLg7vZONIwJCi30nWjFvaA7hM48OUe6MG5MdaRK70fjUM1FMWjR88kkyDGN/3W+ZCSAeML1Wkv9aQTRZjK1o3ZAX9qzNx0zTFBJ34Dx5zFowplbmRzdHJlYW0KZW5kb2JqCgo4IDAgb2JqCjw8Ci9TaXplIDkKL1Jvb3QgMiAwIFIKL0luZm8gMyAwIFIKL0ZpbHRlciAvRmxhdGVEZWNvZGUKL1R5cGUgL1hSZWYKL0xlbmd0aCA0MAovVyBbIDEgMiAyIF0KL0luZGV4IFsgMCA5IF0KPj4Kc3RyZWFtCnicFcSxEQAgDAOxt8MdLdMzFEslWIWAbrMhKTlVWuKAeD9fGGG0A6sKZW5kc3RyZWFtCmVuZG9iagoKc3RhcnR4cmVmCjY2MgolJUVPRg==";

const fileName = "synthetic.pdf";

class UploadProofError extends Schema.TaggedError<UploadProofError>()("UploadProofError", {
  message: Schema.String,
}) {}
const fail = () => UploadProofError.make({ message: "The remote file selection proof failed" });

const digest = (bytes: Uint8Array) =>
  Effect.tryPromise({
    try: async () =>
      Encoding.encodeHex(
        new Uint8Array(await crypto.subtle.digest("SHA-256", Uint8Array.from(bytes).buffer)),
      ),
    catch: fail,
  });

export const runUploadProof = Effect.fn("runUploadProof")(function* (origin: string) {
  const bytes = yield* Effect.fromResult(Encoding.decodeBase64(fixture));
  const checksum = yield* digest(bytes);

  const session = yield* (yield* BrowserRunInteractiveHost).open(
    InteractiveBrowserPolicy.make({
      network: { _tag: "ExactHosts", allowedHosts: [new URL(origin).host] },
      maxActions: 8,
      maxElapsedMillis: 45_000,
      maxReturnedBytes: 4096,
    }),
  );

  yield* session.handle.navigate(BrowserNavigateRequest.make({ url: `${origin}/uploads/` }));
  for (const target of ["input", "chooser"] as const) {
    const selected = yield* session.handle.selectFile(
      BrowserSelectFileRequest.make({
        selector: target === "input" ? "#file" : "#choose",
        target,
        bytes,
        fileName,
        mediaType: "application/pdf",
      }),
    );

    if (selected.fileName !== fileName || selected.size !== bytes.length) return yield* fail();
  }

  const observation = yield* session.handle.readText(
    BrowserReadTextRequest.make({ selector: "#receipts" }),
  );

  if (observation.text.split(checksum).length !== 3) return yield* fail();
  yield* session.close;

  return {
    normalInput: true as const,
    dynamicChooser: true as const,
    checksumMatched: true as const,
    closed: true as const,
  };
}, Effect.scoped);

export const uploadFixture = Effect.fn("uploadFixture")(function* (request: Request) {
  if (new URL(request.url).pathname === "/uploads/receive") {
    if (request.method !== "POST" || Number(request.headers.get("content-length") ?? 0) > 4096)
      return new Response(null, { status: 400 });
    const data = yield* Effect.tryPromise({ try: () => request.formData(), catch: fail });
    const file = data.get("file");

    if (
      !(file instanceof File) ||
      file.size > 2048 ||
      file.name !== fileName ||
      file.type !== "application/pdf"
    )
      return new Response(null, { status: 400 });

    const bytes = new Uint8Array(
      yield* Effect.tryPromise({ try: () => file.arrayBuffer(), catch: fail }),
    );

    return Response.json({
      checksum: yield* digest(bytes),
      size: bytes.length,
      fileName: file.name,
    });
  }

  return new Response(
    `<!doctype html><input id="file" type="file"><button id="choose" type="button">Choose file</button><pre id="receipts"></pre>
  <script>
  const upload = async event => {
    const data = new FormData(); data.set('file', event.target.files[0]);
    const receipt = await (await fetch('/uploads/receive', {method:'POST', body:data})).json();
    document.querySelector('#receipts').textContent += receipt.checksum + '\\n';
  };
  document.querySelector('#file').addEventListener('change', upload);
  document.querySelector('#choose').onclick = () => {
    const input=document.createElement('input'); input.type='file'; input.hidden=true; document.body.append(input);
    input.addEventListener('change',upload); input.click();
  };
  </script>`,
    { headers: { "content-type": "text/html; charset=utf-8" } },
  );
});
