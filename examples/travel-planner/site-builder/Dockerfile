FROM oven/bun:1.4.0 AS bun

FROM docker.io/cloudflare/sandbox:0.13.0-next.738.2

COPY --from=bun /usr/local/bin/bun /usr/local/bin/bun
ENV VP_HOME=/opt/vite-plus
ENV VP_VERSION=0.3.0
ENV CI=true
ENV PATH="/opt/vite-plus/bin:/usr/local/bin:${PATH}"

RUN curl -fsSL https://vite.plus -o /tmp/install-vp.sh \
    && bash /tmp/install-vp.sh \
    && rm /tmp/install-vp.sh \
    && bun --version \
    && vp --version
