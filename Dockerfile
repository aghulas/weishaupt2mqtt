ARG BUILD_FROM=python:3.11-slim-bookworm
FROM ${BUILD_FROM}

ENV LANG=C.UTF-8 \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

RUN apt-get update \
 && apt-get install -y --no-install-recommends \
      ca-certificates \
      curl \
      jq \
      netcat-openbsd \
 && rm -rf /var/lib/apt/lists/*

# Pinned ebusd binary (aghulas/ebusd) verified by SHA-256
ARG TARGETARCH
ARG EBUSD_RELEASE=v26.1-weishaupt.2
ARG EBUSD_SHA256_aarch64=c7854b8aa5852451522b34ac253986c8ebbd74b23b27258d4d492f36e5aeec1d
ARG EBUSD_SHA256_amd64=587ef8962e89f6a894b8e8c4c4962589ff3e2cc257b09df15d2cb5c01ce165c3
RUN set -e; \
    case "${TARGETARCH:-$(dpkg --print-architecture)}" in \
      arm64|aarch64) arch="aarch64"; sha="${EBUSD_SHA256_aarch64}" ;; \
      amd64|x86_64)  arch="amd64";   sha="${EBUSD_SHA256_amd64}" ;; \
      *) echo "Unsupported architecture: ${TARGETARCH}"; exit 1 ;; \
    esac; \
    curl -fsSL -o /usr/bin/ebusd \
      "https://github.com/aghulas/ebusd/releases/download/${EBUSD_RELEASE}/ebusd-${arch}"; \
    echo "${sha}  /usr/bin/ebusd" | sha256sum -c -; \
    chmod 755 /usr/bin/ebusd

# Pinned Weishaupt CSV definitions verified by SHA-256
ARG DEFS_VERSION=1.3.1
ARG DEFS_SHA256=0516e3b148fed6232365942c32428dc7d0082f0aad1ec47ec8168c5d38f6b439
RUN set -e; \
    mkdir -p /etc/weishaupt/config; \
    curl -fsSL -o /tmp/defs.tar.gz \
      "https://github.com/aghulas/ebusd-configuration/releases/download/weishaupt-${DEFS_VERSION}/weishaupt-ebusd-config-${DEFS_VERSION}.tar.gz"; \
    echo "${DEFS_SHA256}  /tmp/defs.tar.gz" | sha256sum -c -; \
    tar xzf /tmp/defs.tar.gz -C /etc/weishaupt/config --strip-components=1; \
    rm -f /tmp/defs.tar.gz

WORKDIR /app
COPY pyproject.toml README.md /app/
COPY src /app/src
COPY data /app/data
RUN pip install --no-cache-dir .

COPY docker/run.sh /run.sh
RUN chmod +x /run.sh

ENTRYPOINT ["/run.sh"]
