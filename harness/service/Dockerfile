FROM python:3.12-slim

WORKDIR /app

# Docker CLI + compose plugin only -- no daemon needed in this image,
# since this container shells out over the mounted host socket
# (Docker-outside-of-Docker) to the REAL daemon running on the host. See
# docker_control.py. Debian's docker.io package bundles the daemon and
# not the client in a usable form here, so both are installed from
# Docker's official static binary releases instead.
RUN apt-get update && apt-get install -y --no-install-recommends \
    curl ca-certificates \
    && rm -rf /var/lib/apt/lists/* \
    && ARCH=$(dpkg --print-architecture) \
    && DOCKER_ARCH=$([ "$ARCH" = "arm64" ] && echo "aarch64" || echo "x86_64") \
    && curl -SL "https://download.docker.com/linux/static/stable/${DOCKER_ARCH}/docker-27.3.1.tgz" \
       | tar -xz --strip-components=1 -C /usr/local/bin docker/docker \
    && mkdir -p /usr/local/lib/docker/cli-plugins \
    && COMPOSE_TAG=$(curl -s https://api.github.com/repos/docker/compose/releases/latest | grep -m1 '"tag_name"' | sed -E 's/.*"([^"]+)".*/\1/' | tail -1) \
    && curl -SL "https://github.com/docker/compose/releases/download/${COMPOSE_TAG}/docker-compose-linux-${DOCKER_ARCH}" \
       -o /usr/local/lib/docker/cli-plugins/docker-compose \
    && chmod +x /usr/local/lib/docker/cli-plugins/docker-compose

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8080"]
