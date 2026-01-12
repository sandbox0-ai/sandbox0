# sandbox0 default sandbox template (runtime-only).
# This image intentionally does NOT define an ENTRYPOINT/CMD.
# The platform will inject and start the sandbox control process at runtime.

FROM ubuntu:24.04

# Avoid prompts during installation
ENV DEBIAN_FRONTEND=noninteractive

# Install languages using Ubuntu 24.04 (Noble) default software repositories.
# This ensures stability and follows the OS's native versions:
# - Python 3.12 (with IPython for better REPL experience)
# - Node.js 18.x
# - Ruby 3.2
# - PHP 8.3
# - Go 1.22
# - R 4.3
# - Perl 5.38
# - Lua 5.4 / LuaJIT
RUN apt-get update \
  && apt-get install -y --no-install-recommends \
    ca-certificates \
    curl \
    gnupg \
    build-essential \
    bash \
    zsh \
    git \
    python3 \
    python3-pip \
    python-is-python3 \
    ipython3 \
    nodejs \
    npm \
    ruby-full \
    lua5.4 \
    luajit \
    php-cli \
    r-base \
    perl \
    golang \
  && rm -rf /var/lib/apt/lists/*

# Add Go to PATH
ENV PATH=$PATH:/usr/lib/go/bin:/root/go/bin

WORKDIR /root

# Intentionally no ENTRYPOINT/CMD here.
