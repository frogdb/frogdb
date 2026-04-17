FROM myoung34/github-runner@sha256:7f0918119b14be0e3e8d20f53b053652daa1f8cd8d9faee8f5de943ef785e7ad

# Build toolchain and CI dependencies for self-hosted ARM64 runner.
#
# What mise handles per-job (via .mise.toml + jdx/mise-action):
#   rust, python (app code), node (app code), just, uv, bun, cargo plugins, helm, ...
#
# What still needs to live in this image (mise can't manage these, or they support
# the runner daemon/actions infrastructure itself):
#   - nodejs/npm:         required at runtime by JavaScript-based GitHub Actions
#                         (the runner executes them before any mise step runs)
#   - python3-pip:        installs pyflakes for actionlint
#   - pyflakes:           pre-installed for actionlint (avoids flaky pipx install at runtime)
#   - clang-18:           C/C++ compiler with ARM SVE/BF16 support (needed by usearch/simsimd)
#   - libclang-18-dev:    libclang for bindgen (librocksdb-sys requires clang_Type_getValueType,
#                         added in LLVM 16)
#   - mise:               toolchain manager — mise-action calls this binary to install per-job tools
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        nodejs npm \
        python3-pip \
        ca-certificates curl gpg \
        lsb-release wget software-properties-common gnupg && \
    # LLVM 18 (clang + libclang for bindgen)
    wget -qO- https://apt.llvm.org/llvm-snapshot.gpg.key | tee /etc/apt/trusted.gpg.d/apt.llvm.org.asc && \
    echo "deb http://apt.llvm.org/$(lsb_release -cs)/ llvm-toolchain-$(lsb_release -cs)-18 main" \
        > /etc/apt/sources.list.d/llvm-18.list && \
    # mise apt repository (provides mise for per-job rust/python/node/cargo plugin installs)
    install -dm 755 /etc/apt/keyrings && \
    wget -qO- https://mise.jdx.dev/gpg-key.pub | gpg --dearmor -o /etc/apt/keyrings/mise-archive-keyring.gpg && \
    echo "deb [signed-by=/etc/apt/keyrings/mise-archive-keyring.gpg arch=$(dpkg --print-architecture)] https://mise.jdx.dev/deb stable main" \
        > /etc/apt/sources.list.d/mise.list && \
    apt-get update && \
    apt-get install -y --no-install-recommends clang-18 libclang-18-dev mise && \
    pip3 install pyflakes && \
    rm -rf /var/lib/apt/lists/*
