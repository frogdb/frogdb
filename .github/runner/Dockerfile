FROM myoung34/github-runner@sha256:bf123e0ad050a4f174f7358b81d740a8da41854f7e15883ba7a6baeb216461cf

# Build toolchain and CI dependencies for self-hosted ARM64 runner.
#
# - nodejs/npm:         required by JavaScript-based GitHub Actions
# - clang-18:           C/C++ compiler with ARM SVE/BF16 support (needed by usearch/simsimd)
# - libclang-18-dev:    libclang for bindgen (librocksdb-sys requires clang_Type_getValueType, added in LLVM 16)
# - pyflakes:           pre-installed for actionlint (avoids flaky pipx install at runtime)
# - golang-go:          required by actions/setup-go when no go-version is specified
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        nodejs npm \
        python3-pip \
        golang-go \
        lsb-release wget software-properties-common gnupg && \
    wget -qO- https://apt.llvm.org/llvm-snapshot.gpg.key | tee /etc/apt/trusted.gpg.d/apt.llvm.org.asc && \
    echo "deb http://apt.llvm.org/$(lsb_release -cs)/ llvm-toolchain-$(lsb_release -cs)-18 main" \
        > /etc/apt/sources.list.d/llvm-18.list && \
    apt-get update && \
    apt-get install -y --no-install-recommends clang-18 libclang-18-dev && \
    pip3 install pyflakes && \
    rm -rf /var/lib/apt/lists/*
