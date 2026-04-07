FROM myoung34/github-runner@sha256:10f124b019c94eeda67819cccaf7217aa5735fd1b78adda0d51981d23a7189df

# Build toolchain and CI dependencies for self-hosted ARM64 runner.
#
# - nodejs/npm:         required by JavaScript-based GitHub Actions
# - clang-18:           C/C++ compiler with ARM SVE/BF16 support (needed by usearch/simsimd)
# - libclang-18-dev:    libclang for bindgen (librocksdb-sys requires clang_Type_getValueType, added in LLVM 16)
# - pipx:               required by raven-actions/actionlint for pyflakes integration
# - golang-go:          required by actions/setup-go when no go-version is specified
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        nodejs npm \
        pipx \
        golang-go \
        lsb-release wget software-properties-common gnupg && \
    wget -qO- https://apt.llvm.org/llvm-snapshot.gpg.key | tee /etc/apt/trusted.gpg.d/apt.llvm.org.asc && \
    echo "deb http://apt.llvm.org/$(lsb_release -cs)/ llvm-toolchain-$(lsb_release -cs)-18 main" \
        > /etc/apt/sources.list.d/llvm-18.list && \
    apt-get update && \
    apt-get install -y --no-install-recommends clang-18 libclang-18-dev && \
    rm -rf /var/lib/apt/lists/*
