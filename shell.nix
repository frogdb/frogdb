# NOTE: We may move to mise (https://mise.jdx.dev/) for toolchain management in the future.
{ pkgs ? import <nixpkgs> {} }:

pkgs.mkShell {
  buildInputs = with pkgs; [
    # Rust toolchain
    rustup

    # Build dependencies
    pkg-config
    openssl

    # Development tools
    just
    cargo-deny
    cargo-watch

    # Cross-compilation
    zig

    # Testing
    redis

    # Jepsen testing
    jdk
    leiningen

    # Profiling
    cargo-flamegraph
    samply
    heaptrack           # Linux only
    linuxPackages.perf  # Linux only
  ];

  # Environment setup
  shellHook = ''
    echo "FrogDB development environment loaded"
  '';
}
