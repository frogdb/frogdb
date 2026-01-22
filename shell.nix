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

    # Testing
    redis

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
