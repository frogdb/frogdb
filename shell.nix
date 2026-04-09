# FrogDB system dependencies (Nix/Linux)
#
# Language runtimes and dev CLI tools live in .mise.toml — run `mise install` after entering
# the nix-shell. This file handles system libraries and specialized packages that mise cannot
# manage.
{ pkgs ? import <nixpkgs> {} }:

pkgs.mkShell {
  buildInputs = with pkgs; [
    # Toolchain manager — installs rust, python, node, just, uv, bun, cargo plugins, ... per .mise.toml
    mise

    # Git hooks manager (installed here so `lefthook install` works before mise has run)
    lefthook

    # Build dependencies — required for librocksdb-sys bindgen and TLS
    pkg-config
    openssl

    # Runtime tools for testing
    redis

    # Jepsen testing
    leiningen            # Clojure build tool (no mise plugin; also pulls in a JDK)
    gnuplot              # Performance graphs

    # Linux-only profilers (no mise plugins)
    heaptrack
    linuxPackages.perf
  ];

  shellHook = ''
    echo "FrogDB development environment loaded"
    echo "Run 'mise install' to fetch language runtimes and dev CLI tools."
  '';
}
