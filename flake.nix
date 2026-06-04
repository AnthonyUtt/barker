{
  description = "A very basic Rust development flake";

  inputs = {
    fenix = {
      url = "github:nix-community/fenix";
      inputs.nixpkgs.follows = "nixpkgs";
    };
    nixpkgs.url = "github:NixOS/nixpkgs/nixos-unstable";
    flake-utils.url = "github:numtide/flake-utils";
  };

  outputs = inputs: with inputs;
    flake-utils.lib.eachDefaultSystem (system:
    let
      pkgs = nixpkgs.legacyPackages.${system};
      rust-toolchain = with fenix.packages.${system}; fromToolchainFile {
        file = ./rust-toolchain.toml;
        sha256 = "sha256-mvUGEOHYJpn3ikC5hckneuGixaC+yGrkMM/liDIDgoU=";
      };
    in {
      devShell = pkgs.mkShell {
        buildInputs = with pkgs; [
          lld
          rust-toolchain
          pkg-config
          rust-analyzer
          # cmake
          # gnumake
          # kdePackages.extra-cmake-modules
          # gdb
          # libx11
          # libx11.dev
          # wayland
          # wayland-protocols
          # libxkbcommon
          # wayland-scanner
          # libxrandr
          # libxinerama
          # libxcursor
          # libxi
          # libGL
        ];
        # LD_LIBRARY_PATH = pkgs.lib.makeLibraryPath buildInputs;
        shellHook = ''
          export RUST_LOG=debug
          export PATH="$PATH:$HOME/.cargo/bin"

          alias gc='git commit'
          alias gca='git commit --amend'
          alias gcm='git commit -m'
          alias gp='git push'
        '';
      };
    }
  );
}
