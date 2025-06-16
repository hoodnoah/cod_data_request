{
  description = "A Nix-flake-based python + Go 1.24 Dev environment";

  inputs = {
    # List of platform identifiers, e.g. "x86_64-linux" etc.
    systems.url = "github:nix-systems/default";

    # Snapshot of nixpkgs, pinned by a FlakeHub wildcard.
    nixpkgs.url = "nixpkgs/nixos-unstable";

    flake-utils.url = "github:numtide/flake-utils";
  };

  # ──────────────────────────────────────────────────────────
  # outputs : receives materialized inputs and *returns* an attr‑set
  # ──────────────────────────────────────────────────────────
  outputs =
    {
      self,
      nixpkgs,
      systems,
      flake-utils,
    }:
    let
      lib = nixpkgs.lib; # Nixpkgs standard library
      eachSystem = lib.genAttrs (import systems);
    in
    {
      devShells = eachSystem (
        system:
        let
          pkgs = nixpkgs.legacyPackages.${system};
          go = pkgs.go_1_24;
        in
        {
          default = pkgs.mkShell {
            # packages placed on $PATH
            packages = with pkgs; [
              # --- Python toolchain ---
              python312
              dotnetCorePackages.sdk_9_0
              curl
              git

              # --- Go toolchain ---
              go
              gotools
              golangci-lint
              gopls
              gomodifytags
              gotests
              godef
            ];

            shellHook = ''
              if [ ! -d .venv ]; then
                echo "Creating python virtual environment..."
                python3 -m venv .venv
              fi

              .venv/bin/pip install --upgrade pip wheel
              .venv/bin/pip install -r requirements.txt
              source .venv/bin/activate
            '';
          };
        }
      );
    };
}
