with import <nixpkgs> {};

mkShell {
  buildInputs = [
    python313
    python313Packages.requests
    pyright
  ];
}
