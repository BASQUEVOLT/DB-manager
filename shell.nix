{pkgs ? import <nixpkgs> {}}:
pkgs.mkShell {
  name = "dbmanager-shell";
  nativeBuildInputs = with pkgs; [
    pyright
    ruff
    (
      let
        db-manager = python3.pkgs.buildPythonPackage {
          pname = "db-manager";
          version = "0.1.0";
          src = ./.;
          format = "setuptools";
          propagatedBuildInputs = with pkgs.python3Packages; [
            mysql-connector
            pandas
            numpy
          ];
          doCheck=false;
        };
      in
        python3.withPackages (ps: [
          db-manager
        ])
    )
  ];
}
