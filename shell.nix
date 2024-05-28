{pkgs ? import <nixpkgs> {}}:
pkgs.mkShell {
  name = "dbmanager-shell";
  nativeBuildInputs = with pkgs; [
    pyright
    ruff
    (
      let
        rdlab_dbconnector = python3.pkgs.buildPythonPackage {
          pname = "rdlab_dbconnector";
          version = "0.1.0";
          src = ./.;
          format = "setuptools";
          propagatedBuildInputs = with pkgs.python3Packages; [
            mysql-connector
            pandas
            numpy
          ];
          doCheck = false;
        };
      in
        python3.withPackages (ps: [
          db-manager
        ])
    )
  ];
}
