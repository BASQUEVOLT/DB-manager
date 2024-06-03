{
  inputs = {
    nixpkgs.url = "nixpkgs/nixos-23.11";
    nixpkgs-unstable.url = "nixpkgs/nixos-unstable";
  };

  outputs = {
    self,
    nixpkgs,
    nixpkgs-unstable,
  }: let
    system = "x86_64-linux";
    pkgs = import nixpkgs {
      inherit system;
      config.allowUnfree = true;
      overlays = [
        (final: prev: {
          unstable = import nixpkgs-unstable {
            system = prev.system;
            config.allowUnfree = true;
          };
        })
      ];
    };
  in {
    devShells.x86_64-linux.rdlab_dbconnector = pkgs.mkShell {
      name = "lab-rd-dashboard";
      packages = [self.packages.x86_64-linux.rdlab_dbconnector];
    };
    packages.x86_64-linux.rdlab_dbconnector = pkgs.python3.pkgs.buildPythonPackage {
      pname = "rdlab_dbconnector";
      version = "0.1.1";
      src = ./.;
      format = "setuptools";
      propagatedBuildInputs = with pkgs.python3Packages; [
        mysql-connector
        pandas
        numpy
      ];
      doCheck = false;
    };
    packages.x86_64-linux.default = self.packages.x86_64-linux.rdlab_dbconnector;
  };
}
