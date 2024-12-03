##  LOCKSS Core Library
The library used by all LAAWS service containing the LOCKSS daemon core functionality.

## Note on branches
The `master` branch is for stable releases and the `develop` branch is for
ongoing development.

## Standard build and deployment
The LOCKSS cluster, including this project, is normally built and deployed using
the LOCKSS Installer, which uses `kubernetes`.

You can find more information about the installation of the LOCKSS system in the
[LOCKSS system manual](https://docs.lockss.org/projects/manual).


### Clone the repo
`git clone ssh://git@gitlab.lockss.org/laaws/lockss-core.git`

### Create the Eclipse project (if so desired)
File -> Import... -> Maven -> Existing Maven Projects

### Build and test
`mvn clean package`

### Replace your build with the downloaded SNAPSHOT version.
`mvn install`
