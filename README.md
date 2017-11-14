[![Build Status](https://travis-ci.org/lockss/lockss-core.svg?branch=master)](https://travis-ci.org/lockss/lockss-core)
##  LOCKSS Core Library
The library used by all LAAWS service containing the core Lockss-daemon functionality.

### Clone the repo
`git clone ssh://git@gitlab.lockss.org/laaws/lockss-core.git`

### Create the Eclipse project (if so desired)
File -> Import... -> Maven -> Existing Maven Projects

### Build and test
`mvn clean package`

### Replace your build with the downloaded SNAPSHOT version.
`mvn install`

### To upload a new official SNAPSHOT
`mvn deploy -Dusername=user -Dpassword=password`
You must have write permissions in Artifactory to deploy a new SNAPSHOT.

### Modifying the pom.xml file
The pom file can be modified to include slightly different build settings.
Currently tests in parallel based on the number of processor you have on the build machine.
