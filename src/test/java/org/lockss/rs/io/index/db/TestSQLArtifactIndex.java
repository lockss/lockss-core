package org.lockss.rs.io.index.db;

import org.lockss.rs.io.index.AbstractArtifactIndexTest;
import org.lockss.test.MockLockssDaemon;

import java.io.File;

public class TestSQLArtifactIndex extends AbstractArtifactIndexTest<SQLArtifactIndex> {

  private SQLArtifactIndexDbManager idxDbManager;
  private MockLockssDaemon theDaemon;
  private String tempDirPath;

  @Override
  protected SQLArtifactIndex makeArtifactIndex() throws Exception {
    // Construct mock LOCKSS daemon
    theDaemon = getMockLockssDaemon();
    theDaemon.setDaemonInited(true);

    // Get the temporary directory used during the test
    tempDirPath = setUpDiskSpace();

    initializeTestDbManager(0, 2);

    return new SQLArtifactIndex();
  }

  private void initializeTestDbManager(int initialVersion, int targetVersion) {
    // Set the database log.
    System.setProperty("derby.stream.error.file",
        new File(tempDirPath, "derby.log").getAbsolutePath());

    // Create the database manager.
    idxDbManager = new SQLArtifactIndexDbManager();
    idxDbManager.initService(theDaemon);

//    assertTrue(repositoryDbManager.setUpDatabase(initialVersion));
    idxDbManager.setTargetDatabaseVersion(targetVersion);
    idxDbManager.startService();

    theDaemon.setSQLArtifactIndexDbManager(idxDbManager);
  }

  @Override
  public void testInitIndex() throws Exception {
    // Intentionally left blank
  }

  @Override
  public void testShutdownIndex() throws Exception {
    // Intentionally left blank
  }
}
