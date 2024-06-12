package org.lockss.rs.io.index.db;

import org.lockss.rs.io.index.AbstractArtifactIndexTest;
import org.lockss.test.ConfigurationUtil;
import org.lockss.test.MockLockssDaemon;
import org.postgresql.ds.PGSimpleDataSource;

import java.io.File;
import java.io.IOException;

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

//    initializeDerby();
    initializePostgreSQL();

    return new SQLArtifactIndex();
  }

  protected void initializePostgreSQL() throws Exception {
    ConfigurationUtil.addFromArgs(
        SQLArtifactIndexDbManager.PARAM_DATASOURCE_USER, "postgres",
        SQLArtifactIndexDbManager.PARAM_DATASOURCE_PASSWORD, "postgresx");

    ConfigurationUtil.addFromArgs(
        SQLArtifactIndexDbManager.DATASOURCE_ROOT + ".dbcp.enabled", "true",
        SQLArtifactIndexDbManager.DATASOURCE_ROOT + ".dbcp.initialSize", "2");

    ConfigurationUtil.addFromArgs(
        SQLArtifactIndexDbManager.PARAM_MAX_RETRY_COUNT, "0",
        SQLArtifactIndexDbManager.PARAM_RETRY_DELAY, "0");

    ConfigurationUtil.addFromArgs(
        SQLArtifactIndexDbManager.PARAM_DATASOURCE_CLASSNAME, PGSimpleDataSource.class.getCanonicalName(),
        SQLArtifactIndexDbManager.PARAM_DATASOURCE_PASSWORD, "postgres");

    idxDbManager = new SQLArtifactIndexDbManager();
    startEmbeddedPgDbManager(idxDbManager);
    idxDbManager.initService(getMockLockssDaemon());

    idxDbManager.setTargetDatabaseVersion(3);
    idxDbManager.startService();

    theDaemon.setSQLArtifactIndexDbManager(idxDbManager);
  }

  private void initializeDerby() throws IOException {
    // Set the database log.
    System.setProperty("derby.stream.error.file",
        new File(tempDirPath, "derby.log").getAbsolutePath());

    // Create the database manager.
    idxDbManager = new SQLArtifactIndexDbManager();
    idxDbManager.initService(theDaemon);

    idxDbManager.setTargetDatabaseVersion(3);
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
