package org.lockss.rs.io.index.db;

import org.junit.Test;
import org.lockss.repository.RepositoryDbManager;
import org.lockss.repository.RepositoryManagerSql;
import org.lockss.test.ConfigurationUtil;
import org.lockss.test.LockssTestCase4;
import org.lockss.test.MockLockssDaemon;
import org.lockss.test.TcpTestUtil;
import org.lockss.util.Logger;
import org.lockss.util.rest.repo.model.Artifact;
import org.lockss.util.rest.repo.util.ArtifactSpec;

import java.io.File;
import java.net.URI;
import java.util.UUID;

public class TestSQLArtifactIndex extends LockssTestCase4 {
  private static final Logger log = Logger.getLogger();

  private MockLockssDaemon theDaemon;
  private String tempDirPath;
  private RepositoryDbManager repositoryDbManager;
  private String dbPort;

  @Override
  public void setUp() throws Exception {
    super.setUp();

    // Get the temporary directory used during the test.
    tempDirPath = setUpDiskSpace();

    theDaemon = getMockLockssDaemon();
    theDaemon.setDaemonInited(true);
    dbPort = Integer.toString(TcpTestUtil.findUnboundTcpPort());
    ConfigurationUtil.addFromArgs(RepositoryDbManager.PARAM_DATASOURCE_PORTNUMBER,
        dbPort);
  }

  @Override
  public void tearDown() throws Exception {
    if (repositoryDbManager != null)
      repositoryDbManager.stopService();

    theDaemon.stopDaemon();
    super.tearDown();
  }

  private void initializeDatabase() throws Exception {
    // Set the database log.
    System.setProperty("derby.stream.error.file",
        new File(tempDirPath, "derby.log").getAbsolutePath());

    // Create the database manager.
    repositoryDbManager = new RepositoryDbManager();
    repositoryDbManager.initService(theDaemon);
    repositoryDbManager.startService();

    theDaemon.setRepositoryDbManager(repositoryDbManager);
  }

  @Test
  public void testAddArtifact() throws Exception {
    ArtifactSpec spec = new ArtifactSpec()
        .setArtifactUuid(UUID.randomUUID().toString())
        .setUrl("test")
        .setStorageUrl(URI.create("test"))
        .setContentLength(1024)
        .setContentDigest("My Digest")
        .setCollectionDate(1234L);

    Artifact artifact = spec.getArtifact();

    initializeDatabase();

    RepositoryManagerSql repodb = new RepositoryManagerSql(repositoryDbManager);

    repodb.addArtifact(artifact);
  }
}
