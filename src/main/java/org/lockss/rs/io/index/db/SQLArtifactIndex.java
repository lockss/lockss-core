package org.lockss.rs.io.index.db;

import org.apache.commons.lang3.StringUtils;
import org.lockss.app.LockssApp;
import org.lockss.db.DbException;
import org.lockss.log.L4JLogger;
import org.lockss.rs.io.index.AbstractArtifactIndex;
import org.lockss.util.os.PlatformUtil;
import org.lockss.util.rest.repo.model.*;
import org.lockss.util.storage.StorageInfo;

import java.io.IOException;
import java.util.UUID;

public class SQLArtifactIndex extends AbstractArtifactIndex {
  private final static L4JLogger log = L4JLogger.getLogger();

  public static String ARTIFACT_INDEX_TYPE = "SQL";

  private SQLArtifactIndexManagerSql idxdb = null;

  @Override
  public void init() {
    try {
      idxdb = new SQLArtifactIndexManagerSql(LockssApp.getManagerByTypeStatic(SQLArtifactIndexDbManager.class));
    } catch (DbException e) {
      throw new IllegalStateException("No database", e);
    }
  }

  @Override
  public boolean isReady() {
    return idxdb != null;
  }

  @Override
  public void start() {
    // Intentionally left blank
  }

  @Override
  public void stop() {
    // Intentionally left blank
  }

  @Override
  public StorageInfo getStorageInfo() {
    // FIXME: Use correct Derby / PostgreSQL data path
    return StorageInfo.fromDF(ARTIFACT_INDEX_TYPE, PlatformUtil.getInstance().getDF("/"));
  }

  @Override
  public void acquireVersionLock(ArtifactIdentifier.ArtifactStem stem) throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void releaseVersionLock(ArtifactIdentifier.ArtifactStem stem) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void indexArtifact(Artifact artifact) throws IOException {
    if (artifact == null) {
      throw new IllegalArgumentException("Null artifact");
    }

    try {
      idxdb.addArtifact(artifact);
    } catch (DbException e) {
      throw new IOException("Could not add artifact to database", e);
    }
  }

  @Override
  public void indexArtifacts(Iterable<Artifact> artifacts) throws IOException {
    try {
      idxdb.addArtifacts(artifacts);
    } catch (DbException e) {
      throw new IOException("Could not add artifact to database", e);
    }
  }

  @Override
  public Artifact getArtifact(String uuid) throws IOException {
    if (StringUtils.isEmpty(uuid)) {
      throw new IllegalArgumentException("Null or empty artifact UUID");
    }

    try {
      return idxdb.getArtifact(uuid);
    } catch (DbException e) {
      throw new IOException("Could not retrieve artifact from database", e);
    }
  }

  @Override
  // Q: Get rid of method from API?
  public Artifact getArtifact(UUID uuid) throws IOException {
    if (uuid == null) {
      throw new IllegalArgumentException("Null UUID");
    }

    return getArtifact(uuid.toString());
  }

  @Override
  public Artifact getArtifact(String namespace, String auid, String url, boolean includeUncommitted)
      throws IOException {
    try {
      return idxdb.getLatestArtifact(namespace, auid, url, includeUncommitted);
    } catch (DbException e) {
      throw new IOException("Could not query database for artifact", e);
    }
  }

  @Override
  public Artifact getArtifactVersion(String namespace, String auid, String url, Integer version, boolean includeUncommitted)
      throws IOException {

    try {
      // Q: Of what use is includeUncommitted here? The tuple (ns, auid, url, ver)
      //  uniquely identifies an artifact and we either have it or we don't
      return idxdb.getArtifact(namespace, auid, url, version, includeUncommitted);
    } catch (DbException e) {
      throw new IOException("Could not query database for artifact", e);
    }
  }

  @Override
  public Artifact commitArtifact(String uuid) throws IOException {
    if (StringUtils.isEmpty(uuid)) {
      throw new IllegalArgumentException("Null or empty artifact UUID");
    }

    try {
      idxdb.commitArtifact(uuid);
      // Q: Remove Artifact return from method signature?
      return getArtifact(uuid);
    } catch (DbException e) {
      throw new IOException("Could not mark artifact as committed in database", e);
    }
  }

  @Override
  // Q: Get rid of method from API?
  public Artifact commitArtifact(UUID uuid) throws IOException {
    if (uuid == null) {
      throw new IllegalArgumentException("Null UUID");
    }

    return commitArtifact(uuid.toString());
  }

  @Override
  public boolean deleteArtifact(String uuid) throws IOException {
    if (StringUtils.isEmpty(uuid)) {
      throw new IllegalArgumentException("Null or empty UUID");
    }

    try {
      if (idxdb.deleteArtifact(uuid) == 0) {
        return false;
      }

      // Q: Remove boolean return from method signature?
      return true;
    } catch (DbException e) {
      throw new IOException("Could not remove artifact from database", e);
    }
  }

  @Override
  // Q: Get rid of method from API?
  public boolean deleteArtifact(UUID uuid) throws IOException {
    if (uuid == null) {
      throw new IllegalArgumentException("Null UUID");
    }

    return deleteArtifact(uuid.toString());
  }

  @Override
  public boolean artifactExists(String uuid) throws IOException {
    if (StringUtils.isEmpty(uuid)) {
      throw new IllegalArgumentException("Null or empty artifact UUID");
    }

    return getArtifact(uuid) != null;
  }

  @Override
  public Artifact updateStorageUrl(String uuid, String storageUrl) throws IOException {
    if (StringUtils.isEmpty(uuid)) {
      throw new IllegalArgumentException("Invalid artifact UUID");
    }

    try {
      if (idxdb.updateStorageUrl(uuid, storageUrl) == 0) {
        throw new IOException("Artifact not found");
      }

      // Q: Remove Artifact return from method signature?
      return getArtifact(uuid);
    } catch (DbException e) {
      throw new IOException("Database error trying to update storage URL", e);
    }
  }

  @Override
  public Iterable<String> getNamespaces() throws IOException {
    try {
      return idxdb.getNamespaces();
    } catch (DbException e) {
      throw new IOException("Database error fetching namespaces", e);
    }
  }

  @Override
  public Iterable<String> getAuIds(String namespace) throws IOException {
    try {
      return idxdb.findAuids(namespace);
    } catch (DbException e) {
      throw new IOException("Database error fetching AUIDs", e);
    }
  }

  @Override
  public Iterable<Artifact> getArtifacts(String namespace, String auid, boolean includeUncommitted)
      throws IOException {
    try {
      return idxdb.findLatestArtifactsOfAllUrlsWithNamespaceAndAuid(namespace, auid, includeUncommitted);
    } catch (DbException e) {
      throw new IOException("Database error fetching artifacts", e);
    }
  }

  @Override
  public Iterable<Artifact> getArtifactsAllVersions(String namespace, String auid, boolean includeUncommitted)
      throws IOException {
    try {
      return idxdb.findArtifactsAllVersionsOfAllUrlsWithNamespaceAndAuid(namespace, auid, includeUncommitted);
    } catch (DbException e) {
      throw new IOException("Database error fetching artifacts", e);
    }
  }

  @Override
  public Iterable<Artifact> getArtifactsAllVersions(String namespace, String auid, String url)
      throws IOException {
    try {
      return idxdb.findArtifactsAllCommittedVersionsOfUrlWithNamespaceAndAuid(namespace, auid, url);
    } catch (DbException e) {
      throw new IOException("Database error fetching artifacts", e);
    }
  }

  @Override
  public Iterable<Artifact> getArtifactsWithUrlFromAllAus(String namespace, String url, ArtifactVersions versions)
      throws IOException {

    if (!(versions == ArtifactVersions.ALL ||
        versions == ArtifactVersions.LATEST)) {
      throw new IllegalArgumentException("Versions must be ALL or LATEST");
    }

    if (namespace == null || url == null) {
      throw new IllegalArgumentException("Namespace or URL is null");
    }

    try {
      return idxdb.findArtifactsAllCommittedVersionsOfUrlAllAuidsInNamespace(namespace, url, versions);
    } catch (DbException e) {
      throw new IOException("Database error fetching artifacts", e);
    }
  }

  @Override
  public Iterable<Artifact> getArtifactsWithPrefix(String namespace, String auid, String prefix)
      throws IOException {
    try {
      return idxdb.findArtifactsLatestCommittedVersionsOfAllUrlsMatchingPrefixWithNamespaceAndAuid(namespace, auid, prefix);
    } catch (DbException e) {
      throw new IOException("Database error fetching artifacts", e);
    }
  }

  @Override
  public Iterable<Artifact> getArtifactsWithPrefixAllVersions(String namespace, String auid, String prefix)
      throws IOException {
    try {
      return idxdb.findArtifactsAllCommittedVersionsOfAllUrlsMatchingPrefixWithNamespaceAndAuid(namespace, auid, prefix);
    } catch (DbException e) {
      throw new IOException("Database error fetching artifacts", e);
    }
  }

  @Override
  public Iterable<Artifact> getArtifactsWithUrlPrefixFromAllAus(String namespace, String prefix, ArtifactVersions versions)
      throws IOException {

    if (!(versions == ArtifactVersions.ALL ||
        versions == ArtifactVersions.LATEST)) {
      throw new IllegalArgumentException("Versions must be ALL or LATEST");
    }

    if (namespace == null) {
      throw new IllegalArgumentException("Namespace is null");
    }

    try {
      return idxdb.findArtifactsAllCommittedVersionsOfUrlByPrefixAllAuidsInNamespace(namespace, prefix, versions);
    } catch (DbException e) {
      throw new IOException("Database error fetching artifacts", e);
    }
  }

  private AuSize findOrComputeAuSize(String namespace, String auid) throws DbException, IOException {
    AuSize result = new AuSize();

    long totalWarcSize = repository.getArtifactDataStore()
        .auWarcSize(namespace, auid);

    result.setTotalWarcSize(totalWarcSize);
    result.setTotalAllVersions(idxdb.getSizeOfArtifacts(namespace, auid, ArtifactVersions.ALL));
    result.setTotalLatestVersions(idxdb.getSizeOfArtifacts(namespace, auid, ArtifactVersions.LATEST));

    return result;
  }

  @Override
  public AuSize auSize(String namespace, String auid) throws IOException {
    try {
      return findOrComputeAuSize(namespace, auid);
    } catch (DbException e) {
      throw new IOException("Could not query AU size from database", e);
    }
  }
}
