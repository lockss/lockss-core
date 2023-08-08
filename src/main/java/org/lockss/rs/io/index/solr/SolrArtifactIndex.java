/*

Copyright (c) 2000-2022, Board of Trustees of Leland Stanford Jr. University

Redistribution and use in source and binary forms, with or without
modification, are permitted provided that the following conditions are met:

1. Redistributions of source code must retain the above copyright notice,
this list of conditions and the following disclaimer.

2. Redistributions in binary form must reproduce the above copyright notice,
this list of conditions and the following disclaimer in the documentation
and/or other materials provided with the distribution.

3. Neither the name of the copyright holder nor the names of its contributors
may be used to endorse or promote products derived from this software without
specific prior written permission.

THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE
LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
POSSIBILITY OF SUCH DAMAGE.

*/

package org.lockss.rs.io.index.solr;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.collections4.IterableUtils;
import org.apache.commons.collections4.IteratorUtils;
import org.apache.commons.collections4.map.LRUMap;
import org.apache.commons.io.filefilter.*;
import org.apache.commons.lang3.StringUtils;
import org.apache.solr.client.solrj.*;
import org.apache.solr.client.solrj.beans.DocumentObjectBinder;
import org.apache.solr.client.solrj.impl.BaseHttpSolrClient;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.client.solrj.request.CoreAdminRequest;
import org.apache.solr.client.solrj.request.QueryRequest;
import org.apache.solr.client.solrj.request.SolrPing;
import org.apache.solr.client.solrj.request.UpdateRequest;
import org.apache.solr.client.solrj.response.*;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.params.CoreAdminParams;
import org.apache.solr.common.util.NamedList;
import org.lockss.app.LockssApp;
import org.lockss.db.DbException;
import org.lockss.log.L4JLogger;
import org.lockss.repository.RepositoryDbManager;
import org.lockss.repository.RepositoryManagerSql;
import org.lockss.rs.BaseLockssRepository;
import org.lockss.rs.io.index.AbstractArtifactIndex;
import org.lockss.rs.io.index.ArtifactIndex;
import org.lockss.util.io.FileUtil;
import org.lockss.util.rest.repo.model.*;
import org.lockss.util.rest.repo.util.ArtifactComparators;
import org.lockss.util.rest.repo.util.SemaphoreMap;
import org.lockss.util.storage.StorageInfo;
import org.lockss.util.time.TimeBase;
import org.lockss.util.time.TimeUtil;

import java.io.File;
import java.io.FileFilter;
import java.io.IOException;
import java.io.InterruptedIOException;
import java.net.URI;
import java.nio.file.Path;
import java.sql.Connection;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.concurrent.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

/**
 * An Apache Solr implementation of ArtifactIndex.
 */
public class SolrArtifactIndex extends AbstractArtifactIndex {
  private final static L4JLogger log = L4JLogger.getLogger();

  private final static String DEFAULT_COLLECTION_NAME = "lockss-repo";
  public final static long DEFAULT_SOLR_HARDCOMMIT_INTERVAL = 15000;

  private static final int SOLR_RETRY_DELAY = 1000; // ms
  private static final int SOLR_RETRY_SHORT = 5;
  private static final int SOLR_RETRY_LONG = 3600;

  private static final SolrQuery.SortClause SORTURI_ASC = new SolrQuery.SortClause("sortUri", SolrQuery.ORDER.asc);
  private static final SolrQuery.SortClause VERSION_DESC = new SolrQuery.SortClause("version", SolrQuery.ORDER.desc);
  private static final SolrQuery.SortClause AUID_ASC = new SolrQuery.SortClause("auid", SolrQuery.ORDER.asc);

  /**
   * Label to describe type of SolrArtifactIndex
   */
  public static String ARTIFACT_INDEX_TYPE = "Solr";

  private static final String SOLR_UPDATE_JOURNAL_NAME = "solr-update-journal";

  // TODO: Currently only used for getStorageInfo()
  private static final Pattern SOLR_COLLECTION_ENDPOINT_PATTERN = Pattern.compile("/solr(/(?<collection>[^/]+)?)?$");

  protected LockssApp theApp = null;

  private Map<String, CompletableFuture<AuSize>> auSizeFutures =
      new ConcurrentHashMap<>();

  private Map<String, Boolean> invalidatedAuSizes =
      Collections.synchronizedMap(new LRUMap<>(100));

  /**
   * The Solr client implementation to use to talk to a Solr server.
   */
  private final SolrClient solrClient;

  /**
   * Two element list containing [username, password].
   */
  private List<String> solrCredentials;

  /**
   * Name of the Solr collection to use.
   */
  private String solrCollection = DEFAULT_COLLECTION_NAME;
  private RepositoryManagerSql repositoryManagerSql = null;

  /**
   * This boolean is used to indicate whether a HttpSolrClient was created internally
   * with a provided Solr URL.
   */
  private final boolean isInternalClient;

  /**
   * Map from artifact stem to semaphore. Used for artifact version locking.
   */
  private SemaphoreMap<ArtifactIdentifier.ArtifactStem> versionLock = new SemaphoreMap<>();

  /**
   * Handle to Solr soft commit journal writer.
   */
  private SolrCommitJournal.SolrJournalWriter solrJournalWriter;

  /**
   * An internal {@code boolean} indicating whether a hard commit is needed.
   */
  private volatile boolean hardCommitNeeded = false;

  /**
   * Last start time seen of the remote Solr index. Used to determine whether
   * Solr has restarted since.
   */
  private static long lastStartTime;

  /**
   * Interval (in ms) between Solr hard commits performed by {@link SolrHardCommitTask}.
   */
  long hardCommitInterval = DEFAULT_SOLR_HARDCOMMIT_INTERVAL;

  /**
   * Constructor. Creates and uses an internal {@link HttpSolrClient} from the provided Solr collection endpoint.
   *
   * @param solrBaseUrl A {@link String} containing the URL to a Solr collection or core.
   */
  public SolrArtifactIndex(String solrBaseUrl) {
    this(solrBaseUrl, null, null);
  }

  public SolrArtifactIndex(String solrBaseUrl, List<String> solrCredentials) {
    this(solrBaseUrl, null, solrCredentials);
  }

  /**
   * Constructor. Creates and uses an internal {@link HttpSolrClient} from the provided Solr base URL and Solr
   * collection or core name.
   *
   * @param solrBaseUrl A {@link String} containing the Solr base URL.
   * @param collection  @ {@link String} containing the name of the Solr collection to use.
   */
  public SolrArtifactIndex(String solrBaseUrl, String collection) {
    this(solrBaseUrl, collection, null);
  }

  /**
   * Constructor.
   *
   * @param solrBaseUrl A {@link String} containing the Solr base URL.
   * @param collection  @ {@link String} containing the name of the Solr collection to use.
   * @param solrCredentials A {@link List<String>} containing the username and password for Solr.
   */
  public SolrArtifactIndex(String solrBaseUrl, String collection, List<String> solrCredentials) {
    // Convert provided Solr base URL to URI object
    URI baseUrl = URI.create(solrBaseUrl);

    // Set Solr collection to use
    this.solrCollection = collection;

    // Determine Solr collection/core name from base URL if collection is null or empty
    if (solrCollection == null || solrCollection.isEmpty()) {
      String restEndpoint = baseUrl.normalize().getPath();
      Matcher m = SOLR_COLLECTION_ENDPOINT_PATTERN.matcher(restEndpoint);

      if (m.find()) {
        // Yes: Get the name of the core from the match results
        solrCollection = m.group("collection");

        // Q: I don't think the regex allows these possibilities?
        if (solrCollection == null || solrCollection.isEmpty()) {
          log.error("Solr collection not specified explicitly or in the Solr base URL");
          throw new IllegalArgumentException("Missing Solr collection name");
        }
      } else {
        // No: Did not match expected pattern
        log.error("Unexpected Solr base URL [solrBaseUrl: {}]", solrBaseUrl);
        throw new IllegalArgumentException("Unexpected Solr base URL");
      }
    }

    // Set the Solr credentials
    this.solrCredentials = solrCredentials;

    // Resolve Solr base REST endpoint
    URI solrUrl = baseUrl.resolve("/solr");

    // Build a HttpSolrClient instance using the base URL
    this.solrClient = new HttpSolrClient.Builder()
        .withBaseSolrUrl(solrUrl.toString())
        .build();

    this.isInternalClient = true;
  }

  /**
   * Adds Solr BasicAuth credentials to a {@link SolrRequest}.
   *
   * @param request
   */
  void addSolrCredentials(SolrRequest request) {
    // Add Solr BasicAuth credentials if present
    if (solrCredentials != null) {
      request.setBasicAuthCredentials(
          /* Username */ solrCredentials.get(0),
          /* Password */ solrCredentials.get(1)
      );
    }
  }

  /**
   * Constructor taking the name of the Solr core to use and an  {@link SolrClient}.
   *
   * @param solrClient The {@link SolrClient} to perform operations to the Solr core through.
   * @param collection A {@link String} containing name of the Solr core to use.
   */
  public SolrArtifactIndex(SolrClient solrClient, String collection) {
    this.solrCollection = collection;
    this.solrClient = solrClient;
    this.isInternalClient = false;
  }

  /**
   * Initializes this {@link SolrArtifactIndex}.
   */
  @Override
  public synchronized void init() {
    log.debug("Initializing Solr artifact index");

    if (getState() == ArtifactIndexState.STOPPED) {
      try {
        // Wait for Solr to become ready
        waitForSolrReady();
      } catch (InterruptedException e) {
        log.error("Interrupted while waiting for Solr to become ready");
        throw new RuntimeException(e);
      }

      // Path to artifact index state directory
      Path indexStateDir =
          ((BaseLockssRepository)repository).getRepositoryStateDir()
              .toPath()
              .resolve("index"); // TODO: Parameterize

      // Ensure index state directory exists
      FileUtil.ensureDirExists(indexStateDir.toFile());

      // Ensure Solr update journal directory exists
      FileUtil.ensureDirExists(getSolrJournalDirectory().toFile());

      setState(ArtifactIndexState.INITIALIZED);
    }
  }

  private void waitForSolrReady() throws InterruptedException {
    int retries = 0;
    boolean notified = false;

    log.info("Waiting for Solr to become ready...");

    while (true) {
      try {
        SolrQuery q = new SolrQuery();
        q.setQuery("*:*");
        q.setRows(1);

        QueryRequest request = new QueryRequest(q);
        addSolrCredentials(request);
        request.process(solrClient, getSolrCollection());

        log.info("Solr is ready");
        break;
      } catch (BaseHttpSolrClient.RemoteSolrException | SolrServerException | IOException e) {
        if (SOLR_RETRY_SHORT < ++retries) {
          if (!notified || (notified && (retries % SOLR_RETRY_LONG == 0))) {
            log.debug2("Could not query Solr core", e);
            log.warn("Still waiting for Solr to become ready...");
            notified = true;
          }
        }
        Thread.sleep(SOLR_RETRY_DELAY);
      }
    }
  }

  /**
   * Starts this {@link SolrArtifactIndex}:
   *
   * <ul>
   *   <li>Replays any Solr operations recorded in the Solr soft commit journal, if one exists.</li>
   *   <li>Schedules a Solr hard commit at regular intervals.</li>
   *   <li>Opens a new journal to log changes made to the Solr index since the last hard commit.</li>
   * </ul>
   */
  @Override
  public synchronized void start() {
    log.debug("Starting Solr artifact index");

    // Replay journal of Solr index updates if it exists
    replayUpdateJournal();

    // Start journal of Solr index updates
    startUpdateJournal();

    // Schedule hard commits
    scheduleHardCommitter();

    // Set index state to RUNNING
    setState(ArtifactIndexState.RUNNING);
  }

  /**
   * Replays all Solr update journal files found under the Solr journal directory.
   */
  public void replayUpdateJournal() {
    log.debug("Starting Solr update journal replay");

    File journalDir = getSolrJournalDirectory().toFile();

    File[] journalFiles = journalDir
        .listFiles((FileFilter) new WildcardFileFilter(SOLR_UPDATE_JOURNAL_NAME + ".*.csv"));

    if (journalFiles == null) {
      throw new RuntimeException("Error searching for journal files");
    }

    if (journalFiles.length > 0) {
      startUpdateJournal();
    }

    for (File journalFile : journalFiles) {
      if (journalFile.exists() && journalFile.isFile()) {
        try (SolrCommitJournal.SolrJournalReader journalReader =
                 new SolrCommitJournal.SolrJournalReader(journalFile.toPath())) {

          journalReader.replaySolrJournal(this);
          journalFile.delete();

        } catch (IOException e) {
          log.error("Could not replay operations from Solr update journal", e);
          throw new RuntimeException("Could not replay journal", e);
        }
      }
    }
  }

  /**
   * Returns the path to the journal maintained by this {@link SolrArtifactIndex}.
   *
   * @return A {@link Path} containing the path of the journal.
   */
  private Path getSolrJournalDirectory() {
    return ((BaseLockssRepository) repository)
        .getRepositoryStateDir()
        .toPath()
        .resolve("index/solr");
  }

  /**
   * Opens a new journal (using {@link SolrCommitJournal.SolrJournalWriter}) to record updates made to the Solr index.
   */
  public synchronized void startUpdateJournal() {
    if (solrJournalWriter == null) {
      log.debug("Creating new Solr journal");

      try {
        // Get journal directory and ensure it exists
        FileUtil.ensureDirExists(getSolrJournalDirectory().toFile());

        // Start new Solr journal writer
        Path journalPath = getSolrJournalDirectory().resolve(generateSolrUpdateJournalName());
        solrJournalWriter = new SolrCommitJournal.SolrJournalWriter(journalPath);
      } catch (IOException e) {
        // FIXME: Revisit
        log.error("Could not create Solr journal", e);
        throw new IllegalStateException("Could not create Solr journal", e);
      }
    } else {
      log.warn("Solr journal already open");
    }
  }

  /**
   * Closes an open {@link SolrCommitJournal.SolrJournalWriter}.
   */
  public synchronized void closeJournal() {
    try {
      if (solrJournalWriter != null) {
        solrJournalWriter.close();
        solrJournalWriter = null;
      }
    } catch (IOException e) {
      // FIXME: Revisit
      log.error("Could not close Solr journal", e);
      throw new IllegalStateException("Could not close Solr journal");
    }
  }

  /**
   * @return The {@link SolrClient} through which this {@link SolrArtifactIndex} interfaces with Solr.
   */
  protected SolrClient getSolrClient() {
    return this.solrClient;
  }

  /**
   * Arrange for the hard commit task to run once, in
   * hardCommitInterval ms.
   */
  private void scheduleHardCommitter() {
    ((BaseLockssRepository) repository).getScheduledExecutorService()
      .schedule(new SolrHardCommitTask(), hardCommitInterval, TimeUnit.MILLISECONDS);
    log.debug("Scheduled Solr hard commit in {}",
               TimeUtil.timeIntervalToString(hardCommitInterval));
  }

  /**
   * Implementation of {@link Runnable} which performs a Solr hard commit if there have been any
   * soft commits since the last hard commit.
   */
  private class SolrHardCommitTask implements Runnable {
    private final static long SOLR_START_TIME_SLOP = 15000;

    @Override
    public void run() {
      try {
        checkForSolrRestart();

        // Proceed only if there were any soft commits since the last hard commit
        if (!hardCommitNeeded) {
          log.debug2("Skipping Solr hard commit");
          return;
        }

        doHardCommit();
      } catch (Exception e) {
        log.error("Unexpected exception", e);
        throw e;
      } finally {
        scheduleHardCommitter();
      }
    }

    private void checkForSolrRestart() {
      try {
        // Get Solr core status
        CoreAdminRequest req = new CoreAdminRequest();
        req.setCoreName(getSolrCollection());
        req.setAction(CoreAdminParams.CoreAdminAction.STATUS);

        // Add credentials to Solr request
        addSolrCredentials(req);

        // Get uptime from Solr core status request
        CoreAdminResponse response = req.process(solrClient);
        Long uptimeMs = response.getUptime(getSolrCollection());
        Long startTime = TimeBase.nowMs() - uptimeMs;

        // Initial case
        if (lastStartTime <= 0) {
          lastStartTime = startTime;
        }

        // Detect if there was a restart
        if (lastStartTime + SOLR_START_TIME_SLOP < startTime) {
          log.warn("Detected a Solr restart");
          lastStartTime = startTime;

//          // Replay journal since last hard commit
//          Path journalPath = getSolrJournalPath();
//          closeJournal();
//
//          try (SolrCommitJournal.SolrJournalReader journalReader =
//                   new SolrCommitJournal.SolrJournalReader(journalPath)) {
//            journalReader.replaySolrJournal(index);
//          } catch (IOException e) {
//            log.error("Could not replay journal", e);
//          }
        }
      } catch (IOException | SolrServerException e) {
        log.error("Could not get Solr uptime", e);
      }
    }

    private void doHardCommit() {
      log.debug2("Performing Solr hard commit");

      try {
        // Start new journal
        SolrCommitJournal.SolrJournalWriter lastJournalWriter = solrJournalWriter;
        Path journalPath = getSolrJournalDirectory().resolve(generateSolrUpdateJournalName());
        solrJournalWriter = new SolrCommitJournal.SolrJournalWriter(journalPath);
        lastJournalWriter.close();

        // Perform a hard commit
        hardCommitNeeded = false;
        handleSolrCommit(SolrCommitStrategy.HARD);

        // Find all journal files and exclude the active one
        IOFileFilter journalFileFilter = new WildcardFileFilter(SOLR_UPDATE_JOURNAL_NAME + ".*.csv");
        IOFileFilter excludeFileFilter =
            new NotFileFilter(new NameFileFilter(String.valueOf(solrJournalWriter.getJournalPath().getFileName())));
        FileFilter filter = new AndFileFilter(journalFileFilter, excludeFileFilter);

        // Remove inactive journal files
        File journalDir = getSolrJournalDirectory().toFile();
        File[] journalFiles = journalDir.listFiles(filter);
        Arrays.stream(journalFiles).forEach(File::delete);
      } catch (IOException | SolrServerException e) {
        log.error("Could not perform Solr hard commit", e);
      }
    }
  }

  /**
   * Generates a new Solr update journal filename.
   *
   * @return A {@link String} containing the generated journal filename.
   */
  private String generateSolrUpdateJournalName() {
    DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyyMMddHHmmss")
        .withZone(ZoneOffset.UTC);
    return String.format("%s.%s.csv", SOLR_UPDATE_JOURNAL_NAME, formatter.format(Instant.now()));
  }

  /**
   * Returns information about the storage size and free space.
   *
   * @return A {@link StorageInfo}
   */
  @Override
  public StorageInfo getStorageInfo() {
    try {
      // Create new StorageInfo object
      StorageInfo info = new StorageInfo(ARTIFACT_INDEX_TYPE);

      // Retrieve Solr core metrics
      MetricsRequest.CoreMetricsRequest req = new MetricsRequest.CoreMetricsRequest();
      addSolrCredentials(req);
      MetricsResponse.CoreMetricsResponse res = req.process(solrClient);

      MetricsResponse.CoreMetrics metrics = res.getCoreMetrics(solrCollection);

      // Populate StorageInfo from Solr core metrics
      info.setName(metrics.getIndexDir());
      info.setSizeKB(StorageInfo.toKBRounded(metrics.getTotalSpace()));
      info.setUsedKB(StorageInfo.toKBRounded(metrics.getIndexSizeInBytes()));
      info.setAvailKB(StorageInfo.toKBRounded(metrics.getUsableSpace()));
      info.setPercentUsed((double) info.getUsedKB() / (double) info.getSizeKB());
      info.setPercentUsedString(Math.round(100 * info.getPercentUsed()) + "%");

      // Return populated StorageInfo
      return info;
    } catch (SolrServerException | IOException e) {
      // Q: Throw or return null?
      log.error("Could not retrieve metrics from Solr", e);
      return null;
    }
  }

  public String getSolrCollection() {
    return solrCollection;
  }

  public void setSolrCollection(String collection) {
    this.solrCollection = collection;
  }

  public long getHardCommitInterval() {
    return hardCommitInterval;
  }

  public SolrArtifactIndex setHardCommitInterval(long interval) {
    this.hardCommitInterval = interval;
    return this;
  }

  /**
   * Returns a Solr response unchanged, if it has a zero status; throws,
   * otherwise.
   *
   * @param solrResponse A SolrResponseBase with the Solr response tobe handled.
   * @param errorMessage A String with a custom error message to be included in
   *                     the thrown exception, if necessary.
   * @return a SolrResponseBase with the passed Solr response unchanged, if it
   * has a zero status.
   * @throws SolrResponseErrorException if the passed Solr response has a
   *                                    non-zero status.
   */
  static <T extends SolrResponseBase> T handleSolrResponse(T solrResponse,
                                                       String errorMessage) throws SolrResponseErrorException {

    log.debug2("solrResponse = {}", solrResponse);

    NamedList<Object> solrResponseResponse = solrResponse.getResponse();

    // Check whether the response does indicate success.
    if (solrResponse.getStatus() == 0
        && solrResponseResponse.get("error") == null
        && solrResponseResponse.get("errors") == null) {
      // Yes: Return the successful response.
      log.debug2("solrResponse indicates success");
      return solrResponse;
    }

    // No: Report the problem.
    log.trace("solrResponse indicates failure");

    SolrResponseErrorException snzse =
        new SolrResponseErrorException(errorMessage, solrResponse);
    log.error(snzse);
    throw snzse;
  }

  @Override
  public void stop() {
    try {
      if (isInternalClient) {
        solrClient.close();
      }

      closeJournal();

      setState(ArtifactIndexState.STOPPED);
    } catch (IOException e) {
      log.error("Could not close Solr client connection", e);
    }
  }

  /**
   * Checks whether the Solr cluster is alive by calling {@code SolrClient#ping()}.
   *
   * @return
   */
  private boolean checkAlive() {
    try {
      // Create new SolrPing request and process it
      SolrPing ping = new SolrPing();
      addSolrCredentials(ping);
      SolrPingResponse response = ping.process(solrClient, solrCollection);

      // Check response for success
      handleSolrResponse(response, "Problem pinging Solr");
      return true;

    } catch (Exception e) {
      log.warn("Could not ping Solr", e);
      return false;
    }
  }

  /**
   * Returns a boolean indicating whether this artifact index is ready.
   *
   * @return a boolean with the indication.
   */
  @Override
  public boolean isReady() {
    return getState() == ArtifactIndexState.RUNNING && checkAlive();
  }

  @Override
  public void acquireVersionLock(ArtifactIdentifier.ArtifactStem stem) throws IOException {
    // Acquire the lock for this artifact stem
    try {
      versionLock.getLock(stem);
    } catch (InterruptedException e) {
      throw new InterruptedIOException("Interrupted while waiting to acquire artifact version lock");
    }

  }

  @Override
  public void releaseVersionLock(ArtifactIdentifier.ArtifactStem stem) {
    // Release the lock for the artifact stem
    versionLock.releaseLock(stem);
  }

  /**
   * Adds an artifact to the artifactIndex.
   *
   * @param artifact The {@link Artifact} to be added to this index.
   */
  @Override
  public void indexArtifact(Artifact artifact) throws IOException {
    if (artifact == null) {
      throw new IllegalArgumentException("Null artifact");
    }

    ArtifactIdentifier artifactId = artifact.getIdentifier();

    if (artifactId == null) {
      throw new IllegalArgumentException("Artifact has null identifier");
    }

    // Add the Artifact to Solr as a bean
    try {
      // Convert Artifact to SolrInputDocument using the SolrClient's DocumentObjectBinder
      SolrInputDocument doc = solrClient.getBinder()
          .toSolrInputDocument(ArtifactSolrDocument.fromArtifact(artifact));

      // Create an UpdateRequest to add the Solr input document
      UpdateRequest req = new UpdateRequest();
      req.add(doc);
      addSolrCredentials(req);

      handleSolrResponse(req.process(solrClient, solrCollection),
          "Problem adding artifact '" + artifact + "' to Solr");

      ObjectMapper objMap = new ObjectMapper();
      logSolrUpdate(SolrCommitJournal.SolrOperation.ADD,
          artifact.getUuid(), objMap.writeValueAsString(artifact));

      handleSolrResponse(handleSolrCommit(SolrCommitStrategy.SOFT), "Problem committing addition of "
          + "artifact '" + artifact + "' to Solr");

    } catch (SolrResponseErrorException | SolrServerException e) {
      throw new IOException(e);
    }

    // Return the Artifact added to the Solr collection
    log.debug2("Added artifact to index [uuid: {}]", artifactId.getUuid());
  }

  /**
   * Bulk index artifacts into Solr.
   *
   * @param artifacts An {@link Iterable<Artifact>} containing the {@link Artifact}s to index.
   */
  private final static long BATCH_SIZE = 1000;
  @Override
  public void indexArtifacts(Iterable<Artifact> artifacts) {
    DocumentObjectBinder objBinder = solrClient.getBinder();

    UpdateRequest req = new UpdateRequest();
    addSolrCredentials(req);
    long docsAdded = 0;

    Iterator<Artifact> ai = artifacts.iterator();
    if (!ai.hasNext()) {
      log.debug("No artifacts in AU to index");
      return;
    }

    boolean isFirstArtifact = true;

    while (ai.hasNext()) {
      Artifact artifact = ai.next();

      // This is ugly but we need the namespace and AUID of this batch of artifacts
      if (isFirstArtifact) {
        try {
          invalidateAuSize(artifact.getNamespace(), artifact.getAuid());
        } catch (DbException e) {
          // TODO
          log.warn("Could not invalidate AU size", e);
        }
        isFirstArtifact = false;
      }

      req.add(objBinder.toSolrInputDocument(ArtifactSolrDocument.fromArtifact(artifact)));
      docsAdded++;

      if (docsAdded % BATCH_SIZE == 0 || !ai.hasNext()) {
        // Process UpdateRequest batch
        try {
          log.debug("Storing batch");
          handleSolrResponse(req.process(solrClient, solrCollection), "Failed to add artifacts");

          if (ai.hasNext()) {
            log.debug("Soft committing batch");
            handleSolrResponse(handleSolrCommit(SolrCommitStrategy.SOFT_ONLY), "Failed to perform soft commit");
          }
        } catch (Exception e) {
          // TODO
          log.error("Failed to perform UpdateRequest", e);
        }

        // Reset for next batch of Artifacts
        req = new UpdateRequest();
        addSolrCredentials(req);
      }
    }

    try {
      log.debug("Hard committing batches");
      handleSolrResponse(handleSolrCommit(SolrCommitStrategy.HARD), "Failed to perform hard commit");
    } catch (Exception e) {
      // TODO
      log.error("Failed to perform hard commit", e);
    }

    log.debug("Total documents added = {}", docsAdded);
  }

  private void logSolrUpdate(SolrCommitJournal.SolrOperation op, String artifactUuid, String data) {
    for (int i = 0; i < 3; i++) {
      try {
        solrJournalWriter.logOperation(op, artifactUuid, data);
        return;
      } catch (IOException e) {
        try {
          Thread.sleep(1000L);
        } catch (InterruptedException ex) {
          break;
        }
      }
    }

    log.error("Could not log to Solr update journal [op: {}, artifactUuid: {}]", op , artifactUuid);
  }

  /**
   * Performs a Solr query against the configured Solr collection.
   *
   * @param q The {@link SolrQuery} to submit.
   * @return A {@link QueryResponse} with the response from the Solr server.
   * @throws IOException
   * @throws SolrServerException
   */
  private QueryResponse handleSolrQuery(SolrQuery q) throws IOException, SolrServerException {
    // Create a QueryRequest from the SolrQuery
    QueryRequest request = new QueryRequest(q);

    // Add Solr BasicAuth credentials if present
    addSolrCredentials(request);

    // Perform the query and return the response
    return request.process(solrClient, solrCollection);
  }

  /**
   * Provides the artifactIndex data of an artifact with a given text artifactIndex
   * identifier.
   *
   * @param artifactUuid A String with the artifact artifactIndex identifier.
   * @return an Artifact with the artifact indexing data.
   */
  @Override
  public Artifact getArtifact(String artifactUuid) throws IOException {
    if (StringUtils.isEmpty(artifactUuid)) {
      throw new IllegalArgumentException("Null or empty artifact UUID");
    }

    // Solr query to perform
    SolrQuery q = new SolrQuery();
    q.setQuery(String.format("id:%s", artifactUuid));
//        q.addFilterQuery(String.format("{!term f=id}%s", artifactUuid));
//        q.addFilterQuery(String.format("committed:%s", true));

    try {
      // Submit the Solr query and get results as Artifact objects
      QueryResponse response =
          handleSolrResponse(handleSolrQuery(q), "Problem performing Solr query");

      long numFound = response.getResults().getNumFound();

      if (numFound == 0) {
        return null;
      } else if (numFound == 1) {
        // Deserialize results into a list of Artifacts
        List<ArtifactSolrDocument> artifacts = response.getBeans(ArtifactSolrDocument.class);

        // Return the artifact
        return artifacts.get(0).toArtifact();
      }

      // This should never happen
      log.warn("Unexpected number of Solr documents in response: {}", numFound);
      throw new RuntimeException("Unexpected number of Solr documents in response");

    } catch (SolrResponseErrorException | SolrServerException e) {
      throw new IOException("Solr error", e);
    }
  }

  /**
   * Provides the artifactIndex data of an artifact with a given artifactIndex identifier
   * UUID.
   *
   * @param artifactUuid An UUID with the artifact artifactIndex identifier.
   * @return an Artifact with the artifact indexing data.
   */
  @Override
  public Artifact getArtifact(UUID artifactUuid) throws IOException {
    if (artifactUuid == null) {
      throw new IllegalArgumentException("Null UUID");
    }

    return this.getArtifact(artifactUuid.toString());
  }

  /**
   * Commits to the artifactIndex an artifact with a given text artifactIndex identifier.
   *
   * @param artifactUuid A String with the artifact artifactIndex identifier.
   * @return an Artifact with the committed artifact indexing data.
   */
  @Override
  public Artifact commitArtifact(String artifactUuid) throws IOException {
    if (!artifactExists(artifactUuid)) {
      log.debug("Artifact does not exist [uuid: {}]", artifactUuid);
      return null;
    }

    // Partial document to perform Solr document update
    SolrInputDocument document = new SolrInputDocument();
    document.addField("id", artifactUuid);

    // Perform an atomic update (see https://lucene.apache.org/solr/guide/6_6/updating-parts-of-documents.html) by
    // setting the type of field modification and its replacement value
    Map<String, Object> fieldModifier = new HashMap<>();
    fieldModifier.put("set", true);
    document.addField("committed", fieldModifier);

    try {
      // Create an update request for this document
      UpdateRequest request = new UpdateRequest();
      request.add(document);
      addSolrCredentials(request);

      // Update the artifact.
      handleSolrResponse(request.process(solrClient, solrCollection), "Problem adding document '"
          + document + "' to Solr");

      logSolrUpdate(SolrCommitJournal.SolrOperation.UPDATE_COMMITTED, artifactUuid, null);

      // Commit changes
      handleSolrResponse(handleSolrCommit(SolrCommitStrategy.SOFT), "Problem committing Solr changes");
    } catch (SolrResponseErrorException | SolrServerException e) {
      throw new IOException("Solr error", e);
    }

    // Return updated Artifact
    Artifact result = getArtifact(artifactUuid);
    try {
      invalidateAuSize(result.getNamespace(), result.getAuid());
    } catch (DbException e) {
      throw new IOException("Could not invalidate AU size", e);
    }

    return result;
  }

  public enum SolrCommitStrategy {
    SOFT_ONLY,
    SOFT,
    HARD
  }

  /**
   * Performs a commit on the Solr collection used by this artifact index.
   *
   * @return An {@link UpdateResponse} containing the response from the Solr server.
   * @throws IOException
   * @throws SolrServerException
   */
  UpdateResponse handleSolrCommit(SolrCommitStrategy strategy) throws IOException, SolrServerException {
    boolean softCommit = true;

    switch (strategy) {
      case SOFT_ONLY:
        softCommit = true;
        // hardCommitNeeded = false;
        break;

      case SOFT:
        softCommit = true;
        hardCommitNeeded = true;
        break;

      case HARD:
        softCommit = false;
        hardCommitNeeded = false;
        break;
    }

    // Update request to commit
    UpdateRequest req = new UpdateRequest();
    req.setAction(UpdateRequest.ACTION.COMMIT, true, true, softCommit);

    // Add Solr credentials if present
    addSolrCredentials(req);

    // Perform commit and return response
    return req.process(solrClient, solrCollection);
  }

  /**
   * Commits to the artifactIndex an artifact with a given artifactIndex identifier UUID.
   *
   * @param artifactUuid An UUID with the artifact artifactIndex identifier.
   * @return an Artifact with the committed artifact indexing data.
   */
  @Override
  public Artifact commitArtifact(UUID artifactUuid) throws IOException {
    if (artifactUuid == null) {
      throw new IllegalArgumentException("Null UUID");
    }

    return commitArtifact(artifactUuid.toString());
  }

  /**
   * Removes from the artifactIndex an artifact with a given text artifactIndex identifier.
   *
   * @param artifactUuid A String with the artifact artifactIndex identifier.
   * @throws IOException if Solr reports problems.
   */
  @Override
  public boolean deleteArtifact(String artifactUuid) throws IOException {
    if (StringUtils.isEmpty(artifactUuid)) {
      throw new IllegalArgumentException("Null or empty UUID");
    }

    Artifact artifact = getArtifact(artifactUuid);

    if (artifact != null) {
      try {
        // Invalidate AU size cache
        invalidateAuSize(artifact.getNamespace(), artifact.getAuid());

        // Create an Solr update request
        UpdateRequest request = new UpdateRequest();
        request.deleteById(artifactUuid);
        addSolrCredentials(request);

        // Remove Solr document for this artifact
        handleSolrResponse(request.process(solrClient, solrCollection), "Problem deleting "
            + "artifact '" + artifactUuid + "' from Solr");

        logSolrUpdate(SolrCommitJournal.SolrOperation.DELETE, artifactUuid, null);

        // Commit changes
        handleSolrResponse(handleSolrCommit(SolrCommitStrategy.SOFT), "Problem committing deletion of "
            + "artifact '" + artifactUuid + "' from Solr");

        // Return true to indicate success
        return true;
      } catch (DbException e) {
        throw new IOException("Could not invalidate AU size", e);
      } catch (SolrResponseErrorException | SolrServerException e) {
        log.error("Could not remove artifact from Solr index [uuid: {}]", artifactUuid, e);
        throw new IOException(
            String.format("Could not remove artifact from Solr index [uuid: %s]", artifactUuid), e
        );
      }
    } else {
      // Artifact not found in index; nothing deleted
      log.debug("Artifact not found [uuid: {}]", artifactUuid);
      return false;
    }
  }

  /**
   * Removes from the artifactIndex an artifact with a given artifactIndex identifier UUID.
   *
   * @param artifactUuid A String with the artifact UUID.
   * @return <code>true</code> if the artifact was removed from in the artifactIndex,
   * <code>false</code> otherwise.
   */
  @Override
  public boolean deleteArtifact(UUID artifactUuid) throws IOException {
    if (artifactUuid == null) {
      throw new IllegalArgumentException("Null UUID");
    }

    return deleteArtifact(artifactUuid.toString());
  }

  /**
   * Provides an indication of whether an artifact with a given text artifactIndex
   * identifier exists in the artifactIndex.
   *
   * @param artifactUuid A String with the artifact identifier.
   * @return <code>true</code> if the artifact exists in the artifactIndex,
   * <code>false</code> otherwise.
   */
  @Override
  public boolean artifactExists(String artifactUuid) throws IOException {
    return getArtifact(artifactUuid) != null;
  }

  @Override
  public Artifact updateStorageUrl(String artifactUuid, String storageUrl) throws IOException {
    if (StringUtils.isEmpty(artifactUuid)) {
      throw new IllegalArgumentException("Invalid artifact UUID");
    }

    if (StringUtils.isEmpty(storageUrl)) {
      throw new IllegalArgumentException("Invalid storage URL: Must not be null or empty");
    }

    // Perform a partial update of an existing Solr document
    SolrInputDocument document = new SolrInputDocument();
    document.addField("id", artifactUuid);

    // Specify type of field modification, field name, and replacement value
    Map<String, Object> fieldModifier = new HashMap<>();
    fieldModifier.put("set", storageUrl);
    document.addField("storageUrl", fieldModifier);

    try {
      // Create an update request for this document
      UpdateRequest request = new UpdateRequest();
      request.add(document);
      addSolrCredentials(request);

      // Update the field
      handleSolrResponse(request.process(solrClient, solrCollection), "Problem adding document '"
          + document + "' to Solr");

      logSolrUpdate(SolrCommitJournal.SolrOperation.UPDATE_STORAGEURL, artifactUuid, storageUrl);

      handleSolrResponse(handleSolrCommit(SolrCommitStrategy.SOFT), "Problem committing addition of "
          + "document '" + document + "' to Solr");
    } catch (SolrException | SolrResponseErrorException | SolrServerException e) {
      throw new IOException(e);
    }

    // Return updated Artifact
    return getArtifact(artifactUuid);
  }

  /**
   * Returns a boolean indicating whether the Solr index is empty.
   *
   * @return A boolean indicating whether the Solr index is empty.
   * @throws IOException
   * @throws SolrServerException
   */
  private boolean isEmptySolrIndex()
      throws SolrResponseErrorException, IOException, SolrServerException {

    // Match all documents but set the number of documents to be returned to zero
    SolrQuery q = new SolrQuery();
    q.setQuery("*:*");
    q.setRows(0);

    // Perform the query
    QueryResponse result =
        handleSolrResponse(handleSolrQuery(q), "Problem performing Solr query");

    // Check whether we matched zero Solr documents
    return result.getResults().getNumFound() == 0;
  }

  /**
   * Returns the namespaces of the committed artifacts in this Solr {@link ArtifactIndex} implementation.
   *
   * @return An {@code Iterator<String>} with the artifactIndex committed artifacts namespaces.
   */
  @Override
  public Iterable<String> getNamespaces() throws IOException {
    try {
      // Cannot perform facet field query on an empty Solr index
      if (isEmptySolrIndex()) {
        return IterableUtils.emptyIterable();
      }

      // Perform a Solr facet query on the namespace field
      SolrQuery q = new SolrQuery();
      q.setQuery("*:*");
      q.setRows(0);

      q.addFacetField("namespace");
      q.setFacetLimit(-1); // Unlimited

      // Get the facet field from the result
      QueryResponse result =
          handleSolrResponse(handleSolrQuery(q), "Problem performing Solr query");

      FacetField ff = result.getFacetField("namespace");

      if (log.isDebug2Enabled()) {
        log.debug2(
            "FacetField: [getName: {}, getValues: {}, getValuesCount: {}]",
            ff.getName(),
            ff.getValues(),
            ff.getValueCount()
        );
      }

      // Transform facet field value names into iterable
      return IteratorUtils.asIterable(
          ff.getValues().stream()
              .filter(x -> x.getCount() > 0)
              .map(FacetField.Count::getName)
              .sorted()
              .iterator()
      );

    } catch (SolrResponseErrorException | SolrServerException e) {
      throw new IOException(e);
    }
  }

  /**
   * Returns a list of Archival Unit IDs (AUIDs) in this namespace.
   *
   * @param namespace A {@code String} containing the namespace.
   * @return A {@code Iterator<String>} iterating over the AUIDs in this namespace.
   * @throws IOException if Solr reports problems.
   */
  @Override
  public Iterable<String> getAuIds(String namespace) throws IOException {
    // We use a Solr facet query but another option is Solr groups. I believe faceting is better in this case,
    // because we are not actually interested in the Solr documents - only aggregate information about them.
    SolrQuery q = new SolrQuery();

    q.setQuery("*:*");

    q.addFilterQuery(String.format("{!term f=namespace}%s", namespace));

    q.setRows(0);

    q.addFacetField("auid");
    q.setFacetLimit(-1); // Unlimited

    try {
      QueryResponse response =
          handleSolrResponse(handleSolrQuery(q), "Problem performing Solr query");

      FacetField ff = response.getFacetField("auid");

      return IteratorUtils.asIterable(
          ff.getValues().stream()
              .filter(x -> x.getCount() > 0)
              .map(FacetField.Count::getName)
              .sorted()
              .iterator()
      );
    } catch (SolrResponseErrorException | SolrServerException e) {
      throw new IOException(e);
    }
  }

  /**
   * Returns the committed artifacts of the latest version of all URLs, from a specified Archival Unit and namespace.
   *
   * @param namespace A {@code String} containing the namespace.
   * @param auid       A {@code String} containing the Archival Unit ID.
   * @return An {@code Iterator<Artifact>} containing the latest version of all URLs in an AU.
   * @throws IOException if Solr reports problems.
   */
  @Override
  public Iterable<Artifact> getArtifacts(String namespace, String auid, boolean includeUncommitted) throws IOException {
    // Create Solr query
    SolrQuery q = new SolrQuery();

    // Match everything
    q.setQuery("*:*");

    // Filter by committed status equal to true?
    if (!includeUncommitted) {
      q.addFilterQuery(String.format("committed:%s", true));
    }

    // Additional filter queries
    q.addFilterQuery(String.format("{!term f=namespace}%s", namespace));
    q.addFilterQuery(String.format("{!term f=auid}%s", auid));
    q.addSort(SORTURI_ASC);
    q.addSort(VERSION_DESC);

    // Ensure the result is not empty for the collapse filter query
    if (isEmptyResult(q)) {
      log.debug2("Solr returned null result set after filtering by [committed: true, namespace: {}, auid: {}]",
          namespace, auid);

      return IterableUtils.emptyIterable();
    }

    // Add a collapse filter
    q.addFilterQuery("{!collapse field=uri max=version}");

    // Perform collapse filter query and return result
    return IteratorUtils.asIterable(
        new SolrQueryArtifactIterator(solrCollection, solrClient, solrCredentials, q));
  }

  /**
   * Returns the artifacts of all versions of all URLs, from a specified Archival Unit and namespace.
   *
   * @param namespace         A String with the namespace.
   * @param auid               A String with the Archival Unit identifier.
   * @param includeUncommitted A {@code boolean} indicating whether to return all the versions among both committed and uncommitted
   *                           artifacts.
   * @return An {@code Iterator<Artifact>} containing the artifacts of all version of all URLs in an AU.
   */
  @Override
  public Iterable<Artifact> getArtifactsAllVersions(String namespace, String auid, boolean includeUncommitted) throws IOException {
    // Create a Solr query
    SolrQuery q = new SolrQuery();

    // Match everything
    q.setQuery("*:*");

    // Add filter by committed status equal to true if we do *not* want to include uncommitted
    if (!includeUncommitted) {
      q.addFilterQuery(String.format("committed:%s", true));
    }

    // Additional filter queries
    q.addFilterQuery(String.format("{!term f=namespace}%s", namespace));
    q.addFilterQuery(String.format("{!term f=auid}%s", auid));
    q.addSort(SORTURI_ASC);
    q.addSort(VERSION_DESC);

    return IteratorUtils.asIterable(
        new SolrQueryArtifactIterator(solrCollection, solrClient, solrCredentials, q));
  }

  /**
   * Returns the committed artifacts of the latest version of all URLs matching a prefix, from a specified Archival
   * Unit and namespace.
   *
   * @param namespace A {@code String} containing the namespace.
   * @param auid       A {@code String} containing the Archival Unit ID.
   * @param prefix     A {@code String} containing a URL prefix.
   * @return An {@code Iterator<Artifact>} containing the latest version of all URLs matching a prefix in an AU.
   * @throws IOException if Solr reports problems.
   */
  @Override
  public Iterable<Artifact> getArtifactsWithPrefix(String namespace, String auid, String prefix) throws IOException {

    SolrQuery q = new SolrQuery();

    q.setQuery("*:*");

    q.addFilterQuery(String.format("committed:%s", true));
    q.addFilterQuery(String.format("{!term f=namespace}%s", namespace));
    q.addFilterQuery(String.format("{!term f=auid}%s", auid));
    q.addFilterQuery(String.format("{!prefix f=uri}%s", prefix));

    q.addSort(SORTURI_ASC);
    q.addSort(VERSION_DESC);

    // Ensure the result is not empty for the collapse filter query
    if (isEmptyResult(q)) {
      log.debug2(
          "Solr returned null result set after filtering by (committed: true, namespace: {}, auid:{}, uri (prefix): {})",
          namespace, auid, prefix);

      return IterableUtils.emptyIterable();
    }

    // Add collapse filter query
    q.addFilterQuery("{!collapse field=uri max=version}");

    // Perform collapse filter query and return result
    return IteratorUtils.asIterable(
        new SolrQueryArtifactIterator(solrCollection, solrClient, solrCredentials, q));
  }

  /**
   * Returns the committed artifacts of all versions of all URLs matching a prefix, from a specified Archival Unit and
   * namespace.
   *
   * @param namespace A String with the namespace.
   * @param auid       A String with the Archival Unit identifier.
   * @param prefix     A String with the URL prefix.
   * @return An {@code Iterator<Artifact>} containing the committed artifacts of all versions of all URLs matching a
   * prefix from an AU.
   */
  @Override
  public Iterable<Artifact> getArtifactsWithPrefixAllVersions(String namespace, String auid, String prefix) throws IOException {

    SolrQuery q = new SolrQuery();

    q.setQuery("*:*");

    q.addFilterQuery(String.format("committed:%s", true));
    q.addFilterQuery(String.format("{!term f=namespace}%s", namespace));
    q.addFilterQuery(String.format("{!term f=auid}%s", auid));
    q.addFilterQuery(String.format("{!prefix f=uri}%s", prefix));

    q.addSort(SORTURI_ASC);
    q.addSort(VERSION_DESC);

    return IteratorUtils.asIterable(
        new SolrQueryArtifactIterator(solrCollection, solrClient, solrCredentials, q));
  }

  /**
   * Returns the committed artifacts of all versions of all URLs matching a prefix, from a specified namespace.
   *
   * @param namespace A String with the namespace.
   * @param urlPrefix     A String with the URL prefix.
   * @param versions   A {@link ArtifactVersions} indicating whether to include all versions or only the latest
   *                   versions of an artifact.
   * @return An {@code Iterator<Artifact>} containing the committed artifacts of all versions of all URLs matching a
   * prefix.
   */
  @Override
  public Iterable<Artifact> getArtifactsWithUrlPrefixFromAllAus(String namespace, String urlPrefix,
                                                                ArtifactVersions versions) throws IOException {

    if (!(versions == ArtifactVersions.ALL ||
        versions == ArtifactVersions.LATEST)) {
      throw new IllegalArgumentException("Versions must be ALL or LATEST");
    }

    if (namespace == null) {
      throw new IllegalArgumentException("Namespace is null");
    }

    SolrQuery q = new SolrQuery();

    q.setQuery("*:*");

    q.addFilterQuery(String.format("committed:%s", true));
    q.addFilterQuery(String.format("{!term f=namespace}%s", namespace));

    if (urlPrefix != null) {
      q.addFilterQuery(String.format("{!prefix f=uri}%s", urlPrefix));
    }

//    // This gives us the right results but does not work with CursorMark-based
//    // pagination despite group.main=true. It is left here as a reminder that
//    // we already tried this approach.
//    q.set("group", true);
//    q.set("group.func", "concat(namespace,auid,uri)");
//    q.set("group.sort", "version desc");
//    q.set("group.limit", 1);
//    q.set("group.main", true);

    q.addSort(SORTURI_ASC);
    q.addSort(AUID_ASC);
    q.addSort(VERSION_DESC);

    Iterator<Artifact> allVersionsIterator =
        new SolrQueryArtifactIterator(solrCollection, solrClient, solrCredentials, q);

    if (versions == ArtifactVersions.LATEST) {
      // Convert Iterator<Artifact> to Stream<Artifact>
      Stream<Artifact> allVersions = StreamSupport.stream(
          Spliterators.spliteratorUnknownSize(allVersionsIterator, Spliterator.ORDERED), false);

      // Group by (Namespace, AUID, URL) then pick highest version from each group
      Stream<Artifact> latestVersions = allVersions
          .collect(Collectors.groupingBy(
              artifact -> artifact.getIdentifier().getArtifactStem(),
              Collectors.maxBy(Comparator.comparingInt(Artifact::getVersion))))
          .values()
          .stream()
          .filter(Optional::isPresent)
          .map(Optional::get);

      // Sort artifacts and return as Iterable<Artifact>
      return IteratorUtils.asIterable(latestVersions
          .sorted(ArtifactComparators.BY_URI_BY_AUID_BY_DECREASING_VERSION)
          .iterator());
    }

    return IteratorUtils.asIterable(allVersionsIterator);
  }

  /**
   * Returns the committed artifacts of all versions of a given URL, from a specified Archival Unit and namespace.
   *
   * @param namespace A {@code String} with the namespace.
   * @param auid       A {@code String} with the Archival Unit identifier.
   * @param url        A {@code String} with the URL to be matched.
   * @return An {@code Iterator<Artifact>} containing the committed artifacts of all versions of a given URL from an
   * Archival Unit.
   */
  @Override
  public Iterable<Artifact> getArtifactsAllVersions(String namespace, String auid, String url) throws IOException {
    SolrQuery q = new SolrQuery();

    q.setQuery("*:*");

    q.addFilterQuery(String.format("committed:%s", true));
    q.addFilterQuery(String.format("{!term f=namespace}%s", namespace));
    q.addFilterQuery(String.format("{!term f=auid}%s", auid));
    q.addFilterQuery(String.format("{!term f=uri}%s", url));

    q.addSort(SORTURI_ASC);
    q.addSort(VERSION_DESC);

    return IteratorUtils.asIterable(
        new SolrQueryArtifactIterator(solrCollection, solrClient, solrCredentials, q));
  }

  /**
   * Returns the committed artifacts of all versions of a given URL, from a specified namespace.
   *
   * @param namespace A {@code String} with the namespace.
   * @param url        A {@code String} with the URL to be matched.
   * @param versions   A {@link ArtifactVersions} indicating whether to include all versions or only the latest
   *                   versions of an artifact.
   * @return An {@code Iterator<Artifact>} containing the committed artifacts of all versions of a given URL.
   */
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

    SolrQuery q = new SolrQuery();

    q.setQuery("*:*");

    q.addFilterQuery(String.format("committed:%s", true));
    q.addFilterQuery(String.format("{!term f=namespace}%s", namespace));
    q.addFilterQuery(String.format("{!term f=uri}%s", url));

//    // This gives us the right results but does not work with CursorMark-based
//    // pagination despite group.main=true. It is left here as a reminder that
//    // we already tried this approach.
//    q.set("group", true);
//    q.set("group.func", "concat(namespace,auid,uri)");
//    q.set("group.sort", "version desc");
//    q.set("group.limit", 1);
//    q.set("group.main", true);

    q.addSort(SORTURI_ASC);
    q.addSort(AUID_ASC);
    q.addSort(VERSION_DESC);

    Iterator<Artifact> allVersionsIterator =
        new SolrQueryArtifactIterator(solrCollection, solrClient, solrCredentials, q);

    if (versions == ArtifactVersions.LATEST) {
      // Convert Iterator<Artifact> to Stream<Artifact>
      Stream<Artifact> allVersions = StreamSupport.stream(
          Spliterators.spliteratorUnknownSize(allVersionsIterator, Spliterator.ORDERED), false);

      // Group by (Namespace, AUID, URL) then pick highest version from each group
      Stream<Artifact> latestVersions = allVersions
          .collect(Collectors.groupingBy(
              artifact -> artifact.getIdentifier().getArtifactStem(),
              Collectors.maxBy(Comparator.comparingInt(Artifact::getVersion))))
          .values()
          .stream()
          .filter(Optional::isPresent)
          .map(Optional::get);

      // Sort artifacts and return as Iterable<Artifact>
      return IteratorUtils.asIterable(latestVersions
          .sorted(ArtifactComparators.BY_URI_BY_AUID_BY_DECREASING_VERSION)
          .iterator());
    }

    return IteratorUtils.asIterable(allVersionsIterator);
  }

  /**
   * Returns the artifact of the latest version of given URL, from a specified Archival Unit and namespace.
   *
   * @param namespace         A {@code String} containing the namespace.
   * @param auid               A {@code String} containing the Archival Unit ID.
   * @param url                A {@code String} containing a URL.
   * @param includeUncommitted A {@code boolean} indicating whether to return the latest version among both committed and uncommitted
   *                           artifacts of a URL.
   * @return An {@code Artifact} representing the latest version of the URL in the AU.
   * @throws IOException if Solr reports problems.
   */
  @Override
  public Artifact getArtifact(String namespace, String auid, String url, boolean includeUncommitted) throws IOException {
    // Solr query to perform
    SolrQuery q = new SolrQuery();
    q.setQuery("*:*");

    if (!includeUncommitted) {
      // Restrict to only committed artifacts
      q.addFilterQuery(String.format("committed:%s", true));
    }

    q.addFilterQuery(String.format("{!term f=namespace}%s", namespace));
    q.addFilterQuery(String.format("{!term f=auid}%s", auid));
    q.addFilterQuery(String.format("{!term f=uri}%s", url));

    // Ensure the result is not empty for the collapse filter query
    if (isEmptyResult(q)) {
      log.debug2(
          "Solr returned null result set after filtering by [namespace: {}, auid: {}, uri: {}]",
          namespace, auid, url);

      return null;
    }

    // FIXME: Is there potential for a race condition between
    //  isEmptyResult() call and performing the collapse filter query?

    // Perform a collapse filter query (must have documents in result set to operate on)
    q.addFilterQuery("{!collapse field=uri max=version}");

    // Get results as an Iterator
    Iterator<Artifact> result =
        new SolrQueryArtifactIterator(solrCollection, solrClient, solrCredentials, q);

    // Return the latest artifact
    if (result.hasNext()) {
      Artifact artifact = result.next();

      if (result.hasNext()) {
        // This should never happen if Solr is working correctly
        String errMsg = "More than one artifact returned for the latest version of (Namespace, AUID, URL)!";
        log.error(errMsg);
        throw new RuntimeException(errMsg);
      }

      return artifact;
    }

    return null;
  }

  /**
   * Returns the artifact of a given version of a URL, from a specified Archival Unit and namespace.
   *
   * @param namespace         A String with the namespace.
   * @param auid               A String with the Archival Unit identifier.
   * @param url                A String with the URL to be matched.
   * @param version            A String with the version.
   * @param includeUncommitted A boolean with the indication of whether an uncommitted artifact may be returned.
   * @return The {@code Artifact} of a given version of a URL, from a specified AU and namespace.
   */
  @Override
  public Artifact getArtifactVersion(String namespace, String auid, String url, Integer version, boolean includeUncommitted) throws IOException {
    SolrQuery q = new SolrQuery();

    q.setQuery("*:*");

    // Only filter by commit status when no uncommitted artifact is to be returned.
    if (!includeUncommitted) {
      q.addFilterQuery(String.format("committed:%s", true));
    }

    q.addFilterQuery(String.format("{!term f=namespace}%s", namespace));
    q.addFilterQuery(String.format("{!term f=auid}%s", auid));
    q.addFilterQuery(String.format("{!term f=uri}%s", url));
    q.addFilterQuery(String.format("version:%s", version));

    Iterator<Artifact> result = new SolrQueryArtifactIterator(solrCollection, solrClient, solrCredentials, q);

    if (result.hasNext()) {
      Artifact artifact = result.next();

      if (result.hasNext()) {
        log.warn("More than one artifact found having same (Namespace, AUID, URL, Version)");
      }

      return artifact;
    }

    return null;
  }

  /**
   * Returns the size, in bytes, of AU in a namepsace.
   *
   * @param namespace A {@code String} containing the namespace.
   * @param auid       A {@code String} containing the Archival Unit ID.
   * @return A {@link AuSize} with byte size statistics of the specified AU.
   */
  // FIXME: Is there potential for a race condition here between checking
  //  the results of the query and performing the collapse filter?
  @Override
  public AuSize auSize(String namespace, String auid) throws IOException {
    try {
      AuSize result = findAuSize(namespace, auid);

      if (result == null) {
        Future<AuSize> ausFuture = getAuSizeFuture(namespace, auid);
        result = ausFuture.get();
      }

      return result;
    } catch (DbException e) {
      throw new IOException("Could not query AU size from database", e);
    } catch (ExecutionException e) {
      log.error("Could not recompute AU size", e.getCause());
      throw (IOException) e.getCause();
    } catch (InterruptedException e) {
      log.error("Interrupted while waiting for AU size recalculation", e);
      throw new IOException("AU size recalculation was interrupted", e);
    }
  }

  private Future<AuSize> getAuSizeFuture(String namespace, String auid) {
    String nsAuid = NamespacedAuid.key(namespace, auid);
    CompletableFuture<AuSize> ausFuture;

    synchronized (auSizeFutures) {
      ausFuture = auSizeFutures.get(nsAuid);
      if (ausFuture != null) {
        return ausFuture;
      } else {
        ausFuture = new CompletableFuture<>();
        auSizeFutures.put(nsAuid, ausFuture);
      }
    }

    try {
      AuSize result = computeAuSize(namespace, auid);
      updateAuSize(namespace, auid, result);
      ausFuture.complete(result);
    } catch (DbException e) {
      log.error("Couldn't update AU size in database", e);
      ausFuture.completeExceptionally(new IOException("Couldn't update AU size in database", e));
    } catch (IOException e) {
      log.error("Couldn't compute AU size", e);
      ausFuture.completeExceptionally(e);
    } finally {
      synchronized (auSizeFutures) {
        auSizeFutures.remove(nsAuid);
      }
    }

    return ausFuture;
  }

  private AuSize computeAuSize(String namespace, String auid) throws IOException {
    log.debug("Starting AU size recalculation [ns: {}, auid: {}]", namespace, auid);

    // Create Solr query
    SolrQuery q = new SolrQuery();
    q.setQuery("*:*");
    q.addFilterQuery(String.format("committed:%s", true));
    q.addFilterQuery(String.format("{!term f=namespace}%s", namespace));
    q.addFilterQuery(String.format("{!term f=auid}%s", auid));

    AuSize result = new AuSize();

    result.setTotalAllVersions(0L);
    result.setTotalLatestVersions(0L);
    // result.setTotalWarcSize(null);

    // Ensure the result is non-empty for the collapse filter query next
    if (isEmptyResult(q)) {
      // YES: No artifacts in AU (i.e., AU doesn't exist)
      result.setTotalWarcSize(0L);
      return result;
    }

    // Disk size calculation
    long totalWarcSize = repository.getArtifactDataStore()
        .auWarcSize(namespace, auid);

    result.setTotalWarcSize(totalWarcSize);

    // Setup the collapse filter query
    q.setGetFieldStatistics(true);
    q.setGetFieldStatistics("contentLength");
    q.setRows(0);

    try {
      //// Perform query for size of all artifact versions
      QueryResponse response1 =
          handleSolrResponse(handleSolrQuery(q), "Problem performing Solr query");

      // Get the contentLength from field statistics
      FieldStatsInfo fieldStats1 = response1.getFieldStatsInfo().get("contentLength");

      // Sum the contentLengths and return
      result.setTotalAllVersions(((Double)fieldStats1.getSum()).longValue());

      //// Perform query for size of latest artifact versions
      q.addFilterQuery("{!collapse field=uri max=version}");

      QueryResponse response2 =
          handleSolrResponse(handleSolrQuery(q), "Problem performing Solr query");

      // Get the contentLength from field statistics
      FieldStatsInfo fieldStats2 = response2.getFieldStatsInfo().get("contentLength");

      // Sum the contentLengths and return
      result.setTotalLatestVersions(((Double)fieldStats2.getSum()).longValue());
    } catch (SolrResponseErrorException | SolrServerException e) {
      throw new IOException("Solr request error", e);
    }

    log.debug("Finished AU size recalculation [ns: {}, auid: {}]", namespace, auid);
    return result;
  }

  private boolean isEmptyResult(SolrQuery q) throws IOException {
    try {
      // Set the number of matched rows to return to zero
      q.setRows(0);

      // Perform query and find the number of documents
      QueryResponse response =
          handleSolrResponse(handleSolrQuery(q), "Problem performing Solr query");

      // Check that the query matched nothing
      return response.getResults().getNumFound() == 0;
    } catch (SolrResponseErrorException | SolrServerException e) {
      throw new IOException(String.format("Caught SolrServerException attempting to execute Solr query: %s", q));
    }
  }

  /**
   * Provides a connection to the database.
   *
   * @return a Connection with the connection to the database.
   * @throws DbException
   *           if any problem occurred accessing the database.
   */
  public Connection getConnection() throws DbException {
    return getRepositoryManagerSql().getConnection();
  }

  /**
   * Provides the repository manager SQL executor.
   *
   * @return a RepositoryManagerSql with the repository manager SQL executor.
   * @throws DbException
   *           if any problem occurred accessing the database.
   */
  private RepositoryManagerSql getRepositoryManagerSql() throws DbException {
    if (repositoryManagerSql == null) {
      if (theApp == null) {
        repositoryManagerSql = new RepositoryManagerSql(
            LockssApp.getManagerByTypeStatic(RepositoryDbManager.class));
      } else {
        repositoryManagerSql = new RepositoryManagerSql(
            theApp.getManagerByType(RepositoryDbManager.class));
      }
    }

    return repositoryManagerSql;
  }

  void setLockssApp(LockssApp lockssApp) {
    this.theApp = lockssApp;
  }

  /**
   * Provides the AuSize associated with the AUID.
   *
   * @param auid
   *          A String with the AUID under which the AuSize is stored.
   * @return a AuSize, or null if not present in the store.
   * @throws DbException
   *           if any problem occurred accessing the data.
   * @throws IOException
   *           if any problem occurred accessing the data.
   */
  public AuSize findAuSize(String namespace, String auid) throws DbException {
    return getRepositoryManagerSql()
        .findAuSize(NamespacedAuid.key(namespace, auid));
  }

  /**
   * Update the AuSize associated with the AUID.
   *
   * @param auid
   *          A String with the AUID under which the AuSize is stored.
   * @param auSize
   *          A AuSize containing sizes statistics for the AU.
   * @return The internal AUID sequence number of the AUID.
   * @throws DbException
   *           if any problem occurred accessing the data.
   */
  public Long updateAuSize(String namespace, String auid, AuSize auSize) throws DbException {
    String nsAuid = NamespacedAuid.key(namespace, auid);

    Long result = getRepositoryManagerSql().updateAuSize(nsAuid, auSize);
    invalidatedAuSizes.remove(nsAuid);
    return result;
  }


  public void invalidateAuSize(String namespace, String auid) throws DbException {
    String nsAuid = NamespacedAuid.key(namespace, auid);

    if (!invalidatedAuSizes.getOrDefault(nsAuid, false)) {
      invalidatedAuSizes.put(nsAuid, true);
      log.debug2("Invalidating AU size [ns: {}, auid: {}]", namespace, auid);
      getRepositoryManagerSql().deleteAuSize(nsAuid);
    }
  }
}
