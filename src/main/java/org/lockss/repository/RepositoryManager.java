/*

Copyright (c) 2000-2020 Board of Trustees of Leland Stanford Jr. University,
all rights reserved.

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in
all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.  IN NO EVENT SHALL
STANFORD UNIVERSITY BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY,
WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR
IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.

Except as contained in this notice, the name of Stanford University shall not
be used in advertising or otherwise to promote the sale, use or other dealings
in this Software without prior written authorization from Stanford University.

*/

package org.lockss.repository;

import org.apache.commons.collections4.IteratorUtils;
import org.apache.commons.collections4.map.LinkedMap;
import org.lockss.app.BaseLockssDaemonManager;
import org.lockss.app.ConfigurableManager;
import org.lockss.app.LockssDaemon;
import org.lockss.app.ServiceDescr;
import org.lockss.config.ConfigManager;
import org.lockss.config.Configuration;
import org.lockss.daemon.RestServicesManager;
import org.lockss.jms.JMSManager;
import org.lockss.log.L4JLogger;
import org.lockss.plugin.ArchivalUnit;
import org.lockss.plugin.AuEvent;
import org.lockss.plugin.AuEventHandler;
import org.lockss.rs.LockssRepositoryFactory;
import org.lockss.util.Constants;
import org.lockss.util.ListUtil;
import org.lockss.util.StringUtil;
import org.lockss.util.os.PlatformUtil;
import org.lockss.util.rest.repo.LockssRepository;
import org.lockss.util.rest.repo.RestLockssRepository;
import org.lockss.util.rest.repo.model.Artifact;
import org.lockss.util.rest.repo.model.ArtifactVersions;
import org.lockss.util.rest.repo.model.RepositoryInfo;
import org.lockss.util.rest.repo.util.ArtifactCache;
import org.lockss.util.storage.StorageInfo;
import org.lockss.util.time.TimeBase;

import java.io.File;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.*;
import java.util.regex.Pattern;

/**
 * RepositoryManager is the center of the per AU repositories.  It manages
 * the repository config parameters.
 */
public class RepositoryManager
    extends BaseLockssDaemonManager implements ConfigurableManager {

  private static L4JLogger log = L4JLogger.getLogger();

  public static final String PREFIX = Configuration.PREFIX + "repository.";

  /** @see #PARAM_CHECK_UNNORMALIZED */
  public enum CheckUnnormalizedMode {No, Log, Fix};

  /** Check for existing nodes with unnormalized names (created by very old
   * daemon that didn't normalize): None, Log, Fix */
  public static final String PARAM_CHECK_UNNORMALIZED =
      PREFIX + "checkUnnormalized";
  public static final CheckUnnormalizedMode DEFAULT_CHECK_UNNORMALIZED =
      CheckUnnormalizedMode.Log;

  /** Temporary specification of (new) LockssRepository for all AU storage.
   * <ul><li>volatile:<i>namespace</i> - use a volatile LockssRepository</li>
   * <li>local:<i>namespace</i>:<i>path</i> - use a local LockssRepository
   * at <i>path</i></li>
   * <li>rest:<i>namespace</i>:<i>url</i> - use a remote LockssRepository
   * at <i>url</i></li></ul>
   */
  public static final String PARAM_V2_REPOSITORY =
      PREFIX + "v2Repository";
  public static final String DEFAULT_V2_REPOSITORY = "volatile:baz";

  /** If true, instruct RestLockssRepository to cache Artifacts on the
   * client side to reduce REST transactions */
  public static final String PARAM_ENABLE_ARTIFACT_CACHE =
    PREFIX + "artifactCache.enable";
  public static final boolean DEFAULT_ENABLE_ARTIFACT_CACHE = true;

  /** Maximum size of Artifact cache */
  public static final String PARAM_ARTIFACT_CACHE_MAX =
    PREFIX + "artifactCache.maxSize";
  public static final int DEFAULT_ARTIFACT_CACHE_MAX = 500;

  /** Maximum size of ArtifactData cache */
  public static final String PARAM_ARTIFACT_DATA_CACHE_MAX =
    PREFIX + "artifactDataCache.maxSize";
  public static final int DEFAULT_ARTIFACT_DATA_CACHE_MAX = 20;

  /** If true, the ArtifactCache will be instrumented, at some performance
   * cost */
  public static final String PARAM_ARTIFACT_CACHE_INSTRUMENT =
    PREFIX + "artifactCache.instrument";
  public static final boolean DEFAULT_ARTIFACT_CACHE_INSTRUMENT = true;

  public static final String PARAM_PERSIST_INDEX_NAME =
      PREFIX + "persistIndexName";
  public static final String DEFAULT_PERSIST_INDEX_NAME = "artifact-index.ser";

  /** Maximum age of AUID -> repo map */
  public static final String PARAM_AUID_REPO_MAP_AGE =
    PREFIX + "auidRepoMapAge";
  public static final long DEFAULT_AUID_REPO_MAP_AGE = Constants.HOUR;


  static final String DISK_PREFIX = PREFIX + "diskSpace.";

  static final String PARAM_DISK_WARN_FRRE_MB = DISK_PREFIX + "warn.freeMB";
  static final int DEFAULT_DISK_WARN_FRRE_MB = 5000;
  static final String PARAM_DISK_FULL_FRRE_MB = DISK_PREFIX + "full.freeMB";
  static final int DEFAULT_DISK_FULL_FRRE_MB = 100;
  static final String PARAM_DISK_WARN_FRRE_PERCENT =
      DISK_PREFIX + "warn.freePercent";
  static final double DEFAULT_DISK_WARN_FRRE_PERCENT = .02;
  static final String PARAM_DISK_FULL_FRRE_PERCENT =
      DISK_PREFIX + "full.freePercent";
  static final double DEFAULT_DISK_FULL_FRRE_PERCENT = .01;

  private PlatformUtil platInfo = PlatformUtil.getInstance();
  private static CheckUnnormalizedMode checkUnnormalized =
      DEFAULT_CHECK_UNNORMALIZED;
  private long auidRepoMapAge = DEFAULT_AUID_REPO_MAP_AGE;

  private RepoSpec v2Repo = null;

  private AuEventHandler auEventHandler;

  PlatformUtil.DF paramDFWarn =
      PlatformUtil.DF.makeThreshold(DEFAULT_DISK_WARN_FRRE_MB,
          DEFAULT_DISK_WARN_FRRE_PERCENT);
  PlatformUtil.DF paramDFFull =
      PlatformUtil.DF.makeThreshold(DEFAULT_DISK_FULL_FRRE_MB,
          DEFAULT_DISK_FULL_FRRE_PERCENT);

  public void startService() {
    super.startService();
    LockssRepositoryStatus.registerAccessors(getDaemon(),
					     getDaemon().getStatusService());
    // register our AU event handler
    auEventHandler = new AuEventHandler.Base() {
      @Override
      public void auCreated(AuEvent event, ArchivalUnit au) {
	flushAuidRepoMap();
      }
    };
    getDaemon().getPluginManager().registerAuEventHandler(auEventHandler);
  }

  public void stopService() {
    if (auEventHandler != null) {
      getDaemon().getPluginManager().unregisterAuEventHandler(auEventHandler);
      auEventHandler = null;
    }
    LockssRepositoryStatus.unregisterAccessors(getDaemon().getStatusService());
    super.stopService();
  }

  public void setConfig(Configuration config, Configuration oldConfig,
      Configuration.Differences changedKeys) {
    if (changedKeys.contains(DISK_PREFIX)) {
      int minMB = config.getInt(PARAM_DISK_WARN_FRRE_MB,
          DEFAULT_DISK_WARN_FRRE_MB);
      double minPer = config.getPercentage(PARAM_DISK_WARN_FRRE_PERCENT,
          DEFAULT_DISK_WARN_FRRE_PERCENT);
      paramDFWarn = PlatformUtil.DF.makeThreshold(minMB, minPer);
      minMB = config.getInt(PARAM_DISK_FULL_FRRE_MB,
          DEFAULT_DISK_FULL_FRRE_MB);
      minPer = config.getPercentage(PARAM_DISK_FULL_FRRE_PERCENT,
          DEFAULT_DISK_FULL_FRRE_PERCENT);
      paramDFFull = PlatformUtil.DF.makeThreshold(minMB, minPer);
    }
    if (changedKeys.contains(PREFIX)) {
      checkUnnormalized =
          (CheckUnnormalizedMode)
              config.getEnum(CheckUnnormalizedMode.class,
                  PARAM_CHECK_UNNORMALIZED, DEFAULT_CHECK_UNNORMALIZED);
      auidRepoMapAge = config.getTimeInterval(PARAM_AUID_REPO_MAP_AGE,
					      DEFAULT_AUID_REPO_MAP_AGE);
      processV2RepoSpec(config.get(PARAM_V2_REPOSITORY, DEFAULT_V2_REPOSITORY));
      reconfigureRepos(config);
    }
  }

  static Pattern REPO_SPEC_PATTERN =
    Pattern.compile("([^:]+):([^:]+)(?::(.*$))?");

  private void processV2RepoSpec(String spec) {
    if (!StringUtil.isNullString(System.getProperty("oldrepo"))) {
      return;
    }
    if (!StringUtil.isNullString(spec)) {
      // currently set this only once
      if (!repoSpecMap.containsKey(spec)) {
	try {
	  RepoSpec rs = RepoSpec.fromSpec(spec);
	  rs.setRepository(createLockssRepository(rs));
	  setV2Repo(rs);
	} catch (Exception e) {
	  log.fatal("Can't create V2 repo", e);
	}
      }
    } else {
      repoSpecMap.remove(spec);
      v2Repo = null;
    }
  }

  private void setV2Repo(RepoSpec rs) {
    repoSpecMap.put(rs.getSpec(), rs);
    v2Repo = rs;
  }

  public static boolean isV2Repo() {
    RepoSpec rs =
      LockssDaemon.getLockssDaemon().getRepositoryManager().getV2Repository();
    return rs != null && rs.getRepository() != null;
  }

  Map<String,RepoSpec> repoSpecMap = new HashMap<>();

  /** Temporary until multiple repos */
  public RepoSpec getV2Repository() {
    return v2Repo;
  }

  public RepoSpec getV2Repository(String spec) {
    return repoSpecMap.get(spec);
  }

  /** Return list of known repository names.  Needs a registration
   * mechanism if ever another repository implementation. */
  public Collection<RepoSpec> getV2RepositoryList() {
    return repoSpecMap.values();
  }

  /** Return the repository containing the specified AU. */
  public RepoSpec findAuRepository(ArchivalUnit au) {
    return findAuRepository(au.getAuId());
  }

  /** Return the repository containing the specified auid. */
  // XXX MULTIREPO
  public RepoSpec findAuRepository(String auid) {
    return getV2Repository();
  }

  /** Return true if the repository on which the specified AU resides is
   * ready
   * @param auid
   * @return true if the repo is ready, false if not
   */
  public boolean isRepoReady(String auid) {
    return isRepoReady(findAuRepository(auid));
  }

  protected boolean isRepoReady(RepoSpec spec) {
    switch (spec.getType()) {
    case "rest":
      // XXX MULTIREPO needs to map spec to binding
      RestServicesManager svcsMgr =
	getApp().getManagerByType(RestServicesManager.class);
      if (svcsMgr != null) {
	RestServicesManager.ServiceStatus stat =
	  svcsMgr.getServiceStatus(getApp().getServiceBinding(ServiceDescr.SVC_REPO));
	if (stat != null) {
	  return stat.isReady();
	}
      }
      return false;
    case "volatile":
    case "local":
      return true;
    default:
      log.warn("Unknown repository type: " + spec.getType());
      return true;
    }
  }

  /** Create a LockssRepository instance according to the spec */
  LockssRepository createLockssRepository(RepoSpec spec) {
    Configuration config = ConfigManager.getCurrentConfig();
    switch (spec.getType()) {
    case "volatile":
      try {
	return LockssRepositoryFactory.createVolatileRepository();
      } catch (IOException e) {
	String msg = "Error creating volatile repository";
	throw new IllegalArgumentException(msg, e);
      }
    case "local":
      File file = new File(spec.getPath());
      String persistedIndexName =
	config.get(PARAM_PERSIST_INDEX_NAME, DEFAULT_PERSIST_INDEX_NAME);
      try {
	return
	  LockssRepositoryFactory.createLocalRepository(file,
							persistedIndexName);
      } catch (IOException e) {
	String msg = "Illegal V2 repository path: " + spec.getPath() +
	  ", persistedIndexName: " + persistedIndexName;
	throw new IllegalArgumentException(msg, e);
      }
    case "rest":
      try {
	URL url = new URL(spec.getPath());
	String serviceUser = null;
	String servicePassword = null;

	// Get the REST client credentials.
	List<String> restClientCredentials =
	    getDaemon().getRestClientCredentials();
        log.trace("restClientCredentials = {}", restClientCredentials);

	// Check whether there is a user name.
	if (restClientCredentials != null && restClientCredentials.size() > 0) {
	  // Yes: Get the user name.
	  serviceUser = restClientCredentials.get(0);
	  log.trace("serviceUser = {}", serviceUser);

	  // Check whether there is a user password.
	  if (restClientCredentials.size() > 1) {
	    // Yes: Get the user password.
	    servicePassword = restClientCredentials.get(1);
	  }
	}

	RestLockssRepository repo = LockssRepositoryFactory
	    .createRestLockssRepository(url, serviceUser, servicePassword);
	configureArtifactCache(repo, config);
	return repo;
      } catch (MalformedURLException e) {
	String msg = "Illegal V2 repository URL: " + spec.getPath() +
	  ": " + e.getMessage();
	throw new IllegalArgumentException(msg);
      } catch (IOException e) {
        throw new IllegalStateException("Could not create REST client", e);
      }
    default:
      throw new IllegalStateException("Unknown type: " + spec.getType());
    }
  }

  private void reconfigureRepos(Configuration config) {
    for (RepoSpec rs : getV2RepositoryList()) {
      if (rs.getRepository() instanceof RestLockssRepository) {
	configureArtifactCache((RestLockssRepository)rs.getRepository(),
			       config);
      }
    }
  }

  private void configureArtifactCache(RestLockssRepository repo,
				      Configuration config) {
    if (config.getBoolean(PARAM_ENABLE_ARTIFACT_CACHE,
			  DEFAULT_ENABLE_ARTIFACT_CACHE)) {
      ArtifactCache artCache = repo.getArtifactCache();
      artCache.setMaxSize(config.getInt(PARAM_ARTIFACT_CACHE_MAX,
					DEFAULT_ARTIFACT_CACHE_MAX),
			  config.getInt(PARAM_ARTIFACT_DATA_CACHE_MAX,
					DEFAULT_ARTIFACT_DATA_CACHE_MAX));
      boolean instrument =
	config.getBoolean(PARAM_ARTIFACT_CACHE_INSTRUMENT,
			  DEFAULT_ARTIFACT_CACHE_INSTRUMENT);
      artCache.enableInstrumentation(instrument);
      // RestLockssRepository will enable the cache only once it has
      // created a JMS consumer for invalidate notifications
      JMSManager mgr = getDaemon().getManagerByType(JMSManager.class);
      repo.enableArtifactCache(true, mgr.getJmsFactory());
    }
  }

  /** Return list of known repository URLs.  Currently there's just one. */
  public List<String> getRepositoryUrlList() {
    if (v2Repo == null) {
      return Collections.emptyList();
    }
    return ListUtil.list(v2Repo.getSpec());
  }

  /** Return list of known repository specs.  Currently there's just one. */
  public List<RepoSpec> getRepositorySpecList() {
    if (v2Repo == null) {
      return Collections.emptyList();
    }
    return ListUtil.list(v2Repo);
  }

  public LockssRepository getRepoRepo(String repoSpec) {
    RepoSpec rspec = getV2Repository(repoSpec);
    if (rspec == null) {
      throw new IllegalArgumentException("Unknown repository: " + repoSpec);
    }
    return rspec.getRepository();
  }

  /** Return list of known LockssRepository.  Currently there's just
   * one. */
  public List<LockssRepository> getRepositoryList() {
    List<LockssRepository> res = new ArrayList<>();
    for (String spec : getRepositoryUrlList()) {
      res.add(getRepoRepo(spec));
    }
    return res;
  }

  /** Return the DF for the given repo spec.  Currently hardwired for the
   * sole confiured repo */
  public RepositoryInfo getRepositoryInfo(String repoSpec)
      throws IOException {
    return getRepoRepo(repoSpec).getRepositoryInfo();
  }

  /** Return the DF for the given repo spec.  Currently hardwired for the
   * sole confiured repo */
  public PlatformUtil.DF getRepositoryDF(String repoSpec) {
    try {
      StorageInfo storageInfo = getRepositoryInfo(repoSpec).getStoreInfo();
      log.trace("storageInfo = {}", storageInfo);

      return PlatformUtil.DF.fromStorageInfo(storageInfo);
    } catch (IOException ioe) {
      log.warn("Exception caught getting repositoryDF: " + ioe.getMessage());
      return new PlatformUtil.DF();
    }
  }

  public Map<String,PlatformUtil.DF> getRepositoryDFMap() {
    Map<String,PlatformUtil.DF> repoMap = new LinkedMap<>();
    for (String repo : getRepositoryUrlList()) {
      repoMap.put(repo, getRepositoryDF(repo));
    }
    return repoMap;
  }

  public String findLeastFullRepository() {
    return findLeastFullRepository(getRepositoryDFMap());
  }

  public String findLeastFullRepository(Map<String,PlatformUtil.DF> repoMap) {
    String mostFree = null;
    for (String repo : repoMap.keySet()) {
      PlatformUtil.DF df = repoMap.get(repo);
      if (df != null) {
        if (mostFree == null ||
            (repoMap.get(mostFree)).getAvail() < df.getAvail()) {
          mostFree = repo;
        }
      }
    }
    return mostFree;
  }

  public PlatformUtil.DF getDiskWarnThreshold() {
    return paramDFWarn;
  }

  public PlatformUtil.DF getDiskFullThreshold() {
    return paramDFFull;
  }

  // BatchAuConfig (via RemoteApi) asks for existing repo info for large
  // numbers of AUIDs at a time. Querying the repo for each is way
  // expensive (esp. as the only way to do it currently is to enumerate the
  // known AUIDs, but would still be a huge number of roundtrips even if
  // there were a way to query by AUID).  Build a map of auid -> repsspec
  // on first use, or if it is stale (PARAM_AUID_REPO_MAP_AGE), or if an
  // auCreated() event has been seen.

  private Map<String,List<String>> auidToRepoSpec;
  private long auidToRepoMapDate;

  /** Build a map of AUID -> repository spec. */
  public synchronized void buildAuMap() {
    Map<String,List<String>> res = new HashMap<>();
    LockssRepository repo = v2Repo.getRepository();
    try {
      for (String auid : repo.getAuIds(v2Repo.getNamespace())) {
	res.put(auid, Collections.singletonList(v2Repo.getSpec()));
      }
    } catch (IOException e) {
      log.warn("Error getting list of AUID in repository namespace");
    }
    auidToRepoSpec = res;
    auidToRepoMapDate = TimeBase.nowMs();
  }

  void flushAuidRepoMap() {
    auidToRepoSpec = null;
  }

  /** Return a list of repo specs where the auid has content.  Currently
   * returns either a singleton or null */
  public List<String> findExistingRepositoriesFor(String auid) {
    if (auidToRepoSpec == null ||
	TimeBase.msSince(auidToRepoMapDate) > auidRepoMapAge) {
      buildAuMap();
    }
    return auidToRepoSpec.get(auid);
  }

  /**
   * Return the disk space used by the AU, including all overhead,
   * optionally calculating it if necessary.
   * @param repoAuPath the full path to an AU dir in a LockssRepositoryImpl
   * @param calcIfUnknown if true, size will calculated if unknown (time
   * consumeing)
   * @return the AU's disk usage in bytes, or -1 if unknown
   */
  // XXXREPO
  public long getRepoDiskUsage(String repoAuPath, boolean calcIfUnknown) {
    return -1;
  }

  /** Search all repositories and AUs for Artifacts with the given URL
   * @param normUrl the normalized URL to search for
   * @return List of Artifacts with that URL, includes only the highest
   * version of each matching Artifact in each AU.
   */
  public List<Artifact> findArtifactsByUrl(String normUrl) {
    return findArtifactsByUrl(normUrl, ArtifactVersions.LATEST);
  }

  /** Search all repositories and AUs for Artifacts with the given URL
   * @param normUrl the normalized URL to search for
   * @param versions specify whether to return all versions of matching
   * artifacts or only the latest of each
   * @return List of Artifacts with that URL
   */
  public List<Artifact> findArtifactsByUrl(String normUrl,
                                           ArtifactVersions versions) {
    List<Artifact> res = new ArrayList<>();
    for (RepoSpec spec : getRepositorySpecList()) {
      LockssRepository repo = spec.getRepository();
      log.debug2("Searching {} for {}, {}", spec, normUrl, versions);
      try {
        Iterable<Artifact> riter =
          repo.getArtifactsWithUrlFromAllAus(spec.getNamespace(),
                                             normUrl,
                                             versions);
        return IteratorUtils.toList(riter.iterator());
      } catch (IOException e) {
        log.warn("Couldn't find repository: {} ({})",
                 normUrl, spec.getNamespace());
      }
    }
    return res;
  }

}
