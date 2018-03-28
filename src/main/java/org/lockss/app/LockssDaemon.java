/*

Copyright (c) 2000-2018 Board of Trustees of Leland Stanford Jr. University,
all rights reserved.

Redistribution and use in source and binary forms, with or without modification,
are permitted provided that the following conditions are met:

1. Redistributions of source code must retain the above copyright notice, this
list of conditions and the following disclaimer.

2. Redistributions in binary form must reproduce the above copyright notice,
this list of conditions and the following disclaimer in the documentation and/or
other materials provided with the distribution.

3. Neither the name of the copyright holder nor the names of its contributors
may be used to endorse or promote products derived from this software without
specific prior written permission.

THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND
ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE FOR
ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
(INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON
ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
(INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

*/
package org.lockss.app;

import java.io.File;
import java.io.IOException;
import java.util.*;
import org.apache.commons.lang3.*;
import static org.lockss.app.ManagerDescs.*;
import org.lockss.util.*;
import org.lockss.alert.*;
import org.lockss.daemon.*;
import org.lockss.daemon.status.*;
import org.lockss.exporter.FetchTimeExportManager;
import org.lockss.exporter.counter.CounterReportsManager;
import org.lockss.account.*;
import org.lockss.hasher.*;
import org.lockss.scheduler.*;
import org.lockss.metadata.MetadataDbManager;
import org.lockss.metadata.MetadataManager;
import org.lockss.plugin.*;
import org.lockss.truezip.*;
import org.lockss.poller.*;
import org.lockss.protocol.*;
import org.lockss.protocol.psm.*;
import org.lockss.repository.*;
import org.lockss.state.*;
import org.lockss.subscription.SubscriptionManager;
import org.lockss.proxy.*;
import org.lockss.proxy.icp.IcpManager;
import org.lockss.config.*;
import org.lockss.crawler.*;
import org.lockss.remote.*;
import org.lockss.clockss.*;
import org.lockss.safenet.*;
import org.apache.commons.collections.map.LinkedMap;

/**
 * The legacy LOCKSS daemon application
 */
public class LockssDaemon extends LockssApp {
  
  private static final Logger log = Logger.getLogger(LockssDaemon.class);

  private static final String PREFIX = Configuration.PREFIX + "daemon.";

  /** List of local IP addresses to which to bind listen sockets for
   * servers (admin ui, content, proxy).  If not set, servers listen on all
   * interfaces.  Does not affect the port on which various servers listen.
   * Changing this requires daemon restart. */
  public static final String PARAM_BIND_ADDRS = PREFIX + "bindAddrs";

  // Parameter keys for daemon managers
  public static final String ACTIVITY_REGULATOR =
    managerKey(ActivityRegulator.class);
  public static final String HASH_SERVICE =
    managerKey(HashService.class);
  public static final String DATAGRAM_COMM_MANAGER =
    managerKey(LcapDatagramComm.class);
  public static final String STREAM_COMM_MANAGER =
    managerKey(LcapStreamComm.class);
  public static final String ROUTER_MANAGER =
    managerKey(LcapRouter.class);
//   public static final String IDENTITY_MANAGER =
//     mamagerKey(IdentityManager.class);
  public static final String CRAWL_MANAGER =
    managerKey(CrawlManager.class);
  public static final String METADATA_MANAGER =
    managerKey(MetadataManager.class);
  public static final String POLL_MANAGER =
    managerKey(PollManager.class);
  public static final String PSM_MANAGER =
    managerKey(PsmManager.class);
  public static final String REPOSITORY_MANAGER =
    managerKey(RepositoryManager.class);
  public static final String LOCKSS_REPOSITORY =
    managerKey(OldLockssRepository.class);
  public static final String HISTORY_REPOSITORY =
    managerKey(HistoryRepository.class);
  public static final String SERVLET_MANAGER =
    managerKey(org.lockss.servlet.AdminServletManager.class);
  public static final String CONTENT_SERVLET_MANAGER =
    managerKey(org.lockss.servlet.ContentServletManager.class);
  public static final String PROXY_MANAGER =
    managerKey(ProxyManager.class);
  public static final String AUDIT_PROXY_MANAGER =
    managerKey(AuditProxyManager.class);
  public static final String FAIL_OVER_PROXY_MANAGER =
    managerKey(FailOverProxyManager.class);
  public static final String REMOTE_API =
    managerKey(RemoteApi.class);
  public static final String REPOSITORY_STATUS =
    managerKey(LockssRepositoryStatus.class);
  public static final String ARCHIVAL_UNIT_STATUS =
    managerKey(ArchivalUnitStatus.class);
  public static final String PLATFORM_CONFIG_STATUS =
    managerKey(PlatformConfigStatus.class);
  public static final String CONFIG_STATUS =
    managerKey(ConfigStatus.class);
  public static final String OVERVIEW_STATUS =
    managerKey(OverviewStatus.class);
  public static final String ICP_MANAGER =
    managerKey(IcpManager.class);
  public static final String CLOCKSS_PARAMS =
    managerKey(ClockssParams.class);
  public static final String SAFENET_MANAGER =
    managerKey(CachingEntitlementRegistryClient.class);
  public static final String COUNTER_REPORTS_MANAGER =
    managerKey(CounterReportsManager.class);
  public static final String SUBSCRIPTION_MANAGER =
    managerKey(SubscriptionManager.class);
  public static final String FETCH_TIME_EXPORT_MANAGER =
    managerKey(org.lockss.exporter.FetchTimeExportManager.class);
  public static final String METADATA_DB_MANAGER =
    managerKey(MetadataDbManager.class);
  public static final String SCHED_SERVICE =
    managerKey(SchedService.class);


  protected static final String DEFAULT_SCHED_SERVICE =
    "org.lockss.scheduler.SchedService";

  // Managers specific to this service.  They are started in this order,
  // following the standard managers specified in BaseLockssDaemon
  private final ManagerDesc[] myManagerDescs = {
    // start plugin manager after generic services
    PLUGIN_MANAGER_DESC,
    SCHED_SERVICE_DESC,
    HASH_SERVICE_DESC,
    SYSTEM_METRICS_DESC,
    ACCOUNT_MANAGER_DESC,
    IDENTITY_MANAGER_DESC,
    PSM_MANAGER_DESC,
    POLL_MANAGER_DESC,
    CRAWL_MANAGER_DESC,
    REPOSITORY_MANAGER_DESC,
    // start metadata manager after plugin manager and database manager.
    METADATA_DB_MANAGER_DESC,
    // start metadata manager after metadata database manager
    METADATA_MANAGER_DESC,
    // start proxy and servlets after plugin manager
    REMOTE_API_DESC,
    // Start the COUNTER reports manager.
    COUNTER_REPORTS_MANAGER_DESC,
    // Start the subscription manager.
    SUBSCRIPTION_MANAGER_DESC,
    // Start the fetch time export manager.
    FETCH_TIME_EXPORT_MANAGER_DESC,
    // NOTE: Any managers that are needed to decide whether a servlet is to be
    // enabled or not (through ServletDescr.isEnabled()) need to appear before
    // the AdminServletManager on the next line.
    SERVLET_MANAGER_DESC,
    CONTENT_SERVLET_MANAGER_DESC,
    PROXY_MANAGER_DESC,
    AUDIT_PROXY_MANAGER_DESC,
    FAIL_OVER_PROXY_MANAGER_DESC,
    // comm after other major services so don't process messages until
    // they're ready
    DATAGRAM_COMM_MANAGER_DESC,
    STREAM_COMM_MANAGER_DESC,
    ROUTER_MANAGER_DESC,
    ICP_MANAGER_DESC,
    PLATFORM_CONFIG_STATUS_DESC,
    CONFIG_STATUS_DESC,
    ARCHIVAL_UNIT_STATUS_DESC,
    REPOSITORY_STATUS_DESC,
    OVERVIEW_STATUS_DESC,
    new ManagerDesc(CLOCKSS_PARAMS, "org.lockss.clockss.ClockssParams") {
      public boolean shouldStart() {
        return isClockss();
      }},
    new ManagerDesc(SAFENET_MANAGER,
		    "org.lockss.safenet.CachingEntitlementRegistryClient") {
      public boolean shouldStart() {
        return isSafenet();
      }},
  };

  // AU-specific manager descriptors.  As each AU is created its managers
  // are started in this order.
  protected final ManagerDesc[] auManagerDescs = {
    new ManagerDesc(ACTIVITY_REGULATOR,
                    "org.lockss.daemon.ActivityRegulator$Factory"),
    // LockssRepository uses ActivityRegulator
    new ManagerDesc(LOCKSS_REPOSITORY,
                    "org.lockss.repository.OldLockssRepositoryImpl$Factory"),
    // HistoryRepository needs no extra managers
    new ManagerDesc(HISTORY_REPOSITORY,
                    "org.lockss.state.HistoryRepositoryImpl$Factory")
  };

  // Maps au to sequenced map of managerKey -> manager instance
  protected HashMap<ArchivalUnit,Map<String,LockssAuManager>> auManagerMaps = 
      new HashMap<ArchivalUnit,Map<String,LockssAuManager>>();

  // Maps managerKey -> LockssAuManager.Factory instance
  protected HashMap<String,LockssAuManager.Factory> auManagerFactoryMap = 
      new HashMap<String,LockssAuManager.Factory>();

  private static LockssDaemon theDaemon;
  private boolean isClockss;

  protected LockssDaemon() {
    super();
    theDaemon = this;
  }

  protected LockssDaemon(List<String> propUrls) {
    super(propUrls);
    theDaemon = this;
  }

  protected LockssDaemon(List<String> propUrls, String groupNames) {
    super(propUrls, groupNames);
    theDaemon = this;
  }

  protected LockssDaemon(String bootstrapPropsUrl, String restConfigServiceUrl,
      List<String> propUrls, String groupNames) {
    super(bootstrapPropsUrl, restConfigServiceUrl, propUrls, groupNames);
    theDaemon = this;
  }

  @Override
  protected ManagerDesc[] getAppManagerDescs() {
    return myManagerDescs;
  }

  /**
   * True iff all managers have been inited.
   * @return true iff all managers have been inited */
  public boolean isDaemonInited() {
    return isAppInited();
  }

  /**
   * True if all managers have been started.
   * @return true iff all managers have been started */
  public boolean isDaemonRunning() {
    return isAppRunning();
  }

  /**
   * Return the LockssDaemon instance
   */
  public static LockssDaemon getLockssDaemon() {
    return theDaemon;
  }

  /**
   * True if running as a CLOCKSS daemon
   */
  public boolean isClockss() {
    return isClockss;
  }

  /**
   * Convenience method returns {@link
   * ClockssParams#isDetectSubscription()}
   */
  public boolean isDetectClockssSubscription() {
    return isClockss() && getClockssParams().isDetectSubscription();
  }

  /**
   * return the ClockssParams instance.
   * @return ClockssParams instance.
   * @throws IllegalArgumentException if the manager is not available.
   */
  public ClockssParams getClockssParams() {
    return (ClockssParams) getManager(CLOCKSS_PARAMS);
  }

  /** Stop the daemon.  Currently only used in testing. */
  public void stopDaemon() {
    stopApp();
  }

  // LockssManager accessors

  /**
   * return the hash service instance
   * @return the HashService
   * @throws IllegalArgumentException if the manager is not available.
   */
  public HashService getHashService() {
    return (HashService) getManager(HASH_SERVICE);
  }

  /**
   * return the sched service instance
   * @return the SchedService
   * @throws IllegalArgumentException if the manager is not available.
   */
  public SchedService getSchedService() {
    return (SchedService) getManager(SCHED_SERVICE);
  }

  /**
   * return the poll manager instance
   * @return the PollManager
   * @throws IllegalArgumentException if the manager is not available.
   */
  public PollManager getPollManager() {
    return (PollManager) getManager(POLL_MANAGER);
  }

  /**
   * return the psm manager instance
   * @return the PsmManager
   * @throws IllegalArgumentException if the manager is not available.
   */
  public PsmManager getPsmManager() {
    return (PsmManager) getManager(PSM_MANAGER);
  }

  /**
   * return the datagram communication manager instance
   * @return the LcapDatagramComm
   * @throws IllegalArgumentException if the manager is not available.
   */
  public LcapDatagramComm getDatagramCommManager()  {
    return (LcapDatagramComm) getManager(DATAGRAM_COMM_MANAGER);
  }

  /**
   * return the stream communication manager instance
   * @return the LcapStreamComm
   * @throws IllegalArgumentException if the manager is not available.
   */
  public LcapStreamComm getStreamCommManager()  {
    return (LcapStreamComm) getManager(STREAM_COMM_MANAGER);
  }

  /**
   * return the communication router manager instance
   * @return the LcapDatagramRouter
   * @throws IllegalArgumentException if the manager is not available.
   */
  public LcapRouter getRouterManager()  {
    return (LcapRouter) getManager(ROUTER_MANAGER);
  }

  /**
   * return the proxy handler instance
   * @return the ProxyManager
   * @throws IllegalArgumentException if the manager is not available.
  */
  public ProxyManager getProxyManager() {
    return (ProxyManager) getManager(PROXY_MANAGER);
  }

  /**
   * return the crawl manager instance
   * @return the CrawlManager
   * @throws IllegalArgumentException if the manager is not available.
   */
  public CrawlManager getCrawlManager() {
    return (CrawlManager) getManager(CRAWL_MANAGER);
  }

  /**
   * return the repository manager instance
   * @return the RepositoryManager
   * @throws IllegalArgumentException if the manager is not available.
   */
  public RepositoryManager getRepositoryManager()  {
    return (RepositoryManager)getManager(REPOSITORY_MANAGER);
  }

  /**
   * return the metadata manager instance
   * @return the MetadataManager
   * @throws IllegalArgumentException if the manager is not available.
   */
  public MetadataManager getMetadataManager() {
    return (MetadataManager) getManager(METADATA_MANAGER);
  }

  /**
   * <p>Retrieves the ICP manager.</p>
   * @return The ICP manager instance.
   * @throws IllegalArgumentException if the manager is not available.
   */
  public IcpManager getIcpManager() {
    return (IcpManager)getManager(ICP_MANAGER);
  }

  /**
   * return the RemoteApi instance.
   * @return RemoteApi instance.
   * @throws IllegalArgumentException if the manager is not available.
   */
  public RemoteApi getRemoteApi() {
    return (RemoteApi) getManager(REMOTE_API);
  }

  /**
   * return the ArchivalUnitStatus instance.
   * @return ArchivalUnitStatus instance.
   * @throws IllegalArgumentException if the manager is not available.
   */
  public ArchivalUnitStatus getArchivalUnitStatus() {
    return (ArchivalUnitStatus) getManager(ARCHIVAL_UNIT_STATUS);
  }

  /**
   * Provides the COUNTER reports manager.
   * 
   * @return a CounterReportsManager with the COUNTER reports manager.
   * @throws IllegalArgumentException
   *           if the manager is not available.
   */
  public CounterReportsManager getCounterReportsManager() {
    return (CounterReportsManager) getManager(COUNTER_REPORTS_MANAGER);
  }

  /**
   * Provides the subscription manager.
   * 
   * @return a SubscriptionManager with the subscription manager.
   * @throws IllegalArgumentException
   *           if the manager is not available.
   */
  public SubscriptionManager getSubscriptionManager() {
    return (SubscriptionManager) getManager(SUBSCRIPTION_MANAGER);
  }

  /**
   * Provides the fetch time export manager.
   * 
   * @return a FetchTimeExportManager with the fetch time export manager.
   * @throws IllegalArgumentException
   *           if the manager is not available.
   */
  public FetchTimeExportManager getFetchTimeExportManager() {
    return (FetchTimeExportManager) getManager(FETCH_TIME_EXPORT_MANAGER);
  }

  /**
   * Provides the metadata database manager instance.
   * 
   * @return a MetadataDbManager with the metadata database manager instance.
   * @throws IllegalArgumentException
   *           if the manager is not available.
   */
  public MetadataDbManager getMetadataDbManager() {
    return (MetadataDbManager) getManager(METADATA_DB_MANAGER);
  }

  /**
   * return the {@link org.lockss.daemon.status.StatusService} instance
   * @return {@link org.lockss.daemon.status.StatusService} instance
   * @throws IllegalArgumentException if the manager is not available.
   */
  public StatusService getStatusService() {
    return (StatusService) getManager(STATUS_SERVICE);
  }

  /**
   * return the EntitlementRegistryClient instance.
   * @return EntitlementRegistryClient instance.
   * @throws IllegalArgumentException if the manager is not available.
   */
  public EntitlementRegistryClient getEntitlementRegistryClient() {
    return (EntitlementRegistryClient) getManager(SAFENET_MANAGER);
  }

  // LockssAuManager accessors

  /**
   * Return an AU-specific lockss manager. This will need to be cast to the
   * appropriate class.
   * @param key the name of the manager
   * @param au the AU
   * @return a LockssAuManager
   * @throws IllegalArgumentException if the manager is not available.
   */
  public static LockssAuManager getStaticAuManager(String key,
                                                   ArchivalUnit au) {
    return theDaemon.getAuManager(key, au);
  }

  /**
   * Return an AU-specific lockss manager. This will need to be cast to the
   * appropriate class.
   * @param key the name of the manager
   * @param au the AU
   * @return a LockssAuManager
   * @throws IllegalArgumentException if the manager is not available.
   */
  public LockssAuManager getAuManager(String key, ArchivalUnit au) {
    LockssAuManager mgr = null;
    LinkedMap auMgrMap =
      (LinkedMap)auManagerMaps.get(au);
    if (auMgrMap != null) {
      mgr = (LockssAuManager)auMgrMap.get(key);
    }
    if (mgr == null) {
      log.error(key + " not found for au: " + au);
      throw new IllegalArgumentException("Unavailable au manager:" + key);
    }
    return mgr;
  }

  /**
   * Get Lockss Repository instance
   * @param au the ArchivalUnit
   * @return the LockssRepository
   * @throws IllegalArgumentException if the manager is not available.
   */
  public OldLockssRepository getLockssRepository(ArchivalUnit au) {
    return (OldLockssRepository)getAuManager(LOCKSS_REPOSITORY, au);
  }

  /**
   * Return the HistoryRepository instance
   * @param au the ArchivalUnit
   * @return the HistoryRepository
   * @throws IllegalArgumentException if the manager is not available.
   */
  public HistoryRepository getHistoryRepository(ArchivalUnit au) {
    return (HistoryRepository)getAuManager(HISTORY_REPOSITORY, au);
  }

  /**
   * Return ActivityRegulator instance
   * @param au the ArchivalUnit
   * @return the ActivityRegulator
   * @throws IllegalArgumentException if the manager is not available.
   */
  public ActivityRegulator getActivityRegulator(ArchivalUnit au) {
    return (ActivityRegulator)getAuManager(ACTIVITY_REGULATOR, au);
  }

  /**
   * Return all ActivityRegulators.
   * @return a list of all ActivityRegulators for all AUs
   */
  public List<ActivityRegulator> getAllActivityRegulators() {
    return getAuManagersOfType(ACTIVITY_REGULATOR);
  }

  /**
   * Return all LockssRepositories.
   * @return a list of all LockssRepositories for all AUs
   */
  public List<OldLockssRepository> getAllLockssRepositories() {
    return getAuManagersOfType(LOCKSS_REPOSITORY);
  }


  // AU specific manager loading, starting, stopping

  protected ManagerDesc[] getAuManagerDescs() {
    return auManagerDescs;
  }

  /**
   * Start or reconfigure all managers necessary to handle the ArchivalUnit.
   * @param au the ArchivalUnit
   * @param auConfig the AU's confignuration
   */
  @SuppressWarnings("unchecked")
  public void startOrReconfigureAuManagers(ArchivalUnit au,
                                           Configuration auConfig)
      throws Exception {
    LinkedMap auMgrMap = (LinkedMap)auManagerMaps.get(au);
    if (auMgrMap != null) {
      // If au has a map it's been created, just set new config
      configAuManagers(au, auConfig, auMgrMap);
    } else {
      // create a new map, init, configure and start managers
      auMgrMap = new LinkedMap();
      initAuManagers(au, auMgrMap);
      // Store map once all managers inited
      auManagerMaps.put(au, auMgrMap);
      configAuManagers(au, auConfig, auMgrMap);
      try {
        startAuManagers(au, auMgrMap);
      } catch (Exception e) {
        log.warning("Stopping managers for " + au);
        stopAuManagers(au);
        throw e;
      }
    }
  }

  /** Stop the managers for the AU in the reverse order in which they
   * appear in the map */
  public void stopAuManagers(ArchivalUnit au) {
    LinkedMap auMgrMap =
      (LinkedMap)auManagerMaps.get(au);
    if (auMgrMap != null) {
      @SuppressWarnings("unchecked")
      List<String> rkeys = ListUtil.reverseCopy(auMgrMap.asList());
      for (String key : rkeys) {
        LockssAuManager mgr = (LockssAuManager)auMgrMap.get(key);
        try {
          mgr.stopService();
        } catch (Exception e) {
          log.warning("Couldn't stop au manager " + mgr, e);
          // continue to try to stop other managers
        }
      }
    }
    auManagerMaps.remove(au);
  }

  /** Create and init all AU managers for the AU, and associate them with
   * their keys in auMgrMap. */
  private void initAuManagers(ArchivalUnit au, LinkedMap auMgrMap)
      throws Exception {
    ManagerDesc descs[] = getAuManagerDescs();
    for (int ix = 0; ix < descs.length; ix++) {
      ManagerDesc desc = descs[ix];
      if (desc.shouldStart()) {
        try {
          LockssAuManager mgr = initAuManager(desc, au);
          auMgrMap.put(desc.key, mgr);
        } catch (Exception e) {
          log.error("Couldn't init AU manager " + desc.key + " for " + au,
                    e);
          // don't try to init remaining managers
          throw e;
        }
      }
    }
  }

  /** Create and init an AU manager. */
  protected LockssAuManager initAuManager(ManagerDesc desc, ArchivalUnit au)
      throws Exception {
    LockssAuManager mgr = instantiateAuManager(desc, au);
    mgr.initService(this);
    return mgr;
  }

  /** Start the managers for the AU in the order in which they appear in
   * the map.  protected so MockLockssDaemon can override to suppress
   * startService() */
  protected void startAuManagers(ArchivalUnit au, Map<String,? extends LockssAuManager> auMgrMap)
      throws Exception {
    for (LockssAuManager mgr : auMgrMap.values()) {
      try {
        mgr.startService();
      } catch (Exception e) {
        log.error("Couldn't start AU manager " + mgr + " for " + au,
                  e);
        // don't try to start remaining managers
        throw e;
      }
    }
  }

  /** (re)configure the au managers */
  private void configAuManagers(ArchivalUnit au, Configuration auConfig,
                                Map<String,? extends LockssAuManager> auMgrMap) {
    for (LockssAuManager mgr : auMgrMap.values()) {
      try {
        mgr.setAuConfig(auConfig);
      } catch (Exception e) {
        log.error("Error configuring AU manager " + mgr + " for " + au, e);
        // continue after config errors
      }
    }
  }

  /** Instantiate a LockssAuManager, using a LockssAuManager.Factory, which
   * is created is necessary */
  private LockssAuManager instantiateAuManager(ManagerDesc desc,
                                               ArchivalUnit au)
      throws Exception {
    String key = desc.key;
    LockssAuManager.Factory factory =
      (LockssAuManager.Factory)auManagerFactoryMap.get(key);
    if (factory == null) {
      factory = instantiateAuFactory(desc);
      auManagerFactoryMap.put(key, factory);
    }
    LockssAuManager mgr = factory.createAuManager(au);
    return mgr;
  }

  /** Instantiate a LockssAuManager.Factory, which is used to create
   * instances of a LockssAuManager */
  private LockssAuManager.Factory instantiateAuFactory(ManagerDesc desc)
      throws Exception {
    String managerName = CurrentConfig.getParam(MANAGER_PREFIX + desc.key,
                                                desc.defaultClass);
    LockssAuManager.Factory factory;
    try {
      factory = (LockssAuManager.Factory)makeInstance(managerName);
    } catch (ClassNotFoundException e) {
      log.warning("Couldn't load au manager factory class " + managerName);
      if (!managerName.equals(desc.defaultClass)) {
        log.warning("Trying default factory class " + desc.defaultClass);
        factory = (LockssAuManager.Factory)makeInstance(desc.defaultClass);
      } else {
        throw e;
      }
    }
    return factory;
  }

  /**
   * Calls 'stopService()' on all AU managers for all AUs,
   */
  public void stopAllAuManagers() {
    ArchivalUnit au;
    while ((au = CollectionUtil.getAnElement(auManagerMaps.keySet())) != null) {
      log.debug2("Stopping all managers for " + au);
      stopAuManagers(au);
    }
    auManagerMaps.clear();
  }

  /**
   * Return the LockssAuManagers of a particular type.
   * @param managerKey the manager type
   * @return a list of LockssAuManagers
   */
  @SuppressWarnings("unchecked")
  <T extends LockssAuManager> List<T> getAuManagersOfType(String managerKey) {
    List<T> res = new ArrayList<T>(auManagerMaps.size());
    for (Map<String,LockssAuManager> auMgrMap : auManagerMaps.values()) {
      Object auMgr = auMgrMap.get(managerKey);
      if (auMgr != null) {
        res.add((T)auMgr);
      }
    }
    return res;
  }

  // Daemon start, stop

  protected OneShotSemaphore ausStarted = new OneShotSemaphore();

  protected void startDaemon() throws Exception {
    startApp();
  }

  protected void startApp() throws Exception {
    super.startApp();
    log.info("Started");
    if (CurrentConfig.getBooleanParam(PARAM_START_PLUGINS,
				      DEFAULT_START_PLUGINS)) {
      getPluginManager().startLoadablePlugins();
      ausStarted.fill();
    }

    AlertManager alertMgr = getAlertManager();
    alertMgr.raiseAlert(Alert.cacheAlert(Alert.DAEMON_STARTED),
			"LOCKSS daemon " +
			ConfigManager.getDaemonVersion().displayString() +
			" started");
  }


  /**
   * Stop the daemon, by stopping the managers in the reverse order of
   * starting.
   */
  protected void stop() {
    appRunning = false;

    // stop all au-specific managers
    stopAllAuManagers();

    super.stop();
  }

  /** Wait until the initial set of AUs have been started.  This must be
   * called only from your own thread (<i>eg</i>, not the startup
   * thread.) */
  public void waitUntilAusStarted() throws InterruptedException {
    ausStarted.waitFull(Deadline.MAX);
  }

  /** Return true if all AUs have been started */
  public boolean areAusStarted() {
    return ausStarted.isFull();
  }

  protected void setConfig(Configuration config, Configuration prevConfig,
                           Configuration.Differences changedKeys) {

    String proj = ConfigManager.getPlatformProject();
    isClockss = "clockss".equalsIgnoreCase(proj);
    isSafenet = "safenet".equalsIgnoreCase(proj);

    super.setConfig(config, prevConfig, changedKeys);
  }

  /**
   * Legacy daemon app.  .  Startup arguments:
   */
  public static void main(String[] args) {
    // ManagerDescs supplied by getAppManagerDescs() above
    AppSpec spec = new AppSpec()
      .setName("Lockss Daemon")
      .setComputeAppManagers(true)
      .setArgs(args)
      .addAppConfig(PARAM_START_PLUGINS, "true")
      .addAppConfig(PluginManager.PARAM_START_ALL_AUS, "true")
      .addAppConfig(PluginManager.PARAM_AU_CONTENT_FROM_WS, "false")
      .setKeepRunning(true);
    startStatic(LockssDaemon.class, spec);
  }
}
