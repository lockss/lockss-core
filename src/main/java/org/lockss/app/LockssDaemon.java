/*

Copyright (c) 2000-2019 Board of Trustees of Leland Stanford Jr. University,
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

import org.apache.commons.collections4.map.LinkedMap;
import org.lockss.alert.Alert;
import org.lockss.alert.AlertManager;
import org.lockss.clockss.ClockssParams;
import org.lockss.config.*;
import org.lockss.config.db.ConfigDbManager;
import org.lockss.crawler.CrawlManager;
import org.lockss.daemon.status.OverviewStatus;
import org.lockss.daemon.status.StatusService;
import org.lockss.exporter.FetchTimeExportManager;
import org.lockss.exporter.counter.CounterReportsManager;
import org.lockss.hasher.HashService;
import org.lockss.metadata.MetadataDbManager;
import org.lockss.metadata.MetadataManager;
import org.lockss.plugin.ArchivalUnit;
import org.lockss.plugin.PluginManager;
import org.lockss.poller.PollManager;
import org.lockss.protocol.LcapRouter;
import org.lockss.protocol.LcapStreamComm;
import org.lockss.protocol.psm.PsmManager;
import org.lockss.proxy.AuditProxyManager;
import org.lockss.proxy.FailOverProxyManager;
import org.lockss.proxy.ProxyManager;
import org.lockss.proxy.icp.IcpManager;
import org.lockss.remote.RemoteApi;
import org.lockss.repository.RepositoryDbManager;
import org.lockss.repository.RepositoryManager;
import org.lockss.safenet.CachingEntitlementRegistryClient;
import org.lockss.safenet.EntitlementRegistryClient;
import org.lockss.scheduler.SchedService;
import org.lockss.state.ArchivalUnitStatus;
import org.lockss.state.StateManager;
import org.lockss.subscription.SubscriptionManager;
import org.lockss.util.CollectionUtil;
import org.lockss.util.ListUtil;
import org.lockss.util.Logger;
import org.lockss.util.OneShotSemaphore;
import org.lockss.util.time.Deadline;
import org.lockss.util.rest.status.*;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.lockss.app.ManagerDescs.*;

import java.util.stream.*;
import java.util.Arrays;
import java.net.*;

/**
 * The legacy LOCKSS daemon application
 */
public class LockssDaemon extends LockssApp {
  
  private static final Logger log = Logger.getLogger();

  private static final String PREFIX = Configuration.PREFIX + "daemon.";

  /** List of local IP addresses to which to bind listen sockets for
   * servers (admin ui, content, proxy).  If not set, servers listen on all
   * interfaces.  Does not affect the port on which various servers listen.
   * Changing this requires daemon restart. */
  public static final String PARAM_BIND_ADDRS = PREFIX + "bindAddrs";

  /** Specifies whether only plugin registry AUs should be crawled, only
   * normal AUs, or both.
   */
  public enum CrawlMode {
    /** Don't crawl at all  */
    None(false, false),
    /** Crawl only plugin registry AUs */
    Plugins(true, false),
    /** Crawl all AUs */
    All(true, true),
    /** Crawl only non-plugin registry AUs */
    NonPlugins(false, true);

    private final boolean isCrawlPlugins;
    private final boolean isCrawlNonPlugins;

    CrawlMode(boolean plug, boolean nonplug) {
      this.isCrawlPlugins = plug;
      this.isCrawlNonPlugins = nonplug;
    }
    public boolean isCrawlPlugins() { return isCrawlPlugins; }
    public boolean isCrawlNonPlugins() { return isCrawlNonPlugins; }
    public boolean isCrawlNothing() { return !(isCrawlPlugins || isCrawlNonPlugins); }
  }

  /** Crawl <b><tt>Plugins</tt></b> only,
   * <b><tt>NonPlugins</tt></b> only, or <b><tt>All</tt></b> AUs
   */
  public static final String PARAM_CRAWL_MODE = PREFIX + "crawlMode";
  static final CrawlMode DEFAULT_CRAWL_MODE = CrawlMode.All;


  /**
   * Set to the abbreviation of the component that should crawl plugin
   * registries
   */
  public static final String PARAM_PLUGINS_CRAWLER = PREFIX + "pluginsCrawler";
  static final String DEFAULT_PLUGINS_CRAWLER = null;

  // Parameter keys for daemon managers
  public static final String HASH_SERVICE =
    managerKey(HashService.class);
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
  public static final String REPOSITORY_DB_MANAGER =
      managerKey(RepositoryDbManager.class);
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
  public static final String ARCHIVAL_UNIT_STATUS =
    managerKey(ArchivalUnitStatus.class);
  public static final String PLATFORM_CONFIG_STATUS =
    managerKey(PlatformConfigStatus.class);
  public static final String BUILD_INFO_STATUS =
    managerKey(BuildInfoStatus.class);
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
  public static final String CONFIG_DB_MANAGER =
    managerKey(ConfigDbManager.class);
  public static final String STATE_MANAGER =
    managerKey(StateManager.class);


  // Managers specific to this service.  They are started in this order,
  // following the standard managers specified in BaseLockssDaemon
  private final ManagerDesc[] myManagerDescs = {
    // start plugin manager after generic services
    CONFIG_DB_MANAGER_DESC,
    PLUGIN_MANAGER_DESC,
    // StateManager must follow PluginManager
    STATE_MANAGER_DESC,
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
    STREAM_COMM_MANAGER_DESC,
    ROUTER_MANAGER_DESC,
    ICP_MANAGER_DESC,
    PLATFORM_CONFIG_STATUS_DESC,
    BUILD_INFO_STATUS_DESC,
    CONFIG_STATUS_DESC,
    ARCHIVAL_UNIT_STATUS_DESC,
    OVERVIEW_STATUS_DESC,
    new ManagerDesc(CLOCKSS_PARAMS, "org.lockss.clockss.ClockssParams") {
      public boolean shouldStart(LockssApp app) {
        return isClockss();
      }},
    new ManagerDesc(SAFENET_MANAGER,
		    "org.lockss.safenet.CachingEntitlementRegistryClient") {
      public boolean shouldStart(LockssApp app) {
        return isSafenet();
      }},
    METADATA_DB_MANAGER_DESC
  };

  // AU-specific manager descriptors.  As each AU is created its managers
  // are started in this order.
  protected final ManagerDesc[] auManagerDescs = {
  };

  // Maps au to sequenced map of managerKey -> manager instance
  protected HashMap<ArchivalUnit,Map<String,LockssAuManager>> auManagerMaps = 
      new HashMap<ArchivalUnit,Map<String,LockssAuManager>>();

  // Maps managerKey -> LockssAuManager.Factory instance
  protected HashMap<String,LockssAuManager.Factory> auManagerFactoryMap = 
      new HashMap<String,LockssAuManager.Factory>();

  private boolean isClockss;
  private CrawlMode paramCrawlMode = DEFAULT_CRAWL_MODE;
  private ServiceBinding pluginsCrawler = null;

  protected LockssDaemon() {
    super();
  }

  protected LockssDaemon(List<String> propUrls) {
    super(propUrls);
  }

  protected LockssDaemon(List<String> propUrls, String groupNames) {
    super(propUrls, groupNames);
  }

  protected LockssDaemon(List<String> bootstrapPropsUrls,
			 String restConfigServiceUrl,
			 List<String> propUrls,
			 String groupNames) {
    super(bootstrapPropsUrls, restConfigServiceUrl, propUrls, groupNames);
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

  static LockssDaemon MOCK_LOCKSS_DAEMON;
  /** Allows test code to cause the static
   * LockssDaemon.getLockssDaemon() to return a MockLockssDaemon */
  public static void setLockssDaemon(LockssDaemon mockLockssDaemon) {
    MOCK_LOCKSS_DAEMON = mockLockssDaemon;
  }

  /**
   * static accessor for the LockssDaemon instance.  In support of Spring and
   * other inverted start-order frameworks, this method will wait a short
   * time for the LockssDaemon instance to be created.
   * @throws IllegalStateException if that doesn't happen quickly
   * @return the LockssDaemon instance
   */
  public static LockssDaemon getLockssDaemon() {
    if (MOCK_LOCKSS_DAEMON != null) {
      return MOCK_LOCKSS_DAEMON;
    }
    // cast is ugly but safe; avoids a redundant WaitableObject in this class
    return (LockssDaemon)getLockssApp();
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
    return (ClockssParams) getManagerByKey(CLOCKSS_PARAMS);
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
    return (HashService) getManagerByKey(HASH_SERVICE);
  }

  /**
   * return the sched service instance
   * @return the SchedService
   * @throws IllegalArgumentException if the manager is not available.
   */
  public SchedService getSchedService() {
    return (SchedService) getManagerByKey(SCHED_SERVICE);
  }

  /**
   * return the poll manager instance
   * @return the PollManager
   * @throws IllegalArgumentException if the manager is not available.
   */
  public PollManager getPollManager() {
    return (PollManager) getManagerByKey(POLL_MANAGER);
  }

  /**
   * return the psm manager instance
   * @return the PsmManager
   * @throws IllegalArgumentException if the manager is not available.
   */
  public PsmManager getPsmManager() {
    return (PsmManager) getManagerByKey(PSM_MANAGER);
  }

  /**
   * return the stream communication manager instance
   * @return the LcapStreamComm
   * @throws IllegalArgumentException if the manager is not available.
   */
  public LcapStreamComm getStreamCommManager()  {
    return (LcapStreamComm) getManagerByKey(STREAM_COMM_MANAGER);
  }

  /**
   * return the communication router manager instance
   * @return the LcapRouter
   * @throws IllegalArgumentException if the manager is not available.
   */
  public LcapRouter getRouterManager()  {
    return (LcapRouter) getManagerByKey(ROUTER_MANAGER);
  }

  /**
   * return the proxy handler instance
   * @return the ProxyManager
   * @throws IllegalArgumentException if the manager is not available.
  */
  public ProxyManager getProxyManager() {
    return (ProxyManager) getManagerByKey(PROXY_MANAGER);
  }

  /**
   * return the crawl manager instance
   * @return the CrawlManager
   * @throws IllegalArgumentException if the manager is not available.
   */
  public CrawlManager getCrawlManager() {
    return (CrawlManager) getManagerByKey(CRAWL_MANAGER);
  }

  /**
   * return the repository manager instance
   * @return the RepositoryManager
   * @throws IllegalArgumentException if the manager is not available.
   */
  public RepositoryManager getRepositoryManager()  {
    return (RepositoryManager)getManagerByKey(REPOSITORY_MANAGER);
  }

  /**
   * return the metadata manager instance
   * @return the MetadataManager
   * @throws IllegalArgumentException if the manager is not available.
   */
  public MetadataManager getMetadataManager() {
    return (MetadataManager) getManagerByKey(METADATA_MANAGER);
  }

  /**
   * <p>Retrieves the ICP manager.</p>
   * @return The ICP manager instance.
   * @throws IllegalArgumentException if the manager is not available.
   */
  public IcpManager getIcpManager() {
    return (IcpManager)getManagerByKey(ICP_MANAGER);
  }

  /**
   * return the RemoteApi instance.
   * @return RemoteApi instance.
   * @throws IllegalArgumentException if the manager is not available.
   */
  public RemoteApi getRemoteApi() {
    return (RemoteApi) getManagerByKey(REMOTE_API);
  }

  /**
   * return the ArchivalUnitStatus instance.
   * @return ArchivalUnitStatus instance.
   * @throws IllegalArgumentException if the manager is not available.
   */
  public ArchivalUnitStatus getArchivalUnitStatus() {
    return (ArchivalUnitStatus) getManagerByKey(ARCHIVAL_UNIT_STATUS);
  }

  /**
   * Provides the COUNTER reports manager.
   * 
   * @return a CounterReportsManager with the COUNTER reports manager.
   * @throws IllegalArgumentException
   *           if the manager is not available.
   */
  public CounterReportsManager getCounterReportsManager() {
    return (CounterReportsManager) getManagerByKey(COUNTER_REPORTS_MANAGER);
  }

  /**
   * Provides the subscription manager.
   * 
   * @return a SubscriptionManager with the subscription manager.
   * @throws IllegalArgumentException
   *           if the manager is not available.
   */
  public SubscriptionManager getSubscriptionManager() {
    return (SubscriptionManager) getManagerByKey(SUBSCRIPTION_MANAGER);
  }

  /**
   * Provides the fetch time export manager.
   * 
   * @return a FetchTimeExportManager with the fetch time export manager.
   * @throws IllegalArgumentException
   *           if the manager is not available.
   */
  public FetchTimeExportManager getFetchTimeExportManager() {
    return (FetchTimeExportManager) getManagerByKey(FETCH_TIME_EXPORT_MANAGER);
  }

  /**
   * Provides the metadata database manager instance.
   * 
   * @return a MetadataDbManager with the metadata database manager instance.
   * @throws IllegalArgumentException
   *           if the manager is not available.
   */
  public MetadataDbManager getMetadataDbManager() {
    return (MetadataDbManager) getManagerByKey(METADATA_DB_MANAGER);
  }

  /**
   * return the {@link org.lockss.daemon.status.StatusService} instance
   * @return {@link org.lockss.daemon.status.StatusService} instance
   * @throws IllegalArgumentException if the manager is not available.
   */
  public StatusService getStatusService() {
    return (StatusService) getManagerByKey(STATUS_SERVICE);
  }

  /**
   * return the EntitlementRegistryClient instance.
   * @return EntitlementRegistryClient instance.
   * @throws IllegalArgumentException if the manager is not available.
   */
  public EntitlementRegistryClient getEntitlementRegistryClient() {
    return (EntitlementRegistryClient) getManagerByKey(SAFENET_MANAGER);
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
    return getLockssDaemon().getAuManager(key, au);
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
      if (desc.shouldStart(this)) {
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

  @Deprecated
  public boolean areLoadablePluginsReady() {
    return getStartupStatus().areAusStarted();
  }

//   /** Shorthand for getStartupStatus().areAusStarted() */
//   public boolean areAusStarted() {
//     return getStartupStatus().areAusStarted();
//   }

  /** Return the status of startup activities (crawling, loading
   * plugins, starting AUs */
  public ApiStatus.StartupStatus getStartupStatus() {
    try {
      return getPluginManager().getStartupStatus();
    } catch (IllegalArgumentException e) {
      // No PluginManager means no StartupStatus
      return ApiStatus.StartupStatus.NONE;
    }
  }

  protected void setConfig(Configuration config, Configuration prevConfig,
                           Configuration.Differences changedKeys) {

    super.setConfig(config, prevConfig, changedKeys);

    String proj = ConfigManager.getPlatformProject();
    isClockss = "clockss".equalsIgnoreCase(proj);
    isSafenet = "safenet".equalsIgnoreCase(proj);
    paramCrawlMode = (CrawlMode)config.getEnum(CrawlMode.class,
					       PARAM_CRAWL_MODE,
					       DEFAULT_CRAWL_MODE);
    String paramPluginsCrawler = config.get(PARAM_PLUGINS_CRAWLER,
					    DEFAULT_PLUGINS_CRAWLER);
    ServiceDescr descr = ServiceDescr.fromAbbrev(paramPluginsCrawler);
    if (descr != null) {
      pluginsCrawler = getServiceBinding(descr);
    }
    log.debug("Plugins crawler: " + descr + ": " + pluginsCrawler);
  }

  public CrawlMode getCrawlMode() {
    return paramCrawlMode;
  }

  public ServiceBinding getPluginsCrawler() {
    return pluginsCrawler;
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
