/*

Copyright (c) 2000-2017 Board of Trustees of Leland Stanford Jr. University,
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

import java.io.*;
import java.util.*;
import org.apache.commons.lang3.*;
import gnu.getopt.Getopt;
import static org.lockss.app.ManagerDescs.*;
import org.lockss.util.*;
import org.lockss.util.net.IPAddr;
import org.lockss.util.time.Deadline;
import org.lockss.util.time.TimeBase;
import org.lockss.util.time.TimeZoneUtil;
import org.lockss.alert.*;
import org.lockss.mail.*;
import org.lockss.config.*;
import org.lockss.account.*;
import org.lockss.daemon.*;
import org.lockss.daemon.status.*;
import org.lockss.plugin.*;
import org.lockss.protocol.*;
import org.lockss.truezip.*;
import org.apache.commons.collections.map.LinkedMap;
import org.apache.commons.lang3.builder.HashCodeBuilder;

/**
 * Configuration and startup of LOCKSS applications.  Application
 * configuration is contained in a {@link LockssApp.AppSpec}, generally
 * passed to {@link #startStatic(AppSpec)} or {@link #startStatic(Class<?
 * extends LockssApp>, AppSpec)}.  May be used as a base class, or can be
 * used directly.
 */
public class LockssApp {
  private static final Logger log =
    Logger.getLoggerWithInitialLevel("LockssApp",
				     Logger.getInitialDefaultLevel());

/**
 * LOCKSS is a trademark of Stanford University.  Stanford hereby grants you
 * limited permission to use the LOCKSS trademark only in connection with
 * this software, including in the User-Agent HTTP request header generated
 * by the software and provided to web servers, provided the software or any
 * output of the software is used solely for the purpose of populating a
 * certified LOCKSS cache from a web server that has granted permission for
 * the LOCKSS system to collect material.  You may not remove or delete any
 * reference to LOCKSS in the software indicating that LOCKSS is a mark owned
 * by Stanford University.  No other permission is granted you to use the
 * LOCKSS trademark or any other trademark of Stanford University.  Without
 * limiting the foregoing, if you adapt or use the software for any other
 * purpose, you must delete all references to or uses of the LOCKSS mark from
 * the software.  All good will associated with your use of the LOCKSS mark
 * shall inure to the benefit of Stanford University.
 */
  private static final String LOCKSS_USER_AGENT = "LOCKSS cache";

  private static final String PREFIX = Configuration.PREFIX + "app.";

  public static final String PARAM_TESTING_MODE = PREFIX + "testingMode";

  static final String PARAM_DAEMON_DEADLINE_REASONABLE =
    PREFIX + "deadline.reasonable.";
  static final String PARAM_DAEMON_DEADLINE_REASONABLE_PAST =
    PARAM_DAEMON_DEADLINE_REASONABLE + "past";
  static final long DEFAULT_DAEMON_DEADLINE_REASONABLE_PAST = Constants.SECOND;

  static final String PARAM_DAEMON_DEADLINE_REASONABLE_FUTURE =
    PARAM_DAEMON_DEADLINE_REASONABLE + "future";
  static final long DEFAULT_DAEMON_DEADLINE_REASONABLE_FUTURE =
    20 * Constants.WEEK;

  public static final JavaVersion MIN_JAVA_VERSION = JavaVersion.JAVA_1_8;

  public static final String PARAM_APP_EXIT_IMM = PREFIX + "exitImmediately";
  public static final boolean DEFAULT_APP_EXIT_IMM = false;

  public static final String PARAM_APP_EXIT_AFTER = PREFIX + "exitAfter";
  public static final long DEFAULT_APP_EXIT_AFTER = 0;

  public static final String PARAM_APP_EXIT_ONCE = PREFIX + "exitOnce";
  public static final boolean DEFAULT_APP_EXIT_ONCE = false;

  public static final String PARAM_DEBUG = PREFIX + "debug";
  public static final boolean DEFAULT_DEBUG = false;

  static final String PARAM_PLATFORM_VERSION =
    Configuration.PREFIX + "platform.version";

  public static final String PARAM_START_PLUGINS =
    Configuration.PREFIX + "startPlugins";
  public static final boolean DEFAULT_START_PLUGINS = false;

  private static final String PARAM_EXERCISE_DNS = PREFIX + "poundDns";
  private static final boolean DEFAULT_EXERCISE_DNS = false;

  public static final String MANAGER_PREFIX =
    Configuration.PREFIX + "manager.";

  // Parameter keys for standard managers
  public static final String WATCHDOG_SERVICE =
    managerKey(WatchdogService.class);
  public static final String MAIL_SERVICE =
    managerKey(MailService.class);
  public static final String STATUS_SERVICE =
    managerKey(StatusService.class);
  public static final String RESOURCE_MANAGER =
    managerKey(ResourceManager.class);
  public static final String RANDOM_MANAGER =
    managerKey(RandomManager.class);
  public static final String ACCOUNT_MANAGER =
    managerKey(AccountManager.class);
  public static final String KEYSTORE_MANAGER =
    managerKey(LockssKeyStoreManager.class);
  public static final String ALERT_MANAGER =
    managerKey(AlertManager.class);
  public static final String TIMER_SERVICE =
    managerKey(TimerQueue.Manager.class);
  public static final String IDENTITY_MANAGER =
    managerKey(IdentityManager.class);
  public static final String PLUGIN_MANAGER =
    managerKey(PluginManager.class);
  public static final String SYSTEM_METRICS =
    managerKey(SystemMetrics.class);
  //   public static final String REMOTE_API = "RemoteApi";
  public static final String URL_MANAGER =
    managerKey(UrlManager.class);
  public static final String CRON =
    managerKey(Cron.class);
  public static final String TRUEZIP_MANAGER =
    managerKey(TrueZipManager.class);
//   public static final String JOB_MANAGER = "JobManager";
//   public static final String JOB_DB_MANAGER = "JobDbManager";


  public static String managerKey(Class cls) {
    return cls.getName();
  }

  // Standard managers to run for all services.  They are started in this
  // order, followed by the service-specific managers specified by
  // subclasses, followed by post managers below
  private final ManagerDesc[] stdPreManagers = {
    RANDOM_MANAGER_DESC,
    RESOURCE_MANAGER_DESC,
    MAIL_SERVICE_DESC,
    ALERT_MANAGER_DESC,
    STATUS_SERVICE_DESC,
    TRUEZIP_MANAGER_DESC,
    URL_MANAGER_DESC,
    TIMER_SERVICE_DESC,
    // keystore manager must be started before any others that need to
    // access managed keystores
    KEYSTORE_MANAGER_DESC,
    JMS_MANAGER_DESC,
    // PluginManager should be here once not dependent on LockssDaemon
//     // start plugin manager after generic services
//     PLUGIN_MANAGER_DESC,
    // Job manager should be here once genericized
//     // Start the job manager.
    // JOB_MANAGER_DESC,
//     // Start the job database manager.
    // JOB_DB_MANAGER_DESC,
  };

  private final ManagerDesc[] stdPostManagers = {
    // Cron might start jobs that access other managers
    new ManagerDesc(CRON, "org.lockss.daemon.Cron"),
    // watchdog last
    new ManagerDesc(WATCHDOG_SERVICE, "org.lockss.daemon.WatchdogService"),

    // unused
//     new ManagerDesc(CLOCKSS_PARAMS, "org.lockss.clockss.ClockssParams") {
//       public boolean shouldStart() {
//         return isClockss();
//       }},
    
  };


  protected AppSpec appSpec;
  protected String bootstrapPropsUrl = null;
  protected String restConfigServiceUrl = null;
  protected List<String> propUrls = null;
  protected List<String> clusterUrls = null;
  protected String groupNames = null;

  protected boolean appInited = false;	// true after all managers inited
  protected volatile boolean appRunning = false; // true after all managers started
  protected OneShotSemaphore appRunningSem = new OneShotSemaphore();
  protected Date startDate;
  protected long appLifetime = DEFAULT_APP_EXIT_AFTER;
  protected Deadline timeToExit = Deadline.at(TimeBase.MAX);
  protected boolean isSafenet = false;

  // Map of managerKey -> manager instance. Need to preserve order so
  // managers are started and stopped in the right order.  This does not
  // need to be synchronized.
  protected LinkedMap managerMap = new LinkedMap();

  protected static LockssApp theApp;
//   private boolean isClockss;
  protected String testingMode;

  protected LockssApp() {
    theApp = this;
  }

  protected LockssApp(AppSpec spec) {
    appSpec = spec;
    theApp = this;
  }

  protected LockssApp(List<String> propUrls) {
    this.propUrls = propUrls;
    theApp = this;
  }

  protected LockssApp(List<String> propUrls, String groupNames) {
    this.propUrls = propUrls;
    this.groupNames = groupNames;
    theApp = this;
  }

  protected LockssApp(String bootstrapPropsUrl, String restConfigServiceUrl,
      List<String> propUrls, String groupNames) {
    this.bootstrapPropsUrl = bootstrapPropsUrl;
    this.restConfigServiceUrl = restConfigServiceUrl;
    this.propUrls = propUrls;
    this.groupNames = groupNames;
    theApp = this;
  }

  public void setAppSpec(AppSpec spec) {
    appSpec = spec;
  }

  public AppSpec getAppSpec() {
    return appSpec;
  }

  /** Return the LOCKSS user-agent string.
   * @return the LOCKSS user-agent string. */
  public static String getUserAgent() {
    return LOCKSS_USER_AGENT;
  }

  /** Return the current testing mode. */
  public String getTestingMode() {
    return testingMode;
  }

  /**
   * True if running as a Safenet daemon
   */
  public boolean isSafenet() {
    return isSafenet;
  }

  /** Starts the standard pre managers, then the per-app managers, then the
   * post managers.  The per-app ManagerDescs are obtained from the AppSpec
   * if present, else getAppManagerDescs() will be called.  Apps may
   * override that if non-static ManagerDescs are needed.  Or may implement
   * this method to completely override the standard manager startup. */
  protected ManagerDesc[] getManagerDescs() {
    LinkedHashSet<ManagerDesc> res = new LinkedHashSet<ManagerDesc>(50);
    Collections.addAll(res, stdPreManagers);
    if (getAppSpec().isComputeAppManagers()) {
      Collections.addAll(res, getAppManagerDescs());
    } else {
      Collections.addAll(res, getAppSpec().getAppManagers());
    }
    Collections.addAll(res, stdPostManagers);
    log.debug2("getManagerDescs: " + res);
    return res.toArray(new ManagerDesc[0]);
  }

  /** Subclasses may override to return their service-specific managers if
   * they don't/can't supply them in.the AppSpec. */
  protected ManagerDesc[] getAppManagerDescs() {
    throw new UnsupportedOperationException("App said it wanted to compute its app specs but didn't implement getAppManagerDescs()");
  }

  // General information accessors

  /**
   * True iff all managers have been inited.
   * @return true iff all managers have been inited */
  public boolean isAppInited() {
    return appInited;
  }

  /**
   * True if all managers have been started.
   * @return true iff all managers have been started */
  public boolean isAppRunning() {
    return appRunning;
  }

  /** Wait until app is running.  This must be called only from your own
   * thread (<i>eg</i>, not the startup thread.) */
  public void waitUntilAppRunning() throws InterruptedException {
    appRunningSem.waitFull(Deadline.MAX);
  }

  /**
   * True if running in debug mode (org.lockss.app.debug=true).
   * @return true iff in debug mode */
  public static boolean isDebug() {
    return CurrentConfig.getBooleanParam(PARAM_DEBUG, DEFAULT_DEBUG);
  }

  /**
   * static accessor for the LockssApp instance
   * @return the LockssApp instance
   */
  public static LockssApp getLockssApp() {
    return theApp;
  }

  /** Return the time the app started running.
   * @return the time the app started running, as a Date
   */
  public Date getStartDate() {
    if (startDate == null) {
      // this happens during testing
      startDate = TimeBase.nowDate();
    }
    return startDate;
  }

  /** Return the app name */
  public String getAppName() {
    return appSpec.getName();
  }

  /** Return a string describing the version of the app and platform */
  public String getVersionInfo() {
    String vApp = BuildInfo.getBuildInfoString();
    if (!StringUtil.isNullString(getAppName())) {
      vApp = getAppName() + " " + vApp;
    }
    PlatformVersion plat = Configuration.getPlatformVersion();
    if (plat != null) {
      vApp = vApp + ", " + plat.displayString();
    }
    return vApp;
  }

  /** Return a string describing the JVM */
  public String getJavaVersionInfo() {
    Properties props = System.getProperties();
    StringBuilder sb = new StringBuilder();
    sb.append(props.get("java.vm.vendor"));
    sb.append(", ");
    sb.append(props.get("java.vm.name"));
    sb.append(", ");
    sb.append(props.get("java.runtime.version"));
    return sb.toString();
  }

  // LockssManager accessors

  /**
   * Find a lockss manager by name.  This will need to be cast to the
   * appropriate class.
   * @param managerKey the name of the manager
   * @return the named lockss manager,
   * @throws IllegalArgumentException if the manager is not available.
   */
  public LockssManager getManagerByKey(String managerKey) {
    LockssManager mgr = (LockssManager) managerMap.get(managerKey);
    if(mgr == null) {
      throw new IllegalArgumentException("Unavailable manager:" + managerKey);
    }
    return mgr;
  }

  /**
   * Find a lockss manager by type.
   * @param mgrType the type of manager (interface, or implementation class
   * if no interface)
   * @return the named lockss manager,
   * @throws IllegalArgumentException if the manager is not available.
   */
  public <T> T getManagerByType(Class<T> mgrType) {
    return (T)getManagerByKey(managerKey(mgrType));
  }

  /**
   * Static manager accessor.  Find a lockss manager by name.  This will
   * need to be cast to the appropriate class.
   * @param managerKey the name of the manager
   * @return the named lockss manager
   * @throws IllegalArgumentException if the manager is not available.
   * @deprecated use {@link #getManagerByKeyStatic(String)}
   */
  public static LockssManager getManager(String managerKey) {
    if (theApp == null) {
      throw new NullPointerException("App has not been created");
    }
    return theApp.getManagerByKey(managerKey);
  }

  /**
   * Static manager accessor.  Find a lockss manager by name.  This will
   * need to be cast to the appropriate class.
   * @param managerKey the name of the manager
   * @return the named lockss manager,
   * @throws IllegalArgumentException if the manager is not available.
   */
  public static LockssManager getManagerByKeyStatic(String managerKey) {
    if (theApp == null) {
      throw new NullPointerException("App has not been created");
    }
    return theApp.getManagerByKey(managerKey);
  }

  /**
   * Static manager accessor.  Find a lockss manager by type.
   * @param mgrType the type of manager (interface, or implementation class
   * if no interface)
   * @return the named lockss manager,
   * @throws IllegalArgumentException if the manager is not available.
   */
  public static <T> T getManagerByTypeStatic(Class<T> mgrType) {
    return theApp.getManagerByType(mgrType);
  }

  // Standard manager accessors

  /**
   * Return the config manager instance.  Special case.
   * @return the ConfigManager
   * @throws IllegalArgumentException if the manager is not available.
   */
  public ConfigManager getConfigManager() {
    return ConfigManager.getConfigManager();
  }

  /**
   * return the alert manager instance
   * @return the AlertManager
   * @throws IllegalArgumentException if the manager is not available.
   */
  public AlertManager getAlertManager() {
    return (AlertManager)getManager(ALERT_MANAGER);
  }

  /**
   * return the SystemMetrics instance.
   * @return SystemMetrics instance.
   * @throws IllegalArgumentException if the manager is not available.
   */
  public SystemMetrics getSystemMetrics() {
    return (SystemMetrics) getManager(SYSTEM_METRICS);
  }

  /**
   * return the plugin manager instance
   * @return the PluginManager
   * @throws IllegalArgumentException if the manager is not available.
   */
  public PluginManager getPluginManager() {
    return (PluginManager) getManager(PLUGIN_MANAGER);
  }

  /**
   * return the Account Manager
   * @return AccountManager
   * @throws IllegalArgumentException if the manager is not available.
   */
  public AccountManager getAccountManager() {
    return (AccountManager) getManager(ACCOUNT_MANAGER);
  }

  /**
   * return the Random Manager
   * @return RandomManager
   * @throws IllegalArgumentException if the manager is not available.
   */
  public RandomManager getRandomManager() {
    return (RandomManager) getManager(RANDOM_MANAGER);
  }

  /**
   * return the Keystore Manager
   * @return KeystoreManager
   * @throws IllegalArgumentException if the manager is not available.
   */
  public LockssKeyStoreManager getKeystoreManager() {
    return (LockssKeyStoreManager) getManager(KEYSTORE_MANAGER);
  }

  /**
   * return the Identity Manager
   * @return IdentityManager
   * @throws IllegalArgumentException if the manager is not available.
   */
  public IdentityManager getIdentityManager() {
    return (IdentityManager) getManager(IDENTITY_MANAGER);
  }

//   /**
//    * return the RemoteApi instance.
//    * @return RemoteApi instance.
//    * @throws IllegalArgumentException if the manager is not available.
//    */
//   public RemoteApi getRemoteApi() {
//     return (RemoteApi) getManager(REMOTE_API);
//   }

//   /**
//    * return the ArchivalUnitStatus instance.
//    * @return ArchivalUnitStatus instance.
//    * @throws IllegalArgumentException if the manager is not available.
//    */
//   public ArchivalUnitStatus getArchivalUnitStatus() {
//     return (ArchivalUnitStatus) getManager(ARCHIVAL_UNIT_STATUS);
//   }

  /**
   * return TrueZipManager instance
   * @return the TrueZipManager
   * @throws IllegalArgumentException if the manager is not available.
   */
  public TrueZipManager getTrueZipManager() {
    return (TrueZipManager)getManager(TRUEZIP_MANAGER);
  }

  // Eventually wants to be here but currently specific to MetadataManager
//   /**
//    * Provides the job manager instance.
//    * 
//    * @return a JobManager with the job manager instance.
//    * @throws IllegalArgumentException
//    *           if the manager is not available.
//    */
//   public JobManager getJobManager() {
//     return (JobManager) getManager(JOB_MANAGER);
//   }

//   /**
//    * Provides the job database manager instance.
//    * 
//    * @return a JobDbManager with the job database manager instance.
//    * @throws IllegalArgumentException
//    *           if the manager is not available.
//    */
//   public JobDbManager getJobDbManager() {
//     return (JobDbManager) getManager(JOB_DB_MANAGER);
//   }


  /**
   * return the {@link org.lockss.daemon.status.StatusService} instance
   * @return {@link org.lockss.daemon.status.StatusService} instance
   * @throws IllegalArgumentException if the manager is not available.
   */
  public StatusService getStatusService() {
    return (StatusService) getManager(STATUS_SERVICE);
  }

  /**
   * return the watchdog service instance
   * @return the WatchdogService
   * @throws IllegalArgumentException if the manager is not available.
   */
  public WatchdogService getWatchdogService() {
    return (WatchdogService)getManager(WATCHDOG_SERVICE);
  }

  /**
   * return the mail manager instance
   * @return the MailService
   * @throws IllegalArgumentException if the manager is not available.
   */
  public MailService getMailService() {
    return (MailService)getManager(MAIL_SERVICE);
  }

  /**
   * return the resource manager instance
   * @return the ResourceManager
   * @throws IllegalArgumentException if the manager is not available.
  */
  public ResourceManager getResourceManager() {
    return (ResourceManager) getManager(RESOURCE_MANAGER);
  }

  // Manager loading, starting, stopping

  /**
   * Load and init the specified manager.  If the manager class is
   * specified by a config parameter and cannot be loaded, fall back to the
   * default class.
   * @param desc entry describing manager to load
   * @return the manager that has been loaded
   * @throws Exception if load fails
   */
  protected LockssManager initManager(ManagerDesc desc) throws Exception {
    String managerName = getManagerClassName(desc);
    LockssManager mgr = instantiateManager(desc);
    try {
      // call init on the service
      log.debug2("create & initService: " + managerName);
      mgr.initService(this);
      managerMap.put(desc.key, mgr);
      return mgr;
    } catch (Exception ex) {
      log.error("Unable to instantiate Lockss Manager "+ managerName, ex);
      throw(ex);
    }
  }

  protected String getManagerClassName(ManagerDesc desc) {
    String key = MANAGER_PREFIX + desc.key;
    return System.getProperty(key,
			      CurrentConfig.getParam(key, desc.defaultClass));
  }

  /** Create an instance of a LockssManager, from the configured or default
   * manager class name */
  protected LockssManager instantiateManager(ManagerDesc desc)
      throws Exception {
    String managerName = getManagerClassName(desc);
    LockssManager mgr;
    try {
      mgr = (LockssManager)makeInstance(managerName);
    } catch (ClassNotFoundException e) {
      log.warning("Couldn't load manager class " + managerName);
      if (!managerName.equals(desc.defaultClass)) {
	log.warning("Trying default manager class " + desc.defaultClass);
	mgr = (LockssManager)makeInstance(desc.defaultClass);
      } else {
	throw e;
      }
    }
    return mgr;
  }

  protected Object makeInstance(String managerClassName)
      throws ClassNotFoundException, InstantiationException,
	     IllegalAccessException {
    log.debug3("Instantiating manager class " + managerClassName);
    Class<?> mgrClass = Class.forName(managerClassName);
    return mgrClass.newInstance();
  }

  /**
   * init all of the managers that support the app.
   * @throws Exception if initialization fails
   */
  protected void initManagers() throws Exception {
    ManagerDesc[] managerDescs = getManagerDescs();
    if (managerDescs == null) {
      log.critical("No manager descs");
      return;
    }
    for(int i=0; i< managerDescs.length; i++) {
      ManagerDesc desc = managerDescs[i];
      if (desc.shouldStart()) {
	if (managerMap.get(desc.key) != null) {
	  throw new RuntimeException("Duplicate manager key: " + desc.key);
	}
	LockssManager mgr = initManager(desc);
      }
    }
    appInited = true;
    // now start the managers in the same order in which they were created
    // (managerMap is a LinkedMap)
    Iterator it = managerMap.values().iterator();
    while(it.hasNext()) {
      LockssManager lm = (LockssManager)it.next();
      try {
	log.debug3("startService: " + lm);
	lm.startService();
      } catch (Exception e) {
	log.error("Couldn't start service " + lm, e);
	// don't try to start remaining managers
	throw e;
      }
    }

    appRunning = true;
    appRunningSem.fill();
    if (getAppSpec() != null) {
      OneShotSemaphore userSem = getAppSpec().getStartedSemaphore();
      if (userSem != null) {
	userSem.fill();
      }
    }
  }

  /** Stop the app.  Currently only used in testing. */
  public void stopApp() {
    stop();
  }

  /**
   * Stop the app, by stopping the managers in the reverse order of
   * starting.
   */
  protected void stop() {
    appRunning = false;

    // stop all single managers
    List rkeys = ListUtil.reverseCopy(managerMap.asList());
    for (Iterator it = rkeys.iterator(); it.hasNext(); ) {
      String key = (String)it.next();
      LockssManager lm = (LockssManager)managerMap.get(key);
      try {
	lm.stopService();
	managerMap.remove(key);
      } catch (Exception e) {
	log.warning("Couldn't stop service " + lm, e);
      }
    }
  }

  // App start, stop

  /**
   * run the app.  Load our properties, initialize our managers, initialize
   * the plugins.
   * @throws Exception if the initialization fails
   */
  protected void startApp() throws Exception {
    startDate = TimeBase.nowDate();

    log.info(getJavaVersionInfo());
    log.info(getVersionInfo() + ": starting");

    // initialize our properties from the urls given
    initProperties();

    // repeat the version info, as we may now be logging to a different target
    // (And to include the platform version, which wasn't availabe before the
    // config was loaded.)
    log.info(getJavaVersionInfo());
    log.info(getVersionInfo() + ": starting managers");

    // startup all services
    initManagers();

    log.info("Started");
  }

  protected void systemExit(int val) {
    if (Boolean.getBoolean("org.lockss.unitTesting")) {
      log.critical("System.exit(" + val + ")");
      throw new RuntimeException("System.exit(" + val + ")");
    } else {
      System.exit(val);
    }
  }

  /**
   * init our configuration and extract any parameters we will use locally
   */
  protected void initProperties() {
    ConfigManager configMgr = ConfigManager.makeConfigManager(bootstrapPropsUrl,
	restConfigServiceUrl, propUrls, groupNames);
    configMgr.setClusterUrls(clusterUrls);

    configMgr.initService(this);
    configMgr.startService();

    // Wait for the configuration to be loaded.
    log.info("Waiting for config");

    if (!configMgr.waitConfig()) {
      log.critical("Initial config load timed out");
      systemExit(Constants.EXIT_CODE_RESOURCE_UNAVAILABLE);
    }

    log.info("Config loaded");

    if (log.isDebug3()) log.debug3(
	Configuration.loggableConfiguration(ConfigManager.getCurrentConfig(),
	    "ConfigManager.getCurrentConfig()"));

    prevExitOnce = CurrentConfig.getBooleanParam(PARAM_APP_EXIT_ONCE,
						 DEFAULT_APP_EXIT_ONCE);

    configMgr.registerConfigurationCallback(new Configuration.Callback() {
	public void configurationChanged(Configuration newConfig,
					 Configuration prevConfig,
					 Configuration.Differences changedKeys) {
	  setConfig(newConfig, prevConfig, changedKeys);
	}
      });
  }

  public Configuration getAppConfig() {
    return getAppSpec().getAppConfig();
  }

  boolean prevExitOnce = false;

  protected void setConfig(Configuration config, Configuration prevConfig,
			   Configuration.Differences changedKeys) {

    // temporary while debugging jvm DNS problem
    if (changedKeys.contains(PARAM_EXERCISE_DNS)) {
      IPAddr.setExerciseDNS(config.getBoolean(PARAM_EXERCISE_DNS,
					      DEFAULT_EXERCISE_DNS));
    }

    testingMode = config.get(PARAM_TESTING_MODE);

    if (changedKeys.contains(PARAM_DAEMON_DEADLINE_REASONABLE)) {
      long maxInPast =
        config.getTimeInterval(PARAM_DAEMON_DEADLINE_REASONABLE_PAST,
                               DEFAULT_DAEMON_DEADLINE_REASONABLE_PAST);
      long maxInFuture =
        config.getTimeInterval(PARAM_DAEMON_DEADLINE_REASONABLE_FUTURE,
                               DEFAULT_DAEMON_DEADLINE_REASONABLE_FUTURE);
      Deadline.setReasonableDeadlineRange(maxInPast, maxInFuture);
    }

    long life = config.getTimeInterval(PARAM_APP_EXIT_AFTER,
				       DEFAULT_APP_EXIT_AFTER);
    if (life != appLifetime) {
      // lifetime changed
      appLifetime = life;
      if (life == 0) {
	// zero is forever
	timeToExit.expireAt(TimeBase.MAX);
      } else {
	// compute new randomized deadline relative to start time
	long start = getStartDate().getTime();
	long min = start + life - life/4;
	long max = start + life + life/4;
	long prevExp = timeToExit.getExpirationTime();
	if (!(min <= prevExp && prevExp <= max)) {
	  // previous end of life is not within new range, so change timer
	  if (min <= TimeBase.nowMs()) {
	    // earliest time is earlier than now.  make random interval at
	    // least an hour long to prevent all daemons from exiting too
	    // close to each other.
	    min = TimeBase.nowMs();
	    max = Math.max(max, min + Constants.HOUR);
	  }
	  Deadline tmp = Deadline.atRandomRange(min, max);
	  timeToExit.expireAt(tmp.getExpirationTime());
	}
      }
    }

    // THIS MUST BE LAST IN THIS ROUTINE
    boolean exitOnce = config.getBoolean(PARAM_APP_EXIT_ONCE,
					 DEFAULT_APP_EXIT_ONCE);
    if (!prevExitOnce && exitOnce) {
      timeToExit.expire();
    } else {
      prevExitOnce = exitOnce;
    }
  }

  /** Start a LockssApp to run the managers specified by the spec.
   * @param spec an AppSpec specifying the application name, additional
   * configuration, and managers to start.
   * @returns the app class instance (possibly later, if keepRunning is
   * used)
   */
  public static <T> LockssApp startStatic(AppSpec spec) {
    return startStatic(LockssApp.class, spec);
  }

  /** Start the specified app class to run the managers specified by the
   * spec.
   * @param appClass the subclass of LockssApp to instantiate and invoke
   * @param spec an AppSpec specifying the application name, additional
   * configuration, and managers to start.
   * @returns the app class instance (possibly later, if keepRunning is
   * used)
   */
  public static <T> LockssApp startStatic(Class<? extends LockssApp> appClass,
					  AppSpec spec) {
    LockssApp app;
    try {
      app = appClass.newInstance();
    } catch (Exception e) {
      throw new RuntimeException("Couldn't instantiate " + appClass, e);
    }
    app.setAppSpec(spec);
    app.newStart();
    return app;
  }

  protected void newStart() {
    if (appSpec == null) {
      log.critical("newStart() requires an AppSpec");
      throw new IllegalArgumentException("startApp() requires an AppSpec");
    }
    log.info("Starting app: " + appSpec.getName());
    JavaVersion minVer = appSpec.getMinJavaVersion();
    if (minVer != null && !SystemUtils.isJavaVersionAtLeast(minVer)) {
      System.err.println("LOCKSS requires at least Java " + minVer +
                         ", this is " + SystemUtils.JAVA_VERSION +
                         ", exiting.");
      systemExit(Constants.EXIT_CODE_JAVA_VERSION);
    }

    if (!TimeZoneUtil.isBasicTimeZoneDataAvailable()) {
      System.err.println("Basic time zone data unavailable, exiting.");
      System.exit(Constants.EXIT_INVALID_TIME_ZONE_DATA);
    }

    // No easy way to enforce maxVer.  There is no
    // SystemUtils.isJavaVersionAtMost(), and while JavaVersion.atLeast()
    // is public, there appears to be no way to get ahold of the current
    // JavaVersion, as below, because JavaVersion.getJavaVersion() is not
    // public

//     JavaVersion maxVer = appSpec.getMaxJavaVersion();
//     if (maxVer != null &&
// 	!(maxVer.atLeast(JavaVersion.getJavaVersion(SystemUtils.JAVA_SPECIFICATION_VERSION)))) {

//       System.err.println("LOCKSS requires at most Java " + maxVer +
//                          ", this is " + SystemUtils.JAVA_VERSION +
//                          ", exiting.");
//       systemExit(Constants.EXIT_CODE_JAVA_VERSION);
//     }

    StartupOptions opts = getStartupOptions(appSpec.getArgs());

    setSystemProperties();

    bootstrapPropsUrl = opts.getBootstrapPropsUrl();
    restConfigServiceUrl = opts.getRestConfigServiceUrl();
    propUrls = opts.getPropUrls();
    clusterUrls = opts.getClusterUrls();
    groupNames = opts.getGroupNames();

    try {
      startApp();
      // raise priority after starting other threads, so we won't get
      // locked out and fail to exit when told.
      Thread.currentThread().setPriority(Thread.NORM_PRIORITY + 2);

    } catch (ResourceUnavailableException e) {
      log.error("Exiting because required resource is unavailable", e);
      systemExit(Constants.EXIT_CODE_RESOURCE_UNAVAILABLE);
      return;                           // compiler doesn't know that
                                        // System.exit() doesn't return
    } catch (Throwable e) {
      log.error("Exception thrown during startup", e);
      systemExit(Constants.EXIT_CODE_EXCEPTION_IN_MAIN);
      return;                           // compiler doesn't know that
                                        // System.exit() doesn't return
    }
    if (CurrentConfig.getBooleanParam(PARAM_APP_EXIT_IMM,
                                      DEFAULT_APP_EXIT_IMM)) {
      try {
        stop();
      } catch (RuntimeException e) {
        // ignore errors stopping app
      }
      systemExit(Constants.EXIT_CODE_NORMAL);
    }
    if (appSpec.isKeepRunning()) {
      keepRunning();
      log.info("Exiting because time to die");
      systemExit(Constants.EXIT_CODE_NORMAL);
    } else {
      log.debug("Returning after starting app");
    }
  }


  protected void keepRunning() {
    while (!timeToExit.expired()) {
      try {
	log.debug("Will exit at " + timeToExit);
	timeToExit.sleep();
      } catch (InterruptedException e) {
	// no action
      }
    }
  }

  protected static StartupOptions getStartupOptions(String[] args) {
    return new StartupOptions().parse(args);
  }

  /** ImageIO gets invoked on user-supplied content (by (nyi) format
   * conversion and PDFBox).  Disable native code libraries to avoid any
   * possibility of exploiting vulnerabilities */
  public static String IMAGEIO_DISABLE_NATIVE_CODE =
    "com.sun.media.imageio.disableCodecLib";

  // static so can run before instantiating this class, which causes more
  // classes to be loaded
  protected static void setSystemProperties() {
    System.setProperty(IMAGEIO_DISABLE_NATIVE_CODE, "true");
  }


  /**
   * LockssManager descriptor.  Specifies a LockssManager that should be
   * subjected to the normal lifecycle: create, initService(),
   * startService(), ... , stopService().  Contains a key/value pair; the
   * key, by convention, is the typename of the manager (i.e., the fully
   * qualified name of an interface or base class, or implementation class
   * if nothing less specific is available).  The value is the fully
   * qualified name of the implementation class to instantiate.
   */
  public static class ManagerDesc {
    String key;		// hash key and config param name
    String defaultClass;      // default class name (or factory class name)

    /** Create a ManagerDesc from a String key and class name */
    public ManagerDesc(String key, String defaultClass) {
      this.key = key;
      this.defaultClass = defaultClass;
    }

    /** Create a ManagerDesc whose typename is the same as the
     * implementation class name */
    public ManagerDesc(String defaultClass) {
      this(defaultClass, defaultClass);
    }

    /** Create a ManagerDesc from a type object and class name */
    public ManagerDesc(Class type, String defaultClass) {
      this(managerKey(type), defaultClass);
    }

    public String getKey() {
      return key;
    }

    public String getDefaultClass() {
      return defaultClass;
    }

    /** Return true iff the manager should be started.  Allows runtime
     * determination of manager inclusion by implementing in an anonymous
     * inner class in a ManagerDesc declaration. */
    public boolean shouldStart() {
      return true;
    }

    @Override
    public int hashCode() {
      HashCodeBuilder hcb = new HashCodeBuilder();
      hcb.append(key);
      hcb.append(defaultClass);
      return hcb.toHashCode();
    }

    @Override
    public boolean equals(Object obj) {
      if (this == obj) {
	return true;
      }
      if (obj instanceof ManagerDesc) {
	ManagerDesc md = (ManagerDesc)obj;
	if (StringUtil.equalStrings(this.key, md.getKey())
	    && StringUtil.equalStrings(defaultClass, md.getDefaultClass())) {
	  return true;
	}
      }
      return false;
    }

    public String toString() {
      StringBuilder sb = new StringBuilder();
      sb.append("[md: ");
      if (!StringUtil.equalStrings(key, defaultClass)) {
	sb.append(key);
	sb.append(": ");
      }
      sb.append(defaultClass);
      sb.append("]");
      return sb.toString();
    }
  }

  /**
   * Command line args startup options container.
   * Supports these arguments:<dl>
   * <dt>-b url</dt>
   *     <dd>Load bootstrap properties from url</dd>
   * <dt>-c url</dt>
   *     <dd>The URL of a REST Configuration service</dd>
   * <dt>-p url<i>n</i></dt>
   *     <dd>Load properties from url<i>n</i></dd>
   * <dt>-p url1 -p url2;url3;url4</dt>
   *     <dd>Load properties from url1 AND from one of</dd>
   *     <dt>(url2 | url3 | url4)</dt>
   * <dd>-g group_name[;group_2;group_3]
   *     Set the daemon groups.  Multiple groups separated by semicolon.</dd>
   * <dt>-s</dt>
   *     <dd>Log the security providers.</dd>
   * <dt>-x dir</dt>
   *     <dd>Load properties from XML files in directory dir.</dd>
   * </dl>
   */
  public static class StartupOptions {

    public static final String OPTION_BOOTSTRAP_PROPURL = "-b";
    public static final String OPTION_REST_CONFIG_SERVICE_URL = "-c";
    public static final String OPTION_PROPURL = "-p";
    public static final String OPTION_CLUSTERURL = "-l";
    public static final String OPTION_GROUP = "-g";
    public static final String OPTION_LOG_CRYPTO_PROVIDERS = "-s";
    public static final String OPTION_XML_PROP_DIR = "-x";

    private String bootstrapPropsUrl;
    private String restConfigServiceUrl;
    private String groupNames;
    private List<String> propUrls = new ArrayList<String>();
    // clusterUrls is a subset of propUrls
    private List<String> clusterUrls = new ArrayList<String>();

    public StartupOptions() {
    }

    @Deprecated
    public StartupOptions(String bootstrapPropsUrl, String restConfigServiceUrl,
	List<String> propUrls, String groupNames) {
      this.bootstrapPropsUrl = bootstrapPropsUrl;
      this.restConfigServiceUrl = restConfigServiceUrl;
      this.propUrls = propUrls;
      this.groupNames = groupNames;
    }

    public String getBootstrapPropsUrl() {
      return bootstrapPropsUrl;
    }

    public String getRestConfigServiceUrl() {
      return restConfigServiceUrl;
    }

    public List<String> getPropUrls() {
      return propUrls;
    }

    public List<String> getClusterUrls() {
      return clusterUrls;
    }

    public String getGroupNames() {
      return groupNames;
    }

    public StartupOptions parse(String[] args) {
      log.debug("Cmdline args: " + ListUtil.fromArray(args));
      for (int i = 0; i < args.length; i++) {
	if (args[i].equals(OPTION_GROUP) && i < args.length - 1) {
	  groupNames = args[++i];
	} else if (args[i].equals(OPTION_PROPURL) && i < args.length - 1) {
	  // TODO: If not available, keep selecting prop files to load
	  // until one is loaded, or the list is exhausted.
	  // For now, just select one at random.
	  Vector<String> v = StringUtil.breakAt(args[++i], ';', -1, true, true);
	  int idx = (int)(Math.random() * v.size());
	  if (log.isDebug3())
	    log.debug3("getStartupOptions(): propUrl: " + v.get(idx));
	  propUrls.add(v.get(idx));
	} else if (args[i].equals(OPTION_CLUSTERURL) && i < args.length - 1) {
	  String clustUrl = args[++i];
	  propUrls.add(clustUrl);
	  clusterUrls.add(clustUrl);
	} else if (args[i].equals(OPTION_LOG_CRYPTO_PROVIDERS)) {
	  SslUtil.logCryptoProviders(true);
	} else if (args[i].equals(OPTION_XML_PROP_DIR) && i < args.length - 1) {
	  // Handle a directory with XML files.
	  String optionXmlDir = args[++i];
	  File xmlDir = new File(optionXmlDir);
	  if (log.isDebug3())
	    log.debug3("getStartupOptions(): xmlDir: " + xmlDir);
	  try {
	    for (String xmlFileName :
		   FileUtil.listDirFilesWithExtension(xmlDir, "xml")) {
	      if (log.isDebug3())
		log.debug3("getStartupOptions(): xmlFileName: " + xmlFileName);
	      propUrls.add(new File(xmlDir, xmlFileName).getPath());
	    }
	  } catch (IOException ioe) {
	    log.error("Cannot process XML directory option '"
		      + OPTION_XML_PROP_DIR + " " +
		      optionXmlDir + "', ignoring: " + ioe.toString());
	  }
	} else if (args[i].equals(OPTION_BOOTSTRAP_PROPURL)
		   && i < args.length - 1) {
	  // Handle bootstrap properties URL.
	  bootstrapPropsUrl = args[++i];
	  if (log.isDebug3()) {
	    log.debug3("getStartupOptions(): bootstrapPropsUrl: " +
		       bootstrapPropsUrl);
	  }
	  propUrls.add(bootstrapPropsUrl);
	} else if (args[i].equals(OPTION_REST_CONFIG_SERVICE_URL)
		   && i < args.length - 1) {
	  // Handle the REST configuration service URL.
	  restConfigServiceUrl = args[++i];
	  if (log.isDebug3()) {
	    log.debug3("getStartupOptions(): " +
		       "restConfigServiceUrl: " + restConfigServiceUrl);
	  }
	}
      }
      return this;
    }
  }

  /**
   * Specification of components and configuration of a lockss application.
   * Consists of:<ul>
   *
   * <li>Application name, used in messages</li>
   *
   * <li>Command line arguments.  See {@link LockssApp.StartupOptions}</li>
   *
   * <li>List of {@link LockssApp.Managerdescs}s, describing the managers
   * needed in addition to the standard ones.</li>
   *
   * <li>Configuration parameters to be appended to the configuration
   * loaded from ConfigService and files</li>
   *
   * <li>Minimum required Java version</li>
   *
   * <li>Maximum required Java version (not supported)</li>
   *
   * <li>Whether startStatic should return after startup, or sleep until
   * told to exit</li>
   *
   * <li>An optional {@link org.lockss.util.OneShotSemaphore} that is
   * filled when the app has been started.</li>
   *
   * </ul>
   * <br/>This is a builder: the setters are chainable.
   */
  public static class AppSpec {
    private String name;
    private String[] args;
    private ManagerDesc[] appManagers;
    private boolean isComputeAppManagers = false;
    private Configuration appConfig;
    private boolean isKeepRunning = false;
    private JavaVersion minJavaVersion = JavaVersion.JAVA_1_8;
//     private JavaVersion maxJavaVersion;
    private OneShotSemaphore startedSem;

    /** Set the name */
    public AppSpec setName(String name) {
      this.name = name;
      return this;
    }

    /** Set the command line args
     * @param args array of individual args
     */
    public AppSpec setArgs(String[] args) {
      this.args = args;
      return this;
    }

    /** Set the app-specific managers
     * @param mgrs array of ManagerDescs, in order in which managers should
     * be started
     */
    public AppSpec setAppManagers(ManagerDesc[] mgrs) {
      appManagers = mgrs;
      return this;
    }

    /** Declare that the app wishes getAppManagerDescs() to be called to
     * compute its list of app managers, rather than finding them in the
     * AppSpec */
    public AppSpec setComputeAppManagers(boolean val) {
      isComputeAppManagers = val;
      return this;
    }

    /** Set the app-specific Configuration */
    public AppSpec setAppConfig(Configuration config) {
      appConfig = config;
      return this;
    }

    /** Set the app-specific Configuration */
    public AppSpec setAppConfig(Properties props) {
      appConfig = ConfigManager.fromPropertiesUnsealed(props);
      return this;
    }

    /** Add to the app-specific Configuration */
    public AppSpec addAppConfig(String key, String val) {
      if (appConfig == null) {
	appConfig = ConfigManager.newConfiguration();
      }
      appConfig.put(key, val);
      return this;
    }

    /** Set the keepRunning flag */
    public AppSpec setKeepRunning(boolean val) {
      this.isKeepRunning = val;
      return this;
    }

    /** Set the started semaphone.  Filled when app is started.  Useful
     * with setKeepRunning(true), in which case {@link
     * #startStatic(AppSpec)} doesn't return and the only way to get one's
     * hands on the app instance is with {@link LockssApp#getLockssApp()},
     * which can't be called until the startup has begun. */
    public AppSpec setStartedSem(OneShotSemaphore sem) {
      this.startedSem = sem;
      return this;
    }

    /** Return the application name */
    public String getName() {
      return name;
    }

    /** Return the command line args, an array of Strings */
    public String[] getArgs() {
      return args;
    }

    /** Set the minimum Java version */
    public AppSpec setMinJavaVersion(JavaVersion min) {
      minJavaVersion = min;
      return this;
    }

    /** Set the maximum Java version */
//     public AppSpec setMaxJavaVersion(JavaVersion max) {
//       maxJavaVersion = max;
//       return this;
//     }

    /** Return the array of app-specific ManagerDescs */
    public ManagerDesc[] getAppManagers() {
      return appManagers == null ? new ManagerDesc[0] : appManagers;
    }

    /** True if getAppManagerDescs() should be called to compute the array
     * of app manager, false if they should be obtained from the AppSpec */
    public boolean isComputeAppManagers() {
      return isComputeAppManagers;
    }

    /** Return the minimum Java version */
    public JavaVersion getMinJavaVersion() {
      return minJavaVersion;
    }

//     /** Return the maximum Java version */
//     public JavaVersion getMaxJavaVersion() {
//       return maxJavaVersion;
//     }

    /** Return the app-specific Configuration */
    public Configuration getAppConfig() {
      return appConfig;
    }

    /** Return true if startStatic() should deley return until told to
     * exit */
    public boolean isKeepRunning() {
      return isKeepRunning;
    }

    public OneShotSemaphore getStartedSemaphore() {
      return startedSem;
    }
  }
}
