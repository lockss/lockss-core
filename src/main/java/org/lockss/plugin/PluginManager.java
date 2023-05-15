  /*

  Copyright (c) 2000-2021 Board of Trustees of Leland Stanford Jr. University,
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
  package org.lockss.plugin;

  import java.io.*;
  import java.nio.file.*;
  import java.net.*;
  import java.security.KeyStore;
  import java.util.*;
  import java.util.jar.*;
  import java.util.regex.*;
  import java.util.stream.*;

  import org.apache.commons.collections4.map.*;
  import org.apache.commons.lang3.tuple.*;
  import org.lockss.alert.*;
  import org.lockss.app.*;
  import org.lockss.config.*;
  import org.lockss.crawler.*;
  import org.lockss.daemon.*;
  import org.lockss.db.DbException;
  import org.lockss.plugin.definable.DefinablePlugin;
  import org.lockss.plugin.base.*;
  import org.lockss.poller.PollSpec;
  import org.lockss.repository.*;
  import org.lockss.util.rest.repo.model.Artifact;
  import org.lockss.util.rest.exception.LockssRestException;
  import org.lockss.state.AuState;
  import org.lockss.util.*;
  import org.lockss.util.io.FileUtil;
  import org.lockss.util.time.Deadline;
  import org.lockss.util.time.TimeBase;
  import org.lockss.util.time.TimeUtil;
  import org.lockss.util.rest.status.*;
  import javax.jms.*;

  import static java.util.UUID.randomUUID;
  import static org.lockss.crawler.CrawlEvent.KEY_COOKIE;

  /**
   * Plugin global functionality
   *
   * @author  TAL
   * @version 0.0
   */
  public class PluginManager
    extends BaseLockssDaemonManager implements ConfigurableManager {

    public static final String PREFIX = Configuration.PREFIX + "plugin.";

    public static final String PARAM_AU_TREE = Configuration.PREFIX + "au";

  /** The location on the platform to store downloaded plugins once they have
      been verified for loading. */
  static final String PARAM_PLUGIN_LOCATION =
    Configuration.PLATFORM + "pluginDir";
  static final String DEFAULT_PLUGIN_LOCATION = "plugins";

  /** List of plugins to load at startup.  Normally used only in testing. */
  static final String PARAM_PLUGIN_REGISTRY = PREFIX + "registry";

  /** List of jars from which to load all plugins at startup.  Normally
   * used only in testing. */
  static final String PARAM_PLUGIN_REGISTRY_JARS = PREFIX + "registryJars";

  /** Pattern describing members of jars listed in {@value
   * #PARAM_PLUGIN_REGISTRY_JARS} that are plugin files. */
  static final String PARAM_PLUGIN_MEMBER_PATTERN =
    PREFIX + "registryMemberPattern";
  static final String DEFAULT_PLUGIN_MEMBER_PATTERN =
    "^(.*Plugin)\\.(?:xml|class)$";

  /** List of plugins not to load, or to remove if already loaded. */
  static final String PARAM_PLUGIN_RETRACT = PREFIX + "retract";

  /** Set to true for at least one config reload period to enqueue crawls
   * of all plugin registries */
  static final String PARAM_CRAWL_PLUGINS_ONCE = PREFIX + "crawlRegistriesOnce";
  static final boolean DEFAULT_CRAWL_PLUGINS_ONCE = false;

  static final String PARAM_REMOVE_STOPPED_AUS = PREFIX + "removeStoppedAus";
  static final boolean DEFAULT_REMOVE_STOPPED_AUS = true;

  /** Global list of plugin registry URLs. */
  static final String PARAM_PLUGIN_REGISTRIES = PREFIX + "registries";

    /** List of user-specified plugin registry URLs. */
    public static final String PARAM_USER_PLUGIN_REGISTRIES =
      PREFIX + "userRegistries";
    /** If true, intern AUIDs in a string pool to save space */
    public static final String PARAM_USE_AUID_POOL =
      PREFIX + "useAuIdStringPool";
    public static final boolean DEFAULT_USE_AUID_POOL = true;
    /** If true, default list of plugin registries from prop server is used
     * in addition to user-specified registries */
    public static final String PARAM_USE_DEFAULT_PLUGIN_REGISTRIES =
      PREFIX + "useDefaultRegistries";
    public static final boolean DEFAULT_USE_DEFAULT_PLUGIN_REGISTRIES = true;

  // Not Implemented
//   /** If true, default plugin signature keystore is used in addition to
//    * user-specified keystore */
//   public static final String PARAM_USE_DEFAULT_KEYSTORE =
//     PREFIX + "useDefaultKeystore";
//   public static final boolean DEFAULT_USE_DEFAULT_KEYSTORE = true;

  /** If true (the default), plugins that appear both in a loadable plugin
   * registry jar and on the local classpath will be loaded from the
   * downloaded registry jar.  If false such plugins will be loaded from
   * the local classpath.  This is useful in development, where
   * lockss-plugins.jar is on the classpath.  Normal true setting simulates
   * production behavior, loading plugins from registry.  Set false to test
   * plugins from the local build. */
  static final String PARAM_PREFER_LOADABLE_PLUGIN =
    PREFIX + "preferLoadable";
  static final boolean DEFAULT_PREFER_LOADABLE_PLUGIN = true;

  /** Common prefix of plugin keystore params */
  static final String KEYSTORE_PREFIX = PREFIX + "keystore.";

  /** The location of a Java keystore to use for verifying
      loadable plugins.  Defaults to the keystore packaged with the daemon. */
  static final String PARAM_KEYSTORE_LOCATION = KEYSTORE_PREFIX + "location";
  static final String DEFAULT_KEYSTORE_LOCATION =
    "org/lockss/plugin/lockss.keystore";
  /** The password for the loadable plugin verification keystore.
      (Not needed, generally should not be set.) */
  static final String PARAM_KEYSTORE_PASSWORD = KEYSTORE_PREFIX + "password";
  static final String DEFAULT_KEYSTORE_PASSWORD = null;

  /** Common prefix of user-specified plugin keystore params */
  static final String USER_KEYSTORE_PREFIX = PREFIX + "userKeystore.";

  /** The location of a Java JKS keystore to use for verifying
      loadable plugins (optional). */
  public static final String PARAM_USER_KEYSTORE_LOCATION =
    USER_KEYSTORE_PREFIX + "location";
  /** The password to use when opening the loadable plugin
      verification keystore (optional). */
  public static final String PARAM_USER_KEYSTORE_PASSWORD =
    USER_KEYSTORE_PREFIX + "password";

  /** If true, accept plugins signed by otherwise-valid certificates that
   * are expired or not yet valid. */
  static final String PARAM_ACCEPT_EXPIRED_CERTS = 
    PREFIX + "acceptExpiredCertificates";
  static final boolean DEFAULT_ACCEPT_EXPIRED_CERTS = true;

    /** The amount of time to wait when processing loadable plugins.
        This process delays the start of AUs, so the timeout should not
        be too long. */
    public static final String PARAM_PLUGIN_LOAD_TIMEOUT =
      PREFIX + "load.timeout";
    public static final long DEFAULT_PLUGIN_LOAD_TIMEOUT =
      Constants.MINUTE;
    /** Disable the automatic caching in URLConnection.  Setting this true
     * will prevent open file descriptors from piling up each time a new
     * version of a plugin is loaded, but may impact performance. */
    public static final String PARAM_DISABLE_URL_CONNECTION_CACHE =
      PREFIX + "disableURLConnectionCache";
    public static final boolean DEFAULT_DISABLE_URL_CONNECTION_CACHE = false;
    /** If true, all configured AUs are started on startup.  If false, AUs
     * are started on demand. */
    public static final String PARAM_START_ALL_AUS =
      PREFIX + "startAllAus";
    // Various tests assume no on-demand AU creation so this has to be true
    // for now.  (Symptom is that they use AUIDs that can't be parsed by
    // on-demand code)
    public static final boolean DEFAULT_START_ALL_AUS = true;
    /** When creating an on-demand AU, if no config is available in config DB
     * or tdb, the AUs config will be inferred from the AUID if this is true,
     * else the creation will fail.  "True" will not lead to correct behavior
     * for all AUs. */
    public static final String PARAM_INFER_CONFIG_FROM_AUID =
      PREFIX + "inferConfigFromAuid";
    public static final boolean DEFAULT_INFER_CONFIG_FROM_AUID = true;
    /** If true, all plugins loaded as they become available.  If false,
     * plugins are loaded as needed for AUs started on demand.  Not fully
     * implemented. */
    // TK
    public static final String PARAM_LOAD_ALL_PLUGINS =
      PREFIX + "loadAllPlugins";
    public static final boolean DEFAULT_LOAD_ALL_PLUGINS = true;
    /** If true, AU configurations may appear in any config file.  This was
     * always the case prior to 1.55, setting this true restores that
     * behavior. */
    public static final String PARAM_ALLOW_GLOBAL_AU_CONFIG =
      PREFIX + "allowGlobalAuConfig";
    public static final boolean DEFAULT_ALLOW_GLOBAL_AU_CONFIG = false;

    /** If true, when a new version of an existing plugin is loaded, all its
     * AUs will be restarted. */
    public static final String PARAM_RESTART_AUS_WITH_NEW_PLUGIN =
      PREFIX + "restartAusWithNewPlugin";
    public static final boolean DEFAULT_RESTART_AUS_WITH_NEW_PLUGIN = true;
    /** The max amount of time to wait after stopping a set of AUs whose
     * plugin has been replaced by a new version, before restarting them. */
    public static final String PARAM_AU_RESTART_MAX_SLEEP =
      PREFIX + "auRestartMaxSleep";
    public static final long DEFAULT_AU_RESTART_MAX_SLEEP = 5 * Constants.SECOND;
    /** The amount of time, per AU, to wait after stopping a set of AUs whose
     * plugin has been replaced by a new version, before restarting them. */
    public static final String PARAM_PER_AU_RESTART_SLEEP =
      PREFIX + "perAuRestartSleep";
    public static final long DEFAULT_PER_AU_RESTART_SLEEP = 500;
    /** The type of plugin we prefer to load, if both are present.
        Can be either "class" or "xml" (case insensitive) */
    public static final String PARAM_PREFERRED_PLUGIN_TYPE =
      PREFIX + "preferredType";
    public static final String DEFAULT_PREFERRED_PLUGIN_TYPE =
      "xml";

  static final String AU_SEARCH_SET_PREFIX = PREFIX + "auSearch.";

  /** If true, use V2 repo index to search for cached URL */
  public static final String PARAM_AU_SEARCH_USE_V2_REPO =
    AU_SEARCH_SET_PREFIX + "searchUseV2Repo";
    public static final boolean DEFAULT_AU_SEARCH_USE_V2_REPO = true;

  /** Step function returns the desired size of an AU search set, given the
   * number of AUs in the search set */
  public static final String PARAM_AU_SEARCH_CACHE_SIZE =
    AU_SEARCH_SET_PREFIX + "cacheSize";
    public static final String DEFAULT_AU_SEARCH_CACHE_SIZE =
      "[1,0],[2,1],[5,2],[10,3],[50,10],[200,20]";

  /** Step function returns the desired size of the per-host recent 404
   * cache, given the number of AUs in the search set */
  public static final String PARAM_AU_SEARCH_404_CACHE_SIZE =
    AU_SEARCH_SET_PREFIX + "404CacheSize";
    public static final String DEFAULT_AU_SEARCH_404_CACHE_SIZE =
      "[10,10],[1000,100],[5000,200]";

  /** If zero, all failed findCachedUrl() searches (though non-empty AU
   * sets) will be cached.  If >0, only those that had to search on disk
   * at least that many times will be cached. */
  public static final String PARAM_AU_SEARCH_MIN_DISK_SEARCHES_FOR_404_CACHE =
    AU_SEARCH_SET_PREFIX + "minDiskSearchesFor404Cache";
    public static final int DEFAULT_AU_SEARCH_MIN_DISK_SEARCHES_FOR_404_CACHE = 2;

  /** Prevents concurrent searches for the same URL.  If findCachedUrl() is
   * called on a URL while another thread is already searching for that
   * URL, second and successive thread will wait for and return result of
   * first thread. */
  public static final String PARAM_PREVENT_CONCURRENT_SEARCHES =
    AU_SEARCH_SET_PREFIX + "preventConcurrent";
    public static final boolean DEFAULT_PREVENT_CONCURRENT_SEARCHES = true;
    /** Root of TitleSet definitions.  */
    public static final String PARAM_TITLE_SETS =
      Configuration.PREFIX + "titleSet";

  static final String TITLE_SET_PARAM_CLASS = "class";
  static final String TITLE_SET_PARAM_NAME = "name";

  static final String TITLE_SET_CLASS_XPATH = "xpath";
  static final String TITLE_SET_XPATH_XPATH = "xpath";

  static final String TITLE_SET_CLASS_ALL_AUS = "AllAus";
  static final String TITLE_SET_CLASS_ACTIVE_AUS = "ActiveAus";
  static final String TITLE_SET_CLASS_INACTIVE_AUS = "InactiveAus";


    // prefix for non-plugin AU params
    public static final String AU_PARAM_RESERVED = "reserved";
    // per AU params known to and processed by daemon, not plugin
    public static final String AU_PARAM_DISABLED = AU_PARAM_RESERVED + ".disabled";
    public static final String AU_PARAM_REPOSITORY = AU_PARAM_RESERVED + ".repository";
    public static final String AU_PARAM_DISPLAY_NAME = AU_PARAM_RESERVED + ".displayName";
    /**
     * If true, use a web service, instead of the repository, to get the Archival
     * Unit URLs and the content of each URL.
     */
    // tk eliminate
    public static final String PARAM_AU_CONTENT_FROM_WS =
        PREFIX + "auContentFromWs";
  public static final boolean DEFAULT_AU_CONTENT_FROM_WS = false;

    /**
     * The parameters of the web service used, instead of the repository, to
     * obtain the list of URLs for an archival unit, if so configured.
     */
    public static final String URL_LIST_WS_PREFIX = PARAM_AU_CONTENT_FROM_WS
        + ".urlListWs.";
    public static final String PARAM_URL_LIST_WS_USER_NAME =
        URL_LIST_WS_PREFIX + "userName";
    public static final String PARAM_URL_LIST_WS_PASSWORD =
        URL_LIST_WS_PREFIX + "password";
    public static final String PARAM_URL_LIST_WS_ADDRESS_LOCATION =
        URL_LIST_WS_PREFIX + "addressLocation";
    public static final String PARAM_URL_LIST_WS_TARGET_NAMESPACE =
        URL_LIST_WS_PREFIX + "targetNameSpace";
    public static final String PARAM_URL_LIST_WS_SERVICE_NAME =
        URL_LIST_WS_PREFIX + "serviceName";
    public static final String PARAM_URL_LIST_WS_TIMEOUT_VALUE =
        URL_LIST_WS_PREFIX + "timeoutValue";
  public static final int DEFAULT_URL_LIST_WS_TIMEOUT_VALUE = 600;

    /**
     * The Repository REST web service location.
     */
    public static final String PARAM_URL_LIST_REST_SERVICE_LOCATION =
        URL_LIST_WS_PREFIX + "restServiceLocation";
    /**
     * The parameters of the web service used, instead of the repository, to
     * obtain the content of a URL for an archival unit, if so configured.
     */
    public static final String URL_CONTENT_WS_PREFIX = PARAM_AU_CONTENT_FROM_WS
        + ".urlContentWs.";
    public static final String PARAM_URL_CONTENT_WS_USER_NAME =
        URL_CONTENT_WS_PREFIX + "userName";
    public static final String PARAM_URL_CONTENT_WS_PASSWORD =
        URL_CONTENT_WS_PREFIX + "password";
    public static final String PARAM_URL_CONTENT_WS_ADDRESS_LOCATION =
        URL_CONTENT_WS_PREFIX + "addressLocation";
    public static final String PARAM_URL_CONTENT_WS_TARGET_NAMESPACE =
        URL_CONTENT_WS_PREFIX + "targetNameSpace";
    public static final String PARAM_URL_CONTENT_WS_SERVICE_NAME =
        URL_CONTENT_WS_PREFIX + "serviceName";
    public static final String PARAM_URL_CONTENT_WS_TIMEOUT_VALUE =
        URL_CONTENT_WS_PREFIX + "timeoutValue";
  public static final int DEFAULT_URL_CONTENT_WS_TIMEOUT_VALUE = 600;

    /**
     * The Repository REST web service location.
     */
    public static final String PARAM_URL_CONTENT_REST_SERVICE_LOCATION =
        URL_CONTENT_WS_PREFIX + "restServiceLocation";
    /**
     * The parameters of the web service used, instead of the repository, to
     * obtain the artifact of a URL, if so configured.
     */
    public static final String URL_ARTIFACT_WS_PREFIX = PARAM_AU_CONTENT_FROM_WS
        + ".urlArtifactWs.";
    public static final String PARAM_URL_ARTIFACT_WS_USER_NAME =
        URL_ARTIFACT_WS_PREFIX + "userName";
    public static final String PARAM_URL_ARTIFACT_WS_PASSWORD =
        URL_ARTIFACT_WS_PREFIX + "password";
    public static final String PARAM_URL_ARTIFACT_WS_ADDRESS_LOCATION =
        URL_ARTIFACT_WS_PREFIX + "addressLocation";
    public static final String PARAM_URL_ARTIFACT_WS_TARGET_NAMESPACE =
        URL_ARTIFACT_WS_PREFIX + "targetNameSpace";
    public static final String PARAM_URL_ARTIFACT_WS_SERVICE_NAME =
        URL_ARTIFACT_WS_PREFIX + "serviceName";
    public static final String PARAM_URL_ARTIFACT_WS_TIMEOUT_VALUE =
        URL_ARTIFACT_WS_PREFIX + "timeoutValue";
  public static final int DEFAULT_URL_ARTIFACT_WS_TIMEOUT_VALUE = 600;

    /**
     * The Repository REST web service location.
     */
    public static final String PARAM_URL_ARTIFACT_REST_SERVICE_LOCATION =
        URL_ARTIFACT_WS_PREFIX + "restServiceLocation";

    public static final List NON_USER_SETTABLE_AU_PARAMS =
      Collections.unmodifiableList(new ArrayList());

    static final String DEFAULT_CONFIGURABLE_PLUGIN_NAME =
      DefinablePlugin.class.getName();

    private static Logger log = Logger.getLogger();

    private boolean useDefaultPluginRegistries =
      DEFAULT_USE_DEFAULT_PLUGIN_REGISTRIES;
    private ConfigManager configMgr;
    private AlertManager alertMgr;
    private RepositoryManager repoMgr;
    private File pluginDir = null;
    private AuOrderComparator auComparator = new AuOrderComparator();

  private final Attributes.Name LOADABLE_PLUGIN_ATTR =
    new Attributes.Name("Lockss-Plugin");

  // List of names of plugins not to load
  List retract = null;

    // maps plugin key(not id) to plugin
    private Map<String,Plugin> pluginMap =
      Collections.synchronizedMap(new HashMap());
    // maps auid to AU
    private Map<String,ArchivalUnit> auMap =
      Collections.synchronizedMap(new HashMap());
    // List of all aus sorted by title.  The UI relies on this behavior.
    private List<ArchivalUnit> auList = null;
    // maps host to collections of AUs.  Used to quickly locate candidate AUs
    // for incoming URLs.  Each collection is sorted in AU order (for proxy
    // manifest index display).
    private Map<String,AuSearchSet> hostAus = new HashMap<String,AuSearchSet>();
    private Set<String> inactiveAuIds =
        Collections.synchronizedSet(new HashSet<String>());
    private List<AuEventHandler> auEventHandlers = new ArrayList<>();
    // Counters for AUs restarted due to new plugin loaded
    private int numAusRestarting = 0;
    private int numFailedAuRestarts = 0;
    // Plugin registry processing
    private Map cuNodeVersionMap = Collections.synchronizedMap(new HashMap());
    // Map of plugin key to PluginInfo
    private Map pluginfoMap = Collections.synchronizedMap(new HashMap());
    private Map<String, Plugin> internalPlugins = new HashMap<String, Plugin>();
    private boolean prevCrawlOnce = false;
    private KeyStore keystore;
    private JarValidator jarValidator;
    private boolean keystoreInited = false;
    private boolean loadablePluginsReady = false;
    private boolean preferLoadablePlugin = DEFAULT_PREFER_LOADABLE_PLUGIN;
    private long registryTimeout = DEFAULT_PLUGIN_LOAD_TIMEOUT;
    private boolean paramRestartAus = DEFAULT_RESTART_AUS_WITH_NEW_PLUGIN;
  private static boolean paramUseAuidPool = DEFAULT_USE_AUID_POOL;
    private boolean paramAllowGlobalAuConfig = DEFAULT_ALLOW_GLOBAL_AU_CONFIG;
    private long paramAuRestartMaxSleep = DEFAULT_AU_RESTART_MAX_SLEEP;
    private long paramPerAuRestartSleep = DEFAULT_PER_AU_RESTART_SLEEP;
    private boolean paramDisableURLConnectionCache =
      DEFAULT_DISABLE_URL_CONNECTION_CACHE;
    private boolean acceptExpiredCertificates = DEFAULT_ACCEPT_EXPIRED_CERTS;
    private boolean paramAuSearchUseV2Repo = DEFAULT_AU_SEARCH_USE_V2_REPO;
    private IntStepFunction auSearchCacheSizeFunc =
      new IntStepFunction(DEFAULT_AU_SEARCH_CACHE_SIZE);
    private IntStepFunction auSearch404CacheSizeFunc =
      new IntStepFunction(DEFAULT_AU_SEARCH_404_CACHE_SIZE);
    private int paramMinDiskSearchesFor404Cache =
      DEFAULT_AU_SEARCH_MIN_DISK_SEARCHES_FOR_404_CACHE;
    private boolean paramPreventConcurrentSearches =
      DEFAULT_PREVENT_CONCURRENT_SEARCHES;
    private boolean paramStartAllAus = DEFAULT_START_ALL_AUS;
    private boolean paramInferConfigFromAuId = DEFAULT_INFER_CONFIG_FROM_AUID;
    private boolean paramLoadAllPlugins = DEFAULT_LOAD_ALL_PLUGINS;
    private Map titleMap = null;
    private List allTitles = null;
  // lock for above
  Object titleMonitor = new Object();
    private List allTitleConfigs = null;
    private Map<String,TitleSet> titleSetMap;
    private TreeSet<TitleSet> titleSets;
  // lock for AU additions/deletions
  Object auAddDelLock = new Object();

  private static Map<String,String> configurablePluginNameMap = new HashMap();
  static {
    configurablePluginNameMap.put(".*ExplodedPlugin$",
				  "org.lockss.plugin.exploded.ExplodedPlugin");
  }
  public static final int PREFER_XML_PLUGIN = 0;
  public static final int PREFER_CLASS_PLUGIN = 1;

  public static final String KEY_URL= "url";
  public static final String KEY_CALLER_ID= "callerId";

    public PluginManager() {
    }

  /* ------- LockssManager implementation ------------------ */
    /**
     * start the plugin manager.
     * @see org.lockss.app.LockssManager#startService()
     */
    public void startService() {
      super.startService();
      configMgr = getDaemon().getConfigManager();

      // Initialize the configuration database, if necessary.
      configMgr.deferredConfigDbInit();

      alertMgr = getDaemon().getAlertManager();
      repoMgr = getDaemon().getRepositoryManager();
      // Initialize the plugin directory.
      initPluginDir();
      configureDefaultTitleSets();
      PluginStatus.register(getDaemon(), this);
      // watch for changes to plugin registry AUs and AUs with stems in hostAus
      registerAuEventHandler(myAuEventHandler);
      if (enableJmsNotifications) {
        startJms();
      }

      if (paramDisableURLConnectionCache) {
        // Up through Java 1.6, even after a ClassLoader is freed, and even
        // if its JarFile is explicitly closed first, the JarFile remains
        // open, using a file descriptor.  This causes open files to
        // accumulate as new versions of plugins are loaded, possibly
        // eventually leading to Too many open files.  Disabling the JarFile
        // cache prevents this from happening.
        try {
    // setDefaultUseCaches() is improperly an instance method, so need to
    // create an instance
    URLConnection foo = new URL("http://example.com/").openConnection();
    log.debug("Disabling URLConnection cache");
    foo.setDefaultUseCaches(false);
        } catch (IOException e) {
    log.warning("Couldn't disable URLConnection cache", e);
        }
      }
      triggerTitleSort();
    }

    /**
     * stop the plugin manager
     * @see org.lockss.app.LockssManager#stopService()
     */
    public void stopService() {
      // tk - checkpoint if nec.
      for (Plugin plugin : pluginMap.values()) {
        plugin.stopPlugin();
      }
      auEventHandlers = new ArrayList<AuEventHandler>();
      PluginStatus.unregister(getDaemon());
      unregisterAuEventHandler(myAuEventHandler);
      super.stopService();
    }

    /**
     * Start loadable plugins, ensure that they are properly loaded, and
     * then start all configured AUs.  This is called by LockssApp
     * after starting all the managers, and ensures everything happens
     * in the right order.
     */
    public void startLoadablePlugins() throws DbException {
      if (log.isDebug3())
        log.debug3("loadablePluginsReady = " + loadablePluginsReady);
      if (loadablePluginsReady) {
        return;
      }

      Configuration config = CurrentConfig.getCurrentConfig();
      if (log.isDebug3())
        log.debug3("config.keySet().size() = " + config.keySet().size());
      waitForRepo();
      log.debug("Initializing loadable plugin registries before starting AUs");
      initLoadablePluginRegistries(getPluginRegistryUrls(config));
      synchStaticPluginList(config);
      if (log.isDebug3())
        log.debug3("Calling configureAllArchivalUnitsAndLoadTheirPlugins()");
      configureAllArchivalUnits();
      loadablePluginsReady = true;
      if (log.isDebug3())
        log.debug3("loadablePluginsReady = " + loadablePluginsReady);
    }

  public void setLoadablePluginsReady(boolean val) {
    loadablePluginsReady = val;
  }

    public boolean getLoadablePluginsReady() {
      return loadablePluginsReady;
    }

    List getPluginRegistryUrls(Configuration config) {
      final String DEBUG_HEADER = "getPluginRegistryUrls(): ";
      if (log.isDebug3()) log.debug3(DEBUG_HEADER
    + "useDefaultPluginRegistries = " + useDefaultPluginRegistries);
      if (useDefaultPluginRegistries) {
        return ListUtil.append(config.getList(PARAM_PLUGIN_REGISTRIES),
             config.getList(PARAM_USER_PLUGIN_REGISTRIES));
      } else {
        return config.getList(PARAM_USER_PLUGIN_REGISTRIES);
      }
    }

    public boolean areAusStarted() {
      return getDaemon().areAusStarted();
    }

    public boolean areAusStartedOrStartOnDemand() {
      return areAusStarted() || isStartAusOnDemand();
    }

    @Override
    public void setConfig(Configuration config, Configuration oldConfig,
        Configuration.Differences changedKeys) {

      if (changedKeys.contains(PREFIX)) {
        registryTimeout = config.getTimeInterval(PARAM_PLUGIN_LOAD_TIMEOUT,
                   DEFAULT_PLUGIN_LOAD_TIMEOUT);

        paramAllowGlobalAuConfig =
    config.getBoolean(PARAM_ALLOW_GLOBAL_AU_CONFIG,
          DEFAULT_ALLOW_GLOBAL_AU_CONFIG);
        paramRestartAus =
    config.getBoolean(PARAM_RESTART_AUS_WITH_NEW_PLUGIN,
          DEFAULT_RESTART_AUS_WITH_NEW_PLUGIN);
        paramPerAuRestartSleep =
    config.getTimeInterval(PARAM_PER_AU_RESTART_SLEEP,
               DEFAULT_PER_AU_RESTART_SLEEP);
        paramAuRestartMaxSleep =
    config.getTimeInterval(PARAM_AU_RESTART_MAX_SLEEP,
               DEFAULT_AU_RESTART_MAX_SLEEP);

        paramUseAuidPool =
    config.getBoolean(PARAM_USE_AUID_POOL, DEFAULT_USE_AUID_POOL);

        preferLoadablePlugin = config.getBoolean(PARAM_PREFER_LOADABLE_PLUGIN,
                   DEFAULT_PREFER_LOADABLE_PLUGIN);

        paramDisableURLConnectionCache =
    config.getBoolean(PARAM_DISABLE_URL_CONNECTION_CACHE,
          DEFAULT_DISABLE_URL_CONNECTION_CACHE);

        acceptExpiredCertificates =
    config.getBoolean(PARAM_ACCEPT_EXPIRED_CERTS,
          DEFAULT_ACCEPT_EXPIRED_CERTS);

        // must set retract before loadablePluginsReady is true as
        // retrievePlugin() may be called before that
        if (changedKeys.contains(PARAM_PLUGIN_RETRACT)) {
    retract = config.getList(PARAM_PLUGIN_RETRACT);
        }

        // If the keystore or password has changed, update.
        if (changedKeys.contains(KEYSTORE_PREFIX) ||
      changedKeys.contains(USER_KEYSTORE_PREFIX)) {
    keystoreInited = false;
    initKeystore(configMgr.getCurrentConfig());
        }

        if (changedKeys.contains(AU_SEARCH_SET_PREFIX)) {
          paramAuSearchUseV2Repo =
            config.getBoolean(PARAM_AU_SEARCH_USE_V2_REPO,
                              DEFAULT_AU_SEARCH_USE_V2_REPO);
    String func = config.get(PARAM_AU_SEARCH_CACHE_SIZE,
           DEFAULT_AU_SEARCH_CACHE_SIZE);
    auSearchCacheSizeFunc = new IntStepFunction(func);
    func = config.get(PARAM_AU_SEARCH_404_CACHE_SIZE,
           DEFAULT_AU_SEARCH_404_CACHE_SIZE);
    auSearch404CacheSizeFunc = new IntStepFunction(func);
    synchronized (hostAus) {
      for (Map.Entry<String,AuSearchSet> ent : hostAus.entrySet()) {
        ent.getValue().setConfig(config, oldConfig, changedKeys);
      }
    }
    paramMinDiskSearchesFor404Cache =
      config.getInt(PARAM_AU_SEARCH_MIN_DISK_SEARCHES_FOR_404_CACHE,
        DEFAULT_AU_SEARCH_MIN_DISK_SEARCHES_FOR_404_CACHE);
    paramPreventConcurrentSearches =
      config.getBoolean(PARAM_PREVENT_CONCURRENT_SEARCHES,
            DEFAULT_PREVENT_CONCURRENT_SEARCHES);
        }

        useDefaultPluginRegistries =
    config.getBoolean(PARAM_USE_DEFAULT_PLUGIN_REGISTRIES,
          DEFAULT_USE_DEFAULT_PLUGIN_REGISTRIES);

        paramStartAllAus =
    config.getBoolean(PARAM_START_ALL_AUS, DEFAULT_START_ALL_AUS);

        paramInferConfigFromAuId =
    config.getBoolean(PARAM_INFER_CONFIG_FROM_AUID,
          DEFAULT_INFER_CONFIG_FROM_AUID);

        paramLoadAllPlugins =
    config.getBoolean(PARAM_LOAD_ALL_PLUGINS, DEFAULT_LOAD_ALL_PLUGINS);

        enableJmsNotifications = config.getBoolean(PARAM_ENABLE_JMS_NOTIFICATIONS,
               DEFAULT_ENABLE_JMS_NOTIFICATIONS);
        notificationTopic = config.get(PARAM_JMS_NOTIFICATION_TOPIC,
               DEFAULT_JMS_NOTIFICATION_TOPIC);
        clientId = config.get(PARAM_JMS_CLIENT_ID, DEFAULT_JMS_CLIENT_ID);
      }

      // Process any changed TitleSets
      if (changedKeys.contains(PARAM_TITLE_SETS)) {
        configureTitleSets(config);
      }

      // Don't load or start other plugins until the daemon is running.
      if (loadablePluginsReady) {
        boolean crawlOnce = config.getBoolean(PARAM_CRAWL_PLUGINS_ONCE,
                DEFAULT_CRAWL_PLUGINS_ONCE);
        if (!prevCrawlOnce && crawlOnce) {
    queuePluginRegistryCrawls();
    prevCrawlOnce = true;
        } else {
    prevCrawlOnce = crawlOnce;
        }

        // Process loadable plugin registries.
        if (changedKeys.contains(PARAM_PLUGIN_REGISTRIES) ||
      changedKeys.contains(PARAM_USER_PLUGIN_REGISTRIES) ||
      changedKeys.contains(PARAM_USE_DEFAULT_PLUGIN_REGISTRIES)) {
    initLoadablePluginRegistries(getPluginRegistryUrls(config));
        }

        // Process the built-in plugin registry.
        if (changedKeys.contains(PARAM_PLUGIN_REGISTRY) ||
      changedKeys.contains(PARAM_PLUGIN_REGISTRY_JARS) ||
      changedKeys.contains(PARAM_PLUGIN_RETRACT)) {
    synchStaticPluginList(config);
        }
      }
    }

    @Override
    public void auConfigChanged(String auid) {
      if (auid == null || isStartAusOnDemand()) {
        // Don't create AUs on config change in on-demand mode
        return;
      }
      try {
        AuConfiguration auConfiguration =
    configMgr.retrieveArchivalUnitConfiguration(auid);
        if (auConfiguration == null) {
    log.warning("Received auConfigChanged message for AU with no configuration data: " + auid);
    return ;
        }
        log.debug("Starting or reconfiguring AU due to AU config changed notification: " + auid);
        startOrReconfigureAu(auConfiguration,
           SkipConfigCondition.ConfigUnchanged);
      } catch (DbException dbe) {
        log.critical("Error getting Archival Unit configuration: Not starting/reconfiguring AU from auConfigChanged message",
         dbe);
        return;
      } catch (LockssRestException lre) {
        log.critical("Error getting Archival Unit configuration: Not starting/reconfiguring AU from auConfigChanged message",
         lre);
        return;
      }
    }

    @Override
    public void auConfigRemoved(String auid) {
      // Do delete AUs on config change even in on-demand mode
      if (auid == null) {
        return;
      }
      ArchivalUnit au = getAuFromIdIfExists(auid);
      if (au != null) {
        log.debug("Stopping AU due to AU config removed notification: " + auid);
        stopAu(au, AuEvent.forAu(au, AuEvent.Type.Delete));
      }
    }

    public int getAuSearchCacheSize(int x) {
      return auSearchCacheSizeFunc.getValue(x);
    }

    public int getAuSearch404CacheSize(int x) {
      return auSearch404CacheSizeFunc.getValue(x);
    }

  private enum SkipConfigCondition {ConfigUnchanged, AuRunning};

    /**
     * Configures all the Archival Units.
     */
    private void configureAllArchivalUnits() {
      Collection<AuConfiguration> auConfigurations = null;

      if (isStartAusOnDemand()) {
        log.debug("In on-demand mode, Not starting AUs");
        return;
      }

      // Get the configurations of all the Archival Units.
      try {
        auConfigurations = configMgr.retrieveAllArchivalUnitConfiguration();
      } catch (DbException dbe) {
        log.critical(
      "Error getting Archival Unit configurations: Not starting AUs",
      dbe);
        return;
      } catch (LockssRestException lre) {
        log.critical(
      "Error getting Archival Unit configurations: Not starting AUs",
      lre);
        return;
      } catch (IOException ioe) {
        log.critical(
      "Error getting Archival Unit configurations: Not starting AUs",
      ioe);
        return;
      }

      if (log.isDebug3())
        log.debug3("auConfigurations.size() = " + auConfigurations.size());

      // Loop over each Archival Unit configuration.
      for (AuConfiguration auConfiguration : auConfigurations) {
        if (log.isDebug3()) log.debug3("auConfiguration = " + auConfiguration);

        startOrReconfigureAu(auConfiguration,
           SkipConfigCondition.ConfigUnchanged);
      }
    }

    private void startOrReconfigureAu(AuConfiguration auConfiguration,
              SkipConfigCondition scc) {
      String auId = auConfiguration.getAuId();
      String pluginKey = pluginIdFromAuId(auId);

      synchronized (auAddDelLock) {
        // Check whether the load of the plugin of this Archival Unit has not
        // been successful.
        if (!ensurePluginLoaded(pluginKey)) {
    // Yes: Do not configure this Archival Unit.
    log.warning("Plugin " + pluginKey
          + " not loaded, not configuring its AUs");
    return;
        }

        String auKey = PluginManager.auKeyFromAuId(auId);
        ArchivalUnit curAu = auMap.get(auId);

        // Convert the Archival Unit configuration to a Configuration object.
        Configuration auConf =
    AuConfigurationUtils.toUnprefixedConfiguration(auConfiguration);

        if (auConf.getBoolean(AU_PARAM_DISABLED, false)) {
    if (curAu != null) {
      if (log.isDebug2()) log.debug2("Deactivating AU: " + auKey);
      deactivateAuOnly(curAu);
    } else {
            inactiveAuIds.add(auId);
          }
    return;
        }

        switch (scc) {
        case ConfigUnchanged:
    if (curAu != null && auConf.equals(curAu.getConfiguration())) {
      if (log.isDebug3())
        log.debug3("AU already configured, not reconfiguring: " + auId);
      return;
    }
    break;
        case AuRunning:
    if (auMap.containsKey(auId)) {
      if (log.isDebug3())
        log.debug3("AU already running, not reconfiguring: " + auKey);
      return;
    }
    log.debug2("Retrying previously unstarted AU id: " + auId);
    break;
        }

        try {
    if (log.isDebug2()) log.debug2("Configuring AU id: " + auKey);

    // Get the plugin of this Archival Unit.
    Plugin plugin = getPlugin(pluginKey);

    // Check the consistency of the Archival Unit configuration.
    try {
      String genAuid = generateAuId(plugin, auConf);
      if (!auId.equals(genAuid)) {
        log.warning("Generated AUID " + genAuid +
        " does not match stored AUID " + auId +
        ". Proceeding anyway.");
      }
    } catch (RuntimeException e) {
      log.warning("Not configuring probable non-AU.  " +
            "Can't generate AUID from config: " + auConf);
      return;
    }

    // Configure this Archival Unit.
    configureAu(plugin, auConf, auId);
    inactiveAuIds.remove(generateAuId(pluginKey, auKey));
        } catch (ArchivalUnit.ConfigurationException e) {
    log.error("Failed to configure AU " + auId, e);
        } catch (Exception e) {
    log.error("Unexpected exception configuring AU " + auKey, e);
        }
      }
    }

    // Called to (try to) start AUs configured for changed plugins, that
    // didn't previously start (either because the plugin didn't exist, or
    // the AU didn't successfully start with the old definition)
    private void configurePlugins(Collection<String> pluginKeys) {
      configurePlugins(pluginKeys, SkipConfigCondition.AuRunning);
    }

    private void configurePlugins(Collection<String> pluginKeys,
          SkipConfigCondition scc) {
      if (log.isDebug2()) log.debug2("pluginKeys = " + pluginKeys);
      Map<String, List<AuConfiguration>> pluginsAuConfs = null;

      try {
        pluginsAuConfs =
      configMgr.retrieveAllPluginsAusConfigurations(pluginKeys);
      } catch (Exception e) {
        log.critical("Error getting AU configurations for plugins: "
      + "Not starting AUs", e);
        return;
      }

      for (String pluginKey : pluginKeys) {
        log.debug2("Configuring plugin key: " + pluginKey);
        // Get from the database the configuration entries for this plugin.
        Configuration pluginConf = ConfigManager.newConfiguration();

        // Get all the Archival Unit configurations for this plugin.
        List<AuConfiguration> pluginAus = pluginsAuConfs.get(pluginKey);
        if (pluginAus != null) {
    // Loop through each Archival Unit configuration found.
    for (AuConfiguration auConfiguration : pluginAus) {
      startOrReconfigureAu(auConfiguration, scc);
    }
        }
      }
    }

    private void configureDefaultTitleSets() {
      if (titleSets == null || titleSets.isEmpty()) {
        TreeSet<TitleSet> list = new TreeSet<TitleSet>();
        list.add(new TitleSetAllAus(getDaemon()));
        list.add(new TitleSetActiveAus(getDaemon()));
        list.add(new TitleSetInactiveAus(getDaemon()));
        installTitleSets(list);
      }
    }

    private void configureTitleSets(Configuration config) {
      TreeSet<TitleSet> list = new TreeSet<TitleSet>();
      Configuration allSets = config.getConfigTree(PARAM_TITLE_SETS);
      for (Iterator iter = allSets.nodeIterator(); iter.hasNext(); ) {
        String id = (String)iter.next();
        Configuration setDef = allSets.getConfigTree(id);
        try {
    TitleSet ts = createTitleSet(setDef);
    if (ts != null) {
      if (log.isDebug2()) {
        log.debug2("Adding TitleSet: " + ts);
      }
      list.add(ts);
    } else {
      log.warning("Null TitleSet created from: " + setDef);
    }
        } catch (RuntimeException e) {
    log.warning("Error creating TitleSet from: " + setDef, e);
        }
      }
      installTitleSets(list);
    }

    private void installTitleSets(TreeSet<TitleSet> sets) {
      Map map = new HashMap();
      for (TitleSet ts : sets) {
        map.put(ts.getId(), ts);
      }
      titleSets = sets;
      titleSetMap = map;
      log.debug2(titleSets.size() + " titlesets");
    }

    private TitleSet createTitleSet(Configuration config) {
      String cls = config.get(TITLE_SET_PARAM_CLASS);
      String name = config.get(TITLE_SET_PARAM_NAME);
      if (cls.equalsIgnoreCase(TITLE_SET_CLASS_XPATH)) {
        return TitleSetXpath.create(getDaemon(), name,
            config.get(TITLE_SET_XPATH_XPATH));
      }
      if (cls.equalsIgnoreCase(TITLE_SET_CLASS_ALL_AUS)) {
        return new TitleSetAllAus(getDaemon());
      }
      if (cls.equalsIgnoreCase(TITLE_SET_CLASS_ACTIVE_AUS)) {
        return new TitleSetActiveAus(getDaemon());
      }
      if (cls.equalsIgnoreCase(TITLE_SET_CLASS_INACTIVE_AUS)) {
        return new TitleSetInactiveAus(getDaemon());
      }
      return null;
    }

    /** Return the TitleSet id to {@link org.lockss.daemon.TitleSet}
     * mapping */
    public Map<String,TitleSet> getTitleSetMap() {
      return (titleSetMap != null) ? titleSetMap : Collections.EMPTY_MAP;
    }

    /** Return the TitleSets, in display order */
    public Collection<TitleSet> getTitleSets() {
      return (titleSets != null) ? titleSets : Collections.EMPTY_LIST;
    }

  /**
   * Convert plugin property key to plugin class name.
   * @param key the key
   * @return the plugin name
   */
  public static String pluginNameFromKey(String key) {
    return StringUtil.replaceString(key, "|", ".");
  }

  /**
   * Convert plugin class name to key suitable for property file.
   * @param className the class name
   * @return the plugin key
   */
  public static String pluginKeyFromName(String className) {
    return StringUtil.replaceString(className, ".", "|");
  }

  /**
   * Convert plugin id to key suitable for property file.  Plugin id is
   * currently the same as plugin class name, but that may change.
   * @param id the plugin id
   * @return String the plugin key
   */
  public static String pluginKeyFromId(String id) {
    // tk - needs to do real mapping from IDs obtained from all available
    // plugins.
    return StringUtil.replaceString(id, ".", "|");
  }

  /**
   * Return the plugin's key
   */
  public static String getPluginKey(Plugin plugin) {
    return pluginKeyFromId(plugin.getPluginId());
  }

  /**
   * Return the unique identifier for an AU based on its plugin id and
   * definitional config params.
   * @param pluginId plugin id (with . not escaped)
   * @param auDefProps Properties with values for all definitional AU params
   * @return The unique identifier for the AU.
   */
  public static String generateAuId(String pluginId, Properties auDefProps) {
    return generateAuId(pluginId,
			PropUtil.propsToCanonicalEncodedString(auDefProps));
  }

  /**
   * Return the unique identifier for an AU based on its plugin and full
   * set of config params..
   * @param plugin the Plugin
   * @param auConfig either the entire AU Configuration or one containing
   * at least all the definitional params.
   * @return The unique identifier for the AU.
   */
  public static String generateAuId(Plugin plugin, Configuration auConfig) {
    return generateAuId(plugin.getPluginId(),
			PluginManager.defPropsFromProps(plugin, auConfig));
  }

  /**
   * Return the unique identifier for an AU based on its plugin id and key
   * constructed from the definitional config params.
   * @param pluginId plugin id (with . not escaped)
   * @param auKey The result of
   * PropUtil#propsToCanonicalEncodedString(Properties) applied to the AU's
   * definitional config params
   * @return The unique identifier for the AU.
   */
  public static String generateAuId(String pluginId, String auKey) {
    String id = pluginKeyFromId(pluginId)+"&"+auKey;
    if (paramUseAuidPool) {
      return StringPool.AUIDS.intern(id);
    } else {
      return id;
    }
  }

  public static Properties defPropsFromProps(Plugin plug,
					     Map<String,String> auConfigProps) {
    Properties res = new Properties();
    for (ConfigParamDescr descr : plug.getAuConfigDescrs()) {
      if (descr.isDefinitional()) {
	String key = descr.getKey();
	String val = auConfigProps.get(key);
	if (val == null) {
	  throw new IllegalArgumentException(key + " is null in: " + auConfigProps);
	}
	res.setProperty(key, val);
      }
    }
    return res;
  }

  public static Properties defPropsFromProps(Plugin plug,
					     Configuration auConfig) {
    Properties res = new Properties();
    for (ConfigParamDescr descr : plug.getAuConfigDescrs()) {
      if (descr.isDefinitional()) {
	String key = descr.getKey();
	String val = auConfig.get(key);
	if (val == null) {
	  throw new IllegalArgumentException(key + " is null in: " + auConfig);
	}
	res.setProperty(key, val);
      }
    }
    return res;
  }

  public static String auKeyFromAuId(String auid) {
    int pos = auid.indexOf("&");
    if (pos < 0) {
      throw new IllegalArgumentException("Illegal AuId: " + auid);
    }
    return auid.substring(pos + 1);
  }

  public static String pluginIdFromAuId(String auid) {
    int pos = auid.indexOf("&");
    if (pos < 0) {
      throw new IllegalArgumentException("Illegal AuId: " + auid);
    }
    return auid.substring(0, pos);
  }

  public static String pluginNameFromAuId(String auid) {
    return pluginNameFromKey(pluginIdFromAuId(auid));
  }

  public static String configKeyFromAuId(String auid) {
    return StringUtil.replaceFirst(auid, "&", ".");
  }

  public static String auConfigPrefix(String auid) {
    return PARAM_AU_TREE + "." + PluginManager.configKeyFromAuId(auid);
  }

  public static String auIdFromConfigPrefixAndKey(String configPrefixAndKey) {
    return configPrefixAndKey.substring(
	(PluginManager.PARAM_AU_TREE + ".").length()).replaceFirst("\\.", "&");
  }

    private void configurePlugin(String pluginKey, Configuration pluginConf,
               SkipConfigCondition scc) {
      if (!ensurePluginLoaded(pluginKey)) {
        log.warning("Plugin " + pluginKey
        + " not loaded, not configuring its AUs");
        return;
      }
      List auList = ListUtil.fromIterator(pluginConf.nodeIterator());
      auList = CollectionUtil.randomPermutation(auList);
      nextAU:
      for (Iterator iter = auList.iterator(); iter.hasNext(); ) {
        String auKey = (String)iter.next();
        String auId = generateAuId(pluginKey, auKey);
        try {
    Configuration auConf = pluginConf.getConfigTree(auKey);
    if (auConf.getBoolean(AU_PARAM_DISABLED, false)) {
      // tk should actually remove AU?
      if (log.isDebug2())
        log.debug2("Not configuring disabled AU id: " + auKey);
      if (auMap.get(auId) == null) {
        // don't add to inactive if it's still running
        inactiveAuIds.add(auId);
      }
      continue nextAU;
    }
    switch (scc) {
    case ConfigUnchanged:
      break;
    case AuRunning:
      if (auMap.containsKey(auId)) {
        if (log.isDebug3())
          log.debug3("AU already running, not reconfiguring: "
         + auKey);
        continue nextAU;
      }
      log.debug2("Retrying previously unstarted AU id: " + auId);
      break;
    }
    if (log.isDebug2()) log.debug2("Configuring AU id: " + auKey);
    Plugin plugin = getPlugin(pluginKey);
    try {
      String genAuid = generateAuId(plugin, auConf);
      if (!auId.equals(genAuid)) {
        log.warning("Generated AUID " + genAuid +
        " does not match stored AUID " + auId +
        ". Proceeding anyway.");
      }
    } catch (RuntimeException e) {
      log.warning("Not configuring probable non-AU.  " +
            "Can't generate AUID from config: " + auConf);
      continue nextAU;
    }
    configureAu(plugin, auConf, auId);
    inactiveAuIds.remove(generateAuId(pluginKey, auKey));
        } catch (ArchivalUnit.ConfigurationException e) {
    log.error("Failed to configure AU " + auId, e);
        } catch (Exception e) {
    log.error("Unexpected exception configuring AU " + auKey, e);
        }
      }
    }

    void configureAu(Plugin plugin, Configuration auConf, String auId)
        throws ArchivalUnit.ConfigurationException {
      if (log.isDebug2()) {
        log.debug2("plugin = " + plugin);
        log.debug2("auConf = " + auConf);
        log.debug2("auId = " + auId);
      }

      Configuration oldConfig = null;
      try {
        ArchivalUnit oldAu = (ArchivalUnit)auMap.get(auId);
        if (oldAu != null) {
    oldConfig = oldAu.getConfiguration();
    if (auConf.equals(oldConfig)) {
      // Don't bother if the config is the same.  (This happens the
      // first time config is loaded after AU created via UI.)
      return;
    }
        }
        ArchivalUnit au = plugin.configureAu(auConf, oldAu);
        if (oldAu != null && oldAu != au) {
    String msg = "Plugin created new AU: " + au +
      ", should have reconfigured old AU: " + oldAu;
    throw new ArchivalUnit.ConfigurationException(msg);
        }
        if (!auId.equals(au.getAuId())) {
    String msg = "Configured AU has unexpected AUID: " + au.getAuId() +
      ", expected: "+ auId;
    throw new ArchivalUnit.ConfigurationException(msg);
        }
        if (!isAuContentFromWs()) {
    try {
      getDaemon().startOrReconfigureAuManagers(au, auConf);
    } catch (Exception e) {
      throw new ArchivalUnit.ConfigurationException(
          "Couldn't configure AU managers", e);
    }
        }
        if (oldAu != null) {
    log.debug("Reconfigured AU " + au);
    signalAuEvent(au,
            AuEvent.forAu(au, AuEvent.Type.Reconfig).setOldConfiguration(oldConfig));
        } else {
    log.debug("Configured AU " + au);
    putAuInMap(au);
    signalAuEvent(au, AuEvent.forAu(au, AuEvent.Type.StartupCreate));
        }
      } catch (ArchivalUnit.ConfigurationException e) {
        throw e;
      } catch (Exception e) {
        log.error("Error configuring AU", e);
        throw new
    ArchivalUnit.ConfigurationException("Unexpected error creating AU", e);
      }
    }

    ArchivalUnit createAu(Plugin plugin, Configuration auConf, AuEvent event)
        throws ArchivalUnit.ConfigurationException {
      final String DEBUG_HEADER = "createAu(): ";
      if (log.isDebug2()) {
        log.debug2(DEBUG_HEADER + "plugin = " + plugin);
        log.debug2(DEBUG_HEADER + "auConf = " + auConf);
        log.debug2(DEBUG_HEADER + "event = " + event);
      }
      String auid = null;
      ArchivalUnit oldAu = null;
      try {
        auid = generateAuId(plugin, auConf);
        if (log.isDebug3()) log.debug3(DEBUG_HEADER + "auid = " + auid);
        if (!isAuContentFromWs()) {
    oldAu = getAuFromIdIfExists(auid);
    if (log.isDebug3()) log.debug3(DEBUG_HEADER + "oldAu = " + oldAu);
        }
      } catch (Exception e) {
        // no action.  Bad/missing config value might cause getAuFromId() to
        // throw.  It will be caught soon when creating the AU; for now we
        // can assume it means the AU doesn't already exist.
      }
      if (oldAu != null) {
        inactiveAuIds.remove(oldAu.getAuId());
        throw new ArchivalUnit.ConfigurationException("Cannot create that AU because it already exists: " + oldAu.getName());
      }
      try {
        ArchivalUnit au = plugin.createAu(auConf);
        inactiveAuIds.remove(au.getAuId());
        log.debug("Created AU " + au);
        if (!isAuContentFromWs()) {
    try {
      getDaemon().startOrReconfigureAuManagers(au, auConf);
    } catch (Exception e) {
      log.error("Couldn't start AU processes", e);
      throw new ArchivalUnit.ConfigurationException(
          "Couldn't start AU processes", e);
    }
        }
        putAuInMap(au);
        event = AuEvent.forAu(au, event);
        signalAuEvent(au, event);
        return au;
      } catch (ArchivalUnit.ConfigurationException e) {
        throw e;
      } catch (Exception e) {
        log.error("Error creating AU", e);
        throw new
    ArchivalUnit.ConfigurationException("Unexpected error creating AU", e);
      }
    }

    /** Stop the AU's activity and remove it, as though it had never been
     * configured.  Does not affect the AU's current repository contents.
     * @return true if the AU was removed, false if it didn't exist or is
     * stale. */
    public boolean stopAu(ArchivalUnit au, AuEvent event) {
      String auid = au.getAuId();
      ArchivalUnit mapAu = (ArchivalUnit)auMap.get(auid);
      if (mapAu == null) {
        log.warning("stopAu(" + au.getName() + "), wasn't in map");
        return false;
      }
      if (mapAu != au) {
        log.warning("stopAu(" + au.getName() + "), but map contains " + mapAu);
        return false;
      }
      log.debug("Deactivating AU: " + au.getName());
      // remove from map first, so no new activity can start (poll messages,
      // RemoteAPI, etc.)
      synchronized (auMap) {
        auMap.remove(auid);
        auList = null;
      }
      delHostAus(au);

      signalAuEvent(au, event);

      try {
        Plugin plugin = au.getPlugin();
        plugin.stopAu(au);
        getDaemon().stopAuManagers(au);
      } catch (Exception e) {
        log.error("Unexpected error stopping AU", e);
        // Shouldn't happen, as stopAuManagers() catches errors in
        // stopService().  Not clear what to do anyway, if some of the
        // managers don't stop cleanly.
      }
      return true;
    }

    /** Return true iff the AU is currently active.  Ruturn false if the AU
     * was deactivated or deleted and this isn't the current instance of the
     * AU. */
    public boolean isActiveAu(ArchivalUnit au) {
      String auid = au.getAuId();
      return auid != null && auMap.get(auid) == au;
    }

    /**
     * Register a handler for AU events: create, delete, reconfigure.  May be
     * called after this manager's initService() (before startService()).
     * @param aueh AuEventHandler to add
     */
    public void registerAuEventHandler(AuEventHandler aueh) {
      log.debug2("registering AuEventHandler " + aueh);
      if (!auEventHandlers.contains(aueh)) {
        auEventHandlers.add(aueh);
      }
    }

    /**
     * Unregister an AuEventHandler
     * @param aueh AuEventHandler to remove
     */
    public void unregisterAuEventHandler(AuEventHandler aueh) {
      log.debug3("unregistering " + aueh);
      auEventHandlers.remove(aueh);
    }

  // JMS notification support

  public static final String JMS_PREFIX = PREFIX + "jms.";

  /** The jms topic at which AuEvent notifications are sent
   * @ParamRelevance Rare
   */
  public static final String PARAM_ENABLE_JMS_NOTIFICATIONS =
    JMS_PREFIX + "enable";
  public static final boolean DEFAULT_ENABLE_JMS_NOTIFICATIONS = false;

  /** The jms topic at which AuEvent notifications are sent
   * @ParamRelevance Rare
   */
  public static final String PARAM_JMS_NOTIFICATION_TOPIC =
    JMS_PREFIX + "topic";
  public static final String DEFAULT_JMS_NOTIFICATION_TOPIC = "AuEventTopic";

  /** The jms clientid of the config manager.
   * @ParamRelevance Rare
   */
  public static final String PARAM_JMS_CLIENT_ID = JMS_PREFIX + "clientId";
  public static final String DEFAULT_JMS_CLIENT_ID = "PluginMgr";


  private String notificationTopic = DEFAULT_JMS_NOTIFICATION_TOPIC;
  private boolean enableJmsNotifications = DEFAULT_ENABLE_JMS_NOTIFICATIONS;
  private String clientId = DEFAULT_JMS_CLIENT_ID;

    void startJms() {
      if (jmsConsumer == null && jmsProducer == null) {
        setUpJmsNotifications();
      }
    }

    void setUpJmsNotifications() {
      setUpJmsReceive(clientId, notificationTopic,
          new MapMessageListener("AuEvent Listener"));
      setUpJmsSend(clientId, notificationTopic);
    }

    /** Send AuEvent to JMS notification topic, if appropriate.  Currently
     * sends only ContentChanged events.  Note that these AU creation and
     * deletion events are <b>not</b> propagated to other cluster members, as
     * they may result from on-demand creation, RestartDelete/RestartCreate,
     * etc., which shouldn't trigger actions in others.  The AU create/delete
     * events of interest are configuration events, handled in
     * ConfigManager. */
    void sendAuEventNotification(ArchivalUnit au, AuEvent event) {
      if (jmsProducer != null) {
        switch (event.getType()) {
        case ContentChanged:
    try {
      jmsProducer.sendMap(event.toMap());
    } catch (JMSException e) {
      log.error("Couldn't send ContentChanged notification", e);
    }
        default:
    // don't send other events
        }
      }
    }

    /** Incoming AuEvent message */
    @Override
    protected void receiveMessage(Map map) {
      AuEvent event = AuEvent.fromMap(map);
      log.debug("Received notification: " + event);
      String auid = event.getAuId();
      ArchivalUnit au = getAuFromIdIfExists(auid);
      signalAuEventInternal(auid, au, event);
    }

  class PlugMgrAuEventHandler extends AuEventHandler.Base {
    @Override public void auContentChanged(AuEvent event, ArchivalUnit au,
					   AuEvent.ContentChangeInfo info) {
      if (loadablePluginsReady && isRegistryAu(au)) {
	processRegistryAus(ListUtil.list(au), paramStartAllAus);
      }
      if (shouldFlush404Cache(au, info)) {
	flush404Cache(au);
      };
    }
    @Override public void auCreated(AuEvent event, ArchivalUnit au) {
      flush404Cache(au);
    }
  }

  private AuEventHandler myAuEventHandler = new PlugMgrAuEventHandler();

    boolean shouldFlush404Cache(ArchivalUnit au,
              AuEvent.ContentChangeInfo chInfo) {
      switch (chInfo.getType()) {
      case Crawl:
        // Hack - routine recrawls fetch start page twice, would flush cache
        // too frequently
        return chInfo.getNumUrls() > 2;
      default:
        return chInfo.getNumUrls() > 0;
      }
    }

    protected void raiseAlert(Alert alert, String msg) {
      if (alertMgr != null) {
        alertMgr.raiseAlert(alert, msg);
      }
    }

    /** Signal an AuEvent to all listeners and the JMS notification channel */
    public void signalAuEvent(final ArchivalUnit au,
            final AuEvent event) {
      if (log.isDebug2()) log.debug2("AuEvent " + event);
      sendAuEventNotification(au, event);
      signalAuEventInternal(au.getAuId(), au, event);
    }

    /** Signal an AuEvent to all local listeners */
    void signalAuEventInternal(final String auid,
             final ArchivalUnit au,
             final AuEvent event) {
      switch (event.getType()) {
      case Create:
        raiseAlert(Alert.auAlert(Alert.AU_CREATED, au), "AU created");
        // falls through
      case RestartCreate:
      case StartupCreate:
      case Reactivate:
        applyAuEvent(new AuEventClosure() {
      public void execute(AuEventHandler hand) {
        try {
          hand.auCreated(event, auid, au);
        } catch (Exception e) {
          log.error("AuEventHandler threw", e);
        }
      }});
        break;
      case Delete:
        raiseAlert(Alert.auAlert(Alert.AU_DELETED, au), "AU deleted");
        // falls through
      case Deactivate:
      case RestartDelete:
        applyAuEvent(new AuEventClosure() {
      public void execute(AuEventHandler hand) {
        try {
          hand.auDeleted(event, auid, au);
        } catch (Exception e) {
          log.error("AuEventHandler threw", e);
        }
      }});
        break;
      case Reconfig:
        final Configuration oldAuConfig = event.getOldConfiguration();
        applyAuEvent(new AuEventClosure() {
      public void execute(AuEventHandler hand) {
        try {
          hand.auReconfigured(event, auid, au, oldAuConfig);
        } catch (Exception e) {
          log.error("AuEventHandler threw", e);
        }
      }});
        break;
      case ContentChanged:
        AuEvent.ContentChangeInfo chInfo = event.getChangeInfo();
        applyAuEvent(new AuEventClosure() {
      public void execute(AuEventHandler hand) {
        try {
          hand.auContentChanged(event, auid, au, chInfo);
        } catch (Exception e) {
          log.error("AuEventHandler threw", e);
        }
      }});
        break;
      default:
        log.error("Unhandled AuEvent: " + event);
      }
    }

  /** Closure applied to each AuEventHandler by {@link
   * #applyAuEvent(AuEventClosure) */
  private interface AuEventClosure {
    public void execute(AuEventHandler hand);
  }

    /** Apply an {@link #AuEventClosure} to each registered {@link
     * #AuEventHandler} */
    private void applyAuEvent(AuEventClosure closure) {
      // copy the list of handlers as it could change during the loop.
      for (AuEventHandler hand : new ArrayList<AuEventHandler>(auEventHandlers)) {
        try {
    closure.execute(hand);
        } catch (Exception e) {
    log.error("AuEventClosure threw", e);
        }
      }
    }

    protected void putAuInMap(ArchivalUnit au) {
      log.debug2("putAuMap(" + au.getAuId() +", " + au);
      synchronized (auMap) {
        ArchivalUnit oldAu = auMap.put(au.getAuId(), au);
        if (oldAu != null) {
    if (oldAu == au) {
      log.debug("Warning: Redundant putAuInMap: " + au, new Throwable());
      return;
    } else {
      log.error("Duplicate AUID in map. old: " + oldAu + ", new: " + au,
          new Throwable());
    }
        }
        auList = null;
      }
      if (!isAuContentFromWs()) {
        addHostAus(au);
      }
    }

    /**
     * Returns an existing Archival Unit given its identifier.  Does not
     * cause the AU to be started.
     *
     * @param auId
     *          A String with the Archival Unit identifier.
     * @return an ArchivalUnit with the requested Archival Unit, iff it
     *          already exists.
     */
    public ArchivalUnit getAuFromIdIfExists(String auId) {
      final String DEBUG_HEADER = "getAuFromIdIfExists(): ";
      if (log.isDebug2()) log.debug2(DEBUG_HEADER + "auId = " + auId);
      return (ArchivalUnit)auMap.get(auId);
    }

    /**
     * Provides an Archival Unit given its identifier, starting it if
     * necessary.
     *
     * @param auId
     *          A String with the Archival Unit identifier.
     * @return an ArchivalUnit with the requested Archival Unit, if either
     *          the AU was already present, or if its config is available and
     *          it can be started on the fly.
     */
    public ArchivalUnit getAuFromId(String auId) {
      final String DEBUG_HEADER = "getAuFromId(): ";
      if (log.isDebug2()) log.debug2(DEBUG_HEADER + "auId = " + auId);

      ArchivalUnit au = (ArchivalUnit)auMap.get(auId);
      if (log.isDebug3()) log.debug3(DEBUG_HEADER + "au = " + au);

      // If AU doesn't exist, maybe create it on-demand
      if (au == null && isStartAusOnDemand()) {
        au = createOnDemandAu(auId);
      }

      if (log.isDebug2()) log.debug2(DEBUG_HEADER + "au = " + au);
      return au;
    }

    ArchivalUnit createOnDemandAu(String auId) {
      final String DEBUG_HEADER = "createOnDemandAu(): ";
      Plugin plugin = getPluginFromAuId(auId);

      Configuration auConfig = null;
      try {
        auConfig = getStoredAuConfigurationAsConfiguration(auId);
      } catch (DbException dbe) {
        log.warning("Error fetching AU config: " + auId, dbe);
      } catch (LockssRestException lre) {
        log.warning("Error fetching AU config: " + auId, lre);
      }
      if (auConfig != null) {
        log.debug("Found stored config for on demand AU: " +
      auId + " : " + auConfig);
      } else {
        auConfig = getAuConfigFromTdb(plugin, auId);
        if (auConfig != null) {
    log.debug("Found tdb config for on demand AU: " +
        auId + " : " + auConfig);
        } else if (paramInferConfigFromAuId) {
    auConfig = getAuConfigFromAuId(auId);
    if (auConfig != null) {
      log.debug("Inferred config for on demand AU: " +
          auId + " : " + auConfig);
    }
        }
      }
      if (auConfig == null) {
        log.warning("Can't create AU on demand: " + auId);
        return null;
      }

      ArchivalUnit au = null;
      // Get the AU.
      try {
        au = createAu(plugin, auConfig,
          AuEvent.forAuId(auId, AuEvent.Type.Create));
        if (log.isDebug3()) log.debug3(DEBUG_HEADER + "au = " + au);
      } catch (Exception e) {
        log.error("Failed to create Archival Unit - auId = " + auId
      + ", auConfig = " + auConfig, e);
      }
      return au;
    }

    Configuration getAuConfigFromTdb(Plugin plugin, String auid) {
      if (plugin instanceof BasePlugin) {
        TitleConfig tc = ((BasePlugin)plugin).getTitleConfigFromAuId(auid);
        if (tc == null) return null;
        return tc.getConfig();
      }
      log.warning("Attempt to get TitleConfig for non-BasePlugin: " + auid);
      return null;
    }

  /** Infer a (possibly incomplete) AU configuration from an AUID.  This
   * should generally not be used as the result will include only
   * definitional params.  Intended for testing */
  public static Configuration getAuConfigFromAuId(String auid) {
    final String DEBUG_HEADER = "getAuConfigFromAuId(): ";
    String auKey = null;
    try {
      auKey = PluginManager.auKeyFromAuId(auid);
      if (log.isDebug3()) log.debug3(DEBUG_HEADER + "auKey = " + auKey);
    } catch (IllegalArgumentException iae) {
      log.warning("Can't infer config from AUID: " + auid, iae);
      return null;
    }

    // Get the properties encoded in the archival unit key.
    Properties props = null;

    try {
      props = PropUtil.canonicalEncodedStringToProps(auKey);
      if (log.isDebug3()) log.debug3(DEBUG_HEADER + "props = " + props);
    } catch (IllegalArgumentException iae) {
      log.warning("Can't infer config from AUID: " + auid, iae);
      return null;
    }

    try {
      Configuration auConfig = ConfigManager.fromPropertiesUnsealed(props);
      if (log.isDebug3()) log.debug3(DEBUG_HEADER + "auConfig = " + auConfig);
      return auConfig;
    } catch (RuntimeException re) {
      log.warning("Can't infer config from AUID: " + auid, re);
      return null;
    }
  }

    public void addHostAus(ArchivalUnit au) {
      try {
        Collection<String> stems = normalizeStems(au.getUrlStems());
        synchronized (hostAus) {
    for (String stem : stems) {
      addAuStem(stem, au);
    }
        }
      } catch (Exception e) {
        log.error("addHostAus()", e);
      }
    }

    private void delHostAus(ArchivalUnit au) {
      try {
        Collection<String> stems = normalizeStems(au.getUrlStems());
        synchronized (hostAus) {
    for (String stem : stems) {
      delAuStem(stem, au);
    }
        }
      } catch (Exception e) {
        log.error("delHostAus()", e);
      }
    }

    public void addAuStem(String stem, ArchivalUnit au) {
      log.debug2("Adding stem: " + stem + ", " + au);
      AuSearchSet searchSet = hostAus.get(stem);
      if (searchSet == null) {
        searchSet = new AuSearchSet(this);
        hostAus.put(stem, searchSet);
      }
      searchSet.addAu(au);
    }

    private void delAuStem(String stem, ArchivalUnit au) {
      log.debug2("Removing stem: " + stem + ", " + au);
      AuSearchSet searchSet = hostAus.get(stem);
      if (searchSet != null) {
        searchSet.delAu(au);
        if (searchSet.isEmpty()) {
    hostAus.remove(stem);
        }
      }
    }

    void flush404Cache(ArchivalUnit au) {
      if (!isAuContentFromWs()) {
        try {
    Collection<String> stems = normalizeStems(au.getUrlStems());
    synchronized (hostAus) {
      for (String stem : stems) {
        AuSearchSet searchSet = hostAus.get(stem);
        if (searchSet != null) {
          if (log.isDebug2()) log.debug2("Flushing 404 cache for: " + stem);
          searchSet.flush404Cache();
        }
      }
    }
        } catch (Exception e) {
    log.error("flush404Cache()", e);
        }
      }
    }

    /** An AU in which we just found a URL is likely to be referenced again
     * soon.  Move it to the head of the list in all AuSearchSets in which it
     * appears. */
    public void promoteAuInSearchSets(ArchivalUnit au) {
      try {
        Collection<String> stems = normalizeStems(au.getUrlStems());
        synchronized (hostAus) {
    for (String stem : stems) {
      AuSearchSet searchSet = hostAus.get(stem);
      if (searchSet != null) {
        makeFirstCandidate(searchSet, au);
      }
    }
        }
      } catch (Exception e) {
        log.error("promoteAuInSearchSet()", e);
      }
    }

    private List<String> normalizeStems(final Collection<String> stems)
        throws MalformedURLException {
      return new ArrayList<String>() {{
    for (String stem : stems) {
      add(UrlUtil.getUrlPrefix(UrlUtil.normalizeUrl(stem)));
    }
        }};
    }

  // These don't belong here
    /**
     * Reconfigure an AU and save the new configuration in the database.  Need to
     * find a better place for this.
     * @param au the AU
     * @param auProps the new AU configuration, using simple prop keys (not
     * prefixed with org.lockss.au.<i>auid</i>)
     * @throws ArchivalUnit.ConfigurationException
     * @throws DbException
     * @throws LockssRestException
     */
    public void setAndSaveAuConfiguration(ArchivalUnit au,
            Properties auProps)
        throws ArchivalUnit.ConfigurationException, DbException,
        LockssRestException {
      setAndSaveAuConfiguration(au,
              ConfigManager.fromPropertiesUnsealed(auProps));
    }

    /**
     * Reconfigure an AU and save the new configuration in the database.  Need to
     * find a better place for this.
     * @param au the AU
     * @param auConf the new AU configuration, using simple prop keys (not
     * prefixed with org.lockss.au.<i>auid</i>)
     * @throws ArchivalUnit.ConfigurationException
     * @throws DbException
     * @throws LockssRestException
     */
    public void setAndSaveAuConfiguration(ArchivalUnit au,
            Configuration auConf)
        throws ArchivalUnit.ConfigurationException, DbException,
        LockssRestException {
      synchronized (auAddDelLock) {
        log.debug("Reconfiguring AU " + au);
        au.setConfiguration(auConf);
        updateAuIndatabase(au, auConf);
      }
    }

    private void updateAuIndatabase(ArchivalUnit au, Configuration auConf)
        throws DbException, LockssRestException {
      if (!auConf.isEmpty()) {
        if (!auConf.isSealed()) {
    auConf.put(AU_PARAM_DISPLAY_NAME, au.getName());
        } else if (StringUtil.isNullString(auConf.get(AU_PARAM_DISPLAY_NAME))) {
    log.debug("Can't add name to sealed AU config: " + auConf,
        new Throwable());
        }
      }
      updateAuInDatabase(au.getAuId(), auConf);
    }

    public void updateAuConfigFromExternalSource(AuConfiguration auConfiguration)
        throws DbException, LockssRestException {
      synchronized (auAddDelLock) {
        configMgr.storeArchivalUnitConfiguration(auConfiguration, true);
      }
    }

    public void updateAuInDatabase(String auid, Configuration auConf)
        throws DbException, LockssRestException {
      String prefix = auConfigPrefix(auid);
      Configuration fqConfig = auConf.addPrefix(prefix);
      synchronized (auAddDelLock) {
        configMgr.storeArchivalUnitConfiguration(
      AuConfigurationUtils.fromConfiguration(prefix, fqConfig));
      }
    }

    /**
     * Create an AU and save its configuration in the database.  Need to find a
     * better place for this.
     * @param plugin the Plugin in which to create the AU
     * @param auProps the new AU configuration, using simple prop keys (not
     * prefixed with org.lockss.au.<i>auid</i>)
     * @return the new AU
     * @throws ArchivalUnit.ConfigurationException
     * @throws DbException
     * @throws LockssRestException
     */
    public ArchivalUnit createAndSaveAuConfiguration(Plugin plugin,
                 Properties auProps)
        throws ArchivalUnit.ConfigurationException, DbException,
        LockssRestException {
      return createAndSaveAuConfiguration(plugin,
            ConfigManager.fromPropertiesUnsealed(auProps));
    }

    /**
     * Create an AU and save its configuration in the database.  Need to find a
     * better place for this.
     * @param plugin the Plugin in which to create the AU
     * @param auConf the new AU configuration, using simple prop keys (not
     * prefixed with org.lockss.au.<i>auid</i>)
     * @return the new AU
     * @throws ArchivalUnit.ConfigurationException
     * @throws DbException
     * @throws LockssRestException
     */
    public ArchivalUnit createAndSaveAuConfiguration(Plugin plugin,
                 Configuration auConf)
        throws ArchivalUnit.ConfigurationException, DbException,
        LockssRestException {
      synchronized (auAddDelLock) {
        auConf.put(AU_PARAM_DISABLED, "false");
        ArchivalUnit au = createAu(plugin, auConf,
                                   AuEvent.model(AuEvent.Type.Create));
        AuState aus = AuUtil.getAuState(au);
        aus.setAuCreationTime(TimeBase.nowMs());
        updateAuIndatabase(au, auConf);
        return au;
      }
    }

    /**
     * Delete AU configuration from the database.  Need to find a better place for
     * this.
     * @param au the ArchivalUnit to be unconfigured
     * @throws DbException
     * @throws LockssRestException
     */
    public void deleteAuConfiguration(ArchivalUnit au)
        throws DbException, LockssRestException {
      synchronized (auAddDelLock) {
        log.debug("Deleting AU config: " + au);
        configMgr.removeArchivalUnitConfiguration(au.getAuId());
      }
    }

    /**
     * Delete AU configuration from the database.  Need to find a better place for
     * this.
     * @param auid the AuId
     * @throws DbException
     * @throws LockssRestException
     */
    public void deleteAuConfiguration(String auid)
        throws DbException, LockssRestException {
      synchronized (auAddDelLock) {
        log.debug("Deleting AU config: " + auid);
        configMgr.removeArchivalUnitConfiguration(auid);
        // might be deleting an inactive au
        inactiveAuIds.remove(auid);
      }
    }

    /**
     * Deactivate an AU in the database.  Does not actually stop the AU.
     * @param au the ArchivalUnit to be deactivated
     * @throws DbException
     * @throws LockssRestException
     */
    public void deactivateAuConfiguration(ArchivalUnit au)
        throws DbException, LockssRestException {
      synchronized (auAddDelLock) {
        log.debug("Deactivating AU: " + au);

        AuConfiguration auConfiguration =
      configMgr.retrieveArchivalUnitConfiguration(au.getAuId());
        if (log.isDebug3()) log.debug3("auConfig = " + auConfiguration);

        if (auConfiguration != null) {
    auConfiguration.getAuConfig().put(AU_PARAM_DISABLED, "true");

    configMgr.storeArchivalUnitConfiguration(auConfiguration);
        }
      }
    }

    /**
     * Delete an AU
     * @param au the ArchivalUnit to be deleted
     * @throws DbException
     * @throws LockssRestException
     */
    public void deleteAu(ArchivalUnit au)
        throws DbException, LockssRestException {
      synchronized (auAddDelLock) {
        deleteAuConfiguration(au);
        if (isRemoveStoppedAus()) {
    stopAu(au, AuEvent.forAu(au, AuEvent.Type.Delete));
        }
      }
    }

    /**
     * Deactivate an AU
     * @param au the ArchivalUnit to be deactivated
     * @throws DbException
     * @throws LockssRestException
     */
    public void deactivateAu(ArchivalUnit au)
        throws DbException, LockssRestException {
      synchronized (auAddDelLock) {
        deactivateAuConfiguration(au);
        deactivateAuOnly(au);
      }
    }

    public boolean isRemoveStoppedAus() {
      return CurrentConfig.getBooleanParam(PARAM_REMOVE_STOPPED_AUS,
             DEFAULT_REMOVE_STOPPED_AUS);
    }

    private void deactivateAuOnly(ArchivalUnit au) {
      if (isRemoveStoppedAus()) {
        String auid = au.getAuId();
        stopAu(au, AuEvent.forAu(au, AuEvent.Type.Deactivate));
        inactiveAuIds.add(auid);
      }
    }

    // Stops and restarts a set of AUs so that they start using the current
    // version of their plugin.  Waits a little while between stopping and
    // starting to allow existing processes to exit.  It's expected that this
    // will cause lots of errors to be logged
    void restartAus(Collection<ArchivalUnit> aus) {
      if (paramRestartAus) {
        log.info("Restarting " + aus.size() + " AUs to use updated plugins.  Exiting processes may log errors; they should be harmless");
        synchronized (auAddDelLock) {
    Map<String, Configuration> configMap =
        new HashMap<String, Configuration>();
    for (ArchivalUnit au : aus) {
      String auid = au.getAuId();
      Configuration auConf = au.getConfiguration();
      configMap.put(auid, auConf);
      numAusRestarting++;
      stopAu(au, AuEvent.forAu(au, AuEvent.Type.RestartDelete));
    }
    try {
      Deadline.in(auRestartSleep(aus.size())).sleep();
    } catch (InterruptedException ex) {
    }

    // The number of remaining AUs to be processed.
    int remainingAus = configMap.entrySet().size();

    // The event used to signal that an AU needs to be added to the batch of
    // AUs to be marked for re-indexing.
    AuEvent batchEvent =
      AuEvent.model(AuEvent.Type.RestartCreate).setInBatch();

    // The event used to signal that an AU needs to be added to the batch of
    // AUs to be marked for re-indexing and the current batch needs to be
    // executed afterwards.
    AuEvent executeEvent = AuEvent.model(AuEvent.Type.RestartCreate);

    AuEvent auEvent = null;
    ArchivalUnit newAu = null;

    for (Map.Entry<String,Configuration> ent : configMap.entrySet()) {
      String auid = ent.getKey();
      Configuration auConf = ent.getValue();
      String pkey = pluginKeyFromId(pluginIdFromAuId(auid));
      Plugin plug = getPlugin(pkey);

      // To find the last AU.
      remainingAus--;

      // Check whether this is the last AU and therefore the batch of AUs to
      // be marked for re-indexing needs to be executed.
      if (remainingAus <= 0) {
        // Yes.
        auEvent = executeEvent;
      } else {
        // No.
        auEvent = batchEvent;
      }

      try {
        newAu = createAu(plug, auConf, auEvent);
        numAusRestarting--;
      } catch (ArchivalUnit.ConfigurationException e) {
        log.error("Failed to restart: " + auid);

        // Check whether the failure happened when trying to execute the
        // batch.
        if (!auEvent.isInBatch()) {
          // Yes: Execute the batch with the last successfully-created AU.
          signalAuEvent(newAu, auEvent);
        }
      }
    }
    numFailedAuRestarts += numAusRestarting;
    numAusRestarting = 0;
        }
      }
    }

    long auRestartSleep(int n) {
      return Math.min(n * paramPerAuRestartSleep, paramAuRestartMaxSleep);
    }

    public int getNumAusRestarting() {
      return numAusRestarting;
    }

    public int getNumFailedAuRestarts() {
      return numFailedAuRestarts;
    }

    /**
     * Return true if the specified Archival Unit is used internally
     * by the LOCKSS daemon.  Currently this is only registry AUs
     */
    public boolean isInternalAu(ArchivalUnit au) {
      return au instanceof RegistryArchivalUnit;
    }

    /**
     * Return true if the specified Archival Unit is a plugin registry AU
     */
    public boolean isRegistryAu(ArchivalUnit au) {
      return au instanceof RegistryArchivalUnit;
    }

    /**
     * Return true if the specified Plugin is used internally
     * by the LOCKSS daemon.
     */
    public boolean isInternalPlugin(Plugin plugin) {
      return internalPlugins.containsKey(plugin.getPluginId());
    }

    /**
     * Return true if the specified Plugin is a Loadable plugin
     */
    public boolean isLoadablePlugin(Plugin plugin) {
      PluginInfo info = (PluginInfo)pluginfoMap.get(getPluginKey(plugin));
      if (info == null) {
        return false;
      }
      return info.isOnLoadablePath();
    }

    /**
     * Return a String identifying the type of plugin {Loadable, Internally,
     * Builtin}
     */
    public String getPluginType(Plugin plugin) {
      if (isLoadablePlugin(plugin)) {
        return "Loadable";
      } else if (isInternalPlugin(plugin)) {
        return "Internal";
      } else {
        return "Builtin";
      }
    }

    /**
     * Return the config tree for an AU id (from the database, not the au itself).
     * @param auid the AU's id.
     * @return the AU's Configuration, with unprefixed keys.
     */
    public AuConfiguration getStoredAuConfiguration(String auid)
        throws DbException, LockssRestException {
      return configMgr.retrieveArchivalUnitConfiguration(auid);
    }

    /**
     * Return the config tree for an AU id (from the database, not the au itself).
     * @param auid the AU's id.
     * @return the AU's Configuration, with unprefixed keys.
     */
    public Configuration getStoredAuConfigurationAsConfiguration(String auid)
        throws DbException, LockssRestException {
      if (log.isDebug2()) log.debug2("auid = " + auid);

      Configuration config = null;
      AuConfiguration auConfiguration =
    configMgr.retrieveArchivalUnitConfiguration(auid);

      if (auConfiguration != null) {
        config = AuConfigurationUtils.toUnprefixedConfiguration(auConfiguration);
      }

      if (log.isDebug2()) log.debug2("config = " + config);
      return config;
    }

  // Loadable Plugin Support

    /**
     * Retrieve a plugin from the specified classloader.  If the
     * clasloader is 'null', this method will use the default
     * classloader.
     */
    PluginInfo retrievePlugin(String pluginKey, ClassLoader loader)
        throws Exception {
      if (pluginfoMap.containsKey(pluginKey)) {
        return (PluginInfo)pluginfoMap.get(pluginKey);
      }
      if (pluginMap.containsKey(pluginKey)) {
        return new PluginInfo(pluginMap.get(pluginKey), loader, null);
      }
      String pluginName = pluginNameFromKey(pluginKey);
      if (retract != null && !retract.isEmpty()) {
        if (retract.contains(pluginName)) {
    log.debug3("Not loading " + pluginName +
         " because it's on the retract list");
    return null;
        }
      }
      if (loader == null) {
        loader = this.getClass().getClassLoader();
      }

      try {
        PluginInfo info = loadPlugin(pluginKey, loader);
        Plugin newPlug = info.getPlugin();

        log.debug2("Found plugin: version " + newPlug.getVersion() +
                   " of " + newPlug.getPluginName());
        return info;
      } catch (PluginException.PluginNotFound e) {
        logAndAlert(pluginName, "Plugin not found", e);
      } catch (PluginException.LinkageError e) {
        logAndAlertStack(pluginName, "Can't load plugin", e);
      } catch (PluginException.IncompatibleDaemonVersion e) {
        logAndAlert(pluginName, "Incompatible Plugin", e);
      } catch (PluginException.InvalidDefinition e) {
        logAndAlert(pluginName, "Error in plugin", e);
      } catch (Exception e) {
        logAndAlertStack(pluginName, "Can't load plugin", e);
      }
      return null;
    }

    void logAndAlertStack(String pluginName, String msg, Exception e) {
      log.error(msg + ": " + pluginName, e);
      alert0(pluginName, msg, e.getMessage());
    }

    void logAndAlert(String pluginName, String msg, Exception e) {
      logAndAlert(pluginName, msg, e.getMessage());
    }

    void logAndAlert(String pluginName, String msg, String emsg) {
      log.error(msg + ": " + pluginName + ": " + emsg);
      alert0(pluginName, msg, emsg);
    }

    void alert0(String pluginName, String msg, String emsg) {
      raiseAlert(Alert.cacheAlert(Alert.PLUGIN_NOT_LOADED),
           String.format("%s: %s\n%s", msg, pluginName, emsg));
    }

    /**
     * Load a plugin from the specified classloader.  If the
     * clasloader is 'null', this method will use the default
     * classloader.
     */
    public PluginInfo loadPlugin(String pluginKey, ClassLoader loader)
        throws Exception {
      String pluginName = pluginNameFromKey(pluginKey);
      if (loader == null) {
        loader = this.getClass().getClassLoader();
      }

      // First look for a loadable plugin definition.
      try {
        log.debug3(pluginName + ": Looking for XML definition.");
        Class c = Class.forName(getConfigurablePluginName(pluginName),
              true, loader);
        log.debug3("Class is " + c.getName());
        DefinablePlugin xmlPlugin = (DefinablePlugin)c.newInstance();
        xmlPlugin.initPlugin(getDaemon(), pluginName, loader);
        if (isCompatible(xmlPlugin)) {
    // found a compatible plugin, return it
    List<String> urls = xmlPlugin.getLoadedFromUrlStrings();
    PluginInfo info = new PluginInfo(xmlPlugin, loader, urls);
    return info;
        } else {
    xmlPlugin.stopPlugin();
    log.warning("Plugin " + pluginName +
          " not started because it requires daemon version " +
          xmlPlugin.getRequiredDaemonVersion());
        }
      } catch (FileNotFoundException ex) {
        log.debug2("No XML plugin: " + pluginName + ": " + ex);
      }
      // throw any other exception

      // If didn't find an XML plugin look for a Plugin class.
      try {
        log.debug3(pluginName + ": Looking for class.");
        Class c = Class.forName(pluginName, true, loader);
        Plugin classPlugin = (Plugin)c.newInstance();
        classPlugin.initPlugin(getDaemon());
        if (isCompatible(classPlugin)) {
    String path = pluginName.replace('.', '/').concat(".class");
    URL url = loader.getResource(path);
    PluginInfo info = new PluginInfo(classPlugin, loader,
             ListUtil.list(url.toString()));
    return info;
        } else {
    classPlugin.stopPlugin();
    String req = " requires daemon version "
      + classPlugin.getRequiredDaemonVersion();
    throw new PluginException.IncompatibleDaemonVersion("Plugin " +
                    pluginName + req);
        }
      } catch (ClassNotFoundException ex) {
        throw new PluginException.PluginNotFound("Plugin " + pluginName
                   + " could not be found");
      } catch (LinkageError e) {
        throw new PluginException.LinkageError("Plugin " + pluginName
                   + " could not be loaded", e);
      }
    }

    /**
     * If the plugin lists a single required version, return true iff
     * the daemon version is >= that.  If it lists multipl required
     * version, return true iff one of them matches the daemon version's
     * major version and the daemon version is <= it.
     */
    protected boolean isCompatible(Plugin plug) {
      List<DaemonVersion> reqVers = getPluginRequiredVersions(plug);
      DaemonVersion dver = getDaemonVersion();
      boolean res = false;
      if (dver == null) {
        res = true; // don't break things during testing
      } else {
        switch (reqVers.size()) {
        case 0:                // plugin has no required daemon version
          res = true;
          break;
        case 1:                // plugin has one required daemon version
          res = dver.compareTo(reqVers.get(0)) >= 0;
          break;
        default:
          // plugin has required daemon version for multiple major versions.
          // Find the one corresponding to this daemon's major version.
          for (DaemonVersion ver : reqVers) {
            if (dver.getMajorVersion() == ver.getMajorVersion()) {
              res = dver.compareTo(ver) >= 0;
              break;
            }
          }
          break;
        }
      }
      if (log.isDebug3())
        log.debug3("Plugin " + plug.getPluginName() +
                   " (req: " + reqVers + ") is " + (res ? "" : "not ") +
       "compatible with daemon " + dver);
      return res;
    }

    List<DaemonVersion> getPluginRequiredVersions(Plugin plug) {
      List<DaemonVersion> res = new ArrayList<>();
      for (String reqVer : plug.getRequiredDaemonVersion()) {
        res.add(new DaemonVersion(reqVer));
      }
      return res;
    }

    // overridable for testing
    protected DaemonVersion getDaemonVersion() {
      return ConfigManager.getDaemonVersion();
    }

    /**
     * (package-level access for unit testing)
     */
    int getPreferredPluginType() {
      String preferredPlugin =
        CurrentConfig.getCurrentConfig().get(PARAM_PREFERRED_PLUGIN_TYPE,
                                             DEFAULT_PREFERRED_PLUGIN_TYPE);

      if (StringUtil.equalStringsIgnoreCase(preferredPlugin.trim(), "xml")) {
        return PREFER_XML_PLUGIN;
      } else if (StringUtil.equalStringsIgnoreCase(preferredPlugin.trim(), "class")) {
        return PREFER_CLASS_PLUGIN;
      } else {
        // By default, if we can't parse the configuration
        return PREFER_XML_PLUGIN;
      }
    }

    /**
     * Load a plugin with the given class name from somewhere in our classpath.
     * @param pluginKey the key for this plugin
     * @return true if loaded
     */
    public boolean ensurePluginLoaded(String pluginKey) {
      return ensurePluginLoaded(pluginKey, null);
    }

    /**
     * Load a plugin with the given class name from somewhere on the path of
     * the supplied ClassLoader
     * @param pluginKey the key for this plugin
     * @param loader the ClassLoader to search for the plugin
     * @return true if loaded
     */
    public boolean ensurePluginLoaded(String pluginKey, ClassLoader loader) {
      log.debug3("ensurePluginLoaded(" + pluginKey + ")");
      if (pluginMap.containsKey(pluginKey)) {
        return true;
      }

      PluginInfo info = (PluginInfo)pluginfoMap.get(pluginKey);
      // if no ClassLoader supplied, see if we have info for a loadable plugin
      if (loader == null && info != null) {
        loader = info.getClassLoader();
      }
      if (loader == null) {
        loader = this.getClass().getClassLoader();
      }

      String pluginName = "";
      try {
        pluginName = pluginNameFromKey(pluginKey);
        log.debug3("Trying to retrieve "+pluginKey);
        info = retrievePlugin(pluginKey, loader);
        if (info != null) {
    setPlugin(pluginKey, info.getPlugin());
    pluginfoMap.put(pluginKey, info);
    return true;
        } else {
    log.debug("Couldn't retrieve "+pluginKey);
    return false;
        }
      } catch (Exception e) {
        log.error("Error instantiating " + pluginName, e);
        return false;
      }
    }

    public Plugin loadBuiltinPlugin(Class pluginClass) {
      return loadBuiltinPlugin(pluginClass.getName());
    }

    protected Plugin loadBuiltinPlugin(String pluginClassName) {
      String pluginKey = pluginKeyFromName(pluginClassName);
      if (ensurePluginLoaded(pluginKey)) {
        return pluginMap.get(pluginKey);
      }
      return null;
    }

    /**
     * Return the plugin with the given key.
     * @param pluginKey the plugin key
     * @return the plugin or null
     */
    public Plugin getPlugin(String pluginKey) {
      return pluginMap.get(pluginKey);
    }

    public Plugin getPluginFromId(String pluginId) {
      return getPlugin(pluginKeyFromId(pluginId));
    }

    /**
     * Provides the plugin of an Archival Unit.
     *
     * @param auid
     *          A String with the Archival Unit identifier.
     * @return a Plugin with the Archival Unit plugin.
     */
    public Plugin getPluginFromAuId(String auid) {
      final String DEBUG_HEADER = "getPluginFromAuId(): ";
      if (log.isDebug2()) log.debug2(DEBUG_HEADER + "auid = " + auid);

      String pluginKey = pluginIdFromAuId(auid);
      if (log.isDebug3()) log.debug3(DEBUG_HEADER + "pluginKey = " + pluginKey);

      Plugin plugin = getPlugin(pluginKeyFromId(pluginKey));
      if (log.isDebug3()) log.debug3(DEBUG_HEADER + "plugin = " + plugin);

      if (plugin == null && !isLoadAllPlugins()) {
        if (log.isDebug3())
    log.debug3(DEBUG_HEADER + "Trying to retrieve plugin " + pluginKey);

        try {
    ensurePluginLoaded(pluginKey);
    plugin = getPlugin(pluginKeyFromId(pluginKey));
        } catch (Exception e) {
          String message = "Error instantiating plugin "
              + pluginNameFromKey(pluginIdFromAuId(auid));
          log.error(message, e);
        }
      }

      return plugin;
    }

    protected void setPlugin(String pluginKey, Plugin plugin) {
      if (log.isDebug3()) {
        log.debug3("PluginManager.setPlugin(" + pluginKey + ", " +
       plugin.getPluginName() + ")");
      }
      Plugin oldPlug = pluginMap.get(pluginKey);
      if (oldPlug != null) {
        String oldName = oldPlug.getPluginName();
        String name = plugin.getPluginName();
        // Alert on new plugin version
        StringBuilder sb = new StringBuilder();
        sb.append("Plugin reloaded: ");
        sb.append(name);
        if (!StringUtil.equalStrings(oldName, name)) {
          sb.append(" (was ");
          sb.append(oldName);
          sb.append(")");
        }
        sb.append("\nVersion: ");
        sb.append(plugin.getVersion());
        String feats = PluginManager.pluginFeatureVersionsString(plugin);
        if (!StringUtil.isNullString(feats)) {
          sb.append("\nFeature versions:\n");
          sb.append(feats);
        }
        raiseAlert(Alert.cacheAlert(Alert.PLUGIN_RELOADED), sb.toString());
        log.debug("Stopping old plugin " + oldName);
        oldPlug.stopPlugin();
      }
      pluginMap.put(pluginKey, plugin);
      log.info("Loaded plugin: version " + plugin.getVersion() +
               " of " + plugin.getPluginName());
      resetTitles();
    }

    void removePlugin(String key) {
      log.debug("Removing plugin " + key);
      pluginMap.remove(key);
      pluginfoMap.remove(key);
    }

  /** Return a string describing the plugin feature versions */
  public static String pluginFeatureVersionsString(Plugin plug) {
    StringBuilder sb = new StringBuilder();
    for (Plugin.Feature feat : Plugin.Feature.values()) {
      String val = plug.getFeatureVersion(feat);
      if (!StringUtil.isNullString(val)) {
        if (sb.length() != 0) {
          sb.append("\n");
        }
        sb.append("  ");
        sb.append(feat);
        sb.append(": ");
        sb.append(val);
      }
    }
    return sb.toString();
  }

    /**
     * Return the class name (of possibly a subclass) of DefinablePlugin
     * that can be configured by an XML file.
     * @param pluginName -  the class name of the plugin wanted
     * @return - the class name of the configurable class that will implement
     * the named plugin
     */
    protected String getConfigurablePluginName(String pluginName) {
      String ret = DEFAULT_CONFIGURABLE_PLUGIN_NAME;
      for (Iterator it = configurablePluginNameMap.keySet().iterator();
     it.hasNext(); ) {
        String regex = (String)it.next();
        if (pluginName.matches(regex)) {
    ret = configurablePluginNameMap.get(regex);
    break;
        }
      }
      return ret;
    }

    /**
     * Find the CachedUrlSet from a PollSpec.
     * @param spec the PollSpec (from an incoming message)
     * @return a CachedUrlSet for the plugin, au and URL in the spec, or
     * null if au not present on this cache
     */
    public CachedUrlSet findCachedUrlSet(PollSpec spec) {
      if (log.isDebug3()) log.debug3(this +".findCachedUrlSet2("+spec+")");
      String auId = spec.getAuId();
      ArchivalUnit au = getAuFromId(auId);
      if (log.isDebug3()) log.debug3("au: " + au);
      if (au == null) return null;
      String url = spec.getUrl();
      CachedUrlSet cus;
      if (AuUrl.isAuUrl(url)) {
        cus = au.getAuCachedUrlSet();
      } else if ((spec.getLwrBound() != null) &&
           (spec.getLwrBound().equals(PollSpec.SINGLE_NODE_LWRBOUND))) {
        cus = au.makeCachedUrlSet(new SingleNodeCachedUrlSetSpec(url));
      } else {
        RangeCachedUrlSetSpec rcuss =
    new RangeCachedUrlSetSpec(url, spec.getLwrBound(), spec.getUprBound());
        cus = au.makeCachedUrlSet(rcuss);
      }
      if (log.isDebug3()) log.debug3("ret cus: " + cus);
      return cus;
    }

    /**
     * Find an AU's top level CachedUrlSet.
     *
     * @param auId
     * @return top level <code>CachedUrlSet</code> for the specified auId
     */
    public CachedUrlSet findCachedUrlSet(String auId) {
      ArchivalUnit au = getAuFromId(auId);
      if (au == null) return null;
      return au.getAuCachedUrlSet();
    }

    /** Return a collection of all AUs that have content on the host of this
     * url, sorted in AU title order.  */
    //  XXX Should do something about the redundant normalization involved in
    // calling more than one of these methods
    public Collection<ArchivalUnit> getCandidateAus(String url)
        throws MalformedURLException {
      String normStem = UrlUtil.getUrlPrefix(UrlUtil.normalizeUrl(url));
      return getCandidateAusFromStem(normStem);
    }

    public Collection<ArchivalUnit> getCandidateAusFromStem(String normStem) {
      synchronized (hostAus) {
        AuSearchSet searchSet = hostAus.get(normStem);
        if (searchSet != null) {
    return searchSet.getSortedAus();
        }
        return Collections.EMPTY_LIST;
      }
    }

    /** Return a collection of all AUs that have content on the host of this
     * url, sorted in AU title order.  */
    // This method needs to copy the list anyway, to avoid CME, so this is as
    // good a place as any to sort it.
    //  XXX Should do something about the redundant normalization involved in
    // calling more than one of these methods
    public SortedSet<ArchivalUnit> getAllStems() {
      synchronized (hostAus) {
        return new TreeSet(hostAus.keySet());
      }
    }

    // Return  list of candiate AUs, used only for testing
    List<ArchivalUnit> getRawCandidateAus(String url)
        throws MalformedURLException {
      String normStem = UrlUtil.getUrlPrefix(UrlUtil.normalizeUrl(url));
      synchronized (hostAus) {
        AuSearchSet searchSet = hostAus.get(normStem);
        if (searchSet == null) {
    return Collections.EMPTY_LIST;
        }
        return ListUtil.fromIterator(searchSet.iterator());
      }
    }

  /** Content requirements of the CU being searched for.  Some callers
   * (proxy) are only interested in CUs with content, some (ServeContent)
   * prefer one with content, some (exploders) don't care. */
  public enum CuContentReq {

//       /** Find the CU with the most recent content */
//       MostRecentContent,

	/** Find a CU with content */
	HasContent,

	/** Find a CU with content if possible, else one without */
	PreferContent,

	/** Find a CU regardless of content */
	DontCare;

      /** Return true if this requirement implies that the specified
       * requirement is met */
      boolean satisfies(CuContentReq req) {
	return compareTo(req) <= 0;
      }

      /** Return true if the presence of content is relevant for this
       * requirement */
      boolean wantsContent() {
	switch (this) {
	case DontCare: return false;
	default: return true;
	}
      }

      /** Return true if the presence of content is required */
      boolean needsContent() {
	switch (this) {
	case HasContent: return true;
	default: return false;
	}
      }
  }

  /** A CU in the recentCu cache, along with a record of the content
   * requirement it satisfies */
  class RecentCu {
    CachedUrl cu;
    CuContentReq contentReq;

    RecentCu(CachedUrl cu, CuContentReq contentReq) {
      this.cu = cu;
      this.contentReq = contentReq;
    }

    /** Return true if this CU satisfies the specified content
     * requirement */
    boolean satisfiesReq(CuContentReq req) {
      return contentReq.satisfies(req);
    }
  }

  private Map<String,RecentCu> recentCuMap = new LRUMap(20);
  private int recentCuHits = 0;
  private int recentCuMisses = 0;
  private int recent404Hits = 0;

    public int getRecentCuHits() {
      return recentCuHits;
    }

    public int getRecentCuMisses() {
      return recentCuMisses;
    }

    public int getRecent404Hits() {
      return recent404Hits;
    }

    public void flushRecentCuCache() {
      synchronized (recentCuMap) {
        recentCuMap.clear();
      }
    }

  /** Describes a search in progress and provides a way to wait for its
   * result. */
  class UrlSearch {
    String url;
    CuContentReq contentReq;
    OneShotSemaphore sem = new OneShotSemaphore();
    CachedUrl result;

    UrlSearch(String url, CuContentReq contentReq) {
      this.url = url;
      this.contentReq = contentReq;
    }

    /** Return true if this search will satisfy the specified content
     * requirement */
    boolean satisfiesReq(CuContentReq req) {
      return contentReq.satisfies(req);
    }

    boolean hasResult() {
      return sem.isFull();
    }

    CachedUrl getResult() {
      try {
	sem.waitFull(Deadline.MAX);
      } catch (InterruptedException e) {
	log.warning("UrlSearch timeout, shouldn't happen: " + url);
	return null;
      }
      return result;
    }

    void putResult(CachedUrl res) {
      this.result = res;
      sem.fill();
    }

    public boolean equals(Object obj) {
      if (this == obj) {
	return true;
      }
      if (obj instanceof UrlSearch) {
	UrlSearch o = (UrlSearch)obj;
	return contentReq.equals(o.contentReq)
	  && url.equals(o.url);
      }
      return false;
    }

    public int hashCode() {
      return url.hashCode() ^ contentReq.hashCode();
    }
  }

  private Map<UrlSearch,UrlSearch> currentUrlSearches =
    new HashMap<UrlSearch,UrlSearch>();
  private int curSearchWaits = 0;
  private int curSearchRes404 = 0;
  private int curSearchResCu = 0;

    int getUrlSearchWaits() {
      return curSearchWaits;
    }

    int getUrlSearchRes404() {
      return curSearchRes404;
    }

    int getUrlSearchResCu() {
      return curSearchResCu;
    }

    /**
     * Searches for an AU that contains content for the URL and returns the
     * corresponding CachedUrl.
     * @param url The URL to search for.
     * @return a CachedUrl, or null if URL not present in any AU
     */
    public CachedUrl findCachedUrl(String url) {
      return findTheCachedUrl(url, CuContentReq.HasContent);
    }

    /**
     * Searches for an AU that contains the URL and returns the corresponding
     * CachedUrl.
     * @param url The URL to search for.
     * @param contentReq selects the requirements for the CU having content
     * @return a CachedUrl, or null if no CU meeting the requirements exists
     * in any AU
     */
    public CachedUrl findCachedUrl(String url, CuContentReq contentReq) {
      return findTheCachedUrl(url, contentReq);
    }

    /** Find a CachedUrl for the URL.
     */
    private CachedUrl findTheCachedUrl(String url, CuContentReq contentReq) {
      // Maintain a small cache of URL -> CU.  When ICP is in use, each URL
      // will likely be looked up twice in quick succession

      CachedUrl res;
      synchronized (recentCuMap) {
        RecentCu rcu = recentCuMap.get(url);
        if (rcu != null) {
    // Ensure we flush CUs belonging to stale AUs.  (The test is cheap,
    // and handling this with AuEvent handler would require a search as
    // map is keyed by CU, not AU.)
    if (!isActiveAu(rcu.cu.getArchivalUnit())) {
      log.debug3("cache hit " + rcu.cu.toString() + " in stale AU: " +
           rcu.cu.getArchivalUnit() + ", flushed");
      recentCuMap.remove(url);
      rcu = null;
    }
        }
        if (rcu != null && !rcu.satisfiesReq(contentReq)) {
    log.debug3("cache hit " + rcu.cu.toString() +
         " but doesn't satisfy content requirement: " +
         rcu.contentReq + " vs. " + contentReq);
    rcu = null;
        }
        if (rcu != null) {
    if (log.isDebug3()) {
      log.debug3("cache hit " + rcu.cu.toString() + ", " + rcu.contentReq);
    }
    recentCuHits++;
    return rcu.cu;
        } else {
    log.debug3("cache miss for " + url);
    recentCuMisses++;
        }
      }
      CachedUrl cu = null;
      if (paramPreventConcurrentSearches) {
        UrlSearch newSearch = new UrlSearch(url, contentReq);
        UrlSearch oldSearch;
        synchronized (currentUrlSearches) {
    oldSearch = currentUrlSearches.get(newSearch);
    if (oldSearch == null) {
      currentUrlSearches.put(newSearch, newSearch);
    }
        }
        if (oldSearch != null) {
    if (log.isDebug2()) {
      log.debug2("Waiting for result from concurrent search: " + url);
    }
    curSearchWaits++;
    CachedUrl oldRes = oldSearch.getResult();
    if (log.isDebug2()) {
      log.debug2("Got result: " + oldRes);
    }
    if (oldRes == null) {
      curSearchRes404++;
    } else {
      curSearchResCu++;
    }
    return oldRes;
        }
        try {
    cu = findTheCachedUrl0(url, contentReq);
        } finally {
    newSearch.putResult(cu);
    synchronized (currentUrlSearches) {
      currentUrlSearches.remove(newSearch);
    }
        }
      } else {
        cu = findTheCachedUrl0(url, contentReq);
      }
      if (cu != null) {
        synchronized (recentCuMap) {
    recentCuMap.put(url, new RecentCu(cu, contentReq));
        }
        return cu;
      }
      return null;
    }

    // overridable for testing only
    protected CachedUrl findTheCachedUrl0(String url, CuContentReq contentReq) {
      List<CachedUrl> lst = findCachedUrls0(url, contentReq, true);
      if (!lst.isEmpty()) {
        return lst.get(0);
      } else {
        return null;
      }
    }

    public List<CachedUrl> findCachedUrls(String url) {
      return findCachedUrls0(url, CuContentReq.HasContent, false);
    }

    public List<CachedUrl> findCachedUrls(String url, CuContentReq contentReq) {
      return findCachedUrls0(url, contentReq, false);
    }

  // instrumentation, mostly for tests
  class FindUrlStats {
    int v1Invocations;
    int v1Results;
    int v2Invocations;
    int v2Results;
    int v2AusConsidered;
    int v2AuUrlsConsidered;
    int v2RedundantUrls;

    void addFrom(FindUrlStats o) {
      v1Invocations += o.v1Invocations;
      v1Results += o.v1Results;
      v2Invocations += o.v2Invocations;
      v2Results += o.v2Results;
      v2AusConsidered += o.v2AusConsidered;
      v2AuUrlsConsidered += o.v2AuUrlsConsidered;
      v2RedundantUrls += o.v2RedundantUrls;
    }

    public String toString() {
      return "[FindUrlStats: v1Inv: " + v1Invocations + ", v1Res: " + v1Results + ", v2nv: " + v2Invocations + ", v2Res: " + v2Results + ", v2Aus: " + v2AusConsidered + ", v2Urls: " + v2AuUrlsConsidered + ", v2Redundant: " + v2RedundantUrls;
    }
  }

  FindUrlStats fUStats;
  FindUrlStats totFUStats = new FindUrlStats();

    public void logFindUrlStats(boolean total) {
      if (total) {
        log.debug("Total: " + totFUStats.toString());
      } else {
        log.debug("Last: " + fUStats.toString());
      }
    }

    private List<CachedUrl> findCachedUrls0(String url, CuContentReq contentReq,
              boolean bestOnly) {
      fUStats = new FindUrlStats();
      List<CachedUrl> res = findCachedUrls1(url, contentReq, bestOnly);
      totFUStats.addFrom(fUStats);
      return res;
    }

    /** Lookup a name directly in the repo, skipping search sets and
     * site normaliztion.  Used to lookup names(which may be URLs,
     * malformed URLs, or non-URLs) in NamedAUs, which have no stems.
     */
    private List<CachedUrl> fastFind(String name,boolean bestOnly) {
      List<CachedUrl> res = new ArrayList<>(bestOnly ? 1 : 15);
      for (Artifact art : repoMgr.findArtifactsByUrl(name)) {
        ArchivalUnit artAu = getAuFromIdIfExists(art.getAuid());
        if (artAu != null) {
          res.add(artAu.makeCachedUrl(art.getUri()));
          if (bestOnly) {
            return res;
          }
        }
      }
      return res;
    }

    private List<CachedUrl> findCachedUrls1(String url, CuContentReq contentReq,
              boolean bestOnly) {
      String normUrl;
      String normStem;
      List<CachedUrl> res;
      boolean isUrl = UrlUtil.isUrl(url);

      // If not a URL, lookup directly in repo.
      if (!UrlUtil.isUrl(url)) {
        return fastFind(url, bestOnly);
      }
      // Else try to normalize it
      try {
        normUrl = UrlUtil.normalizeUrl(url);
        normStem = UrlUtil.getUrlPrefix(normUrl);
      } catch (MalformedURLException e) {
        log.warning("findCachedUrls(" + url +
                    ") (Not an error if this isn't really a URL",
                    e);
        // If that fails, lookup directly in repo.
        return fastFind(url, bestOnly);
      }
      AuSearchSet searchSet;
      synchronized (hostAus) {
        searchSet = hostAus.get(normStem);
      }
      // If there are no AUs w/ matching stem, lookup directly in repo.
      if (searchSet == null || searchSet.isEmpty()) {
        if (log.isDebug3() ) log.debug3("findCachedUrls: No AUs for " + normStem);
        return fastFind(url, bestOnly);
      }

      // Even though it's a URL with a stem that matches one or more
      // definitional AUs, it still might be in a NamedArchivalUnit, so
      // can't rely just on the searchSet.

      if (contentReq.needsContent() && searchSet.isRecent404(normUrl)) {
        if (log.isDebug2()) {
    log.debug2("404 cache hit: " + normUrl);
        }
        recent404Hits++;
        return Collections.EMPTY_LIST;
      }

      if (paramAuSearchUseV2Repo) {
        // Search V2 repo index
        fUStats.v2Invocations++;
        res = findCachedUrlsV2(normUrl, contentReq, bestOnly, searchSet, normUrl);
        if (!res.isEmpty()) {
          fUStats.v2Results++;
          return res;
        } else {
          // if none found and caller requires a CU with content, return none
          switch (contentReq) {
          case HasContent:
            // not found.  Add it to 404 cache
            if (log.isDebug2()) {
              log.debug2("Adding to 404 cache: " + normUrl + ", " + searchSet);
            }
            searchSet.addRecent404(normUrl);
            return Collections.emptyList();
          }
          // otherwise fall through to old method which can find CUs w/out content
        }
      }
      fUStats.v1Invocations++;
      res = findCachedUrlsV1(normUrl, contentReq, bestOnly, searchSet, normUrl);
      if (!res.isEmpty()) {
        fUStats.v1Results++;
      }
      return res;
    }

    private List<CachedUrl> findCachedUrlsV2(String url,
                                             CuContentReq contentReq,
                                             boolean bestOnly,
                                             AuSearchSet searchSet,
                                             String normUrlGeneric) {
      // We don't know what AU it might be in, so can't do plugin-dependent
      // normalization yet.  But only need to do generic normalization once.
      // XXX This isn't quite right, as plugin-specific normalization is
      // normally done first, but we don't think it ever actually matters
      //
      // XXX There is a problem with this when used by *Exploder() classes.
      // In the CLOCKSS case,  we expect huge numbers of AUs to share
      // the same stem,  eg. http://www.elsevier.com/ and each archive
      // that is exploded to include URLs for a large number of them.
      // The optimization that returns the most recent one if it matches
      // will help,  but perhaps not enough.  Ideally we want to search
      // for AUs on the basis of their base_url,  which for
      // ExplodedArchiveUnits is their sole definitional parameter,  so
      // is known unique.

      // Several potentially expensive(ish) operations could potentially be
      // factored out of the loop(s): generic URL normalization, and
      // ArchiveMemberSpec.fromUrl().
      if (log.isDebug2()) {
        log.debug2("findCachedUrlsV2(" + url + ", " + contentReq + ")");
      }
      boolean isTrace = log.isDebug3();
      List<CachedUrl> res = new ArrayList<CachedUrl>(bestOnly ? 1 : 15);
      CachedUrl bestCu = null;
      ArchivalUnit bestAu = null;
      Map<String,List<ArchivalUnit>> urlAus = new HashMap<>();
      int bestScore = 8;
      int numContentChecks = 0;

      // Must apply each possible AU's URL normalizer as they may produce
      // different results
      for (ArchivalUnit au : searchSet) {
        if (!isActiveAu(au)) {
    // This loop can run concurrently with other threads manipulating
    // set of active AUs, so this AU might still disappear at any point.
    continue;
        }

        try {
    if (isTrace) {
      log.debug3("findCachedUrlsV2: " + url + " check "
           + au.toString());
    }
          fUStats.v2AusConsidered++;
  // 	String siteUrl = au.siteNormalizeUrl(noMembUrl);
          String normUrl = UrlUtil.normalizeUrl(url, au);
    if (!normUrl.equals(url)) {
      if (isTrace) log.debug3("Normalized to: " + normUrl);
    }
          String noMembUrl = normUrl;

          // Parse out any member spec.
          // Unit test for archive member lookup is in TestArchiveMembers
          ArchiveMemberSpec ams = ArchiveMemberSpec.fromUrl(au, normUrl);
          if (ams != null) {
            if (isTrace) log.debug2("Recognized archive member: " + ams);
            noMembUrl = ams.getUrl();
          }

          // Search for each distinct URL only once
          List<ArchivalUnit> aus = urlAus.get(noMembUrl);
          if (aus != null) {
            // If we have already looked up this URL just associate this new
            // AU with it
            aus.add(au);
            if (isTrace) {
              log.debug3("Not reconsidering: " + noMembUrl);
            }
            fUStats.v2RedundantUrls++;
            continue;
          }
          aus = new ArrayList<>();
          aus.add(au);
          urlAus.put(noMembUrl, aus);

          fUStats.v2AuUrlsConsidered++;
          for (Artifact art : repoMgr.findArtifactsByUrl(noMembUrl)) {
            ArchivalUnit artAu = getAuFromIdIfExists(art.getAuid());
            if (isTrace) {
              log.debug3("Checking art: " + art.getUri() + ", au: " + artAu);
            }
            if (artAu != null) {
              CachedUrl cu = artAu.makeCachedUrl(art.getUri());;
              // Inclusion in findArtifactsByUrl() implies hasContent(),
              // unless this is an archive member
              boolean hasCont = true;

              // Must reget the ArchiveMemberSpec, if any, as this may be a
              // different AU from the one we used above
              ArchiveMemberSpec ams2 = ArchiveMemberSpec.fromUrl(artAu, normUrl);

              // Skip this AU if its handling of this URL as an archive
              // member differs from the AU that the outer loop is
              // processing.  (We'll get back to this Artifact when the outer
              // loop gets to a different AU)
              if ((ams == null) ^ (ams2 == null)) {
                if (isTrace) {
                  log.debug3("Skipping because ams: " + ams + ", ams2: " + ams2);
                }
                continue;
              }
              if (ams2 != null) {
                // This is an archive member.  Must now check for content.
                // (This is *very* expensive - should skip if
                // ContentReq.DontCare?)
                cu = cu.getArchiveMemberCu(ams2);
                hasCont = cu.hasContent();
              }
              if (bestOnly) {
                int auScore = auScore(artAu, cu, contentReq, hasCont);
                switch (action(contentReq, hasCont)) {
                case Ignore:
                  break;
                case RetCu:
                  makeFirstCandidate(searchSet, artAu);
                  if (log.isDebug3()) log.debug3("findCachedUrls: ret: " + cu);
                  res.add(cu);
                  if (log.isDebug2()) log.debug2("res: " + res);
                  return res;
                case UpdateBest:
                  if (bestCu == null || auScore < bestScore) {
                    AuUtil.safeRelease(bestCu);
                    bestCu = cu;
                    bestAu = artAu;
                    bestScore = auScore;
                  } else {
                    AuUtil.safeRelease(cu);
                  }
                  break;
                }
              } else {
                switch (action(contentReq, hasCont)) {
                case Ignore:
                  break;
                case RetCu:
                case UpdateBest:
                  res.add(cu);
                  break;
                }
              }
            }
          }

        } catch (MalformedURLException ignore) {
    // ignored
        } catch (PluginBehaviorException ignore) {
    // ignored
        } catch (RuntimeException ignore) {
    // ignored
        }
      }
      if (bestOnly) {
        if (bestCu != null) {
    res.add(bestCu);
    makeFirstCandidate(searchSet, bestAu);
    if (isTrace) {
      log.debug3("bestCu was " +
           (bestCu == null ? "null" : bestCu.toString()));
    }
        } else if (numContentChecks >= paramMinDiskSearchesFor404Cache) {
    // not found.  Add it to 404 cache for all contentReq, as will only
    // check for HasContent
    if (log.isDebug2()) {
      log.debug2("Adding to 404 cache: " + normUrlGeneric + ", " + searchSet);
    }
    searchSet.addRecent404(normUrlGeneric);
        }
      }
      if (log.isDebug2()) log.debug2("res: " + res);
      return res;
    }

    // A simple version which makes a single request to the repo for the URL.
    // Doesn't properly handle per-AU normalization.
    // Unused.
    private List<CachedUrl> findCachedUrlsV2Quick(String url,
                                             CuContentReq contentReq,
                                             boolean bestOnly,
                                             AuSearchSet searchSet,
                                             String normUrl) {
      if (log.isDebug2()) {
        log.debug2("findCachedUrlsV2Quick(" + url + ", " + contentReq + ")");
      }
      List<CachedUrl> res = new ArrayList<CachedUrl>(bestOnly ? 1 : 15);
      CachedUrl bestCu = null;
      ArchivalUnit bestAu = null;
      int bestScore = 8;
      for (Artifact art : repoMgr.findArtifactsByUrl(url)) {
        ArchivalUnit au = getAuFromIdIfExists(art.getAuid());
        if (au != null) {
          CachedUrl cu = au.makeCachedUrl(art.getUri());
          if (bestOnly) {
            int auScore = auScore(au, cu, contentReq, true);
            switch (action(contentReq, true)) {
            case Ignore:
              break;
            case RetCu:
              makeFirstCandidate(searchSet, au);
              if (log.isDebug3()) log.debug3("findCachedUrls: ret: " + cu);
              res.add(cu);
              return res;
            case UpdateBest:
              if (bestCu == null || auScore < bestScore) {
                AuUtil.safeRelease(bestCu);
                bestCu = cu;
                bestAu = au;
                bestScore = auScore;
              } else {
                AuUtil.safeRelease(cu);
              }
              break;
            }
          } else {
            switch (action(contentReq, true)) {
            case Ignore:
              break;
            case RetCu:
            case UpdateBest:
              res.add(cu);
              break;
            }
          }
        }
      }
      if (bestOnly) {
        if (bestCu != null) {
    res.add(bestCu);
    makeFirstCandidate(searchSet, bestAu);
    if (log.isDebug3()) {
      log.debug3("bestCu was " +
           (bestCu == null ? "null" : bestCu.toString()));
    }
        }
      }
      return res;
    }

  /* Return either a list of all CUs with the given URL, or the best choice
   * if bestOnly is true.
   */
  // XXX refactor into CU generator & two consumers.

    private List<CachedUrl> findCachedUrlsV1(String url,
                                             CuContentReq contentReq,
                                             boolean bestOnly,
                                             AuSearchSet searchSet,
                                             String normUrl) {
      // We don't know what AU it might be in, so can't do plugin-dependent
      // normalization yet.  But only need to do generic normalization once.
      // XXX This isn't quite right, as plugin-specific normalization is
      // normally done first, but we don't think it ever actually matters
      //
      // XXX There is a problem with this when used by *Exploder() classes.
      // In the CLOCKSS case,  we expect huge numbers of AUs to share
      // the same stem,  eg. http://www.elsevier.com/ and each archive
      // that is exploded to include URLs for a large number of them.
      // The optimization that returns the most recent one if it matches
      // will help,  but perhaps not enough.  Ideally we want to search
      // for AUs on the basis of their base_url,  which for
      // ExplodedArchiveUnits is their sole definitional parameter,  so
      // is known unique.

      if (log.isDebug2()) {
        log.debug2("findCachedUrlsV1(" + url + ", " + contentReq + ")");
      }
      boolean isTrace = log.isDebug3();
      List<CachedUrl> res = new ArrayList<CachedUrl>(bestOnly ? 1 : 15);
      CachedUrl bestCu = null;
      ArchivalUnit bestAu = null;
      int bestScore = 8;
      int numContentChecks = 0;

      for (ArchivalUnit au : searchSet) {
        if (!isActiveAu(au)) {
    // This loop can run concurrently with other threads manipulating
    // set of active AUs, so this AU might still disappear at any point.
    continue;
        }
        try {
    if (isTrace) {
      log.debug3("findCachedUrls: " + normUrl + " check "
           + au.toString());
    }
          // Unit test for archive member lookup is in TestArchiveMembers
          String noMembUrl = normUrl;
          ArchiveMemberSpec ams = ArchiveMemberSpec.fromUrl(au, normUrl);
          if (ams != null) {
            if (isTrace) log.debug3("Recognized archive member: " + ams);
            noMembUrl = ams.getUrl();
          }

          // XXX move up
    String siteUrl = UrlUtil.normalizeUrl(noMembUrl, au);
    if (!siteUrl.equals(noMembUrl)) {
      if (isTrace) log.debug3("Site normalized to: " + siteUrl);
      noMembUrl = siteUrl;
    }
    if (au.shouldBeCached(noMembUrl)) {
      if (isTrace) {
        log.debug3("findCachedUrls: " + noMembUrl + " should be in "
             + au.getAuId());
      }
      CachedUrl cu = au.makeCachedUrl(noMembUrl);
      if (ams != null) {
        cu = cu.getArchiveMemberCu(ams);
      }
      if (isTrace) {
        log.debug3("findCachedUrls(" + normUrl + ") = " + cu);
      }
      if (cu == null) {
        // can this happen?
        continue;
      }
      boolean hasCont = false;
      if (contentReq.wantsContent()) {
        hasCont = cu.hasContent();
        numContentChecks++;
      }
      if (bestOnly) {
        int auScore = auScore(au, cu, contentReq, hasCont);
        switch (action(contentReq, hasCont)) {
        case Ignore:
          break;
        case RetCu:
          makeFirstCandidate(searchSet, au);
          if (isTrace) log.debug3("findCachedUrls: ret: " + cu);
          res.add(cu);
          return res;
        case UpdateBest:
          if (bestCu == null || auScore < bestScore) {
      AuUtil.safeRelease(bestCu);
      bestCu = cu;
      bestAu = au;
      bestScore = auScore;
          } else {
      AuUtil.safeRelease(cu);
          }
          break;
        }
      } else {
        switch (action(contentReq, hasCont)) {
        case Ignore:
          break;
        case RetCu:
        case UpdateBest:
          res.add(cu);
          break;
        }
      }
    }
        } catch (MalformedURLException ignore) {
    // ignored
        } catch (PluginBehaviorException ignore) {
    // ignored
        } catch (RuntimeException ignore) {
    // ignored
        }
      }
      if (bestOnly) {
        if (bestCu != null) {
    res.add(bestCu);
    makeFirstCandidate(searchSet, bestAu);
    if (isTrace) {
      log.debug3("bestCu was " +
           (bestCu == null ? "null" : bestCu.toString()));
    }
        } else if (numContentChecks >= paramMinDiskSearchesFor404Cache) {
    // not found.  Add it to 404 cache for all contentReq, as will only
    // check for HasContent
    if (log.isDebug2()) {
      log.debug2("Adding to 404 cache: " + normUrl + ", " + searchSet);
    }
    searchSet.addRecent404(normUrl);
        }
      }
      return res;
    }

    // Move the AU in which we found the CU to the head of the list, as it's
    // likely next request will be for the same AU.
    private void makeFirstCandidate(AuSearchSet searchSet, ArchivalUnit au) {
      searchSet.addToCache(au);
    }

  enum FindUrlAction {Ignore, RetCu, UpdateBest}

    FindUrlAction action(CuContentReq req, boolean hasContent) {
      switch (req) {
      case DontCare: // doret
        return FindUrlAction.RetCu;
      case PreferContent:
        if (hasContent) {
    return FindUrlAction.RetCu;
        } else {
    return FindUrlAction.UpdateBest;
        }
  //     case MostRecentContent:
      case HasContent:
        if (hasContent) {
    return FindUrlAction.RetCu;
        } else {
    return FindUrlAction.Ignore;
        }
      }
      return FindUrlAction.Ignore;
    }

    // Combine the various elements of desirability into a single score;
    // lower is better, zero is best.
    private int auScore(ArchivalUnit au, CachedUrl cu,
            CuContentReq contentReq,
            boolean hasContent) {
      if (cu == null) return 16;
      int res = 0;
      if (contentReq.wantsContent() && !hasContent) {
        res += 8;
      }
      if (isUnsubscribed(au)) res += 2;
      if (isDamaged(cu)) res += 1;
      return res;
    }

    private boolean isUnsubscribed(ArchivalUnit au) {
      return (getDaemon().isDetectClockssSubscription() &&
        (AuUtil.getAuState(au).getClockssSubscriptionStatus() !=
         AuState.CLOCKSS_SUB_YES));
    }

    // XXX
    private boolean isDamaged(CachedUrl cu) {
      return false;
    }

//   // return true if cu1 is newer than cu2, or cu2 is null
//   // XXX - should compare last-modified times, or crawl times?
//   private boolean cuNewerThan(CachedUrl cu1, CachedUrl cu2) {
//     if (cu2 == null) return true;
//     return false;
// //     CIProperties p1 = cu1.getProperties();
// //     CIProperties p2 = cu2.getProperties();
//     //     Long.parseLong(p1.getProperty(HttpFields.__LastModified, "-1"));
//   }

    /**
     * Return a list of all configured ArchivalUnits.  The UI relies on
     * this being sorted by au title.
     *
     * @return the List of aus
     */
    public List<ArchivalUnit> getAllAus() {
      synchronized (auMap) {
        if (auList == null) {
    long startSort = TimeBase.nowMs();
    List<ArchivalUnit> tmp = new ArrayList<ArchivalUnit>(auMap.values());
    Collections.sort(tmp, auComparator);
    auList = Collections.unmodifiableList(tmp);
    if (log.isDebug2()) {
      long diff = TimeBase.msSince(startSort);
      log.debug2("Sort AUs list: " + TimeUtil.timeIntervalToString(diff));
    }
        }
        return auList;
      }
    }

    /**
     * Return a randomly ordered list of all AUs.
     */
    // putting this here in PluginManager saves having to make an extra copy
    // of the list.
    public List<ArchivalUnit> getRandomizedAus() {
      synchronized (auMap) {
        return CollectionUtil.randomPermutation(auMap.values());
      }
    }

  // XXXX lots of things depend on this.  fix it for env where not all AUs
  // are created
    /** Return the AUIDs all AU that have been explicitly deactivated */
    public Collection<String> getInactiveAuIds() {
      return inactiveAuIds;
    }

  // XXXX lots of things depend on this.  fix it for env where not all AUs
  // are created
    /** Return true if the AUID is that of an AU that has been explicitly
     * deactivated */
    public boolean isInactiveAuId(String auid) {
      return inactiveAuIds.contains(auid);
    }

  private void queuePluginRegistryCrawls() {
    if (isCrawlPlugins()) {
      CrawlManager crawlMgr = getDaemon().getCrawlManager();
      for (ArchivalUnit au : getAllRegistryAus()) {
	      crawlMgr.startNewContentCrawl(au, null);
      }
    }
  }

    /** Return a collection of all RegistryArchivalUnits.  This is a subset
     * of getAllAus() */
    public Collection<ArchivalUnit> getAllRegistryAus() {
      return getRegistryPlugin().getAllAus();
    }

    /** Start a thread to fetch the title list (after AUs are started),
     * causing the keys to be computed and an initial sort */
    void triggerTitleSort() {
      LockssRunnable run =
    new LockssRunnable("Title Sorter") {
      public void lockssRun() {
        try {
          getDaemon().waitUntilAusStarted();
          findAllTitles();
        } catch (InterruptedException e) {
          // just exit
        }
      }
    };
      Thread th = new Thread(run);
      th.start();
    }

    /** Return all the known titles from the title db, sorted by title */
    public List findAllTitles() {
      synchronized (titleMonitor) {
        if (allTitles == null) {
    allTitles = new ArrayList(getTitleMap().keySet());
    Collections.sort(allTitles, CatalogueOrderComparator.SINGLETON);
        }
        return allTitles;
      }
    }

    /** Find all the plugins that support the given title */
    public Collection getTitlePlugins(String title) {
      Collection res = (Collection)getTitleMap().get(title);
      if (res != null) {
        return res;
      }
      return Collections.EMPTY_LIST;
    }

    /** Return all known TitleConfigs */
    public List<TitleConfig> findAllTitleConfigs() {
      synchronized (titleMonitor) {
        if (allTitleConfigs == null) {
    List titles = findAllTitles();
    List res = new ArrayList(titles.size());
    for (Iterator titer = titles.iterator(); titer.hasNext();) {
      String title = (String)titer.next();
      for (Iterator piter = getTitlePlugins(title).iterator();
           piter.hasNext();) {
        Plugin plugin = (Plugin)piter.next();
        TitleConfig tc = plugin.getTitleConfig(title);
        if (tc != null) {
          res.add(tc);
        } else {
          log.warning("getTitleConfig(" + plugin + ", " + title + ") = null");
        }
      }
    }
    allTitleConfigs = res;
        }
        return allTitleConfigs;
      }
    }

    public void resetTitles() {
      synchronized (titleMonitor) {
        titleMap = null;
        allTitles = null;
        allTitleConfigs = null;
      }
    }

    public Map getTitleMap() {
      synchronized (titleMonitor) {
        if (titleMap == null) {
    titleMap = buildTitleMap();
        }
        return titleMap;
      }
    }

    Map buildTitleMap() {
      Map map = new MultiValueMap();
      synchronized (pluginMap) {
        for (Iterator iter = getRegisteredPlugins().iterator();
       iter.hasNext();) {
    Plugin p = (Plugin)iter.next();
    Collection titles = p.getSupportedTitles();
    for (Iterator iter2 = titles.iterator(); iter2.hasNext();) {
      String title = (String)iter2.next();
      if (title != null) {
        map.put(title, p);
      }
    }
        }
      }
      return map;
    }

    /** Return a SortedMap mapping (human readable) plugin name to plugin
     * instance */
    public SortedMap getPluginNameMap() {
      SortedMap pMap = new TreeMap();
      synchronized (pluginMap) {
        for (Iterator iter = getRegisteredPlugins().iterator();
       iter.hasNext(); ) {
    Plugin p = (Plugin)iter.next();
    pMap.put(p.getPluginName(), p);
        }
      }
      return pMap;
    }

    /** @return All plugins that have been registered.  <i>Ie</i>, that are
     * either listed in org.lockss.plugin.registry, or were loaded by a
     * configured AU */
    public Collection getRegisteredPlugins() {
      return pluginMap.values();
    }

    /** @return loadable PluginInfo for plugin, or null */
    public PluginInfo getLoadablePluginInfo(Plugin plugin) {
      return (PluginInfo)pluginfoMap.get(getPluginKey(plugin));
    }

  /**
   * Load all plugin registry plugins.
   */
  void initLoadablePluginRegistries(List urls) {
    final String DEBUG_HEADER = "initLoadablePluginRegistries(): ";
    if (log.isDebug2()) log.debug2(DEBUG_HEADER + "urls = " + urls);
    // Load the keystore if necessary
    initKeystore(configMgr.getCurrentConfig());

    if (urls.isEmpty()) {
      return;
    }

    BinarySemaphore bs = new BinarySemaphore();
    // The life-span of this callback was until it has exhausted all the urls.
    InitialRegistryCallback regCallback = new InitialRegistryCallback(urls, bs);
    getDaemon().getCrawlManager().registerCrawlEventHandler(regCallback);
    List loadAus = new ArrayList();

    for (Iterator iter = urls.iterator(); iter.hasNext(); ) {
      String url = (String) iter.next();
      Configuration auConf = ConfigManager.newConfiguration();
      auConf.put(ConfigParamDescr.BASE_URL.getKey(), url);
      String auId = generateAuId(getRegistryPlugin(), auConf);
      String auKey = auKeyFromAuId(auId);

      // Only process this registry if it is new.
      if (!auMap.containsKey(auId)) {

        try {
          configureAu(getRegistryPlugin(), auConf, auId);
        } catch (ArchivalUnit.ConfigurationException ex) {
          log.error("Failed to configure AU " + auKey, ex);
          regCallback.crawlCompleted(url);
          continue;
        }

        ArchivalUnit registryAu = getAuFromId(auId);

        loadAus.add(registryAu);

        // Trigger a new content crawl if required.
        possiblyStartRegistryAuCrawl(registryAu, url, regCallback);
      } else {
        log.debug2("We already have this AU configured, notifying callback.");
        regCallback.crawlCompleted(url);
      }
    }

    // If another component is crawling plugins, wait for it to finish
    ServiceBinding pCrawler = getDaemon().getPluginsCrawler();
    if (pCrawler != null) {
      new Thread(
              () -> {
                waitRegistriesReady(pCrawler, bs);
              })
          .start();
    }

    // Wait for a while for the AU crawls to complete, then process all the
    // registries in the load list.
    log.debug("Waiting for loadable plugins to finish loading...");
    try {
      if (!bs.take(Deadline.in(registryTimeout))) {
	log.warning("Timed out waiting for registries to finish loading. " +
		    "Remaining registry URLs: " +
		    regCallback.getRegistryUrls());
      }
    } catch (InterruptedException ex) {
      log.warning("InterruptedException while waiting for registry crawls." +
		  "Remaining registry URL list: " +
		  regCallback.getRegistryUrls());
    }
    finally {
      getDaemon().getCrawlManager().unregisterCrawlEventHandler(regCallback);
    }
    processRegistryAus(loadAus);
  }

    RegistryPlugin getRegistryPlugin() {
      return (RegistryPlugin)getInternalPlugin(RegistryPlugin.PLUGIN_ID);
    }

    /**
     * Provides an internal plugin by its identifier, creating it if necessary.
     *
     * @param id
     *          A String with the plugin identifier.
     * @return a Plugin with the requested internal plugin.
     * @exception IllegalArgumentException
     *              if there is no internal plugin with the supplied identifier.
     */
    private synchronized Plugin getInternalPlugin(String id) {
      Plugin internalPlugin = internalPlugins.get(id);

      if (internalPlugin == null) {
        if (ImportPlugin.PLUGIN_ID.equals(id)) {
    internalPlugin = new ImportPlugin();
        } else if (RegistryPlugin.PLUGIN_ID.equals(id)) {
    internalPlugin = new RegistryPlugin();
        } else {
    throw new IllegalArgumentException("Unknown internal plugin id: " + id);
        }

        String pluginKey = pluginKeyFromName(id);
        internalPlugin.initPlugin(getDaemon());
        setPlugin(pluginKey, internalPlugin);
        internalPlugins.put(id, internalPlugin);
      }

      return internalPlugin;
    }

    /**
     * Provides the plugin used to import files into archival units.
     *
     * @return an ImportPlugin with the requested plugin.
     */
    public ImportPlugin getImportPlugin() {
      return (ImportPlugin)getInternalPlugin(ImportPlugin.PLUGIN_ID);
    }

    // Trigger a new content crawl on the registry AU if required.
    protected void possiblyStartRegistryAuCrawl(
        ArchivalUnit registryAu, String url, InitialRegistryCallback cb) {
      if (isCrawlPlugins()) {
        if (registryAu.shouldCrawlForNewContent(AuUtil.getAuState(registryAu))) {
          if (log.isDebug2()) log.debug2("Starting new crawl: " + registryAu);
          Map<String, String> extraData = new HashMap<>();
          extraData.put(KEY_URL, url);
          extraData.put(KEY_CALLER_ID, cb.idStr);
          getDaemon().getCrawlManager().startNewContentCrawl(registryAu,
							   extraData);
        } else {
          if (log.isDebug2()) log.debug2("No crawl needed: " + registryAu);
          // If we're not going to crawl this AU, let the callback know.
          cb.crawlCompleted(url);
        }
      }
    }

    private boolean isCrawlPlugins() {
      try {
        return
    getDaemon().getCrawlManager().isCrawlerEnabled() &&
    getDaemon().getCrawlMode().isCrawlPlugins();
      } catch (IllegalArgumentException e) {
        log.debug("Can't get CrawlManager: " + e);
        return false;
      }
    }

    private void waitRegistriesReady(ServiceBinding pCrawler,
             BinarySemaphore sem) {
      log.debug("Waiting until " + pCrawler +
          " reports plugin registries are ready");
      while (true) {
        try {
    if (getApiStatus(pCrawler).getPluginsReady()) {
      log.debug(pCrawler + " reports plugin registries are now ready");
      sem.give();
      return;
    } else {
      log.debug2("Plugin registries not ready");
    }
        } catch (IOException e) {
    log.warning("Can't check plugins crawler status", e);
        }
        try {
    Deadline.in(Constants.SECOND).sleep();
        } catch (InterruptedException ex) {}
      }
    }

    ApiStatus getApiStatus(ServiceBinding binding) throws LockssRestException {
      return new RestStatusClient(binding.getRestStem())
        .setTimeouts(60 * Constants.SECOND, 60 * Constants.SECOND)
        .getStatus();
    }

    /** Return true if any of the glob patterns in a list match the string */
    boolean globMatch(List<PathMatcher> patList, String name) {
      for (PathMatcher pat : patList) {
        if (pat.matches(Paths.get(name))) {
    return true;
        }
      }
      return false;
    }

    // Ensure plugins listed in o.l.plugin.registry or in jars listed in
    // o.l.plugin.registryJars are loaded.
    void synchStaticPluginList(Configuration config) {
      final String DEBUG_HEADER = "synchStaticPluginList(): ";
      if (log.isDebug2()) log.debug2(DEBUG_HEADER + "Invoked.");
      List<String> nameList = config.getList(PARAM_PLUGIN_REGISTRY);
      if (log.isDebug3()) log.debug3(DEBUG_HEADER + "nameList = " + nameList);
      for (String name : nameList) {
        String key = pluginKeyFromName(name);
        if (log.isDebug3()) log.debug3(DEBUG_HEADER + "key = " + key);
        ensurePluginLoaded(key);
      }

      List<String> jarList = config.getList(PARAM_PLUGIN_REGISTRY_JARS);
      if (log.isDebug3()) log.debug3(DEBUG_HEADER + "jarList = " + jarList);
      if (!jarList.isEmpty()) {
        // Construct list of jar glob patterns from param
        List<PathMatcher> jarPatList = new ArrayList<PathMatcher>();
        for (String jar : jarList) {
    jarPatList.add(FileSystems.getDefault().getPathMatcher("glob:" + jar));
        }
        Pattern pat =
    Pattern.compile(config.get(PARAM_PLUGIN_MEMBER_PATTERN,
             DEFAULT_PLUGIN_MEMBER_PATTERN));
        for (String name : getPluginJarSearchPath()) {
    if (log.isDebug3()) log.debug3(DEBUG_HEADER + "name = " + name);
    if (globMatch(jarPatList, name) ||
        globMatch(jarPatList, new File(name).getName())) {
      ensureJarPluginsLoaded(name, pat);
    }
        }
      }

      // remove plugins on retract list, unless they have one or more
      // configured AUs
      synchronized (pluginMap) {
        if (log.isDebug3()) log.debug3(DEBUG_HEADER + "retract = " + retract);
        if (retract != null) {
    for (Iterator iter = retract.iterator(); iter.hasNext(); ) {
      String name = (String)iter.next();
      String key = pluginKeyFromName(name);
      Plugin plug = getPlugin(key);
      if (plug != null && !isInternalPlugin(plug)) {
        Collection<ArchivalUnit> aus = plug.getAllAus();
        if (aus == null || aus.isEmpty()) {
          removePlugin(key);
        }
      }
    }
        }
      }
      if (log.isDebug2()) log.debug2(DEBUG_HEADER + "Done.");
    }

    // Load plugins in jar whose name matches PARAM_PLUGIN_MEMBER_PATTERN
    void ensureJarPluginsLoaded(String jarname, Pattern pat) {
      try {
        JarFile jar = new JarFile(jarname);
        for (Enumeration<JarEntry> en = jar.entries(); en.hasMoreElements(); ) {
    JarEntry ent = en.nextElement();
    Matcher mat = pat.matcher(ent.getName());
    if (mat.matches()) {
      String membname = mat.group(1);
      String plugname = membname.replace('/', '.');
      String plugkey = pluginKeyFromName(plugname);
      ensurePluginLoaded(plugkey);
    }
        }
      } catch (IOException e) {
        log.error("Couldn't open plugin registry jar: " + jarname);
      }
    }

  // The Spring Boot classloader returns URLs of the form
  // file:/path/to/XxxPlugin.jar!/ for files,
  // file:/path/to/uber.jar!/BOOT-INF/classes!/ for classes in jar, and
  // file:/path/to/uber.jar!/BOOT-INF/lib/lib-jar.jar!/ for embeded
  // dependency jars.  Include only files (as we don't yet know how to
  // search the others for plugins), as simple filenames.

  private static List<String> PLUGIN_JAR_SEACH_PATH;

  final static Pattern SPRING_CLASSPATH_URL = Pattern.compile("file:(.*)!/");

  /** Return the list of jars in which to search for plugin registry jars */
  private static List<String> getPluginJarSearchPath() {
    if (PLUGIN_JAR_SEACH_PATH == null) {
      List<String> res = new ArrayList<>();
      for (String url : clClassPath()) {
	Matcher mat = SPRING_CLASSPATH_URL.matcher(url);
	if (mat.matches()) {
	  String jarfile = mat.group(1);
	  if (jarfile.indexOf("!/") < 0) {
	    res.add(jarfile);
	  }
	} else {
	  res.add(url);
	}
      }
      PLUGIN_JAR_SEACH_PATH = res;
    }
    return PLUGIN_JAR_SEACH_PATH;
  }

  /** Return the classpath from the ClassLoader, if possible, else the
   * System property */
  private static List<String> clClassPath() {
    ClassLoader cl = Thread.currentThread().getContextClassLoader();
    if (cl instanceof URLClassLoader) {
      return Arrays.stream(((URLClassLoader)cl).getURLs())
	.map(URL::getFile)
	.collect(Collectors.toList());
    }
    String cp = System.getProperty("java.class.path");
    if (cp != null) {
      return StringUtil.breakAt(cp, File.pathSeparator);
    }
    return Collections.emptyList();
  }

    /**
     * Initialize the "blessed" loadable plugin directory.
     */
    private void initPluginDir() {
      if (pluginDir != null) {
        return;
      }

      File dir = configMgr.findConfiguredDataDir(PARAM_PLUGIN_LOCATION,
                   DEFAULT_PLUGIN_LOCATION);

      if (dir.isDirectory()) {
        log.debug("Plugin directory " + dir + " exists.  Cleaning up...");
        if (!FileUtil.emptyDir(dir)) {
    log.error("Unable to clean up plugin directory " + dir);
    return;
        }
      } else {
        // This should (hopefully) never ever happen.  Log an error and
        // return for now.
        log.error("Plugin directory " + dir + " cannot be created.  A file " +
      "already exists with that name!");
        return;
      }
      pluginDir = dir;
    }


  /*
   * Helper methods for handling loadable plugins.
   */

    /**
     * Initialize and return the keystore.
     * @param keystoreLoc Location of keystore to use to verify plugin
     * signatures.  Can be
     *  - Absolute path to file (starts with File.separator), or
     *  - URL of keystore (http: or file:), or
     *  - Resource name of keystore on classpath.
     * @param keystorePass Keystore password isn't required to verify
     * signatures so should usually be null.  If non-null, it must be
     * correct.
     */
    KeyStore initKeystore(String keystoreLoc, String keystorePass) {
      KeyStore ks = null;
      try {
        if (keystoreLoc == null) {
    log.error("Plugin keystore not specified, loadable plugins will" +
        "not be available.");
        } else {
    char[] passchar = null;
    if (keystorePass != null) {
      passchar = keystorePass.toCharArray();
    }
    log.debug("Loading keystore: " + keystoreLoc);
          ks = KeyStore.getInstance("JKS", "SUN");
      if (new File(keystoreLoc).exists()) {
      InputStream kin = new FileInputStream(new File(keystoreLoc));
      try {
        ks.load(kin, passchar);
      } finally {
        IOUtil.safeClose(kin);
      }
    } else if (UrlUtil.isHttpOrHttpsUrl(keystoreLoc) ||
                     UrlUtil.isFileUrl(keystoreLoc)) {
      URL keystoreUrl = new URL(keystoreLoc);
            ks.load(keystoreUrl.openStream(), passchar);
          } else {
      InputStream kin =
        getClass().getClassLoader().getResourceAsStream(keystoreLoc);
      if (kin == null) {
        throw new IOException("Keystore resource not found: " +
            keystoreLoc);
      }
      ks.load(kin, passchar);
    }
        }

      } catch (Exception ex) {
        // ensure the keystore is null.
        log.error("Unable to load keystore from " + keystoreLoc, ex);
        return null;
      }

      log.debug("Keystore successfully initialized.");
      return ks;
    }

    private boolean isKeystoreInited() {
      return keystoreInited;
    }

    private void initKeystore(Configuration config) {
      String keystoreLoc;
      String keystorePass;

      if (!isKeystoreInited()) {
        keystoreLoc = config.get(PARAM_USER_KEYSTORE_LOCATION);
        if (!StringUtil.isNullString(keystoreLoc)) {
    keystorePass = config.get(PARAM_USER_KEYSTORE_PASSWORD,
            DEFAULT_KEYSTORE_PASSWORD);
        } else {
    keystoreLoc = config.get(PARAM_KEYSTORE_LOCATION,
           DEFAULT_KEYSTORE_LOCATION);
    keystorePass = config.get(PARAM_KEYSTORE_PASSWORD,
            DEFAULT_KEYSTORE_PASSWORD);
        }
        keystore = initKeystore(keystoreLoc, keystorePass);
        if (keystore != null) {
          keystoreInited = true;
        }
      }
    }

  // used by unit tests.
  public void setKeystoreInited(boolean val) {
    this.keystoreInited = val;
  }


  final static Pattern LIB_JAR_PAT = Pattern.compile("lib/.*\\.jar$");

    /**
     * Given a file representing a JAR, construct a list of available plugin
     * classes to load, and a list of lib jars that should be on the
     * classpath.
     */
    private Pair<List<String>,List<String>> processJarPlugin(File blessedJar)
        throws IOException {
      JarFile jar = new JarFile(blessedJar);
      Manifest manifest = jar.getManifest();
      Map entries = manifest.getEntries();
      List<String> plugins = new ArrayList<>();
      List<String> libjars = new ArrayList<>();

      // Search manifest for plugin names
      for (Iterator manIter = entries.keySet().iterator(); manIter.hasNext();) {
        String key = (String)manIter.next();

        Attributes attrs = manifest.getAttributes(key);

        if (attrs.containsKey(LOADABLE_PLUGIN_ATTR)) {
    String s = StringUtil.replaceString(key, "/", ".");

    String pluginName = null;

    if (StringUtil.endsWithIgnoreCase(key, ".class")) {
      pluginName = StringUtil.replaceString(s, ".class", "");
      log.debug2("Adding '" + pluginName + "' to plugin load list.");
      plugins.add(pluginName);
    } else if (StringUtil.endsWithIgnoreCase(key, ".xml")) {
      pluginName = StringUtil.replaceString(s, ".xml", "");
      log.debug2("Adding '" + pluginName + "' to plugin load list.");
      plugins.add(pluginName);
    }
        }

      }

      // Search for jars in lib/ dir
      File depLibDir =
        new File(FileUtil.getButExtension(blessedJar.toString()), "lib");
      for (Enumeration<JarEntry> en = jar.entries(); en.hasMoreElements(); ) {
        JarEntry ent = en.nextElement();
        Matcher mat = LIB_JAR_PAT.matcher(ent.getName());
        if (mat.matches()) {
    try {
      libjars.add(copyDependentJar(jar, ent, depLibDir));
    } catch (IOException e) {
      log.error("Couldn't copy plugin lib jars", e);
    }
        }
      }

      jar.close();

      return new ImmutablePair(plugins, libjars);
    }

    private String copyDependentJar(JarFile jfile, JarEntry ent, File libdir)
        throws IOException {
      if (!FileUtil.ensureDirExists(libdir)) {
        throw new IOException("Couldn't create plugin lib dir " + libdir);
      }
      try (InputStream is = jfile.getInputStream(ent)) {
        // ent.getName() is "lib/foo.jar"
        File outfile = new File(libdir, new File(ent.getName()).getName());
        log.debug("Copying " + ent + " to " + outfile);
        try (OutputStream os =
       new BufferedOutputStream(new FileOutputStream(outfile))) {
    StreamUtil.copy(is, os);
    return outfile.toString();
        }
      }
    }

    public synchronized void processRegistryAus(List registryAus) {
      processRegistryAus(registryAus, false);
    }

    /**
     * Run through the list of Registry AUs and verify and load any JARs
     * that need to be loaded.
     */
    public synchronized void processRegistryAus(List registryAus,
                  boolean startAus) {

      if (jarValidator == null) {
        jarValidator = new JarValidator(keystore, pluginDir);
      }
      jarValidator.allowExpired(acceptExpiredCertificates);

      // Create temporary plugin and classloader maps
      HashMap<String,PluginInfo> tmpMap = new HashMap<String,PluginInfo>();

      for (Iterator iter = registryAus.iterator(); iter.hasNext(); ) {
        ArchivalUnit au = (ArchivalUnit)iter.next();
        try {
    processOneRegistryAu(au, tmpMap);
        } catch (RuntimeException e) {
    log.error("Error processing plugin registry AU: " + au, e);
        }
      }

      // After the temporary plugin map has been built, install it into
      // the global maps.
      List classloaders = new ArrayList();

      // AUs running under plugins that have been replaced by new versions.
      List<ArchivalUnit> needRestartAus = new ArrayList();
      List<String> changedPluginKeys = new ArrayList<>();

      for (Map.Entry<String,PluginInfo> entry : tmpMap.entrySet()) {
        String key = entry.getKey();
        log.debug2("Adding to plugin map: " + key);
        PluginInfo info = entry.getValue();
        pluginfoMap.put(key, info);
        classloaders.add(info.getClassLoader());

        Plugin oldPlug = getPlugin(key);
        if (oldPlug != null && paramRestartAus) {
    Collection aus = oldPlug.getAllAus();
    if (aus != null) {
      needRestartAus.addAll(aus);
    }
        }

        Plugin newPlug = info.getPlugin();
        setPlugin(key, newPlug);
        if (startAus && newPlug != oldPlug) {
    changedPluginKeys.add(key);
        }
      }

      // Title DBs bundled with plugin jars are currently disabled.  To work
      // correctly, bundled tdb files must be removed from the config if/when
      // the containing plugin is unloaded/superseded.

  //     // Add the JAR's bundled titledb config (if any) to the ConfigManager.
  //     // Do this once at the end so as not to trigger more than one config
  //     // update & reload.
  //     configMgr.addTitleDbConfigFrom(classloaders);

      // Cleanup as a hint to GC.
      tmpMap.clear();
      tmpMap = null;

      if (!needRestartAus.isEmpty()) {
        restartAus(needRestartAus);
      }

      if (startAus && !changedPluginKeys.isEmpty()) {
        // Try to start any AUs configured for changed plugins, that didn't
        // previously start (either because the plugin didn't exist, or the
        // AU didn't successfully start with the old definition)
        configurePlugins(changedPluginKeys);
      }
    }

    protected void processOneRegistryAu(ArchivalUnit au, Map tmpMap) {
      log.debug2("processOneRegistryAu: " + au.getName());
      CachedUrlSet cus = au.getAuCachedUrlSet();

      for (CachedUrl cu : cus.getCuIterable()) {
        String url = cu.getUrl();
        if (StringUtil.endsWithIgnoreCase(url, ".jar")) {
    // This CachedUrl represents a plugin JAR, validate it and
    // process the plugins it contains.

    try {
      processOneRegistryJar(cu, url, au, tmpMap);
    } catch (RuntimeException e) {
      log.error("Error processing plugin jar: " + cu, e);
    }
        }
      }
    }

    protected void processOneRegistryJar(CachedUrl cu, String url,
                 ArchivalUnit au, Map tmpMap) {
      Integer curVersion = Integer.valueOf(cu.getVersion());

      if (cuNodeVersionMap.get(url) == null) {
        cuNodeVersionMap.put(url, Integer.valueOf(-1));
      }

      // If we've already visited this CU, skip it unless the current
      // repository node is a different version (older OR newer)
      Integer oldVersion = (Integer)cuNodeVersionMap.get(url);

      if (oldVersion.equals(curVersion)) {
        log.debug2(url + ": JAR repository and map versions are identical.  Skipping...");
        return;
      }

      File blessedJar = null;
      if (cu.getContentSize() == 0) {
        log.debug("Empty plugin jar: " + cu);
        return;
      } else {
        try {
    // Validate and bless the JAR file from the CU.
    blessedJar = jarValidator.getBlessedJar(cu);
    log.debug2("Plugin jar: " + url + " -> " + blessedJar);
        } catch (IOException ex) {
    log.error("Error processing jar file: " + url, ex);
          raiseAlert(Alert.cacheAlert(Alert.PLUGIN_JAR_NOT_VALIDATED),
                     String.format("Error validating plugin jar: %s\n%s",
                                   url, ex.getMessage()));
    return;
        } catch (JarValidator.JarValidationException ex) {
    log.error("Plugin jar did not validate: " + url, ex);
          raiseAlert(Alert.cacheAlert(Alert.PLUGIN_JAR_NOT_VALIDATED),
                     String.format("Plugin jar could not be validated: %s\n%s",
                                   url, ex.getMessage()));
    return;
        }
      }

      // Update the cuNodeVersion map now that we have the blessed Jar.
      cuNodeVersionMap.put(url, curVersion);

      if (blessedJar != null) {
        loadPluginsFromJar(blessedJar, url, au, cu, tmpMap);
      }
    }

    protected void loadPluginsFromJar(File jarFile, String url,
              ArchivalUnit au, CachedUrl cu,
              Map tmpMap) {
      // Get the list of plugins to load from this jar.
      List<String> loadPlugins;
      List<String> libJars;
      try {
        Pair<List<String>,List<String>> p = processJarPlugin(jarFile);
        loadPlugins = p.getLeft();
        libJars = p.getRight();
      } catch (IOException ex) {
        log.error("Error while getting list of plugins for " +
      jarFile);
        return; // skip this CU.

      }
      log.debug2("Blessed jar: " + jarFile + ", plugins: " + loadPlugins);

      // Although this -should- never happen, it's possible.
      if (loadPlugins.size() == 0) {
        log.warning("Jar " + jarFile +
        " does not contain any plugins.  Skipping...");
        return; // skip this CU.
      }

      // Load the plugin classes
      ClassLoader pluginLoader = null;
      URL blessedUrl;
      try {
        blessedUrl = jarFile.toURL();
        List<URL> cp = new ArrayList<>();
        cp.add(blessedUrl);
        for (String jar : libJars) {
    cp.add(new File(jar).toURL());
        }
        URL[] urls = cp.toArray(new URL[0]);
        log.debug2("Plugin classpath: " + cp);
        pluginLoader =
    preferLoadablePlugin
    ? new LoadablePluginClassLoader(urls, getClass().getClassLoader())
    : new URLClassLoader(urls, getClass().getClassLoader());
      } catch (MalformedURLException ex) {
        log.error("Malformed URL exception attempting to create " +
      "classloader for plugin JAR " + jarFile);
        return; // skip this CU.
      }

      for (String pluginName : loadPlugins) {
        String key = pluginKeyFromName(pluginName);

        Plugin plugin;
        PluginInfo info;
        try {
    info = retrievePlugin(pluginName, pluginLoader);
    if (info == null) {
      log.warning("Probable plugin packaging error: plugin " +
                        pluginName + " could not be loaded from " +
                        cu.getUrl());
            continue;
          } else {
            info.setCuUrl(url);
            info.setRegistryAu(au);
            List urls = info.getResourceUrls();
            if (urls != null && !urls.isEmpty()) {
              String jar = urls.get(0).toString();
              if (jar != null) {
                // If the blessed jar path is a substring of the jar:
                // url from which the actual plugin resource or class
                // was loaded, then it is a loadable plugin.
                boolean isLoadable =
                  jar.indexOf(blessedUrl.getFile()) > 0;
                info.setIsOnLoadablePath(isLoadable);
              }
            }
            plugin = info.getPlugin();
          }
        } catch (Exception ex) {
          log.error(String.format("Unable to load plugin %s", pluginName), ex);
          continue;
        }

        PluginVersion version = null;

        try {
          version = new PluginVersion(plugin.getVersion());
          info.setVersion(version);
        } catch (IllegalArgumentException ex) {
          // Don't let this runtime exception stop the daemon.  Skip the plugin.
          log.error(String.format("Skipping plugin %s: %s", pluginName, ex.getMessage()));
          // must stop plugin to enable it to be collected
          plugin.stopPlugin();
          return;
        }

        if (tmpMap.containsKey(key)) {
          // Plugin already exists in the temporary map, replace with
          // this one if a greater version
          PluginVersion otherVer = ((PluginInfo)tmpMap.get(key)).getVersion();

          if (version.toLong() > otherVer.toLong()) {
            if (log.isDebug2()) {
              log.debug2("Plugin " + plugin.getPluginId() + ": version " +
                         version + " is newer than version " + otherVer +
                         " already in temp map, replacing.");
            }
            // Overwrite old key in temp map
            tmpMap.put(key, info);
          } else {
            // must stop plugin to enable it to be collected
            plugin.stopPlugin();
          }
        } else if (pluginMap.containsKey(key)) {
          // Plugin already exists in the global plugin map.
          // Replace it with a new version if one is available.
          log.debug2("Plugin " + key + " is already in global pluginMap.");
          Plugin otherPlugin = getPlugin(key);
          PluginVersion otherVer =
            new PluginVersion(otherPlugin.getVersion());
          if (version.toLong() > otherVer.toLong()) {
            if (log.isDebug2()) {
              log.debug2("Plugin " + plugin.getPluginId() +
                         ": Newer version " + version +
                         " loaded, will be installed.");
            }
            tmpMap.put(key, info);
          } else {
            if (log.isDebug2()) {
              log.debug2("Plugin " + plugin.getPluginId() +
                         ": Older version " + version +
                         " loaded, will not be installed.");
            }
            // must stop plugin to enable it to be collected
            plugin.stopPlugin();
          }
        } else {
          // Plugin doesn't exist and isn't in the temporary map, add it.
          tmpMap.put(key, info);

          if (log.isDebug2()) {
            log.debug2("Plugin " + plugin.getPluginId() +
                       ": No previous version in temp map.");
          }
        }
      }
    }

    /**
     * Convenience method to provide an indication of whether an Archival Unit is
     * not configured in the daemon and it's not inactive.
     *
     * @param auId
     *          A String with the Archival Unit identifier.
     * @return a boolean with <code>true</code> if the Archival Unit is not
     *         configured in the daemon and it's not inactive, <code>false</code>
     *         otherwise.
     */
    public boolean isNotConfiguredAndNotInactive(String auId) {
      // The third element of the intersection below is there to handle the
      // potential race condition triggered by the Archival Unit being reactivated
      // between the first two calls.
      return getAuFromIdIfExists(auId) == null && !isInactiveAuId(auId)
    && getAuFromIdIfExists(auId) == null;
    }

  /**
   * CrawlManager callback that is responsible for handling Registry
   * AUs when they're finished with their initial crawls.
   */
  static class InitialRegistryCallback extends CrawlEventHandler.Base  {
    List registryUrls;
    String idStr = randomUUID().toString();
    private BinarySemaphore bs;

      /*
       * Set the initial size of the list of registry URLs to process.
       */
      public InitialRegistryCallback(List registryUrls, BinarySemaphore bs) {
        this.registryUrls = Collections.synchronizedList(new ArrayList(registryUrls));
        this.bs = bs;
        if (log.isDebug2()) log.debug2("InitialRegistryCallback: " +
            registryUrls);
        if (registryUrls.isEmpty()) {
          bs.give();
        }
      }

      public void crawlCompleted(String url) {
        // Remove urls from registryUrls as they finish crawling (or it is
        // determine that they don't need to be crawled).  When registryUrls
        // is empty signal the waiting process that it may proceed to load
        // plugins we're done running crawls on all the plugin registries,
        // and we can load the plugin classes.
        registryUrls.remove(url);
        if (log.isDebug2()) log.debug2("Registry crawl complete: " + url +
            ", " + registryUrls.size() + " left");
        if (registryUrls.isEmpty()) {
          if (log.isDebug2()) log.debug2("Registry crawls complete");
          bs.give();
        }
      }

    /**
     * Used only in the case that our semaphore throws an Interrupted
     * exception -- we can print this list to see what was left.
     */
    public List getRegistryUrls() {
      return registryUrls;
    }

    @Override
    protected void handleNewContentCompleted(CrawlEvent event) {
      Map<String, Object> extraData = event.getExtraData();
      if (extraData != null && extraData.containsKey(KEY_CALLER_ID)) {
        if (idStr.equals(extraData.get(KEY_CALLER_ID))) {
          String url = (String) extraData.get(KEY_COOKIE);
          crawlCompleted(url);
        }
      }
    }
  }

    /*
     * Provides the indication of whether the URLs (and their content) of an
     * archival unit should be obtained from a web service instead of the
     * repository.
     *
     * @return a boolean with <code>true</code> if the URLs (and their content) of
     * an archival unit should be obtained from a web service, <code>false</code>
     * otherwise.
     */
    // TK calls to this remain in order to document previous behavior
    public boolean isAuContentFromWs() {
      return false;
  //     return paramAuContentFromWs;
    }

    public boolean isStartAusOnDemand() {
      return !paramStartAllAus;
    }

    public boolean isLoadAllPlugins() {
      return !paramLoadAllPlugins;
    }

    /**
     * A simple class that wraps information about a loadable plugin,
     * used during the loading process.
     */
    public static class PluginInfo {
      private Plugin plugin;
      private ArchivalUnit registryAu;
      private PluginVersion version;
      private ClassLoader classLoader;
      private String cuUrl;
      private URL jarUrl;
      private List<String> resourceUrls;
      private boolean isOnLoadablePath = false;

      public PluginInfo(Plugin plugin, ClassLoader classLoader,
            List<String> resourceUrls) {
        this.plugin = plugin;
        this.classLoader = classLoader;
        this.resourceUrls = resourceUrls;
      }

      public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("[PI: ");
        sb.append(plugin.getPluginName());
        sb.append(", ");
        sb.append(cuUrl);
        sb.append(", ");
        sb.append(jarUrl);
        sb.append(", ");
        sb.append(resourceUrls);
        sb.append(", ");
        sb.append(isOnLoadablePath);
        sb.append("]");
        return sb.toString();
      }

      public Plugin getPlugin() {
        return plugin;
      }

      public PluginVersion getVersion() {
        return version;
      }

      public void setVersion(PluginVersion version) {
        this.version = version;
      }

      public ClassLoader getClassLoader() {
        return classLoader;
      }

      public String getCuUrl() {
        return cuUrl;
      }

      public void setCuUrl(String cuUrl) {
        this.cuUrl = cuUrl;
      }

      public URL getJarUrl() {
        return jarUrl;
      }

      public void setJarUrl(URL jarUrl) {
        this.jarUrl = jarUrl;
      }

      public List<String> getResourceUrls() {
        return resourceUrls;
      }

      public ArchivalUnit getRegistryAu() {
        return registryAu;
      }

      public void setRegistryAu(ArchivalUnit registryAu) {
        this.registryAu = registryAu;
      }

      public boolean isOnLoadablePath() {
        return isOnLoadablePath;
      }

      public void setIsOnLoadablePath(boolean val) {
        this.isOnLoadablePath = val;
      }
    }
  }
