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
package org.lockss.config;

import java.io.*;
import java.util.*;
import java.net.*;
import org.apache.commons.collections.Predicate;
import org.apache.commons.io.*;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.StringEscapeUtils;
import org.apache.oro.text.regex.*;
import org.lockss.app.*;
import org.lockss.account.*;
import org.lockss.clockss.*;
import org.lockss.daemon.*;
import org.lockss.hasher.*;
import org.lockss.mail.*;
import org.lockss.plugin.*;
import org.lockss.protocol.*;
import org.lockss.proxy.*;
import org.lockss.remote.*;
import org.lockss.repository.*;
import org.lockss.servlet.*;
import org.lockss.state.*;
import org.lockss.util.*;
import org.lockss.util.io.LockssSerializable;
import org.lockss.util.os.PlatformUtil;
import org.lockss.util.time.Deadline;
import org.lockss.util.time.TimeBase;
import org.lockss.util.time.TimeUtil;
import org.lockss.util.urlconn.*;
import javax.jms.Message;
import javax.jms.JMSException;
import org.lockss.jms.*;

/** ConfigManager loads and periodically reloads the LOCKSS configuration
 * parameters, and provides services for updating locally changeable
 * configuration.
 *
 * @ParamCategory Configuration
 *
 * @ParamCategoryDoc Configuration These parameters mostly pertain to the
 * configuration mechanism itself.
 *
 * @ParamCategoryDoc Platform Parameters with the
 * <code>org.lockss.config.platform prefix</code> are set by the
 * system-dependent startup scripts, based on information gathered by
 * running hostconfig.  They should generally not be set manually except in
 * testing environments.
 */
public class ConfigManager implements LockssManager {
  /** The common prefix string of all LOCKSS configuration parameters. */
  public static final String PREFIX = Configuration.PREFIX;

  /** Common prefix of platform config params */
  public static final String PLATFORM = Configuration.PLATFORM;
  public static final String DAEMON = Configuration.DAEMON;

  static final String MYPREFIX = PREFIX + "config.";

  /** The interval at which the daemon checks the various configuration
   * files, including title dbs, for changes.
   * @ParamCategory Tuning
   * @ParamRelevance Rare
   */
  static final String PARAM_RELOAD_INTERVAL = MYPREFIX + "reloadInterval";
  static final long DEFAULT_RELOAD_INTERVAL = 30 * Constants.MINUTE;

  /** If set to <i>hostname</i>:<i>port</i>, the configuration server will
   * be accessed via the specified proxy.  Leave unset for direct
   * connection.
   * @ParamCategory Platform
   */
  public static final String PARAM_PROPS_PROXY = PLATFORM + "propsProxy";

  /** If set, the authenticity of the config server will be checked using
   * this keystore.  The value is either an internal name designating a
   * resource (e.g. <tt>&quot;lockss-ca&quot;</tt>, to use the builtin
   * keystore containing the LOCKSS signing cert (see {@link
   * #builtinServerAuthKeystores}), or the filename of a keystore. Can only
   * be set in platform config.
   * @ParamCategory Platform
   */
  public static final String PARAM_SERVER_AUTH_KEYSTORE_NAME =
    MYPREFIX + "serverAuthKeystore";

  /** If set, the daemon will authenticate itself to the config server
   * using this keystore.  The value is the name of the keystore (defined
   * by additional <tt>org.lockss.keyMgr.keystore.&lt;id&gt;.<i>xxx</i></tt>
   * parameters (see {@link org.lockss.daemon.LockssKeyStoreManager}), or
   * <tt>&quot;lockss-ca&quot;</tt>, to use the builtin keystore containing
   * the LOCKSS signing cert.  Can only be set in platform config.
   * @ParamCategory Platform
   */
  public static final String PARAM_CLIENT_AUTH_KEYSTORE_NAME =
    MYPREFIX + "clientAuthKeystore";

  /** Map of internal name to resource location of keystore to use to check
   * authenticity of the config server */
  static Map<String,String> builtinServerAuthKeystores =
    MapUtil.map("lockss-ca", "org/lockss/config/lockss-ca.keystore");

  /** Interval at which the regular GET request to the config server will
   * include an {@value Constants.X_LOCKSS_INFO} with version and other
   * information.  Used in conjunction with logging hooks on the server.
   * @ParamRelevance Rare
   */
  static final String PARAM_SEND_VERSION_EVERY = MYPREFIX + "sendVersionEvery";
  static final long DEFAULT_SEND_VERSION_EVERY = 1 * Constants.DAY;

  static final String WDOG_PARAM_CONFIG = "Config";
  static final long WDOG_DEFAULT_CONFIG = 2 * Constants.HOUR;

  /** Path to local config directory, relative to entries on diskSpacePaths.
   * @ParamRelevance Rare
   */
  public static final String PARAM_CONFIG_PATH = MYPREFIX + "configFilePath";
  public static final String DEFAULT_CONFIG_PATH = "config";

  /** When logging new or changed config, truncate val at this length.
   * @ParamRelevance Rare
   */
  static final String PARAM_MAX_LOG_VAL_LEN =
    MYPREFIX + "maxLogValLen";
  static final int DEFAULT_MAX_LOG_VAL_LEN = 2000;

  /** Config param written to local config files to indicate file version.
   * Not intended to be set manually.
   * @ParamRelevance Rare
   */
  static final String PARAM_CONFIG_FILE_VERSION =
    MYPREFIX + "fileVersion.<filename>";

  /** Set false to disable scheduler.  Used only for unit tests.
   * @ParamRelevance Never
   */
  public static final String PARAM_NEW_SCHEDULER =
    HashService.PREFIX + "use.scheduler";
  static final boolean DEFAULT_NEW_SCHEDULER = true;

  /** Maximum number of AU config changes to to save up during a batch add
   * or remove operation, before writing them to au.txt  
   * @ParamRelevance Rare
   */
  public static final String PARAM_MAX_DEFERRED_AU_BATCH_SIZE =
    MYPREFIX + "maxDeferredAuBatchSize";
  public static final int DEFAULT_MAX_DEFERRED_AU_BATCH_SIZE = 100;

  /** Root of TitleDB definitions.  This is not an actual parameter.
   * @ParamRelevance Never
   */
  public static final String PARAM_TITLE_DB = Configuration.PREFIX + "title";
  /** Prefix of TitleDB definitions.  */
  public static final String PREFIX_TITLE_DB = PARAM_TITLE_DB + ".";

  /** List of URLs of title DBs configured locally using UI.  Do not set
   * manually
   * @ParamRelevance Never
   * @ParamAuto
   */
  public static final String PARAM_USER_TITLE_DB_URLS =
    Configuration.PREFIX + "userTitleDbs";

  /** List of URLs of title DBs to load.  Normally set in lockss.xml
   * @ParamRelevance Common
   */
  public static final String PARAM_TITLE_DB_URLS =
    Configuration.PREFIX + "titleDbs";

  /** List of URLs of auxilliary config files
   * @ParamRelevance LessCommon
   */
  public static final String PARAM_AUX_PROP_URLS =
    Configuration.PREFIX + "auxPropUrls";

  /** false disables SSL SNI name checking, compatible with Java 6 and
   * misconfigured servers.
   * @ParamRelevance BackwardCompatibility
   */
  public static final String PARAM_JSSE_ENABLESNIEXTENSION =
    PREFIX + "jsse.enableSNIExtension";
  static final boolean DEFAULT_JSSE_ENABLESNIEXTENSION = true;

  /** Should be set to allowed TCP ports, based on platform- (and group-)
   * dependent packet filters */
  public static final String PARAM_UNFILTERED_TCP_PORTS =
    Configuration.PLATFORM + "unfilteredTcpPorts";

  public static final String PARAM_UNFILTERED_UDP_PORTS =
    Configuration.PLATFORM + "unfilteredUdpPorts";

  /** Parameters whose values are more prop URLs */
  static final Map<String, Map<String, Object>> URL_PARAMS;
  static
  {
    URL_PARAMS = new HashMap<String, Map<String, Object>>();

    Map<String, Object> auxPropsMap = new HashMap<String, Object>();
    auxPropsMap.put("message", "auxilliary props");
    URL_PARAMS.put(PARAM_AUX_PROP_URLS, auxPropsMap);

    Map<String, Object> userTitleDbMap = new HashMap<String, Object>();
    auxPropsMap.put("message", "user title DBs");
    URL_PARAMS.put(PARAM_USER_TITLE_DB_URLS, userTitleDbMap);

    Map<String, Object> globalTitleDbMap = new HashMap<String, Object>();
    auxPropsMap.put("message", "global titledb");
    URL_PARAMS.put(PARAM_TITLE_DB_URLS, globalTitleDbMap);
  }

  /** Place to put temporary files and directories.  If not set,
   * java.io.tmpdir System property is used.  On a busy, capacious LOCKSS
   * box, this should be a minimum of 50-100MB.
   * @ParamCategory Platform
   * @ParamRelevance Required
   */
  public static final String PARAM_TMPDIR = PLATFORM + "tmpDir";

  /** Used only for testing.  The daemon version is normally loaded from a
   * file created during the build process.  If that file is absent, the
   * daemon version will be obtained from this param, if set.
   * @ParamRelevance Never
   */
  public static final String PARAM_DAEMON_VERSION = DAEMON + "version";

  /** Platform version string (<i>name</i>-<i>ver</i> or
   * <i>name</i>-<i>ver</i>-<i>suffix</i> . <i>Eg</i>, Linux RPM-1).
   * @ParamCategory Platform
   * @ParamRelevance Common
   */
  public static final String PARAM_PLATFORM_VERSION = PLATFORM + "version";

  /** Fully qualified host name (fqdn).  
   * @ParamCategory Platform
   */
  public static final String PARAM_PLATFORM_FQDN = PLATFORM + "fqdn";

  /** Project name (CLOCKSS or LOCKSS)
   * @ParamCategory Platform
   * @ParamRelevance Required
   */
  public static final String PARAM_PLATFORM_PROJECT = PLATFORM + "project";
  public static final String DEFAULT_PLATFORM_PROJECT = "lockss";

  /** Group names.  Boxes with at least one group in common will
   * participate in each others' polls.  Also used to evaluate group=
   * config file conditional,
   * @ParamCategory Platform
   * @ParamRelevance Required
   */
  public static final String PARAM_DAEMON_GROUPS = DAEMON + "groups";
  public static final String DEFAULT_DAEMON_GROUP = "nogroup";
  public static final List DEFAULT_DAEMON_GROUP_LIST =
    ListUtil.list(DEFAULT_DAEMON_GROUP);

  /** Local IP address
   * @ParamCategory Platform
   * @ParamRelevance Required
   */
  public static final String PARAM_PLATFORM_IP_ADDRESS =
    PLATFORM + "localIPAddress";

  /** Second IP address, for CLOCKSS subscription detection
   * @ParamCategory Platform
   * @ParamRelevance Obsolescent
   */
  public static final String PARAM_PLATFORM_SECOND_IP_ADDRESS =
    PLATFORM + "secondIP";

  /** LCAP V3 identity string.  Of the form
   * <code><i>proto</i>:[<i>ip-addr</i>:<i>port</i>]</code>; <i>eg</i>,
   * <code>TCP:[10.33.44.55:9729]</code> or
   * <code>tcp:[0:0:00:0000:0:0:0:1]:9729</code> .  Other boxes in the
   * network must be able to reach this one by connecting to the specified
   * host and port.  If behind NAT, this should be the external, routable,
   * NAT address.
   * @ParamCategory Platform
   * @ParamRelevance Required
   */
  public static final String PARAM_PLATFORM_LOCAL_V3_IDENTITY =
    PLATFORM + "v3.identity";

  /** Initial value of ACL controlling access to the admin UI.  Normally
   * set by hostconfig.  Once the access list is edited in the UI, that
   * list takes precedence over this one.  However, any changes to this one
   * after that point (<i>ie</i>, by rerunning hostconfig), will take
   * effect and be reflected in the list visible in the UI.
   * @ParamCategory Platform
   * @ParamRelevance Common
   */
  public static final String PARAM_PLATFORM_ACCESS_SUBNET =
    PLATFORM + "accesssubnet";

  /** List of filesystem paths to space available to store content and
   * other files
   * @ParamCategory Platform
   * @ParamRelevance Required
   */
  public static final String PARAM_PLATFORM_DISK_SPACE_LIST =
    PLATFORM + "diskSpacePaths";

  /** Email address to which various alerts, reports and backup file may be
   * sent.
   * @ParamCategory Platform
   * @ParamRelevance Common
   */
  public static final String PARAM_PLATFORM_ADMIN_EMAIL =
    PLATFORM + "sysadminemail";
  static final String PARAM_PLATFORM_LOG_DIR = PLATFORM + "logdirectory";
  static final String PARAM_PLATFORM_LOG_FILE = PLATFORM + "logfile";

  /** SMTP relay host that will accept mail from this host.
   * @ParamCategory Platform
   * @ParamRelevance Common
   */
  public static final String PARAM_PLATFORM_SMTP_HOST = PLATFORM + "smtphost";

  /** SMTP relay port, if not the default
   * @ParamCategory Platform
   * @ParamRelevance Rare
   */
  public static final String PARAM_PLATFORM_SMTP_PORT = PLATFORM + "smtpport";

  /** If true, local copies of remote config files will be maintained, to
   * allow daemon to start when config server isn't available.
   * @ParamCategory Platform
   * @ParamRelevance Rare
   */
  public static final String PARAM_REMOTE_CONFIG_FAILOVER = PLATFORM +
    "remoteConfigFailover";
  public static final boolean DEFAULT_REMOTE_CONFIG_FAILOVER = true;

  /** Dir in which to store local copies of remote config files.
   * @ParamCategory Platform
   * @ParamRelevance Rare
   */
  public static final String PARAM_REMOTE_CONFIG_FAILOVER_DIR = PLATFORM +
    "remoteConfigFailoverDir";
  public static final String DEFAULT_REMOTE_CONFIG_FAILOVER_DIR = "remoteCopy";

  /** Maximum acceptable age of a remote config failover file, specified as
   * an integer followed by h, d, w or y for hours, days, weeks and years.
   * Zero means no age limit.
   * @ParamCategory Platform
   * @ParamRelevance Rare
   */
  public static final String PARAM_REMOTE_CONFIG_FAILOVER_MAX_AGE = PLATFORM +
    "remoteConfigFailoverMaxAge";
  public static final long DEFAULT_REMOTE_CONFIG_FAILOVER_MAX_AGE = 0;

  /** Checksum algorithm used to verify remote config failover file
   * @ParamCategory Platform
   * @ParamRelevance Rare
   */
  public static final String PARAM_REMOTE_CONFIG_FAILOVER_CHECKSUM_ALGORITHM =
    PLATFORM + "remoteConfigFailoverChecksumAlgorithm";
  public static final String DEFAULT_REMOTE_CONFIG_FAILOVER_CHECKSUM_ALGORITHM =
    "SHA-256";

  /** Failover file not accepted unless it has a checksum.
   * @ParamCategory Platform
   * @ParamRelevance Rare
   */
  public static final String PARAM_REMOTE_CONFIG_FAILOVER_CHECKSUM_REQUIRED =
    PLATFORM + "remoteConfigFailoverChecksumRequired";
  public static final boolean DEFAULT_REMOTE_CONFIG_FAILOVER_CHECKSUM_REQUIRED =
    true;


  public static final String CONFIG_FILE_UI_IP_ACCESS = "ui_ip_access.txt";
  public static final String CONFIG_FILE_PROXY_IP_ACCESS =
    "proxy_ip_access.txt";
  public static final String CONFIG_FILE_PLUGIN_CONFIG = "plugin.txt";
  public static final String CONFIG_FILE_AU_CONFIG = "au.txt";
  public static final String CONFIG_FILE_BUNDLED_TITLE_DB = "titledb.xml";
  public static final String CONFIG_FILE_CONTENT_SERVERS =
    "content_servers_config.txt";
  public static final String CONFIG_FILE_ACCESS_GROUPS =
    "access_groups_config.txt"; // not yet in use
  public static final String CONFIG_FILE_CRAWL_PROXY = "crawl_proxy.txt";
  public static final String CONFIG_FILE_EXPERT = "expert_config.txt";

  /** Obsolescent - replaced by CONFIG_FILE_CONTENT_SERVERS */
  public static final String CONFIG_FILE_ICP_SERVER = "icp_server_config.txt";
  /** Obsolescent - replaced by CONFIG_FILE_CONTENT_SERVERS */
  public static final String CONFIG_FILE_AUDIT_PROXY =
    "audit_proxy_config.txt";

  public static final String REMOTE_CONFIG_FAILOVER_FILENAME =
    "remote_config_failover_info.xml";

  /** If set to a list of regexps, matching parameter names will be allowed
   * to be set in expert config, and loaded from expert_config.txt
   * @ParamRelevance Rare
   */
  public static final String PARAM_EXPERT_ALLOW = MYPREFIX + "expert.allow";
  public static final List DEFAULT_EXPERT_ALLOW = null;

  /** If set to a list of regexps, matching parameter names will not be
   * allowed to be set in expert config, and loaded from expert_config.txt.
   * The default prohibits using expert config to subvert platform
   * settings, change passwords or keystores, or cause the daemon to exit
   * @ParamRelevance Rare
   */
  public static final String PARAM_EXPERT_DENY = MYPREFIX + "expert.deny";
  static String ODLD = "^org\\.lockss\\.";
  public static final List DEFAULT_EXPERT_DENY =
    ListUtil.list("[pP]assword\\b",
		  ODLD +"platform\\.",
		  ODLD +"keystore\\.",
		  ODLD +"app\\.exit(Once|After|Immediately)$",
		  Perl5Compiler.quotemeta(PARAM_DAEMON_GROUPS),
		  Perl5Compiler.quotemeta(PARAM_AUX_PROP_URLS),
		  Perl5Compiler.quotemeta(IdentityManager.PARAM_LOCAL_IP),
		  Perl5Compiler.quotemeta(IdentityManager.PARAM_LOCAL_V3_IDENTITY),
		  Perl5Compiler.quotemeta(IdentityManager.PARAM_LOCAL_V3_PORT),
		  Perl5Compiler.quotemeta(IpAccessControl.PARAM_ERROR_BELOW_BITS),
		  Perl5Compiler.quotemeta(PARAM_EXPERT_ALLOW),
		  Perl5Compiler.quotemeta(PARAM_EXPERT_DENY),
		  Perl5Compiler.quotemeta(LockssDaemon.PARAM_TESTING_MODE)
		  );

  /** Describes a config file stored on the local disk, normally maintained
   * by the daemon. See writeCacheConfigFile(), et al. */
  public static class LocalFileDescr {
    String name;
    File file;
    KeyPredicate keyPred;
    Predicate includePred;
    boolean needReloadAfterWrite = true;

    LocalFileDescr(String name) {
      this.name = name;
    }

    String getName() {
      return name;
    }

    public File getFile() {
      return file;
    }

    public void setFile(File file) {
      this.file = file;
    }

    KeyPredicate getKeyPredicate() {
      return keyPred;
    }

    LocalFileDescr setKeyPredicate(KeyPredicate keyPred) {
      this.keyPred = keyPred;
      return this;
    }

    Predicate getIncludePredicate() {
      return includePred;
    }

    LocalFileDescr setIncludePredicate(Predicate pred) {
      this.includePred = pred;
      return this;
    }

    boolean isNeedReloadAfterWrite() {
      return needReloadAfterWrite;
    }

    LocalFileDescr setNeedReloadAfterWrite(boolean val) {
      this.needReloadAfterWrite = val;
      return this;
    }

    @Override
    public String toString() {
      return "[LocalFileDescr: name = " + name + ", file = " + file
	  + ", keyPred = " + keyPred + ", includePred = " + includePred
	  + ", needReloadAfterWrite = " + needReloadAfterWrite + "]";
    }
  }

  /** KeyPredicate determines legal keys, and whether illegal keys cause
   * file to fail or are just ignored */
  public interface KeyPredicate extends Predicate {
    public boolean failOnIllegalKey();
  }

  /** Always true predicate */
  KeyPredicate trueKeyPredicate = new KeyPredicate() {
      public boolean evaluate(Object obj) {
	return true;
      }
      public boolean failOnIllegalKey() {
	return false;
      }
      public String toString() {
	return "trueKeyPredicate";
      }};


  /** Allow only params below o.l.title and o.l.titleSet .  For use with
   * title DB files */
  static final String PREFIX_TITLE_SETS_DOT =
    PluginManager.PARAM_TITLE_SETS + ".";

  KeyPredicate titleDbOnlyPred = new KeyPredicate() {
      public boolean evaluate(Object obj) {
	if (obj instanceof String) {
	  return ((String)obj).startsWith(PREFIX_TITLE_DB) ||
	    ((String)obj).startsWith(PREFIX_TITLE_SETS_DOT);
	}
	return false;
      }
      public boolean failOnIllegalKey() {
	return true;
      }
      public String toString() {
	return "titleDbOnlyPred";
      }};

  /** Disallow keys prohibited in expert config file, defined by {@link
   * #PARAM_EXPERT_ALLOW} and {@link #PARAM_EXPERT_DENY} */
  public KeyPredicate expertConfigKeyPredicate = new KeyPredicate() {
      public boolean evaluate(Object obj) {
	if (obj instanceof String) {
	  return isLegalExpertConfigKey((String)obj);
	}
	return false;
      }
      public boolean failOnIllegalKey() {
	return false;
      }
      public String toString() {
	return "expertConfigKeyPredicate";
      }};

  /** Argless predicate that's true if expert config file should be
   * loaded */
  Predicate expertConfigIncludePredicate = new Predicate() {
      public boolean evaluate(Object obj) {
	return enableExpertConfig;
      }
      public String toString() {
	return "expertConfigIncludePredicate";
      }};

  public boolean isExpertConfigEnabled() {
    return enableExpertConfig;
  }

  /** A config param is:<ul><li>allowed if it matches a pattern in
   * <code>org.lockss.config.expert.allow</code> (if set), else</li>
   * <li>disallowed if it matches a pattern in
   * <code>org.lockss.config.expert.deny</code> (if set), else</li>
   * <li>Allowed.</li></ul>. */
  public boolean isLegalExpertConfigKey(String key) {
    if (!expertConfigAllowPats.isEmpty()) {
      for (Pattern pat : expertConfigAllowPats) {
	if (RegexpUtil.getMatcher().contains(key, pat)) {
	  return true;
	}
      }
    }
    if (expertConfigDenyPats.isEmpty()) {
      // If no deny pats, return true iff there are also no allow pats
      return expertConfigAllowPats == null;
    } else {
      for (Pattern pat : expertConfigDenyPats) {
	if (RegexpUtil.getMatcher().contains(key, pat)) {
	  return false;
	}
      }
    }
    // Didn't match either, and there are deny pats.
    return true;
  }

  /** Array of local cache config file names.  Do not use this directly,
   * call {@link #getLocalFileDescrs()} or {@link
   * #getLocalFileDescrMap()}. */
  LocalFileDescr cacheConfigFiles[] = {
    new LocalFileDescr(CONFIG_FILE_UI_IP_ACCESS),
    new LocalFileDescr(CONFIG_FILE_PROXY_IP_ACCESS),
    new LocalFileDescr(CONFIG_FILE_PLUGIN_CONFIG),
    // au.txt updates correspond to changes already made to running
    // structures, so needn't cause a config reload.
    new LocalFileDescr(CONFIG_FILE_AU_CONFIG)
    .setNeedReloadAfterWrite(false),
    new LocalFileDescr(CONFIG_FILE_ICP_SERVER), // obsolescent
    new LocalFileDescr(CONFIG_FILE_AUDIT_PROXY),	// obsolescent
    // must follow obsolescent icp server and audit proxy files
    new LocalFileDescr(CONFIG_FILE_CONTENT_SERVERS),
    new LocalFileDescr(CONFIG_FILE_ACCESS_GROUPS), // not yet in use
    new LocalFileDescr(CONFIG_FILE_CRAWL_PROXY),
    new LocalFileDescr(CONFIG_FILE_EXPERT)
    .setKeyPredicate(expertConfigKeyPredicate)
    .setIncludePredicate(expertConfigIncludePredicate),
  };

  private static final Logger log = Logger.getLogger("org.lockss.log.Config");

  /** A constant empty Configuration object */
  public static final Configuration EMPTY_CONFIGURATION = newConfiguration();
  static {
    EMPTY_CONFIGURATION.seal();
  }

  protected LockssApp theApp = null;

  private List configChangedCallbacks = new ArrayList();

  private LinkedHashMap<String,LocalFileDescr> cacheConfigFileMap = null;

  private List configUrlList;		// list of config file urls
  private List<String> clusterUrls;	// list of config urls that should
					// be included in cluster.xml
  // XXX needs synchronization
  private List pluginTitledbUrlList;	// list of titledb urls (usually
					// jar:) specified by plugins

  private List<String> loadedUrls = Collections.EMPTY_LIST;
  private List<String> specUrls = Collections.EMPTY_LIST;


  private String groupNames;		// daemon group names
  private String recentLoadError;

  // Platform config
  private static Configuration platformConfig =
    ConfigManager.EMPTY_CONFIGURATION;
  // Config of keystore used for loading configs.
  private Configuration platKeystoreConfig;

  // Current configuration instance.
  // Start with an empty one to avoid errors in the static accessors.
  private volatile Configuration currentConfig = EMPTY_CONFIGURATION;

  private OneShotSemaphore haveConfig = new OneShotSemaphore();

  private HandlerThread handlerThread; // reload handler thread

  private ConfigCache configCache;
  private LockssUrlConnectionPool connPool = new LockssUrlConnectionPool();
  private LockssSecureSocketFactory secureSockFact;

  long reloadInterval = 10 * Constants.MINUTE;
  private long sendVersionEvery = DEFAULT_SEND_VERSION_EVERY;
  private int maxDeferredAuBatchSize = DEFAULT_MAX_DEFERRED_AU_BATCH_SIZE;

  private List<Pattern> expertConfigAllowPats;
  private List<Pattern> expertConfigDenyPats;
  private boolean enableExpertConfig;

  private String bootstrapPropsUrl; // The daemon bootstrap properties URL.

  // The Configuration REST web service client.
  RestConfigClient restConfigClient = null;

  // The path to the directory containing any resource configuration files. 
  private static final String RESOURCE_CONFIG_DIR_PATH =
      "org/lockss/config/resourcefile";

  // A map of existing resource configuration files, indexed by file name.
  private Map<String, File> resourceConfigFiles = null;

  // The map of parent configuration files.
  Map<String, ConfigFile> parentConfigFile = new HashMap<String, ConfigFile>();

  // The counter of configuration reload requests. Accessed from separate
  // threads.
  private volatile int configReloadRequestCounter = 0;

  public ConfigManager() {
    this(null, null);

    URL_PARAMS.get(PARAM_AUX_PROP_URLS).put("predicate", trueKeyPredicate);
    URL_PARAMS.get(PARAM_USER_TITLE_DB_URLS).put("predicate", titleDbOnlyPred);
    URL_PARAMS.get(PARAM_TITLE_DB_URLS).put("predicate", titleDbOnlyPred);
  }

  public ConfigManager(List urls) {
    this(urls, null);
  }

  public ConfigManager(List urls, String groupNames) {
    this(null, urls, groupNames);
  }

  public ConfigManager(String bootstrapPropsUrl, List urls, String groupNames) {
    this(bootstrapPropsUrl, null, urls, groupNames);
  }

  public ConfigManager(String bootstrapPropsUrl, String restConfigServiceUrl,
      List urls, String groupNames) {
    this.bootstrapPropsUrl = bootstrapPropsUrl;
    this.restConfigClient = new RestConfigClient(restConfigServiceUrl);
    // Check whether this is not happening in a REST Configuration service
    // environment.
    if (restConfigClient.isActive()) {
      // Yes: Try to reload the configuration much more often.
      reloadInterval = 15 * Constants.SECOND;
    }
    if (urls != null) {
      configUrlList = new ArrayList(urls);
    }
    this.groupNames = groupNames;
    configCache = new ConfigCache(this);
    registerConfigurationCallback(MiscConfig.getConfigCallback());

    // Create the map of resource configuration files.
    resourceConfigFiles = populateResourceFileMap();
  }

  public void setClusterUrls(List<String> urls) {
    clusterUrls = urls;
  }

  public ConfigCache getConfigCache() {
    return configCache;
  }

  LockssUrlConnectionPool getConnectionPool() {
    return connPool;
  }

  public void initService(LockssApp app) throws LockssAppException {
    theApp = app;
  }

  /** Called to start each service in turn, after all services have been
   * initialized.  Service should extend this to perform any startup
   * necessary. */
  public void startService() {
    // Start the configuration handler that will periodically check the
    // configuration files.
    startHandler();
  }

  /** Reset to unconfigured state.  See LockssTestCase.tearDown(), where
   * this is called.)
   */
  public void stopService() {
    stopHandler();
    currentConfig = newConfiguration();
    // this currently runs afoul of Logger, which registers itself once
    // only, on first use.
    configChangedCallbacks = new ArrayList();
    configUrlList = null;
    cacheConfigInited = false;
    cacheConfigDir = null;
    // Reset the config cache.
    configCache = null;
    haveConfig = new OneShotSemaphore();
  }

  public LockssApp getApp() {
    return theApp;
  }
  protected static ConfigManager theMgr;

  public static ConfigManager  makeConfigManager() {
    theMgr = new ConfigManager();
    return theMgr;
  }

  public static ConfigManager makeConfigManager(List urls) {
    theMgr = new ConfigManager(urls);
    return theMgr;
  }

  public static ConfigManager makeConfigManager(List urls, String groupNames) {
    theMgr = new ConfigManager(urls, groupNames);
    return theMgr;
  }

  public static ConfigManager makeConfigManager(String bootstrapPropsUrl,
      String restConfigServiceUrl, List urls, String groupNames) {
    theMgr = new ConfigManager(bootstrapPropsUrl, restConfigServiceUrl, urls,
	groupNames);
    return theMgr;
  }

  public static ConfigManager getConfigManager() {
    return theMgr;
  }

  /** Factory to create instance of appropriate class */
  public static Configuration newConfiguration() {
    return new ConfigurationPropTreeImpl();
  }

  Configuration initNewConfiguration() {
    Configuration newConfig = newConfiguration();

    // Add platform-like params before calling loadList() as they affect
    // conditional processing
    if (groupNames != null) {
      newConfig.put(PARAM_DAEMON_GROUPS, groupNames.toLowerCase());
    }
    return newConfig;
  }

  /** Return current configuration, or an empty configuration if there is
   * no current configuration. */
  public static Configuration getCurrentConfig() {
    if (theMgr == null || theMgr.currentConfig == null) {
      return EMPTY_CONFIGURATION;
    }
    return theMgr.currentConfig;
  }

  public void setCurrentConfig(Configuration newConfig) {
    if (newConfig == null) {
      log.warning("attempt to install null Configuration");
    }
    currentConfig = newConfig;
  }

  /** Create a sealed Configuration object from a Properties */
  public static Configuration fromProperties(Properties props) {
    Configuration config = fromPropertiesUnsealed(props);
    config.seal();
    return config;
  }

  /** Create an unsealed Configuration object from a Properties */
  public static Configuration fromPropertiesUnsealed(Properties props) {
    Configuration config = new ConfigurationPropTreeImpl();
    for (Iterator iter = props.keySet().iterator(); iter.hasNext(); ) {
      String key = (String)iter.next();
      if (props.getProperty(key) == null) {
	log.error(key + " has no value");
	throw new RuntimeException("no value for " + key);
      }
      config.put(key, props.getProperty(key));
    }
    return config;
  }

  /**
   * Convenience methods for getting useful platform settings.
   */
  public static DaemonVersion getDaemonVersion() {
    DaemonVersion daemon = null;

    String ver = BuildInfo.getBuildProperty(BuildInfo.BUILD_RELEASENAME);
    // If BuildInfo doesn't give us a value, see if we already have it
    // in the props.  Useful for testing.
    if (ver == null) {
      ver = getCurrentConfig().get(PARAM_DAEMON_VERSION);
    }
    return ver == null ? null : new DaemonVersion(ver);
  }

  public static Configuration getPlatformConfig() {
    Configuration res = getCurrentConfig();
    if (res.isEmpty()) {
      res = platformConfig;
    }
    return res;
  }

  public static void setPlatformConfig(Configuration config) {
    if (platformConfig.isEmpty()) {
      platformConfig = config;
    } else {
      throw
	new IllegalStateException("Can't override existing  platform config");
    }
  }

  private static PlatformVersion platVer = null;

  public static PlatformVersion getPlatformVersion() {
    if (platVer == null) {
      String ver = getPlatformConfig().get(PARAM_PLATFORM_VERSION);
      if (ver != null) {
	try {
	  platVer = new PlatformVersion(ver);
	} catch (RuntimeException e) {
	  log.warning("Illegal platform version: " + ver, e);
	}
      }
    }
    return platVer;
  }

  public static String getPlatformGroups() {
    return getPlatformConfig().get(PARAM_DAEMON_GROUPS, DEFAULT_DAEMON_GROUP);
  }

  public static List getPlatformGroupList() {
    return getPlatformConfig().getList(PARAM_DAEMON_GROUPS,
				       DEFAULT_DAEMON_GROUP_LIST);
  }

  public static String getPlatformHostname() {
    return getPlatformConfig().get(PARAM_PLATFORM_FQDN);
  }

  public static String getPlatformProject() {
    return getPlatformConfig().get(PARAM_PLATFORM_PROJECT,
				   DEFAULT_PLATFORM_PROJECT);
  }

  /** Wait until the system is configured.  (<i>Ie</i>, until the first
   * time a configuration has been loaded.)
   * @param timer limits the time to wait.  If null, returns immediately.
   * @return true if configured, false if timer expired.
   */
  public boolean waitConfig(Deadline timer) {
    while (!haveConfig.isFull() && !timer.expired()) {
      try {
	haveConfig.waitFull(timer);
      } catch (InterruptedException e) {
	// no action - check timer
      }
    }
    return haveConfig.isFull();
  }

  /** Return true if the first config load has completed. */
  public boolean haveConfig() {
    return haveConfig.isFull();
  }

  /** Wait until the system is configured.  (<i>Ie</i>, until the first
   * time a configuration has been loaded.) */
  public boolean waitConfig() {
    return waitConfig(Deadline.MAX);
  }

  void runCallback(Configuration.Callback cb,
		   Configuration newConfig,
		   Configuration oldConfig,
		   Configuration.Differences diffs) {
    try {
      cb.configurationChanged(newConfig, oldConfig, diffs);
    } catch (Exception e) {
      log.error("callback threw", e);
    }
  }

  void runCallbacks(Configuration newConfig,
		    Configuration oldConfig,
		    Configuration.Differences diffs) {
    // run our own "callback"
    configurationChanged(newConfig, oldConfig, diffs);
    // It's tempting to do
    //     if (needImmediateReload) return;
    // here, as there's no point in running the callbacks yet if we're
    // going to do another config load immediately.  But that optimization
    // requires calculating diffs that encompass both loads.

    // copy the list of callbacks as it could change during the loop.
    List cblist = new ArrayList(configChangedCallbacks);
    for (Iterator iter = cblist.iterator(); iter.hasNext();) {
      if (Thread.currentThread().isInterrupted()) {
	throw new AbortConfigLoadException("Interrupted");
      }
      try {
	Configuration.Callback cb = (Configuration.Callback)iter.next();
	runCallback(cb, newConfig, oldConfig, diffs);
      } catch (RuntimeException e) {
	throw e;
      }
    }
  }

  public Configuration readConfig(List urlList) throws IOException {
    return readConfig(urlList, null);
  }

  /**
   * Return a new <code>Configuration</code> instance loaded from the
   * url list
   */
  public Configuration readConfig(List urlList, String groupNames)
      throws IOException {
    if (urlList == null) {
      return null;
    }

    Configuration newConfig = initNewConfiguration();
    loadList(newConfig, getConfigGenerations(urlList, true, true, "props"));
    return newConfig;
  }

  String getLoadErrorMessage(ConfigFile cf) {
    if (cf != null) {
      StringBuffer sb = new StringBuffer();
      sb.append("Error loading: ");
      sb.append(cf.getFileUrl());
      sb.append("<br>");
      sb.append(HtmlUtil.htmlEncode(cf.getLoadErrorMessage()));
      sb.append("<br>Last attempt: ");
      sb.append(new Date(cf.getLastAttemptTime()));
      return sb.toString();
    } else {
      return "Error loading unknown file: shouldn't happen";
    }
  }

  private Map generationMap = new HashMap();

  int getGeneration(String url) {
    Integer gen = (Integer)generationMap.get(url);
    if (gen == null) return -1;
    return gen.intValue();
  }

  void setGeneration(String url, int gen) {
    generationMap.put(url, new Integer(gen));
  }

  /**
   * @return a List of the urls from which the config is loaded.
   */
  public List getConfigUrlList() {
    return configUrlList;
  }

  /**
   * @return the List of config urls, including auxilliary files (e.g.,
   * specified by {@value PARAM_TITLE_DB_URLS}).
   */
  public List getSpecUrlList() {
    return specUrls;
  }

  /**
   * @return the List of urls from which the config was actually loaded.
   * This differs from {@link #getSpecUrlList()} in that it reflects any
   * failover to local copies.
   */
  public List getLoadedUrlList() {
    return loadedUrls;
  }

  ConfigFile.Generation getConfigGeneration(String url, boolean required,
					    boolean reload, String msg)
      throws IOException {
    return getConfigGeneration(url, required, reload, msg, null);
  }

  ConfigFile.Generation getConfigGeneration(String url, boolean required,
					    boolean reload, String msg,
					    KeyPredicate keyPred)
      throws IOException {
    log.debug2("Loading " + msg + " from: " + url);
    return getConfigGeneration(configCache.find(url),
			       required, reload, msg, keyPred);
  }

  ConfigFile.Generation getConfigGeneration(ConfigFile cf, boolean required,
					    boolean reload, String msg)
      throws IOException {
    return getConfigGeneration(cf, required, reload, msg, null);
  }

  ConfigFile.Generation getConfigGeneration(ConfigFile cf, boolean required,
					    boolean reload, String msg,
					    KeyPredicate keyPred)
      throws IOException {
    final String DEBUG_HEADER =
	"getConfigGeneration(cf, required, reload, msg, keyPred): ";
    if (log.isDebug2()) {
      log.debug2(DEBUG_HEADER + "cf = " + cf);
      log.debug2(DEBUG_HEADER + "required = " + required);
      log.debug2(DEBUG_HEADER + "reload = " + reload);
      log.debug2(DEBUG_HEADER + "msg = " + msg);
      log.debug2(DEBUG_HEADER + "keyPred = " + keyPred);
    }

    try {
      cf.setConnectionPool(connPool);
      if (sendVersionInfo != null && "props".equals(msg)) {
	cf.setProperty(Constants.X_LOCKSS_INFO, sendVersionInfo);
      } else {
	cf.setProperty(Constants.X_LOCKSS_INFO, null);
      }
      if (reload) {
	cf.setNeedsReload();
      }
      if (keyPred != null) {
	cf.setKeyPredicate(keyPred);
      }
      ConfigFile.Generation gen = cf.getGeneration();
      if (log.isDebug2()) log.debug2(DEBUG_HEADER + "gen = " + gen);
      return gen;
    } catch (IOException e) {
      String url = cf.getFileUrl();
      if (e instanceof FileNotFoundException &&
	  StringUtil.endsWithIgnoreCase(url, ".opt")) {
	log.debug2("Not loading props from nonexistent optional file: " + url);
	return null;
      } else if (required) {
	// This load failed.  Fail the whole thing.
	log.warning("Couldn't load props from " + url, e);
	recentLoadError = getLoadErrorMessage(cf);
	throw e;
      } else {
	if (e instanceof FileNotFoundException) {
	  log.debug3("Non-required file not found " + url);
	} else {
	  log.debug3("Unexpected error loading non-required file " + url, e);
	}
	return null;
      }
    }
  }

  public Configuration loadConfigFromFile(String url)
      throws IOException {
    final String DEBUG_HEADER = "loadConfigFromFile(): ";
    if (log.isDebug2()) log.debug2(DEBUG_HEADER + "url = " + url);
    ConfigFile cf = new FileConfigFile(url, this);
    ConfigFile.Generation gen = cf.getGeneration();
    return gen.getConfig();
  }

  boolean isChanged(ConfigFile.Generation gen) {
    boolean val = (gen.getGeneration() != getGeneration(gen.getUrl()));
    return (gen.getGeneration() != getGeneration(gen.getUrl()));
  }

  boolean isChanged(Collection<ConfigFile.Generation> gens) {
    for (Iterator<ConfigFile.Generation> iter = gens.iterator();
	iter.hasNext();) {
      ConfigFile.Generation gen = iter.next();
      if (gen != null && isChanged(gen)) {
	return true;
      }
    }
    return false;
  }

  List<ConfigFile.Generation> getConfigGenerations(Collection urls,
      boolean required, boolean reload, String msg) throws IOException {
    final String DEBUG_HEADER =
	"getConfigGenerations(urls, required, reload, msg): ";
    if (log.isDebug2()) {
      log.debug2(DEBUG_HEADER + StringUtil.loggableCollection(urls, "urls"));
      log.debug2(DEBUG_HEADER + "required = " + required);
      log.debug2(DEBUG_HEADER + "reload = " + reload);
      log.debug2(DEBUG_HEADER + "msg = " + msg);
    }
    return getConfigGenerations(urls, required, reload, msg,
				trueKeyPredicate);
  }

  List<ConfigFile.Generation> getConfigGenerations(Collection urls,
      boolean required, boolean reload, String msg, KeyPredicate keyPred)
      throws IOException {
    final String DEBUG_HEADER =
	"getConfigGenerations(urls, required, reload, msg, keyPred): ";
    if (log.isDebug2()) {
      log.debug2(DEBUG_HEADER + StringUtil.loggableCollection(urls, "urls"));
      log.debug2(DEBUG_HEADER + "required = " + required);
      log.debug2(DEBUG_HEADER + "reload = " + reload);
      log.debug2(DEBUG_HEADER + "msg = " + msg);
      log.debug2(DEBUG_HEADER + "keyPred = " + keyPred);
    }

    if (urls == null) return Collections.EMPTY_LIST;
    List<ConfigFile.Generation> res =
	new ArrayList<ConfigFile.Generation>(urls.size());
    for (Object o : urls) {
      if (Thread.currentThread().isInterrupted()) {
	throw new AbortConfigLoadException("Interrupted");
      }
      ConfigFile.Generation gen;
      if (o instanceof ConfigFile) {
	if (log.isDebug3()) log.debug3(DEBUG_HEADER + "Is ConfigFile.");
	gen = getConfigGeneration((ConfigFile)o, required, reload, msg,
				  keyPred);
      } else if (o instanceof LocalFileDescr) {
	if (log.isDebug3()) log.debug3(DEBUG_HEADER + "Is LocalFileDescr.");
	LocalFileDescr lfd = (LocalFileDescr)o;
	String filename = lfd.getFile().toString();
	Predicate includePred = lfd.getIncludePredicate();
	if (includePred != null && !includePred.evaluate(filename)) {
	  continue;
	}
	KeyPredicate pred = keyPred;
	if (lfd.getKeyPredicate() != null) {
	  pred = lfd.getKeyPredicate();
	}
	gen = getConfigGeneration(filename, required, reload, msg, pred);
      } else {
	if (log.isDebug3())
	  log.debug3(DEBUG_HEADER + "Neither ConfigFile nor LocalFileDescr.");
	String url = o.toString();
	gen = getConfigGeneration(url, required, reload, msg, keyPred);
      }
      if (log.isDebug3()) log.debug3(DEBUG_HEADER + "gen = " + gen);
      if (gen != null) {
	addGenerationToListIfNotInIt(gen, res);
	if (log.isDebug3()) log.debug3(DEBUG_HEADER
	    + StringUtil.loggableCollection(res, "res"));
	addReferencedUrls(gen, required, reload, msg, keyPred, res);
	if (log.isDebug3()) log.debug3(DEBUG_HEADER
	    + StringUtil.loggableCollection(res, "res"));
      }
    }
    if (log.isDebug2())
      log.debug2(DEBUG_HEADER + StringUtil.loggableCollection(res, "res"));
    return res;
  }

  /**
   * Adds a generation to a list only if it's not in the list yet.
   * 
   * @param gen
   *          A ConfigFile.Generation with the generation to be added.
   * @param genList
   *          A List<ConfigFile.Generation> with the list to which to add the
   *          passed generation.
   */
  void addGenerationToListIfNotInIt(ConfigFile.Generation gen,
      List<ConfigFile.Generation> genList) {
    final String DEBUG_HEADER = "addGenerationToListIfNotInIt(): ";
    // The URL of the generation to  be added.
    String url = gen.getUrl();

    // Loop through all the generations already in the list.
    for (ConfigFile.Generation existingGen : genList) {
      // Check whether it is the same generation as the one to be added.
      if (url.equals(existingGen.getUrl())) {
	// Yes: Do not add it.
	log.info(DEBUG_HEADER + "Not adding to the list generation = " + gen
	    + " because is a duplicate of existing generation = "
	    + existingGen);
	return;
      }
    }

    // Add the generation to the list because it's not there already.
    genList.add(gen);
  }

  /**
   * Adds to a target list any generation from a source list that it's not in
   * the target list yet.
   *
   * @param sourceGenList
   *          A List<ConfigFile.Generation> with the source list of generations.
   * @param targetGenList
   *          A List<ConfigFile.Generation> with the target list of generations.
   */
  void addGenerationsToListIfNotInIt(
      List<ConfigFile.Generation> sourceGenList,
      List<ConfigFile.Generation> targetGenList) {
    // Loop through all the generations in the source list.
    for (ConfigFile.Generation genToAdd : sourceGenList) {
      // Add it to the target list if not there already.
      addGenerationToListIfNotInIt(genToAdd, targetGenList);
    }
  }

  /**
   * Adds to a list of generations those other generations referenced by a
   * source generation.
   * 
   * @param gen
   *          A ConfigFile.Generation with the generation that may have
   *          references to other generations.
   * @param required
   *          A boolean with an indication of whether the generation is
   *          required.
   * @param reload
   *          A boolean with an indication of whether a reload is necessary.
   * @param msg
   *          A String with a message.
   * @param keyPred
   *          A KeyPredicate with the predicate.
   * @param targetList
   *          A List<ConfigFile.Generation> where to add the referenced
   *          generations.
   * @throws IOException
   *           if there are problems.
   */
  void addReferencedUrls(ConfigFile.Generation gen, boolean required,
      boolean reload, String msg, KeyPredicate keyPred,
      List<ConfigFile.Generation> targetList) throws IOException {
    final String DEBUG_HEADER = "addReferencedUrls(): ";
    if (log.isDebug2()) {
      log.debug2(DEBUG_HEADER + "gen = " + gen);
      log.debug2(DEBUG_HEADER + "required = " + required);
      log.debug2(DEBUG_HEADER + "reload = " + reload);
      log.debug2(DEBUG_HEADER + "msg = " + msg);
      log.debug2(DEBUG_HEADER + "keyPred = " + keyPred);
      log.debug2(DEBUG_HEADER
	  + StringUtil.loggableCollection(targetList, "targetList"));
    }

    // URL base for relative URLs.
    String base = gen.getUrl();
    ConfigFile cf = gen.getConfigFile();

    if (log.isDebug3()) log.debug3(DEBUG_HEADER + "base = " + base);

    // The configuration with potential references.
    Configuration config = gen.getConfig();
    if (log.isDebug3()) log.debug3(DEBUG_HEADER
	+ Configuration.loggableConfiguration(config, "config"));

    // Loop through all the referencing option keys. 
    for (String includingKey : URL_PARAMS.keySet()) {
      if (log.isDebug3())
	log.debug3(DEBUG_HEADER + "includingKey = " + includingKey);

      // Check whether the configuration with potential references contains this
      // option.
      if (config.containsKey(includingKey)) {
	if (log.isDebug3()) log.debug3(DEBUG_HEADER + "Found references.");

	// Get the configuration values under this key. 
	List<String> urls = config.getList(includingKey);
	if (log.isDebug3()) log.debug3(DEBUG_HEADER + "urls = " + urls);

	// Ignore an empty value.
	if (urls.size() == 0) {
	  log.warning(includingKey + " has empty value");
	  continue;
	}

	// Resolve the URLs obtained, if necessary.
	Collection<String> resolvedUrls = new ArrayList<String>(urls.size());
	for (String url : urls) {
	  if (log.isDebug3()) log.debug3(DEBUG_HEADER + "url = " + url);
	  String resolvedUrl = cf.resolveConfigUrl(url);
	  if (log.isDebug3())
	    log.debug3(DEBUG_HEADER + "resolvedUrl = " + resolvedUrl);
	  resolvedUrls.add(resolvedUrl);

	  // Remember the parent configuration file of this resolved URL. 
	  parentConfigFile.put(resolvedUrl, configCache.find(base));
	}

	// Add the generations of the resolved URLs to the list, if not there
	// already.
	String message = (String)(URL_PARAMS.get(includingKey).get("message"));
	if (log.isDebug3()) log.debug3(DEBUG_HEADER + "message = " + message);
	ConfigManager.KeyPredicate keyPredicate = (ConfigManager.KeyPredicate)
	    (URL_PARAMS.get(includingKey).get("predicate"));
	if (log.isDebug3())
	  log.debug3(DEBUG_HEADER + "keyPredicate = " + keyPredicate);

	addGenerationsToListIfNotInIt(getConfigGenerations(resolvedUrls,
	    false, reload, message, keyPredicate), targetList);

	if (log.isDebug3()) log.debug3(DEBUG_HEADER
	    + StringUtil.loggableCollection(targetList, "targetList"));
      } else {
	if (log.isDebug3()) log.debug3(DEBUG_HEADER + "No references found.");
      }
    }
  }

  List<ConfigFile.Generation> getStandardConfigGenerations(List urls,
      boolean reload) throws IOException {
    final String DEBUG_HEADER = "getStandardConfigGenerations(): ";
    if (log.isDebug2()) {
      log.debug2(DEBUG_HEADER + StringUtil.loggableCollection(urls, "urls"));
      log.debug2(DEBUG_HEADER + "reload = " + reload);
    }

    List<ConfigFile.Generation> res = new ArrayList<ConfigFile.Generation>(20);

    List<ConfigFile.Generation> configGens =
	getConfigGenerations(urls, true, reload, "props");
    if (log.isDebug3()) log.debug3(DEBUG_HEADER
	+ StringUtil.loggableCollection(configGens, "configGens"));

    addGenerationsToListIfNotInIt(configGens, res);

    addGenerationsToListIfNotInIt(getConfigGenerations(pluginTitledbUrlList,
	false, reload, "plugin-bundled titledb", titleDbOnlyPred), res);
    initCacheConfig(configGens);
    addGenerationsToListIfNotInIt(getCacheConfigGenerations(reload), res);
    if (log.isDebug2())
      log.debug2(DEBUG_HEADER + StringUtil.loggableCollection(res, "res"));
    return res;
  }

  List<ConfigFile.Generation> getCacheConfigGenerations(boolean reload)
      throws IOException {
    List<ConfigFile.Generation> localGens = getConfigGenerations(
	getLocalFileDescrs(), false, reload, "cache config");
    if (!localGens.isEmpty()) {
      hasLocalCacheConfig = true;
    }
    return localGens;
  }

  boolean updateConfig() {
    final String DEBUG_HEADER = "updateConfig(): ";
    boolean result = updateConfig(configUrlList);
    if (log.isDebug2()) log.debug2(DEBUG_HEADER + "result = " + result);
    return result;
  }

  public boolean updateConfig(List urls) {
    final String DEBUG_HEADER = "updateConfig(List): ";
    if (log.isDebug2())
      log.debug2(DEBUG_HEADER + StringUtil.loggableCollection(urls, "urls"));
    boolean res = updateConfigOnce(urls, true);
    if (log.isDebug3()) log.debug3(DEBUG_HEADER + "res = " + res);
    if (res) {
      if (!haveConfig.isFull()) {
	schedSetUpJmsNotifications();
      }
      haveConfig.fill();
    }
    connPool.closeIdleConnections(0);
    updateRemoteConfigFailover();

    if (log.isDebug2()) log.debug2(DEBUG_HEADER + "res = " + res);
    return res;
  }

  private String sendVersionInfo;
  private long lastSendVersion;
  private long startUpdateTime;
  private long lastUpdateTime;
  private long startCallbacksTime;

  public long getLastUpdateTime() {
    return lastUpdateTime;
  }

  public boolean updateConfigOnce(List urls, boolean reload) {
    final String DEBUG_HEADER = "updateConfigOnce(): ";
    if (log.isDebug2()) {
      log.debug2(DEBUG_HEADER + StringUtil.loggableCollection(urls, "urls"));
      log.debug2(DEBUG_HEADER + "reload = " + reload);
    }
    startUpdateTime = TimeBase.nowMs();
    if (log.isDebug3()) log.debug3(DEBUG_HEADER + 
	Configuration.loggableConfiguration(currentConfig, "currentConfig"));
    if (currentConfig.isEmpty()) {
      // first load preceded by platform config setup
      setupPlatformConfig(urls);
    }
    if (currentConfig.isEmpty() ||
	TimeBase.msSince(lastSendVersion) >= sendVersionEvery) {
      sendVersionInfo = getVersionString();
      lastSendVersion = TimeBase.nowMs();
    } else {
      sendVersionInfo = null;
    }
    List gens;
    try {
      gens = getStandardConfigGenerations(urls, reload);
    } catch (SocketException | UnknownHostException | FileNotFoundException e) {
      log.error("Error loading config: " + e.toString());
//       recentLoadError = e.toString();
      return false;
    } catch (IOException e) {
      log.error("Error loading config", e);
//       recentLoadError = e.toString();
      return false;
    }

    if (log.isDebug3())
      log.debug3(DEBUG_HEADER + "!isChanged(gens) = " + !isChanged(gens));
    if (!isChanged(gens)) {
      if (reloadInterval >= 10 * Constants.MINUTE) {
	log.info("Config up to date, not updated");
      }
      return false;
    }
    Configuration newConfig = initNewConfiguration();
    loadList(newConfig, gens);
    if (getApp() != null) {
      Configuration appConfig = getApp().getAppConfig();
      if (appConfig != null && !appConfig.isEmpty()) {
	log.debug("Adding app config: " + appConfig);
	newConfig.copyFrom(appConfig);
      }
    }
    if (log.isDebug3()) log.debug3(DEBUG_HEADER + "newConfig = " + newConfig);

    boolean did = installConfig(newConfig, gens);
    long tottime = TimeBase.msSince(startUpdateTime);
    long cbtime = TimeBase.msSince(startCallbacksTime);
    if (did) {
      lastUpdateTime = startUpdateTime;
    }
    if (log.isDebug2() || tottime > Constants.SECOND) {
      if (did) {
	log.debug("Reload time: "
		  + TimeUtil.timeIntervalToString(tottime - cbtime)
		  + ", cb time: " + TimeUtil.timeIntervalToString(cbtime));
      } else {
	log.debug("Reload time: " + TimeUtil.timeIntervalToString(tottime));
      }
    }
    if (log.isDebug2()) log.debug2(DEBUG_HEADER + "did = " + did);
    return did;
  }

  Properties getVersionProps() {
    Properties p = new Properties();
    PlatformVersion pver = getPlatformVersion();
    if (pver != null) {
      putIf(p, "platform", pver.displayString());
    }
    DaemonVersion dver = getDaemonVersion();
    if (dver != null) {
      putIf(p, "daemon", dver.displayString());
    } else {
      putIf(p, "built",
	    BuildInfo.getBuildProperty(BuildInfo.BUILD_TIMESTAMP));
      putIf(p, "built_on", BuildInfo.getBuildProperty(BuildInfo.BUILD_HOST));
    }
    putIf(p, "groups",
	  StringUtil.separatedString(getPlatformGroupList(), ";"));
    putIf(p, "host", getPlatformHostname());
    putIf(p, "peerid",
	  currentConfig.get(IdentityManager.PARAM_LOCAL_V3_IDENTITY));
    return p;
  }

  void putIf(Properties p, String key, String val) {
    if (val != null) {
      p.put(key, val);
    }
  }

  String getVersionString() {
    StringBuilder sb = new StringBuilder();
    for (Iterator iter = getVersionProps().entrySet().iterator();
	 iter.hasNext(); ) {
      Map.Entry ent = (Map.Entry)iter.next();
      sb.append(ent.getKey());
      sb.append("=");
      sb.append(StringUtil.ckvEscape((String)ent.getValue()));
      if (iter.hasNext()) {
	sb.append(",");
      }
    }
    return sb.toString();
  }

  void loadList(Configuration intoConfig,
		Collection<ConfigFile.Generation> gens) {
    final String DEBUG_HEADER = "loadList(): ";
    if (log.isDebug3()) {
      log.debug3(DEBUG_HEADER + "intoConfig = " + intoConfig);
      log.debug3(DEBUG_HEADER + "gens = " + gens);
    }
    for (ConfigFile.Generation gen : gens) {
      if (gen != null) {
	if (log.isDebug3()) log.debug3(DEBUG_HEADER + "gen = " + gen);
	intoConfig.copyFrom(gen.getConfig(), null);
	if (log.isDebug3())
	  log.debug3(DEBUG_HEADER + "intoConfig = " + intoConfig);
      }
    }
    if (log.isDebug2()) log.debug2(DEBUG_HEADER + "Done.");
  }

  void setupPlatformConfig(List urls) {
    final String DEBUG_HEADER = "setupPlatformConfig(): ";
    if (log.isDebug2()) log.debug2(DEBUG_HEADER + "urls = " + urls);
    Configuration platConfig = initNewConfiguration();
    for (Iterator iter = urls.iterator(); iter.hasNext();) {
      Object o = iter.next();
      ConfigFile cf;
      if (o instanceof ConfigFile) {
	cf = (ConfigFile)o;
      } else {
	cf = configCache.find(o.toString());
      }
      if (log.isDebug3()) log.debug3(DEBUG_HEADER + "cf = " + cf);
      if (cf.isPlatformFile()) {
	try {
	  if (log.isDebug3()) {
	    log.debug3("Loading platform file: " + cf);
	  }
	  cf.setNeedsReload();
	  platConfig.load(cf);
	  if (log.isDebug3())
	    log.debug3(DEBUG_HEADER + "platConfig = " + platConfig);
	} catch (IOException e) {
	  log.warning("Couldn't preload platform file " + cf.getFileUrl(), e);
	}
      }
    }
    // init props keystore before sealing, as it may add to the config
    initSocketFactory(platConfig);

    // do this even if no local.txt, to ensure platform-like params (e.g.,
    // group) in initial config get into platformConfig even during testing.
    platConfig.seal();
    platformConfig = platConfig;
    initCacheConfig(platConfig);
    setUpRemoteConfigFailover();
    if (log.isDebug2()) log.debug2(DEBUG_HEADER + "Done.");
  }

  // If a keystore was specified for 
  void initSocketFactory(Configuration platconf) {
    String serverAuthKeystore =
      platconf.getNonEmpty(PARAM_SERVER_AUTH_KEYSTORE_NAME);
    if (serverAuthKeystore != null) {
      platKeystoreConfig = newConfiguration();
      String resource = builtinServerAuthKeystores.get(serverAuthKeystore);
      if (resource != null) {
	// Set up keystore params to point to internal keystore resource.
	String pref = keystorePref(serverAuthKeystore);
	platKeystoreConfig.put(pref + LockssKeyStoreManager.KEYSTORE_PARAM_NAME,
			       serverAuthKeystore);
	platKeystoreConfig.put(pref + LockssKeyStoreManager.KEYSTORE_PARAM_RESOURCE,
			       resource);
      } else {
	// if props keystore name isn't builtin, it's a filename.  Set up
	// keystore params to point to it.
	String ksname = "propserver";
	String pref = keystorePref(ksname);
	platKeystoreConfig.put(pref + LockssKeyStoreManager.KEYSTORE_PARAM_NAME,
			       ksname);
	platKeystoreConfig.put(pref + LockssKeyStoreManager.KEYSTORE_PARAM_FILE,
			       serverAuthKeystore);
	serverAuthKeystore = ksname;
      }
      platconf.copyFrom(platKeystoreConfig);
    }
    String clientAuthKeystore =
      platconf.getNonEmpty(PARAM_CLIENT_AUTH_KEYSTORE_NAME);
    if (serverAuthKeystore != null || clientAuthKeystore != null) {
      log.debug("initSocketFactory: " + serverAuthKeystore +
		", " + clientAuthKeystore);
      secureSockFact = new LockssSecureSocketFactory(serverAuthKeystore,
						     clientAuthKeystore);
    }
  }

  LockssSecureSocketFactory getSecureSocketFactory() {
    return secureSockFact;
  }

  String keystorePref(String name) {
    return LockssKeyStoreManager.PARAM_KEYSTORE
      + "." + StringUtil.sanitizeToIdentifier(name.toLowerCase()) + ".";
  }

  /**
   * Installs the passed configuration as the current one. Used by testing
   * utilities and by REST web services that get their configuration from a
   * separate Configuration REST web service instead of relying on local files.
   * 
   * @param newConfig
   *          A Configuration with the configuration to be installed.
   * @return <code>TRUE</code> if the passed configuration is installed,
   *         <code>FALSE</code> otherwise.
   */
  boolean installConfig(Configuration newConfig) {
    return installConfig(newConfig, Collections.EMPTY_LIST);
  }

  boolean installConfig(Configuration newConfig, List gens) {
    final String DEBUG_HEADER = "installConfig(): ";
    if (log.isDebug2()) {
      log.debug2(DEBUG_HEADER + "newConfig = " + newConfig);
      log.debug2(DEBUG_HEADER + "gens = " + gens);
    }
    if (newConfig == null) {
      if (log.isDebug2()) log.debug2(DEBUG_HEADER + "Returning false.");
      return false;
    }
    copyPlatformParams(newConfig);
    inferMiscParams(newConfig);
    setConfigMacros(newConfig);
    setCompatibilityParams(newConfig);
    newConfig.seal();
    Configuration oldConfig = currentConfig;
    if (!oldConfig.isEmpty() && newConfig.equals(oldConfig)) {
      if (reloadInterval >= 10 * Constants.MINUTE) {
	log.info("Config unchanged, not updated");
      }
      updateGenerations(gens);
      if (log.isDebug2()) log.debug2(DEBUG_HEADER + "Returning false.");
      return false;
    }

    Configuration.Differences diffs = newConfig.differences(oldConfig);
    // XXX for test utils.  ick
    initCacheConfig(newConfig);
    setCurrentConfig(newConfig);
    copyToSysProps(newConfig);
    updateGenerations(gens);
    recordConfigLoaded(newConfig, oldConfig, diffs, gens);
    startCallbacksTime = TimeBase.nowMs();
    runCallbacks(newConfig, oldConfig, diffs);
    // notify other cluster members that the config changed
    // TK should this precede runCallbacks()?
    notifyConfigChanged();
    if (log.isDebug2()) log.debug2(DEBUG_HEADER + "Returning true.");
    return true;
  }

  void updateGenerations(List gens) {
    for (Iterator iter = gens.iterator(); iter.hasNext(); ) {
      ConfigFile.Generation gen = (ConfigFile.Generation)iter.next();
      setGeneration(gen.getUrl(), gen.getGeneration());
    }
  }


  public void configurationChanged(Configuration config,
				   Configuration oldConfig,
				   Configuration.Differences changedKeys) {
    final String DEBUG_HEADER = "configurationChanged(): ";
    if (log.isDebug2()) {
      log.debug2(DEBUG_HEADER + "config = " + config);
      log.debug2(DEBUG_HEADER + "oldConfig = " + oldConfig);
      log.debug2(DEBUG_HEADER + "changedKeys = " + changedKeys);
    }

    if (changedKeys.contains(MYPREFIX)) {
      reloadInterval = config.getTimeInterval(PARAM_RELOAD_INTERVAL,
					      DEFAULT_RELOAD_INTERVAL);
      sendVersionEvery = config.getTimeInterval(PARAM_SEND_VERSION_EVERY,
						DEFAULT_SEND_VERSION_EVERY);
      maxDeferredAuBatchSize =
	config.getInt(PARAM_MAX_DEFERRED_AU_BATCH_SIZE,
		      DEFAULT_MAX_DEFERRED_AU_BATCH_SIZE);
      notificationTopic = config.get(PARAM_JMS_NOTIFICATION_TOPIC,
				     DEFAULT_JMS_NOTIFICATION_TOPIC);
      enableJmsNotifications = config.getBoolean(PARAM_ENABLE_JMS_NOTIFICATIONS,
						 DEFAULT_ENABLE_JMS_NOTIFICATIONS);
      clientId = config.get(PARAM_JMS_CLIENT_ID, DEFAULT_JMS_CLIENT_ID);
    }

    if (changedKeys.contains(PARAM_PLATFORM_VERSION)) {
      platVer = null;
    }
  }

  private void buildLoadedFileLists(List<ConfigFile.Generation> gens) {
    final String DEBUG_HEADER = "buildLoadedFileLists(): ";
    if (log.isDebug2()) log.debug2(DEBUG_HEADER + "gens = " + gens);
    if (gens != null && !gens.isEmpty()) {
      List<String> specNames = new ArrayList<String>(gens.size());
      List<String> loadedNames = new ArrayList<String>(gens.size());
      for (ConfigFile.Generation gen : gens) {
	if (log.isDebug3()) log.debug3(DEBUG_HEADER + "gen = " + gen);
	if (gen != null) {
	  if (log.isDebug3()) log.debug3(DEBUG_HEADER
	      + "gen.getConfigFile().getLoadedUrl() = "
	      + gen.getConfigFile().getLoadedUrl());
	  loadedNames.add(gen.getConfigFile().getLoadedUrl());
	  if (log.isDebug3()) log.debug3(DEBUG_HEADER
	      + "gen.getConfigFile().getFileUrl() = "
	      + gen.getConfigFile().getFileUrl());
	  specNames.add(gen.getConfigFile().getFileUrl());
	}
      }
      loadedUrls = loadedNames;
      specUrls = specNames;
    } else {
      loadedUrls = Collections.EMPTY_LIST;
      specUrls = Collections.EMPTY_LIST;
    }
  }

  private void recordConfigLoaded(Configuration newConfig,
				  Configuration oldConfig,
				  Configuration.Differences diffs,
				  List gens) {
    buildLoadedFileLists(gens);    
    logConfigLoaded(newConfig, oldConfig, diffs, loadedUrls);
  }

  

  private void logConfigLoaded(Configuration newConfig,
			       Configuration oldConfig,
			       Configuration.Differences diffs,
			       List<String> names) {
    StringBuffer sb = new StringBuffer("Config updated, ");
    sb.append(newConfig.keySet().size());
    sb.append(" keys");
    if (!names.isEmpty()) {
      sb.append(" from ");
      sb.append(StringUtil.separatedString(names, ", "));
    }
    log.info(sb.toString());
    if (log.isDebug()) {
      logConfig(newConfig, oldConfig, diffs);
    } else {
      log.info("New TdbAus: " + diffs.getTdbAuDifferenceCount());
    }
  }

  public static final String PARAM_HASH_SVC = LockssApp.MANAGER_PREFIX +
    LockssApp.managerKey(org.lockss.hasher.HashService.class);
  static final String DEFAULT_HASH_SVC = "org.lockss.hasher.HashSvcSchedImpl";

  private void inferMiscParams(Configuration config) {
    // hack to make hash use new scheduler without directly setting
    // org.lockss.manager.HashService, which would break old daemons.
    // don't set if already has a value
    if (config.get(PARAM_HASH_SVC) == null &&
	config.getBoolean(PARAM_NEW_SCHEDULER, DEFAULT_NEW_SCHEDULER)) {
      config.put(PARAM_HASH_SVC, DEFAULT_HASH_SVC);
    }

    // If we were given a temp dir, create a subdir and use that.  This
    // ensures that * expansion in rundaemon won't exceed the maximum
    // command length.

    String tmpdir = config.get(PARAM_TMPDIR);
    if (!StringUtil.isNullString(tmpdir)) {
      File javaTmpDir = new File(tmpdir, "dtmp");
      if (FileUtil.ensureDirExists(javaTmpDir)) {
	FileUtil.setOwnerRWX(javaTmpDir);
	System.setProperty("java.io.tmpdir", javaTmpDir.toString());
      } else {
	log.warning("Can't create/access temp dir: " + javaTmpDir +
		    ", using default: " + System.getProperty("java.io.tmpdir"));
      }
    }
    System.setProperty("jsse.enableSNIExtension",
		       Boolean.toString(config.getBoolean(PARAM_JSSE_ENABLESNIEXTENSION,
							  DEFAULT_JSSE_ENABLESNIEXTENSION)));

    String fromParam = LockssDaemon.PARAM_BIND_ADDRS;
    setIfNotSet(config, fromParam, AdminServletManager.PARAM_BIND_ADDRS);
    setIfNotSet(config, fromParam, ContentServletManager.PARAM_BIND_ADDRS);
    setIfNotSet(config, fromParam, ProxyManager.PARAM_BIND_ADDRS);
    setIfNotSet(config, fromParam, AuditProxyManager.PARAM_BIND_ADDRS);
//     setIfNotSet(config, fromParam, IcpManager.PARAM_ICP_BIND_ADDRS);

    org.lockss.poller.PollManager.processConfigMacros(config);
  }
  
  private void copyToSysProps(Configuration config) {
    // Copy unfiltered port lists for retrieval in PlatformUtil
    setSysProp(PlatformUtil.SYSPROP_UNFILTERED_TCP_PORTS,
               config.get(PARAM_UNFILTERED_TCP_PORTS));
    setSysProp(PlatformUtil.SYSPROP_UNFILTERED_UDP_PORTS,
               config.get(PARAM_UNFILTERED_UDP_PORTS, ""));
    
    // Copy hostname for retrieval in PlatformUtil
    setSysProp(PlatformUtil.SYSPROP_PLATFORM_HOSTNAME,
               config.get(PARAM_PLATFORM_FQDN));
  }

    
  /**
   * Sets the given system property only if the given value is non null.
   */
  protected static void setSysProp(String key, String value) {
    if (value != null) {
      System.setProperty(key, value);
    }
  }
  
  // Backward compatibility for param settings

  /** Obsolete, use org.lockss.ui.contactEmail (daemon 1.32) */
  static final String PARAM_OBS_ADMIN_CONTACT_EMAIL =
    "org.lockss.admin.contactEmail";
  /** Obsolete, use org.lockss.ui.helpUrl (daemon 1.32) */
  static final String PARAM_OBS_ADMIN_HELP_URL = "org.lockss.admin.helpUrl";

  private void setIfNotSet(Configuration config,
			   String fromKey, String toKey) {
    if (config.containsKey(fromKey) && !config.containsKey(toKey)) {
      config.put(toKey, config.get(fromKey));
    }
  }

  private void setCompatibilityParams(Configuration config) {
    setIfNotSet(config,
		PARAM_OBS_ADMIN_CONTACT_EMAIL,
		AdminServletManager.PARAM_CONTACT_ADDR);
    setIfNotSet(config,
		PARAM_OBS_ADMIN_HELP_URL,
		AdminServletManager.PARAM_HELP_URL);
    setIfNotSet(config,
		RemoteApi.PARAM_BACKUP_EMAIL_FREQ,
		RemoteApi.PARAM_BACKUP_FREQ);
  }

  private void setConfigMacros(Configuration config) {
    String acctPolicy = config.get(AccountManager.PARAM_POLICY,
				   AccountManager.DEFAULT_POLICY);
    if ("lc".equalsIgnoreCase(acctPolicy)) {
      setParamsFromPairs(config, AccountManager.POLICY_LC);
    }
    if ("ssl".equalsIgnoreCase(acctPolicy)) {
      setParamsFromPairs(config, AccountManager.POLICY_SSL);
    }
    if ("form".equalsIgnoreCase(acctPolicy)) {
      setParamsFromPairs(config, AccountManager.POLICY_FORM);
    }
    if ("basic".equalsIgnoreCase(acctPolicy)) {
      setParamsFromPairs(config, AccountManager.POLICY_BASIC);
    }
    if ("compat".equalsIgnoreCase(acctPolicy)) {
      setParamsFromPairs(config, AccountManager.POLICY_COMPAT);
    }
  }

  private void setParamsFromPairs(Configuration config, String[] pairs) {
    for (int ix = 0; ix < pairs.length; ix += 2) {
      config.put(pairs[ix], pairs[ix + 1]);
    }
  }

  private void copyPlatformParams(Configuration config) {
    copyPlatformVersionParams(config);
    if (platKeystoreConfig != null) {
      config.copyFrom(platKeystoreConfig);
    }
    String logdir = config.get(PARAM_PLATFORM_LOG_DIR);
    String logfile = config.get(PARAM_PLATFORM_LOG_FILE);
    if (logdir != null && logfile != null) {
      // TK
//       platformOverride(config, FileTarget.PARAM_FILE,
// 		       new File(logdir, logfile).toString());
    }

    conditionalPlatformOverride(config, PARAM_PLATFORM_IP_ADDRESS,
				IdentityManager.PARAM_LOCAL_IP);

    conditionalPlatformOverride(config, PARAM_PLATFORM_IP_ADDRESS,
				ClockssParams.PARAM_INSTITUTION_SUBSCRIPTION_ADDR);
    conditionalPlatformOverride(config, PARAM_PLATFORM_SECOND_IP_ADDRESS,
				ClockssParams.PARAM_CLOCKSS_SUBSCRIPTION_ADDR);

    conditionalPlatformOverride(config, PARAM_PLATFORM_LOCAL_V3_IDENTITY,
				IdentityManager.PARAM_LOCAL_V3_IDENTITY);

    conditionalPlatformOverride(config, PARAM_PLATFORM_SMTP_PORT,
				SmtpMailService.PARAM_SMTPPORT);
    conditionalPlatformOverride(config, PARAM_PLATFORM_SMTP_HOST,
				SmtpMailService.PARAM_SMTPHOST);

    // Add platform access subnet to access lists if it hasn't already been
    // accounted for
    String platformSubnet = config.get(PARAM_PLATFORM_ACCESS_SUBNET);
    appendPlatformAccess(config,
			 AdminServletManager.PARAM_IP_INCLUDE,
			 AdminServletManager.PARAM_IP_PLATFORM_SUBNET,
			 platformSubnet);
    appendPlatformAccess(config,
			 ProxyManager.PARAM_IP_INCLUDE,
			 ProxyManager.PARAM_IP_PLATFORM_SUBNET,
			 platformSubnet);

    String space = config.get(PARAM_PLATFORM_DISK_SPACE_LIST);
    if (!StringUtil.isNullString(space)) {
      String firstSpace =
	((String)StringUtil.breakAt(space, ';', 1).elementAt(0));
      platformOverride(config,
		       OldLockssRepositoryImpl.PARAM_CACHE_LOCATION,
		       firstSpace);
      platformOverride(config, HistoryRepositoryImpl.PARAM_HISTORY_LOCATION,
		       firstSpace);
      platformOverride(config, IdentityManager.PARAM_IDDB_DIR,
		       new File(firstSpace, "iddb").toString());
      platformDefault(config,
		      org.lockss.truezip.TrueZipManager.PARAM_CACHE_DIR,
		      new File(firstSpace, "tfile").toString());
      platformDefault(config,
		      RemoteApi.PARAM_BACKUP_DIR,
		      new File(firstSpace, "backup").toString());
    }
  }

  private void copyPlatformVersionParams(Configuration config) {
    String platformVer = config.get(PARAM_PLATFORM_VERSION);
    if (platformVer == null) {
      return;
    }
    Configuration versionConfig = config.getConfigTree(platformVer);
    if (!versionConfig.isEmpty()) {
     for (Iterator iter = versionConfig.keyIterator(); iter.hasNext(); ) {
       String key = (String)iter.next();
       platformOverride(config, key, versionConfig.get(key));
     }
    }
  }

  /** Override current config value with val */
  private void platformOverride(Configuration config, String key, String val) {
    String oldval = config.get(key);
    if (oldval != null && !StringUtil.equalStrings(oldval, val)) {
      log.warning("Overriding param: " + key + "= " + oldval);
      log.warning("with platform-derived value: " + val);
    }
    config.put(key, val);
  }

  /** Store val in config iff key currently not set */
  private void platformDefault(Configuration config, String key, String val) {
    if (!config.containsKey(key)) {
      platformOverride(config, key, val);
    }
  }

  /** Copy value of platformKey in config iff key currently not set */
  private void conditionalPlatformOverride(Configuration config,
					   String platformKey, String key) {
    String value = config.get(platformKey);
    if (value != null) {
      platformOverride(config, key, value);
    }
  }

  // If the current platform access (subnet) value is different from the
  // value it had the last time the local config file was written, add it
  // to the access list.
  private void appendPlatformAccess(Configuration config, String accessParam,
				    String oldPlatformAccessParam,
				    String platformAccess) {
    String oldPlatformAccess = config.get(oldPlatformAccessParam);
    if (StringUtil.isNullString(platformAccess) ||
	platformAccess.equals(oldPlatformAccess)) {
      return;
    }
    String includeIps = config.get(accessParam);
    includeIps = IpFilter.unionFilters(platformAccess, includeIps);
    config.put(accessParam, includeIps);
  }

  private void logConfig(Configuration config,
			 Configuration oldConfig,
			 Configuration.Differences diffs) {
    int maxLogValLen = config.getInt(PARAM_MAX_LOG_VAL_LEN,
				     DEFAULT_MAX_LOG_VAL_LEN);
    Set<String> diffSet = diffs.getDifferenceSet();
    SortedSet<String> keys = new TreeSet<String>(diffSet);
    int elided = 0;
    int numDiffs = keys.size();
    // keys includes param name prefixes that aren't actual params, so
    // numDiffs is inflated by several.
    for (String key : keys) {
      if (numDiffs <= 40 || log.isDebug3() || shouldParamBeLogged(key)) {
	if (config.containsKey(key)) {
	  String val = config.get(key);
	  log.debug("  " +key + " = " + StringUtils.abbreviate(val, maxLogValLen));
	} else if (oldConfig.containsKey(key)) {
	  log.debug("  " + key + " (removed)");
	}
      } else {
	elided++;
      }
    }
    if (elided > 0) log.debug(elided + " keys elided");
    log.debug("New TdbAus: " + diffs.getTdbAuDifferenceCount());
    if (log.isDebug3()) {
      log.debug3("TdbDiffs: " + diffs.getTdbDifferences());
    }

    if (log.isDebug2()) {
      Tdb tdb = config.getTdb();
      if (tdb != null) {
	log.debug2(StringPool.AU_CONFIG_PROPS.toStats());

	Histogram hist1 = new Histogram(15);
	Histogram hist2 = new Histogram(15);
	Histogram hist3 = new Histogram(15);

	for (TdbAu.Id id : tdb.getAllTdbAuIds()) {
	  TdbAu tau = id.getTdbAu();
	  hist1.addDataPoint(tau.getParams().size());
	  hist2.addDataPoint(tau.getAttrs().size());
	  hist3.addDataPoint(tau.getProperties().size());
	}
	logHist("Tdb Params", hist1);
	logHist("Tdb Attrs", hist2);
	logHist("Tdb Props", hist3);
      }
    }
  }

  private void logHist(String name, Histogram hist) {
    int[] freqs = hist.getFreqs();
    log.debug2(name + " histogram");
    log.debug2("size  number");
    for (int ix = 0; ix <= hist.getMax(); ix++) {
      log.debug(String.format("%2d   %6d", ix, freqs[ix]));
    }
  }

  public static boolean shouldParamBeLogged(String key) {
    return !(key.startsWith(PREFIX_TITLE_DB)
 	     || key.startsWith(PREFIX_TITLE_SETS_DOT)
  	     || key.startsWith(PluginManager.PARAM_AU_TREE + ".")
  	     || StringUtils.endsWithIgnoreCase(key, "password"));
  }

  /**
   * Add a collection of bundled titledb config jar URLs to
   * the pluginTitledbUrlList.
   */
  public void addTitleDbConfigFrom(Collection classloaders) {
    boolean needReload = false;

    for (Iterator it = classloaders.iterator(); it.hasNext(); ) {
      ClassLoader cl = (ClassLoader)it.next();
      URL titleDbUrl = cl.getResource(CONFIG_FILE_BUNDLED_TITLE_DB);
      if (titleDbUrl != null) {
	if (pluginTitledbUrlList == null) {
	  pluginTitledbUrlList = new ArrayList();
	}
	pluginTitledbUrlList.add(titleDbUrl);
	needReload = true;
      }
    }
    // Force a config reload -- this is required to make the bundled
    // title configs immediately available, otherwise they will not be
    // available until the next config reload.
    if (needReload) {
      requestReload();
    }
  }

  public void requestReload() {
    // Increment the counter of configuration reload requests.
    configReloadRequestCounter++;
    requestReloadIn(0);
  }

  public void requestReloadIn(long millis) {
    if (handlerThread != null) {
      handlerThread.forceReloadIn(millis);
    }
  }

  /**
   * Register a {@link Configuration.Callback}, which will be called
   * whenever the current configuration has changed.  If a configuration is
   * present when a callback is registered, the callback will be called
   * immediately.
   * @param c <code>Configuration.Callback</code> to add.  */
  public void registerConfigurationCallback(Configuration.Callback c) {
    log.debug2("registering " + c);
    if (!configChangedCallbacks.contains(c)) {
      configChangedCallbacks.add(c);
      if (!currentConfig.isEmpty()) {
	runCallback(c, currentConfig, ConfigManager.EMPTY_CONFIGURATION,
		    currentConfig.differences(null));  // all differences
      }
    }
  }

  /**
   * Unregister a <code>Configuration.Callback</code>.
   * @param c <code>Configuration.Callback</code> to remove.
   */
  public void unregisterConfigurationCallback(Configuration.Callback c) {
    log.debug3("unregistering " + c);
    configChangedCallbacks.remove(c);
  }

  boolean cacheConfigInited = false;
  File cacheConfigDir = null;
  boolean hasLocalCacheConfig = false;

  boolean isUnitTesting() {
    return Boolean.getBoolean("org.lockss.unitTesting");
  }

  List<Pattern> compilePatternList(List<String> patterns)
      throws MalformedPatternException {
    if (patterns == null) {
      return Collections.EMPTY_LIST;
    }
    int flags = Perl5Compiler.READ_ONLY_MASK;
    List<Pattern> res = new ArrayList<Pattern>(patterns.size());

    for (String pat : patterns) {
      res.add(RegexpUtil.getCompiler().compile(pat, flags));
    }
    return res;
  }

  private String getFromGenerations(List configGenerations, String param,
				    String dfault) {
    for (Iterator iter = configGenerations.iterator(); iter.hasNext(); ) {
      ConfigFile.Generation gen = (ConfigFile.Generation)iter.next();
      if (gen != null) {
	String val = gen.getConfig().get(param);
	if (val != null) {
	  return val;
	}
      }
    }
    return dfault;
  }

  private List getListFromGenerations(List configGenerations, String param,
				      List dfault) {
    for (Iterator iter = configGenerations.iterator(); iter.hasNext(); ) {
      ConfigFile.Generation gen = (ConfigFile.Generation)iter.next();
      if (gen != null) {
	Configuration config = gen.getConfig();
	if (config.containsKey(param)) {
	  return config.getList(param);
	}
      }
    }
    return dfault;
  }

  private void initCacheConfig(String dspace, String relConfigPath) {
    if (cacheConfigInited) return;
    Vector v = StringUtil.breakAt(dspace, ';');
    if (v.size() == 0) {
      log.error(PARAM_PLATFORM_DISK_SPACE_LIST +
		" not specified, not configuring local cache config dir");
      return;
    }
    cacheConfigDir = findRelDataDir(v, relConfigPath, true);
    cacheConfigInited = true;
  }

  private void initCacheConfig(List configGenerations) {
    if (!cacheConfigInited || isChanged(configGenerations)) {
      List<String> expertAllow =
	getListFromGenerations(configGenerations,
			       PARAM_EXPERT_ALLOW, DEFAULT_EXPERT_ALLOW);
      List<String> expertDeny =
	getListFromGenerations(configGenerations,
			       PARAM_EXPERT_DENY, DEFAULT_EXPERT_DENY);
      processExpertAllowDeny(expertAllow, expertDeny);
    }
    if (cacheConfigInited) return;
    String dspace = getFromGenerations(configGenerations,
				       PARAM_PLATFORM_DISK_SPACE_LIST,
				       null);
    String relConfigPath = getFromGenerations(configGenerations,
					      PARAM_CONFIG_PATH,
					      DEFAULT_CONFIG_PATH);
    initCacheConfig(dspace, relConfigPath);
  }

  private void initCacheConfig(Configuration newConfig) {
    List<String> expertAllow =
      newConfig.getList(PARAM_EXPERT_ALLOW, DEFAULT_EXPERT_ALLOW);
    List<String> expertDeny =
      newConfig.getList(PARAM_EXPERT_DENY, DEFAULT_EXPERT_DENY);
    processExpertAllowDeny(expertAllow, expertDeny);

    if (cacheConfigInited) return;
    String dspace = newConfig.get(PARAM_PLATFORM_DISK_SPACE_LIST);
    String relConfigPath = newConfig.get(PARAM_CONFIG_PATH,
					 DEFAULT_CONFIG_PATH);
    initCacheConfig(dspace, relConfigPath);
  }

  private void processExpertAllowDeny(List<String> expertAllow,
				      List<String> expertDeny) {
    log.debug("processExpertAllowDeny("+expertAllow+", "+expertDeny+")");
    try {
      expertConfigAllowPats = compilePatternList(expertAllow);
      expertConfigDenyPats =  compilePatternList(expertDeny);
      enableExpertConfig = true;
    } catch (MalformedPatternException e) {
      log.error("Expert config allow/deny error", e);
      enableExpertConfig = false;
    }
  }

  /**
   * Find or create a directory specified by a config param and default.
   * If value (the param value or default) is an absolute path, that
   * directory is returned (and created if necessary).  If the value is
   * relative, it specifies a directory relative to (one of) the paths on
   * {@value #PARAM_PLATFORM_DISK_SPACE_LIST}.  If one of these directories
   * exists, it (the first one) is returned, else the directory is created
   * relative to the first element of {@value
   * #PARAM_PLATFORM_DISK_SPACE_LIST}
   * @param dataDirParam Name of config param whose value specifies the
   * absolute or relative path of the directory
   * @param dataDirDefault Default absolute or relative path of the
   * directory
   * @return A File object representing the requested directory.
   * @throws RuntimeException if neither the config param nor the default
   * have a non-empty value, or (for relative paths) if {@value
   * #PARAM_PLATFORM_DISK_SPACE_LIST} is not set, or if a directory can't
   * be created.
   */
  public File findConfiguredDataDir(String dataDirParam,
				    String dataDirDefault) {
    return findConfiguredDataDir(dataDirParam, dataDirDefault, true);
  }

  /**
   * Find or create a directory specified by a config param and default.
   * If value (the param value or default) is an absolute path, that
   * directory is returned (and created if necessary).  If the value is
   * relative, it specifies a directory relative to (one of) the paths on
   * {@value #PARAM_PLATFORM_DISK_SPACE_LIST}.  If one of these directories
   * exists, it (the first one) is returned, else the directory is created
   * relative to the first element of {@value
   * #PARAM_PLATFORM_DISK_SPACE_LIST}
   * @param dataDirParam Name of config param whose value specifies the
   * absolute or relative path of the directory
   * @param dataDirDefault Default absolute or relative path of the
   * directory
   * @param create If false and the directory doesn't already exist, the
   * path to where it would be created is returned, but it is not actually
   * created.
   * @return A File object representing the requested directory.
   * @throws RuntimeException if neither the config param nor the default
   * have a non-empty value, or (for relative paths) if {@value
   * #PARAM_PLATFORM_DISK_SPACE_LIST} is not set, or if a directory can't
   * be created.
   */
  public File findConfiguredDataDir(String dataDirParam,
				    String dataDirDefault,
				    boolean create) {
    String dataDirName = getCurrentConfig().get(dataDirParam, dataDirDefault);
    if (StringUtil.isNullString(dataDirName)) {
      throw new RuntimeException("No value or default for " + dataDirParam);
    }
    File dir = new File(dataDirName);
    if (dir.isAbsolute()) {
      if (FileUtil.ensureDirExists(dir)) {
	return dir;
      } else {
	throw new RuntimeException("Could not create data dir: " + dir);
      }
    } else {
      return findRelDataDir(dataDirName, create);
    }
  }

  /**
   * Find or create a directory relative to a path on the platform disk
   * space list.  If not found, it is created under the first element on
   * the platform disk space list.
   *
   * @param relPath Relative pathname of the directory to find or create.
   * @return A File object representing the requested directory.
   * @throws RuntimeException if {@value #PARAM_PLATFORM_DISK_SPACE_LIST}
   * is not set, or if a directory can't be created.
   */
  public File findRelDataDir(String relPath, boolean create) {
    List<String> diskPaths =
      getCurrentConfig().getList(PARAM_PLATFORM_DISK_SPACE_LIST);
    return findRelDataDir(diskPaths, relPath, create);
  }

  private File findRelDataDir(List<String> diskPaths, String relPath,
			      boolean create) {
    if (diskPaths.size() == 0) {
      throw new RuntimeException("No platform disks specified. " +
				 PARAM_PLATFORM_DISK_SPACE_LIST +
				 " must be set.");
    }
    File best = null;
    for (String path : diskPaths) {
      File candidate = new File(path, relPath);
      if (candidate.exists()) {
	if (best == null) {
	best = candidate;
	} else {
	  log.warning("Duplicate data dir found: " +
		      candidate + ", using " + best);
	}
      }
    }
    if (best != null) {
      return best;
    }
    File newDir = new File(diskPaths.get(0), relPath);
    if (create) {
      if (!FileUtil.ensureDirExists(newDir)) {
	throw new RuntimeException("Could not create data dir: " + newDir);
      }
    }
    return newDir;
  }

  /** Return a map from local config file name (sans dir) to its
   * descriptor. */
  public LinkedHashMap<String,LocalFileDescr> getLocalFileDescrMap() {
    if (cacheConfigFileMap == null) {
      LinkedHashMap<String,LocalFileDescr> res =
	new LinkedHashMap<String,LocalFileDescr>();
      for (LocalFileDescr ccf: cacheConfigFiles) {
 	ccf.setFile(new File(cacheConfigDir, ccf.getName()));
	res.put(ccf.getName(), ccf);
      }
      cacheConfigFileMap = res;
    }
    return cacheConfigFileMap;
  }

  /** Return the list of cache config file decrs, in same order in which
   * they were declared. */
  public Collection<LocalFileDescr> getLocalFileDescrs() {
    return getLocalFileDescrMap().values();
  }

  /** Return the LocalFileDescr the named cache config file, or null if
   * none. */
  public LocalFileDescr getLocalFileDescr(String cacheConfigFileName) {
    return getLocalFileDescrMap().get(cacheConfigFileName);
  }

  /** Return a File for the named cache config file */
  public File getCacheConfigFile(String cacheConfigFileName) {
    return new File(cacheConfigDir, cacheConfigFileName);
  }

  /** Return the cache config dir */
  public File getCacheConfigDir() {
    return cacheConfigDir;
  }

  /** Return true if any daemon config has been done on this machine */
  public boolean hasLocalCacheConfig() {
    return hasLocalCacheConfig;
  }

  /**
   * @param url The Jar URL of a bundled title db file.
   * @return Configuration with parameters from the bundled file,
   *         or an empty configuration if it could not be loaded.
   */
  public Configuration readTitledbConfigFile(URL url) {
    log.debug2("Loading bundled titledb from URL: " + url);
    ConfigFile cf = configCache.find(url.toString());
    try {
      return cf.getConfiguration();
    } catch (FileNotFoundException ex) {
      // expected if no bundled title db
    } catch (IOException ex) {
      log.debug("Unexpected exception loading bundled titledb", ex);
    }
    return EMPTY_CONFIGURATION;
  }

  /** Read the named local cache config file from the previously determined
   * cache config directory.
   * @param cacheConfigFileName filename, no path
   * @return Configuration with parameters from file
   */
  public Configuration readCacheConfigFile(String cacheConfigFileName)
      throws IOException {
    final String DEBUG_HEADER = "readCacheConfigFile(): ";
    if (log.isDebug2())
      log.debug2(DEBUG_HEADER + "cacheConfigFileName = " + cacheConfigFileName);

    Configuration res =
	getConfigFileInCache(cacheConfigFileName).getConfiguration();
    if (log.isDebug2()) log.debug2(DEBUG_HEADER
	+ Configuration.loggableConfiguration(res, "res"));
    return res;
  }

  private boolean didWarnNoAuConfig = false;

  /**
   * Return the contents of the local AU config file.
   * @return the Configuration from the AU config file, or an empty config
   * if no config file found
   */
  public Configuration readAuConfigFile() {
    Configuration auConfig;
    try {
      auConfig = readCacheConfigFile(CONFIG_FILE_AU_CONFIG);
      didWarnNoAuConfig = false;
    } catch (IOException e) {
      if (!didWarnNoAuConfig) {
	log.warning("Couldn't read AU config file: " + e.getMessage());
	didWarnNoAuConfig = true;
      }
      auConfig = newConfiguration();
    }
    return auConfig;
  }

  /** Write the named local cache config file into the previously determined
   * cache config directory.
   * @param props properties to write
   * @param cacheConfigFileName filename, no path
   * @param header file header string
   */
  public void writeCacheConfigFile(Properties props,
				   String cacheConfigFileName,
				   String header)
      throws IOException {
    writeCacheConfigFile(fromProperties(props), cacheConfigFileName, header);
  }

  /** Write the named local cache config file into the previously determined
   * cache config directory.
   * @param config Configuration with properties to write
   * @param cacheConfigFileName filename, no path
   * @param header file header string
   */
  public synchronized void writeCacheConfigFile(Configuration config,
						String cacheConfigFileName,
						String header)
      throws IOException {
    writeCacheConfigFile(config, cacheConfigFileName, header, false);
  }

  /** Write the named local cache config file into the previously determined
   * cache config directory.
   * @param config Configuration with properties to write
   * @param cacheConfigFileName filename, no path
   * @param header file header string
   */
  public synchronized void writeCacheConfigFile(Configuration config,
						String cacheConfigFileName,
						String header,
						boolean suppressReload)
      throws IOException {
    if (cacheConfigDir == null) {
      log.warning("Attempting to write cache config file: " +
		  cacheConfigFileName + ", but no cache config dir exists");
      throw new RuntimeException("No cache config dir");
    }
    // Write to a temp file and rename
    File tempfile = File.createTempFile("tmp_config", ".tmp", cacheConfigDir);
    OutputStream os = new FileOutputStream(tempfile);
    // Add fileversion iff it's not already there.
    Properties addtl = null;
    String verProp = configVersionProp(cacheConfigFileName);
    String verVal = "1";
    if (!verVal.equals(config.get(verProp))) {
      addtl = new Properties();
      addtl.put(verProp, verVal);
    }
    config.store(os, header, addtl);
    os.close();
    File cfile = new File(cacheConfigDir, cacheConfigFileName);
    configCache.find(cfile.toString()).writeFromTempFile(tempfile, config);
    log.debug2("Wrote cache config file: " + cfile);
    LocalFileDescr descr = getLocalFileDescr(cacheConfigFileName);
    if (!suppressReload) {
      if (descr == null || descr.isNeedReloadAfterWrite()) {
	requestReload();
      }
    }
  }

  /** Write the named local cache config file into the previously determined
   * cache config directory.
   * @param config Configuration with properties to write
   * @param cacheConfigFileName filename, no path
   * @param header file header string
   */
  public synchronized void writeCacheConfigFile(String text,
						String cacheConfigFileName,
						boolean suppressReload)
      throws IOException {
    if (cacheConfigDir == null) {
      log.warning("Attempting to write cache config file: " +
		  cacheConfigFileName + ", but no cache config dir exists");
      throw new RuntimeException("No cache config dir");
    }
    // Write to a temp file and rename
    File tempfile = File.createTempFile("tmp_config", ".tmp", cacheConfigDir);
    StringUtil.toFile(tempfile, text);
    File cfile = new File(cacheConfigDir, cacheConfigFileName);
    configCache.find(cfile.toString()).writeFromTempFile(tempfile, null);
    if (!suppressReload) {
      requestReload();
    }
  }

  /** Delete the named local cache config file from the cache config
   * directory */
  public synchronized boolean deleteCacheConfigFile(String cacheConfigFileName)
      throws IOException {
    if (cacheConfigDir == null) {
      log.warning("Attempting to write delete config file: " +
		  cacheConfigFileName + ", but no cache config dir exists");
      throw new RuntimeException("No cache config dir");
    }
    File cfile = new File(cacheConfigDir, cacheConfigFileName);
    return cfile.delete();
  }

  /** Return the config version prop key for the named config file */
  public static String configVersionProp(String cacheConfigFileName) {
    String noExt = StringUtil.upToFinal(cacheConfigFileName, ".");
    return StringUtil.replaceString(PARAM_CONFIG_FILE_VERSION,
				    "<filename>", noExt);
  }

  /* Support for batching changes to au.txt, and to prevent a config
   * reload from being triggered each time the AU config file is rewritten.
   * Clients who call startAuBatch() <b>must</b> call finishAuBatch() when
   * done (in a <code>finally</code>), then call requestReload() if
   * appropriate */

  private int auBatchDepth = 0;
  private Configuration deferredAuConfig;
  private List<String> deferredAuDeleteKeys;
  private int deferredAuBatchSize;

  /** Called before a batch of calls to {@link
   * #updateAuConfigFile(Properties, String)} or {@link
   * #updateAuConfigFile(Configuration, String)}, causes updates to be
   * accumulated in memory, up to a maximum of {@link
   * #PARAM_MAX_DEFERRED_AU_BATCH_SIZE}, before they are all written to
   * disk.  {@link #finishAuBatch()} <b>MUST</b> be called at the end of
   * the batch, to ensure the final batch is written.  All removals
   * (<code>auPropKey</code> arg to updateAuConfigFile) in a batch are
   * performed before any additions, so the result of the same sequence of
   * updates in batched and non-batched mode is not necessarily equivalent.
   * It is guaranteed to be so if no AU is updated more than once in the
   * batch.  <br>This speeds up batch AU addition/deletion by a couple
   * orders of magnitude, which will suffice until the AU config is moved
   * to a database.
   */
  public synchronized void startAuBatch() {
    auBatchDepth++;
  }

  public synchronized void finishAuBatch() throws IOException {
    executeDeferredAuBatch();
    if (--auBatchDepth < 0) {
      log.warning("auBatchDepth want negative, resetting to zero",
		  new Throwable("Marker"));
      auBatchDepth = 0;
    }
  }

  private void executeDeferredAuBatch() throws IOException {
    if (deferredAuConfig != null &&
	(!deferredAuConfig.isEmpty() || !deferredAuDeleteKeys.isEmpty())) {
      updateAuConfigFile(deferredAuConfig, deferredAuDeleteKeys);
      deferredAuConfig = null;
      deferredAuDeleteKeys = null;
      deferredAuBatchSize = 0;
    }
  }

  /** Replace one AU's config keys in the local AU config file.
   * @param auProps new properties for AU
   * @param auPropKey the common initial part of all keys in the AU's config
   */
  public void updateAuConfigFile(Properties auProps, String auPropKey)
      throws IOException {
    updateAuConfigFile(fromProperties(auProps), auPropKey);
  }

  /** Replace one AU's config keys in the local AU config file.
   * @param auConfig new config for AU
   * @param auPropKey the common initial part of all keys in the AU's config
   */
  public synchronized void updateAuConfigFile(Configuration auConfig,
					      String auPropKey)
      throws IOException {
    if (auBatchDepth > 0) {
      if (deferredAuConfig == null) {
	deferredAuConfig = newConfiguration();
	deferredAuDeleteKeys = new ArrayList<String>();
	deferredAuBatchSize = 0;
      }
      deferredAuConfig.copyFrom(auConfig);
      if (auPropKey != null) {
	deferredAuDeleteKeys.add(auPropKey);
      }
      if (++deferredAuBatchSize >= maxDeferredAuBatchSize) {
	executeDeferredAuBatch();
      }
    } else {
      updateAuConfigFile(auConfig,
			 auPropKey == null ? null : ListUtil.list(auPropKey));
    }
  }

  /** Replace one or more AUs' config keys in the local AU config file.
   * @param auConfig new config for the AUs
   * @param auPropKeys list of au subtree roots to remove
   */
  private void updateAuConfigFile(Configuration auConfig,
				  List<String> auPropKeys)
      throws IOException {
    Configuration fileConfig;
    try {
      fileConfig = readCacheConfigFile(CONFIG_FILE_AU_CONFIG);
    } catch (FileNotFoundException e) {
      fileConfig = newConfiguration();
    }
    if (fileConfig.isSealed()) {
      fileConfig = fileConfig.copy();
    }
    // first remove all existing values for the AUs
    if (auPropKeys != null) {
      for (String key : auPropKeys) {
	fileConfig.removeConfigTree(key);
      }
    }
    // then add the new config
    for (Iterator iter = auConfig.keySet().iterator(); iter.hasNext();) {
      String key = (String)iter.next();
      fileConfig.put(key, auConfig.get(key));
    }
    // seal it so FileConfigFile.storedConfig() won't have to make a copy
    fileConfig.seal();
    writeCacheConfigFile(fileConfig, CONFIG_FILE_AU_CONFIG,
			 "AU Configuration", auBatchDepth > 0);
  }

  /**
   * <p>Calls {@link #modifyCacheConfigFile(Configuration, Set, String, String)}
   * with a <code>null</code> delete set.</p>
   * @param updateConfig        A {@link Configuration} instance
   *                            containing keys that will be added or
   *                            updated in the file (see above). Can
   *                            be <code>null</code> if no keys are to
   *                            be added or updated.
   * @param cacheConfigFileName A config file name (without path).
   * @param header              A file header string.
   * @throws IOException if an I/O error occurs.
   * @see #modifyCacheConfigFile(Configuration, Set, String, String)
   */
  public synchronized void modifyCacheConfigFile(Configuration updateConfig,
                                                 String cacheConfigFileName,
                                                 String header)
      throws IOException {
    modifyCacheConfigFile(updateConfig, null, cacheConfigFileName, header);
  }

  /**
   * <p>Modifies configuration values in a cache config file.</p>
   * <table>
   *  <thead>
   *   <tr>
   *    <td>Precondition</td>
   *    <td>Postcondition</td>
   *   </tr>
   *  </thead>
   *  <tbody>
   *   <tr>
   *    <td><code>deleteConfig</code> contains key <code>k</code></td>
   *    <td>The file does not contain key <code>k</code></td>
   *   </tr>
   *   <tr>
   *    <td>
   *     <code>updateConfig</code> maps key <code>k</code> to a value
   *     <code>v</code>, and <code>deleteConfig</code> does not
   *     contain key <code>k</code>
   *    </td>
   *    <td>The file maps <code>k</code> to <code>v</code></td>
   *   </tr>
   *   <tr>
   *    <td>
   *     <code>updateConfig</code> and <code>deleteConfig</code> do
   *     not contain key <code>k</code>
   *    </td>
   *    <td>
   *     The file does not contain <code>k</code> if it did not
   *     originally contain <code>k</code>, or maps <code>k</code> to
   *     <code>w</code> if it originally mapped <code>k</code> to
   *     <code>w</code>
   *    </td>
   *   </tr>
   *  </tbody>
   * </table>
   * @param updateConfig        A {@link Configuration} instance
   *                            containing keys that will be added or
   *                            updated in the file (see above). Can
   *                            be <code>null</code> if no keys are to
   *                            be added or updated.
   * @param deleteSet        A set of keys that will be deleted
   *                            in the file (see above). Can be
   *                            <code>null</code> if no keys are to be
   *                            deleted.
   * @param cacheConfigFileName A config file name (without path).
   * @param header              A file header string.
   * @throws IOException              if an I/O error occurs.
   * @throws IllegalArgumentException if a key appears both in
   *                                  <code>updateConfig</code> and
   *                                  in <code>deleteSet</code>.
   */
  public synchronized void modifyCacheConfigFile(Configuration updateConfig,
                                                 Set deleteSet,
                                                 String cacheConfigFileName,
                                                 String header)
      throws IOException {
    Configuration fileConfig;

    // Get config from file
    try {
      fileConfig = readCacheConfigFile(cacheConfigFileName);
    }
    catch (FileNotFoundException fnfeIgnore) {
      fileConfig = newConfiguration();
    }
    if (fileConfig.isSealed()) {
      fileConfig = fileConfig.copy();
    }

    // Add or update
    if (updateConfig != null && !updateConfig.isEmpty()) {
      for (Iterator iter = updateConfig.keyIterator() ; iter.hasNext() ; ) {
        String key = (String)iter.next();
        fileConfig.put(key, updateConfig.get(key));
      }
    }

    // Delete
    if (deleteSet != null && !deleteSet.isEmpty()) {
      if (updateConfig == null) {
        updateConfig = ConfigManager.newConfiguration();
      }
      for (Iterator iter = deleteSet.iterator() ; iter.hasNext() ; ) {
        String key = (String)iter.next();
        if (updateConfig.containsKey(key)) {
          throw new IllegalArgumentException("The following key appears in the update set and in the delete set: " + key);
        }
        else {
          fileConfig.remove(key);
        }
      }
    }

    // Write out file
    writeCacheConfigFile(fileConfig, cacheConfigFileName, header);
  }

  // Remote config failover mechanism maintain local copy of remote config
  // files, uses them on daemon startup if origin file not available

  File remoteConfigFailoverDir;			// Copies written to this dir
  File remoteConfigFailoverInfoFile;		// State file
  RemoteConfigFailoverMap rcfm;
  long remoteConfigFailoverMaxAge = DEFAULT_REMOTE_CONFIG_FAILOVER_MAX_AGE;

  /** Records state of one config failover file */
  static class RemoteConfigFailoverInfo implements LockssSerializable {
    final String url;
    String filename;
    String chksum;
    long date;
    transient File dir;
    transient File tempfile;
    transient int seq;

    RemoteConfigFailoverInfo(String url, File dir, int seq) {
      this.dir = dir;
      this.url = url;
      this.seq = seq;
    }

    void setRemoteConfigFailoverDir(File dir) {
      this.dir = dir;
    }

    String getChksum() {
      return chksum;
    }

    void setChksum(String chk) {
      this.chksum = chk;
    }

    String getUrl() {
      return url;
    }

    String getFilename() {
      return filename;
    }

    boolean exists() {
      return filename != null && getPermFileAbs().exists();
    }

    File getTempFile() {
      return tempfile;
    }

    void setTempFile(File tempfile) {
      this.tempfile = tempfile;
    }

    boolean update() {
      if (tempfile != null) {
	String pname = getOrMakePermFilename();
	File pfile = new File(dir, pname);
	log.debug2("Rename " + tempfile + " -> " + pfile);
	PlatformUtil.updateAtomically(tempfile, pfile);
	tempfile = null;
	date = TimeBase.nowMs();
	filename = pname;
	return true;
      } else {
	return false;
      }
    }

    File getPermFileAbs() {
      if (filename == null) {
	return null;
      }
      return new File(dir, filename);
    }

    long getDate() {
      return date;
    }

    String getOrMakePermFilename() {
      if (filename != null) {
	return filename;
      }
      try {
	log.debug2("Making perm filename from: " + url);
	String path = UrlUtil.getPath(url);
	String name = FilenameUtils.getBaseName(path);
	String ext = FilenameUtils.getExtension(path);
	return String.format("%02d-%s.%s.gz", seq, name, ext);
      } catch (MalformedURLException e) {
	log.warning("Error building fialover filename", e);
	return String.format("%02d-config-file.gz", seq);
      }
    }
  }

  /** Maps URL to rel filename */
  static class RemoteConfigFailoverMap implements LockssSerializable {
    Map<String,RemoteConfigFailoverInfo> map =
      new HashMap<String,RemoteConfigFailoverInfo>();
    int seq;

    RemoteConfigFailoverInfo put(String url, RemoteConfigFailoverInfo rcfi) {
      return map.put(url, rcfi);
    }

    RemoteConfigFailoverInfo get(String url) {
      return map.get(url);
    }

    int nextSeq() {
      return ++seq;
    }

    Collection<RemoteConfigFailoverInfo> getColl() {
      return map.values();
    }

    boolean update() {
      boolean isModified = false;
      for (RemoteConfigFailoverInfo rcfi : getColl()) {
	isModified |= rcfi.update();
      }
      return isModified;
    }

    void setRemoteConfigFailoverDir(File dir) {
      for (RemoteConfigFailoverInfo rcfi : getColl()) {
	rcfi.setRemoteConfigFailoverDir(dir);
      }
    }
  }

  void setUpRemoteConfigFailover() {
    Configuration plat = getPlatformConfig();
    // Check whether this is not happening in a REST Configuration service
    // environment and remote configuration failover is not disabled.
    if (!restConfigClient.isActive() &&
	plat.getBoolean(PARAM_REMOTE_CONFIG_FAILOVER,
			DEFAULT_REMOTE_CONFIG_FAILOVER)) {
      remoteConfigFailoverDir =
	new File(cacheConfigDir, plat.get(PARAM_REMOTE_CONFIG_FAILOVER_DIR,
					  DEFAULT_REMOTE_CONFIG_FAILOVER_DIR));
      if (FileUtil.ensureDirExists(remoteConfigFailoverDir)) {
	if (remoteConfigFailoverDir.canWrite()) {
	  remoteConfigFailoverInfoFile =
	    new File(cacheConfigDir, REMOTE_CONFIG_FAILOVER_FILENAME);
	  rcfm = loadRemoteConfigFailoverMap();
	  remoteConfigFailoverMaxAge =
	    plat.getTimeInterval(PARAM_REMOTE_CONFIG_FAILOVER_MAX_AGE,
				 DEFAULT_REMOTE_CONFIG_FAILOVER_MAX_AGE);
	} else {
	  log.error("Can't write to remote config failover dir: " +
		    remoteConfigFailoverDir);
	  remoteConfigFailoverDir = null;
	  rcfm = null;
	}
      } else {
	log.error("Can't create remote config failover dir: " +
		  remoteConfigFailoverDir);
	remoteConfigFailoverDir = null;
	rcfm = null;
      }
    } else {
      remoteConfigFailoverDir = null;
      rcfm = null;
    }
  }

  public boolean isRemoteConfigFailoverEnabled() {
    return rcfm != null;
  }

  RemoteConfigFailoverInfo getRcfi(String url) {
    if (!isRemoteConfigFailoverEnabled()) return null;
    RemoteConfigFailoverInfo rcfi = rcfm.get(url);
    if (rcfi == null) {
      rcfi = new RemoteConfigFailoverInfo(url,
					  remoteConfigFailoverDir,
					  rcfm.nextSeq());
      rcfm.put(url, rcfi);
    }
    return rcfi;
  }

  public File getRemoteConfigFailoverFile(String url) {
    if (!isRemoteConfigFailoverEnabled()) return null;
    RemoteConfigFailoverInfo rcfi = getRcfi(url);
    if (rcfi == null || !rcfi.exists()) {
      return null;
    }
    if (remoteConfigFailoverMaxAge > 0 &&
	TimeBase.msSince(rcfi.getDate()) > remoteConfigFailoverMaxAge) {
      log.error("Remote config failover file is too old (" +
		StringUtil.timeIntervalToString(TimeBase.msSince(rcfi.getDate())) +
		" > " + TimeUtil.timeIntervalToString(remoteConfigFailoverMaxAge) +
		"): " + url);
      return null;
    }
    return rcfi.getPermFileAbs();
  }    

  public File getRemoteConfigFailoverTempFile(String url) {
    if (!isRemoteConfigFailoverEnabled()) return null;
    RemoteConfigFailoverInfo rcfi = getRcfi(url);
    if (rcfi == null) {
      return null;
    }
    File tempfile = rcfi.getTempFile();
    if (tempfile != null) {
      log.warning("getRemoteConfigFailoverTempFile: temp file already exists for " + url);
      FileUtil.safeDeleteFile(tempfile);
      rcfi.setTempFile(null);
    }
    try {
      tempfile =
	FileUtil.createTempFile("remote_config", ".tmp",
				remoteConfigFailoverDir);
    } catch (IOException e) {
      log.error("Can't create temp file for remote config failover copy of "
		+ url + " in " + remoteConfigFailoverDir, e);
    }
    rcfi.setTempFile(tempfile);
    return tempfile;
  }

  void updateRemoteConfigFailover() {
    if (!isRemoteConfigFailoverEnabled()) return;
    if (rcfm.update()) {
      try {
	storeRemoteConfigFailoverMap(remoteConfigFailoverInfoFile);
      } catch (IOException | SerializationException e) {
	log.error("Error storing remote config failover map", e);
      }
    }
  }

  void storeRemoteConfigFailoverMap(File file)
      throws IOException, SerializationException {
    log.debug2("storeRemoteConfigFailoverMap: " + file);
    try {
      ObjectSerializer serializer = new XStreamSerializer();
      serializer.serialize(file, rcfm);
    } catch (Exception e) {
      log.error("Could not store remote config failover map", e);
      throw e;
    }
  }

  /**
   * Load RemoteConfigFailoverMap from a file
   * @param file         A source file.
   * @return RemoteConfigFailoverMap instance loaded from file (or a default
   *         value).
   */
  RemoteConfigFailoverMap loadRemoteConfigFailoverMap() {
    try {
      log.debug2("Loading RemoteConfigFailoverMap");
      ObjectSerializer deserializer = new XStreamSerializer();
      RemoteConfigFailoverMap map =
	(RemoteConfigFailoverMap)deserializer.deserialize(remoteConfigFailoverInfoFile);
      map.setRemoteConfigFailoverDir(remoteConfigFailoverDir);
      return map;
    } catch (SerializationException.FileNotFound se) {
      log.debug("No RemoteConfigFailoverMap, creating new one");
      return new RemoteConfigFailoverMap();
    } catch (SerializationException se) {
      log.error("Marshalling exception for RemoteConfigFailoverMap", se);
      return new RemoteConfigFailoverMap();
    } catch (Exception e) {
      log.error("Could not load RemoteConfigFailoverMap", e);
      throw new RuntimeException("Could not load RemoteConfigFailoverMap", e);
    }
  }


  // Testing assistance

  void setGroups(String groups) {
    this.groupNames = groups;
  }

  // TinyUI comes up on port 8081 if can't complete initial props load

  TinyUi tiny = null;
  String[] tinyData = new String[1];

  void startTinyUi() {
    TinyUi t = new TinyUi(tinyData);
    updateTinyData();
    t.startTiny();
    tiny = t;
  }

  void stopTinyUi() {
    if (tiny != null) {
      tiny.stopTiny();
      tiny = null;
      // give listener socket a little time to close
      try {
	Deadline.in(2 * Constants.SECOND).sleep();
      } catch (InterruptedException e ) {
      }
    }
  }

  void updateTinyData() {
    tinyData[0] = recentLoadError;
  }

  // Reload thread

  void startHandler() {
    if (handlerThread != null) {
      log.warning("Handler already running; stopping old one first");
      stopHandler();
    } else {
      log.info("Starting handler");
    }
    handlerThread = new HandlerThread("ConfigHandler");
    handlerThread.start();
  }

  void stopHandler() {
    if (handlerThread != null) {
      log.info("Stopping handler");
      handlerThread.stopHandler();
      handlerThread = null;
    } else {
//       log.warning("Attempt to stop handler when it isn't running");
    }
  }

  /**
   * Provides the daemon bootstrap properties URL.
   *
   * @return a String with the daemon bootstrap properties URL.
   */
  public String getBootstrapPropsUrl() {
    return bootstrapPropsUrl;
  }

  /**
   * Provides the Configuration REST service client.
   *
   * @return a RestConfigClient with the Configuration REST service client.
   */
  public RestConfigClient getRestConfigClient() {
    return restConfigClient;
  }

  /**
   * Populates the map of resource configuration files.
   * 
   * @return a Map<String, File> with the map of resource configuration files.
   */
  private Map<String, File> populateResourceFileMap() {
    final String DEBUG_HEADER = "populateResourceFileMap(): ";
    if (log.isDebug2()) log.debug2(DEBUG_HEADER + "Invoked.");

    Map<String, File> result = new HashMap<String, File>();

    // Get the URL of the directory with the resource configuration files.
    URL resourceConfigUrl =
	getClass().getClassLoader().getResource(RESOURCE_CONFIG_DIR_PATH);
    if (log.isDebug3())
      log.debug3(DEBUG_HEADER + "resourceConfigUrl = " + resourceConfigUrl);

    // Check whether there is no directory with the resource configuration
    // files.
    if (resourceConfigUrl == null) {
      // Yes: Nothing more to do.
      if (log.isDebug2()) log.debug2(DEBUG_HEADER + "result = " + result);
      return result;
    }

    // No: Get the directory with the resource configuration files.
    File resourceConfigDir = new File(resourceConfigUrl.getFile());
    if (log.isDebug3())
      log.debug3(DEBUG_HEADER + "resourceConfigDir = " + resourceConfigDir);

    // Check whether the directory is valid.
    if (resourceConfigDir.exists() && resourceConfigDir.isDirectory()
	&& resourceConfigDir.canRead()) {
      // Yes: Get any files in the directory.
      File[] resourceFiles = resourceConfigDir.listFiles();
      if (log.isDebug3())
	log.debug3(DEBUG_HEADER + "resourceFiles = " + resourceFiles);

      // Check whether there are any files in the directory.
      if (resourceFiles != null) {
	// Yes: Loop through all the resource configuration files.
	for (int i = 0; i < resourceFiles.length; i++) {
	  // Add the resource configuration file to the map. 
	  File resourceFile = resourceFiles[i];
	  if (log.isDebug3())
	    log.debug3(DEBUG_HEADER + "resourceFile = " + resourceFile);

	  result.put(resourceFile.getName(), resourceFile);
	}
      }
    }

    if (log.isDebug2()) log.debug2(DEBUG_HEADER + "result = " + result);
    return result;
  }

  /**
   * Provides an indication of whether a resource configuration file with a
   * given name exists.
   * 
   * @param fileName
   *          A String with the name.
   * @return a boolean with <code>true</code> if a resource configuration file
   *         with the given name exists, <code>false</code> otherwise.
   */
  public boolean existsResourceConfigFile(String fileName) {
    return resourceConfigFiles.containsKey(fileName);
  }

  /**
   * Provides a resource configuration file, given its name.
   * 
   * @param fileName
   *          A String with the resource configuration file name.
   * @return a File with the requested resource configuration file, or
   *         <code>null</code> if no resource configuration file with that name
   *         exists.
   */
  public File getResourceConfigFile(String fileName) {
    return resourceConfigFiles.get(fileName);
  }

  /**
   * Provides an input stream to the content of a cached configuration file,
   * ignoring previous history.
   * <br>
   * Use this to stream the file contents.
   * 
   * @param cacheConfigFileName
   *          A String with the cached configuration file name, without path.
   * @return an InputStream with the input stream to the cached configuration
   *         file.
   * @throws IOException
   *           if there are problems.
   */
  public InputStream getCacheConfigFileInputStream(String cacheConfigUrl)
      throws IOException {
    final String DEBUG_HEADER = "getCacheConfigFileInputStream(): ";
    if (log.isDebug2())
      log.debug2(DEBUG_HEADER + "cacheConfigUrl = " + cacheConfigUrl);

    InputStream is = configCache.find(cacheConfigUrl).getInputStream();
    if (log.isDebug2())
      log.debug2(DEBUG_HEADER + "is == null = " + (is == null));
    return is;
  }

  /**
   * Provides a configuration file in the cache.
   * 
   * @param cacheConfigFileName
   *          A String with the cached configuration file name, without path.
   * @return a ConfigFile with the cached configuration file.
   * @throws IOException
   *           if there are problems.
   */
  private ConfigFile getConfigFileInCache(String cacheConfigFileName)
      throws IOException {
    final String DEBUG_HEADER = "getConfigFileInCache(): ";
    if (log.isDebug2())
      log.debug2(DEBUG_HEADER + "cacheConfigFileName = " + cacheConfigFileName);

    ConfigFile cf = null;

    // Check whether it is a resource file.
    if (resourceConfigFiles.containsKey(cacheConfigFileName)) {
      // Yes: Get it from the cache.
      if (log.isDebug3()) log.debug3(DEBUG_HEADER + "It is a Resource file.");
      cf = configCache.find(cacheConfigFileName);
    } else {
      // No: Check whether there is no cache directory.
      if (cacheConfigDir == null) {
	// Yes:
	log.warning("Attempting to read cache config file: " +
	    cacheConfigFileName + ", but no cache config dir exists");
	throw new IOException("No cache config dir");
      }

      // No: Get the name of the cached file.
      String cfile = new File(cacheConfigDir, cacheConfigFileName).toString();
      if (log.isDebug3()) log.debug3(DEBUG_HEADER + "cfile = " + cfile);

      // Get it from the cache.
      cf = configCache.find(cfile);
    }

    if (log.isDebug2()) log.debug2(DEBUG_HEADER + "cf = " + cf);
    return cf;
  }

  /** Create and return an instance of DynamicConfigFile that will generate
   * content determined by the url.  Logic for new types of dynamic config
   * files should be added here. */
  public DynamicConfigFile newDynamicConfigFile(String url) {
    switch (url) {
    case "dyn:cluster.xml":
      return new DynamicConfigFile(url, this) {
	@Override
	protected void generateFileContent(File file,
					   ConfigManager cfgMgr)
	    throws IOException {
	  generateClusterFile(file);
	}
      };
    default:
      throw new IllegalArgumentException("Unknown dynamic config file: " + url);
    }
  }

  void generateClusterFile(File file) throws IOException {
    StringBuilder sb = new StringBuilder();
    if (clusterUrls == null) {
      clusterUrls = Collections.EMPTY_LIST;
    }
    for (String url : clusterUrls) {
      sb.append("      <value>");
      sb.append(StringEscapeUtils.escapeXml(url));
      sb.append("</value>\n");
    }
    Map<String,String> valMap =
      MapUtil.map("PreUrls", sb.toString(),
		  "PostUrls", "");
    try (Writer wrtr = new BufferedWriter(new FileWriter(file))) {
      TemplateUtil.expandTemplate("org/lockss/config/ClusterTemplate.xml",
	  wrtr, valMap);
    }
  }

  /**
   * Provides the configuration of an archival unit given its identifier and
   * plugin.
   * 
   * @param auId
   *          A String with the identifier of the archival unit.
   * @param plugin
   *          A Plugin with the plugin.
   * @return a Configuration with the configuration of the archival unit.
   */
  public TdbAu getTdbAu(String auId, Plugin plugin) {
    final String DEBUG_HEADER = "getTdbAu(): ";
    if (log.isDebug2()) {
      log.debug2(DEBUG_HEADER + "auId = " + auId);
      log.debug2(DEBUG_HEADER + "plugin = " + plugin);
    }

    // Get the Archival Unit title database from the current configuration.
    TdbAu tdbAu = TdbUtil.getTdbAu(auId, plugin);
    if (log.isDebug3()) log.debug3(DEBUG_HEADER + "tdbAu = " + tdbAu);

    // Check whether the Archival Unit title database has not been found and the
    // configuration is obtained via a Configuration REST web service.
    if (tdbAu == null && restConfigClient.isActive()) {
      // Yes.
      try {
	// Get the Archival Unit title database from the Configuration REST web
	// service.
	TdbAu newTdbAu = restConfigClient.getTdbAu(auId);
	if (log.isDebug3()) log.debug3(DEBUG_HEADER + "newTdbAu = " + newTdbAu);
	newTdbAu.prettyLog(2);

	// Create a new title database.
	Tdb copyTdb = new Tdb();

	// Add the new Archival Unit title database to this new title database.
	boolean added = copyTdb.addTdbAu(newTdbAu);
	if (log.isDebug3()) log.debug3(DEBUG_HEADER + "added = " + added);

	if (added) {
	  tdbAu = copyTdb.getTdbAuById(newTdbAu);
	  if (log.isDebug3()) log.debug3(DEBUG_HEADER + "tdbAu = " + tdbAu);
	}

	// Copy the current title database to the new one.
	copyTdb.copyFrom(getCurrentConfig().getTdb());

	// Make a copy of the current configuration.
	Configuration newConfig = getCurrentConfig().copy();

	// Add to this new configuration the new title database.
	newConfig.setTdb(copyTdb);

	// Install the new configuration.
	installConfig(newConfig);
      } catch (Exception e) {
	log.error("Exception caught getting the configuration of Archival Unit "
	    + auId, e);
      }
    }

    if (tdbAu != null) {
      tdbAu.prettyLog(2);
    }

    return tdbAu;
  }

  /**
   * Adds to the configuration, if necessary, the title database of an archival
   * unit given its identifier and plugin.
   * 
   * @param auId
   *          A String with the identifier of the archival unit.
   * @param plugin
   *          A Plugin with the plugin.
   */
  public void addTdbAu(String auId, Plugin plugin) {
    final String DEBUG_HEADER = "addTdbAu(): ";
    if (log.isDebug2()) {
      log.debug2(DEBUG_HEADER + "auId = " + auId);
      log.debug2(DEBUG_HEADER + "plugin = " + plugin);
    }

    // Get the Archival Unit title database from the current configuration.
    TdbAu tdbAu = TdbUtil.getTdbAu(auId, plugin);
    if (log.isDebug3()) log.debug3(DEBUG_HEADER + "tdbAu = " + tdbAu);

    // Check whether the Archival Unit title database has not been found and the
    // configuration is obtained via a Configuration REST web service.
    if (tdbAu == null && restConfigClient.isActive()) {
      // Yes.
      try {
	// Get the Archival Unit title database from the Configuration REST web
	// service.
	TdbAu newTdbAu = restConfigClient.getTdbAu(auId);
	if (log.isDebug3()) log.debug3(DEBUG_HEADER + "newTdbAu = " + newTdbAu);
	newTdbAu.prettyLog(2);

	// Create a new title database.
	Tdb copyTdb = new Tdb();

	// Add the new Archival Unit title database to this new title database.
	boolean added = copyTdb.addTdbAu(newTdbAu);
	if (log.isDebug3()) log.debug3(DEBUG_HEADER + "added = " + added);

	if (added) {
	  tdbAu = copyTdb.getTdbAuById(newTdbAu);
	  if (log.isDebug3()) log.debug3(DEBUG_HEADER + "tdbAu = " + tdbAu);
	}

	// Copy the current title database to the new one.
	copyTdb.copyFrom(getCurrentConfig().getTdb());

	// Make a copy of the current configuration.
	Configuration newConfig = getCurrentConfig().copy();

	// Add to this new configuration the new title database.
	newConfig.setTdb(copyTdb);

	// Install the new configuration.
	installConfig(newConfig);
      } catch (Exception e) {
	log.error("Exception caught getting the configuration of Archival Unit "
	    + auId, e);
      }
    }

    if (tdbAu != null) {
      tdbAu.prettyLog(2);
    }
  }

  /**
   * Provides the configuration of an archival unit given its identifier.
   * 
   * @param auId
   *          A String with the identifier of the archival unit.
   * @return a Configuration with the configuration of the archival unit.
   */
  public Configuration getAuConfig(String auId, Plugin plugin) {
    final String DEBUG_HEADER = "getAuConfig(): ";
    if (log.isDebug2()) {
      log.debug2(DEBUG_HEADER + "auId = " + auId);
      log.debug2(DEBUG_HEADER + "plugin = " + plugin);
    }

    Configuration auConfig = null;

    // Get the Archival Unit title database.
    TdbAu tdbAu = getTdbAu(auId, plugin);

    // Get the Archival Unit configuration, if possible.
    if (tdbAu != null) {
      tdbAu.prettyLog(2);
      Properties properties = new Properties();
      properties.putAll(tdbAu.getParams());

      auConfig = ConfigManager.fromPropertiesUnsealed(properties);
      if (log.isDebug3()) log.debug3(DEBUG_HEADER + "auConfig = " + auConfig);
    }

    // Check whether no Archival Unit configuration was found.
    if (auConfig == null) {
      // Yes: Try to get it from the REST Configuration service.
      if (restConfigClient.isActive()) {
	try {
	  auConfig = restConfigClient.getAuConfig(auId);
	  if (log.isDebug3())
	    log.debug3(DEBUG_HEADER + "auConfig = " + auConfig);
	} catch (Exception e) {
	  log.error("Exception caught getting the configuration of Archival "
	      + "Unit " + auId, e);
	}
      }

      // Check whether no Archival Unit configuration was found.
      if (auConfig == null) {
        // Yes: Create an empty Archival Unit configuration.
        auConfig = ConfigManager.EMPTY_CONFIGURATION;
        if (log.isDebug3()) log.debug3(DEBUG_HEADER + "auConfig = " + auConfig);
      }
    }

    if (log.isDebug2()) log.debug2(DEBUG_HEADER + "auConfig = " + auConfig);
    return auConfig;
  }

  /**
   * Provides the configuration file of the parent URL of another URL.
   * 
   * @param url
   *          A String with The URL for which the parent URL is requested.
   * @return a ConfigFile with ConfigFile of a URL that is the parent of the
   *         passed URL, or <code>null</code> if the passed URL does not have a
   *         parent URL.
   */
  ConfigFile getUrlParent(String url) {
    final String DEBUG_HEADER = "getUrlParent(): ";
    if (log.isDebug2()) log.debug2(DEBUG_HEADER + "url = " + url);

    ConfigFile urlParent = parentConfigFile.get(url);
    if (log.isDebug2()) log.debug2(DEBUG_HEADER + "urlParent = " + urlParent);
    return urlParent;
  }

  // JMS notification support

  public static final String JMS_PREFIX = PREFIX + "jms.";

  /** If true, ConfigManager will send notifications of config-changed
   * events (if it is running as part of a REST config service), or
   * register to receive such events (if it's runnning in a client of a
   * config service).
   * @ParamRelevance Rare
   */
  public static final String PARAM_ENABLE_JMS_NOTIFICATIONS =
    JMS_PREFIX + "enable";
  public static final boolean DEFAULT_ENABLE_JMS_NOTIFICATIONS = true;

  /** The jms topic at which config changed notifications are sent
   * @ParamRelevance Rare
   */
  public static final String PARAM_JMS_NOTIFICATION_TOPIC =
    JMS_PREFIX + "topic";
  public static final String DEFAULT_JMS_NOTIFICATION_TOPIC =
    "ConfigChangedTopic";

  /** The jms clientid of the config manager.
   * @ParamRelevance Rare
   */
  public static final String PARAM_JMS_CLIENT_ID = JMS_PREFIX + "clientId";
//   public static final String DEFAULT_JMS_CLIENT_ID = "ConfigManger";
  public static final String DEFAULT_JMS_CLIENT_ID = null;

  private Consumer jmsConsumer;
  private Producer jmsProducer;
  private String notificationTopic = DEFAULT_JMS_NOTIFICATION_TOPIC;
  private boolean enableJmsNotifications = DEFAULT_ENABLE_JMS_NOTIFICATIONS;
  private String clientId = DEFAULT_JMS_CLIENT_ID;

  // JMS manager starts after ConfigManger, must wait before creating
  // connections

  // TK This is too late.  Could update between client xfer and appRunning,
  // would miss notification (both server and client problem)
  void schedSetUpJmsNotifications() {
    Thread th = new Thread() {
	public void run() {
	  try {
	    if (getApp() != null) {
	      getApp().waitUntilAppRunning();
	    } else {
	      LockssApp app = LockssApp.getLockssApp();
	      if (app != null) {
		app.waitUntilAppRunning();
	      }
	    }
	    setUpJmsNotifications();
	  } catch (InterruptedException e) {}
	}};
    th.start();
  }

  // Overridable for testing
  protected boolean shouldSendNotifications() {
    return enableJmsNotifications && !restConfigClient.isActive();
  }

  protected boolean shouldReceiveNotifications() {
    return enableJmsNotifications && restConfigClient.isActive();
  }

  void setUpJmsNotifications() {
    if (shouldReceiveNotifications()) {
      // If we're a client of a config service, set up a listener
      log.debug("Creating consumer");
      try {
	jmsConsumer =
	  Consumer.createTopicConsumer(clientId,
					   notificationTopic,
					   new MyMessageListener("Config Listener"));
      } catch (JMSException e) {
	log.error("Couldn't create jms consumer", e);
      }
    }
    if (shouldSendNotifications()) {
      log.debug("Creating producer");
      // else set up a notifier
      try {
	jmsProducer = Producer.createTopicProducer(clientId, notificationTopic);
      } catch (JMSException e) {
	log.error("Couldn't create jms producer", e);
      }
    }
  }

  void notifyConfigChanged() {
    if (jmsProducer != null) {
      try {
	jmsProducer.sendText("now");
      } catch (JMSException e) {
	log.error("foo", e);
      }
    }
  }

  /**
   * Provides the input stream to the content of this configuration file if the
   * passed preconditions are met.
   * 
   * @param cacheConfigUrl
   *          A String with the cached configuration file URL.
   * @param preconditions
   *          An HttpRequestPreconditions with the request preconditions to be
   *          met.
   * @return a ConfigFileReadWriteResult with the result of the operation.
   * @throws IOException
   *           if there are problems accessing the configuration file.
   */
  public ConfigFileReadWriteResult conditionallyReadCacheConfigFile(
      String cacheConfigUrl, HttpRequestPreconditions preconditions)
	  throws IOException {
    final String DEBUG_HEADER = "conditionallyReadCacheConfigFile(): ";
    if (log.isDebug2()) {
      log.debug2(DEBUG_HEADER + "cacheConfigUrl = " + cacheConfigUrl);
      log.debug2(DEBUG_HEADER + "preconditions = " + preconditions);
    }

    ConfigFile cf = configCache.find(cacheConfigUrl);
    if (log.isDebug3()) log.debug3(DEBUG_HEADER + "cf = " + cf);
    return cf.conditionallyRead(preconditions);
  }

  /**
   * Writes the passed content to a cached configuration file if the passed
   * preconditions are met.
   * 
   * @param cacheConfigUrl
   *          A String with the cached configuration file URL.
   * @param preconditions
   *          An HttpRequestPreconditions with the request preconditions to be
   *          met.
   * @param inputStream
   *          An InputStream to the content to be written to the cached
   *          configuration file.
   * @return a ConfigFileReadWriteResult with the result of the operation.
   * @throws IOException
   *           if there are problems.
   */
  public ConfigFileReadWriteResult conditionallyWriteCacheConfigFile(
      String cacheConfigUrl, HttpRequestPreconditions preconditions,
      InputStream inputStream) throws IOException {
    final String DEBUG_HEADER = "conditionallyWriteCacheConfigFile(): ";
    if (log.isDebug2()) {
      log.debug2(DEBUG_HEADER + "cacheConfigUrl = " + cacheConfigUrl);
      log.debug2(DEBUG_HEADER + "preconditions = " + preconditions);
      log.debug2(DEBUG_HEADER + "inputStream = " + inputStream);
    }

    if (cacheConfigDir == null) {
      log.warning("Attempting to write cache config file: " + cacheConfigUrl
	  + ", but no cache config dir exists");
      throw new RuntimeException("No cache config dir");
    }

    ConfigFile cf = configCache.find(cacheConfigUrl);
    if (log.isDebug3()) log.debug3(DEBUG_HEADER + "cf = " + cf);

    // Write the file.
    ConfigFileReadWriteResult writeResult =
	cf.conditionallyWrite(preconditions, inputStream);

    // Check whether the file was successfully written.
    if (writeResult.isPreconditionsMet()) {
      // Yes: Reload the configuration.
      requestReload();
    }

    return writeResult;
  }

  /**
   * Provides the counter of configuration reload requests.
   * 
   * @return an int with the counter of configuration reload requests.
   */
  public int getConfigReloadRequestCounter() {
    return configReloadRequestCounter;
  }

  private class MyMessageListener
    extends Consumer.SubscriptionListener {

    MyMessageListener(String listenerName) {
      super(listenerName);
    }

    @Override
    public void onMessage(Message message) {
      requestReload();
//       try {
//         msgObject =  Consumer.convertMessage(message);
//       }
//       catch (JMSException e) {
// 	log.warning("foo", e);
//       }
    }

  }

  class AbortConfigLoadException extends RuntimeException {
    AbortConfigLoadException(String msg) {
      super(msg);
    }

    AbortConfigLoadException(String msg, Throwable cause) {
      super(msg, cause);
    }
  }

  // Handler thread, periodically reloads config

  private class HandlerThread extends LockssThread {
    private long lastReload = 0;
    private volatile boolean goOn = true;
    private Deadline nextReload;
    private volatile boolean running = false;
    private volatile boolean goAgain = false;

    private HandlerThread(String name) {
      super(name);
    }

    public void lockssRun() {
      Thread.currentThread().setPriority(Thread.NORM_PRIORITY + 1);
      startWDog(WDOG_PARAM_CONFIG, WDOG_DEFAULT_CONFIG);
      triggerWDogOnExit(true);

      // repeat every 10ish minutes until first successful load, then
      // according to org.lockss.parameterReloadInterval, or 30 minutes.
      while (goOn) {
	pokeWDog();
	running = true;
	try {
	  if (updateConfig()) {
	    if (tiny != null) {
	      stopTinyUi();
	    }
	    // true iff loaded config has changed
	    if (!goOn) {
	      break;
	    }
	    lastReload = TimeBase.nowMs();
	    //	stopAndOrStartThings(true);
	  } else {
	    if (lastReload == 0) {
	      if (tiny == null) {
		startTinyUi();
	      } else {
		updateTinyData();
	      }
	    }
	  }
	  pokeWDog();			// in case update took a long time
	  long reloadRange = reloadInterval/4;
	  nextReload = Deadline.inRandomRange(reloadInterval - reloadRange,
					      reloadInterval + reloadRange);
	  log.debug2(nextReload.toString());
	  running = false;
	  if (goOn && !goAgain) {
	    try {
	      nextReload.sleep();
	    } catch (InterruptedException e) {
	      // just wakeup and check for exit
	    }
	  }
	} catch (AbortConfigLoadException e) {
	  log.warning("Config reload thread aborted");
	  // just exit
	}
	goAgain = false;
      }
    }

    private void stopHandler() {
      goOn = false;
      this.interrupt();
    }

    void forceReloadIn(long millis) {
      if (running) {
	// can be called from reload thread, in which case an immediate
	// repeat is necessary
	goAgain = true;
      }
      if (nextReload != null) {
	nextReload.expireIn(millis);
      }
    }
  }
}
