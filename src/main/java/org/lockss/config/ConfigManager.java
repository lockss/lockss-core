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
package org.lockss.config;

import java.io.*;
import java.util.*;
import java.util.function.Function;
import java.net.*;
import java.sql.Connection;
import org.apache.commons.collections.Predicate;
import org.apache.commons.io.*;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.StringEscapeUtils;
import org.apache.oro.text.regex.*;
import org.lockss.app.*;
import org.lockss.account.*;
import org.lockss.clockss.*;
import org.lockss.config.db.ConfigDbManager;
import org.lockss.config.db.ConfigManagerSql;
import org.lockss.daemon.*;
import org.lockss.db.DbException;
import org.lockss.db.DbManager;
import org.lockss.hasher.*;
import org.lockss.mail.*;
import org.lockss.plugin.*;
import org.lockss.protocol.*;
import org.lockss.proxy.*;
import org.lockss.remote.*;
import org.lockss.repository.*;
import org.lockss.rs.exception.LockssRestException;
import org.lockss.rs.exception.LockssRestHttpException;
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

  /** Used in testing as an alternative to passing the REST config URL to
   * makeConfigManager() */
  public static final String SYSPROP_REST_CONFIG_SERVICE_URL =
    MYPREFIX + "restConfigServiceUrl";

  /** The interval at which the daemon checks the various configuration
   * files, including title dbs, for changes.
   * @ParamCategory Tuning
   * @ParamRelevance Rare
   */
  static final String PARAM_RELOAD_INTERVAL = MYPREFIX + "reloadInterval";
  static final long DEFAULT_RELOAD_INTERVAL = 30 * Constants.MINUTE;

  /** If set to <i>hostname</i>:<i>port</i>, the configuration server will
   * be accessed via the specified proxy.  For direct connection, leave
   * unset or set to <tt>DIRECT</tt> or <tt>NONE</tt>
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
    userTitleDbMap.put("message", "user title DBs");
    URL_PARAMS.put(PARAM_USER_TITLE_DB_URLS, userTitleDbMap);

    Map<String, Object> globalTitleDbMap = new HashMap<String, Object>();
    globalTitleDbMap.put("message", "global titledb");
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
//   public static final boolean DEFAULT_REMOTE_CONFIG_FAILOVER = true;
  public static final boolean DEFAULT_REMOTE_CONFIG_FAILOVER = false;

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
  public static final String CONFIG_FILE_EXPERT_CLUSTER = "expert_config.txt";
  public static final String CONFIG_FILE_EXPERT_LOCAL =
    "expert_config_local.txt";

  /** Obsolescent - replaced by CONFIG_FILE_CONTENT_SERVERS */
  public static final String CONFIG_FILE_ICP_SERVER = "icp_server_config.txt";
  /** Obsolescent - replaced by CONFIG_FILE_CONTENT_SERVERS */
  public static final String CONFIG_FILE_AUDIT_PROXY =
    "audit_proxy_config.txt";

  public static final String REMOTE_CONFIG_FAILOVER_FILENAME =
    "remote_config_failover_info.xml";

  /** URL of the dynamic cluster config "file" */
  public static final String CLUSTER_URL = "dyn:cluster.xml";

  /** Name of ClientCacheSpec used by HTTPConfigFile */
  public static final String HTTP_CACHE_NAME = "HTTPConfigFile";

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
    boolean includeInCluster = true;
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

    boolean isIncludeInCluster() {
      return includeInCluster;
    }

    LocalFileDescr setIncludeInClusterConfig(boolean val) {
      this.includeInCluster = val;
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
    new LocalFileDescr(CONFIG_FILE_ICP_SERVER), // obsolescent
    new LocalFileDescr(CONFIG_FILE_AUDIT_PROXY),	// obsolescent
    // must follow obsolescent icp server and audit proxy files
    new LocalFileDescr(CONFIG_FILE_CONTENT_SERVERS),
    new LocalFileDescr(CONFIG_FILE_ACCESS_GROUPS), // not yet in use
    new LocalFileDescr(CONFIG_FILE_CRAWL_PROXY),
    new LocalFileDescr(CONFIG_FILE_EXPERT_CLUSTER)
    .setKeyPredicate(expertConfigKeyPredicate)
    .setIncludePredicate(expertConfigIncludePredicate),
    new LocalFileDescr(CONFIG_FILE_EXPERT_LOCAL)
    .setKeyPredicate(expertConfigKeyPredicate)
    .setIncludePredicate(expertConfigIncludePredicate)
    .setIncludeInClusterConfig(false),
  };

  private static final Logger log = Logger.getLogger();

  /** A constant empty Configuration object */
  public static final Configuration EMPTY_CONFIGURATION = newConfiguration();
  static {
    EMPTY_CONFIGURATION.seal();
  }

  protected LockssApp theApp = null;
  protected boolean isInited = false;
  protected boolean isStarted = false;

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

  private File daemonTmpDir;

  private ConfigCache configCache;
  private LockssUrlConnectionPool connPool = new LockssUrlConnectionPool();
  private LockssSecureSocketFactory secureSockFact;

  long reloadInterval = 10 * Constants.MINUTE;
  private long sendVersionEvery = DEFAULT_SEND_VERSION_EVERY;
  private int maxDeferredAuBatchSize = DEFAULT_MAX_DEFERRED_AU_BATCH_SIZE;

  private List<Pattern> expertConfigAllowPats;
  private List<Pattern> expertConfigDenyPats;
  private boolean enableExpertConfig;

  // URLs that are loaded first and go into the platform config, which is
  // available early.
  private List<String> bootstrapPropsUrls;

  // The Configuration REST web service client.
  RestConfigClient restConfigClient = null;

  // The URL of the REST Configuration service.
  String restConfigServiceUrl = null;

  // The map of parent configuration files.
  Map<String, ConfigFile> parentConfigFile = new HashMap<String, ConfigFile>();

  HttpCacheManager hCacheMgr;

  // The counter of configuration reload requests. Accessed from separate
  // threads.
  private volatile int configReloadRequestCounter = 0;

  // The configuration manager SQL executor.
  private ConfigManagerSql configManagerSql = null;

  private boolean noNag = false;

  /** How often to commit when adding Archival Unit configurations to the
   * database.
   * @ParamRelevance Rare
   */
  public static final String PARAM_AU_INSERT_COMMIT_COUNT =
      MYPREFIX + "auInsertCommitCount";
  public static final int DEFAULT_AU_INSERT_COMMIT_COUNT = 50;

  private int auInsertCommitCount = DEFAULT_AU_INSERT_COMMIT_COUNT;

  /** This constructor is used only for tests */
  public ConfigManager() {
    this(null, System.getProperty(SYSPROP_REST_CONFIG_SERVICE_URL), null, null);

    URL_PARAMS.get(PARAM_AUX_PROP_URLS).put("predicate", trueKeyPredicate);
    // Fail the load if file in auxPropUrls is missing, allow missing
    // titledb URLs
    URL_PARAMS.get(PARAM_AUX_PROP_URLS).put("required", true);
    URL_PARAMS.get(PARAM_USER_TITLE_DB_URLS).put("predicate", titleDbOnlyPred);
    URL_PARAMS.get(PARAM_TITLE_DB_URLS).put("predicate", titleDbOnlyPred);
  }

  public ConfigManager(List urls) {
    this(urls, null);
  }

  public ConfigManager(List urls, String groupNames) {
    this(null, urls, groupNames);
  }

  public ConfigManager(List<String> bootstrapPropsUrls,
		       List<String> urls,
		       String groupNames) {
    this(bootstrapPropsUrls, null, urls, groupNames);
  }

  public ConfigManager(List<String> bootstrapPropsUrls,
		       String restConfigServiceUrl,
		       List<String> urls,
		       String groupNames) {
    this.bootstrapPropsUrls = bootstrapPropsUrls;
    this.restConfigServiceUrl = restConfigServiceUrl;
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

  public synchronized HttpCacheManager getHttpCacheManager() {
    if (hCacheMgr == null) {
      hCacheMgr = new HttpCacheManager(getTmpDir());
      // Set up http cache dir for HTTPConfigFile
      File cacheDir = ensureDir(getTmpDir(), "hcfcache");
      log.info("http cache dir: " + cacheDir);
      getHttpCacheManager().getCacheSpec(HTTP_CACHE_NAME)
	.setCacheDir(cacheDir)
	.setResourceFactory(new GzippedFileResourceFactory(cacheDir))
	;
    }
    return hCacheMgr;
  }

  public void initService(LockssApp app) throws LockssAppException {
    isInited = true;
    theApp = app;
  }

  /** Called to start each service in turn, after all services have been
   * initialized.  Service should extend this to perform any startup
   * necessary. */
  public void startService() {
    isStarted = true;
    // Start the configuration handler that will periodically check the
    // configuration files.
    startHandler();
  }

  /** Reset to unconfigured state.  See LockssTestCase.tearDown(), where
   * this is called.)
   */
  public void stopService() {
    stopJms();
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

  public boolean isInited() {
    return isInited;
  }

  /**
   * Return true iff this manager's startService() has been called.
   * @return true if the manager is started
   */
  public boolean isStarted() {
    return isStarted;
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

  public static ConfigManager makeConfigManager(List<String> bootstrapPropsUrls,
						String restConfigServiceUrl,
						List<String> urls,
						String groupNames) {
    theMgr = new ConfigManager(bootstrapPropsUrls, restConfigServiceUrl, urls,
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

  public ConfigManager setNoNag() {
    noNag = true;
    return this;
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

  public static Configuration getPlatformConfigOnly() {
    return platformConfig;
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

  /** Run a config changed callback.  Used only in config-reload context,
   * throws AbortConfigLoadException if interrupted */
  void runCallback(Configuration.Callback cb,
		   Configuration newConfig,
		   Configuration oldConfig,
		   Configuration.Differences diffs) {
    if (Thread.currentThread().isInterrupted()) {
      throw new AbortConfigLoadException("Interrupted");
    }
    try {
      cb.configurationChanged(newConfig, oldConfig, diffs);
    } catch (Exception e) {
      log.error("callback threw", e);
    }
  }

  /** Run all the config changed callbacks.  Used only in config-reload
   * context, throws AbortConfigLoadException if interrupted */
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

    runCallbacks(new java.util.function.Consumer<Configuration.Callback>() {
	public void accept(Configuration.Callback cb) {
	  runCallback(cb, newConfig, oldConfig, diffs);
	}});
  }

  /** Invoke a method on all the callbacks.
   * @param cbInvoker a Consumer that calls the desired
   * Configuration.Callback method */
  private void runCallbacks(java.util.function.Consumer cbInvoker) {
    // copy the list of callbacks as it could change during the loop.
    List cblist = new ArrayList(configChangedCallbacks);
    for (Iterator iter = cblist.iterator(); iter.hasNext();) {
      try {
	Configuration.Callback cb = (Configuration.Callback)iter.next();
	cbInvoker.accept(cb);
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
      String proxySpec = cf.getProxyUsed();
      if (proxySpec != null) {
	sb.append("<br>Using proxy: ");
	sb.append(proxySpec);
      }
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
  public List<String> getSpecUrlList() {
    return specUrls;
  }

  /**
   * @return the List of urls from which the config was actually loaded.
   * This differs from {@link #getSpecUrlList()} in that it reflects any
   * failover to local copies.
   */
  public List<String> getLoadedUrlList() {
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

	Map<String,Object> keyParams = URL_PARAMS.get(includingKey);
	// Add the generations of the resolved URLs to the list, if not there
	// already.
	String message = (String)(keyParams.get("message"));
	if (log.isDebug3()) log.debug3(DEBUG_HEADER + "message = " + message);
	ConfigManager.KeyPredicate keyPredicate =
	  (ConfigManager.KeyPredicate) (keyParams.get("predicate"));
	if (log.isDebug3())
	  log.debug3(DEBUG_HEADER + "keyPredicate = " + keyPredicate);

	boolean req = (boolean)keyParams.getOrDefault("required", false);
	addGenerationsToListIfNotInIt(getConfigGenerations(resolvedUrls,
	    req, reload, message, keyPredicate), targetList);

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
      invalidateClusterFile();
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
  private volatile long lastUpdateTime;
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
    // Add app defaults
    mergeAppConfig(newConfig, LockssApp::getBootDefault, "app bootstrap default");
    mergeAppConfig(newConfig, LockssApp::getAppDefault, "app default");
    loadList(newConfig, gens);
    // Add app un-overridable config
    mergeAppConfig(newConfig, LockssApp::getAppConfig, "app config");
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
      log.debug3(DEBUG_HEADER
	  + "intoConfig.keySet().size() = " + intoConfig.keySet().size());
      log.debug3(DEBUG_HEADER + "gens.size() = " + gens.size());
    }
    for (ConfigFile.Generation gen : gens) {
      if (gen != null) {
	if (log.isDebug3()) log.debug3(DEBUG_HEADER
	    + "gen.getConfig().keySet().size() = "
	    + gen.getConfig().keySet().size());
	intoConfig.copyFrom(gen.getConfig(), null);
	if (log.isDebug3())
	  log.debug3(DEBUG_HEADER
	      + "intoConfig.keySet().size() = " + intoConfig.keySet().size());
      }
    }
    if (log.isDebug2()) log.debug2(DEBUG_HEADER + "Done.");
  }

  void setupPlatformConfig(List urls) {
    final String DEBUG_HEADER = "setupPlatformConfig(): ";
    if (log.isDebug2()) log.debug2(DEBUG_HEADER + "urls = " + urls);
    Configuration platConfig = initNewConfiguration();
    mergeAppConfig(platConfig, LockssApp::getBootDefault,
		   "app bootstrap default");
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
    setUpTmp(platConfig);
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
    boolean didLogConfig =
      recordConfigLoaded(newConfig, oldConfig, diffs, gens);
    startCallbacksTime = TimeBase.nowMs();
    runCallbacks(newConfig, oldConfig, diffs);
    // notify other cluster members that the config changed
    // TK should this precede runCallbacks()?
    if (!didLogConfig) {
      logConfig(newConfig, oldConfig, diffs);
    }
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
      enableJmsSend = config.getBoolean(PARAM_ENABLE_JMS_SEND,
					DEFAULT_ENABLE_JMS_SEND);
      enableJmsReceive = config.getBoolean(PARAM_ENABLE_JMS_RECEIVE,
					   DEFAULT_ENABLE_JMS_RECEIVE);
      clientId = config.get(PARAM_JMS_CLIENT_ID, DEFAULT_JMS_CLIENT_ID);
      auInsertCommitCount = config.getInt(PARAM_AU_INSERT_COMMIT_COUNT,
	  				  DEFAULT_AU_INSERT_COMMIT_COUNT);
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

  private boolean recordConfigLoaded(Configuration newConfig,
				     Configuration oldConfig,
				     Configuration.Differences diffs,
				     List gens) {
    buildLoadedFileLists(gens);
    return logConfigLoaded(newConfig, oldConfig, diffs, loadedUrls);
  }



  private boolean logConfigLoaded(Configuration newConfig,
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
      return true;
    } else {
      log.info("New TdbAus: " + diffs.getTdbAuDifferenceCount());
      return false;
    }
  }

  private File ensureDir(File parent, String dirname) {
    File dir = new File(parent, dirname);
    if (FileUtil.ensureDirExists(dir)) {
	FileUtil.setOwnerRWX(dir);
	return dir;
    } else {
      log.warning("Couldn't create dir: " + dir);
      return null;
    }
  }

  private void setUpTmp(Configuration config) {
    // If we were given a temp dir, create a subdir and use that.  This
    // makes it possible to quickly "delete" on restart by renaming, and
    // avoids potentially huge "*" expansion in rundaemon that might't
    // exceed the maximum command length.

    String tmpdir = config.get(PARAM_TMPDIR);
    if (!StringUtil.isNullString(tmpdir)) {
      File javaTmpDir = ensureDir(new File(tmpdir), "dtmp");
      if (javaTmpDir != null) {
	System.setProperty("java.io.tmpdir", javaTmpDir.toString());
	daemonTmpDir = javaTmpDir;
      } else {
	daemonTmpDir = new File(System.getProperty("java.io.tmpdir"));
	log.warning("Using default tmpdir: " + daemonTmpDir);
      }
    }
  }

  File getTmpDir() {
    return daemonTmpDir != null
      ? daemonTmpDir : new File(System.getProperty("java.io.tmpdir"));
  }

  public static final String PARAM_HASH_SVC = LockssApp.MANAGER_PREFIX +
    LockssApp.managerKey(org.lockss.hasher.HashService.class);
  static final String DEFAULT_HASH_SVC = "org.lockss.hasher.HashSvcSchedImpl";

  private void inferMiscParams(Configuration config) {
//     // hack to make hash use new scheduler without directly setting
//     // org.lockss.manager.HashService, which would break old daemons.
//     // don't set if already has a value
//     if (config.get(PARAM_HASH_SVC) == null &&
// 	config.getBoolean(PARAM_NEW_SCHEDULER, DEFAULT_NEW_SCHEDULER)) {
//       config.put(PARAM_HASH_SVC, DEFAULT_HASH_SVC);
//     }

    setUpTmp(config);

    System.setProperty("jsse.enableSNIExtension",
		       Boolean.toString(config.getBoolean(PARAM_JSSE_ENABLESNIEXTENSION,
							  DEFAULT_JSSE_ENABLESNIEXTENSION)));

    setIfNotSet(config,
		org.lockss.crawler.CrawlManagerImpl.PARAM_EXCLUDE_URL_PATTERN,
		MiscParams.PARAM_EXCLUDE_URL_PATTERN);

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

  void mergeAppConfig(Configuration config,
		      Function<LockssApp,Configuration> getter,
		      String msg) {
    if (getApp() != null) {
      Configuration toMerge = getter.apply(getApp());
      if (toMerge != null && !toMerge.isEmpty()) {
	if (log.isDebug2()) log.debug2("Adding " + msg + ": " + toMerge);
	config.copyFrom(toMerge);
      }
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
      if (noNag) {
	log.debug2(PARAM_PLATFORM_DISK_SPACE_LIST +
		   " not specified, not configuring local cache config dir");
      } else {
	log.error(PARAM_PLATFORM_DISK_SPACE_LIST +
		  " not specified, not configuring local cache config dir");
      }
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
    if (log.isDebug2()) {
      log.debug2("processExpertAllowDeny("+expertAllow+", "+expertDeny+")");
    }
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
    installCacheConfigFile(cacheConfigFileName, tempfile,
			   config, suppressReload);
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
    installCacheConfigFile(cacheConfigFileName, tempfile, null, suppressReload);
  }

  private void installCacheConfigFile(String cacheConfigFileName,
				      File tempfile,
				      Configuration config,
				      boolean suppressReload)
      throws IOException {
    File cfile = new File(cacheConfigDir, cacheConfigFileName);
    ConfigFile cf = configCache.find(cfile.toString());
    cf.writeFromTempFile(tempfile, config);

    // local files are included in the cluster file only if they've been
    // loaded, so ensure that it's been loaded.  Really only necessary the
    // first time the file is written, but harmless in general - the file
    // gets (re)loaded earlier than necessary.
    cf.getConfiguration();

    // If this is the first time this file was written, the cluster file
    // may need to be regenerated.  Doing it every time a local file is
    // written is overkill, but negligible extra work
    invalidateClusterFile();

    log.debug2("Wrote cache config file: " + cfile);
    LocalFileDescr descr = getLocalFileDescr(cacheConfigFileName);
    if (!suppressReload) {
      if (descr == null || descr.isNeedReloadAfterWrite()) {
	requestReload();
      }
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

  // Ideally this whole mechanism could be eliminated by making the HTTP
  // cache use a persistent HttpCacheStorage (e.g.,
  // PersistentManagedHttpCacheStorage)

  File remoteConfigFailoverDir;			// Copies written to this dir
  File remoteConfigFailoverInfoFile;		// State file
  RemoteConfigFailoverMap rcfm;
  long remoteConfigFailoverMaxAge = DEFAULT_REMOTE_CONFIG_FAILOVER_MAX_AGE;

  /** Records state of one config failover file */
  static class RemoteConfigFailoverInfo implements LockssSerializable {
    final String url;
    String filename;
    String chksum;
    long storeDate;
    String etag;
    String lastModified;
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

    String getEtag() {
      return etag;
    }

    void setEtag(String etag) {
      this.etag = etag;
    }

    String getLastModified() {
      return lastModified;
    }

    void setLastModified(String lastModified) {
      this.lastModified = lastModified;
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
	storeDate = TimeBase.nowMs();
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
      return storeDate;
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
      log.debug2("Remote failover disabled");
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

  public RemoteConfigFailoverInfo getRemoteConfigFailoverWithTempFile(String url) {
    if (!isRemoteConfigFailoverEnabled()) return null;
    RemoteConfigFailoverInfo rcfi = getRcfi(url);
    if (rcfi == null) {
      return null;
    }
    File tempfile = rcfi.getTempFile();
    if (tempfile != null) {
      log.warning("getRemoteConfigFailoverWithTempFile: temp file already exists for " + url);
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
    return rcfi;
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
   * Provides the daemon bootstrap properties URLs.
   *
   * @return a List of the daemon bootstrap properties URLs.
   */
  public boolean isBootstrapPropsUrl(String url) {
    return bootstrapPropsUrls != null && bootstrapPropsUrls.contains(url);
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
   * Provides the URL of the REST Configuration service.
   *
   * @return URL of the REST Configuration service.
   */
  public String getRestConfigServiceUrl() {
    return restConfigServiceUrl;
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

    if (cacheConfigDir == null) {
      log.warning("Attempting to read cache config file: " +
		  cacheConfigFileName + ", but no cache config dir exists");
      throw new IOException("No cache config dir");
    }

    // This should theoretically lookup the config file thusly, but tests
    // create cache config files without them getting a registered
    // LocalFileDescr.  Do it the old way for now.
    //     LocalFileDescr lfd = getLocalFileDescr(cacheConfigFileName);
    //     if (lfd != null) {
    //       cf = configCache.find(lfd.getFile().toString());
    //     }
    String cfile = new File(cacheConfigDir, cacheConfigFileName).toString();
    cf = configCache.find(cfile);
    if (log.isDebug2()) log.debug2(DEBUG_HEADER + "cf = " + cf);
    return cf;
  }

  /** Create and return an instance of DynamicConfigFile that will generate
   * content determined by the url.  Logic for new types of dynamic config
   * files should be added here. */
  public DynamicConfigFile newDynamicConfigFile(String url) {
    switch (url) {
    case CLUSTER_URL:
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

  void appendClusterUrl(StringBuilder sb, String url) {
    sb.append("      <value>");
    sb.append(StringEscapeUtils.escapeXml(url));
    sb.append("</value>\n");
  }

  void generateClusterFile(File file) throws IOException {
    StringBuilder sbCluster = new StringBuilder();
    if (clusterUrls == null) {
      clusterUrls = Collections.EMPTY_LIST;
    }
    for (String url : clusterUrls) {
      appendClusterUrl(sbCluster, url);
    }

    StringBuilder sbLocal = new StringBuilder();
    if (true || hasLocalCacheConfig()) {
      for (LocalFileDescr lfd : getLocalFileDescrs()) {
	if (!lfd.isIncludeInCluster()) {
	  continue;
	}
	String filename = lfd.getFile().toString();
	ConfigFile cf = configCache.get(filename);
	if (cf != null && cf.isLoaded()) {
	  appendClusterUrl(sbLocal, filename);
	}
      }
    }

    log.debug2("Dyn PreUrls: " + sbCluster.toString());
    log.debug2("Dyn PostUrls: " + sbLocal.toString());
    Map<String,String> valMap =
      MapUtil.map("PreUrls", sbCluster.toString(),
		  "PostUrls", sbLocal.toString());
    try (Writer wrtr = new BufferedWriter(new FileWriter(file))) {
      TemplateUtil.expandTemplate("org/lockss/config/ClusterTemplate.xml",
	  wrtr, valMap);
    }
  }

  /** Cause the cluster file to be regenerated */
  void invalidateClusterFile() {
    ConfigFile cf = configCache.find(CLUSTER_URL);
    if (cf instanceof DynamicConfigFile) {
      log.debug2("Invalidating: " + cf);
      ((DynamicConfigFile)cf).invalidate();
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
	  AuConfiguration auConfiguration = null;

	  try {
	    auConfiguration =
		restConfigClient.getArchivalUnitConfiguration(auId);
	  } catch (LockssRestHttpException lrhe) {
	    // Do nothing: Continue with a null object.
	    log.error("Exception caught getting the configuration of Archival "
		+ "Unit " + auId, lrhe);
	  }

	  if (log.isDebug3())
	    log.debug3(DEBUG_HEADER + "auConfiguration = " + auConfiguration);
	  auConfig =
	      AuConfigurationUtils.toAuidPrefixedConfiguration(auConfiguration);
	  if (log.isDebug3())
	    log.debug3(DEBUG_HEADER + "auConfig = " + auConfig);
	} catch (LockssRestException lre) {
	  log.error("Exception caught getting the configuration of Archival "
	      + "Unit " + auId, lre);
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

  public static final String JMS_PREFIX = MYPREFIX + "jms.";

  /** If true, ConfigManager will send notifications of config-changed
   * events
   */
  public static final String PARAM_ENABLE_JMS_SEND = JMS_PREFIX + "enableSend";
  public static final boolean DEFAULT_ENABLE_JMS_SEND = false;

  /** If true, ConfigManager will register to receive config-changed events
   * (if it's runnning in a client of a config service).
   * @ParamRelevance Rare
   */
  public static final String PARAM_ENABLE_JMS_RECEIVE =
    JMS_PREFIX + "enableReceive";
  public static final boolean DEFAULT_ENABLE_JMS_RECEIVE = true;

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
  private boolean enableJmsSend = DEFAULT_ENABLE_JMS_SEND;
  private boolean enableJmsReceive = DEFAULT_ENABLE_JMS_RECEIVE;
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

  private boolean isJMSManager() {
    try {
      return null != LockssApp.getManagerByTypeStatic(JMSManager.class);
    } catch (IllegalArgumentException | NullPointerException e) {
      return false;
    }
  }

  // Overridable for testing
  protected boolean shouldSendNotifications() {
    return enableJmsSend && !restConfigClient.isActive()
      && isJMSManager();
  }

  protected boolean shouldReceiveNotifications() {
    return enableJmsReceive && restConfigClient.isActive()
      && isJMSManager();
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

  void stopJms() {
    log.debug("stopJms");
    Producer p = jmsProducer;
    if (p != null) {
      try {
	jmsProducer = null;
	p.close();
	log.debug("Closed producer");
      } catch (JMSException e) {
	log.error("Couldn't stop jms producer", e);
      }
    }
    Consumer c = jmsConsumer;
    if (c != null) {
      try {
	jmsConsumer = null;
	c.close();
      } catch (JMSException e) {
	log.error("Couldn't stop jms consumer", e);
      }
    }
  }

  // JMS notification support

  // Notification message is a map:
  // verb - Name of state class (e.g., AuState)
  // auid - if object is per-AU

  // AU create/delete events are handled here, rather than with
  // PluginManager's AuEvent mechanism, because it's the configuration
  // addition / change / deletion that brings the AU into existence /
  // reconfigures it / deletes it that's of interest to other cluster
  // members, not the happenstance creation/deletion of AU instances in
  // this jvm, which can happen for unrelated reasons.
  //
  // Deactivate is treated as a config change ("store") here because that's
  // what it looks like to the DB store code.

  public static final String CONFIG_NOTIFY_VERB = "verb";
  public static final String CONFIG_NOTIFY_AUID = "auid";


  void notifyAuConfigChanged(String auid, AuConfiguration auConfig) {
    if (jmsProducer != null) {
      Map<String,Object> map = new HashMap<>();
      map.put(CONFIG_NOTIFY_VERB, "AuConfigStored");
      map.put(CONFIG_NOTIFY_AUID, auid);
      try {
	jmsProducer.sendMap(map);
      } catch (JMSException e) {
	log.error("Couldn't send AuConfigStored notification", e);
      }
    }
  }

  void notifyAuConfigRemoved(String auid) {
    if (jmsProducer != null) {
      Map<String,Object> map = new HashMap<>();
      map.put(CONFIG_NOTIFY_VERB, "AuConfigRemoved");
      map.put(CONFIG_NOTIFY_AUID, auid);
      try {
	jmsProducer.sendMap(map);
      } catch (JMSException e) {
	log.error("Couldn't send AuConfigRemoved notification", e);
      }
    }
  }

  void notifyConfigChanged() {
    if (jmsProducer != null) {
      Map<String,Object> map = new HashMap<>();
      map.put(CONFIG_NOTIFY_VERB, "GlobalConfigChanged");
      try {
	jmsProducer.sendMap(map);
      } catch (JMSException e) {
	log.error("foo", e);
      }
    }
  }

  /** Incoming JMS message */
  protected void receiveConfigChangedNotification(Map map) {
    log.debug2("Received notification: " + map);
    try {
      String verb = (String)map.get(CONFIG_NOTIFY_VERB);
      String auid = (String)map.get(CONFIG_NOTIFY_AUID);
      switch (verb) {
      case "GlobalConfigChanged":
	// Global config has changed, signal thread to reload it
	requestReload();
	break;
      case "AuConfigStored":
	// Invoke the auConfigChanged() method of all the callbacks
	runCallbacks(new java.util.function.Consumer<Configuration.Callback>() {
	    public void accept(Configuration.Callback cb) {
	      cb.auConfigChanged(auid);
	    }});
	break;
      case "AuConfigRemoved":
	// Invoke the auConfigRemoved() method of all the callbacks
	runCallbacks(new java.util.function.Consumer<Configuration.Callback>() {
	    public void accept(Configuration.Callback cb) {
	      cb.auConfigRemoved(auid);
	    }});
	break;
      default:
	log.warning("Received unknown config changed notification: " + map);
      }
    } catch (ClassCastException e) {
      log.error("Wrong type field in message: " + map, e);
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

  /**
   * Provides a connection to the database.
   *
   * @return a Connection with the connection to the database.
   * @throws DbException
   *           if any problem occurred accessing the database.
   */
  public Connection getConnection() throws DbException {
    return getConfigManagerSql().getConnection();
  }

  /**
   * Provides the configuration manager SQL executor.
   * 
   * @return a ConfigManagerSql with the configuration manager SQL executor.
   * @throws DbException
   *           if any problem occurred accessing the database.
   */
  private ConfigManagerSql getConfigManagerSql() throws DbException {
    if (configManagerSql == null) {
      if (theApp == null) {
	configManagerSql = new ConfigManagerSql(
	    LockssApp.getManagerByTypeStatic(ConfigDbManager.class));
      } else {
	configManagerSql = new ConfigManagerSql(
	    theApp.getManagerByType(ConfigDbManager.class));
      }
    }

    return configManagerSql;
  }

  /**
   * Provides the Archival Unit configurations that involve some plugins.
   * 
   * @param pluginKeys
   *          A Collection<String> with the identifiers of the plugins for which
   *          the Archival Unit configurations are requested.
   * @return a Map<String, List<AuConfiguration>> with all the Archival Unit
   *         configurations, keyed by plugin.
   * @throws DbException
   *           if any problem occurred accessing the database.
   * @throws IOException
   *           if any IO problem occurred.
   * @throws LockssRestException
   *           if any problem occurred accessing the REST service.
   */
  public Map<String, List<AuConfiguration>> retrieveAllPluginsAusConfigurations(
      Collection<String> pluginKeys)
	  throws DbException, IOException, LockssRestException {
    if (log.isDebug2()) log.debug2("pluginKeys = " + pluginKeys);

    Collection<AuConfiguration> auConfigurations =
	retrieveAllArchivalUnitConfiguration();
    if (log.isDebug3()) log.debug3("auConfigurations = " + auConfigurations);

    Map<String, List<AuConfiguration>> result = new HashMap<>();

    for (AuConfiguration auConfiguration : auConfigurations) {
      if (log.isDebug3()) log.debug3("auConfiguration = " + auConfiguration);
      String auId = auConfiguration.getAuId();
      if (log.isDebug3()) log.debug3("auId = " + auId);
      int endPluginLocation = auId.indexOf("&");
      String pluginId = auId.substring(0, endPluginLocation);
      if (log.isDebug3()) log.debug3("pluginId = " + pluginId);

      // Check whether this plugin is of interest.
      if (pluginKeys.contains(pluginId)) {
	// Yes: Get the configurations of the Archival Units already linked to
	// this plugin in the result.
	List<AuConfiguration> pluginAuConfigs = result.get(pluginId);
	if (log.isDebug3()) log.debug3("pluginAuConfigs = " + pluginAuConfigs);

	// Check whether this is this plugin first Archival Unit processed.
	if (pluginAuConfigs == null) {
	  // Yes: Initialize the Archival Unit configuration collection linked
	  // this plugin.
	  pluginAuConfigs = new ArrayList<>();
	  result.put(pluginId, pluginAuConfigs);
	}

	// Add this Archival Unit configuration to the collection.
	pluginAuConfigs.add(auConfiguration);
      }
    }

    if (log.isDebug2()) log.debug2("result.size() = " + result.size());
    return result;
  }

  /**
   * Stores in the database the configuration of an Archival Unit.
   * 
   * @param auConfiguration
   *          An AuConfiguration with the Archival Unit configuration to be
   *          stored.
   * @throws DbException
   *           if any problem occurred accessing the database.
   * @throws LockssRestException
   *           if any problem occurred accessing the REST service.
   */
  public void storeArchivalUnitConfiguration(AuConfiguration auConfiguration)
      throws DbException, LockssRestException {
    if (log.isDebug2()) log.debug2("auConfiguration = " + auConfiguration);

    // Validate the passed argument.
    if (auConfiguration == null) {
      throw new IllegalArgumentException("AuConfig is null");
    }

    // Get and parse the Archival Unit identifier.
    String auid = auConfiguration.getAuId();
    String pluginId = PluginManager.pluginIdFromAuId(auid);
    String auKey = PluginManager.auKeyFromAuId(auid);

    // Get the configuration to be stored.
    Map<String, String> auConfig = auConfiguration.getAuConfig();

    // Validate the configuration.
    if (auConfig == null || auConfig.isEmpty()) {
      throw new IllegalArgumentException("Empty ArchivalUnit configuration");
    }

    if (log.isDebug3()) log.debug3("restConfigClient.isActive() = "
	+ restConfigClient.isActive());

    if (restConfigClient.isActive()) {
      try {
	restConfigClient.putArchivalUnitConfiguration(auConfiguration);
      } catch (LockssRestHttpException lrhe) {
	// Do nothing.
	log.error("Exception caught storing the Archival Unit configuration '"
	    + auConfiguration + "'", lrhe);
      }
    } else {
      getConfigManagerSql().addArchivalUnitConfiguration(pluginId, auKey,
	  auConfig);
      notifyAuConfigChanged(auid, auConfiguration);
    }
  }

  /**
   * Provides all the Archival Unit configurations stored in the database.
   * 
   * @return a Collection<AuConfiguration> with all the Archival Unit
   *         configurations.
   * @throws DbException
   *           if any problem occurred accessing the database.
   * @throws IOException
   *           if any IO problem occurred.
   * @throws LockssRestException
   *           if any problem occurred accessing the REST service.
   */
  public Collection<AuConfiguration> retrieveAllArchivalUnitConfiguration()
      throws DbException, IOException, LockssRestException {
    if (log.isDebug2()) log.debug2("Invoked");

    Collection<AuConfiguration> result = null;

    if (log.isDebug3()) log.debug3("restConfigClient.isActive() = "
	+ restConfigClient.isActive());

    if (restConfigClient.isActive()) {
      try {
	result = restConfigClient.getAllArchivalUnitConfiguration();
      } catch (LockssRestHttpException lrhe) {
	log.error("Exception caught getting the configurations of all Archival "
	    + "Units", lrhe);
	result = Collections.emptyList();
      }
    } else {
      result = new ArrayList<>();

      // Retrieve from the database all the Archival Unit configurations.
      Map<String, Map<String, String>> auConfigurations = getConfigManagerSql().
	  findAllArchivalUnitConfiguration();

      // Loop through all the retrieved Archival Units identifiers.
      for (String auId : auConfigurations.keySet()) {
	if (log.isDebug3()) log.debug3("auId = " + auId);

	// Get the configuration of this Archival Unit.
	Map<String, String> auConfig = auConfigurations.get(auId);
	if (log.isDebug3()) log.debug3("auConfig = " + auConfig);

	// Add it to the result.
	result.add(new AuConfiguration(auId, auConfig));
      }
    }

    if (log.isDebug2()) log.debug2("result.size() = " + result.size());
    return result;
  }

  /**
   * Provides the configuration of an Archival Unit stored in the database.
   * 
   * @param auid
   *          A String with the Archival Unit identifier.
   * @return an AuConfiguration with the Archival Unit configuration.
   * @throws DbException
   *           if any problem occurred accessing the database.
   * @throws LockssRestException
   *           if any problem occurred accessing the REST service.
   */
  public AuConfiguration retrieveArchivalUnitConfiguration(String auid)
      throws DbException, LockssRestException {
    if (log.isDebug2()) log.debug2("auid = " + auid);

    AuConfiguration result = null;

    if (log.isDebug3()) log.debug3("restConfigClient.isActive() = "
	+ restConfigClient.isActive());

    if (restConfigClient.isActive()) {
      try {
	result = restConfigClient.getArchivalUnitConfiguration(auid);
      } catch (LockssRestHttpException lrhe) {
	// Do nothing: Continue with a null object.
	log.error("Exception caught getting the configuration of Archival "
	    + "Unit " + auid, lrhe);
      }
    } else {
      Connection conn = null;

      try {
	// Get a connection to the database.
	conn = getConnection();

	result = retrieveArchivalUnitConfiguration(conn, auid);
      } finally {
	DbManager.safeRollbackAndClose(conn);
      }
    }

    if (log.isDebug2()) log.debug2("result = " + result);
    return result;
  }

  /**
   * Provides the configuration of an Archival Unit stored in the database.
   * 
   * @param conn
   *          A Connection with the database connection to be used.
   * @param auid
   *          A String with the Archival Unit identifier.
   * @return an AuConfiguration with the Archival Unit configuration.
   * @throws DbException
   *           if any problem occurred accessing the database.
   */
  public AuConfiguration retrieveArchivalUnitConfiguration(Connection conn,
      String auid) throws DbException {
    if (log.isDebug2()) log.debug2("auid = " + auid);

    AuConfiguration result = null;

    // Parse the Archival Unit identifier.
    String pluginId = PluginManager.pluginIdFromAuId(auid);
    String auKey = PluginManager.auKeyFromAuId(auid);

    // Retrieve the Archival Unit configuration stored in the database.
    Map<String, String> auConfig = getConfigManagerSql()
	.findArchivalUnitConfiguration(conn, pluginId, auKey);

    // Check whether a configuration was found.
    if (!auConfig.isEmpty()) {
      // Yes.
      result = new AuConfiguration(auid, auConfig);
    }

    if (log.isDebug2()) log.debug2("result = " + result);
    return result;
  }

  /**
   * Provides the creation time of an Archival Unit configuration stored in the
   * database.
   * 
   * @param auid
   *          A String with the Archival Unit identifier.
   * @return a Long with the Archival Unit configuration creation time, as epoch
   *         milliseconds.
   * @throws DbException
   *           if any problem occurred accessing the database.
   */
  public Long retrieveArchivalUnitConfigurationCreationTime(String auid)
      throws DbException {
    if (log.isDebug2()) log.debug2("auid = " + auid);

    // Parse the Archival Unit identifier.
    String pluginId = PluginManager.pluginIdFromAuId(auid);
    String auKey = PluginManager.auKeyFromAuId(auid);

    // Retrieve the Archival Unit configuration creation time stored in the
    // database.
    Long creationTime =
	getConfigManagerSql().findArchivalUnitCreationTime(pluginId, auKey);

    if (log.isDebug2()) log.debug2("creationTime = " + creationTime);
    return creationTime;
  }

  /**
   * Provides the last update time of an Archival Unit configuration stored in
   * the database.
   * 
   * @param auid
   *          A String with the Archival Unit identifier.
   * @return a Long with the Archival Unit configuration last update time, as
   *         epoch milliseconds.
   * @throws DbException
   *           if any problem occurred accessing the database.
   */
  public Long retrieveArchivalUnitConfigurationLastUpdateTime(String auid)
      throws DbException {
    if (log.isDebug2()) log.debug2("auid = " + auid);

    // Parse the Archival Unit identifier.
    String pluginId = PluginManager.pluginIdFromAuId(auid);
    String auKey = PluginManager.auKeyFromAuId(auid);

    // Retrieve the Archival Unit configuration last update time stored in the
    // database.
    Long lastUpdateTime =
	getConfigManagerSql().findArchivalUnitLastUpdateTime(pluginId, auKey);

    if (log.isDebug2()) log.debug2("creationTime = " + lastUpdateTime);
    return lastUpdateTime;
  }


  /**
   * Removes from the database the configuration of an Archival Unit.
   * 
   * @param auid
   *          A String with the Archival Unit identifier.
   * @throws DbException
   *           if any problem occurred accessing the database.
   * @throws LockssRestException
   *           if any problem occurred accessing the REST service.
   */
  public void removeArchivalUnitConfiguration(String auid)
      throws DbException, LockssRestException {
    if (log.isDebug2()) log.debug2("auid = " + auid);

    if (log.isDebug3()) log.debug3("restConfigClient.isActive() = "
	+ restConfigClient.isActive());

    if (restConfigClient.isActive()) {
      try {
	restConfigClient.deleteArchivalUnitConfiguration(auid);
      } catch (LockssRestHttpException lrhe) {
	// Do nothing.
	log.error("Exception caught deleting the configuration of Archival "
	    + "Unit " + auid, lrhe);
      }
    } else {
      // Parse the Archival Unit identifier.
      String pluginId = PluginManager.pluginIdFromAuId(auid);
      String auKey = PluginManager.auKeyFromAuId(auid);

      // Remove the Archival Unit configuration from the database.
      getConfigManagerSql().removeArchivalUnit(pluginId, auKey);

      notifyAuConfigRemoved(auid);
    }

    if (log.isDebug2()) log.debug2("Done");
    return;
  }

  /**
   * Initializes the configuration database
   */
  public void deferredConfigDbInit() {
    // Load the au.txt file into the database, if appropriate.
    if (log.isDebug3()) log.debug3("restConfigClient.isActive() = "
	+ restConfigClient.isActive());

    if (!restConfigClient.isActive()) {
      loadAuTxtFileIntoDb();
    }
  }

  /**
   * Loads the au.txt file into the database, if found.
   */
  public void loadAuTxtFileIntoDb() {
    log.debug2("Invoked");
    if (log.isDebug3()) log.debug3("cacheConfigDir = " + cacheConfigDir);

    // Locate the au.txt file.
    File auTxtFile = new File(cacheConfigDir, CONFIG_FILE_AU_CONFIG);
    if (log.isDebug3()) log.debug3("auTxtFile = " + auTxtFile);

    // Check whether the file exists.
    if (auTxtFile.exists()) {
      // Yes.
      log.info("Loading file " + auTxtFile
	  + " into the AU configuration database");

      Connection conn = null;
      boolean successful = false;
      int addedCount = 0;
      int totalAddedCount = 0;

      try {
        // Get a connection to the database.
        conn = getConfigManagerSql().getConnection();

	// Load the Archival Unit configurations from the file.
	Configuration orgLockssAu = getConfigGeneration(auTxtFile.getPath(),
	    false, true, "cache config", trueKeyPredicate).getConfig()
	    .getConfigTree(PluginManager.PARAM_AU_TREE);

	// Loop through all the plugins found in the file.
	for (Iterator pluginIter = orgLockssAu.nodeIterator();
	    pluginIter.hasNext();) {
	  String pluginKey = (String)pluginIter.next();
	  if (log.isDebug3()) log.debug3("pluginKey = " + pluginKey);

	  // Loop through all the Archival Units found for this plugin.
	  for (Iterator auIter = orgLockssAu.nodeIterator(pluginKey);
	      auIter.hasNext(); ) {
	    String auKey = (String)auIter.next();
	    if (log.isDebug3()) log.debug3("auKey = " + auKey);
	    String auIdKey = pluginKey + "." + auKey;
	    if (log.isDebug3()) log.debug3("auIdKey = " + auIdKey);

	    // Get the configuration properties for this Archival Unit.
	    Configuration auConf = orgLockssAu.getConfigTree(auIdKey);
	    if (log.isDebug3()) log.debug3("auConf = " + auConf);

	    Map<String, String> auConfig = new HashMap<>();

	    // Loop through all the property keys of this Archival Unit.
	    for (String key : auConf.keySet()) {
	      String value = auConf.get(key);
	      if (log.isDebug3())
		log.debug3("key = " + key + ", value = " + value);

	      // Populate the Archival Unit configuration map with this
	      // property.
	      auConfig.put(key, value);
	    }

	    // Write to the database the configuration properties of this
	    // Archival Unit.
	    Long auSeq = getConfigManagerSql().addArchivalUnitConfiguration(
		conn, pluginKey, auKey, auConfig, false);
	    if (log.isDebug3()) log.debug3("auSeq = " + auSeq);

	    // Commit the configurations written since the last commit if the
	    // maximum pending count has been reached.
	    addedCount++;

	    if (addedCount % auInsertCommitCount == 0) {
	      ConfigDbManager.commitOrRollback(conn, log);
	      totalAddedCount += addedCount;
	      addedCount = 0;
	    }
	  }
	}

	// Commit the configurations written since the last commit.
	if (addedCount > 0) {
	  ConfigDbManager.commitOrRollback(conn, log);
	  totalAddedCount += addedCount;
	}

	successful = true;
	log.info("Loaded " + totalAddedCount
	    + " Archival Unit configurations to the database");
      } catch (DbException dbe) {
	log.critical("Error storing contents of file '" + auTxtFile
	    + "' in the database", dbe);
      } catch (IOException ioe) {
	log.critical("Error reading contents of file '" + auTxtFile + "'", ioe);
      } finally {
	DbManager.safeRollbackAndClose(conn);
      }

      //TODO: Record failed AUs?
      // Mark the au.txt file as migrated, to avoid processing it again, if the
      // process loaded all the Archival Units without errors.
      if (successful) {
	boolean renamed = auTxtFile.renameTo(new File(cacheConfigDir,
	    CONFIG_FILE_AU_CONFIG + ".migrated"));
	if (log.isDebug3()) log.debug3("renamed = " + renamed);
      }
      log.debug2("Done");
    }
  }

  /**
   * Writes to an output stream the Archival Unit configuration database.
   * 
   * @param outputStream
   *          An OutputStream where to write the configurations of all the
   *          Archival Units stored in the database.
   * @throws IOException
   *           if there are problems writing to the output stream
   * @throws DbException
   *           if any problem occurred accessing the database.
   */
  public void writeAuConfigurationDatabaseBackupToZip(OutputStream outputStream)
      throws IOException, DbException {
    getConfigManagerSql().processAllArchivalUnitConfigurations(outputStream);
  }

  private class MyMessageListener
    extends Consumer.SubscriptionListener {

    MyMessageListener(String listenerName) {
      super(listenerName);
    }

    @Override
    public void onMessage(Message message) {
      if (log.isDebug3()) log.debug3("onMessage: " + message);
      try {
        Object msgObject = Consumer.convertMessage(message);
	if (msgObject instanceof Map) {
	  receiveConfigChangedNotification((Map)msgObject);
	} else {
	  log.warning("Unknown notification type: " + msgObject);
	}
      } catch (JMSException e) {
	log.warning("Can't decode JMS message", e);
      }
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
	  if (hCacheMgr != null) {
	    hCacheMgr.cleanResources();
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
