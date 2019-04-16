/*

Copyright (c) 2000-2019 Board of Trustees of Leland Stanford Jr. University,
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

package org.lockss.config;

import java.io.*;
import java.net.*;
import java.util.*;
import javax.jms.*;
import org.apache.activemq.broker.BrokerService;
import org.lockss.test.*;
import org.lockss.app.*;
import org.lockss.util.*;
import org.lockss.util.time.Deadline;
import org.lockss.util.time.TimeBase;
import org.lockss.util.time.TimerUtil;
import org.lockss.util.urlconn.*;
import org.lockss.protocol.*;
import org.junit.*;
import org.lockss.clockss.*;
import org.lockss.config.Configuration;
import org.lockss.config.Tdb;
import org.lockss.config.db.ConfigDbManager;
import org.lockss.servlet.*;
import org.lockss.jms.*;
import org.lockss.util.test.FileTestUtil;

import static org.lockss.config.ConfigManager.*;

/**
 * Test class for <code>org.lockss.config.ConfigManager</code>
 */

public class TestConfigManager extends LockssTestCase4 {

  ConfigManager mgr;
  MyConfigManager mymgr;
  static BrokerService broker;
  MockLockssDaemon theDaemon = null;
  ConfigDbManager dbManager = null;

  @Before
  public void setUp() throws Exception {
    super.setUp();
    theDaemon = getMockLockssDaemon();
    mgr = MyConfigManager.makeConfigManager(theDaemon);
    mymgr = (MyConfigManager)mgr;
  }

  @After
  public void tearDown() throws Exception {
    TimeBase.setReal();
    if (dbManager != null) {
      dbManager.stopService();
    }
    super.tearDown();
  }

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    broker = JMSManager.createBroker(JMSManager.DEFAULT_BROKER_URI);
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    if (broker != null) {
      broker.stop();
    }
  }

  static Logger log = Logger.getLogger();

  private static final String c1 = "prop1=12\nprop2=foobar\nprop3=true\n" +
    "prop5=False\n";
  private static final String c1a = "prop2=xxx\nprop4=yyy\n";

  private static final String c2 =
    "timeint=14d\n" +
    "prop.p1=12\n" +
    "prop.p2=foobar\n" +
    "prop.p3.a=true\n" +
    "prop.p3.b=false\n" +
    "otherprop.p3.b=foo\n";

  private ConfigFile loadFCF(String url) throws IOException {
    FileConfigFile cf = new FileConfigFile(url,null);
    cf.reload();
    return cf;
  }

  @Test
  public void testParam() throws IOException, Configuration.InvalidParam {
    Configuration config = ConfigManager.newConfiguration();
    config.load(loadFCF(FileTestUtil.urlOfString(c2)));
    mgr.setCurrentConfig(config);

    assertEquals("12", CurrentConfig.getParam("prop.p1"));
    assertEquals("foobar", CurrentConfig.getParam("prop.p2"));
    assertTrue(CurrentConfig.getBooleanParam("prop.p3.a", false));
    assertEquals(12, CurrentConfig.getIntParam("prop.p1"));
    assertEquals(554, CurrentConfig.getIntParam("propnot.p1", 554));
    assertEquals(2 * Constants.WEEK,
                 CurrentConfig.getTimeIntervalParam("timeint", 554));
    assertEquals(554, CurrentConfig.getTimeIntervalParam("noparam", 554));
  }

  boolean setCurrentConfigFromUrlList(List<String> l) throws IOException {
    Configuration config = mgr.readConfig(l);
    return mgr.installConfig(config);
  }

  boolean setCurrentConfigFromString(String s)
      throws IOException {
    return setCurrentConfigFromUrlList(ListUtil.list(FileTestUtil.urlOfString(s)));
  }

  @Test
  public void testCurrentConfig() throws IOException {
    assertTrue(setCurrentConfigFromUrlList(ListUtil.
					   list(FileTestUtil.urlOfString(c1),
						FileTestUtil.urlOfString(c1a))));
    assertEquals("12", CurrentConfig.getParam("prop1"));
    Configuration config = ConfigManager.getCurrentConfig();
    assertEquals("12", config.get("prop1"));
    assertEquals("12", config.get("prop1", "wrong"));
    assertEquals("xxx", config.get("prop2"));
    assertTrue(config.getBoolean("prop3", false));
    assertEquals("yyy", config.get("prop4"));
    assertEquals("def", config.get("noprop", "def"));
    assertEquals("def", CurrentConfig.getParam("noprop", "def"));
  }

  @Test
  public void testHaveConfig() throws IOException {
    assertFalse(mgr.haveConfig());
    String u1 = FileTestUtil.urlOfString(c1);
    assertTrue(mgr.updateConfig(ListUtil.list(u1)));
    assertTrue(mgr.haveConfig());
  }

  @Test
  public void testNotifyChanged() throws IOException, JMSException {
    Consumer cons =
      Consumer.createTopicConsumer(null, DEFAULT_JMS_NOTIFICATION_TOPIC);
    mymgr.setShouldSendNotifications("yes");
    getMockLockssDaemon().setAppRunning(true);

    assertFalse(mgr.haveConfig());
    String u1 = FileTestUtil.urlOfString(c1);
    assertTrue(mgr.updateConfig(ListUtil.list(u1)));
    assertTrue(mgr.haveConfig());
    assertNull(cons.receiveMap(TIMEOUT_SHOULD));
    ConfigurationUtil.addFromArgs("sdflj", "sldfkj");
    assertEquals(MapUtil.map(ConfigManager.CONFIG_NOTIFY_VERB,
			     "GlobalConfigChanged"),
		 cons.receiveMap(TIMEOUT_SHOULDNT));
  }

  // Verify that receiving various config changed JMS messages triggers
  // config callback
  @Test
  public void testReceiveNotify() throws IOException, JMSException {
    Producer prod =
      Producer.createTopicProducer(null, DEFAULT_JMS_NOTIFICATION_TOPIC);
    mymgr.setShouldReceiveNotifications("yes");
    mymgr.setUpJmsNotifications();

    SimpleQueue notifications = new SimpleQueue.Fifo();

    mymgr.registerConfigurationCallback(new Configuration.Callback() {
	public void configurationChanged(Configuration newConfig,
					 Configuration oldConfig,
					 Configuration.Differences diffs) {
	  notifications.put("ConfigChanged");
	}
	public void auConfigChanged(String auid) {
	  notifications.put("AuChanged");
	}
	public void auConfigRemoved(String auid) {
	  notifications.put("AuRemoved");
	}
      });

    prod.sendMap(MapUtil.map(ConfigManager.CONFIG_NOTIFY_VERB,
			     "GlobalConfigChanged"));

    // Not expecting configurationChanged() to have been called, as it's
    // called by the config reload thread after the config has been
    // reloaded.
    assertNull(notifications.get(TIMEOUT_SHOULD));

    // AuConfigStored and AuConfigRemoved notifications should call
    // callback to be called
    prod.sendMap(MapUtil.map(ConfigManager.CONFIG_NOTIFY_VERB, "AuConfigStored",
			     ConfigManager.CONFIG_NOTIFY_AUID, "AU&IDIDID"));
    assertEquals("AuChanged", notifications.get(TIMEOUT_SHOULDNT));
    prod.sendMap(MapUtil.map(ConfigManager.CONFIG_NOTIFY_VERB, "AuConfigRemoved",
			     ConfigManager.CONFIG_NOTIFY_AUID, "AU&IDIDID"));
    assertEquals("AuRemoved", notifications.get(TIMEOUT_SHOULDNT));
  }

  volatile Configuration.Differences cbDiffs = null;
  List<Configuration> configs;

  @Test
  public void testCallbackWhenRegister() throws IOException {
    configs = new ArrayList<Configuration>();
    setCurrentConfigFromUrlList(ListUtil.list(FileTestUtil.urlOfString(c1),
					      FileTestUtil.urlOfString(c1a)));
    assertEquals(0, configs.size());
    Configuration config = ConfigManager.getCurrentConfig();
    mgr.registerConfigurationCallback(new Configuration.Callback() {
	public void configurationChanged(Configuration newConfig,
					 Configuration oldConfig,
					 Configuration.Differences diffs) {
	  assertNotNull(oldConfig);
	  configs.add(newConfig);
	  cbDiffs = diffs;
	}
      });
    assertEquals(1, configs.size());
    assertEquals(config, configs.get(0));
    assertTrue(cbDiffs.contains("everything"));
  }

  @Test
  public void testCallback() throws IOException {
    Configuration.Callback cb = new Configuration.Callback() {
	public void configurationChanged(Configuration newConfig,
					 Configuration oldConfig,
					 Configuration.Differences diffs) {
	  log.debug("Notify: " + diffs);
	  cbDiffs = diffs;
	}
      };

    setCurrentConfigFromUrlList(ListUtil.list(FileTestUtil.urlOfString(c1),
					      FileTestUtil.urlOfString(c1a)));
    log.debug(ConfigManager.getCurrentConfig().toString());

    mgr.registerConfigurationCallback(cb);
    assertTrue(setCurrentConfigFromUrlList(ListUtil.
					   list(FileTestUtil.urlOfString(c1a),
						FileTestUtil.urlOfString(c1))));
    assertTrue(cbDiffs.contains("prop2"));
    assertFalse(cbDiffs.contains("prop4"));
    assertEquals(SetUtil.set("prop2"), cbDiffs.getDifferenceSet());
    log.debug(ConfigManager.getCurrentConfig().toString());
    assertTrue(setCurrentConfigFromUrlList(ListUtil.
					   list(FileTestUtil.urlOfString(c1),
						FileTestUtil.urlOfString(c1))));
    assertTrue(cbDiffs.contains("prop4"));
    assertFalse(cbDiffs.contains("prop2"));
    assertEquals(SetUtil.set("prop4"), cbDiffs.getDifferenceSet());
    log.debug(ConfigManager.getCurrentConfig().toString());
    cbDiffs = null;
    assertTrue(setCurrentConfigFromUrlList(ListUtil.
					   list(FileTestUtil.urlOfString(c1),
						FileTestUtil.urlOfString(c1a))));
    assertEquals(SetUtil.set("prop4", "prop2"), cbDiffs.getDifferenceSet());
    log.debug(ConfigManager.getCurrentConfig().toString());

    mgr.unregisterConfigurationCallback(cb);
    cbDiffs = null;
    assertTrue(setCurrentConfigFromUrlList(ListUtil.
					   list(FileTestUtil.urlOfString(c1),
						FileTestUtil.urlOfString(c1))));
    assertNull(cbDiffs);

  }

  @Test
  public void testShouldParamBeLogged() {
    assertFalse(mgr.shouldParamBeLogged(PREFIX_TITLE_DB + "foo.xxx"));
    assertFalse(mgr.shouldParamBeLogged("org.lockss.titleSet.foo.xxx"));
    assertFalse(mgr.shouldParamBeLogged(PREFIX_TITLE_SETS_DOT + "foo.xxx"));
    assertFalse(mgr.shouldParamBeLogged("org.lockss.titleSet.foo.xxx"));
    assertFalse(mgr.shouldParamBeLogged("org.lockss.au.foo.xxx"));
    assertFalse(mgr.shouldParamBeLogged("org.lockss.user.1.password"));
    assertFalse(mgr.shouldParamBeLogged("org.lockss.keystore.1.keyPassword"));

    assertTrue(mgr.shouldParamBeLogged("org.lockss.random.param"));
    assertTrue(mgr.shouldParamBeLogged(PARAM_TITLE_DB_URLS));
    assertTrue(mgr.shouldParamBeLogged("org.lockss.titleDbs"));
    assertTrue(mgr.shouldParamBeLogged(PARAM_AUX_PROP_URLS));
  }

  @Test
  public void testListDiffs() throws IOException {
    String xml1 = "<lockss-config>\n" +
      "<property name=\"org.lockss\">\n" +
      " <property name=\"xxx\" value=\"two;four;six\" />" +
      " <property name=\"foo\">" +
      "  <list>" +
      "   <value>fore</value>" +
      "   <value>17</value>" +
      "  </list>" +
      " </property>" +
      "</property>" +
      "</lockss-config>";

    String xml2 = "<lockss-config>\n" +
      "<property name=\"org.lockss\">\n" +
      " <property name=\"xxx\" value=\"two;four;six\" />" +
      " <property name=\"foo\">" +
      "  <list>" +
      "   <value>fore</value>" +
      "   <value>17</value>" +
      "  </list>" +
      " </property>" +
      " <property name=\"extra\" value=\"2\" />" +
      "</property>" +
      "</lockss-config>";

    String xml3 = "<lockss-config>\n" +
      "<property name=\"org.lockss\">\n" +
      " <property name=\"xxx\" value=\"two;six;four\" />" +
      " <property name=\"foo\">" +
      "  <list>" +
      "   <value>fore</value>" +
      "   <value>18</value>" +
      "  </list>" +
      " </property>" +
      " <property name=\"extra\" value=\"2\" />" +
      "</property>" +
      "</lockss-config>";

    mgr.registerConfigurationCallback(new Configuration.Callback() {
	public void configurationChanged(Configuration newConfig,
					 Configuration oldConfig,
					 Configuration.Differences diffs) {
	  log.debug("Notify: " + diffs);
	  cbDiffs = diffs;
	}
      });

    String u1 = FileTestUtil.urlOfString(xml1, ".xml");
    assertFalse(mgr.waitConfig(Deadline.EXPIRED));
    assertTrue(mgr.updateConfig(ListUtil.list(u1)));
    assertTrue(mgr.waitConfig(Deadline.EXPIRED));
    Configuration config = mgr.getCurrentConfig();
    assertEquals(ListUtil.list("fore", "17"), config.getList("org.lockss.foo"));
    assertEquals(ListUtil.list("two", "four", "six"),
		 config.getList("org.lockss.xxx"));
    assertTrue(cbDiffs.contains("org.lockss.foo"));
    assertTrue(cbDiffs.contains("org.lockss.xxx"));

    // change file contents, ensure correct diffs
    File file = new File(new URL(u1).getFile());
    FileTestUtil.writeFile(file, xml2);
    FileConfigFile cf = (FileConfigFile)mgr.getConfigCache().find(u1);
    cf.m_lastModified = "";		// ensure file is reread
    assertTrue(mgr.updateConfig(ListUtil.list(u1)));
    Configuration config2 = mgr.getCurrentConfig();
    assertEquals(ListUtil.list("fore", "17"),
		 config2.getList("org.lockss.foo"));
    assertEquals(ListUtil.list("two", "four", "six"),
		 config2.getList("org.lockss.xxx"));
    assertFalse(cbDiffs.contains("org.lockss.foo"));
    assertFalse(cbDiffs.contains("org.lockss.xxx"));
    assertTrue(cbDiffs.contains("org.lockss.extra"));

    // once more
    FileTestUtil.writeFile(file, xml3);
    cf.m_lastModified = "a";		// ensure file is reread
    assertTrue(mgr.updateConfig(ListUtil.list(u1)));
    Configuration config3 = mgr.getCurrentConfig();
    assertEquals(ListUtil.list("fore", "18"),
		 config3.getList("org.lockss.foo"));
    assertEquals(ListUtil.list("two", "six", "four"),
		 config3.getList("org.lockss.xxx"));
    assertTrue(cbDiffs.contains("org.lockss.foo"));
    assertTrue(cbDiffs.contains("org.lockss.xxx"));
    assertFalse(cbDiffs.contains("org.lockss.extra"));
  }

  @Test
  public void testPlatformProps() throws Exception {
    Properties props = new Properties();
    props.put("org.lockss.platform.localIPAddress", "1.2.3.4");
    props.put("org.lockss.platform.v3.identity", "tcp:[1.2.3.4]:4321");
//     props.put("org.lockss.platform.logdirectory", "/var/log/foo");
//     props.put("org.lockss.platform.logfile", "bar");
    ConfigurationUtil.setCurrentConfigFromProps(props);
    Configuration config = ConfigManager.getCurrentConfig();
    assertEquals("1.2.3.4", config.get("org.lockss.localIPAddress"));
    assertEquals("tcp:[1.2.3.4]:4321",
		 config.get("org.lockss.localV3Identity"));
//     assertEquals(FileUtil.sysDepPath("/var/log/foo/bar"),
// 		 config.get(FileTarget.PARAM_FILE));
  }

  @Test
  public void testPlatformConfig() throws Exception {
    Properties props = new Properties();
    props.put("org.lockss.platform.localIPAddress", "1.2.3.4");
    props.put("org.lockss.platform.v3.identity", "tcp:[1.2.3.4]:4321");
    ConfigurationUtil.setCurrentConfigFromProps(props);
    Configuration config = ConfigManager.getPlatformConfig();
    assertEquals("1.2.3.4", config.get("org.lockss.localIPAddress"));
    assertEquals("tcp:[1.2.3.4]:4321",
		 config.get("org.lockss.localV3Identity"));
  }

  @Test
  public void testPlatformClockss() throws Exception {
    Properties props = new Properties();
    ConfigurationUtil.setCurrentConfigFromProps(props);
    Configuration config = ConfigManager.getCurrentConfig();
    assertNull(config.get(ClockssParams.PARAM_CLOCKSS_SUBSCRIPTION_ADDR));
    assertNull(config.get(ClockssParams.PARAM_INSTITUTION_SUBSCRIPTION_ADDR));
    props.put("org.lockss.platform.localIPAddress", "1.1.1.1");
    ConfigurationUtil.setCurrentConfigFromProps(props);
    config = ConfigManager.getCurrentConfig();
    assertEquals("1.1.1.1",
		 config.get(ClockssParams.PARAM_INSTITUTION_SUBSCRIPTION_ADDR));
    assertNull(config.get(ClockssParams.PARAM_CLOCKSS_SUBSCRIPTION_ADDR));

    props.put("org.lockss.platform.secondIP", "2.2.2.2");
    ConfigurationUtil.setCurrentConfigFromProps(props);
    config = ConfigManager.getCurrentConfig();
    assertEquals("1.1.1.1",
		 config.get(ClockssParams.PARAM_INSTITUTION_SUBSCRIPTION_ADDR));
    assertEquals("2.2.2.2",
		 config.get(ClockssParams.PARAM_CLOCKSS_SUBSCRIPTION_ADDR));
  }

  static final String CLUST_URL = "dyn:cluster.xml";

  String expClust =
    "  <property name=\"org.lockss.auxPropUrls\">\\n" +
    "    <list append=\"false\">\\n" +
    "      <value>http://host/lockss.xml</value>\\n" +
    "      <value>./cluster.txt</value>\\n" +
    "      <value>encode&lt;me&gt;</value>\\n" +
    "\\n" +
    "      <!-- Put static URLs here -->\\n" +
    "\\n" +
    "    </list>\\n" +
    "  </property>\\n";

  String expClust2 =
    "  <property name=\"org.lockss.auxPropUrls\">\\n" +
    "    <list append=\"false\">\\n" +
    "      <value>http://host/lockss.xml</value>\\n" +
    "      <value>./cluster.txt</value>\\n" +
    "      <value>encode&lt;me&gt;</value>\\n" +
    "\\n" +
    "      <!-- Put static URLs here -->\\n" +
    "      <value>.*/config/expert_config\\.txt</value>\\n" +
    "\\n" +
    "    </list>\\n" +
    "  </property>\\n";

  @Test
  public void testGenerateCluster() throws Exception {
    String tmpdir = getTempDir().toString();
    ConfigurationUtil.addFromArgs(ConfigManager.PARAM_PLATFORM_DISK_SPACE_LIST,
				  tmpdir);
    List exp = ListUtil.list("http://host/lockss.xml", "./cluster.txt",
			     "encode<me>");
    mgr.setClusterUrls(exp);
    File tmpFile = getTempFile("configtmp", "");
    mgr.generateClusterFile(tmpFile);
    String clust = StringUtil.fromFile(tmpFile);
    assertMatchesRE(expClust, clust);

    // Store a local cache config file (expert_config.txt), ensure it
    // shows up in the cluster file
    String relConfigPath =
      CurrentConfig.getParam(ConfigManager.PARAM_CONFIG_PATH,
                             ConfigManager.DEFAULT_CONFIG_PATH);
    String exp_file =
      mgr.getCacheConfigFile(ConfigManager.CONFIG_FILE_EXPERT).toString();

    ConfigFile cf = mgr.getConfigCache().find(exp_file);
    mgr.writeCacheConfigFile(PropUtil.fromArgs("k", "v"),
			     ConfigManager.CONFIG_FILE_EXPERT,
			     "Header");
    // Force it to load else won't be included
    Configuration econfig = cf.getConfiguration();
    assertEquals("v", econfig.get("k"));
    mgr.generateClusterFile(tmpFile);
    String clust2 = StringUtil.fromFile(tmpFile);
    assertMatchesRE(expClust2, clust2);
  }

  @Test
  public void testDynCluster() throws Exception {
    String tempDirPath = setUpDiskSpace();
    List exp = ListUtil.list("http://host/lockss.xml", "./cluster.txt",
			     "encode<me>");
    mgr.setClusterUrls(exp);
    ConfigFile cf = mgr.getConfigCache().find(CLUST_URL);
    Configuration config = cf.getConfiguration();
    assertEquals(exp, config.getList("org.lockss.auxPropUrls"));
    String last = cf.getLastModified();
    // ensure file isn't regenerated each time
    assertSame(config, cf.getConfiguration());
    assertEquals(last, cf.getLastModified());

    TimerUtil.guaranteedSleep(1);
    String fname = ConfigManager.CONFIG_FILE_EXPERT;
    mgr.writeCacheConfigFile(PropUtil.fromArgs("exp.foo", "valfoo"),
			     fname, "header");
    Configuration config2 = cf.getConfiguration();
    assertNotSame(config, config2);
    assertNotEquals(last, cf.getLastModified());
    exp.add(new File(tempDirPath, "config/expert_config.txt").toString());
    assertEquals(exp, config2.getList("org.lockss.auxPropUrls"));

  }

  @Test
  public void testDynClusterNone() throws Exception {
    ConfigFile cf = mgr.getConfigCache().find(CLUST_URL);
    Configuration config = cf.getConfiguration();
    assertEmpty(config.getList("org.lockss.auxPropUrls"));
  }

  @Test
  public void testDynClusterInputStream() throws Exception {
    List exp = ListUtil.list("http://host/lockss.xml", "./cluster.txt",
			     "encode<me>");
    mgr.setClusterUrls(exp);
    String clust =
      StringUtil.fromInputStream(mgr.getCacheConfigFileInputStream(CLUST_URL));
    assertMatchesRE(expClust, clust);
  }

  @Test
  public void testInitSocketFactoryNoKeystore() throws Exception {
    Configuration config = mgr.newConfiguration();
    mgr.initSocketFactory(config);
    assertNull(mgr.getSecureSocketFactory());
  }

  @Test
  public void testInitSocketFactoryFilename() throws Exception {
    Configuration config = mgr.newConfiguration();
    config.put(PARAM_SERVER_AUTH_KEYSTORE_NAME, "/path/to/keystore");
    mgr.initSocketFactory(config);
    String pref = "org.lockss.keyMgr.keystore.propserver.";
    assertEquals("propserver", config.get(pref + "name"));
    assertEquals("/path/to/keystore", config.get(pref + "file"));
    LockssSecureSocketFactory fact = mgr.getSecureSocketFactory();
    assertEquals("propserver", fact.getServerAuthKeystoreName());
    assertNull(fact.getClientAuthKeystoreName());
  }

  @Test
  public void testInitSocketFactoryInternalKeystore() throws Exception {
    Configuration config = mgr.newConfiguration();
    config.put(PARAM_SERVER_AUTH_KEYSTORE_NAME, "lockss-ca");
    mgr.initSocketFactory(config);
    String pref = "org.lockss.keyMgr.keystore.lockssca.";
    assertEquals("lockss-ca", config.get(pref + "name"));
    assertEquals("org/lockss/config/lockss-ca.keystore",
		 config.get(pref + "resource"));
    LockssSecureSocketFactory fact = mgr.getSecureSocketFactory();
    assertEquals("lockss-ca", fact.getServerAuthKeystoreName());
    assertNull(fact.getClientAuthKeystoreName());
  }

  @Test
  public void testInitNewConfiguration() throws Exception {
    mgr =  new ConfigManager(ListUtil.list("foo"), "group1;GROUP2");
    Configuration config = mgr.initNewConfiguration();
    assertEquals("group1;group2", config.getPlatformGroups());
    assertEquals(ListUtil.list("group1", "group2"),
		 config.getPlatformGroupList());
  }

  @Test
  public void testGroup() throws Exception {
    Properties props = new Properties();
    ConfigurationUtil.setCurrentConfigFromProps(props);
    assertEquals("nogroup", ConfigManager.getPlatformGroups());
    props.put(ConfigManager.PARAM_DAEMON_GROUPS, "foog");
    ConfigurationUtil.setCurrentConfigFromProps(props);
    assertEquals("foog", ConfigManager.getPlatformGroups());
    assertEquals(ListUtil.list("foog"), ConfigManager.getPlatformGroupList());
    props.put(ConfigManager.PARAM_DAEMON_GROUPS, "foog;barg");
    ConfigurationUtil.setCurrentConfigFromProps(props);
    assertEquals("foog;barg", ConfigManager.getPlatformGroups());
    assertEquals(ListUtil.list("foog", "barg"),
		 ConfigManager.getPlatformGroupList());
  }

  // platform access not set, ui and proxy access not set
  @Test
  public void testPlatformAccess0() throws Exception {
    Properties props = new Properties();
    ConfigurationUtil.setCurrentConfigFromProps(props);
    Configuration config = ConfigManager.getCurrentConfig();
    assertFalse(config.containsKey("org.lockss.ui.access.ip.include"));
    assertFalse(config.containsKey("org.lockss.proxy.access.ip.include"));
  }

  // platform access not set, ui and proxy access set
  @Test
  public void testPlatformAccess1() throws Exception {
    Properties props = new Properties();
    props.put("org.lockss.ui.access.ip.include", "1.2.3.0/22");
    props.put("org.lockss.proxy.access.ip.include", "1.2.3.0/21");
    ConfigurationUtil.setCurrentConfigFromProps(props);
    Configuration config = ConfigManager.getCurrentConfig();
    assertEquals("1.2.3.0/22", config.get("org.lockss.ui.access.ip.include"));
    assertEquals("1.2.3.0/21",
		 config.get("org.lockss.proxy.access.ip.include"));
  }

  // platform access set, ui and proxy access not set
  @Test
  public void testPlatformAccess2() throws Exception {
    Properties props = new Properties();
    props.put("org.lockss.platform.accesssubnet", "1.2.3.*");
    ConfigurationUtil.setCurrentConfigFromProps(props);
    Configuration config = ConfigManager.getCurrentConfig();
    assertEquals("1.2.3.*", config.get("org.lockss.ui.access.ip.include"));
    assertEquals("1.2.3.*", config.get("org.lockss.proxy.access.ip.include"));
  }

  // platform access set, ui and proxy access set globally, not locally
  @Test
  public void testPlatformAccess3() throws Exception {
    Properties props = new Properties();
    props.put("org.lockss.platform.accesssubnet", "1.2.3.*");
    props.put("org.lockss.ui.access.ip.include", "1.2.3.0/22");
    props.put("org.lockss.proxy.access.ip.include", "1.2.3.0/21");
    ConfigurationUtil.setCurrentConfigFromProps(props);
    Configuration config = ConfigManager.getCurrentConfig();
    assertEquals("1.2.3.*;1.2.3.0/22",
		 config.get("org.lockss.ui.access.ip.include"));
    assertEquals("1.2.3.*;1.2.3.0/21",
		 config.get("org.lockss.proxy.access.ip.include"));
  }

  // platform access set, ui and proxy access set locally
  @Test
  public void testPlatformAccess4() throws Exception {
    Properties props = new Properties();
    props.put("org.lockss.platform.accesssubnet", "1.2.3.*");
    props.put("org.lockss.ui.access.ip.include", "1.2.3.0/22");
    props.put("org.lockss.ui.access.ip.platformAccess", "1.2.3.*");
    props.put("org.lockss.proxy.access.ip.include", "1.2.3.0/21");
    props.put("org.lockss.proxy.access.ip.platformAccess", "3.2.1.0/22");
    ConfigurationUtil.setCurrentConfigFromProps(props);
    Configuration config = ConfigManager.getCurrentConfig();
    assertEquals("1.2.3.0/22",
		 config.get("org.lockss.ui.access.ip.include"));
    assertEquals("1.2.3.*;1.2.3.0/21",
		 config.get("org.lockss.proxy.access.ip.include"));
  }

  // platform access set, ui and proxy access set locally
  @Test
  public void testPlatformAccess5() throws Exception {
    Properties props = new Properties();
    props.put("org.lockss.platform.accesssubnet", "1.2.3.*;4.4.4.0/24");
    props.put("org.lockss.ui.access.ip.include", "1.2.3.0/22;1.2.3.*");
    props.put("org.lockss.proxy.access.ip.include", "1.2.3.0/21;5.5.0.0/18");
    ConfigurationUtil.setCurrentConfigFromProps(props);
    Configuration config = ConfigManager.getCurrentConfig();
    assertEquals("1.2.3.*;4.4.4.0/24;1.2.3.0/22",
		 config.get("org.lockss.ui.access.ip.include"));
    assertEquals("1.2.3.*;4.4.4.0/24;1.2.3.0/21;5.5.0.0/18",
		 config.get("org.lockss.proxy.access.ip.include"));
  }

  @Test
  public void testPlatformSpace2() throws Exception {
    String tmpdir1 = getTempDir().toString();
    String tmpdir2 = getTempDir().toString();
    Properties props = new Properties();
    props.put("org.lockss.platform.diskSpacePaths",
	      StringUtil.separatedString(ListUtil.list(tmpdir1, tmpdir2)
					 , ";"));
    ConfigurationUtil.setCurrentConfigFromProps(props);
    Configuration config = ConfigManager.getCurrentConfig();
    assertEquals(FileUtil.sysDepPath(new File(tmpdir1, "iddb").toString()),
		 config.get("org.lockss.id.database.dir"));
    assertEquals(tmpdir1 + "/tfile",
		 config.get(org.lockss.truezip.TrueZipManager.PARAM_CACHE_DIR));
  }

  @Test
  public void testPlatformSmtp() throws Exception {
    Properties props = new Properties();
    props.put("org.lockss.platform.smtphost", "smtp.example.com");
    props.put("org.lockss.platform.smtpport", "25");
    ConfigurationUtil.setCurrentConfigFromProps(props);
    Configuration config = ConfigManager.getCurrentConfig();
    assertEquals("smtp.example.com",
		 config.get("org.lockss.mail.smtphost"));
    assertEquals("25",
		 config.get("org.lockss.mail.smtpport"));
  }

  @Test
  public void testPlatformVersionConfig() throws Exception {
    Properties props = new Properties();
    props.put(ConfigManager.PARAM_PLATFORM_VERSION, "123");
    props.put("org.lockss.foo", "44");
    props.put("123.org.lockss.foo", "55");
    ConfigurationUtil.setCurrentConfigFromProps(props);
    Configuration config = ConfigManager.getCurrentConfig();
    assertEquals("55", config.get("org.lockss.foo"));
  }

  @Test
  public void testPlatformDifferentVersionConfig() throws Exception {
    Properties props = new Properties();
    props.put(ConfigManager.PARAM_PLATFORM_VERSION, "321");
    props.put("org.lockss.foo", "22");
    props.put("123.org.lockss.foo", "55");
    ConfigurationUtil.setCurrentConfigFromProps(props);
    Configuration config = ConfigManager.getCurrentConfig();
    assertEquals("22", config.get("org.lockss.foo"));
  }

  @Test
  public void testPlatformNoVersionConfig() throws Exception {
    Properties props = new Properties();
    props.put("org.lockss.foo", "11");
    props.put("123.org.lockss.foo", "55");
    ConfigurationUtil.setCurrentConfigFromProps(props);
    Configuration config = ConfigManager.getCurrentConfig();
    assertEquals("11", config.get("org.lockss.foo"));
  }

  @Test
  public void testFindRelDataDirNoDisks() throws Exception {
    ConfigurationUtil.addFromArgs(ConfigManager.PARAM_PLATFORM_DISK_SPACE_LIST,
				  "");
    try {
      mgr.findRelDataDir("rel1", true);
      fail("findRelDataDir() should throw when " +
	   ConfigManager.PARAM_PLATFORM_DISK_SPACE_LIST + " not set");
    } catch (RuntimeException e) {
    }
  }

  @Test
  public void testFindRelDataDir1New() throws Exception {
    String tmpdir = getTempDir().toString();
    ConfigurationUtil.addFromArgs(ConfigManager.PARAM_PLATFORM_DISK_SPACE_LIST,
				  tmpdir);
    File exp = new File(tmpdir, "rel1");
    assertFalse(exp.exists());
    File pdir = mgr.findRelDataDir("rel1", true);
    assertEquals(exp, pdir);
    assertTrue(exp.exists());
  }

  @Test
  public void testFindRelDataDir1Old() throws Exception {
    String tmpdir = getTempDir().toString();
    ConfigurationUtil.addFromArgs(ConfigManager.PARAM_PLATFORM_DISK_SPACE_LIST,
				  tmpdir);
    File exp = new File(tmpdir, "rel2");
    assertFalse(exp.exists());
    assertTrue(FileUtil.ensureDirExists(exp));
    File pdir = mgr.findRelDataDir("rel2", true);
    assertEquals(exp, pdir);
    assertTrue(exp.exists());
  }

  @Test
  public void testFindRelDataDirNNew() throws Exception {
    String tmpdir1 = getTempDir().toString();
    String tmpdir2 = getTempDir().toString();
    List<String> both = ListUtil.list(tmpdir2, tmpdir1);
    assertNotEquals(tmpdir1, tmpdir2);
    ConfigurationUtil.addFromArgs(ConfigManager.PARAM_PLATFORM_DISK_SPACE_LIST,
				  StringUtil.separatedString(both, ";"));
    assertEquals(both, mgr.getCurrentConfig().getList(ConfigManager.PARAM_PLATFORM_DISK_SPACE_LIST));
    File exp1 = new File(tmpdir1, "rel3");
    File exp2 = new File(tmpdir2, "rel3");
    assertFalse(exp1.exists());
    assertFalse(exp2.exists());
    File pdir = mgr.findRelDataDir("rel3", true);
    assertEquals(exp2, pdir);
    assertTrue(exp2.exists());
    assertFalse(exp1.exists());
  }

  @Test
  public void testFindRelDataDirNOld() throws Exception {
    String tmpdir1 = getTempDir().toString();
    String tmpdir2 = getTempDir().toString();
    List<String> both = ListUtil.list(tmpdir1, tmpdir2);
    assertNotEquals(tmpdir1, tmpdir2);
    ConfigurationUtil.addFromArgs(ConfigManager.PARAM_PLATFORM_DISK_SPACE_LIST,
				  StringUtil.separatedString(both, ";"));
    assertEquals(both, mgr.getCurrentConfig().getList(ConfigManager.PARAM_PLATFORM_DISK_SPACE_LIST));
    File exp1 = new File(tmpdir1, "rel4");
    File exp2 = new File(tmpdir2, "rel4");
    assertFalse(exp1.exists());
    assertFalse(exp2.exists());
    assertTrue(FileUtil.ensureDirExists(exp2));
    File pdir = mgr.findRelDataDir("rel4", true);
    assertEquals(exp2, pdir);
    assertTrue(exp2.exists());
    assertFalse(exp1.exists());
  }

  @Test
  public void testFindConfiguredDataDirAbsNew() throws Exception {
    String tmpdir = getTempDir().toString();
    File exp = new File(tmpdir, "rel1");
    assertTrue(exp.isAbsolute());
    String param = "o.l.param7";
    ConfigurationUtil.addFromArgs(param, exp.toString());
    assertFalse(exp.exists());
    File pdir = mgr.findConfiguredDataDir(param, "/illegal.abs.path");
    assertEquals(exp, pdir);
    assertTrue(exp.exists());

    File exp2 = new File(tmpdir, "other");
    assertTrue(exp.isAbsolute());
    assertFalse(exp2.exists());
    File pdir2 = mgr.findConfiguredDataDir("unset.param", exp2.toString());
    assertEquals(exp2, pdir2);
    assertTrue(exp2.exists());
  }

  @Test
  public void testFindConfiguredDataDirAbsOld() throws Exception {
    String tmpdir = getTempDir().toString();
    File exp = new File(tmpdir, "rel1");
    assertTrue(exp.isAbsolute());
    String param = "o.l.param7";
    ConfigurationUtil.addFromArgs(param, exp.toString());
    assertFalse(exp.exists());
    assertTrue(FileUtil.ensureDirExists(exp));
    assertTrue(exp.exists());
    File pdir = mgr.findConfiguredDataDir(param, "/illegal.abs.path");
    assertEquals(exp, pdir);
    assertTrue(exp.exists());

    File exp2 = new File(tmpdir, "other");
    assertTrue(exp.isAbsolute());
    assertFalse(exp2.exists());
    assertTrue(FileUtil.ensureDirExists(exp2));
    assertTrue(exp2.exists());
    File pdir2 = mgr.findConfiguredDataDir("unset.param", exp2.toString());
    assertEquals(exp2, pdir2);
    assertTrue(exp2.exists());
  }

  @Test
  public void testFindConfiguredDataDirRelNew() throws Exception {
    String tmpdir1 = getTempDir().toString();
    String tmpdir2 = getTempDir().toString();
    List<String> both = ListUtil.list(tmpdir1, tmpdir2);
    assertNotEquals(tmpdir1, tmpdir2);
    String param = "o.l.param9";
    ConfigurationUtil.addFromArgs(ConfigManager.PARAM_PLATFORM_DISK_SPACE_LIST,
				  StringUtil.separatedString(both, ";"),
				  param, "rel5");
    File exp1 = new File(tmpdir1, "rel5");
    File exp2 = new File(tmpdir2, "rel5");
    assertFalse(exp1.exists());
    assertFalse(exp2.exists());
    File pdir = mgr.findConfiguredDataDir(param, "other");
    assertEquals(exp1, pdir);
    assertTrue(exp1.exists());

    File exp3 = new File(tmpdir1, "other");
    assertFalse(exp3.exists());
    File pdir3 = mgr.findConfiguredDataDir("unset.param", exp3.toString());
    assertEquals(exp3, pdir3);
    assertTrue(exp3.exists());
  }

  @Test
  public void testFindConfiguredDataDirRelOld() throws Exception {
    String tmpdir1 = getTempDir().toString();
    String tmpdir2 = getTempDir().toString();
    List<String> both = ListUtil.list(tmpdir1, tmpdir2);
    assertNotEquals(tmpdir1, tmpdir2);
    String param = "o.l.param9";
    ConfigurationUtil.addFromArgs(ConfigManager.PARAM_PLATFORM_DISK_SPACE_LIST,
				  StringUtil.separatedString(both, ";"),
				  param, "rel5");
    File exp1 = new File(tmpdir1, "rel5");
    File exp2 = new File(tmpdir2, "rel5");
    assertFalse(exp1.exists());
    assertFalse(exp2.exists());
    assertTrue(FileUtil.ensureDirExists(exp2));
    assertTrue(exp2.exists());
    File pdir = mgr.findConfiguredDataDir(param, "other");
    assertEquals(exp2, pdir);
    assertTrue(exp2.exists());
    assertFalse(exp1.exists());
  }

  @Test
  public void testPlatformConfigDirSetup() throws Exception {
    String tmpdir = getTempDir().toString();
    Properties props = new Properties();
    props.put(ConfigManager.PARAM_PLATFORM_DISK_SPACE_LIST, tmpdir);
    ConfigurationUtil.setCurrentConfigFromProps(props);
    String relConfigPath =
      CurrentConfig.getParam(ConfigManager.PARAM_CONFIG_PATH,
                             ConfigManager.DEFAULT_CONFIG_PATH);
    File cdir = new File(tmpdir, relConfigPath);
    assertTrue(cdir.exists());
    Configuration config = CurrentConfig.getCurrentConfig();
  }

  @Test
  public void testGetVersionString() throws Exception {
    Properties props = new Properties();
    props.put(ConfigManager.PARAM_PLATFORM_VERSION, "321");
    props.put(ConfigManager.PARAM_DAEMON_VERSION, "1.44.2");
    ConfigurationUtil.setCurrentConfigFromProps(props);
    List pairs = StringUtil.breakAt(mgr.getVersionString(), ',');
    String release = BuildInfo.getBuildProperty(BuildInfo.BUILD_RELEASENAME);
    if (release != null) {
      assertEquals(SetUtil.set("groups=nogroup",
			       "platform=OpenBSD CD 321",
			       "daemon=" + release),
		   SetUtil.theSet(pairs));
    } else {
      assertEquals(SetUtil.set("groups=nogroup",
			       "platform=OpenBSD CD 321",
			       "daemon=1.44.2"),
		   SetUtil.theSet(pairs));
    }
    mgr.setGroups("grouper");
    ConfigurationUtil.addFromArgs(IdentityManager.PARAM_LOCAL_V3_IDENTITY,
				  "tcp:[111.32.14.5]:9876");
    pairs = StringUtil.breakAt(mgr.getVersionString(), ',');
    if (release != null) {
      assertEquals(SetUtil.set("groups=grouper",
			       "peerid=tcp:[111.32.14.5]:9876",
			       "platform=OpenBSD CD 321",
			       "daemon=" + release),
		   SetUtil.theSet(pairs));
    } else {
      assertEquals(SetUtil.set("groups=grouper",
			       "peerid=tcp:[111.32.14.5]:9876",
			       "platform=OpenBSD CD 321",
			       "daemon=1.44.2"),
		   SetUtil.theSet(pairs));
    }
  }

  @Test
  public void testMiscTmpdir() throws Exception {
    ConfigurationUtil.setFromArgs(ConfigManager.PARAM_TMPDIR, "/tmp/unlikely");
    assertEquals("/tmp/unlikely/dtmp", System.getProperty("java.io.tmpdir"));
  }

  @Test
  public void testGetHttpCacheManager() throws Exception {
    HttpCacheManager hcm = mgr.getHttpCacheManager();
    ClientCacheSpec ccs = hcm.getCacheSpec(ConfigManager.HTTP_CACHE_NAME);
    assertMatchesRE(".+/hcfcache$", ccs.getCacheDir().toString());
  }

  @Test
  public void testConfigVersionProp() {
    assertEquals("org.lockss.config.fileVersion.foo",
		 ConfigManager.configVersionProp("foo"));
  }

  @Test
  public void testCompatibilityParams() {
    Configuration config = ConfigManager.getCurrentConfig();
    assertEquals(null, config.get(AdminServletManager.PARAM_CONTACT_ADDR));
    assertEquals(null, config.get(AdminServletManager.PARAM_HELP_URL));
    ConfigurationUtil.setFromArgs(ConfigManager.PARAM_OBS_ADMIN_CONTACT_EMAIL,
				  "Nicola@teslasociety.org",
				  ConfigManager.PARAM_OBS_ADMIN_HELP_URL,
				  "help://cause.I.need.somebody/");
    config = ConfigManager.getCurrentConfig();
    assertEquals("Nicola@teslasociety.org",
		 config.get(AdminServletManager.PARAM_CONTACT_ADDR));
    assertEquals("help://cause.I.need.somebody/",
		 config.get(AdminServletManager.PARAM_HELP_URL));
  }

  @Test
  public void testWriteAndReadCacheConfigFile() throws Exception {
    String fname = "test-config";
    String tmpdir = getTempDir().toString();
    Properties props = new Properties();
    props.put(ConfigManager.PARAM_PLATFORM_DISK_SPACE_LIST, tmpdir);
    ConfigurationUtil.setCurrentConfigFromProps(props);
    String relConfigPath =
      CurrentConfig.getParam(ConfigManager.PARAM_CONFIG_PATH,
                             ConfigManager.DEFAULT_CONFIG_PATH);
    File cdir = new File(tmpdir, relConfigPath);
    assertTrue(cdir.exists());
    Properties acprops = new Properties();
    acprops.put("foo.bar" , "12345");
    mgr.writeCacheConfigFile(acprops, fname, "this is a header");

    File acfile = new File(cdir, fname);
    assertTrue(acfile.exists());

    Configuration config2 = mgr.readCacheConfigFile(fname);
    assertEquals("12345", config2.get("foo.bar"));
    assertEquals("1", config2.get("org.lockss.config.fileVersion." + fname));
    assertEquals("wrong number of keys in written config file",
		 2, config2.keySet().size());
  }

  @Test
  public void testWriteStringAndReadCacheConfigFile() throws Exception {
    String fname = "test-config";
    String tmpdir = getTempDir().toString();
    Properties props = new Properties();
    props.put(ConfigManager.PARAM_PLATFORM_DISK_SPACE_LIST, tmpdir);
    ConfigurationUtil.setCurrentConfigFromProps(props);
    String relConfigPath =
      CurrentConfig.getParam(ConfigManager.PARAM_CONFIG_PATH,
                             ConfigManager.DEFAULT_CONFIG_PATH);
    File cdir = new File(tmpdir, relConfigPath);
    assertTrue(cdir.exists());
    mgr.writeCacheConfigFile("foo.bar=12345", fname, false);

    File acfile = new File(cdir, fname);
    assertTrue(acfile.exists());

    Configuration config2 = mgr.readCacheConfigFile(fname);
    assertEquals("12345", config2.get("foo.bar"));
    assertEquals("wrong number of keys in written config file",
		 1, config2.keySet().size());
    mgr.writeCacheConfigFile("foo1.bar1=23456\nx.y=34", fname, false);

    config2 = mgr.readCacheConfigFile(fname);
    assertEquals("23456", config2.get("foo1.bar1"));
  }

  @Test
  public void testCacheConfigFile() throws Exception {
    String tmpdir = getTempDir().toString();
    ConfigurationUtil.setFromArgs(ConfigManager.PARAM_PLATFORM_DISK_SPACE_LIST,
				  tmpdir);
    String relConfigPath =
      CurrentConfig.getParam(ConfigManager.PARAM_CONFIG_PATH,
                             ConfigManager.DEFAULT_CONFIG_PATH);
    File cdir = new File(tmpdir, relConfigPath);
    assertTrue(cdir.exists());
    Properties acprops = new Properties();
    acprops.put("foo.bar" , "12345");
    mgr.writeCacheConfigFile(acprops, ConfigManager.CONFIG_FILE_UI_IP_ACCESS,
			     "this is a header");

    Configuration config = ConfigManager.getCurrentConfig();
    assertNull(config.get("foo.bar"));
    assertTrue(mgr.updateConfig());
    Configuration config2 = ConfigManager.getCurrentConfig();
    assertEquals("12345", config2.get("foo.bar"));
  }

  @Test
  public void testExpertConfigFile() throws Exception {
    String tmpdir = getTempDir().toString();
    File pfile = new File(tmpdir, "props.txt");
    Properties pprops = new Properties();
    pprops.put(ConfigManager.PARAM_PLATFORM_DISK_SPACE_LIST, tmpdir);
    PropUtil.toFile(pfile, pprops);

    String relConfigPath =
      CurrentConfig.getParam(ConfigManager.PARAM_CONFIG_PATH,
                             ConfigManager.DEFAULT_CONFIG_PATH);
    File cdir = new File(tmpdir, relConfigPath);
    cdir.mkdirs();
    File efile = new File(cdir, ConfigManager.CONFIG_FILE_EXPERT);

    assertTrue(cdir.exists());
    String k1 = "org.lockss.foo";
    String k2 = "org.lockss.user.1.password";
    String k3 = "org.lockss.keyMgr.keystore.foo.keyPassword";

    Properties eprops = new Properties();
    eprops.put(k1, "12345");
    eprops.put(k2, "ignore");
    eprops.put(k3 , "ignore2");
    PropUtil.toFile(efile, eprops);

    Configuration config = ConfigManager.getCurrentConfig();
    assertNull(config.get(k1));
    assertNull(config.get(k2));
    assertNull(config.get(k3));
    assertTrue(mgr.updateConfig(ListUtil.list(pfile)));
    Configuration config2 = ConfigManager.getCurrentConfig();
    assertEquals("12345", config2.get(k1));
    assertNull(config2.get(k2));
    assertNull(config2.get(k3));
  }

  @Test
  public void testExpertConfigFileDeny() throws Exception {
    String tmpdir = getTempDir().toString();
    File pfile = new File(tmpdir, "props.txt");
    Properties pprops = new Properties();
    pprops.put(ConfigManager.PARAM_PLATFORM_DISK_SPACE_LIST, tmpdir);
    pprops.put(ConfigManager.PARAM_EXPERT_DENY,
	       "foo;bar;^org\\.lockss\\.platform\\.");
    PropUtil.toFile(pfile, pprops);

    String relConfigPath =
      CurrentConfig.getParam(ConfigManager.PARAM_CONFIG_PATH,
                             ConfigManager.DEFAULT_CONFIG_PATH);
    File cdir = new File(tmpdir, relConfigPath);
    cdir.mkdirs();
    File efile = new File(cdir, ConfigManager.CONFIG_FILE_EXPERT);

    assertTrue(cdir.exists());
    String k1 = "org.lockss.foo";
    String k2 = "org.lockss.user.1.password";
    String k3 = "org.lockss.keyMgr.keystore.foo.keyPassword";
    String k4 = "org.lockss.platform.bar";

    Properties eprops = new Properties();
    eprops.put(k1, "12345");
    eprops.put(k2, "v2");
    eprops.put(k3, "v3");
    eprops.put(k4, "v4");
    PropUtil.toFile(efile, eprops);

    Configuration config = ConfigManager.getCurrentConfig();
    assertNull(config.get(k1));
    assertNull(config.get(k2));
    assertNull(config.get(k3));
    assertNull(config.get(k4));
    assertTrue(mgr.updateConfig(ListUtil.list(pfile)));
    Configuration config2 = ConfigManager.getCurrentConfig();
    assertNull(config2.get(k1));
    assertEquals("v2", config2.get(k2));
    assertNull(config2.get(k3));
    assertNull(config2.get(k4));
  }

  @Test
  public void testExpertConfigFileAllow() throws Exception {
    String tmpdir = getTempDir().toString();
    File pfile = new File(tmpdir, "props.txt");
    Properties pprops = new Properties();
    pprops.put(ConfigManager.PARAM_PLATFORM_DISK_SPACE_LIST, tmpdir);
    pprops.put(ConfigManager.PARAM_EXPERT_DENY, "");
    pprops.put(ConfigManager.PARAM_EXPERT_ALLOW, "foo");
    PropUtil.toFile(pfile, pprops);

    String relConfigPath =
      CurrentConfig.getParam(ConfigManager.PARAM_CONFIG_PATH,
                             ConfigManager.DEFAULT_CONFIG_PATH);
    File cdir = new File(tmpdir, relConfigPath);
    cdir.mkdirs();
    File efile = new File(cdir, ConfigManager.CONFIG_FILE_EXPERT);

    assertTrue(cdir.exists());
    String k1 = "org.lockss.foo";
    String k2 = "org.lockss.user.1.password";
    String k3 = "org.lockss.keyMgr.keystore.foo.keyPassword";

    Properties eprops = new Properties();
    eprops.put(k1, "12345");
    eprops.put(k2, "v2");
    eprops.put(k3, "v3");
    PropUtil.toFile(efile, eprops);

    Configuration config = ConfigManager.getCurrentConfig();
    assertNull(config.get(k1));
    assertNull(config.get(k2));
    assertNull(config.get(k3));
    assertTrue(mgr.updateConfig(ListUtil.list(pfile)));
    Configuration config2 = ConfigManager.getCurrentConfig();
    assertEquals("12345", config2.get(k1));
    assertNull(config2.get(k2));
    assertEquals("v3", config2.get(k3));
  }

  @Test
  public void testExpertConfigFileBoth() throws Exception {
    String tmpdir = getTempDir().toString();
    File pfile = new File(tmpdir, "props.txt");
    Properties pprops = new Properties();
    pprops.put(ConfigManager.PARAM_PLATFORM_DISK_SPACE_LIST, tmpdir);
    pprops.put(ConfigManager.PARAM_EXPERT_DENY,
	       "foo;bar;^org\\.lockss\\.platform\\.");
    pprops.put(ConfigManager.PARAM_EXPERT_ALLOW, "foo");
    PropUtil.toFile(pfile, pprops);

    String relConfigPath =
      CurrentConfig.getParam(ConfigManager.PARAM_CONFIG_PATH,
                             ConfigManager.DEFAULT_CONFIG_PATH);
    File cdir = new File(tmpdir, relConfigPath);
    cdir.mkdirs();
    File efile = new File(cdir, ConfigManager.CONFIG_FILE_EXPERT);

    assertTrue(cdir.exists());
    String k1 = "org.lockss.foo";
    String k2 = "org.lockss.user.1.password";
    String k3 = "org.lockss.keyMgr.keystore.foo.keyPassword";
    String k4 = "org.lockss.platform.bar";

    Properties eprops = new Properties();
    eprops.put(k1, "12345");
    eprops.put(k2, "v2");
    eprops.put(k3, "v3");
    eprops.put(k4, "v4");
    PropUtil.toFile(efile, eprops);

    Configuration config = ConfigManager.getCurrentConfig();
    assertNull(config.get(k1));
    assertNull(config.get(k2));
    assertNull(config.get(k3));
    assertNull(config.get(k4));
    assertTrue(mgr.updateConfig(ListUtil.list(pfile)));
    Configuration config2 = ConfigManager.getCurrentConfig();
    assertEquals("12345", config2.get(k1));
    assertEquals("v2", config2.get(k2));
    assertEquals("v3", config2.get(k3));
    assertNull(config2.get(k4));
  }

  @Test
  public void testExpertConfigDefaultDeny() throws Exception {
    mgr.updateConfig(Collections.EMPTY_LIST);
    assertTrue(mgr.isLegalExpertConfigKey("org.lockss.unrelated.param"));
    assertTrue(mgr.isLegalExpertConfigKey("org.lockss.foo.passwordFrob"));
    assertFalse(mgr.isLegalExpertConfigKey("org.lockss.keystore.abcdy.keyFile"));
    assertFalse(mgr.isLegalExpertConfigKey("org.lockss.foo.password"));
    assertFalse(mgr.isLegalExpertConfigKey("org.lockss.keystore.foo.keyPasswordFile"));
    assertFalse(mgr.isLegalExpertConfigKey("org.lockss.platform.anything"));
    assertFalse(mgr.isLegalExpertConfigKey("org.lockss.app.exitOnce"));
    assertFalse(mgr.isLegalExpertConfigKey("org.lockss.app.exitImmediately"));
    assertTrue(mgr.isLegalExpertConfigKey("org.lockss.app.exitWhenever"));
    assertFalse(mgr.isLegalExpertConfigKey("org.lockss.app.exitAfter"));
    assertFalse(mgr.isLegalExpertConfigKey("org.lockss.auxPropUrls"));
    assertFalse(mgr.isLegalExpertConfigKey("org.lockss.localIPAddress"));
    assertFalse(mgr.isLegalExpertConfigKey("org.lockss.localV3Identity"));
    assertFalse(mgr.isLegalExpertConfigKey("org.lockss.localV3Port"));
    assertFalse(mgr.isLegalExpertConfigKey("org.lockss.config.expert.deny"));
    assertFalse(mgr.isLegalExpertConfigKey("org.lockss.config.expert.allow"));
  }

  void assertWriteArgs(Configuration expConfig, String expCacheConfigFileName,
		       String expHeader, boolean expSuppressReload, List args) {
    if (expConfig != null) assertEquals(expConfig, args.get(0));
    if (expCacheConfigFileName != null)
      assertEquals(expCacheConfigFileName, args.get(1));
    if (expHeader != null) assertEquals(expHeader, args.get(2));
    assertEquals(expSuppressReload, args.get(3));
  }


  @Test
  public void testUpdateAuConfig() throws Exception {
    String tmpdir = getTempDir().toString();
    // establish cache config dir
    ConfigurationUtil.setFromArgs(ConfigManager.PARAM_PLATFORM_DISK_SPACE_LIST,
				  tmpdir);

    Consumer cons =
      Consumer.createTopicConsumer(null, DEFAULT_JMS_NOTIFICATION_TOPIC);
    mymgr.setShouldSendNotifications("yes");
    mymgr.setUpJmsNotifications();

    System.setProperty("derby.stream.error.file",
	new File(tmpdir, "derby.log").getAbsolutePath());

    // Create and start the database manager.
    dbManager = new ConfigDbManager();
    theDaemon.setManagerByType(ConfigDbManager.class, dbManager);
    dbManager.initService(theDaemon);
    dbManager.startService();

    // Check that the database is empty.
    assertEquals(0, mgr.retrieveAllArchivalUnitConfiguration().size());

    // The first AU.
    Properties p = new Properties();
    p.put("org.lockss.au.foo.auid.foo", "111");
    p.put("org.lockss.au.foo.auid.bar", "222");
    p.put("org.lockss.au.foo.auid.baz", "333");

    // Store it.
    mgr.storeArchivalUnitConfiguration(AuConfigurationUtils.fromConfiguration(
	"org.lockss.au.foo.auid", fromProperties(p)));
    // Check that the AU has been stored.
    assertEquals(1, mgr.retrieveAllArchivalUnitConfiguration().size());
    AuConfiguration auConfiguration =
	mgr.retrieveArchivalUnitConfiguration("foo&auid");
    Map<String, String> auConfig = auConfiguration.getAuConfig();
    assertFalse(auConfig.isEmpty());
    assertEquals(3, auConfig.size());
    assertEquals("111", auConfig.get("foo"));
    assertEquals("222", auConfig.get("bar"));
    assertEquals("333", auConfig.get("baz"));

    assertEquals(MapUtil.map(ConfigManager.CONFIG_NOTIFY_VERB, "AuConfigStored",
			     ConfigManager.CONFIG_NOTIFY_AUID, "foo&auid"),
		 cons.receiveMap(TIMEOUT_SHOULDNT));

    // A second AU.
    p = new Properties();
    p.put("org.lockss.au.other.auid.foo", "11");
    p.put("org.lockss.au.other.auid.bar", "22");

    // Store it.
    mgr.storeArchivalUnitConfiguration(AuConfigurationUtils.fromConfiguration(
	"org.lockss.au.other.auid", fromProperties(p)));

    assertEquals(MapUtil.map(ConfigManager.CONFIG_NOTIFY_VERB, "AuConfigStored",
			     ConfigManager.CONFIG_NOTIFY_AUID, "other&auid"),
		 cons.receiveMap(TIMEOUT_SHOULDNT));

    // Check that the AU has been stored.
    assertEquals(2, mgr.retrieveAllArchivalUnitConfiguration().size());
    auConfiguration = mgr.retrieveArchivalUnitConfiguration("foo&auid");
    auConfig = auConfiguration.getAuConfig();
    assertFalse(auConfig.isEmpty());
    assertEquals(3, auConfig.size());
    assertEquals("111", auConfig.get("foo"));
    assertEquals("222", auConfig.get("bar"));
    assertEquals("333", auConfig.get("baz"));

    auConfiguration = mgr.retrieveArchivalUnitConfiguration("other&auid");
    auConfig = auConfiguration.getAuConfig();
    assertFalse(auConfig.isEmpty());
    assertEquals(2, auConfig.size());
    assertEquals("11", auConfig.get("foo"));
    assertEquals("22", auConfig.get("bar"));

    // Update the first AU, removing a property.
    p = new Properties();
    p.put("org.lockss.au.foo.auid.foo", "111");
    p.put("org.lockss.au.foo.auid.bar", "222");

    mgr.storeArchivalUnitConfiguration(AuConfigurationUtils.fromConfiguration(
	"org.lockss.au.foo.auid", fromProperties(p)));

    assertEquals(MapUtil.map(ConfigManager.CONFIG_NOTIFY_VERB, "AuConfigStored",
			     ConfigManager.CONFIG_NOTIFY_AUID, "foo&auid"),
		 cons.receiveMap(TIMEOUT_SHOULDNT));

    // Check that the AU has been stored.
    assertEquals(2, mgr.retrieveAllArchivalUnitConfiguration().size());
    auConfiguration = mgr.retrieveArchivalUnitConfiguration("foo&auid");
    auConfig = auConfiguration.getAuConfig();
    assertFalse(auConfig.isEmpty());
    assertEquals(2, auConfig.size());
    assertEquals("111", auConfig.get("foo"));
    assertEquals("222", auConfig.get("bar"));

    auConfiguration = mgr.retrieveArchivalUnitConfiguration("other&auid");
    auConfig = auConfiguration.getAuConfig();
    assertFalse(auConfig.isEmpty());
    assertEquals(2, auConfig.size());
    assertEquals("11", auConfig.get("foo"));
    assertEquals("22", auConfig.get("bar"));

    mgr.removeArchivalUnitConfiguration("foo&auid");
    assertEquals(MapUtil.map(ConfigManager.CONFIG_NOTIFY_VERB, "AuConfigRemoved",
			     "auid", "foo&auid"),
		 cons.receiveMap(TIMEOUT_SHOULDNT));
  }

  @Test
  public void testGetLocalFileDescrx() throws Exception {
    List<String> expNames =
      ListUtil.list(CONFIG_FILE_UI_IP_ACCESS,
		    CONFIG_FILE_PROXY_IP_ACCESS,
		    CONFIG_FILE_PLUGIN_CONFIG,
		    CONFIG_FILE_ICP_SERVER,
		    CONFIG_FILE_AUDIT_PROXY,
		    CONFIG_FILE_CONTENT_SERVERS,
		    CONFIG_FILE_ACCESS_GROUPS,
		    CONFIG_FILE_CRAWL_PROXY,
		    CONFIG_FILE_EXPERT);

    List<String> names = new ArrayList<String>();
    for (LocalFileDescr descr : mgr.getLocalFileDescrs()) {
      names.add(descr.getName());
    }
    assertEquals(expNames, names);
  }

  @Test
  public void testGetLocalFileDescr() throws Exception {
    LocalFileDescr descr = mgr.getLocalFileDescr(CONFIG_FILE_EXPERT);
    assertEquals(CONFIG_FILE_EXPERT, descr.getName());
    assertTrue(descr.isNeedReloadAfterWrite());
  }

  @Test
  public void testModifyCacheConfigFile() throws Exception {
    // Arbitrary config file
    final String FILE = ConfigManager.CONFIG_FILE_EXPERT;

    String tmpdir = getTempDir().toString();
    ConfigurationUtil.setFromArgs(ConfigManager.PARAM_PLATFORM_DISK_SPACE_LIST,
                                  tmpdir);
    assertNull(CurrentConfig.getParam("foo"));
    assertNull(CurrentConfig.getParam("bar"));
    assertNull(CurrentConfig.getParam("baz"));

    mgr.modifyCacheConfigFile(ConfigurationUtil.fromArgs("foo", "1", "bar", "2"),
                              FILE,
                              null);
    ConfigurationUtil.setFromArgs(ConfigManager.PARAM_PLATFORM_DISK_SPACE_LIST,
                                  tmpdir); // force reload
    assertEquals("1", CurrentConfig.getParam("foo"));
    assertEquals("2", CurrentConfig.getParam("bar"));
    assertNull(CurrentConfig.getParam("baz"));

    mgr.modifyCacheConfigFile(ConfigurationUtil.fromArgs("foo", "111", "baz", "333"),
                              FILE,
                              null);
    ConfigurationUtil.setFromArgs(ConfigManager.PARAM_PLATFORM_DISK_SPACE_LIST,
                                  tmpdir); // force reload
    assertEquals("111", CurrentConfig.getParam("foo"));
    assertEquals("2", CurrentConfig.getParam("bar"));
    assertEquals("333", CurrentConfig.getParam("baz"));

    mgr.modifyCacheConfigFile(ConfigurationUtil.fromArgs("bar", "222"),
                              SetUtil.set("foo"),
                              FILE,
                              null);
    ConfigurationUtil.setFromArgs(ConfigManager.PARAM_PLATFORM_DISK_SPACE_LIST,
                                  tmpdir); // force reload
    assertFalse(CurrentConfig.getCurrentConfig().containsKey("foo"));
    assertEquals("222", CurrentConfig.getParam("bar"));
    assertEquals("333", CurrentConfig.getParam("baz"));

    try {
      mgr.modifyCacheConfigFile(ConfigurationUtil.fromArgs("foo", "1"),
                                SetUtil.set("foo"),
                                FILE,
                                null);
      fail("Failed to throw an IllegalArgumentException when a key was both in the update set and in the delete set");
    } catch (IllegalArgumentException iae) {
      // All is well
    }
  }

  @Test
  public void testReadAuDb() throws Exception {
    log.debug2("Invoked");

    String tempDirPath = setUpDiskSpace();
    if (log.isDebug3()) log.debug3("tempDirPath = " + tempDirPath);

    System.setProperty("derby.stream.error.file",
	new File(tempDirPath, "derby.log").getAbsolutePath());

    // Create and start the database manager.
    dbManager = new ConfigDbManager();
    theDaemon.setManagerByType(ConfigDbManager.class, dbManager);
    dbManager.initService(theDaemon);
    dbManager.startService();

    // Initially, there are no Archival Units.
    Collection<AuConfiguration> c1 = mgr.retrieveAllArchivalUnitConfiguration();
    assertTrue(c1.isEmpty());

    // Create the configuration of an Archival Unit.
    Map<String, String> auConfig = new HashMap<>();
    auConfig.put("foo", "111");
    auConfig.put("bar", "222");
    auConfig.put("bar", "222");

    AuConfiguration auConfiguration = new AuConfiguration("foo&auid", auConfig);

    // Store the Archival Unit configuration.
    mgr.storeArchivalUnitConfiguration(auConfiguration);

    // Verify that the Archival Unit configuration has been stored.
    Collection<AuConfiguration> c2 = mgr.retrieveAllArchivalUnitConfiguration();
    assertFalse(c2.isEmpty());
    assertEquals(1, c2.size());
    assertTrue(c2.contains(auConfiguration));

    log.debug2("Done");
  }

  @Test
  public void testLoadTitleDb() throws IOException {
    String props =       
      "org.lockss.title.title1.title=Air & Space volume 3\n" +
      "org.lockss.title.title1.plugin=org.lockss.testplugin1\n" +
      "org.lockss.title.title1.pluginVersion=4\n" +
      "org.lockss.title.title1.issn=0003-0031\n" +
      "org.lockss.title.title1.journal.link.1.type=continuedBy\n" +
      "org.lockss.title.title1.journal.link.1.journalId=0003-0031\n" +
      "org.lockss.title.title1.param.1.key=volume\n" +
      "org.lockss.title.title1.param.1.value=3\n" +
      "org.lockss.title.title1.param.2.key=year\n" +
      "org.lockss.title.title1.param.2.value=1999\n" +
      "org.lockss.title.title1.attributes.publisher=The Smithsonian Institution";
    String u2 = FileTestUtil.urlOfString(props);
    String u1 = FileTestUtil.urlOfString("a=1\norg.lockss.titleDbs="+u2);
    assertFalse(mgr.waitConfig(Deadline.EXPIRED));
    assertTrue(mgr.updateConfig(ListUtil.list(u1)));
    assertTrue(mgr.waitConfig(Deadline.EXPIRED));
    
    Configuration config = mgr.getCurrentConfig();
    assertEquals("1", config.get("a"));
    
    Tdb tdb = config.getTdb();
    assertNotNull(tdb);
    assertEquals(1, tdb.getTdbAuCount());
  }

  @Test
  public void testLoadAuxProps() throws IOException {
    String xml = "<lockss-config>\n" +
      "<property name=\"org.lockss\">\n" +
      " <property name=\"foo.bar\" value=\"42\"/>" +
      "</property>" +
      "</lockss-config>";

    String u2 = FileTestUtil.urlOfString(xml, ".xml");
    String u1 = FileTestUtil.urlOfString("a=1\norg.lockss.auxPropUrls="+u2);
    assertFalse(mgr.waitConfig(Deadline.EXPIRED));
    assertTrue(mgr.updateConfig(ListUtil.list(u1)));
    assertTrue(mgr.waitConfig(Deadline.EXPIRED));
    Configuration config = mgr.getCurrentConfig();
    assertEquals("42", config.get("org.lockss.foo.bar"));
    assertEquals("1", config.get("a"));
  }

  @Test
  public void testLoadAuxPropsRel() throws IOException {
    String xml = "<lockss-config>\n" +
      "<property name=\"org.lockss\">\n" +
      " <property name=\"foo.bar\" value=\"43\"/>" +
      "</property>" +
      "</lockss-config>";

    String u2 = FileTestUtil.urlOfString(xml, ".xml");
    String u2rel = new File(new URL(u2).getPath()).getName();
    assertTrue(StringUtil.startsWithIgnoreCase(u2, "file"));
    assertFalse(StringUtil.startsWithIgnoreCase(u2rel, "file"));
    String u1 = FileTestUtil.urlOfString("a=1\norg.lockss.auxPropUrls="+u2rel);
    assertFalse(mgr.waitConfig(Deadline.EXPIRED));
    assertTrue(mgr.updateConfig(ListUtil.list(u1)));
    assertTrue(mgr.waitConfig(Deadline.EXPIRED));
    Configuration config = mgr.getCurrentConfig();
    assertEquals("43", config.get("org.lockss.foo.bar"));
    assertEquals("1", config.get("a"));
  }

  @Test
  public void testFailedLoadDoesntSetHaveConfig() throws IOException {
    String u1 = "malformed://url/";
    assertFalse(mgr.waitConfig(Deadline.EXPIRED));
    assertFalse(mgr.updateConfig(ListUtil.list(u1)));
    assertFalse(mgr.waitConfig(Deadline.EXPIRED));
  }

  // Illegal title db key prevents loading the entire file.
  @Test
  public void testLoadIllTitleDb() throws IOException {
    String u2 = FileTestUtil.urlOfString("org.lockss.notTitleDb.foo=bar\n" +
					 "org.lockss.title.x.foo=bar");
    String u1 = FileTestUtil.urlOfString("a=1\norg.lockss.titleDbs="+u2);
    assertTrue(mgr.updateConfig(ListUtil.list(u1)));
    Configuration config = mgr.getCurrentConfig();
    assertEquals(null, config.get("org.lockss.title.x.foo"));
    assertEquals(null, config.get("org.lockss.notTitleDb.foo"));
    assertEquals("1", config.get("a"));
  }

  // Illegal key in expert config file is ignored, rest of file loads.
  @Test
  public void testLoadIllExpert() throws IOException {
    String u2 = FileTestUtil.urlOfString("org.lockss.notTitleDb.foo=bar\n" +
					 "org.lockss.title.x.foo=bar");
    String u1 = FileTestUtil.urlOfString("a=1\norg.lockss.titleDbs="+u2);
    assertTrue(mgr.updateConfig(ListUtil.list(u1)));
    Configuration config = mgr.getCurrentConfig();
    assertEquals(null, config.get("org.lockss.title.x.foo"));
    assertEquals(null, config.get("org.lockss.notTitleDb.foo"));
    assertEquals("1", config.get("a"));
  }

  @Test
  public void testIsChanged() throws IOException {
    List gens;

    String u1 = FileTestUtil.urlOfString("a=1");
    String u2 = FileTestUtil.urlOfString("a=2");
    gens = mgr.getConfigGenerations(ListUtil.list(u1, u2), true, true, "test");
    assertTrue(mgr.isChanged(gens));
    mgr.updateGenerations(gens);
    assertFalse(mgr.isChanged(gens));
    FileConfigFile cf = (FileConfigFile)mgr.getConfigCache().find(u2);
    cf.storedConfig(newConfiguration());
    gens = mgr.getConfigGenerations(ListUtil.list(u1, u2), true, true, "test");
    assertTrue(mgr.isChanged(gens));
  }

  @Test
  public void testLoadList() throws IOException {
    Configuration config = newConfiguration();
    List gens =
      mgr.getConfigGenerations(ListUtil.list(FileTestUtil.urlOfString(c1),
					     FileTestUtil.urlOfString(c1a)),
			       true, true, "props");
    mgr.loadList(config, gens);
    assertEquals("12", config.get("prop1"));
    assertEquals("xxx", config.get("prop2"));
    assertTrue(config.getBoolean("prop3", false));
    assertEquals("yyy", config.get("prop4"));
  }

  @Test
  public void testConnPool() throws IOException {
    LockssUrlConnectionPool pool = mgr.getConnectionPool();
    Configuration config = ConfigManager.newConfiguration();
    config.put("bar", "false");
    MemoryConfigFile cf1 = new MemoryConfigFile("a", config, 1);
    MemoryConfigFile cf2 = new MemoryConfigFile("a", config, 1);
    
    List<ConfigFile.Generation> gens =
      mgr.getConfigGenerations(ListUtil.list(cf1, cf2), true, true, "props");
    for (ConfigFile.Generation gen : gens) {
      MemoryConfigFile cf = (MemoryConfigFile)gen.getConfigFile();
      assertSame(pool, cf.getConnectionPool());
    }
  }

  @Test
  public void testXLockssInfo() throws IOException {
    TimeBase.setSimulated(1000);
    String u1 = FileTestUtil.urlOfString("org.lockss.foo=bar");
    assertTrue(mgr.updateConfig(ListUtil.list(u1)));
    BaseConfigFile cf = (BaseConfigFile)mgr.getConfigCache().find(u1);
    String info = (String)cf.m_props.get("X-Lockss-Info");
    log.error("info = " + info);
    assertMatchesRE("groups=nogroup", info);
    // official build will set daemon, unofficial will set built_on
    assertMatchesRE("daemon=|built_on=", info);
    cf.setNeedsReload();
    assertFalse(mgr.updateConfig(ListUtil.list(u1)));
    info = (String)cf.m_props.get("X-Lockss-Info");
    assertEquals(null, info);
    TimeBase.step(ConfigManager.DEFAULT_SEND_VERSION_EVERY + 1);
    cf.setNeedsReload();
    assertFalse(mgr.updateConfig(ListUtil.list(u1)));
    info = (String)cf.m_props.get("X-Lockss-Info");
    assertMatchesRE("groups=nogroup", info);
    // official build will set daemon, unofficial will set built_on
    assertMatchesRE("daemon=|built_on=", info);
  }

  @Test
  public void testHasLocalCacheConfig() throws Exception {
    assertFalse(mgr.hasLocalCacheConfig());
    // set up local config dir
    String tmpdir = getTempDir().toString();
    Properties props = new Properties();
    props.put(ConfigManager.PARAM_PLATFORM_DISK_SPACE_LIST, tmpdir);
    ConfigurationUtil.setCurrentConfigFromProps(props);
    String relConfigPath =
      CurrentConfig.getParam(ConfigManager.PARAM_CONFIG_PATH,
                             ConfigManager.DEFAULT_CONFIG_PATH);
    File cdir = new File(tmpdir, relConfigPath);
    assertTrue(cdir.exists());

    assertFalse(mgr.hasLocalCacheConfig());

    // loading local shouldn't set flag because no files
    mgr.getCacheConfigGenerations(true);
    assertFalse(mgr.hasLocalCacheConfig());

    // write a local config file
    mgr.writeCacheConfigFile(props, ConfigManager.CONFIG_FILE_EXPERT,
			     "this is a header");

    assertFalse(mgr.hasLocalCacheConfig());

    // load it to set flag
    mgr.getCacheConfigGenerations(true);

    assertTrue(mgr.hasLocalCacheConfig());
  }

  @Test
  public void testFromProperties() throws Exception {
    Properties props = new Properties();
    props.put("foo", "23");
    props.put("bar", "false");
    Configuration config = ConfigManager.fromProperties(props);
    assertEquals(2, config.keySet().size());
    assertEquals("23", config.get("foo"));
    assertEquals("false", config.get("bar"));
  }

  @Test
  public void testRemoteConfigFailoverDisabled() throws Exception {
    String url1 = "http://one/xxx.xml";

    assertFalse(mgr.hasLocalCacheConfig());
    // set up local config dir
    String tmpdir = getTempDir().toString();
    ConfigurationUtil.addFromArgs(ConfigManager.PARAM_PLATFORM_DISK_SPACE_LIST,
				  tmpdir,
				  ConfigManager.PARAM_REMOTE_CONFIG_FAILOVER,
				  "false");
    mgr.setUpRemoteConfigFailover();

    String relConfigPath =
      CurrentConfig.getParam(ConfigManager.PARAM_CONFIG_PATH,
                             ConfigManager.DEFAULT_CONFIG_PATH);
    assertNull(mgr.getRemoteConfigFailoverTempFile(url1));
    assertNull(mgr.getRemoteConfigFailoverFile(url1));
  }

  @Test
  public void testRemoteConfigFailoverNotExist() throws Exception {
    String url1 = "http://one/xxx.xml";

    assertFalse(mgr.hasLocalCacheConfig());
    // set up local config dir
    String tmpdir = getTempDir().toString();
    ConfigurationUtil.addFromArgs(ConfigManager.PARAM_PLATFORM_DISK_SPACE_LIST,
				  tmpdir,
				  ConfigManager.PARAM_REMOTE_CONFIG_FAILOVER,
				  "true");
    mgr.setUpRemoteConfigFailover();

    String relConfigPath =
      CurrentConfig.getParam(ConfigManager.PARAM_CONFIG_PATH,
                             ConfigManager.DEFAULT_CONFIG_PATH);
    assertEquals(null, mgr.getRemoteConfigFailoverFile(url1));

    File tf1 = mgr.getRemoteConfigFailoverTempFile(url1);
    assertMatchesRE("^" + tmpdir + ".*\\.tmp$", tf1.getPath());

    assertEquals(null, mgr.getRemoteConfigFailoverFile(url1));
  }

  @Test
  public void testRemoteConfigFailoverMap() throws Exception {
    String url1 = "http://one/xxx.xml";
    String url2 = "http://one/yyy.txt";

    assertFalse(mgr.hasLocalCacheConfig());
    // set up local config dir
    String tmpdir = getTempDir().toString();
    Properties props = new Properties();
    props.put(ConfigManager.PARAM_PLATFORM_DISK_SPACE_LIST, tmpdir);
    ConfigurationUtil.addFromArgs(ConfigManager.PARAM_PLATFORM_DISK_SPACE_LIST,
				  tmpdir,
				  ConfigManager.PARAM_REMOTE_CONFIG_FAILOVER,
				  "true");
    mgr.setUpRemoteConfigFailover();
    String relConfigPath =
      CurrentConfig.getParam(ConfigManager.PARAM_CONFIG_PATH,
                             ConfigManager.DEFAULT_CONFIG_PATH);
    File tf1 = mgr.getRemoteConfigFailoverTempFile(url1);
    assertMatchesRE("^" + tmpdir + ".*\\.tmp$", tf1.getPath());
    String sss = "sasdflkajsdlfj content dfljasdfl;ajsdf";
    StringUtil.toFile(tf1, sss);
    assertTrue(tf1.exists());
    mgr.updateRemoteConfigFailover();
    File pf1 = mgr.getRemoteConfigFailoverFile(url1);
    assertTrue(pf1.exists());
    assertFalse(tf1.exists());
    assertMatchesRE("^" + tmpdir + ".*\\.xml\\.gz", pf1.getPath());
    assertReaderMatchesString(sss, new FileReader(pf1));

    RemoteConfigFailoverMap rccm = mgr.loadRemoteConfigFailoverMap();
    RemoteConfigFailoverInfo rcci = rccm.get(url1);
    assertEquals(url1, rcci.getUrl());
    assertEquals(pf1.getName(), rcci.getFilename());
    assertEquals("01-xxx.xml.gz", rcci.getFilename());

    File tf2 = mgr.getRemoteConfigFailoverTempFile(url2);
    mgr.updateRemoteConfigFailover();
    File pf2 = mgr.getRemoteConfigFailoverFile(url2);
    assertMatchesRE("02-yyy.txt.gz$", pf2.getPath());
  }

  @Test
  public void testAddGenerationToListIfNotInIt() throws Exception {
    Configuration config = ConfigManager.newConfiguration();
    List<ConfigFile.Generation> targetList =
	new ArrayList<ConfigFile.Generation>();
    assertEquals(0, targetList.size());

    ConfigFile cf1 = loadFCF(FileTestUtil.urlOfString(c1));
    ConfigFile.Generation gen = new ConfigFile.Generation(cf1, config, 1);
    mgr.addGenerationToListIfNotInIt(gen, targetList);
    assertEquals(1, targetList.size());

    mgr.addGenerationToListIfNotInIt(gen, targetList);
    assertEquals(1, targetList.size());

    ConfigFile cf2 = loadFCF(FileTestUtil.urlOfString(c2));
    ConfigFile.Generation gen2 = new ConfigFile.Generation(cf2, config, 2);
    mgr.addGenerationToListIfNotInIt(gen2, targetList);
    assertEquals(2, targetList.size());
    
    mgr.addGenerationToListIfNotInIt(gen, targetList);
    assertEquals(2, targetList.size());
    mgr.addGenerationToListIfNotInIt(gen2, targetList);
    assertEquals(2, targetList.size());

    List<ConfigFile.Generation> sourceList =
	new ArrayList<ConfigFile.Generation>();
    mgr.addGenerationToListIfNotInIt(gen, sourceList);
    mgr.addGenerationToListIfNotInIt(gen2, sourceList);
    assertEquals(2, sourceList.size());

    mgr.addGenerationsToListIfNotInIt(sourceList, targetList);
    assertEquals(2, targetList.size());

    ConfigFile cf3 = loadFCF(FileTestUtil.urlOfString(c1a));
    ConfigFile.Generation gen3 = new ConfigFile.Generation(cf3, config, 3);
    mgr.addGenerationToListIfNotInIt(gen3, sourceList);
    assertEquals(3, sourceList.size());

    mgr.addGenerationsToListIfNotInIt(sourceList, targetList);
    assertEquals(3, targetList.size());
  }

  @Test
  public void testGetUrlParent() throws Exception {
    ConfigFile cf = new FileConfigFile("http://parent", mgr);
    mgr.parentConfigFile.put("http://child", cf);
    assertNull(mgr.getUrlParent(null));
    assertNull(mgr.getUrlParent(""));
    assertNull(mgr.getUrlParent("http://abc"));
    assertNull(mgr.getUrlParent("http://parent"));
    assertEquals(cf, mgr.getUrlParent("http://child"));
  }

  @Test
  public void testRestServiceEffect() {
    // No REST Configuration service configured: This is the REST Configuration
    // service itself.
    ConfigManager noRestServiceConfigManager =
	MyConfigManager.makeConfigManager(getMockLockssDaemon());

    // Long reload interval default.
    assertEquals(10 * Constants.MINUTE,
	noRestServiceConfigManager.reloadInterval);

    // Reconfigure the reload interval.
    ConfigurationUtil.addFromArgs(ConfigManager.PARAM_RELOAD_INTERVAL, "12345");
    assertEquals(12345, noRestServiceConfigManager.reloadInterval);

    // There is a remote configuration failover setup by default.
    // (No longer - don't rely on the default)
    ConfigurationUtil.addFromArgs(ConfigManager.PARAM_REMOTE_CONFIG_FAILOVER,
	"true");
    noRestServiceConfigManager.setUpRemoteConfigFailover();
    assertNotNull(noRestServiceConfigManager.remoteConfigFailoverDir);
    assertNotNull(noRestServiceConfigManager.rcfm);

    // Disable the remote configuration failover system.
    ConfigurationUtil.addFromArgs(ConfigManager.PARAM_REMOTE_CONFIG_FAILOVER,
	"false");
    noRestServiceConfigManager.setUpRemoteConfigFailover();
    assertNull(noRestServiceConfigManager.remoteConfigFailoverDir);
    assertNull(noRestServiceConfigManager.rcfm);

    // Enable the remote configuration failover system.
    ConfigurationUtil.addFromArgs(ConfigManager.PARAM_REMOTE_CONFIG_FAILOVER,
	"true");
    noRestServiceConfigManager.setUpRemoteConfigFailover();
    assertNotNull(noRestServiceConfigManager.remoteConfigFailoverDir);
    assertNotNull(noRestServiceConfigManager.rcfm);

    // There is a REST Configuration service configured: This is a client.
    ConfigManager restServiceConfigManager =
	MyConfigManager.makeConfigManager(getMockLockssDaemon(),
	    "http://localhost:12345");

    // Short reload interval default.
    assertEquals(15 * Constants.SECOND,
	restServiceConfigManager.reloadInterval);

    // Reconfigure the reload interval.
    ConfigurationUtil.addFromArgs(ConfigManager.PARAM_RELOAD_INTERVAL, "12345");
    assertEquals(12345, restServiceConfigManager.reloadInterval);

    // There is no remote configuration failover setup by default.
    restServiceConfigManager.setUpRemoteConfigFailover();
    assertNull(restServiceConfigManager.remoteConfigFailoverDir);
    assertNull(restServiceConfigManager.rcfm);

    // Disable the remote configuration failover system.
    ConfigurationUtil.addFromArgs(ConfigManager.PARAM_REMOTE_CONFIG_FAILOVER,
	"false");
    restServiceConfigManager.setUpRemoteConfigFailover();
    assertNull(restServiceConfigManager.remoteConfigFailoverDir);
    assertNull(restServiceConfigManager.rcfm);

    // Enable the remote configuration failover system.
    ConfigurationUtil.addFromArgs(ConfigManager.PARAM_REMOTE_CONFIG_FAILOVER,
	"true");
    restServiceConfigManager.setUpRemoteConfigFailover();
    assertNull(restServiceConfigManager.remoteConfigFailoverDir);
    assertNull(restServiceConfigManager.rcfm);
  }

  /**
   * Tests the Archival Unit configuration database.
   */
  @Test
  public void testDb() throws Exception {
    log.debug2("Invoked");

    String tempDirPath = setUpDiskSpace();
    if (log.isDebug3()) log.debug3("tempDirPath = " + tempDirPath);

    System.setProperty("derby.stream.error.file",
	new File(tempDirPath, "derby.log").getAbsolutePath());

    // Create and start the database manager.
    dbManager = new ConfigDbManager();
    theDaemon.setManagerByType(ConfigDbManager.class, dbManager);
    dbManager.initService(theDaemon);
    dbManager.startService();

    // Validation.
    try {
      mgr.storeArchivalUnitConfiguration(null);
      fail("Failed to throw storing a null AuConfig");
    } catch (IllegalArgumentException iae) {
      // Expected.
    }

    String pluginId1 = "org|lockss|plugin|SomePlugin1";
    String auid1 = pluginId1 + "&some_key_1";

    try {
      Map<String, String> configuration = null;
      mgr.storeArchivalUnitConfiguration(new AuConfiguration(auid1,
	  configuration));
      fail("Failed to throw storing a null configuration");
    } catch (IllegalArgumentException iae) {
      // Expected.
    }

    try {
      mgr.storeArchivalUnitConfiguration(new AuConfiguration(auid1,
	  new HashMap<String, String>()));
      fail("Failed to throw storing an empty configuration");
    } catch (IllegalArgumentException iae) {
      // Expected.
    }

    // Define the configuration of the first AU.
    Map<String, String> configuration1 = new HashMap<>();
    configuration1.put("au_oai_date", "2014");
    configuration1.put("au_oai_set", "biorisk");
    configuration1.put("reserved.displayName", "BioRisk Volume 2014");

    AuConfiguration auConfiguration1 =
	new AuConfiguration(auid1, configuration1);

    // Store the configuration of the first AU.
    long beforeAdding1 = TimeBase.nowMs();
    mgr.storeArchivalUnitConfiguration(auConfiguration1);
    long afterAdding1 = TimeBase.nowMs();

    // Define the configuration of the second AU.
    String auid2 = pluginId1 + "&some_key_2";

    Map<String, String> configuration2 = new HashMap<>();
    configuration2.put("reserved.disabled", "false");

    AuConfiguration auConfiguration2 =
	new AuConfiguration(auid2, configuration2);

    // Store the configuration of the second AU.
    long beforeAdding2 = TimeBase.nowMs();
    mgr.storeArchivalUnitConfiguration(auConfiguration2);
    long afterAdding2 = TimeBase.nowMs();

    // Retrieve all the AU configurations.
    Collection<AuConfiguration> auConfigs =
	mgr.retrieveAllArchivalUnitConfiguration();
    if (log.isDebug3()) log.debug3("auConfigs = " + auConfigs);

    assertEquals(2, auConfigs.size());
    assertTrue(auConfigs.contains(auConfiguration1));
    assertTrue(auConfigs.contains(auConfiguration2));

    // Retrieve the configuration of the first AU.
    AuConfiguration config1 = mgr.retrieveArchivalUnitConfiguration(auid1);

    assertEquals(auConfiguration1, config1);

    // Retrieve the configuration of the second AU.
    AuConfiguration config2 = mgr.retrieveArchivalUnitConfiguration(auid2);

    assertEquals(auConfiguration2, config2);

    // Retrieve the configuration creation time of the first AU.
    long creationTime1 =
	mgr.retrieveArchivalUnitConfigurationCreationTime(auid1).longValue();
    assertTrue(creationTime1 >= beforeAdding1);
    assertTrue(creationTime1 <= afterAdding1);

    // Retrieve the configuration last update time of the first AU.
    long lastUpdateTime1 =
	mgr.retrieveArchivalUnitConfigurationLastUpdateTime(auid1).longValue();
    assertEquals(creationTime1, lastUpdateTime1);

    // Retrieve the configuration creation time of the second AU.
    long creationTime2 =
	mgr.retrieveArchivalUnitConfigurationCreationTime(auid2).longValue();
    assertTrue(creationTime2 >= beforeAdding2);
    assertTrue(creationTime2 <= afterAdding2);

    // Retrieve the configuration last update time of the second AU.
    long lastUpdateTime2 =
	mgr.retrieveArchivalUnitConfigurationLastUpdateTime(auid2).longValue();
    assertEquals(creationTime2, lastUpdateTime2);

    // Define the updated configuration of the second AU.
    Map<String, String> configuration3 = new HashMap<>();
    configuration3.put("newKey1", "newValue1");
    configuration3.put("newKey2", "newValue2");

    AuConfiguration auConfiguration2new =
	new AuConfiguration(auid2, configuration3);

    // Store the updated configuration of the second AU.
    long beforeAdding2new = TimeBase.nowMs();
    mgr.storeArchivalUnitConfiguration(auConfiguration2new);
    long afterAdding2new = TimeBase.nowMs();

    // Retrieve all the AU configurations.
    auConfigs = mgr.retrieveAllArchivalUnitConfiguration();
    if (log.isDebug3()) log.debug3("auConfigs = " + auConfigs);

    assertEquals(2, auConfigs.size());
    assertTrue(auConfigs.contains(auConfiguration1));
    assertTrue(auConfigs.contains(auConfiguration2new));

    // Retrieve the configuration of the second AU.
    AuConfiguration config2new = mgr.retrieveArchivalUnitConfiguration(auid2);

    assertEquals(auConfiguration2new, config2new);

    assertEquals(creationTime2,
	mgr.retrieveArchivalUnitConfigurationCreationTime(auid2).longValue());

    // Retrieve the configuration last update time of the second AU.
    long lastUpdateTime2new =
	mgr.retrieveArchivalUnitConfigurationLastUpdateTime(auid2).longValue();
    assertTrue(lastUpdateTime2new > creationTime2);
    assertTrue(lastUpdateTime2new >= beforeAdding2new);
    assertTrue(lastUpdateTime2new <= afterAdding2new);

    String pluginId2 = "org|lockss|plugin|SomePlugin2";
    String auid3 = pluginId2 + "&some_key_2";
    Map<String, String> configuration4 = new HashMap<>();
    configuration4.put("au_oai_date", "2019");
    configuration4.put("au_oai_set", "biorisk");
    configuration4.put("reserved.displayName", "BioRisk Volume 2019");

    AuConfiguration auConfiguration3 =
	new AuConfiguration(auid3, configuration4);

    // Store the configuration of the third AU.
    long beforeAdding3 = TimeBase.nowMs();
    mgr.storeArchivalUnitConfiguration(auConfiguration3);
    long afterAdding3 = TimeBase.nowMs();

    Collection<String> pluginKeys = ListUtil.list(pluginId1, pluginId2);
	
    // Retrieve the configurations keyed by plugin.
    Map<String, List<AuConfiguration>> pluginsAusConfigs =
	mgr.retrieveAllPluginsAusConfigurations(pluginKeys);

    // Check the first plugin.
    assertEquals(2, pluginsAusConfigs.size());

    List<AuConfiguration> plugin1Config = pluginsAusConfigs.get(pluginId1);

    assertEquals(2, plugin1Config.size());
    assertTrue(plugin1Config.contains(auConfiguration1));
    assertTrue(plugin1Config.contains(config2new));

    // Check the second plugin.
    List<AuConfiguration> plugin2Config = pluginsAusConfigs.get(pluginId2);

    assertEquals(1, plugin2Config.size());
    assertEquals(auConfiguration3, plugin2Config.get(0));

    // Remove the configuration of the first AU.
    mgr.removeArchivalUnitConfiguration(auid1);

    // Retrieve all the AU configurations.
    auConfigs = mgr.retrieveAllArchivalUnitConfiguration();
    if (log.isDebug3()) log.debug3("auConfigs = " + auConfigs);

    assertEquals(2, auConfigs.size());
    assertTrue(auConfigs.contains(auConfiguration2new));
    assertTrue(auConfigs.contains(auConfiguration3));

    // Retrieve the configuration of the first (deleted) AU.
    AuConfiguration config1new = mgr.retrieveArchivalUnitConfiguration(auid1);
    if (log.isDebug3()) log.debug3("config1new = " + config1new);
    assertNull(config1new);

    // Retrieve the configuration creation time of the first (deleted) AU.
    Long creationTime1new =
	mgr.retrieveArchivalUnitConfigurationCreationTime(auid1);
    if (log.isDebug3()) log.debug3("creationTime1new = " + creationTime1new);
    assertNull(creationTime1new);

    // Retrieve the configuration last update time of the first (deleted) AU.
    Long lastUpdateTime1new =
	mgr.retrieveArchivalUnitConfigurationLastUpdateTime(auid1);
    assertNull(lastUpdateTime1new);

    // Remove the configuration of the second AU.
    mgr.removeArchivalUnitConfiguration(auid2);

    // Retrieve all the AU configurations.
    auConfigs = mgr.retrieveAllArchivalUnitConfiguration();
    if (log.isDebug3()) log.debug3("auConfigs = " + auConfigs);

    assertEquals(1, auConfigs.size());
    assertTrue(auConfigs.contains(auConfiguration3));

    // Retrieve the configuration of the second (deleted) AU.
    config2new = mgr.retrieveArchivalUnitConfiguration(auid2);
    if (log.isDebug3()) log.debug3("config2new = " + config2new);
    assertNull(config2new);

    // Retrieve the configuration creation time of the second (deleted) AU.
    Long creationTime2new =
	mgr.retrieveArchivalUnitConfigurationCreationTime(auid2);
    if (log.isDebug3()) log.debug3("creationTime2new = " + creationTime2new);
    assertNull(creationTime2new);

    // Retrieve the configuration last update time of the second (deleted) AU.
    Long lastUpdateTime2newest =
	mgr.retrieveArchivalUnitConfigurationLastUpdateTime(auid2);
    assertNull(lastUpdateTime2newest);

    // Remove the configuration of the third AU.
    mgr.removeArchivalUnitConfiguration(auid3);

    // Retrieve all the AU configurations.
    auConfigs = mgr.retrieveAllArchivalUnitConfiguration();
    if (log.isDebug3()) log.debug3("auConfigs = " + auConfigs);

    assertEquals(0, auConfigs.size());

    log.debug2("Done");
  }

  private Configuration newConfiguration() {
    return new ConfigurationPropTreeImpl();
  }

  static class MyConfigManager extends ConfigManager {
    List<List> writeArgs = new ArrayList<List>();
    String sendNotifications = "super";
    String receiveNotifications = "super";

    public static ConfigManager makeConfigManager(LockssDaemon daemon) {
      theMgr = new MyConfigManager();
      theMgr.initService(daemon);
      return theMgr;
    }

    public static ConfigManager makeConfigManager(LockssDaemon daemon,
	String restConfigServiceUrl) {
      theMgr = new MyConfigManager(null, restConfigServiceUrl, null, null);
      theMgr.initService(daemon);
      return theMgr;
    }

    public MyConfigManager() {
      super();
    }

    public MyConfigManager(String bootstrapPropsUrl,
	String restConfigServiceUrl, List<String> urls, String groupNames) {
      super(bootstrapPropsUrl, restConfigServiceUrl, urls, groupNames);
    }

    @Override
    public synchronized void writeCacheConfigFile(Configuration config,
						  String cacheConfigFileName,
						  String header,
						  boolean suppressReload)
	throws IOException {
      super.writeCacheConfigFile(config, cacheConfigFileName,
				 header, suppressReload);
      writeArgs.add(ListUtil.list(config, cacheConfigFileName, header, suppressReload));
    }

    void setShouldSendNotifications(String val) {
      sendNotifications = val;
    }

    void setShouldReceiveNotifications(String val) {
      receiveNotifications = val;
    }

    @Override
    protected boolean shouldSendNotifications() {
      switch(sendNotifications) {
      case "yes": return true;
      case "no": return false;
      default: return super.shouldSendNotifications();
      }
    }

    @Override
    protected boolean shouldReceiveNotifications() {
      switch(receiveNotifications) {
      case "yes": return true;
      case "no": return false;
      default: return super.shouldReceiveNotifications();
      }
    }

  }
}
