/*

Copyright (c) 2000-2018, Board of Trustees of Leland Stanford Jr. University.
All rights reserved.

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

package org.lockss.plugin;

import java.io.*;
import java.net.*;
import java.util.*;
import java.security.KeyStore;
import javax.jms.*;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.ActiveMQConnection;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.junit.*;
import org.lockss.app.*;
import org.lockss.config.*;
import org.lockss.daemon.*;
import org.lockss.plugin.base.*;
import org.lockss.plugin.definable.*;
import org.lockss.poller.*;
import org.lockss.repository.*;
import org.lockss.util.*;
import org.lockss.util.jms.*;
import org.lockss.util.time.Deadline;
import org.lockss.util.time.TimerUtil;
import org.lockss.test.*;
import org.lockss.state.*;
import org.lockss.jms.*;
import static org.lockss.plugin.PluginManager.CuContentReq;

/**
 * Test class for org.lockss.plugin.PluginManager
 */
public class TestPluginManager extends LockssTestCase4 {
  static Logger log = Logger.getLogger();

  static BrokerService broker;
  private MyMockLockssDaemon theDaemon;

  static String mockPlugKey =
    PluginManager.pluginKeyFromName(MyMockPlugin.class.getName());
  static Properties props1 = new Properties();
  static Properties props2 = new Properties();
  static Properties props3 = new Properties();
  static {
    props1.setProperty(MockPlugin.CONFIG_PROP_1, "val1");
    props1.setProperty(MockPlugin.CONFIG_PROP_2, "val2");
    props2.setProperty(MockPlugin.CONFIG_PROP_1, "val1");
    props2.setProperty(MockPlugin.CONFIG_PROP_2, "va.l3");//auid contains a dot
    props3.setProperty(BaseArchivalUnit.KEY_NEW_CONTENT_CRAWL_INTERVAL, "3d");
  }

  static String mauauidKey1 = PropUtil.propsToCanonicalEncodedString(props1);
  static String mauauid1 = mockPlugKey+"&"+ mauauidKey1;

  static String mauauidKey2 = PropUtil.propsToCanonicalEncodedString(props2);
  static String mauauid2 = mockPlugKey+"&"+mauauidKey2;

  static String mauauidKey3 = PropUtil.propsToCanonicalEncodedString(props3);
  static String mauauid3 = mockPlugKey+"&"+mauauidKey3;

  static String p1param =
    PluginManager.PARAM_AU_TREE + "." + mockPlugKey + ".";

  static String p1a1param = p1param + mauauidKey1 + ".";
  static String p1a2param = p1param + mauauidKey2 + ".";
  static String p1a3param = p1param + mauauidKey3 + ".";

  private String pluginJar;
  private String signAlias = "goodguy";
  private String pubKeystore = "org/lockss/test/public.keystore";
  private String password = "f00bar";

  private String tempDirPath;

  MyPluginManager mgr;

  @Before
  public void setUp() throws Exception {
    super.setUp();

    tempDirPath = getTempDir().getAbsolutePath() + File.separator;

    // Prepare the loadable plugin directory property, which is
    // created by mgr.startService()
    Properties p = new Properties();
    p.setProperty(ConfigManager.PARAM_PLATFORM_DISK_SPACE_LIST, tempDirPath);
    p.setProperty(PluginManager.PARAM_PLUGIN_LOCATION, "plugins");
    ConfigurationUtil.setCurrentConfigFromProps(p);

    theDaemon = (MyMockLockssDaemon)getMockLockssDaemon();

    theDaemon.setUpAuConfig();

    mgr = new MyPluginManager();
    theDaemon.setPluginManager(mgr);
    theDaemon.setDaemonInited(true);

    UrlManager uMgr = new UrlManager();
    uMgr.initService(theDaemon);
    uMgr.startService();

    RepositoryManager repoMgr = theDaemon.getRepositoryManager();
    repoMgr.startService();

    mgr.setLoadablePluginsReady(true);
    mgr.initService(theDaemon);
  }

  @After
  public void tearDown() throws Exception {
    mgr.stopService();
    theDaemon.stopDaemon();
    super.tearDown();
  }

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    broker = JMSManager.createBroker(JMSManager.DEFAULT_BROKER_URI);
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    if (broker != null) {
      TimerUtil.sleep(1000);
      broker.stop();
    }
  }

  protected MockLockssDaemon newMockLockssDaemon() {
    return new MyMockLockssDaemon();
  }


  private void doConfigAus() throws Exception {
    Map<String, String> auConfig = new HashMap<>();
    auConfig.put(MockPlugin.CONFIG_PROP_1, "val1");
    auConfig.put(MockPlugin.CONFIG_PROP_2, "val2");
    AuConfiguration auConfiguration = new AuConfiguration(mauauid1, auConfig);
    theDaemon.getConfigManager()
    .storeArchivalUnitConfiguration(auConfiguration);
    mgr.ensurePluginLoaded(mockPlugKey);
    MockPlugin mpi = (MockPlugin)mgr.getPlugin(mockPlugKey);
    Configuration configuration =
	AuConfigurationUtils.toUnprefixedConfiguration(auConfiguration);
    mgr.createAu(mpi, configuration, AuEvent.model(AuEvent.Type.Create));

    auConfig = new HashMap<>();
    auConfig.put(MockPlugin.CONFIG_PROP_1, "val1");
    auConfig.put(MockPlugin.CONFIG_PROP_2, "va.l3");
    auConfiguration = new AuConfiguration(mauauid2, auConfig);
    theDaemon.getConfigManager()
    .storeArchivalUnitConfiguration(auConfiguration);
    configuration =
	AuConfigurationUtils.toUnprefixedConfiguration(auConfiguration);
    mgr.createAu(mpi, configuration, AuEvent.model(AuEvent.Type.Create));
    doConfig(new Properties());
  }

  private void doConfig(Properties p) throws Exception {
    // String tempDirPath = getTempDir().getAbsolutePath() + File.separator;
    p.setProperty(ConfigManager.PARAM_PLATFORM_DISK_SPACE_LIST, tempDirPath);
    ConfigurationUtil.addFromProps(p);
  }

  private void minimalConfig() throws Exception {
    // String tempDirPath = getTempDir().getAbsolutePath() + File.separator;
    ConfigurationUtil.addFromArgs(ConfigManager.PARAM_PLATFORM_DISK_SPACE_LIST,
				  tempDirPath);
  }

  // This test disabled because there's currently no privision for
  // disabling the (static) URLConnection cache, and the order in which
  // which test methods run is now unpredictable.
  public void xxxtestDefaultDisableURLConnCache() throws IOException {
    ConfigurationUtil.addFromArgs(PluginManager.PARAM_DISABLE_URL_CONNECTION_CACHE,
	"false");
    assertFalse(PluginManager.DEFAULT_DISABLE_URL_CONNECTION_CACHE);
    mgr.startService();
    URLConnection bar = new URL("http://foo.com/").openConnection();
    assertTrue(bar.getDefaultUseCaches());
  }

  @Test
  public void testDisableURLConnCache() throws IOException {
    ConfigurationUtil.addFromArgs(PluginManager.PARAM_DISABLE_URL_CONNECTION_CACHE,
				  "true");
    mgr.startService();
    URLConnection bar = new URL("http://foo.com/").openConnection();
    assertFalse(bar.getDefaultUseCaches());
  }

  @Test
  public void testNameFromKey() {
    assertEquals("org.lockss.Foo", PluginManager.pluginNameFromKey("org|lockss|Foo"));
  }

  @Test
  public void testKeyFromName() {
    assertEquals("org|lockss|Foo", PluginManager.pluginKeyFromName("org.lockss.Foo"));
  }

  @Test
  public void testKeyFromId() {
    assertEquals("org|lockss|Foo", PluginManager.pluginKeyFromId("org.lockss.Foo"));
  }

  @Test
  public void testGetPreferredPluginType() throws Exception {
    mgr.startService();
    // Prefer XML plugins.
    ConfigurationUtil.addFromArgs(PluginManager.PARAM_PREFERRED_PLUGIN_TYPE,
				  "xml");
    assertEquals(PluginManager.PREFER_XML_PLUGIN,
		 mgr.getPreferredPluginType());

    // Prefer CLASS plugins.
    ConfigurationUtil.addFromArgs(PluginManager.PARAM_PREFERRED_PLUGIN_TYPE,
				  "class");
    assertEquals(PluginManager.PREFER_CLASS_PLUGIN,
		 mgr.getPreferredPluginType());

    // Illegal type.
    ConfigurationUtil.addFromArgs(PluginManager.PARAM_PREFERRED_PLUGIN_TYPE,
				  "foo");
    assertEquals(PluginManager.PREFER_XML_PLUGIN,
		 mgr.getPreferredPluginType());

    // No type specified.
    ConfigurationUtil.addFromArgs("foo", "bar");
    assertEquals(PluginManager.PREFER_XML_PLUGIN,
		 mgr.getPreferredPluginType());
  }

  @Test
  public void testEnsurePluginLoaded() throws Exception {
    mgr.startService();
    // non-existent class shouldn't load
    String key = "org|lockss|NoSuchClass";
    // OK if this logs FileNotFoundException in the log.
    assertFalse(mgr.ensurePluginLoaded(key));
    assertNull(mgr.getPlugin(key));
    // MockPlugin should load
    assertTrue(mgr.ensurePluginLoaded(mockPlugKey));
    Plugin p = mgr.getPlugin(mockPlugKey);
    assertTrue(p.toString(), p instanceof MockPlugin);
    MockPlugin mpi = (MockPlugin)mgr.getPlugin(mockPlugKey);
    assertNotNull(mpi);
    assertEquals(1, mpi.getInitCtr());	// should have been inited once

    // second time shouldn't reload, reinstantiate, or reinitialize plugin
    assertTrue(mgr.ensurePluginLoaded(mockPlugKey));
    MockPlugin mpi2 = (MockPlugin)mgr.getPlugin(mockPlugKey);
    assertSame(mpi, mpi2);
    assertEquals(1, mpi.getInitCtr());
  }

  void setDaemonVersion(String ver) {
    mgr.setDaemonVersion(ver == null ? null : new DaemonVersion(ver));
  }

  @Test
  public void testEnsurePluginLoadedCheckDaemonVersion()
      throws Exception {
    mgr.startService();
    String key = PluginManager.pluginKeyFromName(VerPlugin.class.getName());
    // with insufficient daemon version,
    setDaemonVersion("1.1.1");
    // plugin requiring 1.10.0 should not load
    assertFalse(mgr.ensurePluginLoaded(key));
    // with sufficient daemon version,
    setDaemonVersion("11.1");
    // it should load.
    assertTrue(mgr.ensurePluginLoaded(key));
  }

  static class APlugin extends MockPlugin {
    private List initArgs = new ArrayList();

    public void initPlugin(LockssDaemon daemon) {
      initArgs.add(daemon);
      super.initPlugin(daemon);
    }

    List getInitArgs() {
      return initArgs;
    }
  }


  @Test
  public void testLoadBuiltinPlugin() throws Exception {
    mgr.startService();
    // Non-plugin class shouldn't load
    log.info("Expect ClassCastException");
    assertNull(mgr.loadBuiltinPlugin(String.class));
    Plugin plug = mgr.loadBuiltinPlugin(APlugin.class);
    assertTrue(plug instanceof APlugin);
    assertSame(plug, mgr.loadBuiltinPlugin(APlugin.class));
    assertEquals(1, ((APlugin)plug).getInitArgs().size());
  }

  @Test
  public void testInitPluginRegistry() {
    mgr.startService();
    String n1 = "org.lockss.test.MockPlugin";
    String n2 = ThrowingMockPlugin.class.getName();
    assertEmpty(mgr.getRegisteredPlugins());
    Properties p = new Properties();
    p.setProperty(PluginManager.PARAM_PLUGIN_REGISTRY, n1 + ";" + n2);
    ConfigurationUtil.addFromProps(p);
    Plugin p1 = mgr.getPlugin(PluginManager.pluginKeyFromName(n1));
    assertNotNull(p1);
    assertTrue(p1.toString(), p1 instanceof MockPlugin);
    Plugin p2 = mgr.getPlugin(PluginManager.pluginKeyFromName(n2));
    assertNotNull(p2);
    assertTrue(p2.toString(), p2 instanceof ThrowingMockPlugin);
    assertEquals(SetUtil.set(p1, p2),
		 SetUtil.theSet(mgr.getRegisteredPlugins()));
    p.setProperty(PluginManager.PARAM_PLUGIN_REGISTRY, n1);
    ConfigurationUtil.addFromProps(p);
    assertEquals(SetUtil.set(p1, p2),
		 SetUtil.theSet(mgr.getRegisteredPlugins()));
    p.setProperty(PluginManager.PARAM_PLUGIN_REGISTRY, n1 + ";" + n2);
    ConfigurationUtil.addFromProps(p);
    assertEquals(SetUtil.set(p1, p2),
		 SetUtil.theSet(mgr.getRegisteredPlugins()));
    p.setProperty(PluginManager.PARAM_PLUGIN_REGISTRY, n1 + ";" + n2);
    p.setProperty(PluginManager.PARAM_PLUGIN_RETRACT, n2);
    ConfigurationUtil.addFromProps(p);
    assertEquals(SetUtil.set(p1),
		 SetUtil.theSet(mgr.getRegisteredPlugins()));
    assertNull(mgr.getPlugin(PluginManager.pluginKeyFromName(n2)));
    assertSame(p1, mgr.getPlugin(PluginManager.pluginKeyFromName(n1)));
    p.setProperty(PluginManager.PARAM_PLUGIN_REGISTRY, n1 + ";" + n2);
    p.setProperty(PluginManager.PARAM_PLUGIN_RETRACT, "");
    ConfigurationUtil.addFromProps(p);
    p2 = mgr.getPlugin(PluginManager.pluginKeyFromName(n2));
    assertNotNull(p2);
    assertTrue(p2.toString(), p2 instanceof ThrowingMockPlugin);
  }

  @Test
  public void testEnsurePluginLoadedXml() throws Exception {
    mgr.startService();
    String pname = "org.lockss.test.TestXmlPlugin";
    String key = PluginManager.pluginKeyFromId(pname);
    assertTrue(mgr.ensurePluginLoaded(key));
    Plugin p = mgr.getPlugin(key);
    assertTrue(p.toString(), p instanceof DefinablePlugin);
    MyDefinablePlugin mcpi = (MyDefinablePlugin)mgr.getPlugin(key);
    assertNotNull(mcpi);
    List initArgs = mcpi.getInitArgs();
    assertEquals(1, initArgs.size());
    List args = (List)initArgs.get(0);
    assertEquals(3, args.size());
    assertEquals(pname, args.get(1));
  }

  @Test
  public void testEnsurePluginLoadedXmlCheckDaemonVersion()
      throws Exception {
    mgr.startService();
    // with insufficient daemon version,
    setDaemonVersion("1.1.1");
    // plugin requiring 1.10.0 should not load
    String pname = "org.lockss.test.TestXmlPlugin";
    String key = PluginManager.pluginKeyFromId(pname);
    assertFalse(mgr.ensurePluginLoaded(key));
    // with sufficient daemon version,
    setDaemonVersion("11.1");
    // it should load.
    assertTrue(mgr.ensurePluginLoaded(key));
  }

  @Test
  public void testStop() throws Exception {
    mgr.startService();
    doConfigAus();
    MockPlugin mpi = (MockPlugin)mgr.getPlugin(mockPlugKey);
    assertEquals(0, mpi.getStopCtr());
    mgr.stopService();
    assertEquals(1, mpi.getStopCtr());
  }

  @Test
  public void testAuConfig() throws Exception {
    mgr.startService();
    doConfigAus();
    MockPlugin mpi = (MockPlugin)mgr.getPlugin(mockPlugKey);
    // plugin should be registered
    assertNotNull(mpi);
    // should have been inited once
    assertEquals(1, mpi.getInitCtr());

    // get the two archival units
    ArchivalUnit au1 = mgr.getAuFromIdIfExists(mauauid1);
    ArchivalUnit au2 = mgr.getAuFromIdIfExists(mauauid2);

    // verify the plugin's set of all AUs is {au1, au2}
    assertEquals(SetUtil.set(au1, au2), new HashSet(mgr.getAllAus()));

    // verify au1's configuration
    assertEquals(mauauid1, au1.getAuId());
    MockArchivalUnit mau1 = (MockArchivalUnit)au1;
    Configuration c1 = mau1.getConfiguration();
    assertEquals("val1", c1.get(MockPlugin.CONFIG_PROP_1));
    assertEquals("val2", c1.get(MockPlugin.CONFIG_PROP_2));

    // verify au1's configuration
    assertEquals(mauauid2, au2.getAuId());
    MockArchivalUnit mau2 = (MockArchivalUnit)au2;
    Configuration c2 = mau2.getConfiguration();
    assertEquals("val1", c2.get(MockPlugin.CONFIG_PROP_1));
    assertEquals("va.l3", c2.get(MockPlugin.CONFIG_PROP_2));

    assertEquals(au1, mgr.getAuFromIdIfExists(mauauid1));
  }

  @Test
  public void testAuConfigWithGlobalEntryForNonExistentAU() throws Exception {
    mgr.startService();
    doConfigAus();
    Properties p = new Properties();
    p.setProperty(p1a3param+BaseArchivalUnit.KEY_NEW_CONTENT_CRAWL_INTERVAL, "2d");
    doConfig(p);
    MockPlugin mpi = (MockPlugin)mgr.getPlugin(mockPlugKey);
    // plugin should be registered
    assertNotNull(mpi);
    // should have been inited once
    assertEquals(1, mpi.getInitCtr());

    // get the two archival units
    ArchivalUnit au1 = mgr.getAuFromIdIfExists(mauauid1);
    ArchivalUnit au2 = mgr.getAuFromIdIfExists(mauauid2);

    assertNotNull("AU1 didn't get created", au1);
    assertNotNull("AU2 didn't get created", au2);

    // verify the plugin's set of all AUs is {au1, au2}
    assertEquals(SetUtil.set(au1, au2), new HashSet(mgr.getAllAus()));

    // verify au1's configuration
    assertEquals(mauauid1, au1.getAuId());
    MockArchivalUnit mau1 = (MockArchivalUnit)au1;
    Configuration c1 = mau1.getConfiguration();
    assertEquals("val1", c1.get(MockPlugin.CONFIG_PROP_1));
    assertEquals("val2", c1.get(MockPlugin.CONFIG_PROP_2));

    // verify au1's configuration
    assertEquals(mauauid2, au2.getAuId());
    MockArchivalUnit mau2 = (MockArchivalUnit)au2;
    Configuration c2 = mau2.getConfiguration();
    assertEquals("val1", c2.get(MockPlugin.CONFIG_PROP_1));
    assertEquals("va.l3", c2.get(MockPlugin.CONFIG_PROP_2));

    assertEquals(au1, mgr.getAuFromIdIfExists(mauauid1));
  }

  List createEvents = new ArrayList();
  List deleteEvents = new ArrayList();
  List reconfigEvents = new ArrayList();
  List auChangeEvents = new ArrayList();

  class MyAuEventHandler extends AuEventHandler.Base {
    @Override public void auCreated(AuEvent event, ArchivalUnit au) {
      createEvents.add(au);
    }
    @Override public void auDeleted(AuEvent event, ArchivalUnit au) {
      deleteEvents.add(au);
    }
    @Override public void auReconfigured(AuEvent event, ArchivalUnit au,
					 Configuration oldAuConf) {
      reconfigEvents.add(ListUtil.list(au, oldAuConf));
    }
    @Override public void auContentChanged(AuEvent event, ArchivalUnit au,
					   AuEvent.ContentChangeInfo info) {
      auChangeEvents.add(ListUtil.list(au, info));
    }
  }

  class MyAbsentAuEventHandler extends AuEventHandler.Base {
    @Override public void auCreated(AuEvent event, String auid,
				    ArchivalUnit au) {
      createEvents.add(auid);
    }
    @Override public void auDeleted(AuEvent event, String auid,
				    ArchivalUnit au) {
      deleteEvents.add(auid);
    }
    @Override public void auReconfigured(AuEvent event, String auid,
					 ArchivalUnit au,
					 Configuration oldAuConf) {
      reconfigEvents.add(ListUtil.list(auid, oldAuConf));
    }
    @Override public void auContentChanged(AuEvent event, String auid,
					   ArchivalUnit au,
					   AuEvent.ContentChangeInfo info) {
      auChangeEvents.add(ListUtil.list(auid, info));
    }
  }

  @Test
  public void testAbsentAuEvent() throws Exception {
    ConfigurationUtil.addFromArgs(PluginManager.PARAM_ENABLE_JMS_NOTIFICATIONS,
				  "true");
    mgr.startService();
    minimalConfig();
    // Don't use the connection maintained by JMSManager in case
    // PluginManager's Consumer ignores locally sent messages
    JMSManager jmsMgr =
      getMockLockssDaemon().getManagerByType(JMSManager.class);
    ConnectionFactory connectionFactory =
      new ActiveMQConnectionFactory(jmsMgr.getConnectUri());
    Connection conn = connectionFactory.createConnection();
    conn.start();
    JmsFactory fact = jmsMgr.getJmsFactory();
    JmsProducer prod =
      fact.createTopicProducer(null,
			       PluginManager.DEFAULT_JMS_NOTIFICATION_TOPIC,
			       conn);
    mgr.registerAuEventHandler(new MyAbsentAuEventHandler());
    AuEvent.ContentChangeInfo chInfo = new AuEvent.ContentChangeInfo()
      .setType(AuEvent.ContentChangeInfo.Type.Crawl)
      .setNumUrls(10)
      .setComplete(true);
    AuEvent event =
      AuEvent.forAuId("auid1",
		      AuEvent.Type.ContentChanged).setChangeInfo(chInfo);
    prod.sendMap(event.toMap());
    TimerUtil.sleep(1000);
    assertEquals(ListUtil.list(ListUtil.list("auid1", chInfo)), auChangeEvents);
  }


  @Test
  public void testCreateAu() throws Exception {
    mgr.startService();
    minimalConfig();
    String pid = new ThrowingMockPlugin().getPluginId();
    String key = PluginManager.pluginKeyFromId(pid);
    assertTrue(mgr.ensurePluginLoaded(key));
    ThrowingMockPlugin mpi = (ThrowingMockPlugin)mgr.getPlugin(key);
    assertNotNull(mpi);
    mgr.registerAuEventHandler(new MyAuEventHandler());
    Configuration config = ConfigurationUtil.fromArgs("a", "b");
    ArchivalUnit au = mgr.createAu(mpi, config,
                                   AuEvent.model(AuEvent.Type.Create));

    // verify put in PluginManager map
    String auid = au.getAuId();
    ArchivalUnit aux = mgr.getAuFromIdIfExists(auid);
    assertSame(au, aux);

    // verify got right config
    Configuration auConfig = au.getConfiguration();
    assertEquals("b", auConfig.get("a"));
    assertEquals(1, auConfig.keySet().size());
    assertEquals(mpi, au.getPlugin());

    // verify event handler run
    assertEquals(ListUtil.list(au), createEvents);
    assertEmpty(deleteEvents);
    assertEmpty(reconfigEvents);

    // verify turns RuntimeException into ArchivalUnit.ConfigurationException
    mpi.setCfgEx(new ArchivalUnit.ConfigurationException("should be thrown"));
    try {
      ArchivalUnit au2 = mgr.createAu(mpi, config,
                                      AuEvent.model(AuEvent.Type.Create));
      fail("createAU should have thrown ArchivalUnit.ConfigurationException");
    } catch (ArchivalUnit.ConfigurationException e) {
    }

    mpi.setRtEx(new ExpectedRuntimeException("Ok if in log"));
    try {
      ArchivalUnit au2 = mgr.createAu(mpi, config,
                                      AuEvent.model(AuEvent.Type.Create));
      fail("createAU should have thrown ArchivalUnit.ConfigurationException");
    } catch (ArchivalUnit.ConfigurationException e) {
      // this is what's expected
    } catch (RuntimeException e) {
      fail("createAU threw RuntimeException", e);
    }

  }

  @Test
  public void testConfigureAu() throws Exception {
    mgr.startService();
    minimalConfig();
    String pid = new ThrowingMockPlugin().getPluginId();
    String key = PluginManager.pluginKeyFromId(pid);
    assertTrue(mgr.ensurePluginLoaded(key));
    ThrowingMockPlugin mpi = (ThrowingMockPlugin)mgr.getPlugin(key);
    assertNotNull(mpi);
    mgr.registerAuEventHandler(new MyAuEventHandler());
    Configuration config = ConfigurationUtil.fromArgs("a", "b");
    ArchivalUnit au = mgr.createAu(mpi, config,
                                   AuEvent.model(AuEvent.Type.Create));

    String auid = au.getAuId();
    ArchivalUnit aux = mgr.getAuFromIdIfExists(auid);
    assertSame(au, aux);

    // verify event handler run
    assertEquals(ListUtil.list(au), createEvents);
    assertEmpty(deleteEvents);
    assertEmpty(reconfigEvents);

    // verify can reconfig
    mgr.configureAu(mpi, ConfigurationUtil.fromArgs("a", "c"), auid);
    Configuration auConfig = au.getConfiguration();
    assertEquals("c", auConfig.get("a"));
    assertEquals(1, auConfig.keySet().size());

    // verify event handler run
    assertEquals(ListUtil.list(au), createEvents);
    assertEmpty(deleteEvents);
    assertEquals(ListUtil.list(ListUtil.list(au, config)), reconfigEvents);

    // verify turns RuntimeException into ArchivalUnit.ConfigurationException
    mpi.setCfgEx(new ArchivalUnit.ConfigurationException("should be thrown"));
    try {
      mgr.configureAu(mpi, config, auid);
      fail("configureAU should have thrown ArchivalUnit.ConfigurationException");
    } catch (ArchivalUnit.ConfigurationException e) {
    }

    mpi.setRtEx(new ExpectedRuntimeException("Ok if in log"));
    try {
      mgr.configureAu(mpi, config, auid);
      fail("configureAU should have thrown ArchivalUnit.ConfigurationException");
    } catch (ArchivalUnit.ConfigurationException e) {
      // this is what's expected
    } catch (RuntimeException e) {
      fail("createAU threw RuntimeException", e);
    }

  }

  @Test
  public void testConfigureAuWithBogusAuid() throws Exception {
    mgr.startService();
    minimalConfig();
    String pid = new ThrowingMockPlugin().getPluginId();
    String key = PluginManager.pluginKeyFromId(pid);
    assertTrue(mgr.ensurePluginLoaded(key));
    ThrowingMockPlugin mpi = (ThrowingMockPlugin)mgr.getPlugin(key);
    assertNotNull(mpi);
    // verify doesn't get created if auid doesn't match config
    try {
      mgr.configureAu(mpi, ConfigurationUtil.fromArgs("a", "c"), "bogos_auid");
      fail("Should have thrown ConfigurationException");
    } catch (ArchivalUnit.ConfigurationException e) {
      log.debug(e.toString());
    }
    assertEmpty(theDaemon.getAuMgrsStarted());
  }

  @Test
  public void testDeactivateAu() throws Exception {
    mgr.startService();
    minimalConfig();
    String pid = new ThrowingMockPlugin().getPluginId();
    String key = PluginManager.pluginKeyFromId(pid);

    assertTrue(mgr.ensurePluginLoaded(key));
    ThrowingMockPlugin mpi = (ThrowingMockPlugin)mgr.getPlugin(key);
    assertNotNull(mpi);
    Configuration config = ConfigurationUtil.fromArgs("a", "b");
    ArchivalUnit au = mgr.createAu(mpi, config,
                                   AuEvent.model(AuEvent.Type.Create));
    assertNotNull(au);
    assertTrue(mgr.isActiveAu(au));
    String auId = au.getAuId();
    mgr.configureAu(mpi, config, auId);

    // should not throw.
    try {
      assertFalse(mgr.getInactiveAuIds().contains(auId));
      mgr.registerAuEventHandler(new MyAuEventHandler());
      mgr.deactivateAu(au);
      assertTrue(mgr.getInactiveAuIds().contains(auId));
      assertTrue(mgr.isInactiveAuId(auId));
      assertFalse(mgr.isActiveAu(au));
      // verify event handler run
      assertEmpty(createEvents);
      assertEquals(ListUtil.list(au), deleteEvents);
      assertEmpty(reconfigEvents);
    } catch (Exception ex) {
      fail("Deactivating au should not have thrown", ex);
    }
    // Recreate the AU, will get a new instance
    ArchivalUnit au2 = mgr.createAu(mpi, config,
                                    AuEvent.model(AuEvent.Type.Create));
    assertNotNull(au2);
    assertFalse(mgr.isActiveAu(au));
    assertTrue(mgr.isActiveAu(au2));
    assertFalse(mgr.isInactiveAuId(auId));
  }

  @Test
  public void testCreateAndSaveAndDeleteAuConfiguration() throws Exception {
    mgr.startService();
    minimalConfig();
    String pid = new ThrowingMockPlugin().getPluginId();
    String key = PluginManager.pluginKeyFromId(pid);

    assertTrue(mgr.ensurePluginLoaded(key));
    ThrowingMockPlugin mpi = (ThrowingMockPlugin)mgr.getPlugin(key);
    assertNotNull(mpi);

    Properties props = new Properties();
    props.put("a", "b");

    // Test creating and deleting by au reference.
    ArchivalUnit au1 = mgr.createAndSaveAuConfiguration(mpi, props);
    assertNotNull(au1);
    try {
      mgr.deleteAuConfiguration(au1);
    } catch (Exception e) {
      fail("Deleting au config by AU reference should not have thrown", e);
    }

    // Test creating and deleting by au ID.
    props.put("a", "c");
    ArchivalUnit au2 = mgr.createAndSaveAuConfiguration(mpi, props);
    assertNotNull(au2);
    try {
      mgr.deleteAuConfiguration(au2.getAuId());
    } catch (Exception e) {
      fail("Deleting au config by AU ID should not have thrown", e);
    }

    // Test setAndSaveAuConfiguration
    props.put("a", "d");
    ArchivalUnit au3 = mgr.createAu(mpi,
				    ConfigurationUtil.fromArgs("foo", "bar"),
				    AuEvent.model(AuEvent.Type.Create));
    try {
      mgr.setAndSaveAuConfiguration(au3, props);

      mgr.deleteAu(au3);
    } catch (Exception e) {
      fail("Deleting AU should not have thrown", e);
    }
  }


  private void onDemandSetup() throws Exception {
    ConfigurationUtil.addFromArgs(LockssApp.PARAM_START_PLUGINS, "true");
    ConfigurationUtil.addFromArgs(PluginManager.PARAM_START_ALL_AUS, "false");
    mgr.startService();
    minimalConfig();
    createTitleConfigs();
    cod_mpi = (MockPlugin)mgr.getPlugin(mockPlugKey);
  }

  private void startAllSetup() throws Exception {
    ConfigurationUtil.addFromArgs(LockssApp.PARAM_START_PLUGINS, "true");
    ConfigurationUtil.addFromArgs(PluginManager.PARAM_START_ALL_AUS, "true");
    mgr.startService();
    minimalConfig();
    createTitleConfigs();
    cod_mpi = (MockPlugin)mgr.getPlugin(mockPlugKey);
  }

  @Test
  public void testCreateAusAtStartup() throws Exception {
    startAllSetup();
    // Store two AU configs in DB
    String auid1 = cod_tc1.getAuId(mgr);
    Configuration auc1 = cod_tc1.getConfig();
    mgr.updateAuInDatabase(auid1, auc1);
    String auid2 = cod_tc2.getAuId(mgr);
    Configuration auc2 = cod_tc2.getConfig();
    mgr.updateAuInDatabase(auid2, auc2);
    assertNull(mgr.getAuFromIdIfExists(auid1));
    assertNull(mgr.getAuFromIdIfExists(auid2));
    // Kick off process that starts AUs
    mgr.setLoadablePluginsReady(false);
    mgr.startLoadablePlugins();
    // Ensure that AUs now exist
    ArchivalUnit au1 = mgr.getAuFromIdIfExists(auid1);
    assertNotNull(au1);
    assertSame(cod_mpi, au1.getPlugin());
    assertEquals(cod_tc1.getConfig(), au1.getConfiguration());
    ArchivalUnit au2 = mgr.getAuFromIdIfExists(auid2);
    assertNotNull(au2);
    assertSame(cod_mpi, au2.getPlugin());
    assertEquals(cod_tc2.getConfig(), au2.getConfiguration());
  }

  @Test
  public void testDontCreateAusAtStartup() throws Exception {
    onDemandSetup();
    String auid1 = cod_tc1.getAuId(mgr);
    Configuration auc1 = cod_tc1.getConfig();
    mgr.updateAuInDatabase(auid1, auc1);
    String auid2 = cod_tc2.getAuId(mgr);
    Configuration auc2 = cod_tc2.getConfig();
    mgr.updateAuInDatabase(auid2, auc2);
    assertNull(mgr.getAuFromIdIfExists(auid1));
    assertNull(mgr.getAuFromIdIfExists(auid2));
    mgr.setLoadablePluginsReady(false);
    mgr.startLoadablePlugins();
    assertNull(mgr.getAuFromIdIfExists(auid1));
    assertNull(mgr.getAuFromIdIfExists(auid2));
  }

  // Tests on-demand creation from info in tdb.  Also tests deletion and
  // that created AuEvent is sent
  @Test
  public void testCreateOnDemandAuFromTdb() throws Exception {
    onDemandSetup();
    installTitleConfigs();
    mgr.registerAuEventHandler(new MyAuEventHandler());

    assertEquals(2, cod_tc1.getConfig().keySet().size());
    assertEquals(3, cod_tc2.getConfig().keySet().size());

    String auid1 = cod_tc1.getAuId(mgr);
    String auid2 = cod_tc2.getAuId(mgr);
    assertNull(mgr.getAuFromIdIfExists(auid1));
    ArchivalUnit au1 = mgr.getAuFromId(auid1);
    assertNotNull(au1);
    assertSame(cod_mpi, au1.getPlugin());
    assertEquals(cod_tc1.getConfig(), au1.getConfiguration());

    assertNull(mgr.getAuFromIdIfExists(auid2));
    ArchivalUnit au2 = mgr.getAuFromId(auid2);
    assertNotNull(au2);
    assertSame(cod_mpi, au2.getPlugin());
    assertEquals(cod_tc2.getConfig(), au2.getConfiguration());

    // verify created event was sent
    assertEquals(ListUtil.list(au1, au2), createEvents);
    assertEmpty(deleteEvents);
    assertEmpty(reconfigEvents);

    // Stop it, recreate it, ensure we get a new instance
    mgr.stopAu(au1, AuEvent.forAu(au1, AuEvent.Type.Deactivate));
    assertNull(mgr.getAuFromIdIfExists(auid1));
    ArchivalUnit au1a = mgr.getAuFromId(auid1);
    assertNotNull(au1a);
    assertSame(cod_mpi, au1a.getPlugin());
    assertEquals(cod_tc1.getConfig(), au1a.getConfiguration());
    assertNotSame(au1, au1a);

    assertEquals(ListUtil.list(au1, au2, au1a), createEvents);
    assertEquals(ListUtil.list(au1), deleteEvents);
    assertEmpty(reconfigEvents);
  }

  @Test
  public void testCreateOnDemandAuFromConfig() throws Exception {
    onDemandSetup();
    String auid1 = cod_tc1.getAuId(mgr);
    Configuration auc1 = cod_tc1.getConfig();
    mgr.updateAuInDatabase(auid1, auc1);
    String auid2 = cod_tc2.getAuId(mgr);
    Configuration auc2 = cod_tc2.getConfig();
    mgr.updateAuInDatabase(auid2, auc2);

    assertNull(mgr.getAuFromIdIfExists(auid1));
    ArchivalUnit au1 = mgr.getAuFromId(auid1);
    assertNotNull(au1);
    assertSame(cod_mpi, au1.getPlugin());
    assertEquals(cod_tc1.getConfig(), au1.getConfiguration());

    assertNull(mgr.getAuFromIdIfExists(auid2));
    ArchivalUnit au2 = mgr.getAuFromId(auid2);
    assertNotNull(au2);
    assertSame(cod_mpi, au2.getPlugin());
    assertEquals(cod_tc2.getConfig(), au2.getConfiguration());
  }

  @Test
  public void testCreateOnDemandAuFromAuId() throws Exception {
    onDemandSetup();
    String auid1 = cod_tc1.getAuId(mgr);
    String auid2 = cod_tc2.getAuId(mgr);

    assertNull(mgr.getAuFromIdIfExists(auid1));
    ArchivalUnit au1 = mgr.getAuFromId(auid1);
    assertNotNull(au1);
    assertSame(cod_mpi, au1.getPlugin());
    assertEquals(cod_tc1.getConfig(), au1.getConfiguration());

    assertNull(mgr.getAuFromIdIfExists(auid2));
    // cod_tc2 has a non-def param, which will not end up in the AU's config
    // when created by auid
    ArchivalUnit au2 = mgr.getAuFromId(auid2);
    assertNotNull(au2);
    assertSame(cod_mpi, au2.getPlugin());
    Configuration tc2config = cod_tc2.getConfig().copy();
    assertNotEquals(tc2config, au2.getConfiguration());
    assertEquals("barbar", tc2config.get("nondefp1"));
    tc2config.remove("nondefp1");
    assertEquals(tc2config, au2.getConfiguration());
  }

  @Test
  public void testCreateOnDemandAuFromAuIdDisabled() throws Exception {
    onDemandSetup();
    ConfigurationUtil.addFromArgs(PluginManager.PARAM_INFER_CONFIG_FROM_AUID,
				  "false");
    String auid1 = cod_tc1.getAuId(mgr);
    ArchivalUnit au1 = mgr.getAuFromId(auid1);
    assertNull(au1);
    ConfigurationUtil.addFromArgs(PluginManager.PARAM_INFER_CONFIG_FROM_AUID,
				  "true");
    au1 = mgr.getAuFromId(auid1);
    assertNotNull(au1);
  }

  @Test
  public void testNotifyAuChanged() throws Exception {
    startAllSetup();
    // Store two AU configs in DB
    String auid1 = cod_tc1.getAuId(mgr);
    Configuration auc1 = cod_tc1.getConfig();
    mgr.updateAuInDatabase(auid1, auc1);
    String auid2 = cod_tc2.getAuId(mgr);
    Configuration auc2 = cod_tc2.getConfig();
    mgr.updateAuInDatabase(auid2, auc2);
    assertNull(mgr.getAuFromIdIfExists(auid1));
    assertNull(mgr.getAuFromIdIfExists(auid2));

    // Notification should be ignored in ondemand mode
    ConfigurationUtil.addFromArgs(PluginManager.PARAM_START_ALL_AUS, "false");
    mgr.auConfigChanged(auid1);
    assertNull(mgr.getAuFromIdIfExists(auid1));

    // Return to startAll mode
    ConfigurationUtil.addFromArgs(PluginManager.PARAM_START_ALL_AUS, "true");
    mgr.auConfigChanged(auid1);
    mgr.auConfigChanged(auid2);
    ArchivalUnit au1 = mgr.getAuFromIdIfExists(auid1);
    assertNotNull(au1);
    assertSame(cod_mpi, au1.getPlugin());
    assertEquals(cod_tc1.getConfig(), au1.getConfiguration());

    ArchivalUnit au2 = mgr.getAuFromIdIfExists(auid2);
    assertNotNull(au2);
    assertSame(cod_mpi, au2.getPlugin());
    assertEquals(cod_tc2.getConfig(), au2.getConfiguration());

    auc1.put(PluginManager.AU_PARAM_DISABLED, "true");
    mgr.updateAuInDatabase(auid1, auc1);
    mgr.auConfigChanged(auid1);
    assertNull(mgr.getAuFromIdIfExists(auid1));

    assertNotNull(mgr.getAuFromIdIfExists(auid2));
    mgr.auConfigRemoved(auid2);
    assertNull(mgr.getAuFromIdIfExists(auid2));
  }


  // TitleConfig setup for CreateOnDemand tests

  MockPlugin cod_mpi;
  TitleConfig cod_tc1, cod_tc2;

  public void createTitleConfigs() {
    mgr.ensurePluginLoaded(mockPlugKey);
    mgr.ensurePluginLoaded(mockPlugKey);
    ConfigParamDescr d1 = new ConfigParamDescr(MockPlugin.CONFIG_PROP_1);
    ConfigParamDescr d2 = new ConfigParamDescr(MockPlugin.CONFIG_PROP_2);
    ConfigParamDescr d3 = new ConfigParamDescr("nondefp1");
    cod_tc1 = new TitleConfig("title1", mockPlugKey);
    cod_tc2 = new TitleConfig("title2", mockPlugKey);
    cod_tc1.setParams(ListUtil.list(new ConfigParamAssignment(d1, "a"),
				new ConfigParamAssignment(d2, "foo")));
    cod_tc2.setParams(ListUtil.list(new ConfigParamAssignment(d1, "a2"),
				new ConfigParamAssignment(d2, "foo"),
				new ConfigParamAssignment(d3, "barbar")));
  }

  public void installTitleConfigs() {
    cod_mpi.setTitleConfigMap(MapUtil.map("title 1", cod_tc1, "title 2", cod_tc2),
			  MapUtil.map(cod_tc1.getAuId(mgr), cod_tc1,
				      cod_tc2.getAuId(mgr), cod_tc2));
  }


  // ensure getAllAus() returns AUs in title sorted order
  @Test
  public void testGetAllAus() throws Exception {
    mgr.startService();
    MockArchivalUnit mau1 = new MockArchivalUnit();
    MockArchivalUnit mau2 = new MockArchivalUnit();
    MockArchivalUnit mau3 = new MockArchivalUnit();
    MockArchivalUnit mau4 = new MockArchivalUnit();
    MockArchivalUnit mau5 = new MockArchivalUnit();
    // make sure it sorts by name, not auid
    mau1.setName("foobarm1"); mau1.setAuId("5");
    mau2.setName("foom2"); mau2.setAuId("3");
    mau3.setName("gunt"); mau3.setAuId("4");
    mau4.setName("m4"); mau4.setAuId("1");
    mau5.setName("zzyzx"); mau5.setAuId("2");
    mgr.putAuInMap(mau5);
    mgr.putAuInMap(mau4);
    mgr.putAuInMap(mau2);
    mgr.putAuInMap(mau3);
    mgr.putAuInMap(mau1);
    List allAus = mgr.getAllAus();
    assertEquals(ListUtil.list(mau1, mau2, mau3, mau4, mau5), allAus);
    // ensure list is cached
    assertSame(allAus, mgr.getAllAus());

    MockArchivalUnit mau6 = new MockArchivalUnit();
    mau6.setName("Aardvark"); mau6.setAuId("0");
    mgr.putAuInMap(mau6);
    assertNotSame(allAus, mgr.getAllAus());
    assertEquals(ListUtil.list(mau6, mau1, mau2, mau3, mau4, mau5),
		 mgr.getAllAus());
  }

  @Test
  public void testgetRandomizedAus() throws Exception {
    mgr.startService();
    MockArchivalUnit mau1 = new MockArchivalUnit();
    MockArchivalUnit mau2 = new MockArchivalUnit();
    MockArchivalUnit mau3 = new MockArchivalUnit();
    MockArchivalUnit mau4 = new MockArchivalUnit();
    MockArchivalUnit mau5 = new MockArchivalUnit();
    mgr.putAuInMap(mau5);
    mgr.putAuInMap(mau4);
    mgr.putAuInMap(mau2);
    mgr.putAuInMap(mau3);
    mgr.putAuInMap(mau1);
    Set aus = SetUtil.theSet(mgr.getRandomizedAus());
    assertEquals(SetUtil.set(mau1, mau2, mau3, mau4, mau5), aus);
  }

  @Test
  public void testTitleSets() throws Exception {
    mgr.startService();
    String ts1p = PluginManager.PARAM_TITLE_SETS + ".s1.";
    String ts2p = PluginManager.PARAM_TITLE_SETS + ".s2.";
    String title1 = "Title Set 1";
    String title2 = "Set of Titles";
    String path1 = "[journalTitle='Dog Journal']";
    String path2 = "[journalTitle=\"Dog Journal\" or pluginName=\"plug2\"]";
    Properties p = new Properties();
    p.setProperty(ts1p+"class", "xpath");
    p.setProperty(ts1p+"name", title1);
    p.setProperty(ts1p+"xpath", path1);
    p.setProperty(ts2p+"class", "xpath");
    p.setProperty(ts2p+"name", title2);
    p.setProperty(ts2p+"xpath", path2);
    ConfigurationUtil.addFromProps(p);
    Map map = mgr.getTitleSetMap();
    assertEquals(2, map.size());
    assertEquals(TitleSetXpath.create(theDaemon, title1, path1),
		 map.get(title1));
    assertEquals(TitleSetXpath.create(theDaemon, title2, path2),
		 map.get(title2));
  }

  @Test
  public void testTitleSetOrder() throws Exception {
    mgr.startService();
    String ts1p = PluginManager.PARAM_TITLE_SETS + ".s1.";
    String ts2p = PluginManager.PARAM_TITLE_SETS + ".s2.";
    String ts3p = PluginManager.PARAM_TITLE_SETS + ".s3.";
    String title1 = "All Royal Society Publishing Titles";
    String title2 = "All Royal Society of Chemistry Titles";
    String title3 = "All Royal pains in the ass";
    String path1 = "[journalTitle='Dog Journal']";
    Properties p = new Properties();
    p.setProperty(ts1p+"class", "xpath");
    p.setProperty(ts1p+"name", title1);
    p.setProperty(ts1p+"xpath", path1);
    p.setProperty(ts2p+"class", "xpath");
    p.setProperty(ts2p+"name", title2);
    p.setProperty(ts2p+"xpath", path1);
    p.setProperty(ts3p+"class", "xpath");
    p.setProperty(ts3p+"name", title3);
    p.setProperty(ts3p+"xpath", path1);
    ConfigurationUtil.addFromProps(p);
    List<TitleSet> tsets = new ArrayList<TitleSet>(mgr.getTitleSets());
    assertEquals(3, tsets.size());
    assertEquals(title3, tsets.get(0).getName());
    assertEquals(title2, tsets.get(1).getName());
    assertEquals(title1, tsets.get(2).getName());
  }

  @Test
  public void testIllTitleSets() throws Exception {
    mgr.startService();
    String ts1p = PluginManager.PARAM_TITLE_SETS + ".s1.";
    String ts2p = PluginManager.PARAM_TITLE_SETS + ".s2.";
    String title1 = "Title Set 1";
    String title2 = "Set of Titles";
    // illegal xpath
    String path1 = "[journalTitle='Dog Journal']]";
    String path2 = "[journalTitle=\"Dog Journal\" or pluginName=\"plug2\"]";
    Properties p = new Properties();
    p.setProperty(ts1p+"class", "xpath");
    p.setProperty(ts1p+"name", title1);
    p.setProperty(ts1p+"xpath", path1);
    p.setProperty(ts2p+"class", "xpath");
    p.setProperty(ts2p+"name", title2);
    p.setProperty(ts2p+"xpath", path2);
    ConfigurationUtil.addFromProps(p);
    Map map = mgr.getTitleSetMap();
    assertEquals(1, map.size());
    assertEquals(TitleSetXpath.create(theDaemon, title2, path2),
		 map.get(title2));
  }

  static class MyMockLockssDaemon extends MockLockssDaemon {
    List auMgrsStarted = new ArrayList();

    boolean isStartAuManagers = false;

    public void setStartAuManagers(boolean val) {
      isStartAuManagers = true;
    }

    public void startOrReconfigureAuManagers(ArchivalUnit au,
					     Configuration auConfig)
	throws Exception {
      if (isStartAuManagers) {
	reallyStartOrReconfigureAuManagers(au, auConfig);
      }
      auMgrsStarted.add(au);
    }

    List getAuMgrsStarted() {
      return auMgrsStarted;
    }
  }

  static class MyPluginManager extends PluginManager {
    private ArchivalUnit processOneRegistryAuThrowIf = null;
    private String processOneRegistryJarThrowIf = null;
    private DaemonVersion mockDaemonVersion = null;
    private List<ArchivalUnit> regAus = new ArrayList<ArchivalUnit>();
    private List<String> suppressEnxurePluginLoaded;

    @Override
    public boolean ensurePluginLoaded(String pluginKey) {
      if (suppressEnxurePluginLoaded != null
	  && suppressEnxurePluginLoaded.contains(pluginKey)) {
	return false;
      }
      return super.ensurePluginLoaded(pluginKey);
    }

    void suppressEnxurePluginLoaded(List<String> pluginKeys) {
      suppressEnxurePluginLoaded = pluginKeys;
    }

    @Override
    public boolean isRegistryAu(ArchivalUnit au) {
      return (au instanceof MyMockRegistryArchivalUnit)
	|| super.isRegistryAu(au);
    }

    protected String getConfigurablePluginName(String pluginName) {
      return MyDefinablePlugin.class.getName();
    }
    protected void processOneRegistryAu(ArchivalUnit au, Map tmpMap) {
      if (au == processOneRegistryAuThrowIf) {
	throw new ExpectedRuntimeException("fake error for " + au);
      }
      regAus.add(au);
      super.processOneRegistryAu(au, tmpMap);
    }
    List<ArchivalUnit> getProcessedRegAus() {
      return regAus;
    }
    protected void processOneRegistryJar(CachedUrl cu, String url,
					 ArchivalUnit au, Map tmpMap) {
      if (url.equals(processOneRegistryJarThrowIf)) {
	throw new ExpectedRuntimeException("fake error for " + url);
      }
      super.processOneRegistryJar(cu, url, au, tmpMap);
    }
    void processOneRegistryAuThrowIf(ArchivalUnit au) {
      processOneRegistryAuThrowIf = au;
    }
    void processOneRegistryJarThrowIf(String url) {
      processOneRegistryJarThrowIf = url;
    }

    @Override
    protected void possiblyStartRegistryAuCrawl(ArchivalUnit au,
						String url,
						PluginManager.InitialRegistryCallback cb) {
      cb.crawlCompleted(url);
    }

    Map<String,SimpleQueue> findUrlQueues = new HashMap<String,SimpleQueue>();

    SimpleQueue ensureFindUrlQueue(String url) {
      synchronized (findUrlQueues) {
	SimpleQueue res = findUrlQueues.get(url);
	if (res == null) {
	  res = new SimpleQueue.Fifo();
	  findUrlQueues.put(url, res);
	}
	return res;
      }
    }

    MyPluginManager addToFindUrlQueue(String url, CachedUrl res) {
      SimpleQueue queue = ensureFindUrlQueue(url);
      queue.put(res);
      return this;
    }

    protected CachedUrl findTheCachedUrl0(String url, CuContentReq contentReq) {
      SimpleQueue queue;
      synchronized (findUrlQueues) {
	queue = findUrlQueues.get(url);
      }
      if (queue == null) {
	return super.findTheCachedUrl0(url, contentReq);
      }
      return (CachedUrl)queue.get();
    }

    void setDaemonVersion(DaemonVersion ver) {
      mockDaemonVersion = ver;
    }

    protected DaemonVersion getDaemonVersion() {
      return mockDaemonVersion;
    }
  }

  static class MyDefinablePlugin extends DefinablePlugin {
    private List initArgs = new ArrayList();

    public void initPlugin(LockssDaemon daemon, String extMapName, ClassLoader loader)
	throws FileNotFoundException {
      initArgs.add(ListUtil.list(daemon, extMapName, loader));
      super.initPlugin(daemon, extMapName, loader);
    }

    List getInitArgs() {
      return initArgs;
    }
  }

  static class ThrowingMockPlugin extends MockPlugin {
    RuntimeException rtEx;
    ArchivalUnit.ConfigurationException cfgEx;

    public void setRtEx(RuntimeException rtEx) {
      this.rtEx = rtEx;
    }
    public void setCfgEx(ArchivalUnit.ConfigurationException cfgEx) {
      this.cfgEx = cfgEx;
    }
    public ArchivalUnit createAu0(Configuration config)
	throws ArchivalUnit.ConfigurationException {
      if (rtEx != null) {
	throw rtEx;
      } else if (cfgEx != null) {
	throw cfgEx;
      } else {
	return super.createAu0(config);
      }
    }
    public ArchivalUnit configureAu(Configuration config, ArchivalUnit au)
	throws ArchivalUnit.ConfigurationException {
      if (rtEx != null) {
	throw rtEx;
      } else if (cfgEx != null) {
	throw cfgEx;
      } else {
	return super.configureAu(config, au);
      }
    }
  }


  @Test
  public void testFindCus() throws Exception {
    mgr.startService();
    String url = "http://foo.bar/";
    String lower = "abc";
    String upper = "xyz";

    doConfigAus();
    MockPlugin mpi = (MockPlugin)mgr.getPlugin(mockPlugKey);

    // make a PollSpec with info from a manually created CUS, which should
    // match one of the registered AUs
    CachedUrlSet protoCus = makeCus(mpi, mauauid1, url, lower, upper);
    PollSpec ps1 = new PollSpec(protoCus, Poll.V1_CONTENT_POLL);

    // verify PluginManager can make a CUS for the PollSpec
    CachedUrlSet cus = mgr.findCachedUrlSet(ps1);
    assertNotNull(cus);
    // verify the CUS's CUSS
    CachedUrlSetSpec cuss = cus.getSpec();
    assertEquals(url, cuss.getUrl());
    RangeCachedUrlSetSpec rcuss = (RangeCachedUrlSetSpec)cuss;
    assertEquals(lower, rcuss.getLowerBound());
    assertEquals(upper, rcuss.getUpperBound());

    assertEquals(mauauid1, cus.getArchivalUnit().getAuId());
    // can't test protoCus.getArchivalUnit() .equals( cus.getArchivalUnit() )
    // as we made a fake mock one to build PollSpec, and PluginManager will
    // have created & configured a real mock one.

    CachedUrlSet protoAuCus = makeAuCus(mpi, mauauid1);
    PollSpec ps2 = new PollSpec(protoAuCus, Poll.V1_CONTENT_POLL);

    CachedUrlSet aucus = mgr.findCachedUrlSet(ps2);
    assertNotNull(aucus);
    CachedUrlSetSpec aucuss = aucus.getSpec();
    assertTrue(aucuss instanceof AuCachedUrlSetSpec);
  }

  @Test
  public void testFindSingleNodeCus() throws Exception {
    mgr.startService();
    String url = "http://foo.bar/";
    String lower = PollSpec.SINGLE_NODE_LWRBOUND;

    doConfigAus();
    MockPlugin mpi = (MockPlugin)mgr.getPlugin(mockPlugKey);

    // make a PollSpec with info from a manually created CUS, which should
    // match one of the registered AUs
    CachedUrlSet protoCus = makeCus(mpi, mauauid1, url, lower, null);
    PollSpec ps1 = new PollSpec(protoCus, Poll.V1_CONTENT_POLL);

    // verify PluginManager can make a CUS for the PollSpec
    CachedUrlSet cus = mgr.findCachedUrlSet(ps1);
    assertNotNull(cus);
    // verify the CUS's CUSS
    CachedUrlSetSpec cuss = cus.getSpec();
    assertTrue(cuss instanceof SingleNodeCachedUrlSetSpec);
    assertEquals(url, cuss.getUrl());
  }

  @Test
  public void testGetCandidateAus() throws Exception {
    mgr.startService();
    String h1 = "http://www.foo.org/";
    String h2 = "http://www.bar.org/";

    assertEmpty(mgr.getCandidateAus(h1 + " foo.html"));

    MockArchivalUnit au0 = new MockArchivalUnit("plugin1&au0");
    au0.setName("The Little Prince");
    au0.setUrlStems(ListUtil.list(h2));
    PluginTestUtil.registerArchivalUnit(au0);

    MockArchivalUnit au1 = new MockArchivalUnit("plugin1&au1");
    au1.setName("The Little Dipper");
    au1.setUrlStems(ListUtil.list(h1, h2));
    PluginTestUtil.registerArchivalUnit(au1);

    MockArchivalUnit au2 = new MockArchivalUnit("plugin1&au2");
    au2.setName("Little Richard Journal 10");
    au2.setUrlStems(ListUtil.list(h1, h2));
    PluginTestUtil.registerArchivalUnit(au2);

    MockArchivalUnit au3 = new MockArchivalUnit("plugin1&au3");
    au3.setName("Little Richard Journal 9");
    au3.setUrlStems(ListUtil.list(h1));
    PluginTestUtil.registerArchivalUnit(au3);
		    
    assertIsomorphic(ListUtil.list(au1, au3, au2),
		     mgr.getCandidateAus(h1 + " foo.html"));
    assertIsomorphic(ListUtil.list(au1, au0, au2),
		     mgr.getCandidateAus(h2 + " foo.html"));
    assertIsomorphic(ListUtil.list(h2, h1), mgr.getAllStems());
    mgr.deactivateAu(au2);
    assertIsomorphic(ListUtil.list(au1, au3),
		     mgr.getCandidateAus(h1 + " foo.html"));
    assertIsomorphic(ListUtil.list(au1, au0),
		     mgr.getCandidateAus(h2 + " foo.html"));
    assertIsomorphic(ListUtil.list(h2, h1), mgr.getAllStems());

    mgr.deactivateAu(au0);
    assertIsomorphic(ListUtil.list(au1),
		     mgr.getCandidateAus(h2 + " foo.html"));
    mgr.deactivateAu(au1);
    assertEmpty(mgr.getCandidateAus(h2 + " foo.html"));
  }


  @Test
  public void testCuContentReq() throws Exception {
//     assertTrue(CuContentReq.MostRecentContent.satisfies(CuContentReq.MostRecentContent));
//     assertTrue(CuContentReq.MostRecentContent.satisfies(CuContentReq.HasContent));
//     assertTrue(CuContentReq.MostRecentContent.satisfies(CuContentReq.PreferContent));
//     assertTrue(CuContentReq.MostRecentContent.satisfies(CuContentReq.DontCare));

//     assertFalse(CuContentReq.HasContent.satisfies(CuContentReq.MostRecentContent));
    assertTrue(CuContentReq.HasContent.satisfies(CuContentReq.HasContent));
    assertTrue(CuContentReq.HasContent.satisfies(CuContentReq.PreferContent));
    assertTrue(CuContentReq.HasContent.satisfies(CuContentReq.DontCare));

//     assertFalse(CuContentReq.PreferContent.satisfies(CuContentReq.MostRecentContent));
    assertFalse(CuContentReq.PreferContent.satisfies(CuContentReq.HasContent));
    assertTrue(CuContentReq.PreferContent.satisfies(CuContentReq.PreferContent));
    assertTrue(CuContentReq.PreferContent.satisfies(CuContentReq.DontCare));

//     assertFalse(CuContentReq.DontCare.satisfies(CuContentReq.MostRecentContent));
    assertFalse(CuContentReq.DontCare.satisfies(CuContentReq.HasContent));
    assertFalse(CuContentReq.DontCare.satisfies(CuContentReq.PreferContent));
    assertTrue(CuContentReq.DontCare.satisfies(CuContentReq.DontCare));
  }

  @Test
  public void testFindCachedUrl() throws Exception {
    mgr.startService();
    String url1 = "http://foo.bar/baz";
    String url1a = "http://foo.bar:80/baz";
    String url1b = "http://FOO.BAR:80/baz";
    String url2 = "http://foo.bar/222";
    String url3 = "http://foo.bar/333";
    String url4 = "http://foo.bar/444";
    String url5 = "http://foo.bar/555";
    doConfigAus();
    ConfigurationUtil.addFromArgs(PluginManager.PARAM_AU_SEARCH_404_CACHE_SIZE,
				  "[1,2]",
				  PluginManager.PARAM_AU_SEARCH_MIN_DISK_SEARCHES_FOR_404_CACHE,
				  "0");
    // get the two archival units
    MockArchivalUnit au1 = (MockArchivalUnit)mgr.getAuFromIdIfExists(mauauid1);
    MockArchivalUnit au2 = (MockArchivalUnit)mgr.getAuFromIdIfExists(mauauid2);
    assertEquals(0, mgr.getRecentCuMisses());
    assertEquals(0, mgr.getRecentCuHits());
    assertNull(mgr.findCachedUrl(url1));
    assertNull(mgr.findCachedUrl(url2));
    assertEquals(2, mgr.getRecentCuMisses());
    assertEquals(0, mgr.getRecentCuHits());
    assertEquals(0, mgr.getRecent404Hits());
    CachedUrl cu1 = au1.addUrl(url1, false, true, null);
    CachedUrl cu2 = au2.addUrl(url2, true, true, null);
    CachedUrl cu31 = au1.addUrl(url3, false, true, null);
    CachedUrl cu32 = au2.addUrl(url3, true, true, null);
    assertNull(mgr.findCachedUrl(url1, CuContentReq.HasContent));
    CachedUrl cu = mgr.findCachedUrl(url1, CuContentReq.DontCare);
    assertEquals(url1, cu.getUrl());
    assertSame(au1, cu.getArchivalUnit());
    assertEquals(4, mgr.getRecentCuMisses());
    assertEquals(0, mgr.getRecentCuHits());
    CachedUrl cupref = mgr.findCachedUrl(url1, CuContentReq.PreferContent);
    assertEquals(url1, cupref.getUrl());
    assertSame(au1, cupref.getArchivalUnit());
    assertEquals(5, mgr.getRecentCuMisses());
    assertEquals(0, mgr.getRecentCuHits());

    assertEquals(1, mgr.getRecent404Hits());
    assertNull(mgr.findCachedUrl(url1, CuContentReq.HasContent));
    assertEquals(2, mgr.getRecent404Hits());
    ((MockCachedUrl)cu).setContent("abc");
    signalAuEvent(au1, AuEvent.ContentChangeInfo.Type.Crawl, 1);
    assertNull(mgr.findCachedUrl(url1));
    signalAuEvent(au1, AuEvent.ContentChangeInfo.Type.Crawl, 4);
    CachedUrl rcu1 = mgr.findCachedUrl(url1);
    assertEquals(url1, rcu1.getUrl());
    assertSame(au1, rcu1.getArchivalUnit());
    assertEquals(8, mgr.getRecentCuMisses());
    assertEquals(0, mgr.getRecentCuHits());

    CachedUrl rcu2 = mgr.findCachedUrl(url1a);
    assertEquals(url1, rcu2.getUrl());
    assertSame(au1, rcu2.getArchivalUnit());
    assertEquals(9, mgr.getRecentCuMisses());
    assertEquals(0, mgr.getRecentCuHits());

    CachedUrl rcu3 = mgr.findCachedUrl(url1b);
    assertEquals(url1, rcu3.getUrl());
    assertSame(au1, rcu3.getArchivalUnit());
    assertEquals(10, mgr.getRecentCuMisses());
    assertEquals(0, mgr.getRecentCuHits());

    CachedUrl rcu4 = mgr.findCachedUrl(url2);
    assertEquals(url2, rcu4.getUrl());
    assertSame(au2, rcu4.getArchivalUnit());
    assertEquals(11, mgr.getRecentCuMisses());
    assertEquals(0, mgr.getRecentCuHits());

    // url1 should be in cache
    CachedUrl rcu5 = mgr.findCachedUrl(url1);
    assertEquals(url1, rcu5.getUrl());
    assertSame(au1, rcu5.getArchivalUnit());
    assertEquals(11, mgr.getRecentCuMisses());
    assertEquals(1, mgr.getRecentCuHits());
    assertSame(cu, rcu5);

    // Test PreferContent
    CachedUrl rcu6 = mgr.findCachedUrl(url3, CuContentReq.PreferContent);
    assertEquals(url3, rcu6.getUrl());
    assertTrue(rcu6.hasContent());
    assertSame(au2, rcu6.getArchivalUnit());
    assertEquals(12, mgr.getRecentCuMisses());
    assertEquals(1, mgr.getRecentCuHits());

    // Test findCachedUrls() (returns list)
    assertEquals(ListUtil.list(cu1), mgr.findCachedUrls(url1));
    assertEquals(ListUtil.list(cu2), mgr.findCachedUrls(url2));
    assertSameElements(ListUtil.list(cu32),
		       mgr.findCachedUrls(url3));
    assertSameElements(ListUtil.list(cu32),
		       mgr.findCachedUrls(url3, CuContentReq.HasContent));
    assertSameElements(ListUtil.list(cu31, cu32),
		       mgr.findCachedUrls(url3, CuContentReq.PreferContent));
    ((MockCachedUrl)cu31).setContent("cba");
    assertSameElements(ListUtil.list(cu31, cu32),
		       mgr.findCachedUrls(url3, CuContentReq.HasContent));

    // url1 should still be in cache
    CachedUrl rcu7 = mgr.findCachedUrl(url1);
    assertEquals(url1, rcu7.getUrl());
    assertSame(au1, rcu7.getArchivalUnit());
    assertEquals(12, mgr.getRecentCuMisses());
    assertEquals(2, mgr.getRecentCuHits());
    Plugin plug1 = au1.getPlugin();
    Configuration au1conf = au1.getConfiguration();

    au1.addUrl(url5, true, true, null);

    // stop and start AU1
    mgr.stopAu(au1, AuEvent.forAu(au1, AuEvent.Type.Deactivate));

    // Ensure this one is in 404 cache
    assertEquals(null, mgr.findCachedUrl(url5, CuContentReq.HasContent));

    MockArchivalUnit xmau1 =
      (MockArchivalUnit)mgr.createAu(plug1, au1conf,
                                     AuEvent.model(AuEvent.Type.RestartCreate));
    // Ensure the 404 cache was flushed when AU created
    xmau1.addUrl(url5, true, true, null);
    CachedUrl cu555 = mgr.findCachedUrl(url5, CuContentReq.HasContent);
    assertEquals(xmau1, cu555.getArchivalUnit());

    CachedUrl xcu1 = xmau1.addUrl(url1, true, true, null);
    CachedUrl xcu31 = xmau1.addUrl(url3, true, true, null);

    // url1 cache entry should be detected as invalid and a new CU returned.
    CachedUrl rcu8 = mgr.findCachedUrl(url1);
    assertEquals(url1, rcu8.getUrl());
    assertSame(xmau1, rcu8.getArchivalUnit());
    assertEquals(15, mgr.getRecentCuMisses());
    assertEquals(2, mgr.getRecentCuHits());

    assertEquals(3, mgr.getRecent404Hits());

    assertEquals(0, mgr.getUrlSearchWaits());
  }

  /** Putter puts something onto a queue in a while */
  class Putter extends DoLater {
    SimpleQueue.Fifo queue;
    Object obj1;
    Object obj2;

    Putter(long waitMs, SimpleQueue.Fifo queue, Object obj) {
      this(waitMs, queue, obj, null);
    }

    Putter(long waitMs, SimpleQueue.Fifo queue, Object obj1, Object obj2) {
      super(waitMs);
      this.queue = queue;
      this.obj1 = obj1;
      this.obj2 = obj2;
    }

    protected void doit() {
      queue.put(obj1);
      if (obj2 != null) {
	queue.put(obj2);
      }
    }
  }

  /** Put something onto a queue in a while */
  private Putter putIn(long ms, SimpleQueue.Fifo queue, Object obj) {
    Putter p = new Putter(ms, queue, obj);
    p.start();
    return p;
  }

  AuState setUpAuState(MockArchivalUnit mau) {
    AuState aus = AuTestUtil.setUpMockAus(mau);
//     aus.storeAuState();
    return aus;
  }

  @Test
  public void testFindCachedUrlClockss() throws Exception {
    mgr.startService();
    String url1 = "http://foo.bar/baz";
    String url1a = "http://foo.bar:80/baz";
    String url1b = "http://FOO.BAR:80/baz";
    String url2 = "http://foo.bar/222";
    String url3 = "http://foo.bar/333";
    doConfigAus();
    ConfigurationUtil.addFromArgs(ConfigManager.PARAM_PLATFORM_PROJECT,
				  "clockss");
    assertTrue(theDaemon.isClockss());

    // get the two archival units
    MockArchivalUnit au1 = (MockArchivalUnit)mgr.getAuFromIdIfExists(mauauid1);
    MockArchivalUnit au2 = (MockArchivalUnit)mgr.getAuFromIdIfExists(mauauid2);
    AuState aus1 = setUpAuState(au1);
    AuState aus2 = setUpAuState(au2);
    aus1.setClockssSubscriptionStatus(AuState.CLOCKSS_SUB_NO);
    aus2.setClockssSubscriptionStatus(AuState.CLOCKSS_SUB_YES);

    assertSameElements(ListUtil.list(au1, au2),
		       mgr.getCandidateAus("http://foo.bar/"));

    assertNull(mgr.findCachedUrl(url1));
    assertNull(mgr.findCachedUrl(url2));
    au1.addUrl(url1, true, true, null);
    au2.addUrl(url2, true, true, null);
    au1.addUrl(url3, true, true, null);
    au2.addUrl(url3, true, true, null);
    CachedUrl cu = mgr.findCachedUrl(url1);
    assertEquals(url1, cu.getUrl());
    assertSame(au1, cu.getArchivalUnit());
    // au1 should be at head of candidate list, having been found most recently
    assertIsomorphic(ListUtil.list(au1, au2),
		     mgr.getRawCandidateAus("http://foo.bar/"));
    cu = mgr.findCachedUrl(url1a);
    assertEquals(url1, cu.getUrl());
    assertSame(au1, cu.getArchivalUnit());
    cu = mgr.findCachedUrl(url1b);
    assertEquals(url1, cu.getUrl());
    assertSame(au1, cu.getArchivalUnit());
    assertEquals(AuState.CLOCKSS_SUB_NO,
		 AuUtil.getAuState(cu.getArchivalUnit()).getClockssSubscriptionStatus());

    cu = mgr.findCachedUrl(url2);
    assertEquals(url2, cu.getUrl());
    assertSame(au2, cu.getArchivalUnit());
    // now au2 should be at head of candidate list
    assertIsomorphic(ListUtil.list(au2, au1),
		     mgr.getRawCandidateAus("http://foo.bar/"));

    cu = mgr.findCachedUrl(url3);
    assertEquals(url3, cu.getUrl());
    assertSame(au2, cu.getArchivalUnit());
    assertEquals(AuState.CLOCKSS_SUB_YES,
		 AuUtil.getAuState(cu.getArchivalUnit()).getClockssSubscriptionStatus());
  }

  @Test
  public void testFindCachedUrlWithSiteNormalization()
      throws Exception {
    mgr.startService();
    final String prefix = "http://foo.bar/"; // pseudo crawl rule prefix
    String url0 = "http://foo.bar/xxx/baz"; // normal form of test url
    String url1 = "http://foo.bar/SESSION/xxx/baz"; // should normalize to url0
    String url1a = "http://foo.bar:80/SESSION/xxx/baz"; // same
    String url1b = "http://FOO.BAR/SESSION/xxx/baz"; // same
    String url2 = "http://foo.bar/SESSION/not";	// should not
    // manually create necessary pieces, no config
    MockPlugin mpi = new MockPlugin();
    MockArchivalUnit mau = new MyMockArchivalUnit() {
	// shouldBeCached() is true if URL starts with prefix and doesn't
	// contain SESSION (to ensure shouldBeCached() is called with site
	// normalized URL
	public boolean shouldBeCached(String url) {
	  return url.startsWith(prefix) && (url.indexOf("SESSION") < 0);
	}
	// siteNormalizeUrl() removes "SESSION/" from url
	public String siteNormalizeUrl(String url) {
	  return StringUtil.replaceString(url, "SESSION/", "");
	}
      };
    mau.setPlugin(mpi);
    mau.setAuId("mauauidddd");
    mgr.putAuInMap(mau);
    // neither url is found
    assertNull(mgr.findCachedUrl(url1));
    assertNull(mgr.findCachedUrl(url2));
    // create mock structure so that url0 exists with content
    mau.addUrl(url0, true, true, null);
    // url1 should now be found, as url0
    CachedUrl cu = mgr.findCachedUrl(url1);
    assertNotNull(cu);
    assertEquals(url0, cu.getUrl());
    cu = mgr.findCachedUrl(url1a);
    assertNotNull(cu);
    assertEquals(url0, cu.getUrl());
    cu = mgr.findCachedUrl(url1b);
    assertNotNull(cu);
    assertEquals(url0, cu.getUrl());
    // url2 still not found
    assertNull(mgr.findCachedUrl(url2));
  }

  @Test
  public void testGenerateAuId() {
    mgr.startService();
    Properties props = new Properties();
    props.setProperty("key&1", "val=1");
    props.setProperty("key2", "val 2");
    props.setProperty("key.3", "val:3");
    props.setProperty("key4", "val.4");
    String pluginId = "org|lockss|plugin|Blah";

    String actual = PluginManager.generateAuId(pluginId, props);
    String expected =
      pluginId+"&"+
      "key%261~val%3D1&"+
      "key%2E3~val%3A3&"+
      "key2~val+2&"+
      "key4~val%2E4";
    assertEquals(expected, actual);
  }

  @Test
  public void testGenerateAuIdUniqueInstance() throws Exception {
    mgr.startService();

    String pluginId = "org|lockss|plugin|Blah";
    Properties props = PropUtil.fromArgs("a", "b");

    ConfigurationUtil.addFromArgs(PluginManager.PARAM_USE_AUID_POOL, "true");
    String id = PluginManager.generateAuId(pluginId, props);
    assertEquals("org|lockss|plugin|Blah&a~b", id);
    assertSame(id, PluginManager.generateAuId(pluginId, props));

    ConfigurationUtil.addFromArgs(PluginManager.PARAM_USE_AUID_POOL, "false");
    assertNotSame(id, PluginManager.generateAuId(pluginId, props));
  }

  @Test
  public void testConfigKeyFromAuId() {
    mgr.startService();
    String pluginId = "org|lockss|plugin|Blah";
    String auKey = "base_url~foo&volume~123";

    String totalId = PluginManager.generateAuId(pluginId, auKey);
    String expectedStr = pluginId + "." + auKey;
    assertEquals(expectedStr, PluginManager.configKeyFromAuId(totalId));
  }

  public CachedUrlSet makeCus(Plugin plugin, String auid, String url,
			       String lower, String upper) {
    MockArchivalUnit au = new MockArchivalUnit();
    au.setAuId(auid);
    au.setPlugin(plugin);
    au.setPluginId(plugin.getPluginId());

    CachedUrlSet cus = new MockCachedUrlSet(au,
					    new RangeCachedUrlSetSpec(url,
								      lower,
								      upper));
    return cus;
  }

  public CachedUrlSet makeAuCus(Plugin plugin, String auid) {
    MockArchivalUnit au = new MockArchivalUnit();
    au.setAuId(auid);
    au.setPlugin(plugin);
    au.setPluginId(plugin.getPluginId());

    CachedUrlSet cus = new MockCachedUrlSet(au, new AuCachedUrlSetSpec());
    return cus;
  }

  private KeyStore getKeystoreResource(String name, String pass)
      throws Exception {
    KeyStore ks = KeyStore.getInstance("JKS", "SUN");
    ks.load(ClassLoader.getSystemClassLoader().
	    getResourceAsStream(name), pass.toCharArray());
    return ks;
  }
  
  private File copyKeystoreFileToKnownLocation(String source) throws Exception {
    File keystoreFile = new File(tempDirPath, "copied.keystore");
    InputStream in = getResourceAsStream(source);
    StreamUtil.copy(in, new FileOutputStream(keystoreFile)); 
    return keystoreFile;
  }
  
  @Test
  public void testInitKeystoreAsFile() throws Exception {
    mgr.startService();
    minimalConfig();
    File keystoreFile =
      copyKeystoreFileToKnownLocation("/org/lockss/test/public.keystore");
    KeyStore ks = mgr.initKeystore(keystoreFile.getAbsolutePath(),
                                   "f00bar");
    assertNotNull(ks);
    assertTrue(ks.containsAlias("goodguy"));
  }
  
  @Test
  public void testInitKeystoreAsFileBadPasswordFails() throws Exception {
    mgr.startService();
    minimalConfig();
    File keystoreFile =
      copyKeystoreFileToKnownLocation("/org/lockss/test/public.keystore");
    KeyStore ks = mgr.initKeystore(keystoreFile.getAbsolutePath(),
                                   "f00barrrr"); // bad password
    assertNull(ks);
  }
  
  @Test
  public void testInitKeystoreAsResource() throws Exception {
    mgr.startService();
    minimalConfig();
    KeyStore ks = mgr.initKeystore("org/lockss/test/public.keystore",
                                   "f00bar");
    assertNotNull(ks);
    assertTrue(ks.containsAlias("goodguy"));
  }
  
  @Test
  public void testInitKeystoreAsResourceBadPasswordFails() throws Exception {
    mgr.startService();
    minimalConfig();
    KeyStore ks = mgr.initKeystore("org/lockss/test/public.keystore",
                                   "f00barrrrr");
    assertNull(ks);
  }
  
  @Test
  public void testInitKeystoreAsURL() throws Exception {
    mgr.startService();
    minimalConfig();
    File keystoreFile =
      copyKeystoreFileToKnownLocation("/org/lockss/test/public.keystore");
    String keystoreUrl = keystoreFile.toURI().toURL().toExternalForm();
    KeyStore ks = mgr.initKeystore(keystoreUrl, "f00bar");
    assertNotNull(ks);
    assertTrue(ks.containsAlias("goodguy"));
  }

  @Test
  public void testInitKeystoreAsURLBadPasswordFails() throws Exception {
    mgr.startService();
    minimalConfig();
    File keystoreFile =
      copyKeystoreFileToKnownLocation("/org/lockss/test/public.keystore");
    String keystoreUrl = keystoreFile.toURI().toURL().toExternalForm();
    KeyStore ks = mgr.initKeystore(keystoreUrl, "f00barrrrr");
    assertNull(ks);
  }
  
  @Test
  public void testEmptyInitialRegistryCallback() throws Exception {
    mgr.startService();
    BinarySemaphore bs = new BinarySemaphore();
    PluginManager.InitialRegistryCallback cb =
      new PluginManager.InitialRegistryCallback(Collections.EMPTY_LIST, bs);
    assertTrue(bs.take(Deadline.in(0)));
  }

  @Test
  public void testInitialRegistryCallback() throws Exception {
    mgr.startService();
    BinarySemaphore bs = new BinarySemaphore();
    PluginManager.InitialRegistryCallback cb =
      new PluginManager.InitialRegistryCallback(ListUtil.list("foo", "bar"),
						bs);
    assertFalse(bs.take(Deadline.in(0)));
    cb.crawlCompleted("foo");
    cb.crawlCompleted("bletch");
    assertFalse(bs.take(Deadline.in(0)));
    cb.crawlCompleted("bar");
    assertTrue(bs.take(Deadline.in(0)));
  }

  private void prepareLoadablePluginTests(Properties p) throws Exception {
    pluginJar = "org/lockss/test/good-plugin.jar";
    if (p == null) {
      p = new Properties();
    }
    p.setProperty(PluginManager.PARAM_KEYSTORE_LOCATION,
		  pubKeystore);
    p.setProperty(PluginManager.PARAM_KEYSTORE_PASSWORD,
		  password);
    p.setProperty(PluginManager.PARAM_PLUGIN_REGISTRIES,
		  "");
    ConfigurationUtil.addFromProps(p);
  }

  @Test
  public void testInitLoadablePluginRegistries() throws Exception {
    // Ensure plugin AUs get started even when startAllAus is false
    ConfigurationUtil.addFromArgs(PluginManager.PARAM_START_ALL_AUS, "false");
    mgr.startService();
    Properties p = new Properties();
    prepareLoadablePluginTests(p);
    List urls = ListUtil.list("http://plug1.example.com/blueplugs/",
			      "http://plug1.example.com/redplugs/");
    List urls2 = ListUtil.list("http://plug2.example.com/blueplugs/");
    assertEmpty(mgr.getAllRegistryAus());
    assertEmpty(mgr.getAllAus());
    mgr.initLoadablePluginRegistries(urls);
    assertEquals(SetUtil.theSet(mgr.getAllAus()),
		 SetUtil.theSet(mgr.getAllRegistryAus()));
    assertEquals(2, mgr.getAllRegistryAus().size());
    // ensure that a second call to initLoadablePluginRegistries() adds to
    // the same list of AUs (i.e., uses the same plugin)
    mgr.initLoadablePluginRegistries(urls2);
    assertEquals(SetUtil.theSet(mgr.getAllAus()),
		 SetUtil.theSet(mgr.getAllRegistryAus()));
    assertEquals(3, mgr.getAllRegistryAus().size());
  }

  @Test
  public void testCrawlRegistriesOnce() throws Exception {
    mgr.startService();
    MockCrawlManager mcm = new MockCrawlManager();
    theDaemon.setCrawlManager(mcm);
    ConfigurationUtil.addFromArgs(org.lockss.crawler.CrawlManagerImpl.PARAM_CRAWLER_ENABLED,
				  "true");
    Properties p = new Properties();
    prepareLoadablePluginTests(p);
    List urls = ListUtil.list("http://plug1.example.com/blueplugs/",
			      "http://plug1.example.com/redplugs/");
    List urls2 = ListUtil.list("http://plug2.example.com/blueplugs/");
    assertEmpty(mgr.getAllRegistryAus());
    assertEmpty(mgr.getAllAus());
    mgr.initLoadablePluginRegistries(urls);
    assertEquals(SetUtil.theSet(mgr.getAllAus()),
		 SetUtil.theSet(mgr.getAllRegistryAus()));
    assertEquals(2, mgr.getAllRegistryAus().size());

    assertEmpty(mcm.scheduledCrawls);
    ConfigurationUtil.addFromArgs(PluginManager.PARAM_CRAWL_PLUGINS_ONCE,
				  "true");
    assertEquals(SetUtil.theSet(mgr.getAllRegistryAus()),
		 mcm.scheduledCrawls.keySet());
    mcm.scheduledCrawls.clear();
    assertEmpty(mcm.scheduledCrawls);
    // Set another param that causes PluginManager.setConfig() to run
    ConfigurationUtil.addFromArgs(PluginManager.PARAM_CRAWL_PLUGINS_ONCE + "xx",
				  "1234");
    assertEmpty(mcm.scheduledCrawls);
    ConfigurationUtil.addFromArgs(PluginManager.PARAM_CRAWL_PLUGINS_ONCE,
				  "true");
    assertEmpty(mcm.scheduledCrawls);
    ConfigurationUtil.addFromArgs(PluginManager.PARAM_CRAWL_PLUGINS_ONCE,
				  "false");
    assertEmpty(mcm.scheduledCrawls);
    ConfigurationUtil.addFromArgs(PluginManager.PARAM_CRAWL_PLUGINS_ONCE,
				  "true");
    assertEquals(SetUtil.theSet(mgr.getAllRegistryAus()),
		 mcm.scheduledCrawls.keySet());
  }


  /** Test loading a loadable plugin. */
  public void testLoadLoadablePlugin(boolean preferLoadable) throws Exception {
    mgr.startService();
    Properties p = new Properties();
    p.setProperty(PluginManager.PARAM_PREFER_LOADABLE_PLUGIN,
		  "" + preferLoadable);
    prepareLoadablePluginTests(p);
    String pluginKey = "org|lockss|test|MockConfigurablePlugin";
    // Set up a MyMockRegistryArchivalUnit with the right data.
    List plugins =
      ListUtil.list(pluginJar);
    MyMockRegistryArchivalUnit mmau = new MyMockRegistryArchivalUnit(plugins);
    List registryAus = ListUtil.list(mmau);
    assertNull(mgr.getPlugin(pluginKey));
    mgr.processRegistryAus(registryAus);
    Plugin mockPlugin = mgr.getPlugin(pluginKey);
    assertNotNull(mockPlugin);
    assertEquals(preferLoadable, mgr.isLoadablePlugin(mockPlugin));
    assertFalse(mgr.isInternalPlugin(mockPlugin));
    assertEquals(preferLoadable ? "Loadable" : "Builtin",
		 mgr.getPluginType(mockPlugin));
    assertEquals("1", mockPlugin.getVersion());
    PluginManager.PluginInfo info = mgr.getLoadablePluginInfo(mockPlugin);
    assertEquals(mmau.getNthUrl(1), info.getCuUrl());
    log.debug("isLoadable: " + info.isOnLoadablePath());
    assertEquals(preferLoadable, info.isOnLoadablePath());
    assertSame(mockPlugin, info.getPlugin());
  }

  /** Load a loadable plugin, preferring the loadable version. */
  @Test
  public void testLoadLoadablePluginPreferLoadable() throws Exception {
    testLoadLoadablePlugin(true);
  }

  /** Load a loadable plugin, preferring the library jar version. */
  @Test
  public void testLoadLoadablePluginPreferLibJar() throws Exception {
    testLoadLoadablePlugin(false);
  }

  // plugin-with-libs.jar contains a plugin, and a dependency jar in the
  // lib/ dir
  @Test
  public void testLoadLoadablePluginWithLibJars() throws Exception {
    boolean preferLoadable = true;
    mgr.startService();
    Properties p = new Properties();
    p.setProperty(PluginManager.PARAM_PREFER_LOADABLE_PLUGIN,
		  "" + preferLoadable);
    prepareLoadablePluginTests(p);
    pluginJar = "org/lockss/test/plugin-with-libs.jar";
    String pluginKey = "org|lockss|plugin|plugtest1|PlugTest1Plugin";
    // Set up a MyMockRegistryArchivalUnit with the right data.
    List plugins = ListUtil.list(pluginJar);
    MyMockRegistryArchivalUnit mmau = new MyMockRegistryArchivalUnit(plugins);
    List registryAus = ListUtil.list(mmau);
    assertNull(mgr.getPlugin(pluginKey));
    mgr.processRegistryAus(registryAus);
    Plugin plug = mgr.getPlugin(pluginKey);
    assertNotNull(plug);
    Configuration config =
      ConfigurationUtil.fromArgs("base_url", "http://example.com/a/",
				 "year", "1942");
    ArchivalUnit au1 = mgr.createAu(plug, config,
                                    AuEvent.model(AuEvent.Type.Create));

    assertMatchesRE("^String in toplevel resource\\.\\n"
		    + "String in packaged resource\\.",
		    au1.siteNormalizeUrl("http://foo.bar"));
  }

  // plugin-with-exploded-libs.jar contains a plugin, and the exploded
  // contents of the dependency jar
  @Test
  public void testLoadLoadablePluginWithExplodedLibJars() throws Exception {
    boolean preferLoadable = true;
    mgr.startService();
    Properties p = new Properties();
    p.setProperty(PluginManager.PARAM_PREFER_LOADABLE_PLUGIN,
		  "" + preferLoadable);
    prepareLoadablePluginTests(p);
    pluginJar = "org/lockss/test/plugin-with-exploded-libs.jar";
    String pluginKey = "org|lockss|plugin|plugtest1|PlugTest1Plugin";
    // Set up a MyMockRegistryArchivalUnit with the right data.
    List plugins = ListUtil.list(pluginJar);
    MyMockRegistryArchivalUnit mmau = new MyMockRegistryArchivalUnit(plugins);
    List registryAus = ListUtil.list(mmau);
    assertNull(mgr.getPlugin(pluginKey));
    mgr.processRegistryAus(registryAus);
    Plugin plug = mgr.getPlugin(pluginKey);
    assertNotNull(plug);
    Configuration config =
      ConfigurationUtil.fromArgs("base_url", "http://example.com/a/",
				 "year", "1942");
    ArchivalUnit au1 = mgr.createAu(plug, config,
                                    AuEvent.model(AuEvent.Type.Create));

    assertMatchesRE("^String in toplevel resource\\.\\n"
		    + "String in packaged resource\\.",
		    au1.siteNormalizeUrl("http://foo.bar"));
  }

  /** Runtime errors loading plugins should be caught. */
  @Test
  public void testErrorProcessingRegistryAu() throws Exception {
    mgr.startService();
    String badplug = "org/lockss/test/bad-plugin.jar";
    Properties p = new Properties();
    p.setProperty(PluginManager.PARAM_PREFER_LOADABLE_PLUGIN, "true");
    prepareLoadablePluginTests(p);
    String pluginKey = "org|lockss|test|MockConfigurablePlugin";
    // Set up a MyMockRegistryArchivalUnit with the right data.
    MyMockRegistryArchivalUnit mmau1 =
      new MyMockRegistryArchivalUnit(ListUtil.list(badplug));
    MyMockRegistryArchivalUnit mmau2 =
      new MyMockRegistryArchivalUnit(ListUtil.list(badplug, pluginJar));
    // Make processOneRegistryAu throw on the first au
    mgr.processOneRegistryAuThrowIf(mmau1);
    // Make processOneRegistryJar throw on the first jar in the second au
    mgr.processOneRegistryJarThrowIf(mmau2.getNthUrl(1));
    assertNull(mgr.getPlugin(pluginKey));
    mgr.processRegistryAus(ListUtil.list(mmau1, mmau2));
    // ensure that the one plugin was still loaded
    Plugin mockPlugin = mgr.getPlugin(pluginKey);
    assertNotNull(mockPlugin);
    assertEquals("1", mockPlugin.getVersion());
    PluginManager.PluginInfo info = mgr.getLoadablePluginInfo(mockPlugin);
    assertEquals(mmau2.getNthUrl(2), info.getCuUrl());
    assertTrue(info.isOnLoadablePath());
    assertSame(mockPlugin, info.getPlugin());
  }

  // This test loads two versions of the same plugin.  Because it doesn't
  // matter what jar a plugin is loaded from, an easy way to do this is to
  // load the second version from a different jar (good-plugin2.jar).  See
  // test/scripts/gentestplugins to regenerate these jars.

  @Test
  public void testUpdatePlugin() throws Exception {
    mgr.startService();
    Properties p = new Properties();
    p.setProperty(PluginManager.PARAM_PREFER_LOADABLE_PLUGIN, "true");
    p.setProperty(PluginManager.PARAM_RESTART_AUS_WITH_NEW_PLUGIN, "true");
    p.setProperty(PluginManager.PARAM_AU_RESTART_MAX_SLEEP, "10");
    prepareLoadablePluginTests(p);
    String pluginKey = "org|lockss|test|MockConfigurablePlugin";
    // Set up a MyMockRegistryArchivalUnit with the right data.
    MyMockRegistryArchivalUnit mmau1 =
      new MyMockRegistryArchivalUnit(ListUtil.list(pluginJar));
    assertNull(mgr.getPlugin(pluginKey));
    mgr.processRegistryAus(ListUtil.list(mmau1));
    Plugin plugin1 = mgr.getPlugin(pluginKey);
    assertNotNull(plugin1);
    assertTrue(mgr.isLoadablePlugin(plugin1));
    assertFalse(mgr.isInternalPlugin(plugin1));
    assertEquals("1", plugin1.getVersion());
    PluginManager.PluginInfo info = mgr.getLoadablePluginInfo(plugin1);
    assertEquals(mmau1.getNthUrl(1), info.getCuUrl());
    assertSame(plugin1, info.getPlugin());

    Configuration config = ConfigurationUtil.fromArgs("base_url",
						      "http://example.com/a/"
						      ,"year", "1942");
//     theDaemon.setStartAuManagers(true);
    ArchivalUnit au1 = mgr.createAu(plugin1, config,
                                    AuEvent.model(AuEvent.Type.Create));
    String auid = au1.getAuId();
    assertNotNull(au1);
    assertSame(plugin1, au1.getPlugin());
    assertEquals("http://example.com/a/, 1942", au1.getName());

    // Create a second registry AU (because it also doesn't matter which AU
    // a plugin jar comes from).
    MyMockRegistryArchivalUnit mmau2 =
      new MyMockRegistryArchivalUnit(ListUtil.list(pluginJar,
						   "org/lockss/test/good-plugin2.jar"));
    assertSame(au1, mgr.getAuFromIdIfExists(auid));
    mgr.processRegistryAus(ListUtil.list(mmau2));

    // Ensure the new plugin was installed, the AU is now running as part
    // of that plugin, and the AU's definition has changed appropriately
    Plugin plugin2 = mgr.getPlugin(pluginKey);
    assertNotSame(plugin1, plugin2);
    assertEquals("2", plugin2.getVersion());
    ArchivalUnit au2 = mgr.getAuFromIdIfExists(auid);
    assertNotSame(au1, au2);
    assertSame(plugin2, au2.getPlugin());
    assertEquals("V2: http://example.com/a/, 1942", au2.getName());
    assertEquals(0, mgr.getNumFailedAuRestarts());
  }

  @Test
  public void testStartAuLoadsCdnStems() throws Exception {
    String cdnStem = "http://cdn.host/";
    String baseStem = "http://example.com/";

    theDaemon.setStartAuManagers(true);
    mgr.startService();
    minimalConfig();
    String pname = "org.lockss.test.TestXmlPlugin";
    String key = PluginManager.pluginKeyFromId(pname);
    assertTrue(mgr.ensurePluginLoaded(key));
    Plugin plug = mgr.getPlugin(key);
    assertNotNull(plug);
    Configuration config = ConfigurationUtil.fromArgs("base_url",
						      baseStem + "a/");
    ArchivalUnit au = mgr.createAu(plug, config,
                                   AuEvent.model(AuEvent.Type.Create));
    AuState aus = AuUtil.getAuState(au);
    assertEmpty(aus.getCdnStems());
    aus.addCdnStem(cdnStem);
    assertEquals(ListUtil.list(baseStem, cdnStem), au.getUrlStems());
    assertEquals(ListUtil.list(cdnStem), aus.getCdnStems());
    assertSameElements(ListUtil.list(au),
		       mgr.getCandidateAusFromStem(cdnStem));
    assertSameElements(ListUtil.list(au),
		       mgr.getCandidateAus(cdnStem + "aaa"));
    assertSameElements(ListUtil.list(au),
		       mgr.getCandidateAusFromStem(baseStem));


    PluginTestUtil.unregisterArchivalUnit(au);
    assertEmpty(mgr.getCandidateAusFromStem(cdnStem));
    // Ensure AUs CDN stems are set before addHostAus() is called
    ArchivalUnit au2 = mgr.createAu(plug, config,
				    AuEvent.model(AuEvent.Type.Create));
    assertNotSame(au2, au);
    assertSameElements(ListUtil.list(au2),
		       mgr.getCandidateAusFromStem(cdnStem));
    assertSameElements(ListUtil.list(au2),
		       mgr.getCandidateAusFromStem(baseStem));
  }


  @Test
  public void testRegistryAuEventHandler() throws Exception {
    ConfigurationUtil.addFromArgs(RepositoryManager.PARAM_V2_REPOSITORY,
				  "volatile:baz");
    JmsConsumer cons =
      JMSManager.getJmsFactoryStatic().createTopicConsumer(null, PluginManager.DEFAULT_JMS_NOTIFICATION_TOPIC);
    ConfigurationUtil.addFromArgs(PluginManager.PARAM_ENABLE_JMS_NOTIFICATIONS,
				  "true");
    assertNull(cons.receiveMap(TIMEOUT_SHOULD));
    mgr.setLoadablePluginsReady(false);
    mgr.startService();
    Properties p = new Properties();
    mgr.startLoadablePlugins();
    MyMockRegistryArchivalUnit mrau =
      new MyMockRegistryArchivalUnit(Collections.EMPTY_LIST);
    mrau.setAuId("reg_auid_42");
    assertEmpty(mgr.getProcessedRegAus());
    signalAuEvent(mrau);
    assertEquals(ListUtil.list(mrau), mgr.getProcessedRegAus());
    assertEquals(MapUtil.map("type", "ContentChanged",
			     "auid", "reg_auid_42",
			     "repository_spec", "volatile:baz",
			     "change_info", MapUtil.map("type", "Crawl",
							"complete", true,
							"num_urls", 4)),
		 cons.receiveMap(TIMEOUT_SHOULDNT));
  }

  private void signalAuEvent(final ArchivalUnit au) {
    signalAuEvent(au, AuEvent.ContentChangeInfo.Type.Crawl, 4);
  }

  private void signalAuEvent(final ArchivalUnit au,
			     AuEvent.ContentChangeInfo.Type type,
			     int numUrls) {
    final AuEvent.ContentChangeInfo chInfo = new AuEvent.ContentChangeInfo();
    chInfo.setType(type);
    chInfo.setNumUrls(numUrls);
    chInfo.setComplete(true);
    AuEvent event =
      AuEvent.forAu(au, AuEvent.Type.ContentChanged).setChangeInfo(chInfo);
    mgr.signalAuEvent(au, event);
  }

  // Test that a configured AU that isn't running (because its plugin
  // wasn't loaded) gets started when the plugin is loaded
  @Test
  public void testStartUnstartedAu() throws Exception {
    mgr.startService();
    Properties p = new Properties();
    p.setProperty(PluginManager.PARAM_PREFER_LOADABLE_PLUGIN, "true");
    String k1 = "base_url";
    String v1 = "http://example.com/a/";
    String k2 = "year";
    String v2 = "1942";
    String pluginKey = "org|lockss|test|MockConfigurablePlugin";
    Properties auProps = PropUtil.fromArgs(k1, v1, k2, v2);
    String auid = PluginManager.generateAuId(pluginKey, auProps);

    // Store this Archival Unit configuration in the database.
    Map<String, String> auConfig = new HashMap<>();
    auConfig.put(k1, v1);
    auConfig.put(k2, v2);
    AuConfiguration auConfiguration = new AuConfiguration(auid, auConfig);
    theDaemon.getConfigManager()
    .storeArchivalUnitConfiguration(auConfiguration);

    assertEquals(null, mgr.getAuFromIdIfExists(auid));
    mgr.suppressEnxurePluginLoaded(ListUtil.list(pluginKey));
    prepareLoadablePluginTests(p);
    assertEquals(null, mgr.getAuFromIdIfExists(auid));

    mgr.suppressEnxurePluginLoaded(null);

    // Set up a MyMockRegistryArchivalUnit with the right data.
    MyMockRegistryArchivalUnit mmau1 =
      new MyMockRegistryArchivalUnit(ListUtil.list(pluginJar));
    assertNull(mgr.getPlugin(pluginKey));
    mgr.processRegistryAus(ListUtil.list(mmau1), true);
    Plugin plugin1 = mgr.getPlugin(pluginKey);
    assertNotNull(plugin1);
    assertTrue(mgr.isLoadablePlugin(plugin1));
    assertFalse(mgr.isInternalPlugin(plugin1));
    assertEquals("1", plugin1.getVersion());

    ArchivalUnit au = mgr.getAuFromIdIfExists(auid);
    assertNotNull(au);
  }

  /**
   * Tests PluginManager.isInternalPlugin(Plugin).
   * @throws Exception
   */
  @Test
  public void testIsInternalPlugin() throws Exception {
    Plugin internalPlugin = mgr.getImportPlugin();
    assertNotNull(internalPlugin);
    assertTrue(mgr.isInternalPlugin(internalPlugin));
    internalPlugin = mgr.getRegistryPlugin();
    assertNotNull(internalPlugin);
    assertTrue(mgr.isInternalPlugin(internalPlugin));
    assertTrue(mgr.ensurePluginLoaded(mockPlugKey));
    internalPlugin = mgr.getPlugin(mockPlugKey);
    assertNotNull(internalPlugin);
    assertFalse(mgr.isInternalPlugin(internalPlugin));
  }

  /**
   * a mock Archival Unit used for testing loadable plugin loading.
   */
  private static class MyMockRegistryArchivalUnit extends MockArchivalUnit {
    private MyMockRegistryCachedUrlSet cus;

    public MyMockRegistryArchivalUnit(List jarFiles) {
      super((Plugin)null);
      cus = new MyMockRegistryCachedUrlSet();
      int n = 0;
      for (Iterator iter = jarFiles.iterator(); iter.hasNext(); ) {
	n++;
	cus.addCu(new MockCachedUrl(getNthUrl(n),
				    (String)iter.next(), true));
      }
    }

    public String getNthUrl(int n) {
      return "http://foo.bar/test" + n + ".jar";
    }

    public CachedUrlSet getAuCachedUrlSet() {
      return cus;
    }
  }

  private static class MyMockArchivalUnit extends MockArchivalUnit {
    public Collection getUrlStems() {
      return ListUtil.list("http://foo.bar/");
    }
  }

  private static class MyMockPlugin extends MockPlugin {
    public MyMockPlugin(){
      super();
    }

    protected MockArchivalUnit newMockArchivalUnit() {
      return new MyMockArchivalUnit();
    }
  }

  private static class VerPlugin extends MockPlugin {
    public VerPlugin(){
      super();
    }

    public String getRequiredDaemonVersion() {
      return "1.10.0";
    }
  }

  /**
   * a mock CachedUrlSet used for testing loadable plugin loading.
   */
  private static class MyMockRegistryCachedUrlSet extends MockCachedUrlSet {
    List cuList;

    public MyMockRegistryCachedUrlSet() {
      cuList = new ArrayList();
    }

    public void addCu(MockCachedUrl cu) {
      cuList.add(cu);
    }

    @Override
    public Iterator contentHashIterator() {
      return cuList.iterator();
    }

    @Override
    public CuIterator getCuIterator() {
      return new MockCuIterator(cuList);
    }
  }
}
