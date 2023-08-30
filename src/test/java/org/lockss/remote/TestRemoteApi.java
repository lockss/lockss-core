/*

Copyright (c) 2000-2019, Board of Trustees of Leland Stanford Jr. University.
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

package org.lockss.remote;

import java.io.*;
import java.util.*;
import java.util.zip.*;
import org.lockss.config.*;
import org.lockss.daemon.ConfigParamDescr;
import org.lockss.db.DbException;
import org.lockss.util.test.FileTestUtil;
import org.lockss.mail.MimeMessage;
import org.lockss.metadata.MetadataManager;
import org.lockss.plugin.*;
import org.lockss.protocol.MockIdentityManager;
import org.lockss.util.rest.exception.LockssRestException;
import org.lockss.subscription.SubscriptionManager;
import org.lockss.test.*;
import org.lockss.util.*;
import org.lockss.util.io.*;
import org.lockss.util.os.PlatformUtil;

/**
 * Test class for org.lockss.remote.RemoteApi
 */
public class TestRemoteApi extends LockssTestCase {

  static final String AUID1 = "AUID_1";
  static final String PID1 = "PID_1";

  MockLockssDaemon daemon;
  MyMockPluginManager mpm;
  MyIdentityManager idMgr;
  RemoteApi rapi;
  SubscriptionManager subscriptionManager;
  File tempDir = null;

  public void setUp() throws Exception {
    super.setUp();

    tempDir = getTempDir();
    String tempDirPath = tempDir.getAbsolutePath() + File.separator;
    Properties p = new Properties();
    p.setProperty(ConfigManager.PARAM_PLATFORM_DISK_SPACE_LIST, tempDirPath);
    ConfigurationUtil.setCurrentConfigFromProps(p);

    daemon = getMockLockssDaemon();
    daemon.setUpStateManager();

    MetadataManager metadataManager = new MetadataManager();
    daemon.setMetadataManager(metadataManager);
    metadataManager.initService(daemon);

    daemon.setUpAuConfig();

    mpm = new MyMockPluginManager();

    mpm.mockInit();
    daemon.setPluginManager(mpm);
    rapi = new RemoteApi();
    daemon.setRemoteApi(rapi);
    rapi.initService(daemon);
    idMgr = new MyIdentityManager();
    daemon.setIdentityManager(idMgr);
    idMgr.initService(daemon);
    daemon.setDaemonInited(true);
    rapi.startService();
    subscriptionManager = new SubscriptionManager();
    daemon.setSubscriptionManager(subscriptionManager);
    subscriptionManager.initService(daemon);
    subscriptionManager.startService();
  }

  public void tearDown() throws Exception {
    rapi.stopService();
    daemon.stopDaemon();
    super.tearDown();
  }

  public void testFindAuProxy() {
    MockArchivalUnit mau1 = new MockArchivalUnit();
    AuProxy aup = rapi.findAuProxy(mau1);
    assertNotNull(aup);
    assertSame(mau1, aup.getAu());
    assertSame(aup, rapi.findAuProxy(mau1));
    ArchivalUnit mau2 = mpm.getAuFromIdIfExists(AUID1);
    assertNotNull(mau2);
    AuProxy aup2b = rapi.findAuProxy(mau2);
    AuProxy aup2a = rapi.findAuProxy(AUID1);
    assertNotNull(aup2a);
    assertSame(aup2a, aup2b);
  }

  public void testGetInactiveAus() throws Exception {
    String id1 = "xxx1";
    String id2 = "xxx2";
    mpm.setStoredConfig(id1, ConfigManager.EMPTY_CONFIGURATION);
    mpm.setStoredConfig(id2, ConfigManager.EMPTY_CONFIGURATION);
    mpm.setInactiveAuIds(ListUtil.list(id1, id2));
    assertEquals(ListUtil.list(rapi.findInactiveAuProxy(id1),
			       rapi.findInactiveAuProxy(id2)),
		 rapi.getInactiveAus());
    assertEquals(2, rapi.countInactiveAus());


    MockArchivalUnit mau1 = new MockArchivalUnit(id1);
    AuProxy aup1 = rapi.findAuProxy(mau1);
    assertFalse(aup1 instanceof InactiveAuProxy);
  }

  public void testGetAllAus() throws Exception {
    MockArchivalUnit mau1 = new MockArchivalUnit();
    MockArchivalUnit mau2 = new MockArchivalUnit();
    mpm.setAllAus(ListUtil.list(mau1, mau2));
    assertEquals(ListUtil.list(rapi.findAuProxy(mau1), rapi.findAuProxy(mau2)),
		 rapi.getAllAus());
  }

  public void testMapAus() {
    MockArchivalUnit mau1 = new MockArchivalUnit();
    ArchivalUnit mau2 = mpm.getAuFromIdIfExists(AUID1);
    List mapped = rapi.mapAusToProxies(ListUtil.list(mau1, mau2));
    assertEquals(2, mapped.size());
    assertNotNull(mapped.get(0));
    assertNotNull(mapped.get(1));
    assertSame(rapi.findAuProxy(mau1), (AuProxy)mapped.get(0));
    assertSame(rapi.findAuProxy(mau2), (AuProxy)mapped.get(1));
  }

  public void testFindPluginProxy() throws Exception {
    MockPlugin mp1 = new MockPlugin();
    PluginProxy pp1 = rapi.findPluginProxy(mp1);
    assertNotNull(pp1);
    assertSame(mp1, pp1.getPlugin());
    assertSame(pp1, rapi.findPluginProxy(mp1));
    Plugin mp2 = mpm.getPlugin(PluginManager.pluginKeyFromId(PID1));
    assertNotNull(mp2);
    PluginProxy pp2b = rapi.findPluginProxy(mp2);
    PluginProxy pp2a = rapi.findPluginProxy(PID1);
    assertNotNull(pp2a);
    assertSame(pp2a, pp2b);
  }

  public void testMapPlugins() {
    MockPlugin mp1 = new MockPlugin();
    Plugin mp2 = mpm.getPlugin(PluginManager.pluginKeyFromId(PID1));
    List mapped = rapi.mapPluginsToProxies(ListUtil.list(mp1, mp2));
    assertEquals(2, mapped.size());
    assertNotNull(mapped.get(0));
    assertNotNull(mapped.get(1));
    assertSame(rapi.findPluginProxy(mp1), (PluginProxy)mapped.get(0));
    assertSame(rapi.findPluginProxy(mp2), (PluginProxy)mapped.get(1));
  }

  public void testCreateAndSaveAuConfiguration() throws Exception {
    ConfigParamDescr d1 = ConfigParamDescr.BASE_URL;
    MockPlugin mp1 = new MockPlugin();
    mp1.setAuConfigDescrs(ListUtil.list(d1));

    PluginProxy pp1 = rapi.findPluginProxy(mp1);
    Configuration config = ConfigurationUtil.fromArgs(d1.getKey(), "v1");
    AuProxy aup = rapi.createAndSaveAuConfiguration(pp1, config);
    Pair pair = (Pair)mpm.actions.get(0);
    assertEquals(mp1, pair.one);
    assertEquals(config, pair.two);
    assertEquals(pp1, aup.getPlugin());
    assertEquals(config, aup.getConfiguration());
  }

  public void testSetAndSaveAuConfiguration() throws Exception {
    ConfigParamDescr d1 = ConfigParamDescr.BASE_URL;
    MockPlugin mp1 = new MockPlugin();
    mp1.setAuConfigDescrs(ListUtil.list(d1));
    MockArchivalUnit mau1 = new MockArchivalUnit();
    AuProxy aup = rapi.findAuProxy(mau1);

    PluginProxy pp1 = rapi.findPluginProxy(mp1);
    Configuration config = ConfigurationUtil.fromArgs(d1.getKey(), "v1");
    rapi.setAndSaveAuConfiguration(aup, config);
    Pair pair = (Pair)mpm.actions.get(0);
    assertEquals(mau1, pair.one);
    assertEquals(config, pair.two);
  }

  public void testDeleteAu() throws Exception {
    MockArchivalUnit mau1 = new MockArchivalUnit();
    AuProxy aup = rapi.findAuProxy(mau1);
    rapi.deleteAu(aup);
    Pair pair = (Pair)mpm.actions.get(0);
    assertEquals("Delete", pair.one);
    assertEquals(mau1, pair.two);
  }

  public void testDeactivateAu() throws Exception {
    MockArchivalUnit mau1 = new MockArchivalUnit();
    AuProxy aup = rapi.findAuProxy(mau1);
    rapi.deactivateAu(aup);
    Pair pair = (Pair)mpm.actions.get(0);
    assertEquals("Deactivate", pair.one);
    assertEquals(mau1, pair.two);
  }

  public void testGetStoredAuConfiguration() throws Exception {
    MockArchivalUnit mau1 = new MockArchivalUnit();
    String id = "auid3";
    Configuration config = ConfigurationUtil.fromArgs("k1", "v1");
    mau1.setAuId(id);
    mpm.setStoredConfig(id, config);
    AuProxy aup = rapi.findAuProxy(mau1);
    assertEquals(config, rapi.getStoredAuConfiguration(aup));
  }

  public void testGetCurrentAuConfiguration() throws Exception {
    MockArchivalUnit mau1 = new MockArchivalUnit();
    String id = "auid3";
    Configuration config = ConfigurationUtil.fromArgs("k1", "v1");
    mau1.setAuId(id);
    mpm.setCurrentConfig(id, config);
    AuProxy aup = rapi.findAuProxy(mau1);
    assertNull(rapi.getStoredAuConfiguration(aup));
  }

  void writeAuConfigFile(String s) throws IOException {
    writeCacheConfigFile(ConfigManager.CONFIG_FILE_AU_CONFIG, s);
  }

  void writeCacheConfigFile(String cfileName, String s) throws IOException {
    String tmpdir = tempDir.toString();
    ConfigurationUtil.setFromArgs(ConfigManager.PARAM_PLATFORM_DISK_SPACE_LIST,
				  tmpdir);
    String relConfigPath =
      CurrentConfig.getParam(ConfigManager.PARAM_CONFIG_PATH,
                             ConfigManager.DEFAULT_CONFIG_PATH);
    File cdir = new File(tmpdir, relConfigPath);
    File configFile = new File(cdir, cfileName);
    FileTestUtil.writeFile(configFile, s);
    log.debug("Wrote: " + configFile);
  }

  /**
   * Writes the configuration of an Archival Unit to the database.
   * 
   * @param auConfiguration
   *          An AuConfiguration with the Archival Unit configuration.
   * @throws DbException
   *           if any problem occurred accessing the database.
   * @throws LockssRestException
   *           if any problem occurred accessing the REST service.
   */
  void writeAuDb(AuConfiguration auConfiguration)
      throws DbException, LockssRestException {
    daemon.getConfigManager().storeArchivalUnitConfiguration(auConfiguration);
  }

  /** assert that the stream contains the contents of an au.txt (au config)
   * file with the expected property values
   */
  public void assertIsAuTxt(Properties expectedProps,
			    InputStream in) throws Exception {
    String pat = "# AU Configuration saved .* from machine_foo\norg.lockss.au.FooPlugin.k~v.k=v";
    BufferedInputStream bi = new BufferedInputStream(in);
    bi.mark(10000);
    assertMatchesRE(pat, StringUtil.fromInputStream(bi));
    bi.reset();
    Properties p = new Properties();
    p.load(bi);
    assertEquals(expectedProps, p);
  }

  public void testGetAuConfigBackupStreamV1() throws Exception {
    writeAuConfigFile("org.lockss.au.FooPlugin.k~v.k=v\n");
    ConfigurationUtil.addFromArgs(RemoteApi.PARAM_BACKUP_FILE_VERSION, "v1");
    InputStream is = rapi.getAuConfigBackupStream("machine_foo");
    Properties exp = new Properties();
    exp.put("org.lockss.au.FooPlugin.k~v.k", "v");
    assertIsAuTxt(exp, is);
  }

  public void testGetAuConfigBackupStreamV2() throws Exception {
    Map<String, String> props = new HashMap<>();
    props.put("k", "v");
    AuConfiguration auConfiguration =
	new AuConfiguration("FooPlugin&k~v", props);
    writeAuDb(auConfiguration);
    ConfigurationUtil.addFromArgs(RemoteApi.PARAM_BACKUP_FILE_VERSION, "v2");
    MockArchivalUnit mau1 = new MockArchivalUnit();
    MockArchivalUnit mau2 = new MockArchivalUnit();
    MockArchivalUnit mau3 = new MockArchivalUnit();
    mau1.setAuId("mau1id");
    mau2.setAuId("mau2id");
    mau3.setAuId("mau3id");
    mpm.setAllAus(ListUtil.list(mau1, mau2, mau3));
    idMgr.setAgreeMap(mau1, "agree map 1");
    idMgr.setAgreeMap(mau3, "agree map 3");

    InputStream in = rapi.getAuConfigBackupStream("machine_foo");
    File zip = FileTestUtil.tempFile("foo", ".zip");
    OutputStream out = new FileOutputStream(zip);
    StreamUtil.copy(in, out);
    in.close();
    out.close();

    assertTrue(ZipUtil.isZipFile(zip));
    File tmpdir = getTempDir();
    ZipUtil.unzip(zip, tmpdir);

    String[] dirfiles = tmpdir.list();
    List audirs = new ArrayList();
    Map auagreemap = new HashMap();
    Map austatemap = new HashMap();
    boolean auDbBackupFileFound = false;

    for (int ix = 0; ix < dirfiles.length; ix++) {
      // Check whether it is the backup of the Archival Unit configuration
      // database.
      if (RemoteApi.BACK_FILE_AU_CONFIGURATION_DB.equals(dirfiles[ix])) {
	// Yes: Get the contents of the backup file in a form suitable for
	// comparison.
	Configuration allAuConfig = rapi
	    .getConfigurationFromSavedDbConfig(new File(tmpdir, dirfiles[ix]));
	assertEquals(AuConfigurationUtils.
	    toAuidPrefixedConfiguration(auConfiguration),
	    allAuConfig.getConfigTree(PluginManager.PARAM_AU_TREE));
	auDbBackupFileFound = true;
	continue;
      }
      File audir = new File(tmpdir, dirfiles[ix]);
      if (!audir.isDirectory()) {
	continue;
      }
      audirs.add(audir);
      Properties auprops =
	PropUtil.fromFile(new File(audir, RemoteApi.BACK_FILE_AU_PROPS));
      log.debug("props: " + auprops);
      String auid = auprops.getProperty(RemoteApi.AU_BACK_PROP_AUID);
      File agreefile = new File(audir, RemoteApi.BACK_FILE_AGREE_MAP);
      if (agreefile.exists()) {
	auagreemap.put(auid, agreefile);
      }
      File austatefile = new File(audir, RemoteApi.BACK_FILE_AUSTATE);
      if (austatefile.exists()) {
	austatemap.put(auid, austatefile);
      }
    }

    assertTrue(auDbBackupFileFound);

    assertEquals(3, audirs.size());
    File agreefile;
    assertNotNull(agreefile = (File)auagreemap.get("mau1id"));
    assertEquals("agree map 1", StringUtil.fromFile(agreefile));

    assertNotNull(agreefile = (File)auagreemap.get("mau3id"));
    assertEquals("agree map 3", StringUtil.fromFile(agreefile));

    // XXXAUS
//     File statefile;
//     assertNotNull(statefile = (File)austatemap.get("mau1id"));
//     assertEquals("dummy austate", StringUtil.fromFile(statefile));

    assertEquals(2, auagreemap.size());
    // XXXAUS
//     assertEquals(1, austatemap.size());
  }

  public void testCheckLegalAuConfigTree() throws Exception {
    Properties p = new Properties();
    p.setProperty("org.lockss.au.foobar", "17");
    p.setProperty("org.lockss.config.fileVersion.au", "1");
    assertEquals(1,
		 rapi.checkLegalAuConfigTree(ConfigManager.fromProperties(p)));
    p.setProperty("org.lockss.other.prop", "foo");
    try {
      rapi.checkLegalAuConfigTree(ConfigManager.fromProperties(p));
      fail("checkLegalAuConfigTree() allowed non-AU prop");
    } catch (RemoteApi.InvalidAuConfigBackupFile e) {
    }
    p.remove("org.lockss.other.prop");
    rapi.checkLegalAuConfigTree(ConfigManager.fromProperties(p));
    p.setProperty("org.lockss.au", "xx");
    try {
      rapi.checkLegalAuConfigTree(ConfigManager.fromProperties(p));
      fail("checkLegalAuConfigTree() allowed non-AU prop");
    } catch (RemoteApi.InvalidAuConfigBackupFile e) {
    }
    p.remove("org.lockss.au");
    rapi.checkLegalAuConfigTree(ConfigManager.fromProperties(p));
    p.setProperty("org.lockss.config.fileVersion.au", "0");
    try {
      rapi.checkLegalAuConfigTree(ConfigManager.fromProperties(p));
      fail("checkLegalAuConfigTree() allowed non-AU prop");
    } catch (RemoteApi.InvalidAuConfigBackupFile e) {
    }
  }

  public File toFile(Properties props, File file) throws IOException {
    return toFile(props, file, false);
  }

  public File toFile(Properties props, File file, boolean isAuTxt)
      throws IOException {
    return toFile(ConfigManager.fromPropertiesUnsealed(props), file, isAuTxt);
  }

  public File toFile(Configuration config, File file, boolean isAuTxt)
      throws IOException {
    OutputStream out = new FileOutputStream(file);
    toStream(config, out, isAuTxt);
    out.close();
    return file;
  }

  public void toStream(Configuration config, OutputStream out, boolean isAuTxt)
      throws IOException {
    if (isAuTxt) {
      StringUtil.toOutputStream(out, RemoteApi.AU_BACKUP_FILE_COMMENT + "\n");
      config.put(ConfigManager.configVersionProp(ConfigManager.CONFIG_FILE_AU_CONFIG), "1");
    }
    config.store(out, "");
  }

  public File writeZipBackup(Configuration auTxt, Map auAgreeMap)
      throws IOException {
    File file = FileTestUtil.tempFile("restoretest", ".zip");
    OutputStream out = new BufferedOutputStream(new FileOutputStream(file));
    ZipOutputStream z = new ZipOutputStream(out);
    z.putNextEntry(new ZipEntry(ConfigManager.CONFIG_FILE_AU_CONFIG));
    toStream(auTxt, z, true);
    z.closeEntry();

    int ix = 1;
    if (auAgreeMap != null) {
      for (Iterator iter = auAgreeMap.keySet().iterator(); iter.hasNext(); ) {
	String auid = (String)iter.next();
	Properties auprops = PropUtil.fromArgs(RemoteApi.AU_BACK_PROP_AUID,
					       auid);
	String dir = Integer.toString(ix) + "/";
	z.putNextEntry(new ZipEntry(dir));
	z.putNextEntry(new ZipEntry(dir + RemoteApi.BACK_FILE_AU_PROPS));
	auprops.store(z, "");
	z.closeEntry();
	z.putNextEntry(new ZipEntry(dir + RemoteApi.BACK_FILE_AGREE_MAP));
	StringUtil.toOutputStream(z, (String)auAgreeMap.get(auid));
	z.closeEntry();
	ix++;
      }
    }
    z.close();
    return file;
  }

  Configuration addAuTree(Configuration toConfig, String auid,
			  Properties auprops) {
    if (toConfig == null) {
      toConfig = ConfigManager.newConfiguration();
    }
    String prefix = PluginManager.PARAM_AU_TREE + "." +
      PluginManager.configKeyFromAuId(auid);
    toConfig.addAsSubTree(ConfigManager.fromPropertiesUnsealed(auprops),
			  prefix);
    return toConfig;
  }

  void assertEntry(Properties exp, RemoteApi.BatchAuStatus bas, String auid) {
    assertEntry(exp, bas, auid, null);
  }

  void assertEntry(Properties exp, RemoteApi.BatchAuStatus bas, String auid,
		   String name) {
    RemoteApi.BatchAuStatus.Entry ent = findEntry(bas, auid);
    assertNotNull("No entry for auid " + auid, ent);
    if (name != null) {
      assertEquals(name, ent.getName());
    }

    assertEquals(ConfigManager.fromProperties(exp), ent.getConfig());
  }

  RemoteApi.BatchAuStatus.Entry findEntry(RemoteApi.BatchAuStatus bas,
					  String auid) {
    for (Iterator iter = bas.getStatusList().iterator(); iter.hasNext(); ) {
      RemoteApi.BatchAuStatus.Entry ent =
	(RemoteApi.BatchAuStatus.Entry)iter.next();
      if (auid.equals(ent.getAuId())) {
	return ent;
      }
    }
    return null;
  }

  public void testProcessSavedConfigV1()  throws Exception {
    Properties p1 = new Properties();
    p1.put(ConfigParamDescr.BASE_URL.getKey(), "http://foo.bar/");
    p1.put(ConfigParamDescr.VOLUME_NUMBER.getKey(), "7");
    Properties p2 = new Properties();
    p2.put(ConfigParamDescr.BASE_URL.getKey(), "http://example.com/");
    p2.put(ConfigParamDescr.VOLUME_NUMBER.getKey(), "42");
    String auid1 = PluginManager.generateAuId(PID1, p1);
    String auid2 = PluginManager.generateAuId(PID1, p2);
    Configuration config = addAuTree(null, auid1, p1);
    config = addAuTree(config, auid2, p2);

    File file = toFile(config, FileTestUtil.tempFile("saved1"), true);
    InputStream in = new FileInputStream(file);
    RemoteApi.BatchAuStatus bas = rapi.processSavedConfig(in);
    List statlist = bas.getStatusList();
    assertEquals(2, statlist.size());
    assertNull(bas.getBackupInfo());

    assertEntry(p1, bas, auid1);
    assertEntry(p2, bas, auid2);
  }

  public void testProcessSavedConfigV2()  throws Exception {
    Properties p1 = new Properties();
    p1.put(ConfigParamDescr.BASE_URL.getKey(), "http://foo.bar/");
    p1.put(ConfigParamDescr.VOLUME_NUMBER.getKey(), "7");
    Properties p2 = new Properties();
    p2.put(ConfigParamDescr.BASE_URL.getKey(), "http://example.com/");
    p2.put(ConfigParamDescr.VOLUME_NUMBER.getKey(), "42");

    String auid1 = PluginManager.generateAuId(PID1, p1);
    String auid2 = PluginManager.generateAuId(PID1, p2);
    Configuration config = addAuTree(null, auid1, p1);
    p2.put(PluginManager.AU_PARAM_DISPLAY_NAME, "A name");
    config = addAuTree(config, auid2, p2);

    Map auagreemap = new HashMap();
    auagreemap.put(auid1, "zippity agree map 1");
    auagreemap.put(auid2, "doodah agree map 2");

    File file = writeZipBackup(config, auagreemap);
    InputStream in = new FileInputStream(file);
    RemoteApi.BatchAuStatus bas = rapi.processSavedConfig(in);
    RemoteApi.BackupInfo bi = bas.getBackupInfo();
    try {
      List statlist = bas.getStatusList();
      assertEquals(2, statlist.size());

      assertEntry(p1, bas, auid1);
      assertEntry(p2, bas, auid2, "A name");

      assertNotNull(bi);
      assertNotNull(bi.getAuDir(auid1));
      assertNotNull(bi.getAuDir(auid2));

      Configuration addConfig = ConfigManager.newConfiguration();

      for (Iterator iter = bas.getStatusList().iterator(); iter.hasNext(); ) {
	RemoteApi.BatchAuStatus.Entry ent =
	  (RemoteApi.BatchAuStatus.Entry)iter.next();
	String auid = ent.getAuId();
	String prefix = PluginManager.PARAM_AU_TREE + "." +
	  PluginManager.configKeyFromAuId(auid);
	addConfig.addAsSubTree(ent.getConfig(), prefix);
      }

      idMgr.resetAgreeMap();
      RemoteApi.BatchAuStatus addedbas =
	rapi.batchAddAus(RemoteApi.BATCH_ADD_ADD, addConfig, bi);
      ArchivalUnit au1 = mpm.getAuFromIdIfExists(auid1);
      assertNotNull(au1);
      assertEquals("zippity agree map 1", idMgr.getAgreeMap(au1));
      ArchivalUnit au2 = mpm.getAuFromIdIfExists(auid2);
      assertNotNull(au2);
      assertEquals("doodah agree map 2", idMgr.getAgreeMap(au2));
    } finally {
      if (bi != null) bi.delete();
    }
  }

  // ensure that we buffer up enough to reset the stream after checking the
  // first line
  public void testProcessSavedConfigLarge()  throws Exception {
    int loop = 200;

    Configuration config = null;
    for (int ix = 0; ix < loop; ix++) {
      Properties p = new Properties();
      p.put(ConfigParamDescr.BASE_URL.getKey(), "http://foo.bar/");
      p.put(ConfigParamDescr.VOLUME_NUMBER.getKey(), Integer.toString(ix));
      String auid = PluginManager.generateAuId(PID1, p);
      config = addAuTree(config, auid, p);
    }
    File file = toFile(config, FileTestUtil.tempFile("saved1"), true);
    InputStream in = new FileInputStream(file);
    RemoteApi.BatchAuStatus bas = rapi.processSavedConfig(in);
    List statlist = bas.getStatusList();
    assertEquals(loop, statlist.size());
  }

  public void testProcessSavedConfigNoPlugin()  throws Exception {
    Properties p1 = new Properties();
    p1.put(ConfigParamDescr.BASE_URL.getKey(), "http://foo.bar/");
    p1.put(ConfigParamDescr.VOLUME_NUMBER.getKey(), "7");
    Properties p2 = new Properties();
    p2.put(ConfigParamDescr.BASE_URL.getKey(), "http://example.com/");
    p2.put(ConfigParamDescr.VOLUME_NUMBER.getKey(), "42");
    String auid1 = PluginManager.generateAuId(PID1 + "bogus", p1);
    String auid2 = PluginManager.generateAuId(PID1, p2);
    p1.put(PluginManager.AU_PARAM_DISPLAY_NAME, "A name");
    Configuration config = addAuTree(null, auid1, p1);
    config = addAuTree(config, auid2, p2);

    Map auagreemap = new HashMap();
    auagreemap.put(auid1, "zippity agree map 1");
    auagreemap.put(auid2, "doodah agree map 2");

    File file = writeZipBackup(config, auagreemap);
    InputStream in = new FileInputStream(file);
    RemoteApi.BatchAuStatus bas = rapi.processSavedConfig(in);
    RemoteApi.BackupInfo bi = bas.getBackupInfo();
    try {
      List statlist = bas.getStatusList();
      assertEquals(2, statlist.size());

      assertEntry(p2, bas, auid2);

      RemoteApi.BatchAuStatus.Entry ent1 = findEntry(bas, auid1);
      assertNotNull("No entry for auid " + auid1, ent1);
      assertEquals("A name", ent1.getName());
      assertNull(ent1.getConfig());
    } finally {
      if (bi != null) bi.delete();
    }
  }


  class Pair {
    Object one, two;
    Pair(Object one, Object two) {
      this.one = one;
      this.two = two;
    }
  }

  // Fake PluginManager, should be completely mock, throwing on any
  // unimplemented mock method, but must extend PluginManager because
  // there's no separate interface
  class MyMockPluginManager extends PluginManager {
    List actions = new ArrayList();
    List allAus;
    List inactiveAuIds;

    Map storedConfigs = new HashMap();
    Map currentConfigs = new HashMap();

    void mockInit() {
      MockArchivalUnit mau1 = new MockArchivalUnit();
      mau1.setAuId(AUID1);
      putAuInMap(mau1);
      MockPlugin mp1 = new MockPlugin();
      mp1.setPluginId(PID1);
      setPlugin(pluginKeyFromId(mp1.getPluginId()), mp1);
    }

    void setStoredConfig(String auid, Configuration config) {
      storedConfigs.put(auid, config);
    }

    void setCurrentConfig(String auid, Configuration config) {
      currentConfigs.put(auid, config);
    }

    void setAllAus(List allAus) {
      this.allAus = allAus;
    }

    void setInactiveAuIds(List inactiveAuIds) {
      this.inactiveAuIds = inactiveAuIds;
    }

    public ArchivalUnit createAndSaveAuConfiguration(Plugin plugin,
						     Configuration auConf)
    throws ArchivalUnit.ConfigurationException {
      actions.add(new Pair(plugin, auConf));
      MockArchivalUnit mau = new MockArchivalUnit();
      mau.setPlugin(plugin);
      String auid =PluginManager.generateAuId(plugin, auConf);
      mau.setAuId(auid);
      mau.setConfiguration(auConf);
      putAuInMap(mau);
      return mau;
    }

    public void setAndSaveAuConfiguration(ArchivalUnit au,
					  Configuration auConf)
	throws ArchivalUnit.ConfigurationException {
      actions.add(new Pair(au, auConf));
      au.setConfiguration(auConf);
    }

    public void deleteAuConfiguration(String auid) {
      actions.add(new Pair("Delete", auid));
    }

    public void deleteAuConfiguration(ArchivalUnit au) {
      actions.add(new Pair("Delete", au));
    }

    public void deactivateAuConfiguration(ArchivalUnit au) {
      actions.add(new Pair("Deactivate", au));
    }

    public Configuration getStoredAuConfigurationAsConfiguration(String auid) {
      return (Configuration)storedConfigs.get(auid);
    }

    public Configuration getCurrentAuConfiguration(String auid) {
      return (Configuration)currentConfigs.get(auid);
    }

    public List getAllAus() {
      return allAus;
    }

    public Collection getInactiveAuIds() {
      return inactiveAuIds;
    }
  }
  static class MyIdentityManager extends MockIdentityManager {
    private Map agreeMapContents = new HashMap();

    void setAgreeMap(ArchivalUnit au, String content) {
      agreeMapContents.put(au, content);
    }

    String getAgreeMap(ArchivalUnit au) {
      return (String)agreeMapContents.get(au);
    }

    void resetAgreeMap() {
      agreeMapContents.clear();
    }

    protected void setupLocalIdentities() {
      // do nothing
    }
    public boolean hasAgreeMap(ArchivalUnit au) {
      return agreeMapContents.containsKey(au);
    }
    public void writeIdentityAgreementTo(ArchivalUnit au, OutputStream out)
	throws IOException {
      String s = getAgreeMap(au);
      if (s != null) {
	StringUtil.toOutputStream(out, s);
      }
    }
    public void readIdentityAgreementFrom(ArchivalUnit au, InputStream in)
	throws IOException {
      log.debug("Setting agreement for " + au.getName());
      setAgreeMap(au, StringUtil.fromInputStream(in));
    }

  }

  public void testBackupNone()
      throws Exception {
    writeAuConfigFile("org.lockss.au.FooPlugin.k~v.k=v\n");
    Properties p = new Properties();
    p.put(ConfigManager.PARAM_PLATFORM_ADMIN_EMAIL, "foo@bar");
    p.put(ConfigManager.PARAM_PLATFORM_FQDN, "lockss42.example.com");
    ConfigurationUtil.addFromProps(p);
    MockMailService mgr = new MockMailService();
    getMockLockssDaemon().setMailService(mgr);
    rapi.createConfigBackupFile(RemoteApi.BackupFileDisposition.None);
    assertEmpty("No mail was sent", mgr.getRecs());
  }

  public void testBackupKeep(RemoteApi.BackupFileDisposition bdf)
      throws Exception {
    writeAuConfigFile("org.lockss.au.FooPlugin.k~v.k=v\n");
    String dir = setUpDiskSpace();
    Properties p = new Properties();
    p.put(ConfigManager.PARAM_PLATFORM_ADMIN_EMAIL, "foo@bar");
    p.put(ConfigManager.PARAM_PLATFORM_FQDN, "lockss42.example.com");
    ConfigurationUtil.addFromProps(p);
    MockMailService mgr = new MockMailService();
    getMockLockssDaemon().setMailService(mgr);
    rapi.createConfigBackupFile(bdf);
    File expFile = new File(new File(dir, "backup"),
			    RemoteApi.DEFAULT_BACKUP_FILENAME);
    assertTrue(expFile.exists());
    // zip file should start with "PK"
    InputStream is = new FileInputStream(expFile);
    assertMatchesRE("^PK", StringUtil.fromInputStream(is));
  }

  public void testBackupKeep() throws Exception {
    testBackupKeep(RemoteApi.BackupFileDisposition.Keep);
    MockMailService mgr =
      (MockMailService)getMockLockssDaemon().getMailService();
    assertEmpty("Mail shouldn't have been sent", mgr.getRecs());
  }

  public void testBackupMailAndKeep() throws Exception {
    testBackupKeep(RemoteApi.BackupFileDisposition.MailAndKeep);
    MockMailService mgr =
      (MockMailService)getMockLockssDaemon().getMailService();
    assertNotEmpty("Mail should have been sent", mgr.getRecs());
  }

  void assertBackupMsg(MimeMessage msg, String fqdn,
		       String from, String to,
		       String expectedExt)
      throws IOException, javax.mail.MessagingException {
    assertNotNull(msg);
    assertEquals("LOCKSS box " + fqdn + " <" + from + ">",
		 msg.getHeader("From"));
    assertEquals(to, msg.getHeader("To"));
    assertEquals("Backup file for LOCKSS box " + fqdn + "",
		 msg.getHeader("Subject"));
    assertMatchesRE("^\\w\\w\\w, ",
		    msg.getHeader("Date"));

    javax.mail.internet.MimeBodyPart[] parts = msg.getParts();
    assertEquals(2, parts.length);
    assertMatchesRE("attached file is a backup",
		    (String)parts[0].getContent());
    assertMatchesRE( "retrieve the file\nat http://" + fqdn +
		      ":8081/BatchAuConfig\\?lockssAction=SelectBackup ",
		    (String)parts[0].getContent());
    assertMatchesRE("LOCKSS_Backup_.*\\." + expectedExt,
		    parts[1].getFileName());
    // zip file should start with "PK"
    assertMatchesRE("^PK",
		    StringUtil.fromInputStream(parts[1].getInputStream()));
  }

  public void testBackupEmail(String extParam, String expectedExt)
      throws Exception {
    writeAuConfigFile("org.lockss.au.FooPlugin.k~v.k=v\n");
    Properties p = new Properties();
    if (extParam != null) {
      p.put(RemoteApi.PARAM_BACKUP_FILE_EXTENSION, extParam);
    }
    p.put(ConfigManager.PARAM_PLATFORM_ADMIN_EMAIL, "foo@bar");
    p.put(ConfigManager.PARAM_PLATFORM_FQDN, "lockss42.example.com");
    assertEquals(null, Configuration.getPlatformHostname());
    ConfigurationUtil.addFromProps(p);
    assertEquals("lockss42.example.com", Configuration.getPlatformHostname());
    MockMailService mgr = new MockMailService();
    getMockLockssDaemon().setMailService(mgr);
    rapi.createConfigBackupFile(RemoteApi.BackupFileDisposition.Mail);
    assertEquals("No mail was sent", 1, mgr.getRecs().size());
    MockMailService.Rec rec = mgr.getRec(0);
    MimeMessage msg = (MimeMessage)rec.getMsg();
    try {
      assertBackupMsg(msg, "lockss42.example.com", "foo@bar", "foo@bar",
		      expectedExt);
    } finally {
      msg.delete(true);
    }
  }

  public void testBackupEmailDefaultParams() throws Exception {
    testBackupEmail(null, "zip");
  }

  public void testBackupEmailSet() throws Exception {
    testBackupEmail("bak", "bak");
  }

  public void testBackupEmailOverride() throws Exception {
    writeAuConfigFile("org.lockss.au.FooPlugin.k~v.k=v\n");
    Properties p = new Properties();
//     p.put("org.lockss.backupEmail.enabled", "true");
    p.put(ConfigManager.PARAM_PLATFORM_ADMIN_EMAIL, "foo@bar");
    p.put(ConfigManager.PARAM_PLATFORM_FQDN, "lockss42.example.com");
    p.put(RemoteApi.PARAM_BACKUP_EMAIL_RECIPIENT, "rrr@ccc");
    p.put(RemoteApi.PARAM_BACKUP_EMAIL_FROM, "fff@ccc");
    p.put(RemoteApi.PARAM_BACKUP_EMAIL_SENDER, "xxx@ccc");
    ConfigurationUtil.addFromProps(p);
    MockMailService mgr = new MockMailService();
    getMockLockssDaemon().setMailService(mgr);
    rapi.createConfigBackupFile(RemoteApi.BackupFileDisposition.Mail);
    MockMailService.Rec rec = mgr.getRec(0);
    MimeMessage msg = (MimeMessage)rec.getMsg();
    try {
      assertNotNull(msg);
      assertEquals("fff@ccc", msg.getHeader("From"));
      assertEquals("rrr@ccc", msg.getHeader("To"));
      assertEquals("Backup file for LOCKSS box lockss42.example.com",
		   msg.getHeader("Subject"));

      javax.mail.internet.MimeBodyPart[] parts = msg.getParts();
      assertEquals(2, parts.length);
      assertMatchesRE("attached file is a backup",
		      (String)parts[0].getContent());
      assertMatchesRE("LOCKSS_Backup_.*\\.zip", parts[1].getFileName());
      // zip file should start with "PK"
      assertMatchesRE("^PK",
		      StringUtil.fromInputStream(parts[1].getInputStream()));
    } finally {
      msg.delete(true);
    }
  }

}
