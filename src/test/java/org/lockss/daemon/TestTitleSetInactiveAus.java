/*

Copyright (c) 2000-2018 Board of Trustees of Leland Stanford Jr. University,
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

package org.lockss.daemon;

import java.util.*;
import org.lockss.config.*;
import org.lockss.plugin.*;
import org.lockss.test.*;
import org.lockss.util.*;

/**
 * This is the test class for org.lockss.daemon.TitleSetInactiveAus
 */

public class TestTitleSetInactiveAus extends LockssTestCase {
  private static Logger log = Logger.getLogger();

  PluginManager pluginMgr;
  MockPlugin mp;
  MyMockArchivalUnit mau1, mau2;
  TitleConfig tc1, tc2;
  ConfigParamDescr d1, d2;
  ConfigParamAssignment cpa1, cpa2, cpa3;

  public void setUp() throws Exception {
    super.setUp();
    setUpDiskSpace();
    getMockLockssDaemon().setIdentityManager(new org.lockss.protocol.MockIdentityManager());

    getMockLockssDaemon().setUpAuConfig();

    pluginMgr = getMockLockssDaemon().getPluginManager();
    pluginMgr.startService();
    getMockLockssDaemon().getRemoteApi().startService();
    String tempDir = getTempDir().getAbsolutePath();
    Properties props = new Properties();

    props.setProperty(ConfigManager.PARAM_PLATFORM_DISK_SPACE_LIST, tempDir);
    ConfigurationUtil.addFromProps(props);

    String key = PluginManager.pluginKeyFromId(MockPlugin.class.getName());
    pluginMgr.ensurePluginLoaded(key);
    mp = (MockPlugin)pluginMgr.getPlugin(key);

    d1 = new ConfigParamDescr("base_url");
    d2 = new ConfigParamDescr("volume");
    cpa1 = new ConfigParamAssignment(d1, "a");
    cpa2 = new ConfigParamAssignment(d2, "foo");
    cpa3 = new ConfigParamAssignment(d2, "bar");

    tc1 = new TitleConfig("auname1", mp);
    tc1.setParams(ListUtil.list(cpa1, cpa2));

    tc2 = new TitleConfig("title2", mp);
    tc2.setParams(ListUtil.list(cpa1, cpa3));

    mp.setTitleConfigMap(MapUtil.map(tc1.getDisplayName(), tc1,
				     tc2.getDisplayName(), tc2),
			 MapUtil.map(tc1.getAuId(pluginMgr), tc1,
				     tc2.getAuId(pluginMgr), tc2));

    mp.setAuConfigDescrs(ListUtil.list(d1, d2));
    mau1 = new MyMockArchivalUnit();
    mau2 = new MyMockArchivalUnit();
    mau1.setPlugin(mp);
    mau2.setPlugin(mp);
    mau1.setName("auname1");
    mau2.setName("title2");
    mau1.setConfiguration(ConfigurationUtil.fromArgs("base_url", "a",
						     "volume", "foo"));
    mau2.setConfiguration(ConfigurationUtil.fromArgs("base_url", "a",
						     "volume", "bar"));
  }

  public void tearDown() throws Exception {
    pluginMgr.stopService();
    getMockLockssDaemon().stopDaemon();
    super.tearDown();
  }

  public void test1() throws Exception {
    PluginTestUtil.registerArchivalUnit(mp, mau1);
    PluginTestUtil.registerArchivalUnit(mp, mau2);
    mau1.setTitleConfig(tc1);
    mau2.setTitleConfig(tc2);
    pluginMgr.updateAuInDatabase(mau1.getAuId(), mau1.getConfiguration());
    pluginMgr.deactivateAu(mau1);
    TitleSet ts = new TitleSetInactiveAus(getMockLockssDaemon());
    Collection set = ts.getTitles();
    assertSameElements(SetUtil.set(tc1), set);
    assertEquals(1, ts.countTitles(TitleSet.SET_REACTABLE));
    assertEquals(0, ts.countTitles(TitleSet.SET_ADDABLE));
    assertEquals(0, ts.countTitles(TitleSet.SET_DELABLE));

    TitleConfig fromSet = new ArrayList<TitleConfig>(set).get(0);
    assertSame(tc1, fromSet);
  }

  class MyMockArchivalUnit extends MockArchivalUnit {
    private TitleConfig titleConfig;
    public TitleConfig getTitleConfig() {
      return titleConfig;
    }
    public void setTitleConfig(TitleConfig tc) {
      titleConfig = tc;
    }
  }
}
