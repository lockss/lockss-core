/*
 * $Id$
 */

/*

Copyright (c) 2000-2003 Board of Trustees of Leland Stanford Jr. University,
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

package org.lockss.remote;

import java.io.*;
import java.util.*;
import org.lockss.daemon.*;
import org.lockss.config.*;
import org.lockss.config.db.*;
import org.lockss.plugin.*;
import org.lockss.util.*;
import org.lockss.test.*;

/**
 * Test class for org.lockss.remote.AuProxy
 */
public class TestAuProxy extends LockssTestCase {

  static final String AUID1 = "AUID_1";
  static final String PID1 = "PID_1";

  MockLockssDaemon daemon;
  MyMockRemoteApi mrapi;
  MockArchivalUnit mau;

  public void setUp() throws Exception {
    super.setUp();
    daemon = getMockLockssDaemon();
    daemon.setUpFastAuConfig();
    setUpDiskSpace();
    daemon.startManagers(PluginManager.class);
    mrapi = new MyMockRemoteApi();
    daemon.setRemoteApi(mrapi);
    mrapi.initService(daemon);
    mrapi.startService();
    daemon.setDaemonInited(true);
    mau = new MockArchivalUnit();
  }

  public void tearDown() throws Exception {
    daemon.stopDaemon();
    super.tearDown();
  }

  InMemoryConfigStore getConfigStore() {
    return daemon.getInMemoryConfigStore();
  }

  public void testPresentAuFromAuid() throws Exception {
    String auid = "p1&a2";
    mau.setAuId(auid);
    mrapi.setAuFromId(auid, mau);
    MockPlugin mp = new MockPlugin();
    mrapi.setPluginFromId("p1", mp);
    AuProxy aup = new AuProxy(auid, mrapi);
    assertSame(mau, aup.getAu());
    assertTrue(aup.isActiveAu());
    PluginProxy pp = aup.getPlugin();
    assertSame(mp, pp.getPlugin());
  }

  public void testPresentAuFromAu() throws Exception {
    String auid = "p2&a2";
    mau.setAuId(auid);
    mrapi.setAuFromId(auid, mau);
    MockPlugin mp = new MockPlugin();
    mrapi.setPluginFromId("p2", mp);
    AuProxy aup = new AuProxy(mau, mrapi);
    assertSame(mau, aup.getAu());
    assertTrue(aup.isActiveAu());
    PluginProxy pp = aup.getPlugin();
    assertSame(mp, pp.getPlugin());
  }

  public void testNoSuchAu() throws Exception {
    try {
      AuProxy aup = new AuProxy("p1&a2", mrapi);
      fail("Create from missing auid should throw");
    } catch (AuProxy.NoSuchAU e) {
    }
  }

  public void testAbsentActive() throws Exception {
    String auid = "p1&a3";
    getConfigStore().addArchivalUnitConfiguration(auid,
					     MapUtil.map("base_url", "foo"));
    AuProxy aup = new AuProxy(auid, mrapi);
    assertTrue(aup.isActiveAu());
  }

  public void testAbsentInactive() throws Exception {
    String auid = "p1&a3";
    getConfigStore().addArchivalUnitConfiguration(auid,
					     MapUtil.map("base_url", "foo",
							 "reserved.disabled", "true"));
    AuProxy aup = new AuProxy(auid, mrapi);
    assertFalse(aup.isActiveAu());
  }

  class MyMockRemoteApi extends RemoteApi {
    Map pluginmap = new HashMap();
    Map aumap = new HashMap();

    Plugin getPluginFromId(String pluginid) {
      return (Plugin)pluginmap.get(pluginid);
    }

    void setPluginFromId(String pluginid, Plugin plugin) {
      pluginmap.put(pluginid, plugin);
    }

    @Override
    ArchivalUnit getAuFromId(String auid) {
      return (ArchivalUnit)aumap.get(auid);
    }

    @Override
    public ArchivalUnit getAuFromIdIfExists(String auid) {
      return (ArchivalUnit)aumap.get(auid);
    }

    void setAuFromId(String auid, ArchivalUnit au) {
      aumap.put(auid, au);
    }
  }
}
