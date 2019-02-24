/*
 * $Id$
 */

/*

Copyright (c) 2000-2005 Board of Trustees of Leland Stanford Jr. University,
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

package org.lockss.protocol;

import java.io.File;
import java.util.*;
import java.net.UnknownHostException;
import org.lockss.app.*;
import org.lockss.util.*;
import org.lockss.util.net.IPAddr;
import org.lockss.test.*;

/** Test cases for org.lockss.protocol.IdentityManager that test
 * initialization and startup.  See TestIdentityManager for more
 * IdentityManager tests. */
public class TestIdentityManagerInit extends LockssTestCase {
  static int TEST_V3_PORT = 4456;
  static String TEST_LOCAL_ID = "TCP:[127.1.2.3]:123";

  Object testIdKey;

  private MockLockssDaemon theDaemon;
  private IdentityManager idmgr;

  public void setUp() throws Exception {
    super.setUp();
    theDaemon = getMockLockssDaemon();
  }

  public void configInit(boolean v3, boolean iddb) throws Exception {
    Properties p = new Properties();
    if (!v3) {
      throw new UnsupportedOperationException();
    }
    p.setProperty(IdentityManager.PARAM_LOCAL_V3_IDENTITY, TEST_LOCAL_ID);
    if (iddb) {
      String tempDirPath = getTempDir().getAbsolutePath() + File.separator;
      p.setProperty(IdentityManager.PARAM_IDDB_DIR, tempDirPath + "iddb");
    }
    ConfigurationUtil.setCurrentConfigFromProps(p);

    idmgr = theDaemon.getIdentityManager();
  }

  public void tearDown() throws Exception {
    if (idmgr != null) {
      idmgr.stopService();
      idmgr = null;
    }
    super.tearDown();
  }

  public void testLocalV3Identity() throws Exception {
    configInit(true, false);
    PeerIdentity p1 = idmgr.findPeerIdentity("tcp:[127.0.0.4]:9999");
    assertNotNull(p1);
    assertFalse(p1.isLocalIdentity());

    PeerIdentity p2 = idmgr.findPeerIdentity(TEST_LOCAL_ID);
    assertNotNull(p2);
    assertTrue(p2.isLocalIdentity());
    assertTrue(idmgr.isLocalIdentity(p2));

    assertNotSame(p1, p2);
    assertNotEquals(p1, p2);
  }

  // XXX
  // needs iddb reading tests

}
