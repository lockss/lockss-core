/*

Copyright (c) 2000, Board of Trustees of Leland Stanford Jr. University.
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

package org.lockss.state;

import java.io.*;
import java.net.MalformedURLException;
import java.util.*;

import org.lockss.config.CurrentConfig;
import org.lockss.daemon.RangeCachedUrlSetSpec;
import org.lockss.plugin.*;
import org.lockss.poller.Vote;
import org.lockss.protocol.*;
import org.lockss.repository.OldLockssRepositoryImpl;
import org.lockss.test.*;
import org.lockss.util.*;

import junit.framework.Test;

public abstract class TestHistoryRepositoryImpl extends LockssTestCase {

  /**
   * <p>A version of {@link TestHistoryRepositoryImpl} that forces the
   * serialization compatibility mode to
   * {@link CXSerializer#CASTOR_MODE}.</p>
   * @author Thib Guicherd-Callin
   */
//   public static class WithCastor extends TestHistoryRepositoryImpl {
//     public void setUp() throws Exception {
//       super.setUp();
//       ConfigurationUtil.addFromArgs(CXSerializer.PARAM_COMPATIBILITY_MODE,
//                                     Integer.toString(CXSerializer.CASTOR_MODE));
//     }

//     public void testStoreAuState() throws Exception {
//       // Not bothering to update castor mapping file
//     }
//   }

  /**
   * <p>A version of {@link TestHistoryRepositoryImpl} that forces the
   * serialization compatibility mode to
   * {@link CXSerializer#XSTREAM_MODE}.</p>
   * @author Thib Guicherd-Callin
   */
  public static class WithXStream extends TestHistoryRepositoryImpl {
    public void setUp() throws Exception {
      super.setUp();
      ConfigurationUtil.addFromArgs(CXSerializer.PARAM_COMPATIBILITY_MODE,
                                    Integer.toString(CXSerializer.XSTREAM_MODE));
    }
    public void testStorePollHistories() {
      log.critical("Not executing this Castor-centric test."); // FIXME
    }
  }

  public static Test suite() {
    return variantSuites(TestHistoryRepositoryImpl.class);
  }

  private String tempDirPath;
  private HistoryRepositoryImpl repository;
  private MockLockssDaemon theDaemon;
  private MockArchivalUnit mau;
  private IdentityManager idmgr;
  private String idKey;
  private PeerIdentity testID1 = null;
  private PeerIdentity testID2 = null;

  public void setUp() throws Exception {
    super.setUp();
    theDaemon = getMockLockssDaemon();
    mau = new MockArchivalUnit(new MockPlugin(theDaemon));
    tempDirPath = getTempDir().getAbsolutePath() + File.separator;
    configHistoryParams(tempDirPath);
    repository = (HistoryRepositoryImpl)
        HistoryRepositoryImpl.createNewHistoryRepository(mau);
    repository.initService(theDaemon);
    repository.startService();
    if (idmgr == null) {
      idmgr = theDaemon.getIdentityManager();
      idmgr.startService();
    }
    testID1 = idmgr.stringToPeerIdentity("127.1.2.3");
    testID2 = idmgr.stringToPeerIdentity("127.4.5.6");
  }

  public void tearDown() throws Exception {
    if (idmgr != null) {
      idmgr.stopService();
      idmgr = null;
    }
    repository.stopService();
    super.tearDown();
  }

  /**
   *  Make sure that we have one (and only one) dated peer id set 
   */
  public void testGetNoAuPeerSet() {
    DatedPeerIdSet dpis1;
    DatedPeerIdSet dpis2;
    
    dpis1 = repository.getNoAuPeerSet();
    assertNotNull(dpis1);
    
    dpis2 = repository.getNoAuPeerSet();
    assertNotNull(dpis2);
    
    assertSame(dpis1, dpis2);
  }


  public static void configHistoryParams(String rootLocation)
    throws IOException {
    ConfigurationUtil.addFromArgs(HistoryRepositoryImpl.PARAM_HISTORY_LOCATION,
                                  rootLocation,
                                  OldLockssRepositoryImpl.PARAM_CACHE_LOCATION,
                                  rootLocation,
                                  IdentityManager.PARAM_LOCAL_IP,
                                  "127.0.0.7");
  }

}
