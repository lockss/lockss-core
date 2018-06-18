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

  public void testStoreAuEmptyState() throws Exception {
    HashSet strCol = new HashSet();
    strCol.add("test");
    AuState origState = new AuState(mau, repository);
    repository.storeAuState(origState);
    AuState loadedState = repository.loadAuState();
    assertEquals(-1, loadedState.getLastCrawlTime());
    assertEquals(-1, loadedState.getLastCrawlAttempt());
    assertEquals(-1, loadedState.getLastCrawlResult());
    assertEquals("Unknown code -1", loadedState.getLastCrawlResultMsg());
    assertEquals(-1, loadedState.getLastTopLevelPollTime());
    assertEquals(-1, loadedState.getLastPollStart());
    assertEquals(-1, loadedState.getLastPollResult());
    assertEquals(null, loadedState.getLastPollResultMsg());

    assertEquals(-1, loadedState.getLastPoPPoll());
    assertEquals(-1, loadedState.getLastPoPPollResult());
    assertEquals(-1, loadedState.getLastLocalHashScan());
    assertEquals(0, loadedState.getNumAgreePeersLastPoR());
    assertEquals(0, loadedState.getNumWillingRepairers());
    assertEquals(0, loadedState.getNumCurrentSuspectVersions());
    assertEmpty(loadedState.getCdnStems());
    loadedState.addCdnStem("http://this.is.new/");
    assertEquals(ListUtil.list("http://this.is.new/"), loadedState.getCdnStems());
    loadedState.addCdnStem("http://this.is.new/");
    assertEquals(ListUtil.list("http://this.is.new/"), loadedState.getCdnStems());

    assertEquals(0, loadedState.getPollDuration());
    assertEquals(-1, loadedState.getAverageHashDuration());
    assertEquals(0, loadedState.getClockssSubscriptionStatus());
    assertEquals(null, loadedState.getAccessType());
    assertEquals(SubstanceChecker.State.Unknown, loadedState.getSubstanceState());
    assertEquals(null, loadedState.getFeatureVersion(Plugin.Feature.Substance));
    assertEquals(null, loadedState.getFeatureVersion(Plugin.Feature.Metadata));
    assertEquals(-1, loadedState.getLastMetadataIndex());
    assertEquals(0, loadedState.getLastContentChange());
    assertEquals(mau.getAuId(), loadedState.getArchivalUnit().getAuId());
  }

  public void testStoreAuState() throws Exception {
    HashSet strCol = new HashSet();
    strCol.add("test");
    AuState origState = new AuState(mau,
				    123000, 123123, 41, "woop woop",
				    -1, -1, -1, "deep woop", -1,
				    321000, 222000, 3, "pollres", 12345,
				    965832931,456000, strCol,
				    AuState.AccessType.OpenAccess,
				    2, 1.0, 1.0,
				    SubstanceChecker.State.Yes,
				    "SubstVer3", "MetadatVer7", 111444,
				    12345,
				    111222, // lastPoPPoll
				    7, // lastPoPPollResult
				    222333, // lastLocalHashScan
				    444777, // numAgreePeersLastPoR
				    777444, // numWillingRepairers
				    747474, // numCurrentSuspectVersions
				    ListUtil.list("http://hos.t/pa/th"),
				    repository);

    assertEquals("SubstVer3",
		 origState.getFeatureVersion(Plugin.Feature.Substance));
    assertEquals("MetadatVer7",
		 origState.getFeatureVersion(Plugin.Feature.Metadata));
    assertEquals(111444, origState.getLastMetadataIndex());

    repository.storeAuState(origState);

    String filePath = OldLockssRepositoryImpl.mapAuToFileLocation(tempDirPath,
							       mau);
    filePath += HistoryRepositoryImpl.AU_FILE_NAME;
    File xmlFile = new File(filePath);
    assertTrue(xmlFile.exists());

    origState = null;
    AuState loadedState = repository.loadAuState();
    assertEquals(123000, loadedState.getLastCrawlTime());
    assertEquals(123123, loadedState.getLastCrawlAttempt());
    assertEquals(41, loadedState.getLastCrawlResult());
    assertEquals("woop woop", loadedState.getLastCrawlResultMsg());
    assertEquals(321000, loadedState.getLastTopLevelPollTime());
    assertEquals(222000, loadedState.getLastPollStart());
    assertEquals(3, loadedState.getLastPollResult());
    assertEquals("Inviting Peers", loadedState.getLastPollResultMsg());

    assertEquals(111222, loadedState.getLastPoPPoll());
    assertEquals(7, loadedState.getLastPoPPollResult());
    assertEquals(222333, loadedState.getLastLocalHashScan());

    assertEquals(444777, loadedState.getNumAgreePeersLastPoR());
    assertEquals(777444, loadedState.getNumWillingRepairers());
    assertEquals(747474, loadedState.getNumCurrentSuspectVersions());
    assertEquals(ListUtil.list("http://hos.t/pa/th"),
		 loadedState.getCdnStems());
    loadedState.addCdnStem("http://this.is.new/");
    assertEquals(ListUtil.list("http://hos.t/pa/th", "http://this.is.new/"),
		 loadedState.getCdnStems());

    assertEquals(12345, loadedState.getPollDuration());
    assertEquals(965832931, loadedState.getAverageHashDuration());
    assertEquals(2, loadedState.getClockssSubscriptionStatus());
    assertEquals(AuState.AccessType.OpenAccess, loadedState.getAccessType());
    assertEquals(SubstanceChecker.State.Yes, loadedState.getSubstanceState());
    assertEquals("SubstVer3",
		 loadedState.getFeatureVersion(Plugin.Feature.Substance));
    assertEquals("MetadatVer7",
		 loadedState.getFeatureVersion(Plugin.Feature.Metadata));
    assertEquals(111444, loadedState.getLastMetadataIndex());
    assertEquals(12345, loadedState.getLastContentChange());
    assertEquals(mau.getAuId(), loadedState.getArchivalUnit().getAuId());

    // check crawl urls
    Collection col = loadedState.getCrawlUrls();
    Iterator colIter = col.iterator();
    assertTrue(colIter.hasNext());
    assertEquals("test", colIter.next());
    assertFalse(colIter.hasNext());
  }

  public void testStoreDamagedNodeSet() throws Exception {
    DamagedNodeSet damNodes = new DamagedNodeSet(mau, repository);
    damNodes.nodesWithDamage.add("test1");
    damNodes.nodesWithDamage.add("test2");
    damNodes.cusToRepair.put("cus1", ListUtil.list("cus1-1", "cus1-2"));
    damNodes.cusToRepair.put("cus2", ListUtil.list("cus2-1"));
    assertTrue(damNodes.containsWithDamage("test1"));
    assertTrue(damNodes.containsWithDamage("test2"));
    assertFalse(damNodes.containsWithDamage("test3"));

    repository.storeDamagedNodeSet(damNodes);
    String filePath = OldLockssRepositoryImpl.mapAuToFileLocation(tempDirPath,
							       mau);
    filePath += HistoryRepositoryImpl.DAMAGED_NODES_FILE_NAME;
    File xmlFile = new File(filePath);
    assertTrue(xmlFile.exists());

    damNodes = null;
    damNodes = repository.loadDamagedNodeSet();
    // check damage
    assertTrue(damNodes.containsWithDamage("test1"));
    assertTrue(damNodes.containsWithDamage("test2"));
    assertFalse(damNodes.containsWithDamage("test3"));

    MockCachedUrlSet mcus1 = new MockCachedUrlSet("cus1");
    MockCachedUrlSet mcus2 = new MockCachedUrlSet("cus2");

    // check repairs
    assertTrue(damNodes.containsToRepair(mcus1, "cus1-1"));
    assertTrue(damNodes.containsToRepair(mcus1, "cus1-2"));
    assertFalse(damNodes.containsToRepair(mcus1, "cus2-1"));
    assertTrue(damNodes.containsToRepair(mcus2, "cus2-1"));
    assertEquals(mau.getAuId(), damNodes.theAu.getAuId());

    // check remove
    damNodes.removeFromRepair(mcus1, "cus1-1");
    assertFalse(damNodes.containsToRepair(mcus1, "cus1-1"));
    assertTrue(damNodes.containsToRepair(mcus1, "cus1-2"));
    damNodes.removeFromRepair(mcus1, "cus1-2");
    assertFalse(damNodes.containsToRepair(mcus1, "cus1-2"));
    assertNull(damNodes.cusToRepair.get(mcus1));

    // check remove from damaged nodes
    damNodes.removeFromDamage("test1");
    damNodes.removeFromDamage("test2");
    repository.storeDamagedNodeSet(damNodes);
    damNodes = repository.loadDamagedNodeSet();
    assertNotNull(damNodes);
    assertFalse(damNodes.containsWithDamage("test1"));
    assertFalse(damNodes.containsWithDamage("test2"));
  }

  public void testStoreOverwrite() throws Exception {
    AuState auState = new AuState(mau,
				  123, // lastCrawlTime
				  321, // lastCrawlAttempt
				  -1, // lastCrawlResult
				  null, // lastCrawlResultMsg,
				  -1, // lastDeepCrawlTime
				  -1, // lastDeepCrawlAttempt
				  -1, // lastDeepCrawlResult
				  null, // lastDeepCrawlResultMsg,
				  -1, // lastDeepCrawlDepth
				  321, // lastTopLevelPoll
				  333, // lastPollStart
				  -1, // lastPollresult
				  null, // lastPollresultMsg
				  0, // pollDuration
        -1, //hashDuration
				  -1, // lastTreeWalk
				  null, // crawlUrls
				  null, // accessType
				  1, // clockssSubscriptionState
				  1.0, // v3Agreement
				  1.0, // highestV3Agreement
				  SubstanceChecker.State.Unknown,
				  null, // substanceVersion
				  null, // metadataVersion
				  -1, // lastMetadataIndex
				  0, // lastContentChange
				  444, // lastPoPPoll
				  8, // lastPoPPollResult
				  -1, // lastLocalHashScan
				  27, // numAgreePeersLastPoR
				  72, // numWillingRepirers
				  19, // numCurrentSuspectVersions
				  ListUtil.list("http://foo/"), // cdnStems
				  repository);

    repository.storeAuState(auState);
    String filePath = OldLockssRepositoryImpl.mapAuToFileLocation(tempDirPath,
							       mau);
    filePath += HistoryRepositoryImpl.AU_FILE_NAME;
    File xmlFile = new File(filePath);
    FileInputStream fis = new FileInputStream(xmlFile);
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    StreamUtil.copy(fis, baos);
    fis.close();
    String expectedStr = baos.toString();

    auState = new AuState(mau,
			  1234, // lastCrawlTime
			  4321, // lastCrawlAttempt
			  -1, // lastCrawlResult
			  null, // lastCrawlResultMsg,
			  -1, // lastDeepCrawlTime
			  -1, // lastDeepCrawlAttempt
			  -1, // lastDeepCrawlResult
			  null, // lastDeepCrawlResultMsg,
			  -1, // lastDeepCrawlDepth
			  4321, // lastTopLevelPoll
			  5555, // lastPollStart
			  -1, // lastPollresult
			  null, // lastPollresultMsg
			  0, // pollDuration
        -1,
			  -1, // lastTreeWalk
			  null, // crawlUrls
			  null, // accessType
			  1, // clockssSubscriptionState
			  1.0, // v3Agreement
			  1.0, // highestV3Agreement
			  SubstanceChecker.State.Unknown,
			  null, // substanceVersion
			  null, // metadataVersion
			  -1, // lastMetadataIndex
			  0, // lastContentChange
			  -1, // lastPoPPoll
			  -1, // lastPoPPollResult
			  -1, // lastLocalHashScan
			  13, // numAgreePeersLastPoR
			  31, // numWillingRepairers
			  91, // numCurrentSuspectVersions
			  ListUtil.list("http://foo/"), // cdnStems
			  repository);
    repository.storeAuState(auState);
    assertEquals(1234, auState.getLastCrawlTime());
    assertEquals(4321, auState.getLastCrawlAttempt());
    assertEquals(4321, auState.getLastTopLevelPollTime());
    assertEquals(5555, auState.getLastPollStart());
    assertEquals(13, auState.getNumAgreePeersLastPoR());
    assertEquals(31, auState.getNumWillingRepairers());
    assertEquals(91, auState.getNumCurrentSuspectVersions());
    assertEquals(mau.getAuId(), auState.getArchivalUnit().getAuId());
    assertEquals(ListUtil.list("http://foo/"), auState.getCdnStems());

    fis = new FileInputStream(xmlFile);
    baos = new ByteArrayOutputStream(expectedStr.length());
    StreamUtil.copy(fis, baos);
    fis.close();
    log.info(baos.toString());

    auState = null;
    auState = repository.loadAuState();
    assertEquals(1234, auState.getLastCrawlTime());
    assertEquals(4321, auState.getLastCrawlAttempt());
    assertEquals(4321, auState.getLastTopLevelPollTime());
    assertEquals(5555, auState.getLastPollStart());
    assertEquals(13, auState.getNumAgreePeersLastPoR());
    assertEquals(31, auState.getNumWillingRepairers());
    assertEquals(91, auState.getNumCurrentSuspectVersions());
    assertEquals(mau.getAuId(), auState.getArchivalUnit().getAuId());

    auState = new AuState(mau,
			  123, // lastCrawlTime
			  321, // lastCrawlAttempt
			  -1, // lastCrawlResult
			  null, // lastCrawlResultMsg,
			  -1, // lastDeepCrawlTime
			  -1, // lastDeepCrawlAttempt
			  -1, // lastDeepCrawlResult
			  null, // lastDeepCrawlResultMsg,
			  -1, // lastDeepCrawlDepth
			  321, // lastTopLevelPoll
			  333, // lastPollStart
			  -1, // lastPollresult
			  null, // lastPollresultMsg
			  0, // pollDuration
        -1,
			  -1, // lastTreeWalk
			  null, // crawlUrls
			  null, // accessType
			  1, // clockssSubscriptionState
			  1.0, // v3Agreement
			  1.0, // highestV3Agreement
			  SubstanceChecker.State.Unknown,
			  null, // substanceVersion
			  null, // metadataVersion
			  -1, // lastMetadataIndex
			  0, // lastContentChange
			  444, // lastPoPPoll
			  8, // lastPoPPollResult
			  -1, // lastLocalHashScan
			  27, // numAgreePeersLastPoR
			  72, // numWillingRepairers
			  19, // numCurrentSuspectVersions
			  ListUtil.list("http://foo/"), // cdnStems
			  repository);
    repository.storeAuState(auState);
    fis = new FileInputStream(xmlFile);
    baos = new ByteArrayOutputStream(expectedStr.length());
    StreamUtil.copy(fis, baos);
    fis.close();
    assertEquals(expectedStr, baos.toString());
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
