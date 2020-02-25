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
import java.util.*;

import org.lockss.app.*;
import org.lockss.daemon.Crawler;
import org.lockss.plugin.*;
import org.lockss.poller.v3.V3Poller;
import org.lockss.poller.v3.V3Poller.PollVariant;
import org.lockss.test.*;
import org.lockss.log.*;
import org.lockss.util.*;
import org.lockss.util.io.LockssSerializable;
import org.lockss.util.time.TimeBase;

public class TestAuState extends LockssTestCase {
  L4JLogger log = L4JLogger.getLogger();
  MockLockssDaemon daemon;
  MyStateManager stateMgr;
  MockPlugin mplug;
  MockArchivalUnit mau;

  public void setUp() throws Exception {
    super.setUp();
    daemon = getMockLockssDaemon();

    stateMgr = daemon.setUpStateManager(new MyStateManager());
    mplug = new MockPlugin(daemon);
    mau = new MockArchivalUnit(mplug);
  }


  public void tearDown() throws Exception {
    TimeBase.setReal();
    super.tearDown();
  }

  /*
   * Abbreviate the verbose constructor
   */
  private AuState makeAuState(ArchivalUnit au,
			      long lastCrawlTime,
			      long lastCrawlAttempt,
			      long lastTopLevelPoll,
			      long lastPollStart,
			      long lastTreeWalk,
			      HashSet crawlUrls,
			      int clockssSubscriptionStatus,
			      double v3Agreement,
			      double highestV3Agreement,
			      StateManager stateMgr) {
    return new AuState(au,
		       lastCrawlTime,
		       lastCrawlAttempt,
		       -1,
		       null,
		       -1,
		       -1,
		       -1,
		       null,
		       -1,
		       lastTopLevelPoll,
		       lastPollStart,
		       -1,
		       null,
		       0,
        -1,
		       lastTreeWalk,
		       crawlUrls,
		       null,
		       clockssSubscriptionStatus,
		       v3Agreement,
		       highestV3Agreement,
		       SubstanceChecker.State.Unknown,
		       null,                          // substanceFeatureVersion
		       null,                          // metadataFeatureVersion
		       -1,                            // lastMetadataIndex
		       TimeBase.nowMs(),              // lastContentChange
		       -1,
		       -1,
		       -1,
		       -1, // numWillingRepairers
		       -1, // numCurrentSuspectVersions
		       0,
		       null,
		       true,
		       stateMgr);
  }

  int t1 = 456;
  int t2 = 12000;
  int t3 = 14000;
  int t4 = 17000;
  int t5 = 23000;
  int t6 = 25000;
  int t7 = 25001;

  public void testCrawlStarted() throws Exception {
    MyAuState aus = getMyAuState(mau);
    assertEquals(-1, aus.getLastCrawlTime());
    assertEquals(-1, aus.getLastCrawlAttempt());
    assertEquals(-1, aus.getLastCrawlResult());
    assertEquals("Unknown code -1", aus.getLastCrawlResultMsg());
    assertEquals(-1, aus.getLastDeepCrawlTime());
    assertEquals(-1, aus.getLastDeepCrawlAttempt());
    assertEquals(-1, aus.getLastDeepCrawlResult());
    assertEquals("Unknown code -1", aus.getLastDeepCrawlResultMsg());
    assertEquals(-1, aus.getLastDeepCrawlDepth());
    assertFalse(aus.isCrawlActive());
    assertFalse(aus.hasCrawled());
    assertEquals(0, stateMgr.getAuStateUpdateCount());

    TimeBase.setSimulated(t1);
    aus.newCrawlStarted();
    // these should now reflect the previous crawl, not the active one
    assertEquals(-1, aus.getLastCrawlTime());
    assertEquals(-1, aus.getLastCrawlAttempt());
    assertEquals(-1, aus.getLastCrawlResult());
    assertEquals("Unknown code -1", aus.getLastCrawlResultMsg());

    // not a deep crawl so these are unaffected
    assertEquals(-1, aus.getLastDeepCrawlTime());
    assertEquals(-1, aus.getLastDeepCrawlAttempt());
    assertEquals(-1, aus.getLastDeepCrawlResult());
    assertEquals("Unknown code -1", aus.getLastDeepCrawlResultMsg());
    assertEquals(-1, aus.getLastDeepCrawlDepth());

    assertTrue(aus.isCrawlActive());
    assertFalse(aus.hasCrawled());
    assertEquals(1, stateMgr.getAuStateUpdateCount());

    TimeBase.setSimulated(t2);
    aus.newCrawlFinished(Crawler.STATUS_ERROR, "Plorg");
    assertEquals(-1, aus.getLastCrawlTime());
    assertEquals(t1, aus.getLastCrawlAttempt());
    assertEquals(-1, aus.getLastDeepCrawlTime());
    assertEquals(-1, aus.getLastDeepCrawlAttempt());
    assertEquals(-1, aus.getLastDeepCrawlResult());
    assertEquals("Unknown code -1", aus.getLastDeepCrawlResultMsg());
    assertEquals(-1, aus.getLastDeepCrawlDepth());
    assertEquals(Crawler.STATUS_ERROR, aus.getLastCrawlResult());
    assertEquals("Plorg", aus.getLastCrawlResultMsg());
    assertFalse(aus.isCrawlActive());
    assertFalse(aus.hasCrawled());
    assertEquals(2, stateMgr.getAuStateUpdateCount());

    TimeBase.setSimulated(t3);
    aus.newCrawlFinished(Crawler.STATUS_SUCCESSFUL, "Syrah");
    assertEquals(t3, aus.getLastCrawlTime());
    assertEquals(t1, aus.getLastCrawlAttempt());
    assertEquals(-1, aus.getLastDeepCrawlTime());
    assertEquals(-1, aus.getLastDeepCrawlAttempt());
    assertEquals(-1, aus.getLastDeepCrawlResult());
    assertEquals("Unknown code -1", aus.getLastDeepCrawlResultMsg());
    assertEquals(-1, aus.getLastDeepCrawlDepth());
    assertEquals(Crawler.STATUS_SUCCESSFUL, aus.getLastCrawlResult());
    assertEquals("Syrah", aus.getLastCrawlResultMsg());
    assertFalse(aus.isCrawlActive());
    assertTrue(aus.hasCrawled());
    assertEquals(3, stateMgr.getAuStateUpdateCount());

    aus = aus.simulateStoreLoad();
    assertEquals(t3, aus.getLastCrawlTime());
    assertEquals(t1, aus.getLastCrawlAttempt());
    assertEquals(-1, aus.getLastDeepCrawlTime());
    assertEquals(-1, aus.getLastDeepCrawlAttempt());
    assertEquals(-1, aus.getLastDeepCrawlResult());
    assertEquals("Unknown code -1", aus.getLastDeepCrawlResultMsg());
    assertEquals(-1, aus.getLastDeepCrawlDepth());
    assertEquals(Crawler.STATUS_SUCCESSFUL, aus.getLastCrawlResult());
    assertEquals("Syrah", aus.getLastCrawlResultMsg());
    assertFalse(aus.isCrawlActive());
    assertTrue(aus.hasCrawled());

    TimeBase.setSimulated(t4-1);
    aus.deepCrawlStarted(999 /* unused */);
    TimeBase.setSimulated(t4);
    aus.newCrawlFinished(Crawler.STATUS_SUCCESSFUL, "Syrah", 43);
    assertEquals(t4, aus.getLastCrawlTime());
    assertEquals(t4-1, aus.getLastCrawlAttempt());
    assertEquals(t4, aus.getLastDeepCrawlTime());
    assertEquals(t4-1, aus.getLastDeepCrawlAttempt());
    assertEquals(43, aus.getLastDeepCrawlDepth());
    assertEquals(Crawler.STATUS_SUCCESSFUL, aus.getLastCrawlResult());
    assertEquals("Syrah", aus.getLastCrawlResultMsg());
    assertEquals(Crawler.STATUS_SUCCESSFUL, aus.getLastDeepCrawlResult());
    assertEquals("Syrah", aus.getLastDeepCrawlResultMsg());

    TimeBase.setSimulated(t5);
    aus.newCrawlStarted();
    assertEquals(t4, aus.getLastCrawlTime());
    assertEquals(t4-1, aus.getLastCrawlAttempt());
    assertEquals(t4, aus.getLastDeepCrawlTime());
    assertEquals(t4-1, aus.getLastDeepCrawlAttempt());
    assertEquals(Crawler.STATUS_SUCCESSFUL, aus.getLastCrawlResult());
    assertEquals("Syrah", aus.getLastCrawlResultMsg());
    assertEquals(Crawler.STATUS_SUCCESSFUL, aus.getLastDeepCrawlResult());
    assertEquals("Syrah", aus.getLastDeepCrawlResultMsg());
    assertTrue(aus.hasCrawled());

    TimeBase.setSimulated(t6);
    aus.newCrawlFinished(Crawler.STATUS_SUCCESSFUL, "Shiraz");
    assertEquals(t6, aus.getLastCrawlTime());
    assertEquals(t5, aus.getLastCrawlAttempt());
    assertEquals(t4, aus.getLastDeepCrawlTime());
    assertEquals(43, aus.getLastDeepCrawlDepth());
    assertEquals(Crawler.STATUS_SUCCESSFUL, aus.getLastCrawlResult());
    assertEquals("Shiraz", aus.getLastCrawlResultMsg());
    assertEquals(Crawler.STATUS_SUCCESSFUL, aus.getLastDeepCrawlResult());
    assertEquals("Syrah", aus.getLastDeepCrawlResultMsg());


    TimeBase.setSimulated(t6);
    aus.newCrawlFinished(Crawler.STATUS_FETCH_ERROR, "Syrah", 999);
    assertEquals(t6, aus.getLastCrawlTime());
    assertEquals(t5, aus.getLastCrawlAttempt());
    assertEquals(t4, aus.getLastDeepCrawlTime());
    assertEquals(43, aus.getLastDeepCrawlDepth());
  }

  public void testDaemonCrashedDuringCrawl() throws Exception {
    MyAuState aus = getMyAuState(mau);
    assertEquals(-1, aus.getLastCrawlTime());
    assertEquals(-1, aus.getLastCrawlAttempt());
    assertEquals(-1, aus.getLastCrawlResult());
    assertFalse(aus.isCrawlActive());

    TimeBase.setSimulated(t1);
    aus.newCrawlStarted();
    // these should now reflect the previous crawl, not the active one
    assertEquals(-1, aus.getLastCrawlTime());
    assertEquals(-1, aus.getLastCrawlAttempt());
    assertEquals(-1, aus.getLastCrawlResult());
    assertTrue(aus.isCrawlActive());

    TimeBase.setSimulated(t2);
    aus = aus.simulateStoreLoad();
    assertEquals(-1, aus.getLastCrawlTime());
    assertEquals(t1, aus.getLastCrawlAttempt());
    assertEquals(Crawler.STATUS_RUNNING_AT_CRASH, aus.getLastCrawlResult());
    assertFalse(aus.isCrawlActive());

    TimeBase.setSimulated(t3);
    aus.newCrawlStarted();
    assertEquals(-1, aus.getLastCrawlTime());
    assertEquals(t1, aus.getLastCrawlAttempt());
    assertEquals(Crawler.STATUS_RUNNING_AT_CRASH, aus.getLastCrawlResult());

    TimeBase.setSimulated(t4);
    aus.newCrawlFinished(Crawler.STATUS_SUCCESSFUL, "Plorg");
    assertEquals(t4, aus.getLastCrawlTime());
    assertEquals(t3, aus.getLastCrawlAttempt());
    assertEquals(Crawler.STATUS_SUCCESSFUL, aus.getLastCrawlResult());
    assertEquals(-1, aus.getLastDeepCrawlTime());
    assertEquals(-1, aus.getLastDeepCrawlAttempt());
    assertEquals("Plorg", aus.getLastCrawlResultMsg());
    assertEquals(-1, aus.getLastDeepCrawlResult());
    assertEquals("Unknown code -1", aus.getLastDeepCrawlResultMsg());
  }

  public void testDaemonCrashedDuringDeepCrawl() throws Exception {
    MyAuState aus = getMyAuState(mau);
    assertEquals(-1, aus.getLastCrawlTime());
    assertEquals(-1, aus.getLastCrawlAttempt());
    assertEquals(-1, aus.getLastCrawlResult());
    assertEquals("Unknown code -1", aus.getLastCrawlResultMsg());
    assertEquals(-1, aus.getLastDeepCrawlTime());
    assertEquals(-1, aus.getLastDeepCrawlAttempt());
    assertEquals(-1, aus.getLastDeepCrawlResult());
    assertEquals("Unknown code -1", aus.getLastDeepCrawlResultMsg());
    assertFalse(aus.isCrawlActive());

    TimeBase.setSimulated(t1);
    aus.deepCrawlStarted(444);
    // these should now reflect the previous crawl, not the active one
    assertEquals(-1, aus.getLastCrawlTime());
    assertEquals(-1, aus.getLastCrawlAttempt());
    assertEquals(-1, aus.getLastCrawlResult());
    assertEquals("Unknown code -1", aus.getLastCrawlResultMsg());
    assertEquals(-1, aus.getLastDeepCrawlTime());
    assertEquals(-1, aus.getLastDeepCrawlAttempt());
    assertEquals(-1, aus.getLastDeepCrawlResult());
    assertEquals("Unknown code -1", aus.getLastDeepCrawlResultMsg());

    assertTrue(aus.isCrawlActive());

    TimeBase.setSimulated(t2);
    aus = aus.simulateStoreLoad();
    assertEquals(-1, aus.getLastCrawlTime());
    assertEquals(t1, aus.getLastCrawlAttempt());
    assertEquals(Crawler.STATUS_RUNNING_AT_CRASH, aus.getLastCrawlResult());
    assertEquals("Interrupted by daemon exit", aus.getLastCrawlResultMsg());
    assertEquals(-1, aus.getLastDeepCrawlTime());
    assertEquals(456, aus.getLastDeepCrawlAttempt());
    assertEquals(Crawler.STATUS_RUNNING_AT_CRASH, aus.getLastDeepCrawlResult());
    assertEquals("Interrupted by daemon exit", aus.getLastDeepCrawlResultMsg());
    assertFalse(aus.isCrawlActive());

    TimeBase.setSimulated(t3);
    aus.deepCrawlStarted(444);
    assertEquals(-1, aus.getLastCrawlTime());
    assertEquals(t1, aus.getLastCrawlAttempt());
    assertEquals(Crawler.STATUS_RUNNING_AT_CRASH, aus.getLastCrawlResult());

    TimeBase.setSimulated(t4);
    aus.newCrawlFinished(Crawler.STATUS_SUCCESSFUL, "Sirah", 789);
    assertEquals(t4, aus.getLastCrawlTime());
    assertEquals(t3, aus.getLastCrawlAttempt());
    assertEquals(Crawler.STATUS_SUCCESSFUL, aus.getLastCrawlResult());
    assertEquals(t4, aus.getLastDeepCrawlTime());
    assertEquals(t3, aus.getLastDeepCrawlAttempt());
    assertEquals("Sirah", aus.getLastCrawlResultMsg());
    assertEquals(Crawler.STATUS_SUCCESSFUL, aus.getLastDeepCrawlResult());
    assertEquals("Sirah", aus.getLastDeepCrawlResultMsg());
  }

  public void testPollDuration() throws Exception {
    MyAuState aus = getMyAuState(mau);
    assertEquals(0, aus.getPollDuration());
    aus.setPollDuration(1000);
    assertEquals(1000, aus.getPollDuration());
    aus.setPollDuration(2000);
    assertEquals(1500, aus.getPollDuration());
  }

  public void testPollTimeAndResult() throws Exception {
    MyAuState aus = getMyAuState(mau);
    assertEquals(-1, aus.getLastTopLevelPollTime());
    assertEquals(-1, aus.getLastPollStart());
    assertEquals(-1, aus.getLastPollResult());
    assertEquals(null, aus.getLastPollResultMsg());
    assertEquals(-1, aus.getLastPoPPoll());
    assertEquals(-1, aus.getLastPoPPollResult());
    assertEquals(null, aus.getLastPoPPollResultMsg());
    assertEquals(-1, aus.getLastLocalHashScan());
    assertEquals(-1, aus.getLastTimePollCompleted());
    assertEquals(0, aus.getPollDuration());

    TimeBase.setSimulated(t1);
    aus.pollStarted();
    // running poll
    assertEquals(t1, aus.getLastPollStart());
    // These haven't been updated yet
    assertEquals(-1, aus.getLastTopLevelPollTime());
    assertEquals(-1, aus.getLastPollResult());
    assertEquals(0, aus.getPollDuration());
    assertEquals(-1, aus.getLastTimePollCompleted());

    TimeBase.setSimulated(t2);
    aus.pollFinished(V3Poller.POLLER_STATUS_ERROR, PollVariant.PoR);
    assertEquals(-1, aus.getLastTopLevelPollTime());
    assertEquals(t1, aus.getLastPollStart());
    assertEquals(V3Poller.POLLER_STATUS_ERROR, aus.getLastPollResult());
    assertEquals("Error", aus.getLastPollResultMsg());
    assertEquals(t2, aus.getPollDuration());
    assertEquals(-1, aus.getLastPoPPoll());
    assertEquals(-1, aus.getLastPoPPollResult());
    assertEquals(null, aus.getLastPoPPollResultMsg());
    assertEquals(-1, aus.getLastLocalHashScan());
    assertEquals(-1, aus.getLastTimePollCompleted());

    TimeBase.setSimulated(t3);
    aus.pollFinished(V3Poller.POLLER_STATUS_COMPLETE, PollVariant.PoR);
    assertEquals(t3, aus.getLastTopLevelPollTime());
    assertEquals(t1, aus.getLastPollStart());
    assertEquals(V3Poller.POLLER_STATUS_COMPLETE, aus.getLastPollResult());
    assertEquals("Complete", aus.getLastPollResultMsg());
    assertEquals((t3 + t2) / 2, aus.getPollDuration());
    assertEquals(-1, aus.getLastPoPPoll());
    assertEquals(-1, aus.getLastPoPPollResult());
    assertEquals(null, aus.getLastPoPPollResultMsg());
    assertEquals(-1, aus.getLastLocalHashScan());
    assertEquals(t3, aus.getLastTimePollCompleted());

    TimeBase.setSimulated(t4);
    aus.pollFinished(V3Poller.POLLER_STATUS_NO_QUORUM, PollVariant.PoP);
    assertEquals(t3, aus.getLastTopLevelPollTime());
    assertEquals(t1, aus.getLastPollStart());
    assertEquals(V3Poller.POLLER_STATUS_COMPLETE, aus.getLastPollResult());
    assertEquals("Complete", aus.getLastPollResultMsg());
    assertEquals((t3 + t2) / 2, aus.getPollDuration());
    assertEquals(-1, aus.getLastPoPPoll());
    assertEquals(V3Poller.POLLER_STATUS_NO_QUORUM, aus.getLastPoPPollResult());
    assertEquals(-1, aus.getLastLocalHashScan());
    assertEquals("No Quorum", aus.getLastPoPPollResultMsg());
    assertEquals(t3, aus.getLastTimePollCompleted());

    TimeBase.setSimulated(t5);
    aus.pollFinished(V3Poller.POLLER_STATUS_COMPLETE, PollVariant.PoP);
    assertEquals(t3, aus.getLastTopLevelPollTime());
    assertEquals(t1, aus.getLastPollStart());
    assertEquals(V3Poller.POLLER_STATUS_COMPLETE, aus.getLastPollResult());
    assertEquals("Complete", aus.getLastPollResultMsg());
    assertEquals((t3 + t2) / 2, aus.getPollDuration());
    assertEquals(t5, aus.getLastPoPPoll());
    assertEquals(V3Poller.POLLER_STATUS_COMPLETE, aus.getLastPoPPollResult());
    assertEquals("Complete", aus.getLastPoPPollResultMsg());
    assertEquals(-1, aus.getLastLocalHashScan());
    assertEquals(t5, aus.getLastTimePollCompleted());
    aus.pollFinished(V3Poller.POLLER_STATUS_NO_QUORUM, PollVariant.PoP);

    TimeBase.setSimulated(t6);
    aus.pollFinished(V3Poller.POLLER_STATUS_COMPLETE, PollVariant.Local);
    assertEquals(t3, aus.getLastTopLevelPollTime());
    assertEquals(t1, aus.getLastPollStart());
    assertEquals(V3Poller.POLLER_STATUS_COMPLETE, aus.getLastPollResult());
    assertEquals("Complete", aus.getLastPollResultMsg());
    assertEquals((t3 + t2) / 2, aus.getPollDuration());
    assertEquals(t5, aus.getLastPoPPoll());
    assertEquals(V3Poller.POLLER_STATUS_NO_QUORUM, aus.getLastPoPPollResult());
    assertEquals("No Quorum", aus.getLastPoPPollResultMsg());
    assertEquals(t6, aus.getLastLocalHashScan());
    assertEquals(t5, aus.getLastTimePollCompleted());

    aus = aus.simulateStoreLoad();
    assertEquals(t3, aus.getLastTopLevelPollTime());
    assertEquals(t1, aus.getLastPollStart());
    assertEquals(V3Poller.POLLER_STATUS_COMPLETE, aus.getLastPollResult());

    TimeBase.setSimulated(t7);
    aus.pollStarted();
    assertEquals(t3, aus.getLastTopLevelPollTime());
    assertEquals(t7, aus.getLastPollStart());
    assertEquals(V3Poller.POLLER_STATUS_COMPLETE, aus.getLastPollResult());
  }

  public void testV3Agreement() throws Exception {
    MyAuState aus = getMyAuState(mau);
    assertEquals(-1.0, aus.getV3Agreement());
    assertEquals(-1.0, aus.getHighestV3Agreement());

    aus.setV3Agreement(0.0);
    assertEquals(0.0, aus.getV3Agreement());
    assertEquals(0.0, aus.getHighestV3Agreement());

    aus.setV3Agreement(0.5);
    assertEquals(0.5, aus.getV3Agreement());
    assertEquals(0.5, aus.getHighestV3Agreement());

    aus.setV3Agreement(0.3);
    assertEquals(0.3, aus.getV3Agreement());
    assertEquals(0.5, aus.getHighestV3Agreement());
  }

  public void testTreeWalkFinished() {
    AuState auState = makeAuState(mau, -1, -1, -1, -1, 123, null,
				  1, -1.0, 1.0, stateMgr);
    assertEquals(123, auState.getLastTreeWalkTime());

    TimeBase.setSimulated(456);
    auState.setLastTreeWalkTime();
    assertEquals(456, auState.getLastTreeWalkTime());
  }

  public void testGetUrls() {
    HashSet stringCollection = new HashSet();
    stringCollection.add("test");

    AuState auState = makeAuState(mau, -1, -1, -1, -1, 123,
				  stringCollection, 1, -1.0, 1.0, stateMgr);
    Collection col = auState.getCrawlUrls();
    Iterator colIter = col.iterator();
    assertTrue(colIter.hasNext());
    assertEquals("test", colIter.next());
    assertFalse(colIter.hasNext());
  }

  public void testClockssSubscriptionStatus() {
    AuState aus = stateMgr.getAuState(mau);
    assertEquals(AuState.CLOCKSS_SUB_UNKNOWN,
		 aus.getClockssSubscriptionStatus());
    assertEquals("Unknown", aus.getClockssSubscriptionStatusString());

    aus.setClockssSubscriptionStatus(AuState.CLOCKSS_SUB_YES);
    assertEquals(AuState.CLOCKSS_SUB_YES,
		 aus.getClockssSubscriptionStatus());
    assertEquals("Yes", aus.getClockssSubscriptionStatusString());

    aus.setClockssSubscriptionStatus(AuState.CLOCKSS_SUB_NO);
    assertEquals(AuState.CLOCKSS_SUB_NO,
		 aus.getClockssSubscriptionStatus());
    assertEquals("No", aus.getClockssSubscriptionStatusString());

    aus.setClockssSubscriptionStatus(AuState.CLOCKSS_SUB_INACCESSIBLE);
    assertEquals(AuState.CLOCKSS_SUB_INACCESSIBLE,
		 aus.getClockssSubscriptionStatus());
    assertEquals("Inaccessible", aus.getClockssSubscriptionStatusString());
  }    

  public void testAccessType() {
    AuState aus = stateMgr.getAuState(mau);
    assertFalse(aus.isOpenAccess());
    aus.setAccessType(AuState.AccessType.Subscription);
    assertEquals(AuState.AccessType.Subscription, aus.getAccessType());
    assertFalse(aus.isOpenAccess());
    aus.setAccessType(AuState.AccessType.OpenAccess);
    assertEquals(AuState.AccessType.OpenAccess, aus.getAccessType());
    assertTrue(aus.isOpenAccess());
  }

  public void testSubstanceState() {
    AuState aus = stateMgr.getAuState(mau);
    assertEquals(SubstanceChecker.State.Unknown, aus.getSubstanceState());
    assertFalse(aus.hasNoSubstance());
    aus.setSubstanceState(SubstanceChecker.State.Yes);
    assertEquals(1, stateMgr.getAuStateUpdateCount());
    assertEquals(SubstanceChecker.State.Yes, aus.getSubstanceState());
    assertFalse(aus.hasNoSubstance());
    aus.setSubstanceState(SubstanceChecker.State.No);
    assertEquals(2, stateMgr.getAuStateUpdateCount());
    assertEquals(SubstanceChecker.State.No, aus.getSubstanceState());
    assertTrue(aus.hasNoSubstance());
    assertNotEquals("2", aus.getFeatureVersion(Plugin.Feature.Substance));
    mplug.setFeatureVersionMap(MapUtil.map(Plugin.Feature.Substance, "2"));
    aus.setSubstanceState(SubstanceChecker.State.Yes);
    // changing both the substance state and feature version should store
    // only once
    assertEquals(3, stateMgr.getAuStateUpdateCount());
    assertEquals(SubstanceChecker.State.Yes, aus.getSubstanceState());
    assertEquals("2", aus.getFeatureVersion(Plugin.Feature.Substance));
  }

  public void testFeatureVersion() {
    AuState aus = stateMgr.getAuState(mau);
    assertNull(aus.getFeatureVersion(Plugin.Feature.Substance));
    assertNull(aus.getFeatureVersion(Plugin.Feature.Metadata));
    assertNull(aus.getFeatureVersion(Plugin.Feature.Poll));
    aus.setFeatureVersion(Plugin.Feature.Metadata, "foo");
    assertNull(aus.getFeatureVersion(Plugin.Feature.Substance));
    assertEquals("foo", aus.getFeatureVersion(Plugin.Feature.Metadata));
    aus.setFeatureVersion(Plugin.Feature.Substance, "sub_42");
    assertEquals("sub_42", aus.getFeatureVersion(Plugin.Feature.Substance));
    assertEquals("foo", aus.getFeatureVersion(Plugin.Feature.Metadata));
    assertNull(aus.getFeatureVersion(Plugin.Feature.Poll));
  }

  public void testLastMetadataIndex() {
    AuState aus = stateMgr.getAuState(mau);
    assertEquals(-1, aus.getLastMetadataIndex());
    aus.setLastMetadataIndex(123);
    assertEquals(123, aus.getLastMetadataIndex());
  }
    
  public void testLastContentChange() {
    TimeBase.setSimulated(10);
    AuState aus = stateMgr.getAuState(mau);
    aus.newCrawlStarted();
    TimeBase.step(10);
    aus.contentChanged();
    assertEquals(20,aus.getLastContentChange());
    TimeBase.step(10);
    aus.contentChanged();
    assertEquals(20,aus.getLastContentChange());
    TimeBase.step(10);
    aus.newCrawlFinished(1, "foo");
    TimeBase.step(10);
    aus.contentChanged();
    assertEquals(50,aus.getLastContentChange());
  }
    
  public void testNumCurrentSuspectVersions() {
    MyAuSuspectUrlVersions asuv = new MyAuSuspectUrlVersions();
    stateMgr.setAsuv(asuv);

    AuState aus = stateMgr.getAuState(mau);
    assertEquals(0, aus.getNumCurrentSuspectVersions());
    // ensure this isn't automatically recomputed, as that would happen
    // when stateMgr loads the object during startAuManagers, before the
    // AU is fully created.
    aus.setNumCurrentSuspectVersions(-1);
    assertEquals(-1, aus.getNumCurrentSuspectVersions());
    asuv.setCountResult(17);
    aus.recomputeNumCurrentSuspectVersions();
    assertEquals(17, aus.getNumCurrentSuspectVersions());
    aus.incrementNumCurrentSuspectVersions(-1);
    assertEquals(16, aus.getNumCurrentSuspectVersions());

    aus.setNumCurrentSuspectVersions(-1);
    asuv.setCountResult(6);
    aus.incrementNumCurrentSuspectVersions(-1);
    assertEquals(5, aus.getNumCurrentSuspectVersions());
  }

  public void testCdnStems() {
    AuState aus = stateMgr.getAuState(mau);
    assertEquals(Collections.EMPTY_LIST, aus.getCdnStems());
    aus.addCdnStem("http://fff.uselesstld");
    assertClass(ArrayList.class, aus.getCdnStems());
    assertEquals(ListUtil.list("http://fff.uselesstld"), aus.getCdnStems());
    aus.addCdnStem("ccc");
    assertEquals(ListUtil.list("http://fff.uselesstld", "ccc"),
		 aus.getCdnStems());

    aus.setCdnStems(new LinkedList(ListUtil.list("a", "b")));
    assertClass(ArrayList.class, aus.getCdnStems());
    assertEquals(ListUtil.list("a", "b"), aus.getCdnStems());
    aus.setCdnStems(null);
    assertEmpty(aus.getCdnStems());
    aus.addCdnStem("https://a.b/");
    aus.addCdnStem("https://b.a/");
    assertEquals(ListUtil.list("https://a.b/", "https://b.a/"),
		 aus.getCdnStems());
  }

  String ausFields[] = {
    "auId",
    "lastCrawlTime",
    "lastCrawlAttempt",
    "lastCrawlResultMsg",
    "lastCrawlResult",
    "lastDeepCrawlAttempt",
    "lastDeepCrawlDepth",
    "lastDeepCrawlResult",
    "lastDeepCrawlResultMsg",
    "lastDeepCrawlTime",
    "lastPollStart",
    "lastPollResult",
    "pollDuration",
    "clockssSubscriptionStatus",
    "hasSubstance",
    "v3Agreement",
    "highestV3Agreement",
    "accessType",
    "lastMetadataIndex",
    "lastContentChange",
    "lastPoPPoll",
    "lastPoPPollResult",
    "lastLocalHashScan",
    "numAgreePeersLastPoR",
    "numWillingRepairers",
    "numCurrentSuspectVersions",
    "cdnStems",
    "lastTopLevelPollTime",
    "auCreationTime",
    "metadataVersion",
    "substanceVersion",
    "averageHashDuration",
    "isMetadataExtractionEnabled"};

  String ignFields[] = {
    "lastPollAttempt",
    "previousCrawlState",
    "au",
    "stateMgr",
    "needSave",
    "batchSaveDepth",
    "lastTreeWalk",
    "crawlUrls",
    "hasV3Poll",
    "lastPollResultMsg",
    "urlUpdateCntr",
    "crawlUrls",
  };

  String ausFields2[] = {
    "lastCrawlTime",
    "lastCrawlAttempt",
    "lastCrawlResultMsg",
    "lastCrawlResult"};

  public void testToJson() throws IOException {
    Set<String> f2set = new HashSet<>(Arrays.asList(ausFields2));
    AuState aus = makeAuState(mau, -1, -1, -1, -1, 123, null,
			      1, -1.0, 1.0, stateMgr);
    // serialize whole object
    String json1 = aus.toJson();
    // serialize only selected fields
    String json2 = aus.toJson(SetUtil.set(ausFields2));
    log.debug("json1: " + json1);
    log.debug("json2: " + json2);
    Map map1 = AuUtil.jsonToMap(json1);
    Map map2 = AuUtil.jsonToMap(json2);
    assertSameElements(ausFields, map1.keySet());
    assertSameElements(ausFields2, map2.keySet());

    // these fields should never appear
    for (String s : ignFields) {
      assertFalse(map1.containsKey(s));
      assertFalse(map2.containsKey(s));
    }
  }

  public void testFromJson() throws IOException {
    Set<String> f2set = new HashSet<>(Arrays.asList(ausFields2));
    AuState aus1 = makeAuState(mau, 1, 2, 3, 4, 5, null,
			       1, 0.5, 1.5, stateMgr);
    AuState aus2 = makeAuState(mau, -1, -1, -1, -1, 123, null,
			       1, -1.0, 1.0, stateMgr);
    assertEquals(-1, aus2.getLastCrawlTime());
    assertEquals(-1, aus2.getLastTopLevelPollTime());
    assertEquals(-1.0, aus2.getV3Agreement());
    assertEquals(1.0, aus2.getHighestV3Agreement());
    String json1 = aus1.toJson("v3Agreement");
    log.debug2("json1: " + json1);
    AuState upd = aus2.updateFromJson(json1, getMockLockssDaemon());
    assertSame(upd, aus2);
    assertEquals(-1, aus2.getLastCrawlTime());
    assertEquals(-1, aus2.getLastTopLevelPollTime());
    assertEquals(.5, aus2.getV3Agreement());
    assertEquals(1.0, aus2.getHighestV3Agreement());
  }

  public void testBatch() {
    AuState aus = stateMgr.getAuState(mau);
    assertEquals(0, stateMgr.getAuStateUpdateCount());
    aus.setNumAgreePeersLastPoR(1);
    aus.setNumWillingRepairers(3);
    aus.setNumCurrentSuspectVersions(5);
    assertEquals(3, stateMgr.getAuStateUpdateCount());

    aus.batchSaves();
    aus.setNumAgreePeersLastPoR(2);
    aus.setNumWillingRepairers(4);
    aus.setNumCurrentSuspectVersions(6);
    assertEquals(3, stateMgr.getAuStateUpdateCount());
    aus.unBatchSaves();
    assertEquals(4, stateMgr.getAuStateUpdateCount());

    aus.batchSaves();
    aus.setNumAgreePeersLastPoR(4);
    aus.batchSaves();
    aus.setNumWillingRepairers(8);
    aus.setNumCurrentSuspectVersions(12);
    assertEquals(4, stateMgr.getAuStateUpdateCount());
    aus.unBatchSaves();
    assertEquals(4, stateMgr.getAuStateUpdateCount());
    aus.unBatchSaves();
    assertEquals(5, stateMgr.getAuStateUpdateCount());
  }

  // Return the serialized representation of the AuState
  String ser(AuState aus) throws Exception {
    return aus.toJson();
  }

  // Deserialize and return AuState
  AuState deser(String s) throws Exception {
    return new AuState(AuUtil.updateFromJson(new AuStateBean(), s));
  }

  // Ensure that fields added to AuState get their default value when
  // deserialized from old files not containing the field
  public void testNewField() throws Exception {
    AuState aus = makeAuState(mau, -1, -1, -1, -1, 123, null,
			      1, -1.0, 1.0, stateMgr);

    String ser = ser(aus);
    String edser = ser.replaceAll("numAgreePeersLastPoR", "nonExistentField");
    log.debug2("old: " + ser);
    log.debug2("new: " + edser);
    AuState newaus = deser(edser);
    assertEquals(-1, newaus.getNumAgreePeersLastPoR());
    assertEquals(Collections.EMPTY_LIST, newaus.getCdnStems());
  }

  MyAuState getMyAuState(MockArchivalUnit mau) {
    return (MyAuState)stateMgr.getAuState(mau);
  }

  static class MyAuState extends AuState implements Cloneable {
    public MyAuState(ArchivalUnit au, StateManager stateMgr) {
      super(au, stateMgr);
    }
    MyAuState simulateStoreLoad() throws CloneNotSupportedException {
//       MyAuState ret = (MyAuState)this.clone();
      MyAuState ret = this;
      ret.previousCrawlState = null;
      return ret;
    }
  }

  static class MyStateManager extends CachingStateManager {
    int auStateStoreCount = 0;
    int auStateUpdateCount = 0;
    MyAuSuspectUrlVersions asuv;


    @Override
    public void initService(LockssDaemon daemon) throws LockssAppException {
      super.initService(daemon);
    }

    @Override
    protected AuState newDefaultAuState(ArchivalUnit au) {
      return new MyAuState(au, this);
    }

    @Override
    protected AuStateBean newDefaultAuStateBean(String key) {
      return new AuStateBean();
    }

    @Override
    public void storeAuState(AuState auState) {
      auStateStoreCount++;
      super.storeAuState(auState);
    }

    @Override
    public void updateAuState(AuState aus, Set<String> fields) {
      auStateUpdateCount++;
      super.updateAuState(aus, fields);
    }

    public int getAuStateStoreCount() {
      return auStateStoreCount;
    }

    public int getAuStateUpdateCount() {
      return auStateUpdateCount;
    }

    @Override
    public AuSuspectUrlVersions getAuSuspectUrlVersions(String auid) {
      return asuv;
    }

    @Override
    public boolean hasAuSuspectUrlVersions(String key) {
      return asuv != null;
    }

    public void setAsuv(MyAuSuspectUrlVersions asuv) {
      this.asuv = asuv;
    }
  }


  static class MyAuSuspectUrlVersions extends AuSuspectUrlVersions {
    int countResult;

    @Override
    public int countCurrentSuspectVersions(ArchivalUnit au) {
      return countResult;
    }

    public void setCountResult(int n) {
      countResult = n;
    }
  }

}
