/*

Copyright (c) 2000-2020 Board of Trustees of Leland Stanford Jr. University,
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

package org.lockss.state;

import java.util.*;
import junit.framework.*;
import org.lockss.state.*;
import org.lockss.plugin.ArchivalUnit;

/**
 * This is a mock version of <code>ArchivalUnit</code> used for testing
 */

public class MockAuState extends AuState {

  HashSet crawlUrls = new HashSet();
    private long auCreate = -1;

  public MockAuState(ArchivalUnit au) {
    this(au, -1, -1, -1, null);
  }

  public MockAuState() {
    this(null, -1, -1, -1, null);
  }

  public MockAuState(ArchivalUnit au,
		     long lastCrawlTime, long lastPollTime,
                     long lastTreeWalk, StateManager stateMgr) {
    this(au, lastCrawlTime, -1, lastPollTime, -1, lastTreeWalk,
	 null, stateMgr);
  }

  public MockAuState(ArchivalUnit au,
		     long lastCrawlTime, long lastCrawlAttempt,
		     long lastPollTime, long lastPollStart,
                     long lastTreeWalk, StateManager stateMgr) {
    this(au, lastCrawlTime, lastCrawlAttempt, lastPollTime, lastPollStart,
	 lastTreeWalk, null, stateMgr);
  }

  public MockAuState(ArchivalUnit au,
		     long lastCrawlTime, long lastCrawlAttempt,
		     long lastPollTime, long lastPollStart,
                     long lastTreeWalk, HashSet crawlUrls,
                     StateManager stateMgr) {

    super(au,
	  lastCrawlTime,
	  lastCrawlAttempt,
	  -1, //lastCrawlResult
	  null, // lastCrawlResultMsg
	  -1, // lastDeepCrawlTime
	  -1, // lastDeepCrawlAttempt
	  -1, // lastDeepCrawlResult
	  null, // lastDeepCrawlResultMsg,
	  -1, // lastDeepCrawlDepth
	  lastPollTime, //lastTopLevelPollTime
	  lastPollStart,
	  -1, // lastPollResult
	  null, // lastPollResultMsg
	  0L, // pollDuration
	  0L,
	  lastTreeWalk,
	  crawlUrls,
	  null, // accessType
	  CLOCKSS_SUB_UNKNOWN, // clockssSubscriptionState
	  -1.0, // v3Agreement
	  -1.0, // highestV3Agreement
	  SubstanceChecker.State.Unknown,
	  null, // substanceVersion
	  null, // metadataVersion
	  -1, //lastMetadataIndex
	  0L, // lastContentChange
	  -1L, // lastPoPPoll
	  -1, // lastPoPPollResult
	  -1L, // lastLocalHashScan
	  -1, // numAgreePeersLastPoR
	  -1, // numWillingRepairers
	  -1, // numCurrentSuspectVersions
	  null, // cdnStems
	  true, // isMetadataExtractionEnabled
	  stateMgr);
  }

  public long getAuCreationTime() {
    if (auCreate >= 0) {
      return auCreate;
    }
    return super.getAuCreationTime();
  }

  public void setAuCreationTime(long time) {
    auCreate = time;
  }

  public void setLastCrawlTime(long newCrawlTime) {
    bean.lastCrawlTime = newCrawlTime;
  }

  public void setLastContentChange(long newTime) {
    bean.lastContentChange = newTime;
  }

  public void setLastTopLevelPollTime(long newPollTime) {
    bean.lastTopLevelPollTime = newPollTime;
  }

  @Override
  public void setV3Agreement(double d) {
    bean.v3Agreement = d;
  }

  public void setHighestV3Agreement(double d) {
    bean.highestV3Agreement = d;
  }

  public void setLastPollResult(int result) {
    bean.lastPollResult = result;
  }

  public void setLastPollStart(long time) {
    bean.lastPollStart = time;
  }

  public void setLastToplevalPoll(long time) {
    bean.lastTopLevelPollTime = time;
  }

  public void setLastTreeWalkTime(long newTreeWalkTime) {
    lastTreeWalk = newTreeWalkTime;
  }

  public void setLastCrawlAttempt(long lastCrawlAttempt) {
    bean.lastCrawlAttempt = lastCrawlAttempt;
  }

  public void newCrawlFinished(int result, String resultMsg) {
    super.newCrawlFinished(result, resultMsg);
  }

  public void setLastCrawlResult(int result, String resultMsg) {
    bean.lastCrawlResult = result;
    bean.lastCrawlResultMsg = resultMsg;
  }

  public HashSet getCrawlUrls() {
    return crawlUrls;
  }

  boolean suppressRecomputeNumCurrentSuspectVersions = false;

  public void setSuppressRecomputeNumCurrentSuspectVersions(boolean val) {
    suppressRecomputeNumCurrentSuspectVersions = val;
  }

  @Override
  public synchronized int recomputeNumCurrentSuspectVersions() {
    if (suppressRecomputeNumCurrentSuspectVersions) {
      return 0;
    }
    return super.recomputeNumCurrentSuspectVersions();
  }
}

