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


package org.lockss.state;

import java.io.*;
import java.util.*;
import com.fasterxml.jackson.annotation.*;

import org.lockss.plugin.*;
import org.lockss.app.*;
import org.lockss.util.*;
import org.lockss.util.io.LockssSerializable;
import org.lockss.util.time.TimeBase;
import org.lockss.daemon.*;
import org.lockss.crawler.CrawlerStatus;
import org.lockss.poller.v3.*;
import org.lockss.repository.*;

/**
 * AuState contains the state information for an au.<br>
 *
 * In this class, default values *will* be present after deserialization,
 * for fields that did not exist when the object was serialized.  {@see
 * org.lockss.util.XStreamSerializer#CLASSES_NEEDING_CONSTRUCTOR}
 */
public class AuState implements LockssSerializable {

  private static final Logger logger = Logger.getLogger();

  public enum AccessType {OpenAccess, Subscription};


  protected AuStateBean bean;

  protected transient long lastPollAttempt; // last time we attempted to
					    // start a poll

  // Non-persistent state vars

  // saves previous lastCrawl* state while crawl is running
  protected transient AuStateBean previousCrawlState = null;

  // Runtime (non-state) vars
  protected transient ArchivalUnit au;

  private transient StateManager stateMgr;

  private transient int batchSaveDepth = 0;

  // deprecated, kept for compatibility with old state files
  protected transient long lastTreeWalk = -1;

  // should be deprecated?
  protected HashSet crawlUrls;

  // deprecated, kept for compatibility with old state files
  /** @deprecated */
  protected transient boolean hasV3Poll = false;

  // No longer set, never had a non-standard value
  protected transient String lastPollResultMsg;   // result of last poll

  protected String auId = null;

  /** No-arg constructor for testing. */
  AuState() {
  }

  /** Bean constructor for testing. */
  AuState(AuStateBean bean) {
    this.bean = bean;
    if (bean == null) logger.critical("null bean", new Throwable());
  }

  public AuState(ArchivalUnit au) {
    this(au, LockssDaemon.getManagerByTypeStatic(StateManager.class));
  }

  public AuState(ArchivalUnit au, StateManager stateMgr) {
    this(au, stateMgr, new AuStateBean());
  }

  public AuState(ArchivalUnit au, StateManager stateMgr, AuStateBean bean) {
    this.au = au;
    this.stateMgr = stateMgr;
    this.bean = bean;
    if (bean == null) logger.critical("null bean", new Throwable());
  }


  public AuState(ArchivalUnit au,
		 long lastCrawlTime, long lastCrawlAttempt,
		 int lastCrawlResult, String lastCrawlResultMsg,
		 long lastDeepCrawlTime, long lastDeepCrawlAttempt,
		 int lastDeepCrawlResult, String lastDeepCrawlResultMsg,
		 int lastDeepCrawlDepth,
		 long lastTopLevelPollTime, long lastPollStart,
		 int lastPollResult, String lastPollResultMsg,
		 long pollDuration,
		 long averageHashDuration,
		 long lastTreeWalk, HashSet crawlUrls,
		 AccessType accessType,
		 int clockssSubscriptionStatus,
		 double v3Agreement,
		 double highestV3Agreement,
		 SubstanceChecker.State hasSubstance,
		 String substanceVersion,
		 String metadataVersion,
		 long lastMetadataIndex,
		 long lastContentChange,
		 long lastPoPPoll,
		 int lastPoPPollResult,
		 long lastLocalHashScan,
		 int numAgreePeersLastPoR,
		 int numWillingRepairers,
		 int numCurrentSuspectVersions,
		 List<String> cdnStems,
		 StateManager stateMgr) {
    this(au, stateMgr);
    this.au = au;
    this.stateMgr = stateMgr;

    bean.lastCrawlTime = lastCrawlTime;
    bean.lastCrawlAttempt = lastCrawlAttempt;
    bean.lastCrawlResult = lastCrawlResult;
    bean.lastCrawlResultMsg = lastCrawlResultMsg;
    bean.lastDeepCrawlTime = lastDeepCrawlTime;
    bean.lastDeepCrawlAttempt = lastDeepCrawlAttempt;
    bean.lastDeepCrawlResult = lastDeepCrawlResult;
    bean.lastDeepCrawlResultMsg = lastDeepCrawlResultMsg;
    bean.lastDeepCrawlDepth = lastDeepCrawlDepth;
    bean.lastTopLevelPollTime = lastTopLevelPollTime;
    bean.lastPollStart = lastPollStart;
    bean.lastPollResult = lastPollResult;
    this.lastPollResultMsg = lastPollResultMsg;
    bean.pollDuration = pollDuration;
    bean.averageHashDuration = averageHashDuration;
    this.lastTreeWalk = lastTreeWalk;
    this.crawlUrls = crawlUrls;
    bean.accessType = accessType;
    bean.clockssSubscriptionStatus = clockssSubscriptionStatus;
    bean.v3Agreement = v3Agreement;
    bean.highestV3Agreement = highestV3Agreement;
    bean.hasSubstance = hasSubstance;
    bean.substanceVersion = substanceVersion;
    bean.metadataVersion = metadataVersion;
    bean.lastMetadataIndex = lastMetadataIndex;
    bean.lastContentChange = lastContentChange;
    bean.lastPoPPoll = lastPoPPoll;
    bean.lastPoPPollResult = lastPoPPollResult;
    bean.lastLocalHashScan = lastLocalHashScan;
    bean.numAgreePeersLastPoR = numAgreePeersLastPoR;
    bean.numWillingRepairers = numWillingRepairers;
    bean.numCurrentSuspectVersions = numCurrentSuspectVersions;
    bean.cdnStems = cdnStems;

    if (cdnStems != null) {
      flushAuCaches();
    }
  }

  /**
   * Returns the AuStateBean contains the actual data
   */
  public AuStateBean getBean() {
    return bean;
  }

  /**
   * Returns the au
   * @return the au
   */
  public ArchivalUnit getArchivalUnit() {
    return au;
  }

  public boolean isCrawlActive() {
    return previousCrawlState != null;
  }

  /**
   * Returns the date/time the au was created.
   * @return au creation time
   * If there is a Lockss repository exception, this method returns -1.
   */
  public long getAuCreationTime() {
    return bean.auCreationTime;
  }

  /**
   * Returns the date/time the au was created.
   * @return au creation time
   * If there is a Lockss repository exception, this method returns -1.
   */
  public void setAuCreationTime(long time) {
    bean.auCreationTime = time;
    needSave("auCreationTime");
  }

  /**
   * Returns the last completed new content crawl time of the au.
   * @return the last crawl time in ms
   */
  public long getLastCrawlTime() {
    return bean.lastCrawlTime;
  }

  /**
   * Returns the last time a new content crawl was attempted.
   * @return the last crawl time in ms
   */
  public long getLastCrawlAttempt() {
    if (isCrawlActive()) {
      return previousCrawlState.getLastCrawlAttempt();
    }
    return bean.lastCrawlAttempt;
  }

  /**
   * Returns the result code of the last new content crawl
   */
  public int getLastCrawlResult() {
    if (isCrawlActive()) {
      return previousCrawlState.getLastCrawlResult();
    }
    return bean.lastCrawlResult;
  }

  /**
   * Returns the result of the last new content crawl
   */
  public String getLastCrawlResultMsg() {
    if (isCrawlActive()) {
      return previousCrawlState.getLastCrawlResultMsg();
    }
    if (bean.lastCrawlResultMsg == null) {
      return CrawlerStatus.getDefaultMessage(bean.lastCrawlResult);
    }
    return bean.lastCrawlResultMsg;
  }

  /**
   * Returns the last completed deep crawl time of the au.
   * @return the last deep crawl time in ms
   */
  public long getLastDeepCrawlTime() {
    return bean.lastDeepCrawlTime;
  }

  /**
   * Returns the last time a deep crawl was attempted
   * @return the last deep crawl start time in ms
   */
  public long getLastDeepCrawlAttempt() {
    if (isCrawlActive()) {
      return previousCrawlState.getLastDeepCrawlAttempt();
    }
    return bean.lastDeepCrawlAttempt;
  }

  /**
   * Returns the result code of the last deep content crawl
   */
  public int getLastDeepCrawlResult() {
    if (isCrawlActive()) {
      return previousCrawlState.getLastDeepCrawlResult();
    }
    return bean.lastDeepCrawlResult;
  }

  /**
   * Returns the result of the last deep content crawl
   */
  public String getLastDeepCrawlResultMsg() {
    if (isCrawlActive()) {
      return previousCrawlState.getLastDeepCrawlResultMsg();
    }
    if (bean.lastDeepCrawlResultMsg == null) {
      return CrawlerStatus.getDefaultMessage(bean.lastDeepCrawlResult);
    }
    return bean.lastDeepCrawlResultMsg;
  }

  /**
   * Returns the depth of the last deep crawl of the au.
   * @return the depth of last deep crawl
   */
  public int getLastDeepCrawlDepth() {
    return bean.lastDeepCrawlDepth;
  }

  /**
   * @return last time metadata indexing was completed.
   */
  public long getLastMetadataIndex() {
    return bean.lastMetadataIndex;
  }

  /**
   * Set last time metadata indexing was completed.
   */
  public synchronized void setLastMetadataIndex(long time) {
    bean.lastMetadataIndex = time;
    needSave("lastMetadataIndex");
  }

  /**
   * @return last time a new version of a URL was created. Note that
   * only the first such change per crawl is noted.
   */
  public long getLastContentChange() {
    return bean.lastContentChange;
  }

  /**
   * Returns true if the AU has ever successfully completed a new content
   * crawl
   */
  public boolean hasCrawled() {
    return getLastCrawlTime() >= 0;
  }

  /**
   * Returns the last time a PoR poll completed.
   * @return the last poll time in ms
   */
  public long getLastTopLevelPollTime() {
    return bean.lastTopLevelPollTime;
  }

  /**
   * Returns the last time a PoP poll completed.
   * @return the last poll time in ms
   */
  public long getLastPoPPoll() {
    return bean.lastPoPPoll;
  }

  /**
   * Returns the last time a Local hash scan completed.
   * @return the last scan time in ms
   */
  public long getLastLocalHashScan() {
    return bean.lastLocalHashScan;
  }

  /**
   * Returns the last PoP poll result.
   * @return the last poll time in ms
   */
  public int getLastPoPPollResult() {
    return bean.lastPoPPollResult;
  }

  /**
   * Returns the last time a PoP or PoR poll completed.
   * @return the last poll time in ms
   */
  public long getLastTimePollCompleted() {
    return Math.max(bean.lastTopLevelPollTime, bean.lastPoPPoll);
  }

  /**
   * Returns the last time a poll started
   * @return the last poll time in ms
   */
  public long getLastPollStart() {
    return bean.lastPollStart;
  }

  /**
   * Returns the last time a poll was attempted, since the last daemon
   * restart
   * @return the last poll time in ms
   */
  public long getLastPollAttempt() {
    return lastPollAttempt;
  }

  /**
   * Returns the result code of the last poll
   */
  public int getLastPollResult() {
    return bean.lastPollResult;
  }

  /**
   * Returns the result of the last PoR poll
   */
  public String getLastPollResultMsg() {
    if (bean.lastPollResult < 0) {
      return null;
    }
    try {
      return V3Poller.getStatusString(bean.lastPollResult);
    } catch (IndexOutOfBoundsException e) {
      return "Poll result " + bean.lastPollResult;
    }
  }

  /**
   * Returns the result of the last PoP poll
   */
  public String getLastPoPPollResultMsg() {
    if (bean.lastPoPPollResult < 0) {
      return null;
    }
    try {
      return V3Poller.getStatusString(bean.lastPoPPollResult);
    } catch (IndexOutOfBoundsException e) {
      return "Poll result " + bean.lastPoPPollResult;
    }
  }
  public long getAverageHashDuration() {
    return bean.averageHashDuration;
  }

  public void setLastHashDuration(long newDuration) {
    if (newDuration < 0) {
      logger.warning("Tried to update hash with negative duration.");
      return;
    }
    bean.averageHashDuration = newDuration;
  }

  public int getNumAgreePeersLastPoR() {
    return bean.numAgreePeersLastPoR;
  }

  public synchronized void setNumAgreePeersLastPoR(int n) {
    if (bean.numAgreePeersLastPoR != n) {
      bean.numAgreePeersLastPoR = n;
      needSave("numAgreePeersLastPoR");
    }
  }

  public int getNumWillingRepairers() {
    return bean.numWillingRepairers;
  }

  public synchronized void setNumWillingRepairers(int n) {
    if (bean.numWillingRepairers != n) {
      if (logger.isDebug3()) {
	logger.debug3("setNumWillingRepairers: " +
		      bean.numWillingRepairers + " -> " + n);
      }
      bean.numWillingRepairers = n;
      needSave("numWillingRepairers");
    }
  }

  public int getNumCurrentSuspectVersions() {
    return bean.numCurrentSuspectVersions;
  }

  public synchronized void setNumCurrentSuspectVersions(int n) {
    if (bean.numCurrentSuspectVersions != n) {
      if (logger.isDebug3()) {
	logger.debug3("setNumCurrentSuspectVersions: " +
		      bean.numCurrentSuspectVersions + " -> " + n);
      }
      bean.numCurrentSuspectVersions = n;
      needSave("numCurrentSuspectVersions");
    }
  }

  public synchronized void incrementNumCurrentSuspectVersions(int n) {
    if (bean.numCurrentSuspectVersions < 0) {
      // If -1, this object was deserialized from a file written before
      // this field existed, so it needs to be computed.
      recomputeNumCurrentSuspectVersions();
    }
    setNumCurrentSuspectVersions(getNumCurrentSuspectVersions() + n);
  }

  public synchronized int recomputeNumCurrentSuspectVersions() {
    int n = 0;
    if (AuUtil.hasSuspectUrlVersions(au)) {
      AuSuspectUrlVersions asuv = AuUtil.getSuspectUrlVersions(au);
      n = asuv.countCurrentSuspectVersions(au);
    }
    logger.debug2("recomputeNumCurrentSuspectVersions(" + au + "): " +
		  bean.numCurrentSuspectVersions + " -> " + n);
    setNumCurrentSuspectVersions(n);
    return n;
  }

  public List<String> getCdnStems() {
    return bean.cdnStems != null ?  bean.cdnStems
      : (List<String>)Collections.EMPTY_LIST;
  }

  public synchronized void setCdnStems(List<String> stems) {
    logger.debug("setCdnStems: " + stems);
    if (!getCdnStems().equals(stems)) {
      bean.cdnStems = stems == null ? null : ListUtil.minimalArrayList(stems);
      needSave("cdnStems");
      flushAuCaches();
    }
  }

  public synchronized void addCdnStem(String stem) {
    logger.debug("addCdnStem: " + stem);
    if (!getCdnStems().contains(stem)) {
      if (bean.cdnStems == null) {
	bean.cdnStems = new ArrayList(4);
      }
      bean.cdnStems.add(stem);
      AuUtil.getDaemon(au).getPluginManager().addAuStem(stem, au);
      needSave("cdnStems");
      flushAuCaches();
    }
  }

  /**
   * Returns the running average poll duration, or 0 if unknown
   */
  public long getPollDuration() {
    return bean.pollDuration;
  }

  /**
   * Update the poll duration to the average of current and previous
   * average.  Return the new average.
   */
  public long setPollDuration(long duration) {
    if (bean.pollDuration == 0) {
      bean.pollDuration = duration;
    } else {
      bean.pollDuration = (bean.pollDuration + duration + 1) / 2;
    }
    return bean.pollDuration;
  }

  /**
   * Returns the last treewalk time for the au.
   * @return the last treewalk time in ms
   */
  public long getLastTreeWalkTime() {
    return lastTreeWalk;
  }

  private void saveLastCrawl() {
    if (previousCrawlState != null) {
      logger.error("saveLastCrawl() called twice", new Throwable());
    }
    previousCrawlState = saveCrawlState();
  }

  /**
   * Sets the last time a crawl was attempted.
   */
  public void newCrawlStarted() {
    newCrawlStarted(false);
  }

  /**
   * Sets the last time a deep crawl was attempted.
   */
  public void deepCrawlStarted(int depth) {
    newCrawlStarted(true);
  }

  private synchronized void newCrawlStarted(boolean isDeep) {
    saveLastCrawl();
    long now = TimeBase.nowMs();
    bean.lastCrawlAttempt = now;
    bean.lastCrawlResult = Crawler.STATUS_RUNNING_AT_CRASH;
    bean.lastCrawlResultMsg = null;
    if (isDeep) {
      bean.lastDeepCrawlAttempt = now;
      bean.lastDeepCrawlResult = Crawler.STATUS_RUNNING_AT_CRASH;
      bean.lastDeepCrawlResultMsg = null;
      needSave("lastCrawlAttempt", "lastCrawlResult", "lastCrawlResultMsg",
	       "lastDeepCrawlAttempt", "lastDeepCrawlResult", "lastDeepCrawlResultMsg");
    } else {
      needSave("lastCrawlAttempt", "lastCrawlResult", "lastCrawlResultMsg");
    }
  }

  /**
   * Sets the last crawl time to the current time.  Saves itself to disk.
   */
  public synchronized void newCrawlFinished(int result, String resultMsg) {
    newCrawlFinished(result, resultMsg, -1);
  }
  /**
   * Sets the last crawl time to the current time.  Saves itself to disk.
   */
  public synchronized void newCrawlFinished(int result, String resultMsg,
					    int depth) {
    switch (result) {
    case Crawler.STATUS_SUCCESSFUL:
      long now = TimeBase.nowMs();
      bean.lastCrawlTime = now;
      if (depth > 0) {
	bean.lastDeepCrawlTime = now;
	bean.lastDeepCrawlDepth = depth;
      }
      // fall through
    default:
      bean.lastCrawlResult = result;
      bean.lastCrawlResultMsg = resultMsg;
      if (depth > 0) {
	bean.lastDeepCrawlResult = result;
	bean.lastDeepCrawlResultMsg = resultMsg;
      }
      break;
    case Crawler.STATUS_ACTIVE:
      logger.warning("Storing Active state", new Throwable());
      break;
    }
    previousCrawlState = null;
    needSave("lastCrawlTime", "lastCrawlAttempt",
	     "lastCrawlResult", "lastCrawlResultMsg");
  }

  /**
   * Records a content change
   */
  public synchronized void contentChanged() {
    // Is a crawl in progress?
    if (previousCrawlState != null) {
      // Is the previous content change after the start of this
      // crawl?
      if (bean.lastContentChange > bean.lastCrawlAttempt) {
	// Yes - we already know this crawl changed things
	return;
      }
    }
    // Yes - this is the first change in this crawl.
    bean.lastContentChange = TimeBase.nowMs();
    needSave("lastContentChange");
  }

  private AuStateBean saveCrawlState() {
    AuStateBean res = new AuStateBean();
    res.lastCrawlResultMsg = getLastCrawlResultMsg();
    res.lastCrawlResult = getLastCrawlResult();
    res.lastCrawlAttempt = getLastCrawlAttempt();
    res.lastDeepCrawlResultMsg = getLastDeepCrawlResultMsg();
    res.lastDeepCrawlResult = getLastDeepCrawlResult();
    res.lastDeepCrawlAttempt = getLastDeepCrawlAttempt();
    return res;
  }

  /**
   * Sets the last time a poll was started.
   */
  public synchronized void pollStarted() {
    bean.lastPollStart = TimeBase.nowMs();
    needSave("lastPollStart");
  }

  /**
   * Sets the last time a poll was attempted.
   */
  public synchronized void pollAttempted() {
    lastPollAttempt = TimeBase.nowMs();
    needSave("lastPollAttempt");
  }

  /**
   * Sets the last poll time to the current time.
   */
  public synchronized void pollFinished(int result,
					V3Poller.PollVariant variant) {
    long now = TimeBase.nowMs();
    boolean complete = result == V3Poller.POLLER_STATUS_COMPLETE;
    switch (variant) {
    case PoR:
      if (complete) {
	bean.lastTopLevelPollTime = now;
      }
      bean.lastPollResult = result;
      setPollDuration(TimeBase.msSince(lastPollAttempt));
      break;
    case PoP:
      if (complete) {
	bean.lastPoPPoll = now;
      }
      bean.lastPoPPollResult = result;
      break;
    case Local:
      if (complete) {
	bean.lastLocalHashScan = now;
      }
      break;
    }
    needSave("lastTopLevelPollTime", "lastPollResult", "pollDuration",
	     "lastPoPPoll", "lastPoPPollResult", "lastLocalHashScan");
  }

  /**
   * Sets the last poll time to the current time. Only for V1 polls.
   */
  public void pollFinished() {
    pollFinished(V3Poller.POLLER_STATUS_COMPLETE,
		 V3Poller.PollVariant.PoR); // XXX Bogus!
  }

  public synchronized void setV3Agreement(double d) {
    bean.v3Agreement = d;
    if (bean.v3Agreement > bean.highestV3Agreement) {
      bean.highestV3Agreement = bean.v3Agreement;
      needSave("v3Agreement", "highestV3Agreement");
    } else {
      needSave("v3Agreement");
    }
  }

  /**
   * @return agreement in last V3 poll
   */
  public double getV3Agreement() {
    return bean.v3Agreement;
  }
  
  public double getHighestV3Agreement() {
    // We didn't used to track highest, so return last if no highest recorded
    return bean.v3Agreement > bean.highestV3Agreement
      ? bean.v3Agreement : bean.highestV3Agreement;
  }
  
  public synchronized void setSubstanceState(SubstanceChecker.State state) {
    batchSaves();
    setFeatureVersion(Plugin.Feature.Substance,
		      au.getPlugin().getFeatureVersion(Plugin.Feature.Substance));
    if (getSubstanceState() != state) {
      bean.hasSubstance = state;
      needSave("hasSubstance");
    }
    unBatchSaves();
  }

  public SubstanceChecker.State getSubstanceState() {
    if (bean.hasSubstance == null) {
      return SubstanceChecker.State.Unknown;
    }
    return bean.hasSubstance;
  }

  public boolean hasNoSubstance() {
    return bean.hasSubstance == SubstanceChecker.State.No;
  }

  /** Get the version string that was last set for the given feature */
  public String getFeatureVersion(Plugin.Feature feat) {
    switch (feat) {
    case Substance: return bean.substanceVersion;
    case Metadata: return bean.metadataVersion;
    default: return null;
    }
  }

  /** Set the version of the feature that was just used to process the
   * AU */
  public synchronized void setFeatureVersion(Plugin.Feature feat, String ver) {
    String over = getFeatureVersion(feat);
    if (!StringUtil.equalStrings(ver, over)) {
      switch (feat) {
      case Substance:
	bean.substanceVersion = ver;
	needSave("substanceVersion");
	break;
      case Metadata:
	bean.metadataVersion = ver;
	needSave("metadataVersion");
	break;
      default:
      }
    }
  }

  /**
   * Sets the last treewalk time to the current time.  Does not save itself
   * to disk, as it is desireable for the treewalk to run every time the
   * server restarts.  Consequently, it is non-persistent.
   * @deprecated
   */
  void setLastTreeWalkTime() {
    lastTreeWalk = TimeBase.nowMs();
  }

  /**
   * Gets the collection of crawl urls.
   * @return a {@link Collection}
   * @deprecated
   */
  public HashSet getCrawlUrls() {
    if (crawlUrls==null) {
      crawlUrls = new HashSet();
    }
    return crawlUrls;
  }

  public void setAccessType(AccessType accessType) {
    // don't store, this will get stored at end of crawl
    bean.accessType = accessType;
  }

  public AccessType getAccessType() {
    return bean.accessType;
  }

  public boolean isOpenAccess() {
    return bean.accessType == AccessType.OpenAccess;
  }

  // CLOCKSS status

  public static final int CLOCKSS_SUB_UNKNOWN = 0;
  public static final int CLOCKSS_SUB_YES = 1;
  public static final int CLOCKSS_SUB_NO = 2;
  public static final int CLOCKSS_SUB_INACCESSIBLE = 3;
  public static final int CLOCKSS_SUB_NOT_MAINTAINED = 4;

  /**
   * Return the CLOCKSS subscription status: CLOCKSS_SUB_UNKNOWN,
   * CLOCKSS_SUB_YES, CLOCKSS_SUB_NO
   */
  public int getClockssSubscriptionStatus() {
    return bean.clockssSubscriptionStatus;
  }

  public String getClockssSubscriptionStatusString() {
    int status = getClockssSubscriptionStatus();
    switch (status) {
    case CLOCKSS_SUB_UNKNOWN: return "Unknown";
    case CLOCKSS_SUB_YES: return "Yes";
    case CLOCKSS_SUB_NO: return "No";
    case CLOCKSS_SUB_INACCESSIBLE: return "Inaccessible";
    case CLOCKSS_SUB_NOT_MAINTAINED: return "";
    default: return "Unknown status " + status;
    }
  }

  public synchronized void setClockssSubscriptionStatus(int val) {
    if (bean.clockssSubscriptionStatus != val) {
      bean.clockssSubscriptionStatus = val;
      needSave("clockssSubscriptionStatus");
    }
  }

  /**
   * Returns the auid
   * @return the auid
   */
  public String getAuId() {
    return bean.auId;
  }

  public void setAuId(String auId) {
    if (auId != null) {
      throw new IllegalStateException("Cannot change AUId from '" + this.auId
	  + "' to '" + auId);
    }

    bean.auId = auId;
  }

  /** Start a batch of updates, deferring saving until unBatchSaves() is
   * called. */
  public synchronized void batchSaves() {
    if (logger.isDebug3()) {
      logger.debug3("Start batch: " + (batchSaveDepth + 1));
    }
    ++batchSaveDepth;
  }

  /** End the update batch, saving if any changes have been made. */
  public synchronized void unBatchSaves() {
    if (batchSaveDepth == 0) {
      logger.warning("unBatchSaves() called when no batch in progress",
		     new Throwable());
      return;
    }
    if (logger.isDebug3()) {
      logger.debug3("End batch: " + batchSaveDepth);
    }
    if (--batchSaveDepth == 0 &&
	changedFields != null && !changedFields.isEmpty()) {
      storeAuState(SetUtil.theSet(changedFields));
    }
  }

  private List<String> changedFields;

  /** Save the named fields or, if we are in a deferred batch, just
   * remember them */
  protected synchronized void needSave(String ... fields) {
    if (batchSaveDepth == 0) {
      storeAuState(fields);
    } else {
      if (changedFields == null) {
	changedFields = new ArrayList<>(5);
      }
      for (String f : fields) {
	changedFields.add(f);
      }
    }
  }

  /** Update the saved state to reflect the changed made to the named
   * fields.
   * @param fields fields to store.  If null, all fields are stored
   */
  public synchronized void storeAuState(String ... fields) {
    switch (fields.length) {
    case 0:
      storeAuState(Collections.EMPTY_SET);
      break;
    case 1:
      storeAuState(Collections.singleton(fields[0]));
      break;
    default:
      Set<String> s = new HashSet<>();
      Collections.addAll(s, fields);
      storeAuState(s);
      break;
    }
  }

  /** Update the saved state to reflect the changed made to the named
   * fields.
   * @param fields fields to store.  If null, all fields are stored
   */
  public synchronized void storeAuState(Set<String> fields) {
    getStateMgr().updateAuState(this, fields);
    changedFields = null;
  }

  /** Serialize entire object to json string */
  public String toJson() throws IOException {
    return bean.toJson();
  }

  /** Serialize a single field to json string */
  public String toJson(String field) throws IOException {
    return bean.toJson(field);
  }

  /** Serialize named fields to json string */
  public String toJson(Set<String> fields) throws IOException {
    return bean.toJson(fields);
  }

  /** Deserialize a json string into this AuState, replacing only those
   * fields that are present in the json string
   * @param json json string
   * @param app
   */
  public AuState updateFromJson(String json, LockssApp app) throws IOException {
    bean.updateFromJson(json, app);
    return this;
  }

  private StateManager getStateMgr() {
    if (stateMgr == null) {
      // XXX very handy for test.  alternative?
      return LockssDaemon.getManagerByTypeStatic(StateManager.class);
    }
    return stateMgr;
  }

  // XXX must call this when load or change cdnStems
  protected void flushAuCaches() {
    try {
      au.setConfiguration(au.getConfiguration());
    } catch (ArchivalUnit.ConfigurationException e) {
      logger.error("Shouldn't happen: au.setConfiguration(au.getConfiguration())",
		   e);
    }
  }

  public String toString() {
    StringBuffer sb = new StringBuffer();
    sb.append("[AuState: ");
    sb.append("lastCrawlTime=");
    sb.append(new Date(bean.lastCrawlTime));
    sb.append(", ");
    sb.append("lastCrawlAttempt=");
    sb.append(new Date(bean.lastCrawlAttempt));
    sb.append(", ");
    sb.append("lastCrawlResult=");
    sb.append(bean.lastCrawlResult);
    sb.append(", ");
    sb.append("lastTopLevelPollTime=");
    sb.append(new Date(bean.lastTopLevelPollTime));
//     sb.append(", ");
//     sb.append("clockssSub=");
//     sb.append(clockssSubscriptionStatus);
    sb.append(", ");
    sb.append("cdn=");
    sb.append(bean.cdnStems);
    sb.append("]");
    return sb.toString();
  }
}
