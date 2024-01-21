/*

Copyright (c) 2000-2024 Board of Trustees of Leland Stanford Jr. University,
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

package org.lockss.poller;

import java.io.*;
import java.util.*;
import java.security.*;

import org.lockss.config.*;
import org.lockss.daemon.*;
import org.lockss.plugin.*;
import org.lockss.poller.v3.V3Poller;
import org.lockss.protocol.*;
import org.lockss.scheduler.*;
import org.lockss.util.*;
import org.lockss.util.time.TimeBase;

import static org.lockss.util.time.TimeUtil.timeIntervalToString;

public class PollUtil {

  public static Logger log = Logger.getLogger();

  public static String makeShortPollKey(String pollKey) {
    if (pollKey == null || pollKey.length() <= 10) {
      return pollKey;
    }
    return pollKey.substring(0, 10);
  }

  /**
   * Find a schedulable duration allowing vote and tally duration.
   *
   * @param hashEst The estimated hash duration.
   * @param tgtVoteDuration  the target duration for vote
   * @param tgtTallyDuration the target duration for tally
   * @param pm the Poll manager
   * @return -1 if no time could be found, otherwise the schedulable duration
   *         that was found.
   */
  public static long findV3SchedulableDuration(long hashEst,
      long tgtVoteDuration,
      long tgtTallyDuration,
      PollManager pm) {
    long tgtDuration = tgtVoteDuration + tgtTallyDuration;
    long minPollDuration = Math.max(tgtDuration, getV3MinPollDuration());
    long maxPollDuration = getV3MaxPollDuration();

    // fraction of entire duration that is vote duration
    double voteFract =
        (double)tgtVoteDuration / (double)(tgtVoteDuration + tgtTallyDuration);

    double mult = getExtendPollMultiplier();

    // amount to increment total duration each time through loop
    long durationIncr = (long)(tgtVoteDuration * mult);

    long duration = minPollDuration;
    while (duration < maxPollDuration) {
      long voteDuration = (long)(duration * voteFract);
      long now = TimeBase.nowMs();
      TimeInterval intrvl =
          new TimeInterval(now + voteDuration, now + duration);
      if (canV3PollBeScheduled(intrvl, hashEst, pm)) {
        if (log.isDebug2()) {
          log.debug2("Found schedulable duration for hash:" +
              timeIntervalToString(hashEst) + " in " +
              toString(intrvl));
        }
        return duration;
      } else {
        if (log.isDebug2()) {
          log.debug2("No room in schedule for hash: " +
              timeIntervalToString(hashEst) + " in " +
              toString(intrvl));
        }
      }
      duration += durationIncr;
    }
    log.info("Found no time for " +
        timeIntervalToString(duration) +
        " poll within " +
        timeIntervalToString(maxPollDuration));
    return -1;
  }

  static String toString(TimeInterval intrvl) {
    return "[" + timeIntervalToString(intrvl.getBeginTime()) +
        ", " + timeIntervalToString(intrvl.getEndTime()) + "]";
  }

  public static boolean canV3PollBeScheduled(TimeInterval scheduleWindow,
      long hashTime,
      PollManager pm) {
    // Should really never happen.
    long dur = scheduleWindow.getTotalTime();
    if (hashTime > dur) {
      log.error("Inconceivable!  Hash time (" +
          timeIntervalToString(hashTime) +
          ") greater than schedule window (" +
          timeIntervalToString(dur) + ")");
      return false;
    }
    StepTask task =
        new StepTask(scheduleWindow, hashTime, null, null) {
          public int step(int n) {
            // this will never be executed
            return n;
          }
        };
    return pm.getDaemon().getSchedService().isTaskSchedulable(task);
  }

  public static long getV3MinPollDuration() {
    return
        CurrentConfig.getTimeIntervalParam(V3Poller.PARAM_MIN_POLL_DURATION,
            V3Poller.DEFAULT_MIN_POLL_DURATION);
  }

  public static long getV3MaxPollDuration() {
    return
        CurrentConfig.getTimeIntervalParam(V3Poller.PARAM_MAX_POLL_DURATION,
            V3Poller.DEFAULT_MAX_POLL_DURATION);
  }

  public static int getVoteDurationMultiplier() {
    return
        CurrentConfig.getIntParam(V3Poller.PARAM_VOTE_DURATION_MULTIPLIER,
            V3Poller.DEFAULT_VOTE_DURATION_MULTIPLIER);
  }

  public static long getVoteDurationPadding() {
    return
        CurrentConfig.getTimeIntervalParam(V3Poller.PARAM_VOTE_DURATION_PADDING,
            V3Poller.DEFAULT_VOTE_DURATION_PADDING);
  }

  public static int getTallyDurationMultiplier() {
    return
        CurrentConfig.getIntParam(V3Poller.PARAM_TALLY_DURATION_MULTIPLIER,
            V3Poller.DEFAULT_TALLY_DURATION_MULTIPLIER);
  }

  public static long getTallyDurationPadding() {
    return
        CurrentConfig.getTimeIntervalParam(V3Poller.PARAM_TALLY_DURATION_PADDING,
            V3Poller.DEFAULT_TALLY_DURATION_PADDING);
  }

  public static long getReceiptPadding() {
    return CurrentConfig.getTimeIntervalParam(V3Poller.PARAM_RECEIPT_PADDING,
        V3Poller.DEFAULT_RECEIPT_PADDING);
  }

  public static double getExtendPollMultiplier() {
    return CurrentConfig.getDoubleParam(V3Poller.PARAM_POLL_EXTEND_MULTIPLIER,
        V3Poller.DEFAULT_POLL_EXTEND_MULTIPLIER);
  }

  public static long calcV3Duration(PollSpec ps, PollManager pm) {
    return calcV3Duration(ps.getCachedUrlSet().estimatedHashDuration(), pm);
  }

  public static long calcV3Duration(long hashEst, PollManager pm) {
    long tgtVoteDuration = v3TargetVoteDuration(hashEst);
    long tgtTallyDuration = v3TargetTallyDuration(hashEst);

    if (log.isDebug2()) {
      log.debug2("[calcDuration] Hash estimate: " +
          timeIntervalToString(hashEst));
      log.debug2("[calcDuration] Target vote duration: " +
          timeIntervalToString(tgtVoteDuration));
      log.debug2("[calcDuration] Target tally duration: " +
          timeIntervalToString(tgtTallyDuration));
    }

    long scheduleTime = findV3SchedulableDuration(hashEst,
        tgtVoteDuration,
        tgtTallyDuration,
        pm);
    if (log.isDebug2()) {
      log.debug("[calcDuration] findV3SchedulableDuration returns "
          + timeIntervalToString(scheduleTime));
    }
    if (scheduleTime < 0) {
      return scheduleTime;
    }
    return scheduleTime + getReceiptPadding();
  }

  public static long v3TargetVoteDuration(long hashEst) {
    long estVoteDuration  = hashEst * getVoteDurationMultiplier() +
        getVoteDurationPadding();
    log.debug2("[estimatedVoteDuration] Estimated Vote Duration: " +
        timeIntervalToString(estVoteDuration));
    return estVoteDuration;
  }

  public static long v3TargetTallyDuration(long hashEst) {
    long estTallyDuration = hashEst * getTallyDurationMultiplier() +
        getTallyDurationPadding();
    log.debug2("[estimatedTallyDuration] Estimated Tally Duration: " +
        timeIntervalToString(estTallyDuration));
    return estTallyDuration;
  }

  /** This is bassackwards and needs refactoring.  The methods above
   * compute the total poll duration by summing all the phases, but do not
   * record those boundaries because they're called by the poll factory,
   * before the poll exists.  This method takes the total duration and
   * solved for the vote deadline and tally deadline based on the
   * parameters that went into the original claculation */
  public static TimeInterval calcV3TallyWindow(long hashEst,
      long totalDuration) {
    long sum = totalDuration - getReceiptPadding();
    double ratio =
        (double)(hashEst * getVoteDurationMultiplier()
            + getVoteDurationPadding())
            / (double)(hashEst * getTallyDurationMultiplier()
            + getTallyDurationPadding());
    long voteDuration = (long)((ratio * sum) / (ratio + 1));
    long tallyDuration = sum - voteDuration;
    long now = TimeBase.nowMs();
    TimeInterval res = new TimeInterval(now + voteDuration,
        now + voteDuration + tallyDuration);
    log.debug2("[calcV3TallyWindow] " + timeIntervalToString(hashEst) +
        " hash in " + timeIntervalToString(totalDuration) +
        ", ratio = " + ratio + ", tally window: " + toString(res));
    return res;
  }


  public static boolean canUseHashAlgorithm(String hashAlg) {
    if (hashAlg == null) {
      return false;
    }
    try {
      MessageDigest.getInstance(hashAlg);
      return true;
    } catch (NoSuchAlgorithmException ex) {
      return false;
    }
  }

  public static MessageDigest createMessageDigest(String hashAlg) {
    try {
      return MessageDigest.getInstance(hashAlg);
    } catch (NoSuchAlgorithmException ex) {
      throw new ShouldNotHappenException("Algorithm "+hashAlg+" not known.");
    }
  }

  public static MessageDigest[] createMessageDigestArray(int len,
      String hashAlg) {
    MessageDigest[] digests = new MessageDigest[len];
    try {
      for (int ix = 0; ix < len; ix++) {
        digests[ix] = MessageDigest.getInstance(hashAlg);
      }
      return digests;
    } catch (NoSuchAlgorithmException ex) {
      throw new ShouldNotHappenException("Algorithm "+hashAlg+" not known.");
    }
  }

  public static byte[] makeHashNonce(int len) {
    return ByteArray.makeRandomBytes(len);
  }

  public static File getPollStateRoot() {
    ConfigManager cfgMgr = ConfigManager.getConfigManager();
    Configuration config = cfgMgr.getCurrentConfig();

    // Old code has separate abs and rel params.  Abs was checked first, so
    // use its param name and pass the rel value as the default.
    String relPath = config.get(V3Poller.PARAM_REL_STATE_PATH,
        V3Poller.DEFAULT_REL_STATE_PATH);
    return cfgMgr.findConfiguredDataDir(V3Poller.PARAM_STATE_PATH, relPath);

  }

  public static File ensurePollStateRoot() {
    File stateDir = getPollStateRoot();
    if (stateDir == null ||
        (!stateDir.exists() && !stateDir.mkdir()) ||
        !stateDir.canWrite()) {
      throw new IllegalArgumentException("Configured V3 data directory " +
          stateDir +
          " does not exist or cannot be " +
          "written to.");
    }
    return stateDir;
  }

  public static int countWillingRepairers(ArchivalUnit au,
      PollManager pollMgr,
      IdentityManager idMgr) {
    double repairThreshold = pollMgr.getMinPercentForRepair();
    int willing = 0;
    Map<PeerIdentity, PeerAgreement> porHints =
        idMgr.getAgreements(au, AgreementType.POR_HINT);
    for (Map.Entry<PeerIdentity, PeerAgreement> ent: porHints.entrySet()) {
      PeerIdentity pid = ent.getKey();
      PeerAgreement agreement = ent.getValue();
      // XXX Not just less-than, but less-or-equal?
      if (agreement.getHighestPercentAgreement() <= repairThreshold) {
        if (log.isDebug3()) {
          log.debug3("Not willing: " + repairThreshold + " >= " + agreement);
        }
        continue;
      }
      if (pollMgr.isNoInvitationSubnet(pid)) {
        if (log.isDebug3()) {
          log.debug3("No invitation subnet: " + pid);
        }
        continue;
      }
      PeerIdentityStatus status = idMgr.getPeerIdentityStatus(pid);
      long lastMessageTime = status.getLastMessageTime();
      long noMessageFor = TimeBase.nowMs() - lastMessageTime;
      if (noMessageFor > pollMgr.getWillingRepairerLiveness()) {
        if (log.isDebug3()) {
          log.debug3("No message for " + noMessageFor + ": " + pid);
        }
        continue;
      }
      willing++;
    }
    return willing;
  }

  public static void closeVoteBlocks(VoteBlocks vbs) {
    if (vbs != null) {
      vbs.close();
    }
  }
}
