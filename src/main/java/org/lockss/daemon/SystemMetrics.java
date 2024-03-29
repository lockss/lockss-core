/*
 * $Id$
 */

/*

Copyright (c) 2000-2014 Board of Trustees of Leland Stanford Jr. University,
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

import java.io.IOException;
import java.security.MessageDigest;
import java.util.Hashtable;

import org.lockss.app.*;
import org.lockss.config.*;
import org.lockss.hasher.HashService;
import org.lockss.plugin.PluginManager;
import org.lockss.protocol.LcapMessage;
import org.lockss.util.*;
import org.lockss.util.time.Deadline;
import org.lockss.util.time.TimeBase;

/**
 * A singleton class which provides access to various system calculations, such
 * as hash speed estimates.
 */
public class SystemMetrics
  extends BaseLockssManager implements ConfigurableManager {
  static final String PREFIX = Configuration.PREFIX + "metrics.";

  /**
   * The interval at which memory usage should be logged, or 0 to disable
   */
  public static final String PARAM_MEM_LOG_INTERVAL =
    PREFIX + "logMem.interval";
  static final long DEFAULT_MEM_LOG_INTERVAL = 0;

  /**
   * Configuration parameter name for duration, in ms, for which the hash test
   * should run.
   */
  public static final String PARAM_HASH_TEST_DURATION =
    PREFIX + "hash.duration";
  static final long DEFAULT_HASH_TEST_DURATION = 100 * Constants.SECOND;

  /**
   * Configuration parameter name for the number of bytes per step in the hash
   * test.
   */
  public static final String PARAM_HASH_TEST_BYTE_STEP =
    PREFIX + "hash.stepsize";
  static final int DEFAULT_HASH_TEST_BYTE_STEP = 10 * 1024;

  /**
   * Configuration parameter name for the default hash speed for new AUs.
   */
  public static final String PARAM_DEFAULT_HASH_SPEED =
    PREFIX + "default.hash.speed";
  static final int DEFAULT_DEFAULT_HASH_SPEED = 250;

  /**
   * Configuration parameter name for the slowest hash rate in the cache group.
   */
  public static final String PARAM_SLOWEST_RATE = PREFIX + "slowest.hashrate";
  static final int DEFAULT_SLOWEST_RATE = 250;

  private static Logger logger = Logger.getLogger();

  Hashtable estimateTable = new Hashtable();
  MessageDigest defaultDigest = LcapMessage.getDefaultMessageDigest();
  private HashService hashService = null;
  private PluginManager pluginMgr = null;
  int defaultSpeed = DEFAULT_DEFAULT_HASH_SPEED;
  private TimerQueue.Request req;
  private long memLogInterval = DEFAULT_MEM_LOG_INTERVAL;
  private String lastStringPoolStats;

  public void startService() {
    super.startService();
    try {
      hashService = getApp().getManagerByType(HashService.class);
    } catch (IllegalArgumentException e) {
      // ignore
    }
    try {
      pluginMgr = getApp().getManagerByType(PluginManager.class);
    } catch (IllegalArgumentException e) {
      // ignore
    }
    resetConfig();
  }

  public void setConfig(Configuration newConfig,
			Configuration prevConfig,
			Configuration.Differences changedKeys) {
    if (changedKeys.contains(PARAM_DEFAULT_HASH_SPEED)) {
      defaultSpeed = newConfig.getInt(PARAM_DEFAULT_HASH_SPEED,
				      DEFAULT_DEFAULT_HASH_SPEED);
      estimateTable.clear();
    }
    if (changedKeys.contains(PARAM_MEM_LOG_INTERVAL)) {
      memLogInterval = newConfig.getTimeInterval(PARAM_MEM_LOG_INTERVAL,
						 DEFAULT_MEM_LOG_INTERVAL);
      if (memLogInterval > 0) {
	doMemLog();
      }
    }
  }

  public synchronized void stopService() {
    if (req != null) {
      TimerQueue.cancel(req);
    }
    super.stopService();
  }

  /**
   * Returns a hash estimate based on the default algorithm.  If no
   * estimate is available, returns the slowest hash speed.
   * @return the hash speed estimate in bytes/ms
   * @throws NoHashEstimateAvailableException if no estimate is available
   */
  public int getBytesPerMsHashEstimate() {
    try {
      return getBytesPerMsHashEstimate(null, defaultDigest);
    } catch (IOException e) {
      return defaultSpeed;
    }
  }

  /**
   * Returns an estimate of the hash speed for this hasher.
   * First tries to get a real value from the HashService.  If not available,
   * calculates an estimate.
   * @param hasher the CachedUrlSetHasher to test
   * @param digest the hashing algorithm
   * @return hash speed in bytes/ms
   * @throws IOException
   */
  public int getBytesPerMsHashEstimate(CachedUrlSetHasher hasher,
				       MessageDigest digest)
      throws IOException {
    int speed = -1;
    if (hashService != null) {
      speed = hashService.getHashSpeed(digest);
    }
    if (speed <= 0) {
      speed = getHashSpeedEstimate(hasher, digest);
    }
    return speed;
  }

  /**
   * Returns an estimate of the hash speed for this hasher.
   * First tries to get a real value from the HashService.  If not available,
   * calculates an estimate.
   * @param hasher the CachedUrlSetHasher to test
   * @param digest the hashing algorithm
   * @return hash speed in bytes/ms
   * @throws IOException
   */
  private int getHashSpeedEstimate(CachedUrlSetHasher hasher,
				   MessageDigest digest)
      throws IOException {
    Integer estimate = (Integer)estimateTable.get(digest.getAlgorithm());
    if (estimate==null) {
      // don't calculate; use default instead
      estimate = Integer.valueOf(defaultSpeed);
      //      estimate = Integer.valueOf(measureHashSpeed(hasher, digest));
      estimateTable.put(digest.getAlgorithm(), estimate);
    }
    return estimate.intValue();
  }

  /**
   * Calculate an estimate of the hash speed for a hasher.
   * Tests by hashing the CachedUrlSet for a small period of time.
   * @param hasher the CachedUrlSetHasher to test
   * @param digest the hashing algorithm
   * @return an int for estimated bytes/ms
   * @throws IOException
   */
  public int measureHashSpeed(CachedUrlSetHasher hasher, MessageDigest digest)
      throws IOException {
    long timeTaken = 0;
    long bytesHashed = 0;
    boolean earlyFinish = false;
    long hashDuration =
      CurrentConfig.getTimeIntervalParam(PARAM_HASH_TEST_DURATION,
                                         DEFAULT_HASH_TEST_DURATION);
    int hashStep =
      CurrentConfig.getIntParam(PARAM_HASH_TEST_BYTE_STEP,
                                DEFAULT_HASH_TEST_BYTE_STEP);

    long startTime = TimeBase.nowMs();
    Deadline deadline = Deadline.in(hashDuration);
    while (!deadline.expired() && !hasher.finished()) {
      bytesHashed += hasher.hashStep(hashStep);
    }
    timeTaken = TimeBase.msSince(startTime);
    if (timeTaken==0) {
      logger.warning("Test finished in zero time: using bytesHashed as estimate.");
      return (int)bytesHashed;
    }
    int res = (int)(bytesHashed / timeTaken);
    logger.debug("Measured hash speed: " + res);
    return res;
  }

  /**
   * Return the hash speed of the slowest cache
   * @return the slowest speed
   */
  public int getSlowestHashSpeed() {
    return CurrentConfig.getIntParam(PARAM_SLOWEST_RATE, DEFAULT_SLOWEST_RATE);
  }

  /** Exception thrown if no hash estimate is available. */
  public static class NoHashEstimateAvailableException extends Exception {
    public NoHashEstimateAvailableException() {
      super();
    }
  }

  void schedMemLog() {
    if (req != null) {
      TimerQueue.cancel(req);
    }
    if (memLogInterval > 0) {
      req = TimerQueue.schedule(Deadline.in(memLogInterval),
				memLogCallback, null);
    }
  }

  // Memory logger TimerQueue callback.
  private TimerQueue.Callback memLogCallback =
    new TimerQueue.Callback() {
      public void timerExpired(Object cookie) {
	doMemLog();
      }
      public String toString() {
	return "Memory logger";
      }
    };

  // Log the current memory usage
  private void doMemLog() {
    Runtime rt = Runtime.getRuntime();
    long tot = rt.totalMemory();
    long free = rt.freeMemory();
    long max = rt.maxMemory();
    logger.info("Memory Used: " + K(tot - free) +
		", Free: " + K(free) +
		", Total: " + K(tot) +
		", Max: " + K(max) +
		( (pluginMgr == null) ? "" :
		  (", " + pluginMgr.getAllAus().size() + " AUs")));
    if (logger.isDebug2()) {
      String spStats = StringPool.allStats();
      // Log StringPool stats iff they have changed
      if (spStats != null && !spStats.equals(lastStringPoolStats)) {
        logger.debug2("String Pools:\n" + spStats);
        lastStringPoolStats = spStats;
      }
    }
    schedMemLog();
  }

  private String K(long n) {
    return StringUtil.sizeToString(n);
  }
}
