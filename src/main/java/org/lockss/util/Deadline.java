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

package org.lockss.util;

import java.util.*;

import org.lockss.util.time.*;
import org.lockss.util.time.TimeBase;

/**
 * Deadline represents a time (at which some operation must complete).
 * 
 * @deprecated {@code org.lockss.util.Deadline} is deprecated (but is used by
 *             plugins); use {@code org.lockss.util.time.Deadline} instead.
 */
@Deprecated
public class Deadline extends org.lockss.util.time.Deadline {

  /** A long time from now.
   *
   * @deprecated {@code org.lockss.util.Deadline} is deprecated (but is used by
   *             plugins); use {@code org.lockss.util.time.Deadline} instead.
   */
  @Deprecated
  public static final Deadline MAX =
    new Deadline(new ConstantDate(TimeBase.MAX));

  /** An expired Deadline.
   *
   * @deprecated {@code org.lockss.util.Deadline} is deprecated (but is used by
   *             plugins); use {@code org.lockss.util.time.Deadline} instead.
   */
  @Deprecated
  public static final Deadline EXPIRED = new Deadline(new ConstantDate(0));

  /** Create a Deadline that expires at the specified Date, with the
   * specified duration.  Done this way so factory methods don't risk a
   * timer tick between getting the current time, and the constructor
   * computing the duration, which would then be different from what was
   * specified.
   * @param at the Date
   * @param duration the duration
   * @param checkReasonable if true, log a warning if the Deadline is
   * either in the past or unreasonably far in the future.
   *
   * @deprecated {@code org.lockss.util.Deadline} is deprecated (but is used by
   *             plugins); use {@code org.lockss.util.time.Deadline} instead.
   */
  @Deprecated
  private Deadline(Date at, long duration, boolean checkReasonable) {
    super(at, duration, checkReasonable);
  }

  /** Create a Deadline that expires at the specified Date, with the
   * specified duration.  Done this way so factory methods don't risk a
   * timer tick between getting the current time, and the constructor
   * computing the duration, which would then be different from what was
   * specified.
   * @param at the Date
   * @param duration the duration
   *
   * @deprecated {@code org.lockss.util.Deadline} is deprecated (but is used by
   *             plugins); use {@code org.lockss.util.time.Deadline} instead.
   */
  @Deprecated
  private Deadline(Date at, long duration) {
    super(at, duration);
  }

  /** Create a Deadline that expires at the specified Date.
   * @param at the Date
   *
   * @deprecated {@code org.lockss.util.Deadline} is deprecated (but is used by
   *             plugins); use {@code org.lockss.util.time.Deadline} instead.
   */
  @Deprecated
  private Deadline(Date at, boolean checkReasonable) {
    super(at, checkReasonable);
  }

  /** Create a Deadline that expires at the specified Date.
   * @param at the Date
   *
   * @deprecated {@code org.lockss.util.Deadline} is deprecated (but is used by
   *             plugins); use {@code org.lockss.util.time.Deadline} instead.
   */
  @Deprecated
  private Deadline(Date at) {
    super(at);
  }

  /** Create a Deadline that expires at the specified date.
   * @param at the time in ms
   *
   * @deprecated {@code org.lockss.util.Deadline} is deprecated (but is used by
   *             plugins); use {@code org.lockss.util.time.Deadline} instead.
   */
  @Deprecated
  private Deadline(long at, boolean checkReasonable) {
    super(at, checkReasonable);
  }

  /** Create a Deadline that expires at the specified date.
   * @param at the time in ms
   *
   * @deprecated {@code org.lockss.util.Deadline} is deprecated (but is used by
   *             plugins); use {@code org.lockss.util.time.Deadline} instead.
   */
  @Deprecated
  private Deadline(long at) {
    super(at);
  }

  /** Create a Deadline that expires in <code>duration</code> milliseconds.
   * @param duration in ms
   * @return the Deadline
   *
   * @deprecated {@code org.lockss.util.Deadline} is deprecated (but is used by
   *             plugins); use {@code org.lockss.util.time.Deadline} instead.
   */
  @Deprecated
  public static Deadline in(long duration) {
    return new Deadline(new Date(nowMs() + duration), duration);
  }

  /** Create a Deadline representing the specified Date.
   * @param at the Date
   * @return the Deadline
   *
   * @deprecated {@code org.lockss.util.Deadline} is deprecated (but is used by
   *             plugins); use {@code org.lockss.util.time.Deadline} instead.
   */
  @Deprecated
  public static Deadline at(Date at) {
    return new Deadline(at);
  }

  /** Create a Deadline representing the specified date/time.
   * @param at date/time in milliseconds from the epoch.
   * @return the Deadline
   *
   * @deprecated {@code org.lockss.util.Deadline} is deprecated (but is used by
   *             plugins); use {@code org.lockss.util.time.Deadline} instead.
   */
  @Deprecated
  public static Deadline at(long at) {
    return new Deadline(at);
  }

  /** Create a Deadline representing the specified date/time.  This is
   * similar to {@link #at(long)} but suppresses the sanity check.  It is
   * intended to be used when loading or restoring a saved deadline.
   * @param at date/time in milliseconds from the epoch.
   * @return the Deadline
   *
   * @deprecated {@code org.lockss.util.Deadline} is deprecated (but is used by
   *             plugins); use {@code org.lockss.util.time.Deadline} instead.
   */
  @Deprecated
  public static Deadline restoreDeadlineAt(long at) {
    return new Deadline(at, false);
  }

  /** Create a Deadline representing a random time between
   * <code>earliest</code> (inclusive) and <code>latest</code> (exclusive).
   * The random time is uniformly distributed between the endpoints.
   * @param earliest The earliest possible time
   * @param latest The latest possible time
   * @return the Deadline
   *
   * @deprecated {@code org.lockss.util.Deadline} is deprecated (but is used by
   *             plugins); use {@code org.lockss.util.time.Deadline} instead.
   */
  @Deprecated
  public static Deadline atRandomRange(long earliest, long latest) {
    return new Deadline(earliest + getRandom().nextLong(latest - earliest));
  }

  /** Create a Deadline representing a random time between now (inclusive)
   * and <code>before</code> (exclusive).  The random time is uniformly
   * distributed.
   * @param before The time before which the deadline should expire
   * @return the Deadline
   *
   * @deprecated {@code org.lockss.util.Deadline} is deprecated (but is used by
   *             plugins); use {@code org.lockss.util.time.Deadline} instead.
   */
  @Deprecated
  public static Deadline atRandomBefore(long before) {
    return atRandomRange(nowMs(), before);
  }

  /** Create a Deadline representing a random time between
   * <code>earliest</code> (inclusive) and <code>latest</code> (exclusive).
   * The random time is uniformly distributed between the endpoints.
   * @param earliest The earliest possible time
   * @param latest The latest possible time
   * @return the Deadline
   *
   * @deprecated {@code org.lockss.util.Deadline} is deprecated (but is used by
   *             plugins); use {@code org.lockss.util.time.Deadline} instead.
   */
  @Deprecated
  public static Deadline atRandomRange(Deadline earliest, Deadline latest) {
    return atRandomRange(earliest.getExpirationTime(),
			 latest.getExpirationTime());
  }

  /** Create a Deadline representing a random time between now (inclusive)
   * and <code>before</code> (exclusive).  The random time is uniformly
   * distributed.
   * @param before The time before which the deadline should expire
   * @return the Deadline
   *
   * @deprecated {@code org.lockss.util.Deadline} is deprecated (but is used by
   *             plugins); use {@code org.lockss.util.time.Deadline} instead.
   */
  @Deprecated
  public static Deadline atRandomBefore(Deadline before) {
    return atRandomRange(nowMs(), before.getExpirationTime());
  }

  /** Create a Deadline representing a random time between
   * <code>minDuration</code> (inclusive) and <code>maxDuration</code>
   * (exclusive) milliseconds from now.  The random time is uniformly
   * distributed between the endpoints.
   * @param minDuration The minimum duration, in milliseconds.
   * @param maxDuration The maximum duration, in milliseconds.
   * @return the Deadline
   *
   * @deprecated {@code org.lockss.util.Deadline} is deprecated (but is used by
   *             plugins); use {@code org.lockss.util.time.Deadline} instead.
   */
  @Deprecated
  public static Deadline inRandomRange(long minDuration, long maxDuration) {
    return atRandomRange(nowMs() + minDuration, nowMs() + maxDuration);
  }

  /** Create a Deadline representing a random time between now (inclusive)
   * and <code>maxDuration</code> (exclusive) milliseconds from now.  The
   * random time is uniformly distributed.
   * @param maxDuration The maximum duration, in milliseconds.
   * @return the Deadline
   *
   * @deprecated {@code org.lockss.util.Deadline} is deprecated (but is used by
   *             plugins); use {@code org.lockss.util.time.Deadline} instead.
   */
  @Deprecated
  public static Deadline inRandomBefore(long maxDuration) {
    return inRandomRange(0, maxDuration);
  }

  /** Create a Deadline representing a random time deviating from the
   * meanDuration by at most delta.  The random time is uniformly distributed.
   * @param meanDuration The mean duration, in milliseconds.
   * @param delta the max deviation
   * @return the Deadline
   *
   * @deprecated {@code org.lockss.util.Deadline} is deprecated (but is used by
   *             plugins); use {@code org.lockss.util.time.Deadline} instead.
   */
  @Deprecated
  public static Deadline inRandomDeviation(long meanDuration, long delta) {
    return inRandomRange(meanDuration - delta, meanDuration + delta);
  }

//   /** Return a timer whose duration is a random, normally distrubuted value
//    * whose mean is <code>meanDuration</code> and standard deviation
//    * <code>stddev</code>.  */
//   public Deadline withinOf(double stddev, long meanDuration) {
//     super(meanDuration + (long)(stddev * getRandom().nextGaussian()));
//   }

  /**
   * @deprecated {@code org.lockss.util.Deadline} is deprecated (but is used by
   *             plugins); use {@code org.lockss.util.time.Deadline} instead.
   */
  @Deprecated
  public static void setReasonableDeadlineRange(long maxInPast,
						long maxInFuture) {
    org.lockss.util.time.Deadline.setReasonableDeadlineRange(maxInPast, maxInFuture);
  }

  /**
   * Return the earlier of two deadlines
   * @param d1 first Deadline
   * @param d2 second Deadline
   * @return d1 if it is before d2, else d2
   *
   * @deprecated {@code org.lockss.util.Deadline} is deprecated (but is used by
   *             plugins); use {@code org.lockss.util.time.Deadline} instead.
   */
  @Deprecated
  public static synchronized Deadline earliest(Deadline d1, Deadline d2) {
    return d1.before(d2) ? d1 : d2;
  }

  /**
   * Return the later of two deadlines
   * @param d1 first Deadline
   * @param d2 second Deadline
   * @return d2 if d1 is before it, else d1
   *
   * @deprecated {@code org.lockss.util.Deadline} is deprecated (but is used by
   *             plugins); use {@code org.lockss.util.time.Deadline} instead.
   */
  @Deprecated
  public static synchronized Deadline latest(Deadline d1, Deadline d2) {
    return d1.before(d2) ? d2 : d1;
  }

  /**
   * The Deadline.Callback interface defines the
   * method that will be called if/when a deadline changes.
   *
   * @deprecated {@code org.lockss.util.Deadline} is deprecated (but is used by
   *             plugins); use {@code org.lockss.util.time.Deadline} instead.
   */
  @Deprecated
  public interface Callback extends org.lockss.util.time.Deadline.Callback {
    /**
     * Called when the deadline's duration is changed.
     * @param deadline  the Deadline that changed.
     *
     * @deprecated {@code org.lockss.util.Deadline} is deprecated (but is used by
     *             plugins); use {@code org.lockss.util.time.Deadline} instead.
     */
    @Deprecated
    default void changed(org.lockss.util.time.Deadline deadline) {
      changed((Deadline)deadline);
    }
    
    void changed(Deadline deadline);
  }

  /**
   * A Deadline.Callback that interrupts a thread.  Example use:<pre>
    Deadline.InterruptCallback cb = new Deadline.InterruptCallback();
    try {
      deadline.registerCallback(cb);
      while (queue.isEmpty() && !deadline.expired()) {
        this.wait(deadline.getSleepTime());
      }
    } finally {
      cb.disable();
      deadline.unregisterCallback(cb);
    }<pre>
   *
   * @deprecated {@code org.lockss.util.Deadline} is deprecated (but is used by
   *             plugins); use {@code org.lockss.util.time.Deadline} instead.
   */
  @Deprecated
  public static class InterruptCallback extends org.lockss.util.time.Deadline.InterruptCallback implements Callback {

    @Override
    public synchronized void changed(Deadline deadline) {
      super.changed((org.lockss.util.time.Deadline)deadline);
    }
    
  }

}
