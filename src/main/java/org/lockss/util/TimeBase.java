/*
 * $Id$
 */

/*

Copyright (c) 2000-2012 Board of Trustees of Leland Stanford Jr. University,
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

import org.lockss.util.*;

import java.text.*;

/**
 * TimeBase allows use of a simulated time base for testing.
 * 
 * Instead of calling <code>System.currentTimeMillis()</code> or <code>new
 * Date()</code>, other parts of the system should call {@link #nowMs()} or
 * {@link #nowDate()}. When in real mode (the default), these methods return the
 * same value as the normal methods. In simulated mode, they return the contents
 * of an internal counter, which can be incremented programmatically. This
 * allows time-dependent functions to be tested quickly and predictably.
 * 
 * @deprecated {@code org.lockss.util.TimeBase} is deprecated (but is used by
 *             plugins); use {@code org.lockss.util.time.TimeBase} in
 *             lockss-util instead.
 */
@Deprecated
public class TimeBase {

  /** A long time from now.
   * 
   * @deprecated {@code org.lockss.util.TimeBase} is deprecated (but is used by
   *             plugins); use {@code org.lockss.util.time.TimeBase} in
   *             lockss-util instead.
   */
  @Deprecated
  public static final long MAX = org.lockss.util.time.TimeBase.MAX;

  /**
   * @deprecated {@code org.lockss.util.TimeBase} is deprecated (but is used by
   *             plugins); use {@code org.lockss.util.time.TimeBase} in
   *             lockss-util instead.
   */
  @Deprecated
  private static volatile boolean isSimulated = false;

  /**
   * @deprecated {@code org.lockss.util.TimeBase} is deprecated (but is used by
   *             plugins); use {@code org.lockss.util.time.TimeBase} in
   *             lockss-util instead.
   */
  @Deprecated
  private static volatile long simulatedTime;

  /** No instances
   * 
   * @deprecated {@code org.lockss.util.TimeBase} is deprecated (but is used by
   *             plugins); use {@code org.lockss.util.time.TimeBase} in
   *             lockss-util instead.
   */
  @Deprecated
  private TimeBase() {
  }

  /** Set TimeBase into real mode.
   * 
   * @deprecated {@code org.lockss.util.TimeBase} is deprecated (but is used by
   *             plugins); use {@code org.lockss.util.time.TimeBase} in
   *             lockss-util instead.
   */
  @Deprecated
  public static void setReal() {
    org.lockss.util.time.TimeBase.setReal();
  }

  /** Set TimeBase into simulated mode.
   * @param time  Simulated time to set as current
   * 
   * @deprecated {@code org.lockss.util.TimeBase} is deprecated (but is used by
   *             plugins); use {@code org.lockss.util.time.TimeBase} in
   *             lockss-util instead.
   */
  @Deprecated
  public static void setSimulated(long time) {
    org.lockss.util.time.TimeBase.setSimulated(time);
  }

  /** Set TimeBase into simulated mode.
   * @param time Date/time string to set as current time, in format
   * <code>yyyy/MM/dd HH:mm:ss</code>
   * 
   * @deprecated {@code org.lockss.util.TimeBase} is deprecated (but is used by
   *             plugins); use {@code org.lockss.util.time.TimeBase} in
   *             lockss-util instead.
   */
  @Deprecated
  public static void setSimulated(String dateTime) throws ParseException {
    org.lockss.util.time.TimeBase.setSimulated(dateTime);
  }

  /** Set TimeBase into simulated mode, at time 0
   *
   * @deprecated {@code org.lockss.util.TimeBase} is deprecated (but is used by
   *             plugins); use {@code org.lockss.util.time.TimeBase} in
   *             lockss-util instead.
   */
  @Deprecated
  public static void setSimulated() {
    org.lockss.util.time.TimeBase.setSimulated();
  }

  /** Return true iff simulated time base is in effect
   *
   * @deprecated {@code org.lockss.util.TimeBase} is deprecated (but is used by
   *             plugins); use {@code org.lockss.util.time.TimeBase} in
   *             lockss-util instead.
   */
  @Deprecated
  public static boolean isSimulated() {
    return org.lockss.util.time.TimeBase.isSimulated();
  }

  /** Step simulated time base by n ticks
   * 
   * @deprecated {@code org.lockss.util.TimeBase} is deprecated (but is used by
   *             plugins); use {@code org.lockss.util.time.TimeBase} in
   *             lockss-util instead.
   */
  @Deprecated
  public static void step(long n) {
    org.lockss.util.time.TimeBase.step(n);
  }

  /** Step simulated time base by 1 tick
   * 
   * @deprecated {@code org.lockss.util.TimeBase} is deprecated (but is used by
   *             plugins); use {@code org.lockss.util.time.TimeBase} in
   *             lockss-util instead.
   */
  @Deprecated
  public static void step() {
    org.lockss.util.time.TimeBase.step();
  }

  /** Return the current time, in milliseconds.  In real mode, this returns
   * System.currentTimeMillis(); in simulated mode it returns the simulated
   * time.
   * 
   * @deprecated {@code org.lockss.util.TimeBase} is deprecated (but is used by
   *             plugins); use {@code org.lockss.util.time.TimeBase} in
   *             lockss-util instead.
   */
  @Deprecated
  public static long nowMs() {
    return org.lockss.util.time.TimeBase.nowMs();
  }

  /** Return the current time, as a Date.  In real mode, this returns
   * new Date(); in simulated mode it returns the simulated time as a Date.
   * 
   * @deprecated {@code org.lockss.util.TimeBase} is deprecated (but is used by
   *             plugins); use {@code org.lockss.util.time.TimeBase} in
   *             lockss-util instead.
   */
  @Deprecated
  public static Date nowDate() {
    return org.lockss.util.time.TimeBase.nowDate();
  }

  /** Return the number of milliseconds since the argument
   * @param when a time
   * 
   * @deprecated {@code org.lockss.util.TimeBase} is deprecated (but is used by
   *             plugins); use {@code org.lockss.util.time.TimeBase} in
   *             lockss-util instead.
   */
  @Deprecated
  public static long msSince(long when) {
    return org.lockss.util.time.TimeBase.msSince(when);
  }

  /** Return the number of milliseconds until the argument
   * @param when a time
   * 
   * @deprecated {@code org.lockss.util.TimeBase} is deprecated (but is used by
   *             plugins); use {@code org.lockss.util.time.TimeBase} in
   *             lockss-util instead.
   */
  @Deprecated
  public static long msUntil(long when) {
    return org.lockss.util.time.TimeBase.msUntil(when);
  }

  /** Return a Calendar set to the current real or simulated time
   * 
   * @deprecated {@code org.lockss.util.TimeBase} is deprecated (but is used by
   *             plugins); use {@code org.lockss.util.time.TimeBase} in
   *             lockss-util instead.
   */
  @Deprecated
  public static Calendar nowCalendar() {
    return org.lockss.util.time.TimeBase.nowCalendar();
  }

}
