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

package org.lockss.util;

import java.util.*;

/**
 * @deprecated {@link org.lockss.util.TimeZoneUtil} is deprecated (but is used
 *             in plugins); use {@link org.lockss.util.time.TimeZoneUtil} in
 *             lockss-util instead.
 */
@Deprecated
public class TimeZoneUtil {

  /**
   * <p>
   * Returns a {@link TimeZone} instance with exactly the given identifier, or
   * throws {@link IllegalArgumentException}.
   * </p>
   * <p>
   * This is useful because {@link TimeZone#getTimeZone(String)} always returns
   * something but not necessarily what the caller intended; if the time zone
   * identifier has a typo, or if the underlying system's Java time zone data is
   * broken or missing (e.g. well-known annoyance with third-party Java 7 on
   * Ubuntu 16.04), GMT is returned, probably unbeknownst to the caller. The
   * downside of this method is that {@link TimeZone#getTimeZone(String)} might
   * return a {@link TimeZone} instance that is equivalent to the one requested
   * but with another identifier, which this method refuses to do.
   * </p>
   * 
   * @param tzid
   *          A time zone identifier.
   * @return A {@link TimeZone} instance, for which {@link TimeZone#getID()}
   *         returns the same as the given time zone identifier.
   * @throws IllegalArgumentException
   * @since 1.74
   * @deprecated {@link org.lockss.util.TimeZoneUtil} is deprecated (but is used
   *             in plugins); use {@link org.lockss.util.time.TimeZoneUtil} in
   *             lockss-util instead.
   * @see TimeZone#getAvailableIDs()
   * @see TimeZone#getTimeZone(String)
   */
  @Deprecated
  public static TimeZone getExactTimeZone(String tzid) throws IllegalArgumentException {
    return org.lockss.util.time.TimeZoneUtil.getExactTimeZone(tzid);
  }
  
  /**
   * <p>
   * Performs a test to determine if basic time zone data is available. On some
   * systems (e.g. third-party Java 7 Ubuntu 16.04), Java time zone data may be
   * broken or missing, and the effect is that
   * {@link TimeZone#getTimeZone(String)} silently returns GMT.
   * </p>
   * 
   * @return True if and only the test time zones in {@link #BASIC_TIME_ZONES}
   *         seem to exist properly.
   * @since 1.74
   * @deprecated {@link org.lockss.util.TimeZoneUtil} is deprecated (but is used
   *             in plugins); use {@link org.lockss.util.time.TimeZoneUtil} in
   *             lockss-util instead.
   */
  @Deprecated
  public static boolean isBasicTimeZoneDataAvailable() {
    return org.lockss.util.time.TimeZoneUtil.isBasicTimeZoneDataAvailable();
  }
  
  /**
   * @deprecated {@link org.lockss.util.TimeZoneUtil} is deprecated (but is used
   *             in plugins); use {@link org.lockss.util.time.TimeZoneUtil} in
   *             lockss-util instead.
   */
  @Deprecated
  public static final List<String> BASIC_TIME_ZONES = Arrays.asList(
      "GMT",
      "UTC",
      "America/Los_Angeles",
      "America/New_York",
      "US/Pacific",
      "US/Eastern"
  );
                                                                
}
