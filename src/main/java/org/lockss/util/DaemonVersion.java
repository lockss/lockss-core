/*

Copyright (c) 2000-2019, Board of Trustees of Leland Stanford Jr. University
All rights reserved.

Redistribution and use in source and binary forms, with or without
modification, are permitted provided that the following conditions are met:

1. Redistributions of source code must retain the above copyright notice,
this list of conditions and the following disclaimer.

2. Redistributions in binary form must reproduce the above copyright notice,
this list of conditions and the following disclaimer in the documentation
and/or other materials provided with the distribution.

3. Neither the name of the copyright holder nor the names of its contributors
may be used to endorse or promote products derived from this software without
specific prior written permission.

THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE
LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
POSSIBILITY OF SUCH DAMAGE.

*/

package org.lockss.util;

import java.util.StringTokenizer;
import java.util.regex.*;

/**
 * Representation of a daemon version.
 * @see #DaemonVersion(String)
 */
public class DaemonVersion implements Version, Comparable<DaemonVersion> {

  public static final Pattern PATTERN = Pattern.compile("(?:([01])(?:-[0-9a-zA-Z]+)?\\.)?([0-9]{1,3})(?:-[0-9a-zA-Z]+)?\\.([0-9]{1,3})(?:-[0-9a-zA-Z]+)?");
  
  private String m_verStr;
  private int m_versionMajor;
  private int m_versionMinor;
  private int m_versionBuild;

  /**
   * Construct a Daemon Version from a string.
   *
   * Valid formats are period-separated tokens, each consisting of up to
   * three digits optionally followed by a dash and any nonempty string (where
   * the optional dash and string are ignored); either three tokens if the
   * leading token is 0 or 1, or two tokens otherwise. Examples of valid
   * versions: 2.0, 2.0-alpha (sorts the same as 2.0), 2-b.0 (sorts the same
   * as 2.0), 1.2.3, 1.2.3-testing (sorts the same as 1.2.3), 1.2-b.3 (sorts
   * the same as 1.2.3), 0.1.0. Examples of invalid versions: 0.1 (leading
   * token 0 but only two tokens), 1.2 (leading token 1 but only two tokens),
   * 2.0.0.0 (too many tokens), 7 (not enough tokens), 2a.3b.4c (string not
   * separated from digits by dash), 2.9999.0 (too many digits).
   */
  public DaemonVersion(String ver) {
    Matcher mat = PATTERN.matcher(ver);
    if (!mat.matches()) {
      throw new IllegalArgumentException("Illegal format for Daemon Version: " + ver);
    }
    m_verStr = ver;

    try {
      if (StringUtil.isNullString(mat.group(1))) {
        m_versionMajor = Integer.parseInt(mat.group(2));
        if (m_versionMajor < 2) {
          throw new IllegalArgumentException("Illegal format for Daemon Version: " + ver);
        }
        m_versionMinor = Integer.parseInt(mat.group(3));
        m_versionBuild = 0;
      }
      else {
        m_versionMajor = Integer.parseInt(mat.group(1));
        m_versionMinor = Integer.parseInt(mat.group(2));
        m_versionBuild = Integer.parseInt(mat.group(3));
      }
    } catch (NumberFormatException ex) {
      throw new IllegalArgumentException(ex.toString());
    }

  }

  public long toLong() {
    return 1_000_000L * m_versionMajor + 1_000L * m_versionMinor + m_versionBuild;
  }

  public int compareTo(DaemonVersion other) {
    long x = toLong() - other.toLong();
    if (x > 0) return 1;
    if (x < 0) return -1;
    return 0;
  }

  public String displayString() {
    return m_verStr;
  }

  public String toString() {
    return "[DaemonVersion " + toLong() + "]";
  }

  public int getMajorVersion() {
    return m_versionMajor;
  }

  public int getMinorVersion() {
    return m_versionMinor;
  }

  public int getBuildVersion() {
    return m_versionBuild;
  }
}
