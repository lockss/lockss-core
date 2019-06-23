/*

Copyright (c) 2000-2019 Board of Trustees of Leland Stanford Jr. University,
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

package org.lockss.config;

import java.io.*;

/**
 * A FileConfigFile representing a previously created failover file.
 * Attributes (lastModified) come from it.
 */
public class FailoverFileConfigFile extends FileConfigFile {
  protected ConfigManager.RemoteConfigFailoverInfo m_rcfi;

  /** Create a FailoverFileConfigFile for the url, backed by the failover
   * file named in the supplied RemoteConfigFailoverInfo */
  public FailoverFileConfigFile(String url, ConfigManager cfgMgr,
				ConfigManager.RemoteConfigFailoverInfo rcfi)  {
    super(url, cfgMgr);
    m_rcfi = rcfi;
  }

  /** Return the lastModified saved with the failover file */
  @Override
  protected String calcNewLastModified() throws IOException {
    if (m_rcfi != null) {
      if (m_rcfi.getLastModified() != null) {
	return m_rcfi.getLastModified();
      }
    }
    return super.calcNewLastModified();
  }

  /** Return the lastModified saved with the failover file */
  @Override
  public String getLastModified() {
    if (m_rcfi != null) {
      if (m_rcfi.getLastModified() != null) {
	return m_rcfi.getLastModified();
      }
    }
    return super.getLastModified();
  }

  @Override
  public String toString() {
    return "[FailoverFileConfigFile: m_fileFile=" + m_fileFile + ", " +
      super.toString() + "]";
  }
}
