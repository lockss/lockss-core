/*

Copyright (c) 2000-2017 Board of Trustees of Leland Stanford Jr. University,
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
import org.lockss.util.*;

/**
 * A ConfigFile whose content is generated dynamically.  Subclasses must
 * implement {@link #generateFileContent(File, ConfigManager)} to generate
 * the content. */
public abstract class DynamicConfigFile extends FileConfigFile {

  public DynamicConfigFile(String url, ConfigManager cfgMgr)  {
    super(url, cfgMgr);
  }

  // Temp file is created lazily
  protected void initFile() {
  }

   public InputStream getInputStream() throws IOException {
     if (m_fileFile == null || !m_fileFile.exists() ) {
       generateFile();
     }
     return super.getInputStream();
   }

  protected void generateFile() throws IOException {
     if (m_fileFile == null) {
       String suff = "." + FileUtil.getExtension(m_fileUrl);
       m_fileFile = FileUtil.createTempFile("dyn", suff);
     }
     generateFileContent(m_fileFile, m_cfgMgr);
  }

  /** Concrete subclass must implement this method, to store the file
   * content.
   * @param file file in which the content should be stored
   * @param cfgMgr ConfigManager
   */
  protected abstract void generateFileContent(File file, ConfigManager cfgMgr)
      throws IOException;

  /**
   * Provides the last modification timestamp of this file.
   */
  protected String calcNewLastModified() {
    if (m_fileFile == null || !m_fileFile.exists() ) {
      return "-2";
    } else {
      return super.calcNewLastModified();
    }
  }

  /** Return true of the URL matches the pattern for dynamic config files:
   * <code>dyn:<i>name</i></code>
   * @param url A String with the URLto be examined.
   * @return true if a dynamic config url
   */
  static boolean isDynamicConfigUrl(String url) {
    return url.startsWith("dyn:");
  }

}
