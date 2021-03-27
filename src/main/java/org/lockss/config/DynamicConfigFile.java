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

package org.lockss.config;

import java.io.*;
import org.lockss.util.*;
import org.lockss.util.io.FileUtil;

/**
 * A ConfigFile whose content is generated dynamically.  Subclasses must
 * implement {@link #generateFileContent(File, ConfigManager)} to generate
 * the content. */
public abstract class DynamicConfigFile extends FileConfigFile {

  private boolean valid = false;
  // Time the file was last generated.  More precise than file's timestamp.
  long newLastModified = 0;

  public DynamicConfigFile(String url, ConfigManager cfgMgr)  {
    super(url, cfgMgr);
  }

  // Temp file is created lazily
  protected void initFile() {
  }

  private void generateFileIfNecessary() throws IOException {
    if (m_fileFile == null || !m_fileFile.exists() || !valid) {
      generateFile();
    }
  }

  public InputStream getInputStream() throws IOException {
    generateFileIfNecessary();
    return super.getInputStream();
  }

  protected void generateFile() throws IOException {
     if (m_fileFile == null) {
       String suff = "." + FileUtil.getExtension(m_fileUrl);
       m_fileFile = FileUtil.createTempFile("dyn", suff);

       // deleteOnExit() is appropriate here because the file is created
       // only once.  Its lifetime corresponds to that of the jvm, and
       // there are at most a few of them
       m_fileFile.deleteOnExit();
     }
     generateFileContent(m_fileFile, m_cfgMgr);
     valid = true;
     // keep track of last generation time, so can notice updates with more
     // precision than filesystem.  This is mostly useful for unit test.
     newLastModified = TimeBase.nowMs();
  }

  /** Concrete subclass must implement this method, to store the file
   * content.
   * @param file file in which the content should be stored
   * @param cfgMgr ConfigManager
   */
  protected abstract void generateFileContent(File file, ConfigManager cfgMgr)
      throws IOException;

  /** Cause the content to be regenerated */
  public void invalidate() {
    // setting m_fileFile null here would create concurrency issues
    log.debug("Invalidating: " + m_fileUrl);
    valid = false;
    setNeedsReload();
  }

  /**
   * Regenerate the file if necessary, then return the last generation
   * time.  This is more precise than the file modification time and allows
   * for easier testing */
  protected String calcNewLastModified() throws IOException {
    generateFileIfNecessary();
    return Long.toString(newLastModified);
  }

  /** Return true of the URL matches the pattern for dynamic config files:
   * <code>dyn:<i>name</i></code>
   * @param url A String with the URLto be examined.
   * @return true if a dynamic config url
   */
  static boolean isDynamicConfigUrl(String url) {
    return url.startsWith("dyn:");
  }

  /**
   * Provides the input stream to the content of this configuration file if the
   * passed preconditions are met.
   * 
   * @param preconditions
   *          An HttpRequestPreconditions with the request preconditions to be
   *          met.
   * @return a ConfigFileReadWriteResult with the result of the operation.
   * @throws IOException
   *           if there are problems.
   */
  @Override
  public synchronized ConfigFileReadWriteResult conditionallyRead(HttpRequestPreconditions preconditions)
      throws IOException {
    final String DEBUG_HEADER = "conditionallyRead(" + m_fileUrl + "): ";
    if (log.isDebug2())
      log.debug2(DEBUG_HEADER + "preconditions = " + preconditions);

    if (m_fileFile == null) {
      generateFile();
    }

    return super.conditionallyRead(preconditions);
  }

  /**
   * Used for logging and testing and debugging.
   */
  @Override
  public String toString() {
    return "[DynamicConfigFile: " + super.toString() + "]";
  }
}
