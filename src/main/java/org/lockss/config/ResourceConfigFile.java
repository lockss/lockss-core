/*

 Copyright (c) 2017 Board of Trustees of Leland Stanford Jr. University,
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

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.zip.GZIPInputStream;
import org.lockss.util.StringUtil;

/**
 * A ConfigFile loaded as a resource.
 */
public class ResourceConfigFile extends BaseConfigFile {

  private File file = null;

  /**
   * Constructor.
   *
   * @param url
   *          A String withe the URL of the file.
   * @param cfgMgr
   *          A ConfigManager with the configuration manager.
   */
  public ResourceConfigFile(String url, ConfigManager cfgMgr) {
    super(url, cfgMgr);
    final String DEBUG_HEADER = "ResourceConfigFile(): ";
    file = cfgMgr.getResourceConfigFile(url);
    if (log.isDebug3()) log.debug3(DEBUG_HEADER + "file = " + file);
  }

  /**
   * Provides an input stream to the content of this file.
   * 
   * @return an InputStream with the input stream to the file contents.
   * @throws IOException
   *           if there are problems.
   */
  @Override
  protected InputStream getInputStreamIfModified() throws IOException {
    final String DEBUG_HEADER = "getInputStreamIfModified(" + m_fileUrl + "): ";
    // The semantics of this are a bit odd, because File.lastModified()
    // returns a long, but we store it as a String.  We're not comparing,
    // just checking equality, so this should be OK
    String lm = calcNewLastModified();

    // Only reload the file if the last modified timestamp is different.
    if (lm.equals(m_lastModified)) {
      if (log.isDebug2()) log.debug2(DEBUG_HEADER
	  + "File has not changed on disk, not reloading: " + m_fileUrl);
      return null;
    }

    if (log.isDebug2()) {
      if (m_lastModified == null) {
	log.debug2(DEBUG_HEADER
	    + "No previous file loaded, loading: " + m_fileUrl);
      } else {
	log.debug2(DEBUG_HEADER + "File has new time (" + lm +
	    "), reloading: " + m_fileUrl);
      }
    }

    return getInputStream();
  }

  /**
   * Provides an input stream to the content of this file, ignoring previous
   * history.
   * <br />
   * Use this to stream the file contents.
   * 
   * @return an InputStream with the input stream to the file contents.
   * @throws IOException
   *           if there are problems.
   */
  @Override
  public InputStream getInputStream() throws IOException {
    InputStream in = new FileInputStream(file);

    if (StringUtil.endsWithIgnoreCase(file.getName(), ".gz")) {
      in = new GZIPInputStream(in);
    }

    return in;
  }

  /**
   * Provides the last modification timestamp as a text string.
   * 
   * @return a String with the last modification timestamp.
   */
  @Override
  protected String calcNewLastModified() {
    final String DEBUG_HEADER = "calcNewLastModified(" + m_fileUrl + "): ";
    if (log.isDebug2())
      log.debug2(DEBUG_HEADER + "m_lastModified = " + m_lastModified);

    if (m_lastModified == null) {
      String result = Long.toString(file.lastModified());
      if (log.isDebug2()) log.debug2(DEBUG_HEADER + "result = " + result);
      return result;
    }

    String result = m_lastModified;
    if (log.isDebug2()) log.debug2(DEBUG_HEADER + "result = " + result);
    return result;
  }
}
