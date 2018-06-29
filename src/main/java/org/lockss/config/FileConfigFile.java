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
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.util.List;
import java.util.zip.*;
import org.lockss.util.*;
import org.springframework.http.MediaType;

/**
 * A simple wrapper class around the representation of a
 * generic Configuration loaded from disk.
 */

public class FileConfigFile extends BaseConfigFile {
  protected File m_fileFile;

  public FileConfigFile(String url, ConfigManager cfgMgr)  {
    super(url, cfgMgr);
    initFile();
  }

  /**
   * Given a file spec as either a String (path name) or url (file://),
   * return a File object.
   *
   * NB: Java 1.4 supports constructing File objects from a file: URI,
   * which will eliminate the need for this method.
   */
  protected File makeFile() {
    String file = getFileUrl();
    if (UrlUtil.isFileUrl(file)) {
      String fileLoc = file.substring("file:".length());
      return new File(fileLoc);
    } else {
      return new File(file);
    }
  }

  protected void initFile() {
    m_fileFile = makeFile();
  }

  File getFile() {
    return m_fileFile;
  }

  /** Notify us that the file was just written, with these contents, so we
   * can remember the modification time. */
  // XXX ConfigFile should handle file writing internally
  public void storedConfig(Configuration newConfig) throws IOException {
      
    ConfigurationPropTreeImpl nc;
    if (newConfig.isSealed() &&
	newConfig instanceof ConfigurationPropTreeImpl) {
      nc = (ConfigurationPropTreeImpl)newConfig;
    } else {
      nc = new ConfigurationPropTreeImpl();
      nc.copyFrom(newConfig);
    }
    nc.seal();
    m_config = nc;
    m_lastModified = calcNewLastModified();
    log.debug2("storedConfig at: " + m_lastModified);
    m_generation++;
  }

   protected InputStream getInputStreamIfModified() throws IOException {
     final String DEBUG_HEADER =
	 "getInputStreamIfModified(" + m_fileUrl + "): ";
     if (log.isDebug2()) log.debug2(DEBUG_HEADER
	 + "reloadUnconditionally = " + reloadUnconditionally);
     // The semantics of this are a bit odd, because File.lastModified()
     // returns a long, but we store it as a String.  We're not comparing,
     // just checking equality, so this should be OK
     String lm = calcNewLastModified();

    // Only reload the file if the last modified timestamp is different.
    if (!reloadUnconditionally && lm.equals(m_lastModified)) {
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
    reloadUnconditionally = false;
    return getInputStream();
   }

  /**
   * Provides an input stream to the content of this file.
   * <br>
   * Use this to stream the file contents.
   * 
   * @return an InputStream with the input stream to the file contents.
   * @throws IOException
   *           if there are problems.
   */
   public InputStream getInputStream() throws IOException {
     InputStream in = new FileInputStream(m_fileFile);
     if (StringUtil.endsWithIgnoreCase(m_fileFile.getName(), ".gz")) {
       in = new GZIPInputStream(in);
     }
     return in;
   }

  /**
   * Provides the last modification timestamp of this file.
   */
  protected String calcNewLastModified() {
    final String DEBUG_HEADER = "calcNewLastModified(" + m_fileUrl + "): ";
    if (log.isDebug2())
      log.debug2(DEBUG_HEADER + "m_lastModified = " + m_lastModified);
    if (m_lastModified == null) {
      String result = Long.toString(m_fileFile.lastModified());
      if (log.isDebug2()) log.debug2(DEBUG_HEADER + "result = " + result);
      return result;
    }

    try {
      if (Long.parseLong(m_lastModified) < m_fileFile.lastModified()) {
	String result = Long.toString(m_fileFile.lastModified());
	if (log.isDebug2()) log.debug2(DEBUG_HEADER + "result = " + result);
	return result;
      }
    } catch (NumberFormatException nfe) {
      String result = Long.toString(m_fileFile.lastModified());
      if (log.isDebug2()) log.debug2(DEBUG_HEADER + "result = " + result);
      return result;
    }

    String result = m_lastModified;
    if (log.isDebug2()) log.debug2(DEBUG_HEADER + "result = " + result);
    return result;
  }

  /**
   * Do the actual writing of the file to the disk by renaming a temporary file.
   * 
   * @param tempfile
   *          A File with the source temporary file.
   * @param config
   *          A Configuration with the configuration to be written.
   * @throws IOException
   *           if there are problems.
   */
  @Override
  public void writeFromTempFile(File tempfile, Configuration config)
      throws IOException {
    final String DEBUG_HEADER = "writeFromTempFile(" + m_fileUrl + "): ";
    if (log.isDebug2()) log.debug2(DEBUG_HEADER + "tempfile = " + tempfile);

    File target = new File(getFileUrl());
    if (log.isDebug3()) log.debug3(DEBUG_HEADER + "target = " + target);

    if (!PlatformUtil.updateAtomically(tempfile, target)) {
      throw new RuntimeException("Couldn't rename temp file: " + tempfile
	  + " to: " + target);
    }

    log.info(DEBUG_HEADER + "m_lastModified = " + m_lastModified);

    // Check whether there was a previous last modification timestamp.
    if (m_lastModified != null) {
      // Yes: Loop until the current timestamp is different than the previous
      // timestamp.
      while (Long.toString(TimeBase.nowMs()).equals(m_lastModified)) {
	if (log.isDebug3()) log.debug3(DEBUG_HEADER + "Sleeping for 1 ms...");
	try { Thread.sleep(1); } catch (InterruptedException ie) {}
      }
    }

    // Update the last modification timestamp.
    m_lastModified = Long.toString(TimeBase.nowMs());
    if (log.isDebug3())
      log.debug3(DEBUG_HEADER + "m_lastModified = " + m_lastModified);
    
    if (log.isDebug2())
      log.debug2(DEBUG_HEADER + "Wrote cache config file: " + target);

    reloadUnconditionally = true;
    if (log.isDebug3()) log.debug3(DEBUG_HEADER
	+ "reloadUnconditionally = " + reloadUnconditionally);

    if (config == null) {
      config = getConfiguration();
    }

    storedConfig(config);
  }

  /**
   * Provides the input stream to the content of this configuration file if the
   * passed preconditions are met.
   * 
   * @param ifMatch
   *          A List<String> with an asterisk or values equivalent to the
   *          "If-Unmodified-Since" request header but with a granularity of 1
   *          ms.
   * @param ifNoneMatch
   *          A List<String> with an asterisk or values equivalent to the
   *          "If-Modified-Since" request header but with a granularity of 1 ms.
   * @return a ConfigFileReadWriteResult with the result of the operation.
   * @throws IOException
   *           if there are problems.
   */
  @Override
  public ConfigFileReadWriteResult conditionallyRead(List<String> ifMatch,
      List<String> ifNoneMatch) throws IOException {
    final String DEBUG_HEADER = "conditionallyRead(" + m_fileUrl + "): ";
    if (log.isDebug2()) {
      log.debug2(DEBUG_HEADER + "ifMatch = " + ifMatch);
      log.debug2(DEBUG_HEADER + "ifNoneMatch = " + ifNoneMatch);
    }

    synchronized(this) {
      // Use the last modification timestamp as this file version unique
      // identifier.
      String versionUniqueId = calcNewLastModified();
      if (log.isDebug3())
	log.debug3(DEBUG_HEADER + "versionUniqueId = " + versionUniqueId);

      // Get the file content type.
      MediaType contentType = MediaType.TEXT_PLAIN;

      if (m_fileType == ConfigFile.XML_FILE) {
	contentType = MediaType.TEXT_XML;
      }

      ConfigFileReadWriteResult readResult = null;

      // Check whether the precondition is met.
      if (isPreconditionMet(ifMatch, ifNoneMatch,
	  "\"" + versionUniqueId + "\"")) {
	// Yes: The precondition is met.
	readResult = new ConfigFileReadWriteResult(getInputStream(),
	    versionUniqueId, true, contentType);
      } else {
	// No: The precondition is not met.
	readResult = new ConfigFileReadWriteResult(null, versionUniqueId, false,
	    contentType);
      }

      if (log.isDebug2())
	log.debug2(DEBUG_HEADER + "readResult = " + readResult);
      return readResult;
    }
  }

  /**
   * Writes the passed content to this configuration file if the passed
   * preconditions are met.
   * 
   * @param ifMatch
   *          A List<String> with an asterisk or values equivalent to the
   *          "If-Unmodified-Since" request header but with a granularity of 1
   *          ms.
   * @param ifNoneMatch
   *          A List<String> with an asterisk or values equivalent to the
   *          "If-Modified-Since" request header but with a granularity of 1 ms.
   * @param inputStream
   *          An InputStream to the content to be written to this configuration
   *          file.
   * @return a ConfigFileReadWriteResult with the result of the operation.
   * @throws IOException
   *           if there are problems.
   */
  @Override
  public ConfigFileReadWriteResult conditionallyWrite(List<String> ifMatch,
      List<String> ifNoneMatch, InputStream inputStream) throws IOException {
    final String DEBUG_HEADER = "conditionallyWrite(" + m_fileUrl + "): ";
    if (log.isDebug2()) {
      log.debug2(DEBUG_HEADER + "ifMatch = " + ifMatch);
      log.debug2(DEBUG_HEADER + "ifNoneMatch = " + ifNoneMatch);
      log.debug2(DEBUG_HEADER + "inputStream = " + inputStream);
    }

    synchronized(this) {
      String lastModified = calcNewLastModified();
      String lastModifiedAsEtag = "\"" + lastModified + "\"";
      ConfigFileReadWriteResult writeResult = null;

      // Check whether the precondition is met.
      if (isPreconditionMet(ifMatch, ifNoneMatch, lastModifiedAsEtag)) {
	// Yes: Write the contents to a temporary file and rename it.
	File tempfile = File.createTempFile("tmp_config", ".tmp",
	    m_cfgMgr.getCacheConfigDir());
	if (log.isDebug3()) log.debug3(DEBUG_HEADER + "tempfile = " + tempfile);

	Files.copy(inputStream, tempfile.toPath(),
	    StandardCopyOption.REPLACE_EXISTING);
	writeFromTempFile(tempfile, null);

	writeResult = new ConfigFileReadWriteResult(null, calcNewLastModified(),
	    true, null);
      } else {
	// No: The precondition is not met.
	writeResult =
	    new ConfigFileReadWriteResult(null, lastModified, false, null);
      }

      if (log.isDebug2())
	log.debug2(DEBUG_HEADER + "writeResult = " + writeResult);
      return writeResult;
    }
  }

  /**
   * Provides an indication of whether this configuration file meets the passed
   * precondition.
   * 
   * @param ifMatch
   *          A List<String> with an asterisk or values equivalent to the
   *          "If-Unmodified-Since" request header but with a granularity of 1
   *          ms.
   * @param ifNoneMatch
   *          A List<String> with an asterisk or values equivalent to the
   *          "If-Modified-Since" request header but with a granularity of 1 ms.
   * @param candidateTag
   *          A String with the entity tag of this configuration file to be used
   *          to check the precondition.
   * @return a boolean with <code>true</code> if the precondition is met,
   *         <code>false</code> otherwise.
   */
  public boolean isPreconditionMet(List<String> ifMatch,
      List<String> ifNoneMatch, String candidateTag) {
    final String DEBUG_HEADER = "isPreconditionMet(" + m_fileUrl + "): ";
    if (log.isDebug2()) {
      log.debug2(DEBUG_HEADER + "ifMatch = " + ifMatch);
      log.debug2(DEBUG_HEADER + "ifNoneMatch = " + ifNoneMatch);
      log.debug2(DEBUG_HEADER + "candidateTag = " + candidateTag);
    }

    // Check whether there are If-Match entity tags.
    if (ifMatch != null && !ifMatch.isEmpty()) {
      // Yes: Loop through all the If-Match entity tags.
      for (String etag : ifMatch) {
	if (log.isDebug3()) log.debug3(DEBUG_HEADER + "etag = " + etag);

	// Check whether it is an asterisk.
	if ("*".equals(etag)) {
	  // Yes: Check whether the file actually exists in the filesystem.
	  if (m_fileFile.exists()) {
	    // Yes: The precondition is met, nothing else to check.
	    if (log.isDebug2()) log.debug2(DEBUG_HEADER + "result = true");
	    return true;
	  }
	} else {
	  // No: Check whether this etag matches the current file.
	  if (candidateTag.equals(etag)) {
	    // Yes: The precondition is met, nothing else to check.
	    if (log.isDebug2()) log.debug2(DEBUG_HEADER + "result = true");
	    return true;
	  }
	}
      }

      // No match: The precondition is not met.
      if (log.isDebug2()) log.debug2(DEBUG_HEADER + "result = false");
      return false;
    // No: Check whether there are If-None-Match entity tags.
    } else if (ifNoneMatch != null && !ifNoneMatch.isEmpty()) {
      // Yes: Loop through all the If-None-Match entity tags.
      for (String etag : ifNoneMatch) {
	if (log.isDebug3()) log.debug3(DEBUG_HEADER + "etag = " + etag);

	// Check whether it is an asterisk.
	if ("*".equals(etag)) {
	  // Yes: Check whether the file actually exists in the filesystem.
	  if (m_fileFile.exists()) {
	    // Yes: The precondition is not met, nothing else to check.
	    if (log.isDebug2()) log.debug2(DEBUG_HEADER + "result = false");
	    return false;
	  }
	} else {
	  // No: Check whether this etag matches the current file.
	  if (candidateTag.equals(etag)) {
	    // Yes: The precondition is not met, nothing else to check.
	    if (log.isDebug2()) log.debug2(DEBUG_HEADER + "result = false");
	    return false;
	  }
	}
      }

      // No match: The precondition is met.
      if (log.isDebug2()) log.debug2(DEBUG_HEADER + "result = true");
      return true;
    }

    // There are no preconditions.
    if (log.isDebug2()) log.debug2(DEBUG_HEADER + "result = true");
    return true;
  }
}
