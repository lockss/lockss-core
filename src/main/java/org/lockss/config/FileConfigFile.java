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
import org.lockss.util.os.PlatformUtil;
import org.lockss.util.time.TimeBase;
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
   * 
   * @return a String with the new last-modified time.
   * @throws IOException
   *           if there are problems.
   */
  protected String calcNewLastModified() throws IOException {
    final String DEBUG_HEADER = "calcNewLastModified(" + m_fileUrl + "): ";
    if (log.isDebug2())
      log.debug2(DEBUG_HEADER + "m_lastModified = " + m_lastModified);
    if (m_lastModified == null) {
      // Handle a missing file first.
      InputStream in = null;

      try {
	// This will throw if the file does not exist.
	in = new FileInputStream(m_fileFile);
      } finally {
	if (in != null) {
	  in.close();
	}
      }

      // The file exists.
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
  public synchronized void writeFromTempFile(File tempfile,
					     Configuration config)
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
   * @param preconditions
   *          An HttpRequestPreconditions with the request preconditions to be
   *          met.
   * @return a ConfigFileReadWriteResult with the result of the operation.
   * @throws IOException
   *           if there are problems.
   */
  @Override
  public ConfigFileReadWriteResult conditionallyRead(HttpRequestPreconditions
      preconditions) throws IOException {
    final String DEBUG_HEADER = "conditionallyRead(" + m_fileUrl + "): ";
    if (log.isDebug2())
      log.debug2(DEBUG_HEADER + "preconditions = " + preconditions);

    synchronized(this) {
      // Get the last modification timestamp of the file.
      String lastModified = calcNewLastModified();
      if (log.isDebug3())
	log.debug3(DEBUG_HEADER + "lastModified = " + lastModified);

      // Use the last modification timestamp for the ETag.
      String lastModifiedAsEtag = "\"" + lastModified + "\"";
      if (log.isDebug3())
	log.error(DEBUG_HEADER + "lastModifiedAsEtag = " + lastModifiedAsEtag);

      // Get the file content type.
      MediaType contentType = MediaType.TEXT_PLAIN;

      if (m_fileType == ConfigFile.XML_FILE) {
	contentType = MediaType.TEXT_XML;
      }

      if (log.isDebug3())
	log.debug3(DEBUG_HEADER + "contentType = " + contentType);

      long contentLength = m_fileFile.length();
      if (log.isDebug3())
	log.debug3(DEBUG_HEADER + "contentLength = " + contentLength);

      ConfigFileReadWriteResult readResult = null;

      // Check whether the preconditions are met.
      if (arePreconditionsMet(preconditions, lastModified, lastModifiedAsEtag))
      {
	// Yes: The preconditions are met.
	readResult = new ConfigFileReadWriteResult(getInputStream(),
	    lastModified, lastModifiedAsEtag, true, contentType, contentLength);
      } else {
	// No: The precondition are not met.
	readResult = new ConfigFileReadWriteResult(null, lastModified,
	    lastModifiedAsEtag, false, contentType, contentLength);
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
   * @param preconditions
   *          An HttpRequestPreconditions with the request preconditions to be
   *          met.
   * @param inputStream
   *          An InputStream to the content to be written to this configuration
   *          file.
   * @return a ConfigFileReadWriteResult with the result of the operation.
   * @throws IOException
   *           if there are problems.
   */
  @Override
  public ConfigFileReadWriteResult conditionallyWrite(HttpRequestPreconditions
      preconditions, InputStream inputStream) throws IOException {
    final String DEBUG_HEADER = "conditionallyWrite(" + m_fileUrl + "): ";
    if (log.isDebug2()) {
      log.debug2(DEBUG_HEADER + "preconditions = " + preconditions);
      log.debug2(DEBUG_HEADER + "inputStream = " + inputStream);
    }

    synchronized(this) {
      // Get the last modification timestamp of the file.
      String lastModified = "0";

      try {
	lastModified = calcNewLastModified();
      } catch (FileNotFoundException fnfe) {
	// Ignore, as we are writing.
      }

      // Use the last modification timestamp for the ETag.
      String lastModifiedAsEtag = "\"" + lastModified + "\"";
      if (log.isDebug3())
	log.debug3(DEBUG_HEADER + "lastModifiedAsEtag = " + lastModifiedAsEtag);

      ConfigFileReadWriteResult writeResult = null;

      // Check whether the preconditions are met.
      if (arePreconditionsMet(preconditions, lastModified, lastModifiedAsEtag))
      {
	// Yes: Write the contents to a temporary file and rename it.
	File tempfile = File.createTempFile("tmp_config", ".tmp",
	    m_cfgMgr.getCacheConfigDir());
	if (log.isDebug3()) log.debug3(DEBUG_HEADER + "tempfile = " + tempfile);

	Files.copy(inputStream, tempfile.toPath(),
	    StandardCopyOption.REPLACE_EXISTING);

	long contentLength = tempfile.length();
	if (log.isDebug3())
	  log.debug3(DEBUG_HEADER + "contentLength = " + contentLength);

	writeFromTempFile(tempfile, null);

	// Get the new file last modification data to be returned.
	lastModified = calcNewLastModified();
	lastModifiedAsEtag = "\"" + lastModified + "\"";
	if (log.isDebug3()) log.debug3(DEBUG_HEADER + "lastModifiedAsEtag = "
	    + lastModifiedAsEtag);

	writeResult = new ConfigFileReadWriteResult(null, lastModified,
	    lastModifiedAsEtag, true, null, contentLength);
      } else {
	// No: The preconditions are not met.
	writeResult = new ConfigFileReadWriteResult(null, lastModified,
	    lastModifiedAsEtag, false, null, 0);
      }

      if (log.isDebug2())
	log.debug2(DEBUG_HEADER + "writeResult = " + writeResult);
      return writeResult;
    }
  }

  /**
   * Provides an indication of whether this configuration file meets the passed
   * preconditions.
   * 
   * @param preconditions
   *          An HttpRequestPreconditions with the request preconditions to be
   *          met.
   * @param modificationTime
   *          A String with the modification time of this configuration file to
   *          be used to check the preconditions.
   * @param candidateTag
   *          A String with the entity tag of this configuration file to be used
   *          to check the preconditions.
   * @return a boolean with <code>true</code> if all the preconditions are met,
   *         <code>false</code> otherwise.
   */
  public boolean arePreconditionsMet(HttpRequestPreconditions preconditions,
      String modificationTime, String candidateTag) {
    final String DEBUG_HEADER = "arePreconditionsMet(" + m_fileUrl + "): ";
    if (log.isDebug2()) {
      log.debug2(DEBUG_HEADER + "preconditions = " + preconditions);
      log.debug2(DEBUG_HEADER + "modificationTime = " + modificationTime);
      log.debug2(DEBUG_HEADER + "candidateTag = " + candidateTag);
    }

    // Get the individual preconditions.
    List<String> ifMatch = null;
    String ifModifiedSince = null;
    List<String> ifNoneMatch = null;
    String ifUnmodifiedSince = null;

    if (preconditions != null) {
	ifMatch = preconditions.getIfMatch();
	ifModifiedSince = preconditions.getIfModifiedSince();
	ifNoneMatch = preconditions.getIfNoneMatch();
	ifUnmodifiedSince = preconditions.getIfUnmodifiedSince();
    }

    // Check whether there are If-Match entity tags.
    if (ifMatch != null && !ifMatch.isEmpty()) {
      // Yes: Assume no entity tag match.
      boolean etagMatch = false;

      // Yes: Loop through all the If-Match entity tags.
      for (String etag : ifMatch) {
	if (log.isDebug3()) log.debug3(DEBUG_HEADER + "ifMatchEtag = " + etag);

	// Check whether it is an asterisk.
	if ("*".equals(etag)) {
	  // Yes: Check whether the file actually exists in the filesystem.
	  if (m_fileFile.exists()) {
	    // Yes: The precondition is met, nothing else to check.
	    if (log.isDebug2()) log.debug2(DEBUG_HEADER + "etagMatch = true");
	    etagMatch = true;
	    break;
	  }
	} else {
	  // No: Check whether this etag matches the current file.
	  if (candidateTag.equals(etag)) {
	    // Yes: The precondition is met, nothing else to check.
	    if (log.isDebug2()) log.debug2(DEBUG_HEADER + "etagMatch = true");
	    etagMatch = true;
	    break;
	  }
	}
      }

      // Check whether there was no entity tag match.
      if (!etagMatch) {
	// Yes: The precondition is not met.
	if (log.isDebug2()) log.debug2(DEBUG_HEADER + "result = false");
	return false;
      }

      // No: Check whether there is a mismatch between any "If-Unmodified-Since"
      // timestamp and the modification time.
      if (!StringUtil.isNullString(ifUnmodifiedSince)
	  && !ifUnmodifiedSince.equals(modificationTime)) {
	// Yes: The precondition is not met.
	if (log.isDebug2()) log.debug2(DEBUG_HEADER + "result = false");
	return false;
      }

      // No: The preconditions are all met.
      if (log.isDebug2()) log.debug2(DEBUG_HEADER + "result = true");
      return true;
    }

    // Check whether there is an If-Unmodified-Since condition.
    if (!StringUtil.isNullString(ifUnmodifiedSince)) {
      // Yes: Get its match with the modification time.
      boolean result = ifUnmodifiedSince.equals(modificationTime);
      if (log.isDebug2()) log.debug2(DEBUG_HEADER + "result = " + result);
      return result;
    }

    // Check whether there are If-None-Match entity tags.
    if (ifNoneMatch != null && !ifNoneMatch.isEmpty()) {
      // Yes: Assume no entity tag match.
      boolean etagNoMatch = true;

      // Yes: Loop through all the If-None-Match entity tags.
      for (String etag : ifNoneMatch) {
	if (log.isDebug3())
	  log.debug3(DEBUG_HEADER + "ifNoneMatchEtag = " + etag);

	// Check whether it is an asterisk.
	if ("*".equals(etag)) {
	  // Yes: Check whether the file actually exists in the filesystem.
	  if (m_fileFile.exists()) {
	    // Yes: The precondition is not met, nothing else to check.
	    if (log.isDebug2())
	      log.debug2(DEBUG_HEADER + "etagNoMatch = false");
	    etagNoMatch = false;
	    break;
	  }
	} else {
	  // No: Check whether this etag matches the current file.
	  if (candidateTag.equals(etag)) {
	    // Yes: The precondition is not met, nothing else to check.
	    if (log.isDebug2())
	      log.debug2(DEBUG_HEADER + "etagNoMatch = false");
	    etagNoMatch = false;
	    break;
	  }
	}
      }

      // Check whether there was an entity tag match.
      if (!etagNoMatch) {
	// Yes: The precondition is not met.
	if (log.isDebug2()) log.debug2(DEBUG_HEADER + "result = false");
	return false;
      }

      // No: Check whether there is a mismatch with the modification time.
      if (!StringUtil.isNullString(ifModifiedSince)
	  && ifModifiedSince.equals(modificationTime)) {
	// Yes: The precondition is not met.
	if (log.isDebug2()) log.debug2(DEBUG_HEADER + "result = false");
	return false;
      }

      // No: The preconditions are all met.
      if (log.isDebug2()) log.debug2(DEBUG_HEADER + "result = true");
      return true;
    }

    // Check whether there is an If-Modified-Since condition.
    if (!StringUtil.isNullString(ifModifiedSince)) {
      // Yes: Get its match with the modification time.
      boolean result = !ifModifiedSince.equals(modificationTime);
      if (log.isDebug2()) log.debug2(DEBUG_HEADER + "result = " + result);
      return result;
    }

    // There are no preconditions.
    if (log.isDebug2()) log.debug2(DEBUG_HEADER + "result = true");
    return true;
  }

  /**
   * Used for logging and testing and debugging.
   */
  @Override
  public String toString() {
    return "[FileConfigFile: m_fileFile=" + m_fileFile + ", " + super.toString()
    + "]";
  }
}
