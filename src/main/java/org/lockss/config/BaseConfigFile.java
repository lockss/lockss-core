/*

Copyright (c) 2000-2018 Board of Trustees of Leland Stanford Jr. University,
all rights reserved.

Redistribution and use in source and binary forms, with or without modification,
are permitted provided that the following conditions are met:

1. Redistributions of source code must retain the above copyright notice, this
list of conditions and the following disclaimer.

2. Redistributions in binary form must reproduce the above copyright notice,
this list of conditions and the following disclaimer in the documentation and/or
other materials provided with the distribution.

3. Neither the name of the copyright holder nor the names of its contributors
may be used to endorse or promote products derived from this software without
specific prior written permission.

THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND
ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE FOR
ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
(INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON
ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
(INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

*/

package org.lockss.config;

import java.io.*;
import java.net.*;
import java.util.*;
import org.apache.commons.lang3.StringUtils;
import org.lockss.util.*;
import org.lockss.util.time.TimeBase;
import org.lockss.util.urlconn.*;

/**
 * Common functionality for a config file loadable from a URL or filename,
 * and parseable as either XML or props.
 */
public abstract class BaseConfigFile implements ConfigFile {
  
  // Shared with subclasses
  protected static final Logger log = Logger.getLogger();

  protected ConfigManager m_cfgMgr;
  protected int m_fileType;
  protected String m_lastModified;
  // FileConfigFile assumes the url doesn't change
  protected String m_fileUrl;
  protected String m_loadedUrl;
  protected String m_loadError = "Not yet loaded";
  protected IOException m_IOException;
  protected volatile long m_lastAttempt;
  protected boolean m_needsReload = true;
  protected boolean reloadUnconditionally = false;
  protected ConfigurationPropTreeImpl m_config;
  protected int m_generation = 0;
  protected Map<String, Object> m_props;
  protected ConfigManager.KeyPredicate keyPred;
  protected boolean m_isPlatformFile;

  /**
   * Create a ConfigFile for the URL
   */
  public BaseConfigFile(String url, ConfigManager cfgMgr) {
    if (StringUtil.endsWithIgnoreCase(url, ".xml") ||
	StringUtil.endsWithIgnoreCase(url, ".xml.gz") ||
	StringUtil.endsWithIgnoreCase(url, ".xml.opt")) {
      m_fileType = ConfigFile.XML_FILE;
    } else {
      m_fileType = ConfigFile.PROPERTIES_FILE;
    }
    m_fileUrl = url;
    m_cfgMgr = cfgMgr;
  }

  void setConfigManager(ConfigManager configMgr) {
    m_cfgMgr = configMgr;
  }

  @Override
  public String getFileUrl() {
    return m_fileUrl;
  }

  @Override
  public String getLoadedUrl() {
    return m_loadedUrl != null ? m_loadedUrl : m_fileUrl;
  }

  @Override
  public String resolveConfigUrl(String relUrl) {
    final String DEBUG_HEADER = "resolveConfigUrl(): ";
    String base = getFileUrl();
    if (log.isDebug2()) {
      log.debug2(DEBUG_HEADER + "base = " + base);
      log.debug2(DEBUG_HEADER + "relUrl = " + relUrl);
    }
    try {
      return UrlUtil.resolveUri(base, relUrl);
    } catch (MalformedURLException e) {
      log.error("Can't resolve, base: " + base + ", rel: " + relUrl, e);
      return relUrl;
    }
  }

  /** Return true if this file might contain platform values that are
   * needed in order to properly parse other config files.
   */
  public boolean isPlatformFile() {
    if (m_isPlatformFile) {
      return true;
    }
    if (m_cfgMgr != null) {
      return m_cfgMgr.isBootstrapPropsUrl(m_fileUrl);
    }
    return false;
  }

  public void setPlatformFile(boolean val) {
    m_isPlatformFile = val;
  }

  public int getFileType() {
    return m_fileType;
  }

  public String getLastModified() {
    return m_lastModified;
  }

  public long getLastAttemptTime() {
    return m_lastAttempt;
  }

  public Generation getGeneration() throws IOException {
    ensureLoaded();
    synchronized (this) {
      return new Generation(this, m_config, m_generation);
    }
  }

  public String getLoadErrorMessage() {
    return m_loadError;
  }

  public boolean isLoaded() {
    return m_loadError == null;
  }

  private void ensureLoaded() throws IOException {
    if (m_needsReload || isCheckEachTime()) {
      reload();
    }
  }

  /** Return true if the file should be checked for modification each time
   * getConfiguration() is called.  If false, the file will only be checked
   * on the first call, and after calls to setNeedsReload().  Subclasses
   * use this to modify the default behavior.
   */
  protected boolean isCheckEachTime() {
    return true;
  }

  /**
   * Instruct the ConfigFile to check for modifications the next time it's
   * accessed
   */
  public void setNeedsReload() {
    m_needsReload = true;
  }

  public void setConnectionPool(LockssUrlConnectionPool connPool) {
  }

  public void setProperty(String key, Object val) {
    if (m_props == null) {
      m_props = new HashMap<String, Object>();
    }
    m_props.put(key, val);
  }

  /** Return the Configuration object built from this file
   */
  public Configuration getConfiguration() throws IOException {
    ensureLoaded();
    return m_config;
  }

  /**
   * Reload the contents if changed.
   */
  protected void reload() throws IOException {
    final String DEBUG_HEADER = "reload(" + m_fileUrl + "): ";
    m_lastAttempt = TimeBase.nowMs();
    if (log.isDebug3())
      log.debug3(DEBUG_HEADER + "m_lastAttempt = " + m_lastAttempt);
    try {
      InputStream in = getInputStreamIfModifiedNoCache();
      if (in != null) {
	try {
	  setConfigFrom(in);
	  loadFinished();
	} finally {
	  IOUtil.safeClose(in);
	}
      }
    } catch (FileNotFoundException ex) {
      log.debug2("File not found: " + m_fileUrl);
      m_IOException = ex;
      m_loadError = ex.toString();
      throw ex;
    } catch (IOException ex) {
      log.warning("Exception loading " + m_fileUrl, ex);
      m_IOException = ex;
      if (m_loadError == null ||
	  !StringUtil.equalStrings(ex.getMessage(), m_loadError)) {
	// Some subs set m_loadError to exception message.  Don't overwrite
	// those with message that includes java exception class
	m_loadError = ex.toString();
      }
      throw ex;
    }
  }

  protected void setConfigFrom(InputStream in) throws IOException {
    final String DEBUG_HEADER = "setConfigFrom(" + m_fileUrl + "): ";
    ConfigurationPropTreeImpl newConfig = new ConfigurationPropTreeImpl();
    try {
      Tdb tdb = new Tdb();
      PropertyTree propTree = newConfig.getPropertyTree();
      if (log.isDebug3())
	log.debug3(DEBUG_HEADER + "m_fileType = " + m_fileType);

      // Load the configuration
      if (m_fileType == XML_FILE) {
	XmlPropertyLoader.load(propTree, tdb, in);
      } else {
	propTree.load(in);
	extractTdb(propTree, tdb);
      }

      if (log.isDebug3()) {
	log.debug3(DEBUG_HEADER
	    + Configuration.loggableConfiguration(newConfig, "newConfig"));
	log.debug3(DEBUG_HEADER + "tdb.isEmpty() = " + tdb.isEmpty());
      }

      if (!tdb.isEmpty()) {
        newConfig.setTdb(tdb);
      }

      filterConfig(newConfig);
      if (log.isDebug3()) log.debug3(DEBUG_HEADER
	  + Configuration.loggableConfiguration(newConfig, "newConfig"));
      
      // update stored configuration atomically
      newConfig.seal();
      m_config = newConfig;
      m_loadError = null;
      m_IOException = null;
      m_lastModified = calcNewLastModified();
      if (log.isDebug3())
	log.debug3(DEBUG_HEADER + "m_lastModified = " + m_lastModified);
      m_generation++;
      m_needsReload = false;
    } catch (IOException ex) {
      throw ex;
    } catch (Exception ex) {
      log.debug(getFileUrl() +
		": Unexpected non-IO error loading configuration", ex);
      throw new IOException(ex.toString());
    }
  }

  /**
   * Extract title database entries from the PropertyTree and add them to Tdb.
   * 
   * @param propTree the property tree
   * @param tdb the title database
   * @return <code>true</code> if title database entries were extracted
   */
  protected boolean extractTdb(PropertyTree propTree, Tdb tdb) {
    PropertyTree tdbTree = propTree.getTree(ConfigManager.PARAM_TITLE_DB);
    if (tdbTree.isEmpty()) {
      return false;
    }
    
    // remove title database keys from propTree
    // (why doesn't PropertyTree encapsulate this?)
    for (Object key : new HashSet(propTree.keySet())) {
      if (((String)key).startsWith(ConfigManager.PREFIX_TITLE_DB)) {
        propTree.remove(key);
      }
    }

    // process title database
    Enumeration elements = tdbTree.getNodes();
    while (elements.hasMoreElements()) {
      String element = (String)elements.nextElement();
      PropertyTree tdbProps = tdbTree.getTree(element);
      try {
        tdb.addTdbAuFromProperties(tdbProps);
      } catch (Throwable ex) {
        log.error("Error processing TdbAu entry " + element + ": " + ex.getMessage());
      }
    }
    
    return true;
  }
  
  public void setKeyPredicate(ConfigManager.KeyPredicate pred) {
    keyPred = pred;
  }

  protected void filterConfig(Configuration config) throws IOException {
    if (keyPred != null) {
      List<String> delKeys = null;
      for (String key : config.keySet()) {
	if (!keyPred.evaluate(key)) {
	  String msg = "Illegal config key: " + key + " = "
	    + StringUtils.abbreviate(config.get(key), 50) + " in " + m_fileUrl;
	  if (keyPred.failOnIllegalKey()) {
	    log.error(msg);
	    throw new IOException(msg);
	  } else {
	    log.warning(msg);
	    if (delKeys == null) {
	      delKeys = new ArrayList<String>();
	    }
	    delKeys.add(key);
	  }
	}
      }
      if (delKeys != null) {
	for (String key : delKeys) {
	  config.remove(key);
	}
      }	
    }
  }

  /**
   * Return an InputStream on the contents of the file, or null if the file
   * hasn't changed.
   */
  protected InputStream getInputStreamIfModifiedNoCache()
      throws IOException {
    return getInputStreamIfModified();
  }

  /**
   * Return an InputStream on the contents of the file, or null if the file
   * hasn't changed.
   */
  protected abstract InputStream getInputStreamIfModified() throws IOException;

  /**
   * Called after file has been completely read.
   */
  protected void loadFinished() {
  }

  /**
   * Return the new last-modified time
   * 
   * @return a String with the new last-modified time.
   * @throws IOException
   *           if there are problems.
   */
  protected abstract String calcNewLastModified() throws IOException;

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
    throw new UnsupportedOperationException();
  }

  /**
   * Used for logging and testing and debugging.
   */
  public String toString() {
    return "{url=" + m_fileUrl + "; isLoaded=" + (m_config != null) +
      "; lastModified=" + m_lastModified + "; isPlatformFile()=" +
      isPlatformFile() + "}";
  }
}
