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

import java.util.*;
import org.lockss.util.*;

/**
 * A memory cache of files used to populate our Configuration.
 */
public class ConfigCache {
  private static Logger log = Logger.getLogger();

  private ConfigManager configMgr;
  private Map<String, ConfigFile> m_configMap =
      new HashMap<String, ConfigFile>();

  public ConfigCache(ConfigManager configMgr) {
    this.configMgr = configMgr;
  }

  /**
   * Return the existing ConfigFile for the url or file, if any, else null
   * @return the ConfigFile or null if no such
   */
  public ConfigFile get(String url) {
    return m_configMap.get(url);
  }

  /**
   * Find or create a ConfigFile for the url
   * @return an exisiting or new ConfigFile of the type appropriate for the
   * URL
   */
  public synchronized ConfigFile find(String url) {
    final String DEBUG_HEADER = "find(): ";
    if (log.isDebug2()) log.debug2(DEBUG_HEADER + "url = " + url);
    ConfigFile cf = get(url);
    if (log.isDebug3()) log.debug3(DEBUG_HEADER + "cf = " + cf);
    if (cf == null) {
      // doesn't yet exist in the cache, add it.
      log.debug2("Adding " + url);
      BaseConfigFile bcf;
      // Check whether it is a dynamically created configuration file.
      if (DynamicConfigFile.isDynamicConfigUrl(url)) {
	if (log.isDebug3()) log.debug3(DEBUG_HEADER + "Is DynamicConfigFile.");
	if (configMgr == null) {
	  throw new IllegalStateException("Can't create DynamicConfigFile without a ConfigManager: " + url);
	}
	bcf = configMgr.newDynamicConfigFile(url);
	// Check whether it is a resource configuration file.
      } else if (ResourceConfigFile.isResourceConfigUrl(url, configMgr)) {
	// Yes.
	if (log.isDebug3()) log.debug3(DEBUG_HEADER + "Is ResourceConfigFile.");
	bcf = new ResourceConfigFile(url, configMgr);
	// No: Check whether it is a REST service configuration file.
      } else if (RestConfigFile.isRestConfigUrl(url, configMgr)) {
	// Yes.
	if (log.isDebug3()) log.debug3(DEBUG_HEADER + "Is RestConfigFile.");
	bcf = new RestConfigFile(url, configMgr);
	// No: Check whether it is an HTTP configuration file.
      } else if (UrlUtil.isHttpOrHttpsUrl(url)) {
	// Yes.
	if (log.isDebug3()) log.debug3(DEBUG_HEADER + "Is HTTPConfigFile.");
	bcf = new HTTPConfigFile(url, configMgr);
	// No: Check whether it is a JAR configuration file.
      } else if (UrlUtil.isJarUrl(url)) {
	// Yes.
	if (log.isDebug3()) log.debug3(DEBUG_HEADER + "Is JarConfigFile.");
	bcf = new JarConfigFile(url, configMgr);
      } else {
	// No: It is a local filesystem configuration file.
	if (log.isDebug3()) log.debug3(DEBUG_HEADER + "Is FileConfigFile.");
	bcf = new FileConfigFile(url, configMgr);
      }
      m_configMap.put(url, bcf);
      cf = bcf;
    }
    if (log.isDebug2()) log.debug2(DEBUG_HEADER + "cf = " + cf);
    return cf;
  }

//   public synchronized void remove(String url) {
//     m_configMap.remove(url);
//   }

  int size() {
    return m_configMap.size();
  }
}
