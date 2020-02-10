/*

Copyright (c) 2020 Board of Trustees of Leland Stanford Jr. University,
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
package org.lockss.importer;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.security.Provider;
import java.security.Security;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import org.lockss.app.LockssDaemon;
import org.lockss.config.ConfigManager;
import org.lockss.config.Configuration;
import org.lockss.daemon.Crawler;
import org.lockss.hasher.HashResult;
import org.lockss.log.L4JLogger;
import org.lockss.plugin.ArchivalUnit;
import org.lockss.plugin.AuUtil;
import org.lockss.plugin.CachedUrl;
import org.lockss.plugin.Plugin;
import org.lockss.plugin.PluginManager;
import org.lockss.plugin.UrlCacher;
import org.lockss.plugin.UrlData;
import org.lockss.util.ByteArray;
import org.lockss.util.CIProperties;
import org.lockss.util.IOUtil;
import org.lockss.util.PropUtil;
import org.lockss.util.PropertiesUtil;
import org.lockss.util.StreamUtil;
import org.lockss.util.StringUtil;
import org.lockss.util.time.TimeBase;

/**
 * Imports a file into an Archival Unit.
 */
public class Importer {
  private final static L4JLogger log = L4JLogger.getLogger();

  // The key of the property used to request a checksum check of imported
  // content.
  private static final String CONTENT_CHECKSUM_KEY = "Checksum";

  // The key of the plugin used to import files.
  private static final String PLUGIN_KEY = "org|lockss|plugin|ImportPlugin";

  // The message digest algorithms supported by this platform.
  private List<String> supportedMessageDigestAlgorithms = null;

  // The keys of properties to be loaded in the archival unit configuration.
  private List<String> auConfigKeys =
      Arrays.asList(PluginManager.AU_PARAM_DISPLAY_NAME);

  // The plugin manager.
  private PluginManager pluginMgr =
      LockssDaemon.getLockssDaemon().getPluginManager();

  /**
   * Imports a file.
   * 
   * @param input             An InputStream with the content to be imported.
   * @param targetBaseUrlPath A String with the base URL path of the target AU.
   * @param targetUrl         A String with the target AU URL.
   * @param userProperties    A {@code List<String>} with the user-specified
   *                          properties.
   * @throws IOException              if there are problems storing the content.
   * @throws NoSuchAlgorithmException if there are problems with the hashing
   *                                  algorithm.
   */
  public void importFile(InputStream input, String targetBaseUrlPath,
      String targetUrl, List<String> userProperties)
	  throws IOException, NoSuchAlgorithmException {
    log.debug2("targetBaseUrlPath = {}", targetBaseUrlPath);
    log.debug2("targetUrl = {}", targetUrl);
    log.debug2("userProperties = {}", userProperties);

    // Validate the presence of the content to be imported.
    if (input == null) {
      throw new IllegalArgumentException(
	  "Null input stream to content to be imported");
    }

    // Validate the specified Archival Unit base URL path.
    if (targetBaseUrlPath == null) {
      throw new IllegalArgumentException("Null base_url path");
    } else if (targetBaseUrlPath.trim().length() == 0) {
      throw new IllegalArgumentException("Empty target base_url path '"
	  + targetBaseUrlPath + "'");
    }

    // Validate the specified Archival Unit URL.
    if (targetUrl == null) {
      throw new IllegalArgumentException("Null url");
    } else if (targetUrl.trim().length() == 0) {
      throw new IllegalArgumentException("Empty target url '" + targetUrl
	  + "'");
    }

    // Get the user properties map.
    Map<String, String> properties =
	PropertiesUtil.convertListToMap(userProperties);
    log.trace("properties = {}", properties);

    // The temporary file, if needed.
    File tmpFile = null;

    try {
      // Get the value of a checksum request, if any.
      String checksumRequest = properties.get(CONTENT_CHECKSUM_KEY);
      log.trace("checksumRequest = {}", checksumRequest);

      // Check whether a checksum request is made.
      if (checksumRequest != null) {
	// Yes: Get the hash.
	HashResult hashResult = validateChecksumRequest(checksumRequest);

	// Save a copy of the content in a temporary location while checking the
	// checksum.
	tmpFile = checkContent(input, hashResult);

	// Clean up.
	if (input != null) {
	  try {
	    input.close();
	  } catch (IOException ioe) {
	    log.warn("Exception caught closing input stream", ioe);
	  }
	}

	// Continue using the copy just made.
	input = new FileInputStream(tmpFile);
      }

      // Get the identifier of the archival unit where to store the imported
      // file.
      String auId = makeAuId(targetBaseUrlPath);
      log.trace("auId = {}", auId);

      // Set the import timestamp.
      CIProperties headers = new CIProperties();
      headers.put(CachedUrl.PROPERTY_FETCH_TIME,
	  Long.toString(TimeBase.nowMs()));
      log.trace("headers = {}", headers);

      // Get the archival unit, if it exists.
      ArchivalUnit au = pluginMgr.getAuFromId(auId);
      log.trace("au = {}", au);

      // Check whether the archival unit does not exist.
      if (au == null) {
	//  Yes: Create it.
	au = createAu(auId, pluginMgr, properties, headers);
	log.trace("au = {}", au);
      } else {
	// No: Check whether the AU has been crawled already.
	if (AuUtil.getAuState(au).hasCrawled()) {
	  // Yes: Report the problem.
	  throw new IllegalStateException(
	      "Target Archival Unit has crawled already");
	}

	processProperties(properties, null, headers);
      }

      log.trace("headers = {}", headers);

      // TODO: Mark the AU as non-crawlable and non-repairable.
      // TL email 11/17/2015 1:24 PM:
      // We need to add a
      // real property to the AU to precent crawling and repair from publisher,
      // and you'll need to set that when you create an AU for import.  I'm still
      // contemplating where to put that.

      // TODO: Validate targetUrl?
      // TL email 11/16/2015 1:46 PM:
      // As long as we're using URL and not URI we'll still be stuck accepting
      // only legal URLs that have a URLStreamHandlerFactory.  The WS import()
      // method should probably check legality before it accepts the file, rather
      // than let bogus URLs cause errors later.
      /*String targetUrl = importParams.getTargetUrl();
    	if (log.isDebug3()) log.debug3(DEBUG_HEADER + "targetUrl = " + targetUrl);
    	URL url = new URL(targetUrl);
    	String protocol = url.getProtocol();
    	if (log.isDebug3()) log.debug3(DEBUG_HEADER + "protocol = " + protocol);

    	URLStreamHandler ush = URL.getURLStreamHandler(protocol);*/

      // Create the URL cacher.
      UrlCacher uc = au.makeUrlCacher(new UrlData(input, headers, targetUrl));
      log.trace("uc = {}", uc);
      log.trace("uc.getClass() = {}", uc.getClass());

      // Store the file.
      uc.storeContent();

      // Alert the NodeManager that the content crawl has finished.
      AuUtil.getAuState(au).newCrawlFinished(Crawler.STATUS_SUCCESSFUL, null);

      AuUtil.getAuContentSize(au, false);
      AuUtil.getAuDiskUsage(au, false);
    } finally {
      if (input != null) {
	try {
	  input.close();
	} catch (IOException ioe) {
	  log.warn("Exception caught closing input stream", ioe);
	}
      }

      if (tmpFile != null) {
	if (!tmpFile.delete()) {
	  tmpFile.deleteOnExit();
	}
      }
    }
  }

  /**
   * Processes and validates a checksum request.
   * 
   * @param checksumRequest A String with the checksum request.
   * @return a HashResult containing the checksum request.
   */
  private HashResult validateChecksumRequest(String checksumRequest) {
    HashResult hashResult = HashResult.make(checksumRequest);
    String algorithm = hashResult.getAlgorithm();

    // Check for an invalid algorithm.
    if (StringUtil.isNullString(algorithm)) {
      throw new IllegalArgumentException("Invalid checksum algorithm '"
	  + algorithm + "'");
    }

    // Check for an unsupported algorithm.
    if (!getSupportedMessageDigestAlgorithms().contains(algorithm)) {
      throw new IllegalArgumentException("Unsupported checksum algorithm '"
	  + algorithm + "'");
    }

    return hashResult;
  }

  /**
   * Provides a list of the names of the supported message digest algorithms.
   * 
   * @return a List<String> with the list of the supported message digest
   *         algorithms names.
   */
  public List<String> getSupportedMessageDigestAlgorithms() {
    if (supportedMessageDigestAlgorithms == null) {
      supportedMessageDigestAlgorithms = new ArrayList<String>();

      for (Provider provider : Security.getProviders()) {
	log.trace("provider = {}", provider);

        for (Provider.Service service : provider.getServices()) {
          log.trace("service = {}", service);

          if ("MessageDigest".equals(service.getType())) {
            supportedMessageDigestAlgorithms.add(service.getAlgorithm());
            log.trace("algorithm = {}", service.getAlgorithm());

            String displayService = service.toString();
            int beginIndex =
        	displayService.indexOf("aliases: [") + "aliases: [".length();

            if (beginIndex >= "aliases: [".length()) {
              int endIndex = displayService.indexOf("]", beginIndex);
              String aliases = displayService.substring(beginIndex, endIndex);

              for (String alias : StringUtil.breakAt(aliases, ",")) {
                supportedMessageDigestAlgorithms.add(alias.trim());
                log.trace("alias = {}", alias.trim());
              }
            }
          }
        }
      }

      log.trace("supportedMessageDigestAlgorithms = {}",
	  supportedMessageDigestAlgorithms);
    }

    return supportedMessageDigestAlgorithms;
  }

  /**
   * Checks the checksum of content.
   * 
   * @param input      An InputStream to the content.
   * @param hashResult A HashResult against which to check the content.
   * @return a File with a copy of the checked content.
   * @throws IOException              if there are problems copying the content.
   * @throws NoSuchAlgorithmException if the specified algorithm does not exist.
   */
  private File checkContent(InputStream input, HashResult hashResult)
      throws IOException, NoSuchAlgorithmException {
    File tmpFile = File.createTempFile("imported", "", null);

    MessageDigest md = MessageDigest.getInstance(hashResult.getAlgorithm());

    if (md == null) {
      throw new RuntimeException("No digest could be obtained");
    }

    FileOutputStream fos = null;

    try {
      fos = new FileOutputStream(tmpFile);
    } finally {
      IOUtil.safeClose(fos);
    }

    try {
      StreamUtil.copy(input, fos, -1, null, true, md);
    } finally {
      IOUtil.safeClose(fos);
    }

    if (!hashResult.equalsBytes(md.digest())) {
      throw new IllegalArgumentException("Checksum error: Expected = '"
	  + ByteArray.toHexString(hashResult.getBytes()) + "', Found = '"
	  + ByteArray.toHexString(md.digest()) + "'");
    }

    return tmpFile;
  }

  /**
   * Creates the import Archival Unit AUID for a base URL path.
   * 
   * @param baseUrlPath A String with the base URL path.
   * @return a String with the import Archival Unit AUID.
   */
  static String makeAuId(String baseUrlPath) {
    String baseUrlHost = "import%2Elockss%2Eorg";
    return PLUGIN_KEY + "&base_url~http%3A%2F%2F" + baseUrlHost + "%2F"
	+ baseUrlPath;
  }

  /**
   * Creates the import Archival Unit.
   * 
   * @param auId       A String with the import Archival Unit AUID.
   * @param pluginMgr  A PluginManager with the plugin manager.
   * @param properties A Map<String, String> with user-specified properties.
   * @param headers    A CIProperties with the headers.
   * @return an ArchivalUnit with the Archival Unit created.
   */
  private ArchivalUnit createAu(String auId, PluginManager pluginMgr,
      Map<String, String> properties, CIProperties headers) {
    log.debug2("auId = {}", auId);
    log.debug2("properties = {}", properties);
    log.debug2("headers = {}", headers);

    ArchivalUnit au = null;

    // Get the plugin identifier.
    String pluginId = null;

    try {
      pluginId = PluginManager.pluginIdFromAuId(auId);
      log.trace("pluginId = {}", pluginId);
    } catch (Exception e) {
      throw new RuntimeException("Error getting the plugin identifier: ", e);
    }

    Plugin plugin = null;

    if (PLUGIN_KEY.equals(pluginId)) {
      // Get the plugin.
      plugin = pluginMgr.getImportPlugin();

      if (plugin == null) {
	throw new RuntimeException("Invalid pluginId '" + pluginId + "'");
      }

      // Now that the Import plugin has been loaded, get the archival unit
      // again, if it exists.
      au = pluginMgr.getAuFromId(auId);
      log.trace("au = {}", au);

      // Check whether the archival unit now exists.
      if (au != null) {
	processProperties(properties, null, headers);
      }
    } else {
      // Get the plugin.
      plugin = pluginMgr.getPlugin(pluginId);

      if (plugin == null) {
	boolean pluginLoaded = pluginMgr.ensurePluginLoaded(pluginId);
	log.trace("pluginLoaded = {}", pluginLoaded);

	if (pluginLoaded) {
	  plugin = pluginMgr.getPlugin(pluginId);
	}

	if (plugin == null) {
	  throw new RuntimeException("Invalid pluginId '" + pluginId + "'");
	}
      }
    }
  
    // Check whether the archival unit still does not exist.
    if (au == null) {
      // Yes: Get the archival unit key.
      String auKey = null;

      try {
	auKey = PluginManager.auKeyFromAuId(auId);
	log.trace("auKey = {}", auKey);
      } catch (IllegalArgumentException iae) {
	throw new RuntimeException("Error getting AuKey: ", iae);
      }

      // Get the properties encoded in the archival unit key.
      Properties props = null;

      try {
	props = PropUtil.canonicalEncodedStringToProps(auKey);
	log.trace("props = " + props);
      } catch (IllegalArgumentException iae) {
	throw new RuntimeException("Invalid AuKey: ", iae);
      }

      // Initialize the archival unit configuration.
      Configuration auConfig = null;

      try {
	auConfig = ConfigManager.fromPropertiesUnsealed(props);
	log.trace("auConfig = {}", auConfig);
      } catch (RuntimeException re) {
	throw new RuntimeException("Invalid AuKey properties: ", re);
      }

      processProperties(properties, auConfig, headers);

      // Add the archival unit.
      try {
	au = pluginMgr.createAndSaveAuConfiguration(plugin, auConfig);
	log.trace("au = {}", au);
      } catch (Exception e) {
	throw new RuntimeException("Error creating AU: ", e);
      }
    }

    log.debug2("au = {}", au);
    return au;
  }

  /**
   * Loads the user-specified properties into the archival unit configuration or
   * the headers.
   * 
   * @param properties
   *          A String[] with the user-specified properties.
   * @param auConfig
   *          A Configuration with the target archival unit configuration.
   * @param headers
   *          A CIProperties with the headers.
   */
  private void processProperties(Map<String, String> properties,
      Configuration auConfig, CIProperties headers) {
    log.debug2("properties = {}", properties);
    log.debug2("auConfig = {}", auConfig);
    log.debug2("headers = {}", headers);

    if (properties != null && properties.size() > 0) {
      for (String key : properties.keySet()) {
	log.trace("key = {}", key);

	String value = properties.get(key);
	log.trace("value = {}", value);

	if (auConfig != null && auConfigKeys.contains(key)) {
	  auConfig.put(key, value);
	  log.trace("property '{}={}' stored in auConfig", key, value);
	} else {
	  headers.put(key, value);
	  log.trace("property '{}={}' stored in headers", key, value);
	}
      }
    }
  }
}
