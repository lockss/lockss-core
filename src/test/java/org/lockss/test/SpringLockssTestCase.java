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
package org.lockss.test;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.Map;
import java.util.Properties;
import org.lockss.config.ConfigManager;
import org.lockss.util.Logger;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.client.RestTemplate;
import org.springframework.web.util.UriComponents;
import org.springframework.web.util.UriComponentsBuilder;

/**
 * Base class for Spring REST web service test classes.
 */
public abstract class SpringLockssTestCase extends LockssTestCase4 {
  public static final String PLATFORM_DISK_SPACE_CONFIG_FILENAME =
      "platform.opt";

  private static final Logger log =
      Logger.getLoggerWithInitialLevel("SpringLockssTestCase",
                                       Logger.getInitialDefaultLevel());

  /**
   * Provides the value in a file for a property with a given name.
   * 
   * @param propertyName
   *          A String with the name of the configuration property.
   * @param configFile
   *          A File with the configuration file containing the configuration
   *          property value.
   * @return a String with the requested property value.
   */
  protected static String getPropertyValueFromFile(String propertyName,
      File configFile) {
    if (log.isDebug2()) {
      log.debug2("propertyName = " + propertyName);
      log.debug2("configFile = " + configFile.getAbsolutePath());
    }

    String propertyValue = null;
    FileInputStream is = null;

    try {
      // Get the properties from the configuration file.
      is = new FileInputStream(configFile);
      Properties properties = new Properties();
      properties.load(is);

      // Get the requested property value.
      propertyValue = properties.getProperty(propertyName);
      if (log.isDebug3()) log.debug3("propertyValue = " + propertyValue);
    } catch (IOException ioe) {
      log.warning("Caught exception getting properties from file: " + ioe);
    } finally {
      try {
	is.close();
      } catch (IOException ioe) {
	log.warning("Caught exception closing file input stream: " + ioe);
      }
    }

    if (log.isDebug2()) log.debug2("propertyValue = " + propertyValue);
    return propertyValue;
  }

  /**
   * Provides the indication of whether an external REST service is available.
   * 
   * @param restServiceLocation
   *          A String with the REST service location template.
   * @param uriMap
   *          A Map<String, String> with the map of values to be interpolated in
   *          the REST service location template.
   * @param successStatusCode
   *          An int with the expected successful status code returned by the
   *          REST service.
   * @return a boolean with <code>true</code> if the external REST service is
   *         available, <code> false</code> otherwise.
   */
  protected static boolean checkExternalRestService(String restServiceLocation,
      Map<String, String> uriMap, int successStatusCode) {
    if (log.isDebug2()) {
      log.debug2("restServiceLocation = " + restServiceLocation);
      log.debug2("uriMap = " + uriMap);
      log.debug2("successStatusCode = " + successStatusCode);
    }

    boolean isServiceAvailable = false;

    // Initialize the request to the REST service.
    RestTemplate restTemplate = new RestTemplate();

    // Initialize the request headers.
    HttpHeaders headers = new HttpHeaders();
    headers.setContentType(MediaType.APPLICATION_JSON);

    // Create the URI of the request to the REST service.
    UriComponents uriComponents = UriComponentsBuilder
	.fromUriString(restServiceLocation).build().expand(uriMap);

    URI uri = UriComponentsBuilder.newInstance()
	.uriComponents(uriComponents).build().encode().toUri();
    if (log.isDebug3()) log.debug3("Making request to '" + uri + "'...");

    try {
      // Make the request to the REST service.
      ResponseEntity<String> result = restTemplate.exchange(uri, HttpMethod.GET,
	  new HttpEntity<String>(null, headers), String.class);

      // Get its response.
      int statusCode = result.getStatusCode().value();
      if (log.isDebug3()) log.debug3("Done: statusCode = " + statusCode);

      // Determine whether the request has been successful.
      isServiceAvailable = statusCode == successStatusCode;
    } catch (Exception e) {
      if (log.isDebug3()) log.debug3("Done: No REST service.");
    }

    if (log.isDebug2())
      log.debug2("isServiceAvailable = " + isServiceAvailable);
    return isServiceAvailable;
  }

  /**
   * Creates a file that will communicate to the test REST service where its
   * data is located.
   *
   * @param dirPath
   *          A String with the path to the directory where the file is to be
   *          created.
   * @return a String with the path to the created file.
   * @throws IOException
   *           if there are problems.
   */
  protected String createPlatformDiskSpaceConfigFile(String dirPath)
      throws IOException {
    if (log.isDebug2()) log.debug2("dirPath = " + dirPath);

    // The configuration option with the temporary directory where the test data
    // resides.
    String platformDiskSpaceConfigParam =
	ConfigManager.PARAM_PLATFORM_DISK_SPACE_LIST + "=" + dirPath + "/cache"
	    + System.lineSeparator();
    if (log.isDebug3()) log.debug3("platformDiskSpaceConfigParam = '"
	+ platformDiskSpaceConfigParam +"'.");

    // The path to the file.
    String platformDiskSpaceConfigPath = dirPath + File.pathSeparator
	+ PLATFORM_DISK_SPACE_CONFIG_FILENAME;

    // Create the file.
    Files.write(Paths.get(platformDiskSpaceConfigPath),
	platformDiskSpaceConfigParam.getBytes(), StandardOpenOption.CREATE);

    if (log.isDebug2()) log.debug2("platformDiskSpaceConfigPath = "
	+ platformDiskSpaceConfigPath);
    return platformDiskSpaceConfigPath;
  }
}
