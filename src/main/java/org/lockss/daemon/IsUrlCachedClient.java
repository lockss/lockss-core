/*

 Copyright (c) 2016-2018 Board of Trustees of Leland Stanford Jr. University,
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
package org.lockss.daemon;

import java.net.Authenticator;
import java.net.PasswordAuthentication;
import java.net.URL;
import java.util.Iterator;
import javax.xml.namespace.QName;
import javax.xml.ws.Service;
import org.lockss.config.ConfigManager;
import org.lockss.config.CurrentConfig;
import org.lockss.laaws.rs.client.RestLockssRepositoryClient;
import org.lockss.laaws.rs.model.ArtifactIndexData;
import org.lockss.plugin.PluginManager;
import org.lockss.util.Logger;
import org.lockss.util.StringUtil;
import org.lockss.ws.content.ContentService;

/**
 * A client for the ContentService.isUrlCached() web service operation or for
 * the equivalent Repository REST web service.
 */
public class IsUrlCachedClient {
  private static Logger log = Logger.getLogger(IsUrlCachedClient.class);
  private static final String TIMEOUT_KEY =
      "com.sun.xml.internal.ws.request.timeout";

  /**
   * Provides an indication of whether a URL is cached.
   * 
   * @param url
   *          A String with the URL.
   * @return a boolean with the indication.
   * @throws Exception
   *           if there are problems getting the indication.
   */
  public boolean isUrlCached(String url) throws Exception {
    final String DEBUG_HEADER = "isUrlCached(): ";
    if (log.isDebug2()) log.debug2(DEBUG_HEADER + "url = " + url);

    // Get the URL of the configured REST Repository web service.
    String repoServiceUrl = ConfigManager.getCurrentConfig()
	.get(PluginManager.PARAM_REPOSERVICE_URL,
	    PluginManager.DEFAULT_REPOSERVICE_URL);
    if (log.isDebug3())
      log.debug3(DEBUG_HEADER + "repoServiceUrl = " + repoServiceUrl);

    // Check whether the URL of the configured REST Repository web service does
    // not exist.
    if (StringUtil.isNullString(repoServiceUrl)) {
      // Yes: Try to use the configured old REST service location.
      String restServiceLocation = CurrentConfig.getParam(
	  PluginManager.PARAM_URL_ARTIFACT_REST_SERVICE_LOCATION);
      if (log.isDebug3()) log.debug3(DEBUG_HEADER + "restServiceLocation = "
	  + restServiceLocation);

      // Check whether a REST service location has been configured.
      if (restServiceLocation != null
	  && restServiceLocation.trim().length() > 0) {
	// Yes: Get the indication of whether the URL is cached from the REST
	// service.
	boolean isUrlCached = new GetUrlRepositoryPropertiesClient()
	    .getUrlRepositoryProperties(url).getItems().size() > 0;
	if (log.isDebug2())
	  log.debug2(DEBUG_HEADER + "isUrlCached = " + isUrlCached);
	return isUrlCached;
      } else {
	// No: Get the indication from the non-REST service.
	return getProxy().isUrlCached(url, null);
      }
    } else {
      // No: Use the configured REST Repository web service.
      try {
	// Get the configured REST Repository web service collection name.
	String repoServiceCollection = ConfigManager.getCurrentConfig()
	    .get(PluginManager.PARAM_REPOSERVICE_COLLECTION,
		PluginManager.DEFAULT_REPOSERVICE_COLLECTION);
	if (log.isDebug3()) log.debug3(DEBUG_HEADER
	    + "repoServiceCollection = " + repoServiceCollection);

	RestLockssRepositoryClient rlrc =
	    new RestLockssRepositoryClient(new URL(repoServiceUrl));

	// Use the REST Repository web service to locate the artifacts.
	Iterator<ArtifactIndexData> artifactIterator = rlrc
	    .getArtifactsWithUriPrefix(repoServiceCollection, url);

	if (artifactIterator == null) {
	  throw new Exception("No artifacts found for URL '" + url + "'");
	}

	// Loop through all the results obtained from the REST Repository web
	// service.
	while (artifactIterator.hasNext()) {
	  // Check whether the URL matches.
	  if (url.equals(artifactIterator.next().getUri())) {
	    // Yes: Found, nothing else to do.
	    if (log.isDebug2())
	      log.debug2(DEBUG_HEADER + "Found: Returning true");
	    return true;
	  }
	}

	if (log.isDebug2())
	  log.debug2(DEBUG_HEADER + "Not found: Returning false");
	return false;
      } catch (Exception e) {
	log.warning("Exception caught: ", e);
      }

      return false;
    }
  }

  /**
   * Provides a proxy to the web service.
   * 
   * @return a ContentService with the proxy to the web service.
   * @throws Exception
   *           if there are problems getting the proxy.
   */
  protected ContentService getProxy() throws Exception {
    final String DEBUG_HEADER = "getProxy(): ";
    authenticate();
    String addressLocation = CurrentConfig.getParam(
	PluginManager.PARAM_URL_ARTIFACT_WS_ADDRESS_LOCATION);
    if (log.isDebug3())
      log.debug3(DEBUG_HEADER + "addressLocation = " + addressLocation);

    String targetNamespace = CurrentConfig.getParam(
	PluginManager.PARAM_URL_ARTIFACT_WS_TARGET_NAMESPACE);
    if (log.isDebug3())
      log.debug3(DEBUG_HEADER + "targetNamespace = " + targetNamespace);

    String serviceName = CurrentConfig.getParam(
	PluginManager.PARAM_URL_ARTIFACT_WS_SERVICE_NAME);
    if (log.isDebug3())
      log.debug3(DEBUG_HEADER + "serviceName = " + serviceName);

    Service service = Service.create(new URL(addressLocation), new QName(
	targetNamespace, serviceName));

    ContentService port = service.getPort(ContentService.class);

    // Set the client connection timeout.
    int timeoutValue = CurrentConfig.getIntParam(
	PluginManager.PARAM_URL_ARTIFACT_WS_TIMEOUT_VALUE,
	PluginManager.DEFAULT_URL_ARTIFACT_WS_TIMEOUT_VALUE);
    ((javax.xml.ws.BindingProvider) port).getRequestContext().put(TIMEOUT_KEY,
	new Integer(timeoutValue*1000));

    return port;
  }

  /**
   * Sets the authenticator that will be used by the networking code when the
   * HTTP server asks for authentication.
   */
  protected void authenticate() {
    Authenticator.setDefault(new Authenticator() {
      @Override
      protected PasswordAuthentication getPasswordAuthentication() {
	String userName = CurrentConfig
	    .getParam(PluginManager.PARAM_URL_ARTIFACT_WS_USER_NAME);
	String password = CurrentConfig
	    .getParam(PluginManager.PARAM_URL_ARTIFACT_WS_PASSWORD);
	return new PasswordAuthentication(userName, password.toCharArray());
      }
    });
  }
}
